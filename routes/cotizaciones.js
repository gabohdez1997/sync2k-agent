const express = require('express');
const router = express.Router();
const { sql, getPool, getServers, getExchangeRate } = require('../db');
const { executeWrite, writeResponse, paginatedResponse } = require('../helpers/multiSede');

console.log("🛠️ [AGENT] Iniciando Módulo de Cotizaciones (Versión Robusta v2)");

/**
 * @swagger
 * tags:
 *   name: Cotizaciones
 *   description: Gestión de cotizaciones de venta
 */

// 1. GET /api/v1/cotizaciones - Listado con filtros avanzados
router.get('/', async (req, res) => {
    try {
        const page  = parseInt(req.query.page)  || 1;
        const limit = parseInt(req.query.limit) || 12;
        const { sede, doc_num, co_cli, co_ven, fec_d, fec_h } = req.query;
        
        const servers = getServers();
        const targets = sede ? servers.filter(s => s.id === sede) : servers;

        const allData = await Promise.all(targets.map(async (srv) => {
            try {
                const pool = await getPool(srv.id, req.sqlAuth);
                const request = pool.request();
                let whereClauses = ["1=1"];

                if (doc_num) {
                    request.input('doc_num', sql.VarChar, `%${doc_num}%`);
                    whereClauses.push("c.doc_num LIKE @doc_num");
                }
                if (co_cli) {
                    request.input('co_cli_search', sql.VarChar, `%${co_cli}%`);
                    whereClauses.push("(c.co_cli LIKE @co_cli_search OR cl.cli_des LIKE @co_cli_search)");
                }
                if (co_ven) {
                    request.input('co_ven_search', sql.VarChar, co_ven);
                    whereClauses.push("c.co_ven = @co_ven_search");
                }
                if (fec_d) {
                    request.input('fec_d', sql.SmallDateTime, fec_d);
                    whereClauses.push("c.fec_emis >= @fec_d");
                }
                if (fec_h) {
                    request.input('fec_h', sql.SmallDateTime, fec_h);
                    whereClauses.push("c.fec_emis <= @fec_h");
                }

                const whereSQL = whereClauses.join(" AND ");
                const result = await request.query(`
                    SELECT RTRIM(c.doc_num) AS doc_num, RTRIM(c.descrip) AS descrip,
                           RTRIM(c.co_cli)  AS co_cli,  RTRIM(cl.cli_des) AS cli_des,
                           c.fec_emis, c.fec_venc, c.anulado,
                           RTRIM(c.co_mone) AS co_mone, c.tasa, c.total_neto
                    FROM saCotizacionCliente c
                    LEFT JOIN saCliente cl ON c.co_cli = cl.co_cli
                    WHERE ${whereSQL}
                    ORDER BY c.fec_emis DESC, c.doc_num DESC
                `);
                return result.recordset.map(c => ({ ...c, sede_id: srv.id, sede_nombre: srv.name }));
            } catch (e) { return []; }
        }));

        const combined = [].concat(...allData);
        combined.sort((a, b) => new Date(b.fec_emis) - new Date(a.fec_emis));
        return paginatedResponse(res, combined, page, limit);
    } catch (error) {
        res.status(500).json({ success: false, message: 'Error al consultar cotizaciones.', error: error.message });
    }
});

// 2. POST /api/v1/cotizaciones - Crear
router.post('/', async (req, res) => {
    const data = req.body;
    console.log("📥 [AGENT] Recibiendo Cotización:", JSON.stringify({ ...data, renglones: data.renglones?.length }, null, 2));

    if (!data.co_cli || !data.renglones) {
        return res.status(400).json({ success: false, message: 'Campos obligatorios: co_cli, renglones' });
    }

    const outcome = await executeWrite(req.query.sede || null, req.sqlAuth, async (pool) => {
        // 1. Cargar Catálogos y Parámetros Globales (Fallbacks)
        const [resMoneda, resAlma, resVen, resCond, resTran, resSucu, resCli] = await Promise.all([
            pool.request().query(`SELECT TOP 1 RTRIM(g_moneda) AS g_moneda FROM par_emp`),
            pool.request().query(`SELECT TOP 1 RTRIM(co_alma) AS co_alma FROM saAlmacen`),
            pool.request().query(`SELECT TOP 1 RTRIM(co_ven)  AS co_ven  FROM saVendedor`),
            pool.request().query(`SELECT TOP 1 RTRIM(co_cond) AS co_cond FROM saCondicionPago`),
            pool.request().query(`SELECT TOP 1 RTRIM(co_tran) AS co_tran FROM saTransporte`),
            pool.request().query(`SELECT TOP 1 RTRIM(co_sucur) AS co_sucur FROM saSucursal`),
            pool.request().input('co_cli', sql.Char(16), data.co_cli).query(`SELECT RTRIM(co_mone) as co_mone, RTRIM(cond_pag) as cond_pag, RTRIM(co_ven) as co_ven, RTRIM(co_zon) as co_zon, RTRIM(co_sucu_in) as co_sucu FROM saCliente WHERE co_cli = @co_cli`)
        ]);

        const cli = resCli.recordset[0] || {};
        
        // Determinar TasaBCV de forma Robusta
        const tasaBCV = await getExchangeRate(pool);
        console.log(`💱 [AGENT] Tasa BCV detectada para esta transacción: ${tasaBCV}`);

        // Jerarquía de Defaults: Web > Cliente > Empresa > Catálogo
        const defMone  = cli.co_mone  || resMoneda.recordset[0]?.g_moneda || 'BS';
        const defVen   = cli.co_ven   || resVen.recordset[0]?.co_ven     || '01';
        const defCond  = cli.cond_pag || resCond.recordset[0]?.co_cond   || '01';
        const defZon   = cli.co_zon   || '01';
        const defAlma  = resAlma.recordset[0]?.co_alma   || '01';
        const defTran  = resTran.recordset[0]?.co_tran   || '01';
        const defSucu  = resSucu.recordset[0]?.co_sucur  || '01';

        const auditUser = (req.profitUser || req.sqlAuth?.user || '01').substring(0, 10).toUpperCase();
        const tsDate    = new Date();
        const fVenc     = new Date();
        fVenc.setDate(tsDate.getDate() + 7); 

        // exchange rate for conversion if needed
        const tasaDoc = tasaBCV;
        const isUSD = data.showUSD === true;
        const renglones = data.renglones;

        console.log(`🏦 [AGENT] Documento en USD: ${isUSD} | Tasa Doc: ${tasaDoc}`);

        let totalBruto = 0;
        let totalImp = 0;
        
        // Calculate totals. If isUSD is true, we convert to local currency (Bs) 
        // because Profit stores transacted values in Bs.
        data.renglones.forEach(r => {
            const qty = Number(r.cantidad || 0);
            const prcBase = Number(r.precio || 0);
            const pImp = Number(r.porc_imp || 0);
            
            // Convert to Bs if the document is in USD
            const prcBs = isUSD ? (prcBase * tasaDoc) : prcBase;
            
            const sub = qty * prcBs;
            const imp = (sub * pImp) / 100;
            totalBruto += sub;
            totalImp += imp;
        });

        const totalNeto = totalBruto + totalImp;
        console.log(`💰 [AGENT] Totales: Bruto=${totalBruto.toFixed(2)} | Neto=${totalNeto.toFixed(2)}`);

        const transaction = new sql.Transaction(pool);
        await transaction.begin();

        try {
            // --- RESERVA ATÓMICA DE CORRELATIVO ---
            // Buscamos la serie vinculada a CCLI_NUM y aumentamos el contador de forma segura
            // Filtramos por co_serie IS NOT NULL para evitar filas de configuración vacías
            const resCorr = await transaction.request().query(`
                UPDATE saSerie 
                SET prox_n = prox_n + 1, fe_us_mo = GETDATE()
                OUTPUT INSERTED.prox_n, RTRIM(INSERTED.desde_a) as prefijo
                WHERE co_serie = (
                    SELECT TOP 1 co_serie 
                    FROM saConsecutivo 
                    WHERE LTRIM(RTRIM(co_consecutivo)) = 'CCLI_NUM'
                      AND co_serie IS NOT NULL
                )
            `);

            if (!resCorr.recordset.length) {
                throw new Error("No se pudo obtener el correlativo oficial (CCLI_NUM). Verifique la configuración de series en Profit.");
            }

            const { prox_n, prefijo } = resCorr.recordset[0];
            // Si el usuario espera 4331 y prox_n era 4330, el UPDATE lo llevó a 4331.
            // Usamos directamente el valor incrementado.
            const docNum = (prefijo || '') + (prox_n).toString().padStart(10, '0');

            // --- DESCRIPCIÓN ---
            const safeDescrip = (data.descrip || 'COTIZACIÓN WEB').toUpperCase().substring(0, 60);
            const safeComent  = (data.comentario || 'Generado via Web App').substring(0, 500); 
            const safeDir     = (data.dir_ent || '').substring(0, 500);

            const rH = new sql.Request(transaction);
            rH.input('sDoc_Num',         sql.Char(20),         docNum);
            rH.input('sDescrip',         sql.VarChar(60),      safeDescrip);
            rH.input('sCo_Cli',          sql.Char(16),         data.co_cli);
            rH.input('sCo_Tran',         sql.Char(6),          data.co_tran || defTran);
            rH.input('sCo_Mone',         sql.Char(6),          isUSD ? usdCode : defMone);
            rH.input('sCo_Ven',          sql.Char(6),          data.co_ven  || defVen);
            rH.input('sCo_Cond',         sql.Char(6),          data.co_cond || defCond);
            rH.input('sdFec_Emis',       sql.SmallDateTime,    tsDate);
            rH.input('sdFec_Venc',       sql.SmallDateTime,    fVenc);
            rH.input('sdFec_Reg',        sql.SmallDateTime,    tsDate);
            rH.input('bAnulado',         sql.Bit,              0);
            rH.input('sStatus',          sql.Char(1),          '0');
            rH.input('deTasa',           sql.Decimal(21, 8),   tasaDoc);
            rH.input('sN_Control',       sql.VarChar(20),      '');
            rH.input('deMonto_Desc_Glob',sql.Decimal(18, 2),   0);
            rH.input('deMonto_Reca',     sql.Decimal(18, 2),   0);
            rH.input('deSaldo',          sql.Decimal(18, 2),   totalNeto >= 0 ? totalNeto : 0);
            rH.input('deTotal_Bruto',    sql.Decimal(18, 2),   totalBruto >= 0 ? totalBruto : 0);
            rH.input('deMonto_Imp',      sql.Decimal(18, 2),   totalImp >= 0 ? totalImp : 0);
            rH.input('deMonto_Imp2',     sql.Decimal(18, 2),   0);
            rH.input('deMonto_Imp3',     sql.Decimal(18, 2),   0);
            rH.input('deOtros1',         sql.Decimal(18, 2),   0);
            rH.input('deOtros2',         sql.Decimal(18, 2),   0);
            rH.input('deOtros3',         sql.Decimal(18, 2),   0);
            rH.input('deTotal_Neto',     sql.Decimal(18, 2),   totalNeto >= 0 ? totalNeto : 0);
            rH.input('sDis_Cen',         sql.VarChar(sql.MAX), '');
            rH.input('sComentario',      sql.VarChar(sql.MAX), safeComent);
            rH.input('sDir_Ent',         sql.VarChar(sql.MAX), safeDir);
            rH.input('bContrib',         sql.Bit,              1);
            rH.input('bImpresa',         sql.Bit,              0);
            rH.input('sSalestax',        sql.Char(8),          null);
            rH.input('sImpfis',          sql.VarChar(20),      '');
            rH.input('sImpfisfac',       sql.VarChar(20),      '');
            rH.input('bVen_Ter',         sql.Bit,              0);
            rH.input('sCo_Us_In',        sql.Char(6),          auditUser);
            rH.input('sCo_Sucu_In',      sql.Char(6),          data.co_sucu_in || defSucu);
            rH.input('sMaquina',         sql.VarChar(60),      'SYNC2K');
            
            await rH.execute('pInsertarCotizacionCliente');

            // Obtener GUID
            const guidRes = await new sql.Request(transaction)
                .input('doc_num', sql.Char(20), docNum)
                .query('SELECT rowguid FROM saCotizacionCliente WHERE doc_num = @doc_num');
            const rowguidDoc = guidRes.recordset[0].rowguid;

            // INSERT RENGLONES
            for(let i=0; i < renglones.length; i++) {
                const item = renglones[i];
                const qty = Number(item.cantidad || 0);
                const prcBase = Number(item.precio || 0);
                
                // Calculate prices in both currencies
                let prcBs = isUSD ? (prcBase * tasaDoc) : prcBase;
                let prcUSD = isUSD ? prcBase : (prcBase / tasaDoc);

                // --- SEGURIDAD: Detección de enriquecimiento fallido ---
                // Si el documento es en BS, pero el precio recibido es sospechosamente bajo 
                // (ej. 12.01) y la tasa es alta (ej. 477), es probable que la web 
                // haya enviado el precio USD por error.
                if (!isUSD && prcBase > 0 && prcBase < (tasaDoc / 2) && prcBase < 100) {
                    console.log(`⚠️ [AGENT] Alerta: Precio BS sospechosamente bajo (${prcBase}). Posible desajuste USD/BS. Corrigiendo...`);
                    prcBs = prcBase * tasaDoc;
                    prcUSD = prcBase;
                }

                console.log(`📝 [AGENT] Renglon ${i+1}: Art=${item.co_art} | Qty=${qty} | PrcIn=${prcBase} | PrcBS=${prcBs.toFixed(2)} | PrcUSD=${prcUSD.toFixed(4)}`);
                
                const pImp = Number(item.porc_imp || 0);
                const lineSubtotal = qty * prcBs;
                const lineImp = (lineSubtotal * pImp) / 100;

                // Dinamically find the correct unit for this article if not provided
                let lineUni = item.co_uni;
                if (!lineUni) {
                    const uniRes = await transaction.request()
                        .input('co_art', sql.Char(30), item.co_art)
                        .query(`SELECT TOP 1 RTRIM(co_uni) as co_uni FROM saArtUnidad 
                                WHERE LTRIM(RTRIM(co_art)) = LTRIM(RTRIM(@co_art)) 
                                ORDER BY uni_principal DESC`);
                    lineUni = uniRes.recordset[0]?.co_uni || 'UNI';
                }

                const rL = new sql.Request(transaction);
                rL.input('iReng_Num',    sql.Int,              i + 1);
                rL.input('sDoc_Num',     sql.Char(20),         docNum);
                rL.input('sCo_Art',      sql.Char(30),         item.co_art);
                rL.input('sDes_Art',     sql.VarChar(120),      (item.art_des || '').substring(0, 120));
                rL.input('sCo_Uni',      sql.Char(6),          lineUni);
                rL.input('sCo_Alma',     sql.Char(6),          item.co_alma || defAlma);
                rL.input('sCo_Precio',   sql.Char(6),          item.co_precio || '01');
                rL.input('sTipo_Imp',    sql.Char(1),          item.tipo_imp || '1');
                rL.input('deTotal_Art',  sql.Decimal(18, 5),    qty);
                rL.input('deSTotal_Art', sql.Decimal(18, 5),    qty); 
                rL.input('dePrec_Vta',   sql.Decimal(18, 5),    prcBs);
                rL.input('sPorc_Desc',   sql.VarChar(15),       '0');
                rL.input('deMonto_Desc', sql.Decimal(18, 5),    0);
                rL.input('deReng_Neto',  sql.Decimal(18, 2),    lineSubtotal);
                rL.input('dePendiente',  sql.Decimal(18, 5),    qty);
                rL.input('dePendiente2', sql.Decimal(18, 5),    0);
                rL.input('deOtros',      sql.Decimal(18, 5),    0);
                rL.input('deMonto_Dev',  sql.Decimal(18, 5),    0);
                rL.input('deMonto_Desc_Glob',   sql.Decimal(18, 5), 0);
                rL.input('deMonto_reca_Glob',   sql.Decimal(18, 5), 0);
                rL.input('deOtros1_glob',       sql.Decimal(18, 5), 0);
                rL.input('deOtros2_glob',       sql.Decimal(18, 5), 0);
                rL.input('deOtros3_glob',       sql.Decimal(18, 5), 0);
                rL.input('deMonto_imp_afec_glob',  sql.Decimal(18, 5), 0);
                rL.input('deMonto_imp2_afec_glob', sql.Decimal(18, 5), 0);
                rL.input('deMonto_imp3_afec_glob', sql.Decimal(18, 5), 0);
                rL.input('sTipo_Doc',           sql.Char(4),        'CCLI');
                rL.input('gRowguid_Doc',        sql.UniqueIdentifier, rowguidDoc);
                rL.input('sNum_Doc',            sql.VarChar(20),      docNum);
                rL.input('dePorc_Imp',          sql.Decimal(18, 5),   item.porc_imp || 0);
                rL.input('dePorc_Imp2',         sql.Decimal(18, 5),   0);
                rL.input('dePorc_Imp3',         sql.Decimal(18, 5),   0);
                rL.input('deMonto_Imp',         sql.Decimal(18, 5),   lineImp);
                rL.input('deMonto_Imp2',        sql.Decimal(18, 5),   0);
                rL.input('deMonto_Imp3',        sql.Decimal(18, 5),   0);
                rL.input('deTotal_Dev',         sql.Decimal(18, 5),   0);
                rL.input('sComentario',         sql.VarChar(sql.MAX), '');
                rL.input('sCo_Sucu_In',         sql.Char(6),          item.co_sucu_in || defSucu);
                rL.input('sCo_Us_In',           sql.Char(6),          auditUser);
                rL.input('sREVISADO',           sql.Char(1),          '0');
                rL.input('sTRASNFE',            sql.Char(1),          '0');
                rL.input('sMaquina',            sql.VarChar(60),      'SYNC2K');
                
                await rL.execute('pInsertarRenglonesCotizacionCliente');

                // Asegurar precio OM (en dólares) directamente en la tabla
                await transaction.request()
                    .input('om', sql.Decimal(18, 5), prcUSD)
                    .input('doc', sql.Char(20), docNum)
                    .input('reng', sql.Int, i + 1)
                    .query(`UPDATE saCotizacionClienteReng SET prec_vta_om = @om WHERE doc_num = @doc AND reng_num = @reng`);
            }

            await transaction.commit();
            return { doc_num: docNum, detail: 'Cotización creada exitosamente' };
        } catch (err) {
            console.error('[AGENT] Error en transacción de cotización:', err);
            if (transaction._aborted === false) {
                try { await transaction.rollback(); } catch (e) { /* ignore rollback error */ }
            }
            throw err;
        }
    });

    return writeResponse(res, outcome, `Sede "${req.query.sede}" no encontrada.`);
});

module.exports = router;
