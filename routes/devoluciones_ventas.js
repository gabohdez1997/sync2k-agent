const express = require('express');
const router = express.Router();
const { sql, getPool, getServers, getExchangeRate } = require('../db');
const { executeWrite, paginatedResponse, padProfit } = require('../helpers/multiSede');
const { getProximoConsecutivo } = require('../helpers/consecutivos');

/**
 * @swagger
 * tags:
 *   name: DevolucionesVentas
 *   description: Gestión de Devoluciones de Venta a Clientes (Caja)
 */

// --- LISTAR DEVOLUCIONES ---
router.get('/', async (req, res) => {
    try {
        const page  = parseInt(req.query.page)  || 1;
        const limit = parseInt(req.query.limit) || 12;
        const { sede, doc_num, co_cli, co_ven, co_us_in, fec_d, fec_h, search, anulado } = req.query;
        
        const servers = getServers();
        const targets = sede ? servers.filter(s => s.id === sede) : servers;

        const allData = await Promise.all(targets.map(async (srv) => {
            try {
                const pool = await getPool(srv.id, req.sqlAuth);
                const request = pool.request();
                let whereClauses = ["1=1"];

                if (anulado !== undefined) {
                    request.input('anulado_filter', sql.Bit, anulado === 'true' || anulado === '1' ? 1 : 0);
                    whereClauses.push("d.anulado = @anulado_filter");
                }
                if (doc_num) {
                    request.input('doc_num', sql.VarChar, `%${doc_num}%`);
                    whereClauses.push("(d.doc_num LIKE @doc_num OR d.nro_doc LIKE @doc_num)");
                }
                if (co_cli) {
                    request.input('co_cli_search', sql.VarChar, `%${co_cli}%`);
                    whereClauses.push("(d.co_cli LIKE @co_cli_search OR cl.cli_des LIKE @co_cli_search OR cl.rif LIKE @co_cli_search)");
                }
                if (search) {
                    request.input('search_all', sql.VarChar, `%${search}%`);
                    whereClauses.push("(d.doc_num LIKE @search_all OR d.nro_doc LIKE @search_all OR d.co_cli LIKE @search_all OR cl.cli_des LIKE @search_all OR cl.rif LIKE @search_all OR d.descrip LIKE @search_all)");
                }
                if (co_ven) {
                    request.input('co_ven_filter', sql.VarChar, co_ven.trim().toUpperCase());
                    whereClauses.push("LTRIM(RTRIM(d.co_ven)) = @co_ven_filter");
                }
                if (co_us_in) {
                    request.input('co_us_in_filter', sql.VarChar, co_us_in.trim().toUpperCase());
                    whereClauses.push("LTRIM(RTRIM(d.co_us_in)) = @co_us_in_filter");
                }
                if (fec_d) {
                    request.input('fec_d', sql.SmallDateTime, fec_d);
                    whereClauses.push("d.fec_emis >= @fec_d");
                }
                if (fec_h) {
                    request.input('fec_h', sql.SmallDateTime, fec_h);
                    whereClauses.push("d.fec_emis < DATEADD(day, 1, @fec_h)");
                }

                const whereSQL = whereClauses.join(" AND ");
                
                const result = await request.query(`
                    SELECT RTRIM(d.doc_num) AS doc_num, RTRIM(d.descrip) AS descrip,
                           RTRIM(d.co_cli)  AS co_cli,  RTRIM(cl.cli_des) AS cli_des, RTRIM(cl.rif) AS rif,
                           d.fec_emis, d.fec_venc, d.fec_reg, d.fe_us_in AS fec_us_in, d.fe_us_mo AS fec_us_mo, d.anulado,
                           RTRIM(d.co_mone) AS co_mone, d.tasa, d.total_neto, d.total_bruto, d.monto_imp,
                           RTRIM(d.co_tipo_doc) AS co_tipo_doc, RTRIM(d.nro_doc) AS nro_doc, RTRIM(d.n_control) AS n_control,
                           RTRIM(d.co_ven) AS co_ven, RTRIM(v.ven_des) AS ven_des, RTRIM(d.co_us_in) AS co_us_in, RTRIM(d.co_sucu_in) AS co_sucu_in
                    FROM saDevolucionCliente d
                    LEFT JOIN saCliente cl ON d.co_cli = cl.co_cli
                    LEFT JOIN saVendedor v ON d.co_ven = v.co_ven
                    WHERE ${whereSQL}
                    ORDER BY d.fec_emis DESC, d.doc_num DESC
                `);

                return result.recordset.map(c => ({ ...c, sede_id: srv.id, sede_nombre: srv.name }));
            } catch (e) { 
                console.error(`[DEVOLUCIONES] Error en sede ${srv.id}:`, e.message);
                return []; 
            }
        }));

        const combined = [].concat(...allData);
        combined.sort((a, b) => new Date(b.fec_emis) - new Date(a.fec_emis));
        return paginatedResponse(res, combined, page, limit);
    } catch (error) {
        res.status(500).json({ success: false, message: 'Error al consultar Devoluciones.', error: error.message });
    }
});

// --- OBTENER DETALLE DEVOLUCION ---
router.get('/:doc_num', async (req, res) => {
    try {
        const { doc_num } = req.params;
        const { sede } = req.query;
        const servers = getServers();
        const targets = sede ? servers.filter(s => s.id === sede) : servers;

        if (targets.length === 0)
            return res.status(404).json({ success: false, message: `Sede "${sede}" no encontrada.` });

        const results = await Promise.all(targets.map(async (srv) => {
            try {
                const pool = await getPool(srv.id, req.sqlAuth);

                const [resEnc, resReng, currentRate] = await Promise.all([
                    pool.request().input('doc_num', sql.VarChar, doc_num).query(`
                        SELECT RTRIM(d.doc_num) AS doc_num, RTRIM(d.descrip) AS descrip,
                               RTRIM(d.co_cli)  AS co_cli,  RTRIM(cl.cli_des) AS cli_des,
                               RTRIM(d.co_ven)  AS co_ven,  RTRIM(v.ven_des)  AS ven_des,
                               RTRIM(d.co_cond) AS co_cond, RTRIM(cd.cond_des) AS cond_des,
                               d.fec_emis, d.fec_venc, d.fec_reg, d.fe_us_in AS fec_us_in, d.fe_us_mo AS fec_us_mo, d.anulado,
                               RTRIM(d.co_mone) AS co_mone, d.tasa,
                               d.total_bruto, d.monto_imp, d.total_neto, d.saldo,
                               RTRIM(d.co_tipo_doc) AS co_tipo_doc, RTRIM(d.nro_doc) AS nro_doc, RTRIM(d.n_control) AS n_control,
                               RTRIM(d.comentario) AS comentario,
                               RTRIM(cl.rif) AS rif, RTRIM(cl.direc1) AS direc1, 
                               RTRIM(cl.telefonos) AS telefonos, RTRIM(cl.email) AS email,
                               RTRIM(cl.co_zon) AS co_zon, RTRIM(z.zon_des) AS zon_des, 
                               cl.contribu_e, cl.porc_esp,
                               RTRIM(d.co_us_in) AS co_us_in, RTRIM(d.co_sucu_in) AS co_sucu_in
                        FROM saDevolucionCliente d
                        LEFT JOIN saCliente      cl ON d.co_cli  = cl.co_cli
                        LEFT JOIN saVendedor     v  ON d.co_ven  = v.co_ven
                        LEFT JOIN saCondicionPago cd ON d.co_cond = cd.co_cond
                        LEFT JOIN saZona         z  ON cl.co_zon = z.co_zon
                        WHERE LTRIM(RTRIM(d.doc_num)) = LTRIM(RTRIM(@doc_num))
                    `),
                    pool.request().input('doc_num', sql.VarChar, doc_num).query(`
                        SELECT r.reng_num, RTRIM(r.co_art) AS co_art, RTRIM(a.art_des) AS art_des,
                               RTRIM(a.co_lin) AS co_lin, RTRIM(a.co_subl) AS co_subl,
                               r.total_art AS cantidad, r.pendiente, RTRIM(r.co_alma) AS co_alma,
                               r.co_precio AS co_precio, r.prec_vta AS precio,
                               RTRIM(r.tipo_imp) AS tipo_imp, r.porc_imp, r.reng_neto AS total_renglon,
                               r.prec_vta_om, RTRIM(r.co_uni) AS co_uni, RTRIM(u.des_uni) AS unidad,
                               RTRIM(r.tipo_doc) AS tipo_doc, RTRIM(r.num_doc) AS num_doc, r.rowguid_doc,
                               r.rowguid
                        FROM saDevolucionClienteReng r
                        LEFT JOIN saArticulo a ON r.co_art = a.co_art
                        LEFT JOIN saUnidad u ON r.co_uni = u.co_uni
                        WHERE LTRIM(RTRIM(r.doc_num)) = LTRIM(RTRIM(@doc_num))
                        ORDER BY r.reng_num
                    `),
                    getExchangeRate(pool)
                ]);

                if (!resEnc.recordset.length) return null;
                return { 
                    ...resEnc.recordset[0], 
                    renglones: resReng.recordset, 
                    tasa_actual: currentRate,
                    sede_id: srv.id, 
                    sede_nombre: srv.name 
                };
            } catch (e) {
                return { sede_id: srv.id, sede_nombre: srv.name, error: e.message };
            }
        }));

        const found = results.filter(r => r && !r.error);
        if (!found.length)
            return res.status(404).json({ success: false, message: 'Devolución no encontrada.' });

        res.status(200).json({ success: true, count: found.length, data: results.filter(r => r !== null) });

    } catch (error) {
        res.status(500).json({ success: false, message: 'Error al consultar Devolución.', error: error.message });
    }
});

// --- ANULAR DEVOLUCION ---
router.post('/:doc_num/anular', async (req, res) => {
    try {
        const { doc_num } = req.params;
        const { sede } = req.query;

        const outcome = await executeWrite(sede || null, req.sqlAuth, async (pool) => {
            const resH = await pool.request().input('doc_num', sql.VarChar, doc_num).query(
                `SELECT rowguid, anulado, RTRIM(co_us_in) AS co_us_in, RTRIM(nro_doc) AS nro_doc, RTRIM(co_tipo_doc) AS co_tipo_doc
                 FROM saDevolucionCliente
                 WHERE LTRIM(RTRIM(doc_num)) = LTRIM(RTRIM(@doc_num))`
            );
            if (!resH.recordset.length) throw new Error('Devolución no existe.');

            const { anulado, nro_doc } = resH.recordset[0];
            if (anulado) {
                throw new Error(`La devolución ${doc_num} ya está anulada.`);
            }

            // Obtener renglones de la devolución
            const resL = await pool.request().input('doc_num', sql.VarChar, doc_num).query(
                `SELECT reng_num, co_art, co_alma, co_uni, total_art, rowguid, RTRIM(tipo_doc) AS tipo_doc, RTRIM(num_doc) AS num_doc, rowguid_doc 
                 FROM saDevolucionClienteReng 
                 WHERE LTRIM(RTRIM(doc_num)) = LTRIM(RTRIM(@doc_num))`
            );

            const transaction = new sql.Transaction(pool);
            await transaction.begin();

            try {
                const auditUser = (req.profitUser || 'API').substring(0, 10).toUpperCase();

                // 1. Anular cabecera de la devolución
                await transaction.request()
                    .input('doc_num', sql.Char(20), padProfit(doc_num, 20))
                    .input('auditUser', sql.Char(6), padProfit(auditUser, 6))
                    .query(`
                        UPDATE saDevolucionCliente
                        SET anulado = 1,
                            fe_us_mo = GETDATE(),
                            co_us_mo = @auditUser
                        WHERE LTRIM(RTRIM(doc_num)) = LTRIM(RTRIM(@doc_num))
                    `);

                // 2. Anular Nota de Crédito asociada en saDocumentoVenta si existe
                if (nro_doc) {
                    await transaction.request()
                        .input('nro_doc', sql.Char(20), padProfit(nro_doc, 20))
                        .input('auditUser', sql.Char(6), padProfit(auditUser, 6))
                        .query(`
                            UPDATE saDocumentoVenta
                            SET anulado = 1,
                                saldo = 0,
                                fe_us_mo = GETDATE(),
                                co_us_mo = @auditUser
                            WHERE LTRIM(RTRIM(nro_doc)) = LTRIM(RTRIM(@nro_doc))
                              AND LTRIM(RTRIM(co_tipo_doc)) = 'N/CR'
                        `);
                }

                // 3. Revertir inventario (Restar de 'ACT' porque se había sumado al devolver)
                // Y revertir total_dev en la factura original (saFacturaVentaReng)
                for (const line of resL.recordset) {
                    const rStock = new sql.Request(transaction);
                    rStock.input('sCo_Alma',              sql.Char(6),  line.co_alma);
                    rStock.input('sCo_Art',               sql.Char(30), line.co_art);
                    rStock.input('sCo_Uni',               sql.Char(6),  line.co_uni);
                    rStock.input('deCantidad',            sql.Decimal(18, 5), line.total_art);
                    rStock.input('sTipoStock',            sql.Char(4),  'ACT');
                    rStock.input('bSumarStock',           sql.Bit,      0); // Restar stock
                    rStock.input('bPermiteStockNegativo', sql.Bit,      1);
                    await rStock.execute('pStockActualizar');

                    // Revertir total_dev en saFacturaVentaReng
                    if (line.rowguid_doc) {
                        await transaction.request()
                            .input('qty', sql.Decimal(18, 5), line.total_art)
                            .input('rowguid_doc', sql.UniqueIdentifier, line.rowguid_doc)
                            .input('auditUser', sql.Char(6), padProfit(auditUser, 6))
                            .query(`
                                UPDATE saFacturaVentaReng
                                SET total_dev = CASE WHEN total_dev >= @qty THEN total_dev - @qty ELSE 0 END,
                                    fe_us_mo = GETDATE(),
                                    co_us_mo = @auditUser
                                WHERE rowguid = @rowguid_doc;
                            `);
                    }
                }

                await transaction.commit();
                return { success: true, message: `Devolución ${doc_num} anulada con éxito.` };
            } catch (errTx) {
                await transaction.rollback();
                throw errTx;
            }
        });

        res.status(200).json({ success: true, message: outcome.message || 'Devolución anulada.', data: outcome });
    } catch (error) {
        console.error('[DEVOLUCION ANULAR ERROR]:', error);
        res.status(500).json({ success: false, message: 'Error al anular devolución.', error: error.message });
    }
});

// --- CREAR DEVOLUCION ---
router.post('/', async (req, res) => {
    try {
        const data = req.body;
        const { sede } = req.query;

        if (!data || !data.co_cli) {
            return res.status(400).json({ success: false, message: 'Datos incompletos: se requiere cliente (co_cli).' });
        }

        if (!Array.isArray(data.renglones) || data.renglones.length === 0) {
            return res.status(400).json({ success: false, message: 'Debe incluir al menos un renglón para procesar la devolución.' });
        }

        const outcome = await executeWrite(sede || null, req.sqlAuth, async (pool, srv) => {
            // 1. Validaciones previas de la factura origen y renglones
            const facturaNum = (data.factura_origen || data.num_doc || data.renglones[0]?.num_doc || '').trim();
            if (!facturaNum) {
                throw new Error('Debe especificar la factura de venta de origen.');
            }

            // Consultar factura original
            const facCheck = await pool.request().input('doc_num', sql.VarChar, facturaNum).query(`
                SELECT doc_num, co_cli, anulado, tasa, co_mone, co_ven, co_cond, co_tran, co_sucu_in
                FROM saFacturaVenta
                WHERE LTRIM(RTRIM(doc_num)) = LTRIM(RTRIM(@doc_num))
            `);

            if (!facCheck.recordset.length) {
                throw new Error(`La factura de origen ${facturaNum} no existe.`);
            }

            const facOrig = facCheck.recordset[0];
            if (facOrig.anulado) {
                throw new Error(`La factura ${facturaNum} está anulada. No se pueden procesar devoluciones de facturas anuladas.`);
            }

            // Consultar renglones actuales de la factura origen
            const facRengCheck = await pool.request().input('doc_num', sql.VarChar, facturaNum).query(`
                SELECT reng_num, co_art, total_art, ISNULL(total_dev, 0) AS total_dev, rowguid
                FROM saFacturaVentaReng
                WHERE LTRIM(RTRIM(doc_num)) = LTRIM(RTRIM(@doc_num))
            `);

            const mapRengOrig = new Map();
            facRengCheck.recordset.forEach(r => {
                mapRengOrig.set(String(r.rowguid).toLowerCase(), r);
                mapRengOrig.set(String(r.reng_num), r);
            });

            // Validar que ninguna cantidad devuelta supere la cantidad facturada disponible
            for (const item of data.renglones) {
                const qty = Number(item.cantidad || 0);
                if (qty <= 0) {
                    throw new Error(`La cantidad a devolver para el artículo ${item.co_art} debe ser mayor a cero.`);
                }

                let origReng = null;
                if (item.rowguid_doc && mapRengOrig.has(String(item.rowguid_doc).toLowerCase())) {
                    origReng = mapRengOrig.get(String(item.rowguid_doc).toLowerCase());
                } else if (item.reng_num && mapRengOrig.has(String(item.reng_num))) {
                    origReng = mapRengOrig.get(String(item.reng_num));
                }

                if (origReng) {
                    const facturada = Number(origReng.total_art);
                    const previaDev = Number(origReng.total_dev);
                    const disponible = Math.max(0, facturada - previaDev);

                    if (qty > disponible) {
                        throw new Error(`La cantidad a devolver (${qty}) para el artículo ${String(item.co_art).trim()} supera la cantidad máxima disponible (${disponible}). Cantidad facturada: ${facturada}, previamente devuelta: ${previaDev}.`);
                    }
                }
            }

            // 2. Cargar catálogos auxiliares de Profit
            const [resCli, resUSD, resTasa, resVen, resCond, resAlma, resSucu, resTran, resTax] = await Promise.all([
                pool.request().input('co_cli', sql.Char(16), data.co_cli).query(`SELECT RTRIM(co_mone) as co_mone, RTRIM(cond_pag) as cond_pag, RTRIM(co_ven) as co_ven, RTRIM(co_sucu_in) as co_sucu FROM saCliente WHERE co_cli = @co_cli`),
                pool.request().query(`SELECT TOP 1 RTRIM(co_mone) AS co_mone FROM saMoneda WHERE LTRIM(RTRIM(co_mone)) IN ('US$','USD','DOL','$','US')`),
                pool.request().query(`SELECT TOP 1 tasa_v FROM saTasa WHERE LTRIM(RTRIM(co_mone)) IN ('US$','USD','DOL','$','US') ORDER BY fecha DESC`),
                pool.request().query(`SELECT TOP 1 RTRIM(co_ven)  AS co_ven  FROM saVendedor`),
                pool.request().query(`SELECT TOP 1 RTRIM(co_cond) AS co_cond FROM saCondicionPago`),
                pool.request().query(`SELECT TOP 1 RTRIM(co_alma) AS co_alma FROM saAlmacen`),
                pool.request().query(`SELECT TOP 1 RTRIM(co_sucur) AS co_sucur FROM saSucursal`),
                pool.request().query(`SELECT TOP 1 RTRIM(co_tran) AS co_tran FROM saTransporte`),
                pool.request().query(`SELECT TOP 1 RTRIM(tax_id) AS tax_id FROM saTax`)
            ]);

            const cli = resCli.recordset[0] || {};
            const usdCode = resUSD.recordset[0]?.co_mone || 'US$';
            const defVen   = facOrig.co_ven  || cli.co_ven   || resVen.recordset[0]?.co_ven   || '01';
            const defCond  = facOrig.co_cond || cli.cond_pag || resCond.recordset[0]?.co_cond || '01';
            const defAlma  = resAlma.recordset[0]?.co_alma   || '01';
            const defSucu  = facOrig.co_sucu_in || resSucu.recordset[0]?.co_sucur || '01';
            const defTran  = facOrig.co_tran || resTran.recordset[0]?.co_tran || '01';
            const rawTax   = resTax.recordset[0]?.tax_id;
            const defTax   = rawTax ? rawTax.trim() : null;

            const auditUser = (req.profitUser || req.sqlAuth?.user || 'API').substring(0, 10).toUpperCase();
            const tsDate    = new Date();

            // Resolver tasa
            const currentTasa = resTasa.recordset[0]?.tasa_v || 1;
            let tasaDoc = Number(data.tasa || facOrig.tasa || currentTasa);
            if (tasaDoc <= 1 && currentTasa > 1) {
                tasaDoc = currentTasa;
            }

            // Calcular montos en Bs y USD
            let totalBrutoBs = 0;
            let totalImpBs   = 0;

            data.renglones.forEach(item => {
                const qty = Number(item.cantidad || 0);
                const prcUSD = Number(item.precio || 0);
                const pImp = Number(item.porc_imp || 0);

                const prcBs = prcUSD * tasaDoc;
                const subBs = Math.round((qty * prcBs) * 100) / 100;
                const impBs = Math.round(((subBs * pImp) / 100) * 100) / 100;

                totalBrutoBs += subBs;
                totalImpBs   += impBs;
            });

            const totalNetoBs = Math.round((totalBrutoBs + totalImpBs) * 100) / 100;

            // Determinar sucursal
            const branchCodes = srv.profit_branch_codes || [];
            const defaultCodeObj = branchCodes.find(b => b.is_default === true) || branchCodes[0] || { code: defSucu };
            const sucuCode = data.force_sucu || defSucu || defaultCodeObj.code;

            const transaction = new sql.Transaction(pool);
            await transaction.begin();

            try {
                // Generar correlativo de Devolución Cliente (V005)
                const corrDev = await getProximoConsecutivo({
                    runner: transaction,
                    co_tipo_serie: 'DEVOLUCION_CLIENTE',
                    co_sucur: sucuCode
                });
                const docNumDev = corrDev.docNum;

                // Generar correlativo de Nota de Crédito (V020)
                const corrNCR = await getProximoConsecutivo({
                    runner: transaction,
                    co_tipo_serie: 'NOTA_CREDITO_VENTA',
                    co_sucur: sucuCode
                });
                const docNumNCR = corrNCR.docNum;

                console.log(`✨ [DEVOLUCION] Consecutivos generados: Dev=${docNumDev}, N/CR=${docNumNCR}`);

                // Insertar Cabecera de saDevolucionCliente
                const rH = new sql.Request(transaction);
                rH.input('sDoc_Num',          sql.Char(20),         padProfit(docNumDev, 20));
                rH.input('sDescrip',          sql.VarChar(60),      (data.descrip || `DEV. FAC ${facturaNum}`).substring(0, 60));
                rH.input('sCo_Cli',           sql.Char(16),         padProfit(data.co_cli, 16));
                rH.input('sCo_Tran',          sql.Char(6),          padProfit(data.co_tran || defTran, 6));
                rH.input('sCo_Mone',          sql.Char(6),          padProfit(usdCode, 6));
                rH.input('sCo_Cta_Ingr_Egr',  sql.Char(20),         null);
                rH.input('sCo_Ven',           sql.Char(6),          padProfit(data.co_ven || defVen, 6));
                rH.input('sCo_Cond',          sql.Char(6),          padProfit(data.co_cond || defCond, 6));
                rH.input('sdFec_Emis',        sql.SmallDateTime,    tsDate);
                rH.input('sdFec_Venc',        sql.SmallDateTime,    tsDate);
                rH.input('sdFec_Reg',         sql.SmallDateTime,    tsDate);
                rH.input('bAnulado',          sql.Bit,              0);
                rH.input('sStatus',           sql.Char(1),          '0');
                rH.input('deTasa',            sql.Decimal(21, 8),   tasaDoc);
                rH.input('sN_Control',        sql.Char(20),         padProfit(docNumNCR, 20));
                rH.input('sNro_Doc',          sql.Char(20),         padProfit(docNumNCR, 20));
                rH.input('sPorc_Desc_Glob',   sql.Char(15),         null);
                rH.input('deMonto_Desc_Glob', sql.Decimal(18, 2),   0);
                rH.input('sPorc_Reca',        sql.Char(15),         null);
                rH.input('deMonto_Reca',      sql.Decimal(18, 2),   0);
                rH.input('deSaldo',           sql.Decimal(18, 2),   totalNetoBs);
                rH.input('deTotal_Bruto',     sql.Decimal(18, 2),   totalBrutoBs);
                rH.input('deMonto_Imp',       sql.Decimal(18, 2),   totalImpBs);
                rH.input('deMonto_Imp2',      sql.Decimal(18, 2),   0);
                rH.input('deMonto_Imp3',      sql.Decimal(18, 2),   0);
                rH.input('deOtros1',          sql.Decimal(18, 2),   0);
                rH.input('deOtros2',          sql.Decimal(18, 2),   0);
                rH.input('deOtros3',          sql.Decimal(18, 2),   0);
                rH.input('deTotal_Neto',      sql.Decimal(18, 2),   totalNetoBs);
                rH.input('sDis_Cen',          sql.VarChar(sql.MAX), null);
                rH.input('sComentario',       sql.VarChar(sql.MAX), (data.comentario || `Devolución de factura ${facturaNum}`).substring(0, 500));
                rH.input('sDir_Ent',          sql.VarChar(sql.MAX), null);
                rH.input('bContrib',          sql.Bit,              data.contrib ?? 0);
                rH.input('bImpresa',          sql.Bit,              0);
                rH.input('sSalestax',         sql.Char(8),          defTax);
                rH.input('sImpfis',           sql.VarChar(20),      null);
                rH.input('sImpfisfac',        sql.VarChar(15),      null);
                rH.input('bVen_Ter',          sql.Bit,              0);
                rH.input('sCampo1',           sql.VarChar(60),      null);
                rH.input('sCampo2',           sql.VarChar(60),      null);
                rH.input('sCampo3',           sql.VarChar(60),      null);
                rH.input('sCampo4',           sql.VarChar(60),      null);
                rH.input('sCampo5',           sql.VarChar(60),      null);
                rH.input('sCampo6',           sql.VarChar(60),      null);
                rH.input('sCampo7',           sql.VarChar(60),      null);
                rH.input('sCampo8',           sql.VarChar(60),      null);
                rH.input('sCo_Us_In',         sql.Char(6),          padProfit(auditUser, 6));
                rH.input('sCo_Sucu_In',       sql.Char(6),          padProfit(sucuCode, 6));
                rH.input('sRevisado',         sql.Char(1),          null);
                rH.input('sTrasnfe',          sql.Char(1),          null);
                rH.input('sMaquina',          sql.VarChar(60),      'SYNC2K');

                await rH.execute('pInsertarDevolucionCliente');

                // Actualizar co_tipo_doc = 'N/CR' en saDevolucionCliente
                await transaction.request()
                    .input('doc_num', sql.Char(20), padProfit(docNumDev, 20))
                    .query(`UPDATE saDevolucionCliente SET co_tipo_doc = 'N/CR' WHERE doc_num = @doc_num`);

                // Insertar Renglones de la Devolución
                for (let i = 0; i < data.renglones.length; i++) {
                    const item = data.renglones[i];
                    const qty = Number(item.cantidad || 0);
                    const prcUSD = Number(item.precio || 0);
                    const pImp = Number(item.porc_imp || 0);
                    const coPrecio = String(item.co_precio || '01').trim().substring(0, 6);

                    const prcBs = prcUSD * tasaDoc;
                    const subBs = Math.round((qty * prcBs) * 100) / 100;
                    const impBs = Math.round(((subBs * pImp) / 100) * 100) / 100;

                    const finalUni = String(item.co_uni || '01').trim();
                    const rowguidDocVal = (item.rowguid_doc && String(item.rowguid_doc).trim()) ? String(item.rowguid_doc).trim() : null;

                    const rL = new sql.Request(transaction);
                    rL.input('iReng_Num',          sql.Int,              i + 1);
                    rL.input('sDoc_Num',           sql.Char(20),         padProfit(docNumDev, 20));
                    rL.input('sCo_Art',            sql.Char(30),         padProfit(item.co_art, 30));
                    rL.input('sDes_Art',           sql.VarChar(120),     (item.art_des || '').substring(0, 120));
                    rL.input('sCo_Uni',            sql.Char(6),          padProfit(finalUni, 6));
                    rL.input('sSco_Uni',           sql.Char(6),          null);
                    rL.input('sCo_Alma',           sql.Char(6),          padProfit(item.co_alma || defAlma, 6));
                    rL.input('sCo_Precio',         sql.Char(6),          padProfit(coPrecio || '01', 6));
                    rL.input('sTipo_Imp',          sql.Char(1),          item.tipo_imp || '1');
                    rL.input('sTipo_Imp2',         sql.Char(1),          null);
                    rL.input('sTipo_Imp3',         sql.Char(1),          null);
                    rL.input('deTotal_Art',        sql.Decimal(18, 5),   qty);
                    rL.input('deSTotal_Art',       sql.Decimal(18, 5),   0);
                    rL.input('dePrec_Vta',         sql.Decimal(18, 5),   prcBs);
                    rL.input('sPorc_Desc',         sql.VarChar(15),      null);
                    rL.input('deMonto_Desc',       sql.Decimal(18, 5),   0);
                    rL.input('deReng_Neto',        sql.Decimal(18, 2),   subBs);
                    rL.input('dePendiente',        sql.Decimal(18, 5),   qty);
                    rL.input('dePendiente2',       sql.Decimal(18, 5),   0);
                    rL.input('deMonto_Desc_Glob',  sql.Decimal(18, 5),   0);
                    rL.input('deMonto_reca_Glob',  sql.Decimal(18, 5),   0);
                    rL.input('deOtros1_glob',      sql.Decimal(18, 5),   0);
                    rL.input('deOtros2_glob',      sql.Decimal(18, 5),   0);
                    rL.input('deOtros3_glob',      sql.Decimal(18, 5),   0);
                    rL.input('deMonto_imp_afec_glob',  sql.Decimal(18, 5), 0);
                    rL.input('deMonto_imp2_afec_glob', sql.Decimal(18, 5), 0);
                    rL.input('deMonto_imp3_afec_glob', sql.Decimal(18, 5), 0);
                    rL.input('sTipo_Doc',          sql.Char(4),          'FACT');
                    rL.input('gRowguid_Doc',       sql.UniqueIdentifier, rowguidDocVal || null);
                    rL.input('sNum_Doc',           sql.VarChar(20),      padProfit(facturaNum, 20));
                    rL.input('dePorc_Imp',         sql.Decimal(18, 5),   pImp);
                    rL.input('dePorc_Imp2',        sql.Decimal(18, 5),   0);
                    rL.input('dePorc_Imp3',        sql.Decimal(18, 5),   0);
                    rL.input('deMonto_Imp',        sql.Decimal(18, 5),   impBs);
                    rL.input('deMonto_Imp2',       sql.Decimal(18, 5),   0);
                    rL.input('deMonto_Imp3',       sql.Decimal(18, 5),   0);
                    rL.input('deOtros',            sql.Decimal(18, 5),   0);
                    rL.input('deTotal_Dev',        sql.Decimal(18, 5),   0);
                    rL.input('deMonto_Dev',        sql.Decimal(18, 5),   0);
                    rL.input('sComentario',        sql.VarChar(sql.MAX),  '');
                    rL.input('sDis_Cen',           sql.VarChar(sql.MAX),  null);
                    rL.input('sCo_Sucu_In',        sql.Char(6),           padProfit(sucuCode, 6));
                    rL.input('sCo_Us_In',          sql.Char(6),           padProfit(auditUser, 6));
                    rL.input('sREVISADO',          sql.Char(1),           null);
                    rL.input('sTRASNFE',           sql.Char(1),           null);
                    rL.input('sMaquina',           sql.VarChar(60),      'SYNC2K');

                    await rL.execute('pInsertarRenglonesDevolucionCliente');

                    // Asignar precio en moneda original prec_vta_om
                    await transaction.request()
                        .input('om', sql.Decimal(18, 5), prcUSD)
                        .input('doc', sql.Char(20), padProfit(docNumDev, 20))
                        .input('reng', sql.Int, i + 1)
                        .query(`UPDATE saDevolucionClienteReng SET prec_vta_om = @om WHERE doc_num = @doc AND reng_num = @reng`);

                    // 3. Devolver mercancía al stock (Sumar stock tipo 'ACT')
                    const rStock = new sql.Request(transaction);
                    rStock.input('sCo_Alma',              sql.Char(6),  padProfit(item.co_alma || defAlma, 6));
                    rStock.input('sCo_Art',               sql.Char(30), padProfit(item.co_art, 30));
                    rStock.input('sCo_Uni',               sql.Char(6),  padProfit(finalUni, 6));
                    rStock.input('deCantidad',            sql.Decimal(18, 5), qty);
                    rStock.input('sTipoStock',            sql.Char(4),  'ACT');
                    rStock.input('bSumarStock',           sql.Bit,      1); // Sumar stock (entra de vuelta)
                    rStock.input('bPermiteStockNegativo', sql.Bit,      1);
                    await rStock.execute('pStockActualizar');

                    // 4. Actualizar total_dev en la factura original (saFacturaVentaReng)
                    if (rowguidDocVal) {
                        await transaction.request()
                            .input('qty', sql.Decimal(18, 5), qty)
                            .input('rowguid_doc', sql.UniqueIdentifier, rowguidDocVal)
                            .input('auditUser', sql.Char(6), padProfit(auditUser, 6))
                            .query(`
                                UPDATE saFacturaVentaReng
                                SET total_dev = ISNULL(total_dev, 0) + @qty,
                                    fe_us_mo = GETDATE(),
                                    co_us_mo = @auditUser
                                WHERE rowguid = @rowguid_doc;
                            `);
                    }
                }

                // 5. Insertar Nota de Crédito en saDocumentoVenta
                const rDoc = new sql.Request(transaction);
                rDoc.input('sCo_Tipo_Doc', sql.Char(6), 'N/CR');
                rDoc.input('sNro_Doc', sql.Char(20), padProfit(docNumNCR, 20));
                rDoc.input('sCo_Cli', sql.Char(16), padProfit(data.co_cli, 16));
                rDoc.input('sCo_Ven', sql.Char(6), padProfit(data.co_ven || defVen, 6));
                rDoc.input('sCo_Mone', sql.Char(6), padProfit(usdCode, 6));
                rDoc.input('sMov_Ban', sql.Char(20), null);
                rDoc.input('sCo_Cta_Ingr_Egr', sql.Char(20), null);
                rDoc.input('deTasa', sql.Decimal(21, 8), tasaDoc);
                rDoc.input('sObserva', sql.VarChar(sql.MAX), (data.descrip || `DEV. FAC ${facturaNum}`).substring(0, 60));
                rDoc.input('sdFec_Reg', sql.SmallDateTime, tsDate);
                rDoc.input('sdFec_Emis', sql.SmallDateTime, tsDate);
                rDoc.input('sdFec_Venc', sql.SmallDateTime, tsDate);
                rDoc.input('bAnulado', sql.Bit, 0);
                rDoc.input('bAut', sql.Bit, 1);
                rDoc.input('bContrib', sql.Bit, data.contrib ?? 0);
                rDoc.input('sDoc_Orig', sql.Char(6), 'DEVO');
                rDoc.input('sNro_Orig', sql.VarChar(20), padProfit(docNumDev, 20));
                rDoc.input('sNro_Che', sql.VarChar(20), null);
                rDoc.input('deMonto_Imp', sql.Decimal(18, 2), totalImpBs);
                rDoc.input('deSaldo', sql.Decimal(18, 2), totalNetoBs);
                rDoc.input('deTotal_Bruto', sql.Decimal(18, 2), totalBrutoBs);
                rDoc.input('deMonto_Desc_Glob', sql.Decimal(18, 2), 0);
                rDoc.input('sPorc_Desc_Glob', sql.VarChar(15), null);
                rDoc.input('sPorc_Reca', sql.VarChar(15), null);
                rDoc.input('deMonto_Reca', sql.Decimal(18, 2), 0);
                rDoc.input('deTotal_Neto', sql.Decimal(18, 2), totalNetoBs);
                rDoc.input('deMonto_Imp2', sql.Decimal(18, 2), 0);
                rDoc.input('deMonto_Imp3', sql.Decimal(18, 2), 0);
                
                const tipoImp = data.renglones[0]?.tipo_imp || '1';
                rDoc.input('sTipo_Imp', sql.Char(1), tipoImp);
                rDoc.input('iTipo_Origen', sql.Int, null);
                
                const porcImp = data.renglones[0]?.porc_imp || 0;
                rDoc.input('dePorc_Imp', sql.Decimal(18, 5), porcImp);
                rDoc.input('dePorc_Imp2', sql.Decimal(18, 5), 0);
                rDoc.input('dePorc_Imp3', sql.Decimal(18, 5), 0);
                rDoc.input('sNum_Comprobante', sql.Char(14), null);
                rDoc.input('sN_Control', sql.VarChar(20), docNumNCR);
                rDoc.input('sDis_Cen', sql.VarChar(sql.MAX), null);
                rDoc.input('deComis1', sql.Decimal(18, 2), 0);
                rDoc.input('deComis2', sql.Decimal(18, 2), 0);
                rDoc.input('deComis3', sql.Decimal(18, 2), 0);
                rDoc.input('deComis4', sql.Decimal(18, 2), 0);
                rDoc.input('deComis5', sql.Decimal(18, 2), 0);
                rDoc.input('deComis6', sql.Decimal(18, 2), 0);
                rDoc.input('deAdicional', sql.Decimal(18, 2), 0);
                rDoc.input('sSalestax', sql.Char(8), defTax);
                rDoc.input('bVen_Ter', sql.Bit, 0);
                rDoc.input('sImpfis', sql.VarChar(20), null);
                rDoc.input('sImpfisfac', sql.VarChar(15), null);
                rDoc.input('sImp_nro_z', sql.Char(15), null);
                rDoc.input('deOtros1', sql.Decimal(18, 2), 0);
                rDoc.input('deOtros2', sql.Decimal(18, 2), 0);
                rDoc.input('deOtros3', sql.Decimal(18, 2), 0);
                rDoc.input('sCampo1', sql.VarChar(60), null);
                rDoc.input('sCampo2', sql.VarChar(60), null);
                rDoc.input('sCampo3', sql.VarChar(60), null);
                rDoc.input('sCampo4', sql.VarChar(60), null);
                rDoc.input('sCampo5', sql.VarChar(60), null);
                rDoc.input('sCampo6', sql.VarChar(60), null);
                rDoc.input('sCampo7', sql.VarChar(60), null);
                rDoc.input('sCampo8', sql.VarChar(60), null);
                rDoc.input('sRevisado', sql.Char(1), null);
                rDoc.input('sTrasnfe', sql.Char(1), null);
                rDoc.input('sCo_Sucu_In', sql.Char(6), padProfit(sucuCode, 6));
                rDoc.input('sCo_Us_In', sql.Char(6), padProfit(auditUser, 6));
                rDoc.input('sMaquina', sql.VarChar(60), 'SYNC2K');

                await rDoc.execute('pInsertarDocumentoVenta');

                await transaction.commit();

                return {
                    doc_num: docNumDev,
                    nro_doc: docNumNCR,
                    factura_origen: facturaNum,
                    total_neto_bs: totalNetoBs,
                    total_neto_usd: Math.round((totalNetoBs / tasaDoc) * 100) / 100,
                    tasa: tasaDoc
                };
            } catch (errTx) {
                await transaction.rollback();
                throw errTx;
            }
        });

        res.status(200).json({
            success: true,
            message: `Devolución ${outcome.doc_num} procesada con éxito (Nota de Crédito: ${outcome.nro_doc}).`,
            data: outcome
        });

    } catch (error) {
        console.error('[CREAR DEVOLUCION ERROR]:', error);
        res.status(500).json({ success: false, message: 'Error al registrar Devolución.', error: error.message });
    }
});

module.exports = router;
