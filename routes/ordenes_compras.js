const express = require('express');
const router = express.Router();
const { sql, getPool, getServers, getExchangeRate } = require('../db');
const { executeWrite, writeResponse, paginatedResponse, padProfit } = require('../helpers/multiSede');

console.log("🛒 [AGENT] Iniciando Módulo de Órdenes de Compra (saOrdenCompra)");

/**
 * @swagger
 * tags:
 *   name: OrdenesCompras
 *   description: Gestión de órdenes de compra
 */

// --- OBTENER LISTADO ---
router.get('/', async (req, res) => {
    try {
        const page  = parseInt(req.query.page)  || 1;
        const limit = parseInt(req.query.limit) || 12;
        const { sede, doc_num, co_prov, fec_d, fec_h, search, status, only_available, only_pending } = req.query;
        
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
                if (co_prov) {
                    request.input('co_prov_search', sql.VarChar, `%${co_prov}%`);
                    whereClauses.push("(c.co_prov LIKE @co_prov_search OR p.prov_des LIKE @co_prov_search OR c.doc_num LIKE @co_prov_search OR p.rif LIKE @co_prov_search)");
                }
                if (search) {
                    request.input('search_all', sql.VarChar, `%${search}%`);
                    whereClauses.push("(c.doc_num LIKE @search_all OR c.co_prov LIKE @search_all OR p.prov_des LIKE @search_all OR p.rif LIKE @search_all OR c.descrip LIKE @search_all)");
                }
                if (fec_d) {
                    request.input('fec_d', sql.SmallDateTime, fec_d);
                    whereClauses.push("c.fec_emis >= @fec_d");
                }
                if (fec_h) {
                    request.input('fec_h', sql.SmallDateTime, fec_h);
                    whereClauses.push("c.fec_emis <= @fec_h");
                }
                if (status) {
                    const statusVals = status.split(',').map(s => s.trim());
                    const statusClauses = [];
                    statusVals.forEach((val, idx) => {
                        const paramName = `status_val_${idx}`;
                        request.input(paramName, sql.Char(1), val);
                        statusClauses.push(`c.status = @${paramName}`);
                    });
                    if (statusClauses.length > 0) {
                        whereClauses.push(`(${statusClauses.join(' OR ')})`);
                    }
                }
                if (only_available === 'true' || only_pending === 'true') {
                    whereClauses.push("c.anulado = 0 AND EXISTS (SELECT 1 FROM saOrdenCompraReng r WHERE r.doc_num = c.doc_num AND r.pendiente > 0)");
                }

                const whereSQL = whereClauses.join(" AND ");
                
                const result = await request.query(`
                    SELECT RTRIM(c.doc_num) AS doc_num, RTRIM(c.descrip) AS descrip,
                           RTRIM(c.co_prov) AS co_prov, RTRIM(p.prov_des) AS prov_des, RTRIM(p.rif) AS rif,
                           c.fec_emis, c.fec_venc, c.fec_reg, c.fe_us_in AS fec_us_in, c.fe_us_mo AS fec_us_mo, 
                           RTRIM(c.co_us_in) AS co_us_in, RTRIM(c.co_us_mo) AS co_us_mo,
                           c.anulado,
                           RTRIM(c.co_mone) AS co_mone, 
                           CASE 
                               WHEN c.tasa > 1 THEN c.tasa
                               ELSE ISNULL(
                                   (SELECT TOP 1 t.tasa_v FROM saTasa t WHERE LTRIM(RTRIM(t.co_mone)) IN ('USD', 'US$', 'DOL', '$', 'US') AND CONVERT(VARCHAR(10), t.fecha, 120) <= CONVERT(VARCHAR(10), c.fec_emis, 120) ORDER BY t.fecha DESC),
                                   ISNULL(
                                       (SELECT TOP 1 t.tasa_v FROM saTasa t WHERE LTRIM(RTRIM(t.co_mone)) IN ('USD', 'US$', 'DOL', '$', 'US') ORDER BY t.fecha DESC),
                                       1.0
                                   )
                               )
                           END AS tasa,
                           c.total_neto, c.total_bruto, c.monto_imp,
                           RTRIM(c.co_cond) AS co_cond, RTRIM(cd.cond_des) AS cond_des,
                           CASE 
                               WHEN c.anulado = 1 THEN '3'
                               WHEN NOT EXISTS (SELECT 1 FROM saOrdenCompraReng r WHERE r.doc_num = c.doc_num) THEN '0'
                               WHEN (SELECT SUM(r.pendiente) FROM saOrdenCompraReng r WHERE r.doc_num = c.doc_num) = 0 THEN '2'
                               WHEN (SELECT SUM(r.pendiente) FROM saOrdenCompraReng r WHERE r.doc_num = c.doc_num) = (SELECT SUM(r.total_art) FROM saOrdenCompraReng r WHERE r.doc_num = c.doc_num) THEN '0'
                               ELSE '1'
                           END AS status
                    FROM saOrdenCompra c
                    LEFT JOIN saProveedor p ON c.co_prov = p.co_prov
                    LEFT JOIN saCondicionPago cd ON c.co_cond = cd.co_cond
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
        res.status(500).json({ success: false, message: 'Error al consultar órdenes de compra.', error: error.message });
    }
});

// --- OBTENER DETALLE ---
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
                        SELECT RTRIM(c.doc_num) AS doc_num, RTRIM(c.descrip) AS descrip,
                               RTRIM(c.co_prov) AS co_prov, RTRIM(p.prov_des) AS prov_des,
                               RTRIM(c.co_cond) AS co_cond, 
                               ISNULL(NULLIF(RTRIM(cd.cond_des), ''), ISNULL(NULLIF(RTRIM(cdp.cond_des), ''), RTRIM(c.co_cond))) AS cond_des,
                               RTRIM(p.cond_pag) AS prov_cond_pag,
                               RTRIM(cdp.cond_des) AS prov_cond_des,
                               c.fec_emis, c.fec_venc, c.fec_reg, c.fe_us_in AS fec_us_in, c.fe_us_mo AS fec_us_mo, 
                               c.anulado,
                               RTRIM(c.co_mone) AS co_mone, 
                               CASE 
                                   WHEN c.tasa > 1 THEN c.tasa
                                   ELSE ISNULL(
                                       (SELECT TOP 1 t.tasa_v FROM saTasa t WHERE LTRIM(RTRIM(t.co_mone)) IN ('USD', 'US$', 'DOL', '$', 'US') AND CONVERT(VARCHAR(10), t.fecha, 120) <= CONVERT(VARCHAR(10), c.fec_emis, 120) ORDER BY t.fecha DESC),
                                       ISNULL(
                                           (SELECT TOP 1 t.tasa_v FROM saTasa t WHERE LTRIM(RTRIM(t.co_mone)) IN ('USD', 'US$', 'DOL', '$', 'US') ORDER BY t.fecha DESC),
                                           1.0
                                       )
                                   )
                               END AS tasa,
                               c.total_bruto, c.monto_imp, c.total_neto,
                                RTRIM(c.comentario) AS comentario,
                                RTRIM(c.campo8) AS campo8,
                                RTRIM(c.dir_ent) AS dir_ent,
                                RTRIM(c.co_us_in) AS co_us_in,
                                RTRIM(c.co_us_mo) AS co_us_mo,
                               RTRIM(p.rif) AS rif, RTRIM(p.direc1) AS direc1, 
                               RTRIM(p.telefonos) AS telefonos, RTRIM(p.email) AS email,
                               RTRIM(p.co_zon) AS co_zon, RTRIM(z.zon_des) AS zon_des, 
                               p.contribu_e, p.porc_esp,
                               CASE 
                                   WHEN c.anulado = 1 THEN '3'
                                   WHEN NOT EXISTS (SELECT 1 FROM saOrdenCompraReng r WHERE r.doc_num = c.doc_num) THEN '0'
                                   WHEN (SELECT SUM(r.pendiente) FROM saOrdenCompraReng r WHERE r.doc_num = c.doc_num) = 0 THEN '2'
                                   WHEN (SELECT SUM(r.pendiente) FROM saOrdenCompraReng r WHERE r.doc_num = c.doc_num) = (SELECT SUM(r.total_art) FROM saOrdenCompraReng r WHERE r.doc_num = c.doc_num) THEN '0'
                                   ELSE '1'
                               END AS status
                        FROM saOrdenCompra c
                        LEFT JOIN saProveedor    p   ON c.co_prov = p.co_prov
                        LEFT JOIN saCondicionPago cd  ON c.co_cond = cd.co_cond
                        LEFT JOIN saCondicionPago cdp ON p.cond_pag = cdp.co_cond
                        LEFT JOIN saZona         z   ON p.co_zon  = z.co_zon
                        WHERE LTRIM(RTRIM(c.doc_num)) = LTRIM(RTRIM(@doc_num))
                    `),
                    pool.request().input('doc_num', sql.VarChar, doc_num).query(`
                        SELECT r.reng_num, RTRIM(r.co_art) AS co_art, 
                               ISNULL(NULLIF(RTRIM(r.des_art), ''), RTRIM(a.art_des)) AS art_des,
                               RTRIM(a.co_lin) AS co_lin, RTRIM(a.co_subl) AS co_subl,
                               RTRIM(a.modelo) AS modelo,
                               r.total_art AS cantidad, r.pendiente, r.rowguid AS rowguid_doc,
                               RTRIM(r.co_alma) AS co_alma,
                               r.cost_unit AS precio, r.cost_unit AS cost_unit,
                               RTRIM(r.tipo_imp) AS tipo_imp, r.porc_imp, r.reng_neto AS total_renglon,
                               r.cost_unit_om, RTRIM(r.co_uni) AS co_uni, RTRIM(u.des_uni) AS unidad,
                               RTRIM(r.comentario) AS comentario_reng
                        FROM saOrdenCompraReng r
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
            return res.status(404).json({ success: false, message: 'Orden de compra no encontrada.' });

        res.status(200).json({ success: true, count: found.length, data: results.filter(r => r !== null) });

    } catch (error) {
        res.status(500).json({ success: false, message: 'Error al consultar orden de compra.', error: error.message });
    }
});

// --- GUARDAR O ACTUALIZAR (POST UNIFICADO) ---
router.post('/', async (req, res) => {
    const data = req.body;
    console.log("📥 [AGENT] Recibiendo Orden de Compra (UNIFIED POST):", JSON.stringify({ ...data, renglones: data.renglones?.length }, null, 2));

    if (!data.co_prov || !data.renglones || !Array.isArray(data.renglones) || data.renglones.length === 0) {
        return res.status(400).json({ success: false, message: 'Campos obligatorios: co_prov, renglones' });
    }

    const outcome = await executeWrite(req.query.sede || null, req.sqlAuth, async (pool, srv) => {
        // 1. Cargar Catálogos y Parámetros Globales
        const [resMoneda, resUSD, resAlma, resCond, resSucu, resProv, resTax, resTasa, resCtaIE, resTran] = await Promise.all([
            pool.request().query(`SELECT TOP 1 RTRIM(g_moneda) AS g_moneda FROM par_emp`),
            pool.request().query(`SELECT TOP 1 RTRIM(co_mone)  AS co_mone   FROM saMoneda WHERE LTRIM(RTRIM(co_mone)) IN ('US$','USD','DOL','$','US') OR mone_des LIKE '%Dolar%'`),
            pool.request().query(`SELECT TOP 1 RTRIM(co_alma) AS co_alma FROM saAlmacen`),
            pool.request().query(`SELECT TOP 1 RTRIM(co_cond) AS co_cond FROM saCondicionPago`),
            pool.request().query(`SELECT TOP 1 RTRIM(co_sucur) AS co_sucur FROM saSucursal ORDER BY CASE WHEN RTRIM(co_sucur) = '01' THEN 0 ELSE 1 END, co_sucur`),
            pool.request().input('co_prov', sql.Char(16), data.co_prov).query(`SELECT RTRIM(co_mone) as co_mone, RTRIM(cond_pag) as cond_pag, RTRIM(co_sucu_in) as co_sucu FROM saProveedor WHERE co_prov = @co_prov`),
            pool.request().query(`SELECT TOP 1 RTRIM(tax_id) AS tax_id FROM saTax`),
            pool.request().query(`SELECT TOP 1 tasa_v FROM saTasa WHERE LTRIM(RTRIM(co_mone)) IN ('US$','USD','DOL','$','US') ORDER BY fecha DESC`),
            pool.request().query(`SELECT TOP 1 RTRIM(co_cta_ingr_egr) AS co_cta_ingr_egr FROM saCuentaIngEgr WHERE RTRIM(co_cta_ingr_egr) = '02'`),
            pool.request().query(`SELECT TOP 1 RTRIM(co_tran) AS co_tran FROM saTransporte`)
        ]);

        const prov = resProv.recordset[0] || {};
        const usdCode = resUSD.recordset[0]?.co_mone || 'US$';
        const bsCode   = resMoneda.recordset[0]?.g_moneda || 'BS';
        const defCond  = prov.cond_pag || resCond.recordset[0]?.co_cond || '01';

        // Resolución dinámica de sucursal por sede
        const configuredSucu = (srv?.profit_branch_codes || []).find(b => b.is_default)?.code 
            || (srv?.profit_branch_codes || [])[0]?.code 
            || (srv?.profit_branch_codes || [])[0];
        const defSucu  = configuredSucu || resSucu.recordset[0]?.co_sucur || '01';

        // Resolución dinámica del almacén por defecto según la sede/sucursal
        const resAlmaSucu = await pool.request()
            .input('sucuCode', sql.Char(6), padProfit(defSucu, 6))
            .query(`
                SELECT TOP 1 RTRIM(co_alma) AS co_alma 
                FROM saAlmacen 
                ORDER BY CASE 
                    WHEN LTRIM(RTRIM(co_sucur)) = LTRIM(RTRIM(@sucuCode)) THEN 0 
                    WHEN LTRIM(RTRIM(co_alma)) = LTRIM(RTRIM(@sucuCode)) THEN 1
                    WHEN LTRIM(RTRIM(co_alma)) = '01' THEN 2 
                    ELSE 3 
                END, co_alma
            `);
        const defAlma  = resAlmaSucu.recordset[0]?.co_alma || resAlma.recordset[0]?.co_alma || '01';

        const defCtaIE = resCtaIE.recordset[0]?.co_cta_ingr_egr || '02';
        const defTran  = resTran.recordset[0]?.co_tran || '01';
        const rawTax   = resTax.recordset[0]?.tax_id;
        const defTax   = rawTax ? rawTax.trim() : null;
        
        const auditUser = (req.profitUser || req.sqlAuth?.user || 'API').substring(0, 6).toUpperCase();
        const tsDate    = new Date();
        const fVenc     = new Date(tsDate);
        fVenc.setDate(fVenc.getDate() + 7); 

        const transaction = new sql.Transaction(pool);
        await transaction.begin();

        try {
            let docNum = data.doc_num;
            const isUpdate = !!docNum;
            let existingHeader = null;

            if (isUpdate) {
                // --- MODO SOBREESCRITURA (UPDATE) ---
                console.log(`🔄 [AGENT] Resguardando datos de orden de compra existente: ${docNum}`);
                const resPre = await transaction.request().input('doc_num', sql.VarChar, docNum).query(
                    `SELECT TOP 1 * FROM saOrdenCompra WHERE LTRIM(RTRIM(doc_num)) = LTRIM(RTRIM(@doc_num))`
                );
                
                if (resPre.recordset.length === 0) {
                    throw new Error(`La orden de compra ${docNum} no existe para ser editada.`);
                }
                
                existingHeader = resPre.recordset[0];
                const currentStatus = String(existingHeader.status || '').trim();
                const isAnulada = !!existingHeader.anulado;
                if (isAnulada || currentStatus !== '0') {
                    throw new Error(`La orden de compra ${docNum} no está sin procesar. No se permite editar (status=${currentStatus || 'N/A'}${isAnulada ? ', anulada=1' : ''}).`);
                }
                const { validador, rowguid } = existingHeader;

                // Resguardar renglones
                const resL = await transaction.request().input('doc_num', sql.VarChar, docNum).query(
                    `SELECT reng_num, rowguid FROM saOrdenCompraReng WHERE LTRIM(RTRIM(doc_num)) = LTRIM(RTRIM(@doc_num))`
                );

                console.log(`🗑️ [AGENT] Eliminando versión anterior para re-inserción...`);
                for (const line of resL.recordset) {
                    const rE = new sql.Request(transaction);
                    rE.input('sDoc_NumOri',  sql.Char(20),          docNum);
                    rE.input('iReng_NumOri', sql.Int,               line.reng_num);
                    rE.input('sCo_Us_Mo',    sql.Char(6),           auditUser);
                    rE.input('sCo_Sucu_Mo',  sql.Char(6),           defSucu);
                    rE.input('sMaquina',     sql.VarChar(60),        'SYNC2K');
                    rE.input('gRowguid',     sql.UniqueIdentifier,  line.rowguid);
                    await rE.execute('pEliminarRenglonesOrdenCompra');
                }

                const rHE = new sql.Request(transaction);
                rHE.input('sDoc_NumOri', sql.Char(20),          docNum);
                rHE.input('tsValidador', sql.VarBinary,         validador);
                rHE.input('sMaquina',    sql.VarChar(60),        'SYNC2K');
                rHE.input('sCo_Us_Mo',   sql.Char(6),           auditUser);
                rHE.input('sCo_Sucu_Mo', sql.Char(6),           defSucu);
                rHE.input('gRowguid',    sql.UniqueIdentifier,  rowguid);
                await rHE.execute('pEliminarOrdenCompra');
                
            } else {
                // --- MODO NUEVO ---
                let corrRow = null;

                // 1) Ruta estándar de Profit para consecutivo de orden de compra
                const resCorr = await transaction.request().query(`
                    UPDATE saSerie
                    SET prox_n = prox_n + 1, fe_us_mo = GETDATE()
                    OUTPUT INSERTED.prox_n, RTRIM(INSERTED.desde_a) as prefijo
                    WHERE co_serie = (
                        SELECT TOP 1 co_serie
                        FROM saConsecutivo
                        WHERE UPPER(LTRIM(RTRIM(co_consecutivo))) IN ('OCOM_NUM', 'ORDC_NUM', 'O_CO_NUM', 'ORD_NUM')
                           OR UPPER(LTRIM(RTRIM(co_consecutivo))) LIKE '%OCOM%'
                           OR UPPER(LTRIM(RTRIM(co_consecutivo))) LIKE '%ORDC%'
                    )
                `);
                corrRow = resCorr.recordset[0] || null;

                // 2) Último fallback: generar desde el máximo doc_num existente
                if (!corrRow) {
                    const fallbackRes = await transaction.request().query(`
                        SELECT
                            ISNULL(MAX(CAST(RIGHT(LTRIM(RTRIM(doc_num)), 10) AS BIGINT)), 0) + 1 AS prox_n,
                            (
                                SELECT TOP 1 RTRIM(desde_a)
                                FROM saSerie
                                WHERE LTRIM(RTRIM(ISNULL(desde_a, ''))) <> ''
                            ) AS prefijo
                        FROM saOrdenCompra
                        WHERE TRY_CAST(RIGHT(LTRIM(RTRIM(doc_num)), 10) AS BIGINT) IS NOT NULL
                    `);
                    corrRow = fallbackRes.recordset[0] || null;
                }

                if (!corrRow || !corrRow.prox_n) {
                    throw new Error("No se pudo obtener el correlativo de orden de compra.");
                }

                const proxN = Number(corrRow.prox_n || 0);
                docNum = proxN.toString().padStart(10, '0');
                console.log(`✨ [AGENT] Nuevo número de orden de compra generado: ${docNum}`);
            }

            let isUSD = data.showUSD === true; 
            if (data.showUSD === undefined) {
                isUSD = String(data.co_mone || existingHeader?.co_mone || '').includes('US');
            }
            
            const currentTasa = resTasa.recordset[0]?.tasa_v || 1;
            let tasaDoc = Number(data.tasa || existingHeader?.tasa || currentTasa);
            
            if (tasaDoc <= 1 && currentTasa > 1) {
                tasaDoc = currentTasa;
            }
            
            // Recalcular Totales desde Renglones
            let totalBruto = 0;
            let totalImp   = 0;

            if (data.renglones && Array.isArray(data.renglones)) {
                data.renglones.forEach(item => {
                    const qty = Number(item.cantidad || 0);
                    const prcIn = Number(item.precio || item.cost_unit || 0);
                    const pImp = Number(item.porc_imp || 0);
                    
                    const prcBs = isUSD ? (prcIn * tasaDoc) : prcIn;
                    const sub = Math.round((qty * prcBs) * 100) / 100;
                    const imp = Math.round(((sub * pImp) / 100) * 100) / 100;
                    
                    totalBruto += sub;
                    totalImp   += imp;
                });
            }

            const totalNeto = Math.round((totalBruto + totalImp) * 100) / 100;
            const finalMone = isUSD ? usdCode : bsCode;
            
            const rH = new sql.Request(transaction);
            rH.input('sDoc_Num',          sql.Char(20),         padProfit(docNum, 20));
            rH.input('sNro_Fact',         sql.Char(20),         padProfit(existingHeader?.nro_fact || data.nro_fact || (data.n_control || ''), 20));
            rH.input('sDescrip',          sql.VarChar(60),      (data.descrip || existingHeader?.descrip || 'ORDEN DE COMPRA WEB').substring(0, 60));
            rH.input('sCo_Prov',          sql.Char(16),         padProfit(data.co_prov || existingHeader?.co_prov, 16));
            rH.input('sCo_Cta_Ingr_Egr',  sql.Char(20),         null);
            rH.input('sCo_Mone',          sql.Char(6),          padProfit(finalMone, 6));
            rH.input('sCo_Cond',          sql.Char(6),          padProfit(data.co_cond || existingHeader?.co_cond || defCond, 6));
            rH.input('sN_Control',        sql.VarChar(20),      existingHeader?.n_control || (data.n_control || ''));
            rH.input('sPorc_Desc_Glob',   sql.VarChar(15),      existingHeader?.porc_desc_glob || null);
            const finalFecEmis = isUpdate ? existingHeader.fec_emis : (data.fec_emis ? new Date(data.fec_emis) : tsDate);
            const finalFecVenc = data.fec_venc ? new Date(data.fec_venc) : (isUpdate ? existingHeader.fec_venc : fVenc);
            rH.input('sdFec_Emis',        sql.SmallDateTime,    finalFecEmis);
            rH.input('sdFec_Venc',        sql.SmallDateTime,    finalFecVenc);
            rH.input('sdFec_Reg',         sql.SmallDateTime,    isUpdate ? existingHeader.fec_reg : tsDate);
            rH.input('bAnulado',          sql.Bit,              existingHeader?.anulado || 0);
            rH.input('sStatus',           sql.Char(1),          existingHeader?.status  || '0');
            rH.input('deTasa',            sql.Decimal(21, 8),   tasaDoc);
            rH.input('sPorc_Reca',        sql.VarChar(15),      existingHeader?.porc_reca || null);
            rH.input('deSaldo',           sql.Decimal(18, 2),   totalNeto);
            rH.input('deTotal_Bruto',     sql.Decimal(18, 2),   totalBruto);
            rH.input('deTotal_Neto',      sql.Decimal(18, 2),   totalNeto);
            rH.input('deMonto_Desc_Glob', sql.Decimal(18, 2),   0);
            rH.input('deMonto_Reca',      sql.Decimal(18, 2),   0);
            rH.input('deOtros1',          sql.Decimal(18, 2),   0);
            rH.input('deOtros2',          sql.Decimal(18, 2),   0);
            rH.input('deOtros3',          sql.Decimal(18, 2),   0);
            rH.input('deMonto_Imp',       sql.Decimal(18, 2),   totalImp);
            rH.input('deMonto_Imp2',      sql.Decimal(18, 2),   0);
            rH.input('deMonto_Imp3',      sql.Decimal(18, 2),   0);
            rH.input('sDir_Ent',          sql.VarChar(sql.MAX), (data.dir_ent || existingHeader?.dir_ent || '').substring(0, 100));
            const cleanComment = String(data.comentario || existingHeader?.comentario || '')
                .replace(/\s*\|\s*EDITADO V\u00cdA API/gi, '')
                .replace(/\s*\|\s*CREADO V\u00cdA API/gi, '')
                .replace(/\s*\|\s*EDITADO VIA API/gi, '')
                .replace(/\s*\|\s*CREADO VIA API/gi, '')
                .trim();
            rH.input('sComentario',       sql.VarChar(sql.MAX), cleanComment ? cleanComment.substring(0, 500) : null);
            rH.input('bImpresa',          sql.Bit,              existingHeader?.impresa || 0);
            const rawTaxVal = (data.salestax || existingHeader?.salestax || defTax || '').trim();
            const finalTax  = rawTaxVal === '' ? null : rawTaxVal;
            rH.input('sSalestax',         sql.Char(8),          finalTax);
            rH.input('sDis_Cen',          sql.VarChar(sql.MAX), existingHeader?.dis_cen || '');
            rH.input('sCampo1',           sql.VarChar(60),      data.campo1 || existingHeader?.campo1 || null);
            rH.input('sCampo2',           sql.VarChar(60),      data.campo2 || existingHeader?.campo2 || null);
            rH.input('sCampo3',           sql.VarChar(60),      data.campo3 || existingHeader?.campo3 || null);
            rH.input('sCampo4',           sql.VarChar(60),      data.campo4 || existingHeader?.campo4 || null);
            rH.input('sCampo5',           sql.VarChar(60),      data.campo5 || existingHeader?.campo5 || null);
            rH.input('sCampo6',           sql.VarChar(60),      data.campo6 || existingHeader?.campo6 || null);
            rH.input('sCampo7',           sql.VarChar(60),      data.campo7 || existingHeader?.campo7 || null);
            rH.input('sCampo8',           sql.VarChar(60),      (isUpdate ? 'Editado vía API' : 'Creado vía API'));
            rH.input('sRevisado',         sql.Char(1),          null);
            rH.input('sTrasnfe',          sql.Char(1),          null);
            rH.input('sCo_Us_In',         sql.Char(6),          padProfit(isUpdate ? (existingHeader.co_us_in || auditUser) : auditUser, 6));
            rH.input('sCo_Sucu_In',       sql.Char(6),          padProfit(isUpdate ? (existingHeader.co_sucu_in || defSucu) : (data.co_sucu_in || defSucu), 6));
            rH.input('sMaquina',          sql.VarChar(60),      'SYNC2K');
            rH.input('bNac',              sql.Bit,              existingHeader?.nacional ?? (data.nacional ?? 1));
            await rH.execute('pInsertarOrdenCompra');

            // Pre-consultar y validar unidades de medida contra saArtUnidad para evitar FK_saOrdenCompraReng_saArtUnidad
            for (const item of (data.renglones || [])) {
                let validCoUni = null;
                if (item.co_uni && String(item.co_uni).trim() !== '') {
                    try {
                        const checkUniRes = await pool.request()
                            .input('co_art_chk', sql.Char(30), padProfit(item.co_art, 30))
                            .input('co_uni_chk', sql.Char(6), padProfit(item.co_uni, 6))
                            .query('SELECT TOP 1 RTRIM(co_uni) as co_uni FROM saArtUnidad WHERE LTRIM(RTRIM(co_art)) = LTRIM(RTRIM(@co_art_chk)) AND LTRIM(RTRIM(co_uni)) = LTRIM(RTRIM(@co_uni_chk))');
                        if (checkUniRes.recordset && checkUniRes.recordset.length > 0) {
                            validCoUni = checkUniRes.recordset[0].co_uni;
                        }
                    } catch (e) {}
                }

                if (!validCoUni) {
                    try {
                        const artUniRes = await pool.request()
                            .input('co_art_check', sql.Char(30), padProfit(item.co_art, 30))
                            .query('SELECT TOP 1 RTRIM(co_uni) as co_uni FROM saArtUnidad WHERE LTRIM(RTRIM(co_art)) = LTRIM(RTRIM(@co_art_check)) ORDER BY uni_principal DESC');
                        if (artUniRes.recordset && artUniRes.recordset.length > 0 && artUniRes.recordset[0].co_uni) {
                            validCoUni = artUniRes.recordset[0].co_uni;
                        }
                    } catch (e) {}
                }

                item._resolved_co_uni = validCoUni || (item.co_uni ? String(item.co_uni).trim() : '01');
            }

            for (let i = 0; i < data.renglones.length; i++) {
                const item = data.renglones[i];
                const qty = Number(item.cantidad || 0);
                const prcIn = Number(item.precio || item.cost_unit || 0);
                const pImp = Number(item.porc_imp || 0);

                const prcBs = isUSD ? (prcIn * tasaDoc) : prcIn;
                const prcUSD = isUSD ? prcIn : (prcIn / tasaDoc);
                const sub = Math.round((qty * prcBs) * 100) / 100;
                const imp = Math.round(((sub * pImp) / 100) * 100) / 100;

                const finalUni = item._resolved_co_uni || '01';

                const rL = new sql.Request(transaction);
                rL.input('iReng_Num',          sql.Int,              i + 1);
                rL.input('sDoc_Num',           sql.Char(20),         padProfit(docNum, 20));
                rL.input('sCo_Art',            sql.Char(30),         padProfit(item.co_art, 30));
                rL.input('sDes_Art',           sql.VarChar(120),     (item.art_des || '').substring(0, 120));
                rL.input('sCo_Uni',            sql.Char(6),          padProfit(finalUni, 6));
                rL.input('sSCo_Uni',           sql.Char(6),          padProfit(finalUni, 6));
                rL.input('sCo_Alma',           sql.Char(6),          padProfit(defAlma, 6));
                rL.input('sTipo_Imp',          sql.Char(1),          item.tipo_imp || '1');
                rL.input('sTipo_Imp2',         sql.Char(1),          null);
                rL.input('sTipo_Imp3',         sql.Char(1),          null);
                rL.input('sTipo_Doc',          sql.Char(4),          null);
                rL.input('sPorc_Desc',         sql.Char(15),         null);
                rL.input('sNum_Doc',           sql.Char(20),         null);
                rL.input('gRowGuid_Doc',       sql.UniqueIdentifier, null);
                rL.input('deReng_Neto',        sql.Decimal(18, 2),   sub);
                rL.input('deCost_Unit',        sql.Decimal(18, 5),   prcBs);
                rL.input('deCost_Unit_OM',     sql.Decimal(18, 5),   prcUSD);
                rL.input('deTotal_Art',        sql.Decimal(18, 5),   qty);
                rL.input('deSTotal_Art',       sql.Decimal(18, 5),   0);
                rL.input('deOtros',            sql.Decimal(18, 5),   0);
                rL.input('dePorc_Imp',         sql.Decimal(18, 5),   pImp);
                rL.input('dePorc_Imp2',        sql.Decimal(18, 5),   0);
                rL.input('dePorc_Imp3',        sql.Decimal(18, 5),   0);
                rL.input('deMonto_Imp',        sql.Decimal(18, 5),   imp);
                rL.input('deMonto_Imp2',       sql.Decimal(18, 5),   0);
                rL.input('deMonto_Imp3',       sql.Decimal(18, 5),   0);
                rL.input('dePorc_Gas',         sql.Decimal(18, 2),   0);
                rL.input('deTotal_Dev',        sql.Decimal(18, 5),   0);
                rL.input('deMonto_Dev',        sql.Decimal(18, 5),   0);
                rL.input('dePendiente2',       sql.Decimal(18, 5),   0);
                rL.input('sComentario',        sql.VarChar(sql.MAX), item.comentario || '');
                rL.input('bLote_Asignado',     sql.Bit,              0);
                rL.input('deMonto_Desc_Glob',  sql.Decimal(18, 5),   0);
                rL.input('deMonto_reca_Glob',  sql.Decimal(18, 5),   0);
                rL.input('deOtros1_glob',      sql.Decimal(18, 5),   0);
                rL.input('deOtros2_glob',      sql.Decimal(18, 5),   0);
                rL.input('deOtros3_glob',      sql.Decimal(18, 5),   0);
                rL.input('deMonto_imp_afec_glob',  sql.Decimal(18, 5), 0);
                rL.input('deMonto_imp2_afec_glob', sql.Decimal(18, 5), 0);
                rL.input('deMonto_imp3_afec_glob', sql.Decimal(18, 5), 0);
                rL.input('deMonto_Desc',       sql.Decimal(18, 5),   0);
                rL.input('dePendiente',        sql.Decimal(18, 5),   qty);
                rL.input('iReng_Doc',          sql.Int,              null);
                rL.input('sDis_Cen',           sql.VarChar(sql.MAX), null);
                rL.input('sCo_Sucu_In',        sql.Char(6),          padProfit(isUpdate ? (existingHeader.co_sucu_in || defSucu) : (data.co_sucu_in || defSucu), 6));
                rL.input('sCo_Us_In',          sql.Char(6),          padProfit(isUpdate ? (existingHeader.co_us_in || auditUser) : auditUser, 6));
                rL.input('sRevisado',          sql.Char(1),          null);
                rL.input('sTrasnfe',           sql.Char(1),          null);
                rL.input('sMaquina',           sql.VarChar(60),      'SYNC2K');
                rL.input('deCosto_Adi1',       sql.Decimal(18, 5),   0);
                rL.input('deCosto_Adi2',       sql.Decimal(18, 5),   0);
                rL.input('deCosto_Adi3',       sql.Decimal(18, 5),   0);
                await rL.execute('pInsertarRenglonesOrdenCompra');
            }

            await transaction.commit();
            return { doc_num: docNum, detail: isUpdate ? 'Actualizado exitosamente' : 'Creado con éxito' };
        } catch (err) {
            if (transaction._aborted === false) await transaction.rollback();
            throw err;
        }
    });

    return writeResponse(res, outcome);
});

// --- ELIMINAR ORDEN DE COMPRA ---
router.delete('/:doc_num', async (req, res) => {
    try {
        const { doc_num } = req.params;
        const { sede } = req.query;

        const outcome = await executeWrite(sede || null, req.sqlAuth, async (pool, srv) => {
            const resH = await pool.request().input('doc_num', sql.VarChar, doc_num).query(
                `SELECT validador, rowguid, RTRIM(status) AS status, anulado
                 FROM saOrdenCompra
                 WHERE LTRIM(RTRIM(doc_num)) = LTRIM(RTRIM(@doc_num))`
            );
            if (!resH.recordset.length) throw new Error('Orden de compra no existe.');

            const { validador, rowguid, status, anulado } = resH.recordset[0];
            const currentStatus = String(status || '').trim();
            const isAnulada = !!anulado;
            if (isAnulada || currentStatus !== '0') {
                throw new Error(`No se puede eliminar ${doc_num}: solo se permiten órdenes de compra sin procesar (status=${currentStatus || 'N/A'}${isAnulada ? ', anulada=1' : ''}).`);
            }
            const resL = await pool.request().input('doc_num', sql.VarChar, doc_num).query(
                `SELECT reng_num, rowguid FROM saOrdenCompraReng WHERE LTRIM(RTRIM(doc_num)) = LTRIM(RTRIM(@doc_num))`
            );

            const transaction = new sql.Transaction(pool);
            await transaction.begin();

            try {
                const configuredSucu = (srv?.profit_branch_codes || []).find(b => b.is_default)?.code 
                    || (srv?.profit_branch_codes || [])[0]?.code 
                    || (srv?.profit_branch_codes || [])[0];
                const resSucu = await pool.request().query(`SELECT TOP 1 RTRIM(co_sucur) AS co_sucur FROM saSucursal ORDER BY CASE WHEN RTRIM(co_sucur) = '01' THEN 0 ELSE 1 END, co_sucur`);
                const defSucu = configuredSucu || resSucu.recordset[0]?.co_sucur || '01';
                const auditUser = (req.profitUser || 'API').substring(0, 6).toUpperCase();

                for (const line of resL.recordset) {
                    const rL = new sql.Request(transaction);
                    rL.input('sDoc_NumOri',  sql.Char(20),          doc_num);
                    rL.input('iReng_NumOri', sql.Int,               line.reng_num);
                    rL.input('sCo_Us_Mo',    sql.Char(6),           auditUser);
                    rL.input('sCo_Sucu_Mo',  sql.Char(6),           defSucu);
                    rL.input('sMaquina',     sql.VarChar(60),        'SYNC2K');
                    rL.input('gRowguid',     sql.UniqueIdentifier,  line.rowguid);
                    await rL.execute('pEliminarRenglonesOrdenCompra');
                }

                const rH = new sql.Request(transaction);
                rH.input('sDoc_NumOri', sql.Char(20),          doc_num);
                rH.input('tsValidador', sql.VarBinary,         validador);
                rH.input('sMaquina',    sql.VarChar(60),        'SYNC2K');
                rH.input('sCo_Us_Mo',   sql.Char(6),           auditUser);
                rH.input('sCo_Sucu_Mo', sql.Char(6),           defSucu);
                rH.input('gRowguid',    sql.UniqueIdentifier,  rowguid);
                await rH.execute('pEliminarOrdenCompra');

                await transaction.commit();
                return { success: true, doc_num: doc_num };
            } catch (err) {
                if (transaction._aborted === false) await transaction.rollback();
                throw err;
            }
        });

        return writeResponse(res, outcome);
    } catch (error) {
        res.status(500).json({ success: false, message: 'Error al eliminar.', error: error.message });
    }
});

// --- ANULAR ORDEN DE COMPRA ---
router.post('/:doc_num/anular', async (req, res) => {
    try {
        const { doc_num } = req.params;
        const { sede } = req.query;

        const outcome = await executeWrite(sede || null, req.sqlAuth, async (pool) => {
            const resH = await pool.request().input('doc_num', sql.VarChar, doc_num).query(
                `SELECT validador, rowguid, RTRIM(status) AS status, anulado
                 FROM saOrdenCompra
                 WHERE LTRIM(RTRIM(doc_num)) = LTRIM(RTRIM(@doc_num))`
            );
            if (!resH.recordset.length) throw new Error('Orden de compra no existe.');

            const { status, anulado } = resH.recordset[0];
            const currentStatus = String(status || '').trim();
            const isAnulada = !!anulado;
            if (isAnulada) {
                throw new Error(`La orden de compra ${doc_num} ya está anulada.`);
            }
            if (currentStatus !== '0') {
                throw new Error(`No se puede anular la orden de compra ${doc_num} porque ya ha sido procesada o modificada (status=${currentStatus}).`);
            }

            const transaction = new sql.Transaction(pool);
            await transaction.begin();

            try {
                const auditUser = (req.profitUser || 'API').substring(0, 6).toUpperCase();

                // Anular cabecera
                await transaction.request()
                    .input('doc_num', sql.Char(20), padProfit(doc_num, 20))
                    .input('auditUser', sql.Char(6), padProfit(auditUser, 6))
                    .query(`
                        UPDATE saOrdenCompra
                        SET anulado = 1,
                            fe_us_mo = GETDATE(),
                            co_us_mo = @auditUser
                        WHERE LTRIM(RTRIM(doc_num)) = LTRIM(RTRIM(@doc_num))
                    `);

                await transaction.commit();
                return { success: true, doc_num: doc_num };
            } catch (err) {
                if (transaction._aborted === false) await transaction.rollback();
                throw err;
            }
        });

        return writeResponse(res, outcome);
    } catch (error) {
        res.status(500).json({ success: false, message: 'Error al anular orden de compra.', error: error.message });
    }
});

module.exports = router;
