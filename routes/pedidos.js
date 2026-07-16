const express = require('express');
const router = express.Router();
const { sql, getPool, getServers, getExchangeRate } = require('../db');
const { executeWrite, writeResponse, paginatedResponse, padProfit } = require('../helpers/multiSede');

console.log("ðŸ› ï¸ [AGENT] Iniciando Módulo de Pedidos (Versión Unificada POST)");

/**
 * @swagger
 * tags:
 *   name: Pedidos
 *   description: Gestión de Pedidos de venta
 */

// --- OBTENER LISTADO ---
router.get('/', async (req, res) => {
    try {
        const page  = parseInt(req.query.page)  || 1;
        const limit = parseInt(req.query.limit) || 12;
        const { sede, doc_num, co_cli, co_ven, fec_d, fec_h, search, status, anulado } = req.query;
        
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
                    whereClauses.push("(c.co_cli LIKE @co_cli_search OR cl.cli_des LIKE @co_cli_search OR c.doc_num LIKE @co_cli_search OR cl.rif LIKE @co_cli_search)");
                }
                if (search) {
                    request.input('search_all', sql.VarChar, `%${search}%`);
                    whereClauses.push("(c.doc_num LIKE @search_all OR c.co_cli LIKE @search_all OR cl.cli_des LIKE @search_all OR cl.rif LIKE @search_all)");
                }
                if (co_ven) {
                    request.input('co_ven_filter', sql.VarChar, co_ven.trim().toUpperCase());
                    whereClauses.push("LTRIM(RTRIM(c.co_ven)) = @co_ven_filter");
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
                        whereClauses.push(`(${statusClauses.join(" OR ")})`);
                    }
                }
                if (anulado !== undefined && anulado !== '') {
                    request.input('anulado_val', sql.Bit, anulado === 'true' || anulado === '1' ? 1 : 0);
                    whereClauses.push("c.anulado = @anulado_val");
                }

                const whereSQL = whereClauses.join(" AND ");
                
                const [result, currentRate] = await Promise.all([
                    request.query(`
                        SELECT RTRIM(c.doc_num) AS doc_num, RTRIM(c.descrip) AS descrip,
                               RTRIM(c.co_cli)  AS co_cli,  RTRIM(cl.cli_des) AS cli_des,
                               c.fec_emis, c.fec_venc, c.fec_reg, c.fe_us_in AS fec_us_in, c.fe_us_mo AS fec_us_mo, RTRIM(c.status) AS status, c.anulado,
                               RTRIM(c.co_mone) AS co_mone, c.tasa, c.total_neto,
                               RTRIM(c.co_ven) AS co_ven, RTRIM(v.ven_des) AS ven_des
                        FROM saPedidoVenta c
                        LEFT JOIN saCliente cl ON c.co_cli = cl.co_cli
                        LEFT JOIN saVendedor v ON c.co_ven = v.co_ven
                        WHERE ${whereSQL}
                        ORDER BY c.fec_emis DESC, c.doc_num DESC
                    `),
                    getExchangeRate(pool)
                ]);

                return result.recordset.map(c => ({ 
                    ...c, 
                    tasa_actual: currentRate,
                    sede_id: srv.id, 
                    sede_nombre: srv.name 
                }));
            } catch (e) { 
                console.error(`[PEDIDOS] Error en sede ${srv.id}:`, e.message);
                return []; 
            }
        }));

        const combined = [].concat(...allData);
        combined.sort((a, b) => new Date(b.fec_emis) - new Date(a.fec_emis));
        return paginatedResponse(res, combined, page, limit);
    } catch (error) {
        res.status(500).json({ success: false, message: 'Error al consultar Pedidos.', error: error.message });
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
                               RTRIM(c.co_cli)  AS co_cli,  RTRIM(cl.cli_des) AS cli_des,
                               RTRIM(c.co_ven)  AS co_ven,  RTRIM(v.ven_des)  AS ven_des,
                               RTRIM(c.co_cond) AS co_cond, RTRIM(cd.cond_des) AS cond_des,
                               c.fec_emis, c.fec_venc, c.fec_reg, c.fe_us_in AS fec_us_in, c.fe_us_mo AS fec_us_mo, RTRIM(c.status) AS status, c.anulado,
                               RTRIM(c.co_mone) AS co_mone, c.tasa,
                               c.total_bruto, c.monto_imp, c.total_neto,
                               RTRIM(c.comentario) AS comentario,
                               RTRIM(c.dir_ent) AS dir_ent,
                               RTRIM(cl.rif) AS rif, RTRIM(cl.direc1) AS direc1, 
                               RTRIM(cl.telefonos) AS telefonos, RTRIM(cl.email) AS email,
                               RTRIM(cl.co_zon) AS co_zon, RTRIM(z.zon_des) AS zon_des, 
                               cl.contribu_e, cl.porc_esp
                        FROM saPedidoVenta c
                        LEFT JOIN saCliente      cl ON c.co_cli  = cl.co_cli
                        LEFT JOIN saVendedor     v  ON c.co_ven  = v.co_ven
                        LEFT JOIN saCondicionPago cd ON c.co_cond = cd.co_cond
                        LEFT JOIN saZona         z  ON cl.co_zon = z.co_zon
                        WHERE LTRIM(RTRIM(c.doc_num)) = LTRIM(RTRIM(@doc_num))
                    `),
                    pool.request().input('doc_num', sql.VarChar, doc_num).query(`
                        SELECT r.reng_num, RTRIM(r.co_art) AS co_art, RTRIM(a.art_des) AS art_des,
                               RTRIM(a.co_lin) AS co_lin, RTRIM(a.co_subl) AS co_subl, RTRIM(a.tipo) AS tipo_art,
                                r.total_art AS cantidad, r.pendiente, RTRIM(r.co_alma) AS co_alma,
                               r.co_precio AS co_precio, r.prec_vta AS precio,
                               RTRIM(r.tipo_imp) AS tipo_imp, r.porc_imp, r.reng_neto AS total_renglon,
                                r.prec_vta_om, RTRIM(r.co_uni) AS co_uni, RTRIM(u.des_uni) AS unidad,
                               RTRIM(r.tipo_doc) AS tipo_doc, RTRIM(r.num_doc) AS num_doc, r.rowguid_doc, r.rowguid
                        FROM saPedidoVentaReng r
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
            return res.status(404).json({ success: false, message: 'Cotización no encontrada.' });

        res.status(200).json({ success: true, count: found.length, data: results.filter(r => r !== null) });

    } catch (error) {
        res.status(500).json({ success: false, message: 'Error al consultar cotización.', error: error.message });
    }
});

// --- GUARDAR O ACTUALIZAR (POST UNIFICADO) ---
router.post('/', async (req, res) => {
    const data = req.body;
    console.log("ðŸ“¥ [AGENT] Recibiendo Cotización (UNIFIED POST):", JSON.stringify({ ...data, renglones: data.renglones?.length }, null, 2));

    if (!data.co_cli || !data.renglones) {
        return res.status(400).json({ success: false, message: 'Campos obligatorios: co_cli, renglones' });
    }

    const outcome = await executeWrite(req.query.sede || null, req.sqlAuth, async (pool) => {
        // 1. Cargar Catálogos y Parámetros Globales (Fallbacks)
        const [resMoneda, resUSD, resAlma, resVen, resCond, resSucu, resCli, resTax, resTasa, resCtaIE, resTran] = await Promise.all([
            pool.request().query(`SELECT TOP 1 RTRIM(g_moneda) AS g_moneda FROM par_emp`),
            pool.request().query(`SELECT TOP 1 RTRIM(co_mone)  AS co_mone   FROM saMoneda WHERE LTRIM(RTRIM(co_mone)) IN ('US$','USD','DOL','$','US') OR mone_des LIKE '%Dolar%'`),
            pool.request().query(`SELECT TOP 1 RTRIM(co_alma) AS co_alma FROM saAlmacen`),
            pool.request().query(`SELECT TOP 1 RTRIM(co_ven)  AS co_ven  FROM saVendedor`),
            pool.request().query(`SELECT TOP 1 RTRIM(co_cond) AS co_cond FROM saCondicionPago`),
            pool.request().query(`SELECT TOP 1 RTRIM(co_sucur) AS co_sucur FROM saSucursal`),
            pool.request().input('co_cli', sql.Char(16), data.co_cli).query(`SELECT RTRIM(co_mone) as co_mone, RTRIM(cond_pag) as cond_pag, RTRIM(co_ven) as co_ven, RTRIM(co_sucu_in) as co_sucu FROM saCliente WHERE co_cli = @co_cli`),
            pool.request().query(`SELECT TOP 1 RTRIM(tax_id) AS tax_id FROM saTax`),
            pool.request().query(`SELECT TOP 1 tasa_v FROM saTasa WHERE LTRIM(RTRIM(co_mone)) IN ('US$','USD','DOL','$','US') ORDER BY fecha DESC`),
            pool.request().query(`SELECT TOP 1 RTRIM(co_cta_ingr_egr) AS co_cta_ingr_egr FROM saCuentaIngEgr`),
            pool.request().query(`SELECT TOP 1 RTRIM(co_tran) AS co_tran FROM saTransporte`)
        ]);

        const cli = resCli.recordset[0] || {};
        const usdCode = resUSD.recordset[0]?.co_mone || 'US$';
        const bsCode   = resMoneda.recordset[0]?.g_moneda || 'BS';
        const defVen   = cli.co_ven   || resVen.recordset[0]?.co_ven     || '01';
        const defCond  = cli.cond_pag || resCond.recordset[0]?.co_cond   || '01';
        const defAlma  = resAlma.recordset[0]?.co_alma   || '01';
        const defSucu  = resSucu.recordset[0]?.co_sucur  || '01';
        const defCtaIE = resCtaIE.recordset[0]?.co_cta_ingr_egr || '01';
        const defTran  = resTran.recordset[0]?.co_tran || '01';
        const rawTax   = resTax.recordset[0]?.tax_id;
        const defTax   = rawTax ? rawTax.trim() : null;
        
        const auditUser = (req.profitUser || req.sqlAuth?.user || 'API').substring(0, 10).toUpperCase();
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
                console.log(`ðŸ”„ [AGENT] Resguardando datos de cotización existente: ${docNum}`);
                const resPre = await transaction.request().input('doc_num', sql.VarChar, docNum).query(
                    `SELECT TOP 1 * FROM saPedidoVenta WHERE LTRIM(RTRIM(doc_num)) = LTRIM(RTRIM(@doc_num))`
                );
                
                if (resPre.recordset.length === 0) {
                    throw new Error(`La cotización ${docNum} no existe para ser editada.`);
                }
                
                existingHeader = resPre.recordset[0];
                const currentStatus = String(existingHeader.status || '').trim();
                const isAnulada = !!existingHeader.anulado;
                if (isAnulada || currentStatus !== '0') {
                    throw new Error(`La cotización ${docNum} no está sin procesar. No se permite editar (status=${currentStatus || 'N/A'}${isAnulada ? ', anulada=1' : ''}).`);
                }
                const { validador, rowguid } = existingHeader;

                // Resguardar renglones
                const resL = await transaction.request().input('doc_num', sql.VarChar, docNum).query(
                    `SELECT reng_num, co_art, co_alma, co_uni, total_art, rowguid, RTRIM(tipo_doc) AS tipo_doc, RTRIM(num_doc) AS num_doc, rowguid_doc FROM saPedidoVentaReng WHERE LTRIM(RTRIM(doc_num)) = LTRIM(RTRIM(@doc_num))`
                );

                console.log(`🗑️ [AGENT] Eliminando versión anterior para re-inserción...`);
                for (const line of resL.recordset) {
                    const rStock = new sql.Request(transaction);
                    rStock.input('sCo_Alma',              sql.Char(6),  line.co_alma);
                    rStock.input('sCo_Art',               sql.Char(30), line.co_art);
                    rStock.input('sCo_Uni',               sql.Char(6),  line.co_uni);
                    rStock.input('deCantidad',            sql.Decimal(18, 5), line.total_art);
                    rStock.input('sTipoStock',            sql.Char(4),  'COM');
                    rStock.input('bSumarStock',           sql.Bit,      0); // Restar
                    rStock.input('bPermiteStockNegativo', sql.Bit,      1);
                    await rStock.execute('pStockActualizar');

                    // --- REVERSIÓN DE COTIZACIÓN ANTES DE ELIMINAR ---
                    if ((line.tipo_doc === 'COTI' || line.tipo_doc === 'CCLI') && line.rowguid_doc && line.num_doc) {
                        console.log(`📈 [AGENT] Revirtiendo ${line.total_art} al pendiente de cotización ${line.num_doc}, renglón guid: ${line.rowguid_doc}`);
                        const rRevert = new sql.Request(transaction);
                        rRevert.input('qty', sql.Decimal(18, 5), line.total_art);
                        rRevert.input('rowguid_doc', sql.UniqueIdentifier, line.rowguid_doc);
                        rRevert.input('num_doc', sql.Char(20), padProfit(line.num_doc, 20));
                        rRevert.input('auditUser', sql.Char(6), padProfit(auditUser, 6));

                        await rRevert.query(`
                            UPDATE saCotizacionClienteReng
                            SET pendiente = CASE WHEN pendiente + @qty > total_art THEN total_art ELSE pendiente + @qty END,
                                fe_us_mo = GETDATE(),
                                co_us_mo = @auditUser
                            WHERE rowguid = @rowguid_doc AND doc_num = @num_doc;

                            DECLARE @total_qty DECIMAL(18,5), @pending_qty DECIMAL(18,5);
                            SELECT @total_qty = SUM(total_art), @pending_qty = SUM(pendiente)
                            FROM saCotizacionClienteReng
                            WHERE doc_num = @num_doc;

                            UPDATE saCotizacionCliente
                            SET status = CASE 
                                WHEN @pending_qty = 0 THEN '2'
                                WHEN @pending_qty < @total_qty THEN '1'
                                ELSE '0'
                            END,
                            fe_us_mo = GETDATE(),
                            co_us_mo = @auditUser
                            WHERE doc_num = @num_doc;
                        `);
                    }

                    const rE = new sql.Request(transaction);
                    rE.input('sDoc_NumOri',  sql.Char(20),          docNum);
                    rE.input('iReng_NumOri', sql.Int,               line.reng_num);
                    rE.input('sCo_Us_Mo',    sql.Char(6),           auditUser);
                    rE.input('sCo_Sucu_Mo',  sql.Char(6),           defSucu);
                    rE.input('sMaquina',     sql.VarChar(60),        'SYNC2K');
                    rE.input('gRowguid',     sql.UniqueIdentifier,  line.rowguid);
                    await rE.execute('pEliminarRenglonesPedidoVenta');
                }

                const rHE = new sql.Request(transaction);
                rHE.input('sDoc_NumOri', sql.Char(20),          docNum);
                rHE.input('tsValidador', sql.VarBinary,         validador);
                rHE.input('sMaquina',    sql.VarChar(60),        'SYNC2K');
                rHE.input('sCo_Us_Mo',   sql.Char(6),           auditUser);
                rHE.input('sCo_Sucu_Mo', sql.Char(6),           defSucu);
                rHE.input('gRowguid',    sql.UniqueIdentifier,  rowguid);
                await rHE.execute('pEliminarPedidoVenta');
                
            } else {
                // --- MODO NUEVO ---
                let corrRow = null;

                // 1) Ruta estándar de Profit.
                const resCorr = await transaction.request().query(`
                    UPDATE saSerie
                    SET prox_n = prox_n + 1, fe_us_mo = GETDATE()
                    OUTPUT INSERTED.prox_n, RTRIM(INSERTED.desde_a) as prefijo
                    WHERE co_serie = (
                        SELECT TOP 1 co_serie
                        FROM saConsecutivo
                        WHERE UPPER(LTRIM(RTRIM(co_consecutivo))) = 'PVEN_NUM'
                    )
                `);
                corrRow = resCorr.recordset[0] || null;

                // 2) Fallback por instalaciones que usan otros códigos de consecutivo.
                if (!corrRow) {
                    const resCorrAlt = await transaction.request().query(`
                        UPDATE saSerie
                        SET prox_n = prox_n + 1, fe_us_mo = GETDATE()
                        OUTPUT INSERTED.prox_n, RTRIM(INSERTED.desde_a) as prefijo
                        WHERE co_serie = (
                            SELECT TOP 1 co_serie
                            FROM saConsecutivo
                            WHERE UPPER(LTRIM(RTRIM(co_consecutivo))) IN ('PVEN_NUM','PED_NUM','PEDIDO_NUM')
                               OR UPPER(LTRIM(RTRIM(co_consecutivo))) LIKE '%PVEN%'
                               OR UPPER(LTRIM(RTRIM(co_consecutivo))) LIKE '%PED%'
                        )
                    `);
                    corrRow = resCorrAlt.recordset[0] || null;
                }

                // 3) àšltimo fallback: generar desde el máximo doc_num existente.
                if (!corrRow) {
                    const fallbackRes = await transaction.request().query(`
                        SELECT
                            ISNULL(MAX(CAST(RIGHT(LTRIM(RTRIM(doc_num)), 10) AS BIGINT)), 0) + 1 AS prox_n,
                            (
                                SELECT TOP 1 RTRIM(desde_a)
                                FROM saSerie
                                WHERE LTRIM(RTRIM(ISNULL(desde_a, ''))) <> ''
                            ) AS prefijo
                        FROM saPedidoVenta
                        WHERE TRY_CAST(RIGHT(LTRIM(RTRIM(doc_num)), 10) AS BIGINT) IS NOT NULL
                    `);
                    corrRow = fallbackRes.recordset[0] || null;
                }

                if (!corrRow || !corrRow.prox_n) {
                    throw new Error("No se pudo obtener el correlativo.");
                }

                const proxN = Number(corrRow.prox_n || 0);
                // Regla solicitada: correlativo solo numérico de 10 caracteres.
                docNum = proxN.toString().padStart(10, '0');
                console.log(`âœ¨ [AGENT] Nuevo número generado: ${docNum}`);
            }

            // --- VALIDAR DISPONIBILIDAD DE STOCK ---
            if (data.renglones && Array.isArray(data.renglones) && data.renglones.length > 0) {
                const requestedStock = {};
                for (const item of data.renglones) {
                    const coArt = String(item.co_art || '').trim().toUpperCase();
                    const coAlma = String(item.co_alma || defAlma).trim().toUpperCase();
                    const qty = Number(item.cantidad || 0);
                    if (!coArt || qty <= 0) continue;
                    
                    const key = `${coArt}|${coAlma}`;
                    if (!requestedStock[key]) {
                        requestedStock[key] = {
                            co_art: coArt,
                            co_alma: coAlma,
                            art_des: item.art_des || coArt,
                            qty: 0
                        };
                    }
                    requestedStock[key].qty += qty;
                }

                const uniqueArticulos = [...new Set(Object.values(requestedStock).map(r => r.co_art))];
                if (uniqueArticulos.length > 0) {
                    const requestStockCheck = new sql.Request(transaction);
                    const artParams = [];
                    uniqueArticulos.forEach((art, idx) => {
                        const paramName = `art_${idx}`;
                        requestStockCheck.input(paramName, sql.Char(30), padProfit(art, 30));
                        artParams.push(`@${paramName}`);
                    });

                    const resStockCheck = await requestStockCheck.query(`
                        SELECT 
                            RTRIM(a.co_art) AS co_art,
                            RTRIM(a.tipo) AS tipo_art,
                            RTRIM(a.co_lin) AS co_lin,
                            RTRIM(s.co_alma) AS co_alma,
                            ISNULL(SUM(CASE WHEN RTRIM(s.tipo)='ACT' THEN s.stock ELSE 0 END), 0) -
                            ISNULL(SUM(CASE WHEN RTRIM(s.tipo)='COM' THEN s.stock ELSE 0 END), 0) AS stock_disponible
                        FROM saArticulo a
                        LEFT JOIN saStockAlmacen s ON a.co_art = s.co_art
                        WHERE a.co_art IN (${artParams.join(', ')})
                        GROUP BY a.co_art, a.tipo, a.co_lin, s.co_alma
                    `);

                    const dbStockMap = {};
                    const articleInfo = {};

                    resStockCheck.recordset.forEach(row => {
                        const art = String(row.co_art || '').trim().toUpperCase();
                        const alma = String(row.co_alma || '').trim().toUpperCase();
                        const stockDisp = Number(row.stock_disponible || 0);

                        articleInfo[art] = {
                            tipo_art: String(row.tipo_art || '').trim().toUpperCase(),
                            co_lin: String(row.co_lin || '').trim().toUpperCase()
                        };

                        if (alma) {
                            dbStockMap[`${art}|${alma}`] = stockDisp;
                        }
                    });

                    const errors = [];
                    for (const key of Object.keys(requestedStock)) {
                        const req = requestedStock[key];
                        const art = req.co_art;
                        const alma = req.co_alma;
                        const requestedQty = req.qty;

                        const info = articleInfo[art];
                        if (!info) {
                            errors.push(`El artículo "${req.art_des}" (${art}) no existe en el catálogo.`);
                            continue;
                        }

                        if (info.tipo_art === 'S' || info.tipo_art === '2' || info.co_lin === '09') {
                            continue;
                        }

                        const availableStock = dbStockMap[`${art}|${alma}`] || 0;
                        if (availableStock < requestedQty) {
                            errors.push(`El artículo "${req.art_des}" (${art}) en el almacén "${alma}" no tiene suficiente stock disponible. Disponible: ${availableStock.toFixed(2)}, solicitado: ${requestedQty.toFixed(2)}.`);
                        }
                    }

                    if (errors.length > 0) {
                        throw new Error(`Validación de Stock Fallida:\n` + errors.join('\n'));
                    }
                }
            }

            let isUSD = data.showUSD === true; 
            if (data.showUSD === undefined) {
                isUSD = String(data.co_mone || existingHeader?.co_mone || '').includes('US');
            }
            
            const currentTasa = resTasa.recordset[0]?.tasa_v || 1;
            let tasaDoc = Number(data.tasa || existingHeader?.tasa || currentTasa);
            
            if (tasaDoc <= 1 && currentTasa > 1) {
                tasaDoc = currentTasa; // Auto-corrección de bug anterior
            }
            
            // Recalcular Totales desde Renglones (Importante para que no guarde en cero)
            // En Profit, los totales de cabecera suelen almacenarse en MONEDA BASE (BS)
            let totalBruto = 0;
            let totalImp   = 0;

            if (data.renglones && Array.isArray(data.renglones)) {
                data.renglones.forEach(item => {
                    const qty = Number(item.cantidad || 0);
                    const prcIn = Number(item.precio || 0);
                    const pImp = Number(item.porc_imp || 0);
                    
                    // Convertir a BS para la cabecera
                    const prcBs = isUSD ? (prcIn * tasaDoc) : prcIn;
                    
                    const sub = Math.round((qty * prcBs) * 100) / 100;
                    const imp = Math.round(((sub * pImp) / 100) * 100) / 100;
                    
                    totalBruto += sub;
                    totalImp   += imp;
                });
            }

            const totalNeto = Math.round((totalBruto + totalImp) * 100) / 100;

            // En Profit, los documentos suelen guardarse con la moneda 'US$' si se transan en dólares, 
            // e internamente se mantiene la tasa para conversiones contables en BS.
            const finalMone = isUSD ? usdCode : bsCode;
            
            const rH = new sql.Request(transaction);
            rH.input('sDoc_Num',          sql.Char(20),         padProfit(docNum, 20));
            rH.input('sDescrip',          sql.VarChar(60),      (data.descrip || existingHeader?.descrip || 'PEDIDO WEB').substring(0, 60));
            rH.input('sCo_Cli',           sql.Char(16),         padProfit(data.co_cli  || existingHeader?.co_cli, 16));
            rH.input('sCo_Cta_Ingr_Egr',  sql.Char(20),         null);
            rH.input('sCo_Tran',          sql.Char(6),          padProfit(existingHeader?.co_tran || data.co_tran || defTran, 6));
            rH.input('sCo_Mone',          sql.Char(6),          padProfit(finalMone, 6));
            rH.input('sCo_Ven',           sql.Char(6),          padProfit(data.co_ven  || existingHeader?.co_ven  || defVen, 6));
            rH.input('sCo_Cond',          sql.Char(6),          padProfit(data.co_cond || existingHeader?.co_cond || defCond, 6));
            const finalFecEmis = isUpdate ? existingHeader.fec_emis : tsDate;
            rH.input('sdFec_Emis',        sql.SmallDateTime,    finalFecEmis);
            rH.input('sdFec_Venc',        sql.SmallDateTime,    finalFecEmis);
            rH.input('sdFec_Reg',         sql.SmallDateTime,    isUpdate ? existingHeader.fec_reg : tsDate);
            rH.input('bAnulado',          sql.Bit,              existingHeader?.anulado || 0);
            rH.input('sStatus',           sql.Char(1),          existingHeader?.status  || '0');
            rH.input('deTasa',            sql.Decimal(21, 8),   tasaDoc);
            rH.input('sN_Control',        sql.VarChar(20),      existingHeader?.n_control || '');
            rH.input('sNro_Doc',          sql.VarChar(20),      existingHeader?.nro_doc || null);
            rH.input('sPorc_Desc_Glob',   sql.VarChar(15),      existingHeader?.porc_desc_glob || null);
            rH.input('deMonto_Desc_Glob', sql.Decimal(18, 2),   0);
            rH.input('sPorc_Reca',        sql.VarChar(15),      existingHeader?.porc_reca || null);
            rH.input('deMonto_Reca',      sql.Decimal(18, 2),   0);
            rH.input('deSaldo',           sql.Decimal(18, 2),   totalNeto);
            rH.input('deTotal_Bruto',     sql.Decimal(18, 2),   totalBruto);
            rH.input('deMonto_Imp',       sql.Decimal(18, 2),   totalImp);
            rH.input('deMonto_Imp2',      sql.Decimal(18, 2),   0);
            rH.input('deMonto_Imp3',      sql.Decimal(18, 2),   0);
            rH.input('deOtros1',          sql.Decimal(18, 2),   0);
            rH.input('deOtros2',          sql.Decimal(18, 2),   0);
            rH.input('deOtros3',          sql.Decimal(18, 2),   0);
            rH.input('deTotal_Neto',      sql.Decimal(18, 2),   totalNeto);
            rH.input('sDis_Cen',          sql.VarChar(sql.MAX), existingHeader?.dis_cen || '');
            const finalComment = (data.comentario ? data.comentario + ' | ' : '') + (isUpdate ? 'Editado vía API' : 'Creado vía API');
            rH.input('sComentario',       sql.VarChar(sql.MAX), finalComment.substring(0, 500));
            rH.input('sDir_Ent',          sql.VarChar(sql.MAX), (data.dir_ent || existingHeader?.dir_ent || '').substring(0, 100));
            rH.input('bContrib',          sql.Bit,              existingHeader?.contrib ?? 1);
            rH.input('bImpresa',          sql.Bit,              existingHeader?.impresa || 0);
            const rawTaxVal = (data.salestax || existingHeader?.salestax || defTax || '').trim();
            const finalTax  = rawTaxVal === '' ? null : rawTaxVal;
            
            rH.input('sSalestax',         sql.Char(8),          finalTax);
            rH.input('sImpfis',           sql.VarChar(20),      existingHeader?.impfis || '');
            rH.input('sImpfisfac',        sql.VarChar(20),      existingHeader?.impfisfac || '');
            rH.input('bVen_Ter',          sql.Bit,              existingHeader?.ven_ter || 0);
            rH.input('sCo_Us_In',         sql.Char(6),          padProfit(isUpdate ? (existingHeader.co_us_in || auditUser) : auditUser, 6));
            rH.input('sCo_Sucu_In',       sql.Char(6),          padProfit(isUpdate ? (existingHeader.co_sucu_in || defSucu) : (data.co_sucu_in || defSucu), 6));
            rH.input('sRevisado',         sql.Char(1),          null);
            rH.input('sTrasnfe',          sql.Char(1),          null);
            rH.input('sMaquina',          sql.VarChar(60),      'SYNC2K');
            await rH.execute('pInsertarPedidoVenta');

            const guidRes = await transaction.request().input('doc_num', sql.Char(20), docNum).query('SELECT rowguid FROM saPedidoVenta WHERE doc_num = @doc_num');
            const rowguidDoc = guidRes.recordset[0]?.rowguid;

            for (let i = 0; i < data.renglones.length; i++) {
                const item = data.renglones[i];
                const qty = Number(item.cantidad || 0);
                const prcIn = Number(item.precio || 0);
                const pImp = Number(item.porc_imp || 0);
                const coPrecio = String(
                    item.co_precio || item.id_precio || item.co_tipo_precio || '01'
                ).trim().substring(0, 6);

                const prcBs = isUSD ? (prcIn * tasaDoc) : prcIn;
                const prcUSD = isUSD ? prcIn : (prcIn / tasaDoc);
                const sub = Math.round((qty * prcBs) * 100) / 100;
                const imp = Math.round(((sub * pImp) / 100) * 100) / 100;

                const rL = new sql.Request(transaction);
                rL.input('iReng_Num',          sql.Int,              i + 1);
                rL.input('sDoc_Num',           sql.Char(20),         padProfit(docNum, 20));
                rL.input('sCo_Art',            sql.Char(30),         padProfit(item.co_art, 30));
                rL.input('sDes_Art',           sql.VarChar(120),      (item.art_des || '').substring(0, 120));
                const rawUni = String(item.co_uni || '').trim();
                const finalUni = rawUni === '' ? 'UNI' : rawUni; // Fallback por si acaso

                rL.input('sCo_Uni',            sql.Char(6),          padProfit(finalUni, 6));
                rL.input('sSco_Uni',           sql.Char(6),          padProfit(finalUni, 6)); // La clave: nunca enviar NULL a la unidad secundaria
                rL.input('sCo_Alma',           sql.Char(6),          padProfit(item.co_alma || defAlma, 6));
                rL.input('sCo_Precio',         sql.Char(6),          padProfit(coPrecio || '01', 6));
                rL.input('sTipo_Imp',          sql.Char(1),          item.tipo_imp || '1');
                rL.input('sTipo_Imp2',         sql.Char(1),          null);
                rL.input('sTipo_Imp3',         sql.Char(1),          null);
                rL.input('deTotal_Art',        sql.Decimal(18, 5),    qty);
                rL.input('deSTotal_Art',       sql.Decimal(18, 5),    0);
                rL.input('dePrec_Vta',         sql.Decimal(18, 5),    prcBs);
                rL.input('sPorc_Desc',         sql.VarChar(15),      null);
                rL.input('deMonto_Desc',       sql.Decimal(18, 5),    0); // NO PERMITE NULL
                rL.input('deReng_Neto',        sql.Decimal(18, 2),    sub);
                rL.input('dePendiente',        sql.Decimal(18, 5),    qty);
                rL.input('dePendiente2',       sql.Decimal(18, 5),    0);
                rL.input('deMonto_Desc_Glob',  sql.Decimal(18, 5),    0);
                rL.input('deMonto_reca_Glob',  sql.Decimal(18, 5),    0);
                rL.input('deOtros1_glob',      sql.Decimal(18, 5),    0);
                rL.input('deOtros2_glob',      sql.Decimal(18, 5),    0);
                rL.input('deOtros3_glob',      sql.Decimal(18, 5),    0);
                rL.input('deMonto_imp_afec_glob',  sql.Decimal(18, 5), 0);
                rL.input('deMonto_imp2_afec_glob', sql.Decimal(18, 5), 0);
                rL.input('deMonto_imp3_afec_glob', sql.Decimal(18, 5), 0);
                const tipoDocVal = (item.tipo_doc && String(item.tipo_doc).trim()) ? String(item.tipo_doc).trim().toUpperCase() : null;
                const numDocVal = (item.num_doc && String(item.num_doc).trim()) ? String(item.num_doc).trim() : null;
                const rowguidDocVal = (item.rowguid_doc && String(item.rowguid_doc).trim()) ? String(item.rowguid_doc).trim() : null;

                rL.input('sTipo_Doc',          sql.Char(4),          tipoDocVal ? padProfit(tipoDocVal, 4) : null);
                rL.input('gRowguid_Doc',       sql.UniqueIdentifier, rowguidDocVal || null);
                rL.input('sNum_Doc',           sql.VarChar(20),      numDocVal ? padProfit(numDocVal, 20) : null);
                rL.input('dePorc_Imp',         sql.Decimal(18, 5),    pImp);
                rL.input('dePorc_Imp2',        sql.Decimal(18, 5),    0);
                rL.input('dePorc_Imp3',        sql.Decimal(18, 5),    0);
                rL.input('deMonto_Imp',        sql.Decimal(18, 5),    imp);
                rL.input('deMonto_Imp2',       sql.Decimal(18, 5),    0);
                rL.input('deMonto_Imp3',       sql.Decimal(18, 5),    0);
                rL.input('deOtros',            sql.Decimal(18, 5),    0); // NO PERMITE NULL
                rL.input('deTotal_Dev',        sql.Decimal(18, 5),    0);
                rL.input('deMonto_Dev',        sql.Decimal(18, 5),    0); // NO PERMITE NULL
                rL.input('sComentario',        sql.VarChar(sql.MAX),  '');
                rL.input('sDis_Cen',           sql.VarChar(sql.MAX),  null);
                rL.input('sCo_Sucu_In',        sql.Char(6),           padProfit(data.co_sucu_in || defSucu, 6));
                rL.input('sCo_Us_In',          sql.Char(6),           padProfit(auditUser, 6));
                rL.input('sREVISADO',          sql.Char(1),           null);
                rL.input('sTRASNFE',           sql.Char(1),           null);
                rL.input('sMaquina',           sql.VarChar(60),      'SYNC2K');
                await rL.execute('pInsertarRenglonesPedidoVenta');

                await transaction.request()
                    .input('om', sql.Decimal(18, 5), prcUSD)
                    .input('doc', sql.Char(20), padProfit(docNum, 20))
                    .input('reng', sql.Int, i + 1)
                    .query(`UPDATE saPedidoVentaReng SET prec_vta_om = @om WHERE doc_num = @doc AND reng_num = @reng`);

                // --- DECREMENTAR PENDIENTE EN COTIZACIÓN ORIGEN ---
                if ((tipoDocVal === 'COTI' || tipoDocVal === 'CCLI') && rowguidDocVal && numDocVal) {
                    console.log(`📉 [AGENT] Descontando ${qty} del pendiente en cotización ${numDocVal}, renglón guid: ${rowguidDocVal}`);
                    const rQuote = new sql.Request(transaction);
                    rQuote.input('qty', sql.Decimal(18, 5), qty);
                    rQuote.input('rowguid_doc', sql.UniqueIdentifier, rowguidDocVal);
                    rQuote.input('num_doc', sql.Char(20), padProfit(numDocVal, 20));
                    rQuote.input('auditUser', sql.Char(6), padProfit(auditUser, 6));
                    
                    await rQuote.query(`
                        UPDATE saCotizacionClienteReng
                        SET pendiente = CASE WHEN pendiente >= @qty THEN pendiente - @qty ELSE 0 END,
                            fe_us_mo = GETDATE(),
                            co_us_mo = @auditUser
                        WHERE rowguid = @rowguid_doc AND doc_num = @num_doc;

                        DECLARE @total_qty DECIMAL(18,5), @pending_qty DECIMAL(18,5);
                        SELECT @total_qty = SUM(total_art), @pending_qty = SUM(pendiente)
                        FROM saCotizacionClienteReng
                        WHERE doc_num = @num_doc;

                        UPDATE saCotizacionCliente
                        SET status = CASE 
                            WHEN @pending_qty = 0 THEN '2' -- Procesado totalmente
                            WHEN @pending_qty < @total_qty THEN '1' -- Procesado parcial
                            ELSE '0' -- Sin procesar
                        END,
                        fe_us_mo = GETDATE(),
                        co_us_mo = @auditUser
                        WHERE doc_num = @num_doc;
                    `);
                }

                // --- COMPROMETER STOCK ---
                const rStockIn = new sql.Request(transaction);
                rStockIn.input('sCo_Alma',              sql.Char(6),  padProfit(item.co_alma || defAlma, 6));
                rStockIn.input('sCo_Art',               sql.Char(30), padProfit(item.co_art, 30));
                rStockIn.input('sCo_Uni',               sql.Char(6),  padProfit(finalUni, 6));
                rStockIn.input('deCantidad',            sql.Decimal(18, 5), qty);
                rStockIn.input('sTipoStock',            sql.Char(4),  'COM');
                rStockIn.input('bSumarStock',           sql.Bit,      1); // Sumar (comprometer)
                rStockIn.input('bPermiteStockNegativo', sql.Bit,      1);
                await rStockIn.execute('pStockActualizar');
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

// --- ELIMINAR COTIZACIÓN ---
router.delete('/:doc_num', async (req, res) => {
    try {
        const { doc_num } = req.params;
        const { sede } = req.query;

        const outcome = await executeWrite(sede || null, req.sqlAuth, async (pool) => {
            const resH = await pool.request().input('doc_num', sql.VarChar, doc_num).query(
                `SELECT validador, rowguid, RTRIM(status) AS status, anulado
                 FROM saPedidoVenta
                 WHERE LTRIM(RTRIM(doc_num)) = LTRIM(RTRIM(@doc_num))`
            );
            if (!resH.recordset.length) throw new Error('Pedido no existe.');

            const { validador, rowguid, status, anulado } = resH.recordset[0];
            const currentStatus = String(status || '').trim();
            const isAnulada = !!anulado;
            if (isAnulada || currentStatus !== '0') {
                throw new Error(`No se puede eliminar ${doc_num}: solo se permiten Pedidos sin procesar (status=${currentStatus || 'N/A'}${isAnulada ? ', anulada=1' : ''}).`);
            }
            const resL = await pool.request().input('doc_num', sql.VarChar, doc_num).query(
                `SELECT reng_num, co_art, co_alma, co_uni, total_art, rowguid, RTRIM(tipo_doc) AS tipo_doc, RTRIM(num_doc) AS num_doc, rowguid_doc FROM saPedidoVentaReng WHERE LTRIM(RTRIM(doc_num)) = LTRIM(RTRIM(@doc_num))`
            );

            const transaction = new sql.Transaction(pool);
            await transaction.begin();

            try {
                const resSucu = await pool.request().query(`SELECT TOP 1 RTRIM(co_sucur) AS co_sucur FROM saSucursal`);
                const defSucu = resSucu.recordset[0]?.co_sucur || '01';
                const auditUser = (req.profitUser || 'API').substring(0, 10).toUpperCase();

                console.log(`🧹 [AGENT] Devolviendo stock de ${resL.recordset.length} renglones...`);
                for (const line of resL.recordset) {
                    const rStock = new sql.Request(transaction);
                    rStock.input('sCo_Alma',              sql.Char(6),  line.co_alma);
                    rStock.input('sCo_Art',               sql.Char(30), line.co_art);
                    rStock.input('sCo_Uni',               sql.Char(6),  line.co_uni);
                    rStock.input('deCantidad',            sql.Decimal(18, 5), line.total_art);
                    rStock.input('sTipoStock',            sql.Char(4),  'COM');
                    rStock.input('bSumarStock',           sql.Bit,      0); // Restar
                    rStock.input('bPermiteStockNegativo', sql.Bit,      1);
                    await rStock.execute('pStockActualizar');

                    // --- REVERSIÓN DE COTIZACIÓN ANTES DE ELIMINAR ---
                    if ((line.tipo_doc === 'COTI' || line.tipo_doc === 'CCLI') && line.rowguid_doc && line.num_doc) {
                        console.log(`📈 [AGENT] Revirtiendo ${line.total_art} al pendiente de cotización ${line.num_doc}, renglón guid: ${line.rowguid_doc}`);
                        const rRevert = new sql.Request(transaction);
                        rRevert.input('qty', sql.Decimal(18, 5), line.total_art);
                        rRevert.input('rowguid_doc', sql.UniqueIdentifier, line.rowguid_doc);
                        rRevert.input('num_doc', sql.Char(20), padProfit(line.num_doc, 20));
                        rRevert.input('auditUser', sql.Char(6), padProfit(auditUser, 6));

                        await rRevert.query(`
                            UPDATE saCotizacionClienteReng
                            SET pendiente = CASE WHEN pendiente + @qty > total_art THEN total_art ELSE pendiente + @qty END,
                                fe_us_mo = GETDATE(),
                                co_us_mo = @auditUser
                            WHERE rowguid = @rowguid_doc AND doc_num = @num_doc;

                            DECLARE @total_qty DECIMAL(18,5), @pending_qty DECIMAL(18,5);
                            SELECT @total_qty = SUM(total_art), @pending_qty = SUM(pendiente)
                            FROM saCotizacionClienteReng
                            WHERE doc_num = @num_doc;

                            UPDATE saCotizacionCliente
                            SET status = CASE 
                                WHEN @pending_qty = 0 THEN '2'
                                WHEN @pending_qty < @total_qty THEN '1'
                                ELSE '0'
                            END,
                            fe_us_mo = GETDATE(),
                            co_us_mo = @auditUser
                            WHERE doc_num = @num_doc;
                        `);
                    }

                    const rL = new sql.Request(transaction);
                    rL.input('sDoc_NumOri',  sql.Char(20),          doc_num);
                    rL.input('iReng_NumOri', sql.Int,               line.reng_num);
                    rL.input('sCo_Us_Mo',    sql.Char(6),           auditUser);
                    rL.input('sCo_Sucu_Mo',  sql.Char(6),           defSucu);
                    rL.input('sMaquina',     sql.VarChar(60),        'SYNC2K');
                    rL.input('gRowguid',     sql.UniqueIdentifier,  line.rowguid);
                    await rL.execute('pEliminarRenglonesPedidoVenta');
                }

                const rH = new sql.Request(transaction);
                rH.input('sDoc_NumOri', sql.Char(20),          doc_num);
                rH.input('tsValidador', sql.VarBinary,         validador);
                rH.input('sMaquina',    sql.VarChar(60),        'SYNC2K');
                rH.input('sCo_Us_Mo',   sql.Char(6),           auditUser);
                rH.input('sCo_Sucu_Mo', sql.Char(6),           defSucu);
                rH.input('gRowguid',    sql.UniqueIdentifier,  rowguid);
                await rH.execute('pEliminarPedidoVenta');

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

// --- ANULAR PEDIDO ---
router.post('/:doc_num/anular', async (req, res) => {
    try {
        const { doc_num } = req.params;
        const { sede } = req.query;

        const outcome = await executeWrite(sede || null, req.sqlAuth, async (pool) => {
            const resH = await pool.request().input('doc_num', sql.VarChar, doc_num).query(
                `SELECT validador, rowguid, RTRIM(status) AS status, anulado
                 FROM saPedidoVenta
                 WHERE LTRIM(RTRIM(doc_num)) = LTRIM(RTRIM(@doc_num))`
            );
            if (!resH.recordset.length) throw new Error('Pedido no existe.');

            const { status, anulado } = resH.recordset[0];
            const currentStatus = String(status || '').trim();
            const isAnulada = !!anulado;
            if (isAnulada) {
                throw new Error(`El pedido ${doc_num} ya está anulado.`);
            }
            if (currentStatus !== '0') {
                throw new Error(`No se puede anular el pedido ${doc_num} porque ya ha sido procesado o facturado (status=${currentStatus}).`);
            }

            const resL = await pool.request().input('doc_num', sql.VarChar, doc_num).query(
                `SELECT reng_num, co_art, co_alma, co_uni, total_art, rowguid, RTRIM(tipo_doc) AS tipo_doc, RTRIM(num_doc) AS num_doc, rowguid_doc FROM saPedidoVentaReng WHERE LTRIM(RTRIM(doc_num)) = LTRIM(RTRIM(@doc_num))`
            );

            const transaction = new sql.Transaction(pool);
            await transaction.begin();

            try {
                const auditUser = (req.profitUser || 'API').substring(0, 10).toUpperCase();

                // 1. Anular cabecera
                await transaction.request()
                    .input('doc_num', sql.Char(20), padProfit(doc_num, 20))
                    .input('auditUser', sql.Char(6), padProfit(auditUser, 6))
                    .query(`
                        UPDATE saPedidoVenta
                        SET anulado = 1,
                            fe_us_mo = GETDATE(),
                            co_us_mo = @auditUser
                        WHERE LTRIM(RTRIM(doc_num)) = LTRIM(RTRIM(@doc_num))
                    `);

                // 2. Anular renglones: poner pendiente a 0
                await transaction.request()
                    .input('doc_num', sql.Char(20), padProfit(doc_num, 20))
                    .input('auditUser', sql.Char(6), padProfit(auditUser, 6))
                    .query(`
                        UPDATE saPedidoVentaReng
                        SET pendiente = 0,
                            fe_us_mo = GETDATE(),
                            co_us_mo = @auditUser
                        WHERE LTRIM(RTRIM(doc_num)) = LTRIM(RTRIM(@doc_num))
                    `);

                // 3. Devolver stock comprometido (restar stock tipo 'COM')
                console.log(`🧹 [AGENT] Restando stock comprometido de ${resL.recordset.length} renglones...`);
                for (const line of resL.recordset) {
                    const rStock = new sql.Request(transaction);
                    rStock.input('sCo_Alma',              sql.Char(6),  line.co_alma);
                    rStock.input('sCo_Art',               sql.Char(30), line.co_art);
                    rStock.input('sCo_Uni',               sql.Char(6),  line.co_uni);
                    rStock.input('deCantidad',            sql.Decimal(18, 5), line.total_art);
                    rStock.input('sTipoStock',            sql.Char(4),  'COM');
                    rStock.input('bSumarStock',           sql.Bit,      0); // Restar (liberar comprometido)
                    rStock.input('bPermiteStockNegativo', sql.Bit,      1);
                    await rStock.execute('pStockActualizar');

                    // --- REVERSIÓN DE COTIZACIÓN ASOCIADA ---
                    if ((line.tipo_doc === 'COTI' || line.tipo_doc === 'CCLI') && line.rowguid_doc && line.num_doc) {
                        console.log(`📈 [AGENT] Revirtiendo ${line.total_art} al pendiente de cotización ${line.num_doc}, renglón guid: ${line.rowguid_doc}`);
                        const rRevert = new sql.Request(transaction);
                        rRevert.input('qty', sql.Decimal(18, 5), line.total_art);
                        rRevert.input('rowguid_doc', sql.UniqueIdentifier, line.rowguid_doc);
                        rRevert.input('num_doc', sql.Char(20), padProfit(line.num_doc, 20));
                        rRevert.input('auditUser', sql.Char(6), padProfit(auditUser, 6));

                        await rRevert.query(`
                            UPDATE saCotizacionClienteReng
                            SET pendiente = CASE WHEN pendiente + @qty > total_art THEN total_art ELSE pendiente + @qty END,
                                fe_us_mo = GETDATE(),
                                co_us_mo = @auditUser
                            WHERE rowguid = @rowguid_doc AND doc_num = @num_doc;

                            DECLARE @total_qty DECIMAL(18,5), @pending_qty DECIMAL(18,5);
                            SELECT @total_qty = SUM(total_art), @pending_qty = SUM(pendiente)
                            FROM saCotizacionClienteReng
                            WHERE doc_num = @num_doc;

                            UPDATE saCotizacionCliente
                            SET status = CASE 
                                WHEN @pending_qty = 0 THEN '2'
                                WHEN @pending_qty < @total_qty THEN '1'
                                ELSE '0'
                            END,
                            fe_us_mo = GETDATE(),
                            co_us_mo = @auditUser
                            WHERE doc_num = @num_doc;
                        `);
                    }
                }

                await transaction.commit();
                return { success: true, doc_num: doc_num };
            } catch (err) {
                if (transaction._aborted === false) await transaction.rollback();
                throw err;
            }
        });

        return writeResponse(res, outcome);
    } catch (error) {
        res.status(500).json({ success: false, message: 'Error al anular pedido.', error: error.message });
    }
});

// --- PROCESAR / LIBERAR PEDIDO PARCIAL ---
router.post('/:doc_num/procesar', async (req, res) => {
    try {
        const { doc_num } = req.params;
        const { sede } = req.query;

        const outcome = await executeWrite(sede || null, req.sqlAuth, async (pool) => {
            const resH = await pool.request().input('doc_num', sql.VarChar, doc_num).query(
                `SELECT validador, rowguid, RTRIM(status) AS status, anulado
                 FROM saPedidoVenta
                 WHERE LTRIM(RTRIM(doc_num)) = LTRIM(RTRIM(@doc_num))`
            );
            if (!resH.recordset.length) throw new Error('Pedido no existe.');

            const { status, anulado } = resH.recordset[0];
            const currentStatus = String(status || '').trim();
            const isAnulada = !!anulado;
            if (isAnulada) {
                throw new Error(`El pedido ${doc_num} está anulado y no se puede procesar.`);
            }
            if (currentStatus !== '1') {
                throw new Error(`Solo se pueden liberar/procesar pedidos parcialmente procesados (status=1). Estado actual: ${currentStatus}.`);
            }

            const resL = await pool.request().input('doc_num', sql.VarChar, doc_num).query(
                `SELECT reng_num, co_art, co_alma, co_uni, pendiente, rowguid FROM saPedidoVentaReng WHERE LTRIM(RTRIM(doc_num)) = LTRIM(RTRIM(@doc_num))`
            );

            const transaction = new sql.Transaction(pool);
            await transaction.begin();

            try {
                const auditUser = (req.profitUser || 'API').substring(0, 10).toUpperCase();

                // 1. Cambiar estado a Procesado (status = '2')
                await transaction.request()
                    .input('doc_num', sql.Char(20), padProfit(doc_num, 20))
                    .input('auditUser', sql.Char(6), padProfit(auditUser, 6))
                    .query(`
                        UPDATE saPedidoVenta
                        SET status = '2',
                            fe_us_mo = GETDATE(),
                            co_us_mo = @auditUser
                        WHERE LTRIM(RTRIM(doc_num)) = LTRIM(RTRIM(@doc_num))
                    `);

                // 2. Renglones: poner pendiente a 0
                await transaction.request()
                    .input('doc_num', sql.Char(20), padProfit(doc_num, 20))
                    .input('auditUser', sql.Char(6), padProfit(auditUser, 6))
                    .query(`
                        UPDATE saPedidoVentaReng
                        SET pendiente = 0,
                            fe_us_mo = GETDATE(),
                            co_us_mo = @auditUser
                        WHERE LTRIM(RTRIM(doc_num)) = LTRIM(RTRIM(@doc_num))
                    `);

                // 3. Devolver stock comprometido restante (restar stock tipo 'COM' por la cantidad pendiente)
                console.log(`🧹 [AGENT] Liberando stock comprometido de ${resL.recordset.length} renglones para pedido ${doc_num}...`);
                for (const line of resL.recordset) {
                    const pendingQty = Number(line.pendiente || 0);
                    if (pendingQty > 0) {
                        const rStock = new sql.Request(transaction);
                        rStock.input('sCo_Alma',              sql.Char(6),  line.co_alma);
                        rStock.input('sCo_Art',               sql.Char(30), line.co_art);
                        rStock.input('sCo_Uni',               sql.Char(6),  line.co_uni);
                        rStock.input('deCantidad',            sql.Decimal(18, 5), pendingQty);
                        rStock.input('sTipoStock',            sql.Char(4),  'COM');
                        rStock.input('bSumarStock',           sql.Bit,      0); // Restar (liberar comprometido)
                        rStock.input('bPermiteStockNegativo', sql.Bit,      1);
                        await rStock.execute('pStockActualizar');
                    }
                }

                await transaction.commit();
                return { success: true, doc_num: doc_num };
            } catch (err) {
                if (transaction._aborted === false) await transaction.rollback();
                throw err;
            }
        });

        return writeResponse(res, outcome);
    } catch (error) {
        res.status(500).json({ success: false, message: 'Error al liberar pedido parcialmente procesado.', error: error.message });
    }
});

module.exports = router;

