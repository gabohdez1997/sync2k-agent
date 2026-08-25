const express = require('express');
const router = express.Router();
const { sql, getPool, getServers, getExchangeRate } = require('../db');
const { executeWrite, writeResponse, paginatedResponse, padProfit } = require('../helpers/multiSede');
const { getProximoConsecutivo } = require('../helpers/consecutivos');

console.log("📦 [AGENT] Iniciando Módulo de Notas de Recepción de Compra (saNotaRecepcionCompra)");

/**
 * Helper para formatear fecha a YYYY-MM-DD sin desajustes de zona horaria
 */
function safeDate(val) {
    if (!val) return null;
    if (val instanceof Date) return val.toISOString().split('T')[0];
    return String(val).split('T')[0];
}

// =========================================================================
// 1. LISTAR NOTAS DE RECEPCIÓN (HISTORIAL)
// =========================================================================
router.get('/', async (req, res) => {
    try {
        const page  = parseInt(req.query.page)  || 1;
        const limit = parseInt(req.query.limit) || 12;
        const { sede, doc_num, orden_compra, co_prov, fec_d, fec_h, search, status } = req.query;
        
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
                if (orden_compra) {
                    request.input('orden_compra', sql.VarChar, `%${orden_compra}%`);
                    whereClauses.push("EXISTS (SELECT 1 FROM saNotaRecepcionCompraReng r WHERE r.doc_num = c.doc_num AND r.num_doc LIKE @orden_compra)");
                }
                if (co_prov) {
                    request.input('co_prov_search', sql.VarChar, `%${co_prov}%`);
                    whereClauses.push("(c.co_prov LIKE @co_prov_search OR p.prov_des LIKE @co_prov_search OR p.rif LIKE @co_prov_search)");
                }
                if (search) {
                    request.input('search_all', sql.VarChar, `%${search}%`);
                    whereClauses.push(`(
                        c.doc_num LIKE @search_all 
                        OR p.prov_des LIKE @search_all 
                        OR c.co_prov LIKE @search_all 
                        OR p.rif LIKE @search_all
                        OR c.n_control LIKE @search_all
                        OR EXISTS (SELECT 1 FROM saNotaRecepcionCompraReng r WHERE r.doc_num = c.doc_num AND r.num_doc LIKE @search_all)
                    )`);
                }
                if (fec_d) {
                    request.input('fec_d', sql.SmallDateTime, new Date(`${fec_d}T00:00:00`));
                    whereClauses.push("c.fec_emis >= @fec_d");
                }
                if (fec_h) {
                    request.input('fec_h', sql.SmallDateTime, new Date(`${fec_h}T23:59:59`));
                    whereClauses.push("c.fec_emis <= @fec_h");
                }
                if (status !== undefined && status !== null && status !== '' && status !== 'all') {
                    if (status === 'anulado') {
                        whereClauses.push("c.anulado = 1");
                    } else if (status === 'activo') {
                        whereClauses.push("c.anulado = 0");
                    } else {
                        request.input('status_val', sql.Char(1), status);
                        whereClauses.push("c.status = @status_val AND c.anulado = 0");
                    }
                }

                const whereSQL = whereClauses.join(" AND ");

                const querySQL = `
                    SELECT 
                        RTRIM(c.doc_num) AS doc_num,
                        RTRIM(c.descrip) AS descrip,
                        RTRIM(c.co_prov) AS co_prov,
                        RTRIM(p.prov_des) AS prov_des,
                        RTRIM(p.rif) AS rif,
                        c.fec_emis,
                        c.fec_venc,
                        c.fec_reg,
                        c.fe_us_in AS fec_us_in,
                        c.fe_us_mo AS fec_us_mo,
                        c.anulado,
                        RTRIM(c.co_mone) AS co_mone,
                        c.tasa,
                        c.total_bruto,
                        c.monto_imp,
                        c.total_neto,
                        c.saldo,
                        RTRIM(c.n_control) AS n_control,
                        RTRIM(c.nro_fact) AS nro_fact,
                        RTRIM(c.campo8) AS campo8,
                        RTRIM(c.co_us_in) AS co_us_in,
                        RTRIM(c.co_us_mo) AS co_us_mo,
                        RTRIM(c.co_sucu_in) AS co_sucu_in,
                        (SELECT COUNT(*) FROM saNotaRecepcionCompraReng r WHERE r.doc_num = c.doc_num) AS cant_renglones,
                        (SELECT ISNULL(SUM(r.total_art), 0) FROM saNotaRecepcionCompraReng r WHERE r.doc_num = c.doc_num) AS total_unidades,
                        (
                            SELECT TOP 1 RTRIM(r.num_doc) 
                            FROM saNotaRecepcionCompraReng r 
                            WHERE r.doc_num = c.doc_num AND r.num_doc IS NOT NULL AND LTRIM(RTRIM(r.num_doc)) <> ''
                        ) AS orden_compra,
                        RTRIM(oc_info.oc_co_us_in) AS oc_co_us_in,
                        RTRIM(oc_info.oc_doc_num) AS oc_doc_num,
                        (
                            SELECT TOP 1 RTRIM(al.des_alma)
                            FROM saNotaRecepcionCompraReng r
                            LEFT JOIN saAlmacen al ON r.co_alma = al.co_alma
                            WHERE r.doc_num = c.doc_num
                        ) AS almacen_des,
                        CASE 
                            WHEN c.anulado = 1 THEN '3' -- Anulado
                            ELSE ISNULL(RTRIM(c.status), '0')
                        END AS status,
                        '${srv.name || srv.id}' AS sede_nombre,
                        '${srv.id}' AS sede_id
                    FROM saNotaRecepcionCompra c
                    LEFT JOIN saProveedor p ON c.co_prov = p.co_prov
                    OUTER APPLY (
                        SELECT TOP 1 oc.doc_num AS oc_doc_num, oc.co_us_in AS oc_co_us_in
                        FROM saOrdenCompra oc
                        WHERE oc.doc_num = (
                            SELECT TOP 1 nrr.num_doc 
                            FROM saNotaRecepcionCompraReng nrr 
                            WHERE nrr.doc_num = c.doc_num AND nrr.num_doc IS NOT NULL AND LTRIM(RTRIM(nrr.num_doc)) <> ''
                        ) OR (c.n_control IS NOT NULL AND oc.doc_num = c.n_control)
                    ) oc_info
                    WHERE ${whereSQL}
                    ORDER BY c.fec_emis DESC, c.doc_num DESC
                `;

                const result = await request.query(querySQL);
                return result.recordset || [];
            } catch (err) {
                console.error(`❌ [AGENT] Error en sede ${srv.name || srv.id} al consultar notas de recepción:`, err.message);
                return [];
            }
        }));

        const flattened = allData.flat();
        // Ordenar globalmente por fecha descendente
        flattened.sort((a, b) => new Date(b.fec_emis).getTime() - new Date(a.fec_emis).getTime());

        return paginatedResponse(res, flattened, page, limit);
    } catch (error) {
        console.error("❌ [AGENT] Error general en GET /notas-recepcion:", error);
        res.status(500).json({ success: false, message: 'Error interno del servidor.', error: error.message });
    }
});

// =========================================================================
// 2. BUSCAR ÓRDENES DE COMPRA PENDIENTES DE RECEPCIÓN
// =========================================================================
router.get('/ordenes-pendientes', async (req, res) => {
    try {
        const { sede, search, co_prov } = req.query;
        const servers = getServers();
        const targets = sede ? servers.filter(s => s.id === sede) : servers;

        const allOrders = await Promise.all(targets.map(async (srv) => {
            try {
                const pool = await getPool(srv.id, req.sqlAuth);
                const request = pool.request();
                let whereClauses = [
                    "c.anulado = 0",
                    "c.status IN ('0', '1')", // Sin procesar o parcialmente procesada
                    "EXISTS (SELECT 1 FROM saOrdenCompraReng r WHERE r.doc_num = c.doc_num AND r.pendiente > 0)"
                ];

                if (search) {
                    request.input('search', sql.VarChar, `%${search}%`);
                    whereClauses.push("(c.doc_num LIKE @search OR p.prov_des LIKE @search OR c.co_prov LIKE @search OR p.rif LIKE @search OR c.descrip LIKE @search)");
                }
                if (co_prov) {
                    request.input('co_prov', sql.VarChar, `%${co_prov}%`);
                    whereClauses.push("(c.co_prov LIKE @co_prov OR p.prov_des LIKE @co_prov OR p.rif LIKE @co_prov)");
                }

                const whereSQL = whereClauses.join(" AND ");

                const querySQL = `
                    SELECT 
                        RTRIM(c.doc_num) AS doc_num,
                        RTRIM(c.descrip) AS descrip,
                        RTRIM(c.co_prov) AS co_prov,
                        RTRIM(p.prov_des) AS prov_des,
                        RTRIM(p.rif) AS rif,
                        RTRIM(p.direc1) AS prov_dir,
                        RTRIM(p.telefonos) AS telefonos,
                        RTRIM(c.co_cond) AS co_cond,
                        ISNULL(NULLIF(RTRIM(cd.cond_des), ''), RTRIM(c.co_cond)) AS cond_des,
                        c.fec_emis,
                        c.fec_venc,
                        c.tasa,
                        RTRIM(c.co_mone) AS co_mone,
                        c.total_bruto,
                        c.monto_imp,
                        c.total_neto,
                        c.status,
                        (SELECT COUNT(*) FROM saOrdenCompraReng r WHERE r.doc_num = c.doc_num) AS total_renglones,
                        (SELECT COUNT(*) FROM saOrdenCompraReng r WHERE r.doc_num = c.doc_num AND r.pendiente > 0) AS renglones_pendientes,
                        (SELECT ISNULL(SUM(r.total_art), 0) FROM saOrdenCompraReng r WHERE r.doc_num = c.doc_num) AS cant_total,
                        (SELECT ISNULL(SUM(r.pendiente), 0) FROM saOrdenCompraReng r WHERE r.doc_num = c.doc_num) AS cant_pendiente,
                        '${srv.name || srv.id}' AS sede_nombre,
                        '${srv.id}' AS sede_id
                    FROM saOrdenCompra c
                    LEFT JOIN saProveedor p ON c.co_prov = p.co_prov
                    LEFT JOIN saCondicionPago cd ON c.co_cond = cd.co_cond
                    WHERE ${whereSQL}
                    ORDER BY c.fec_emis DESC, c.doc_num DESC
                `;

                const result = await request.query(querySQL);
                return result.recordset || [];
            } catch (err) {
                console.error(`❌ [AGENT] Error en sede ${srv.name || srv.id} al consultar órdenes pendientes:`, err.message);
                return [];
            }
        }));

        const flattened = allOrders.flat();
        flattened.sort((a, b) => new Date(b.fec_emis).getTime() - new Date(a.fec_emis).getTime());

        res.json({ success: true, data: flattened });
    } catch (error) {
        console.error("❌ [AGENT] Error en GET /ordenes-pendientes:", error);
        res.status(500).json({ success: false, message: 'Error interno del servidor.', error: error.message });
    }
});

// =========================================================================
// 3. CONSULTAR RENGLONES DE UNA ORDEN DE COMPRA PARA RECEPCIÓN
// =========================================================================
router.get('/ordenes-pendientes/:doc_num', async (req, res) => {
    try {
        const { doc_num } = req.params;
        const { sede } = req.query;

        const servers = getServers();
        const targets = sede ? servers.filter(s => s.id === sede) : servers;

        for (const srv of targets) {
            try {
                const pool = await getPool(srv.id, req.sqlAuth);

                // 1. Cabecera de la Orden
                const headRes = await pool.request()
                    .input('doc_num', sql.Char(20), padProfit(doc_num, 20))
                    .query(`
                        SELECT 
                            RTRIM(c.doc_num) AS doc_num,
                            RTRIM(c.descrip) AS descrip,
                            RTRIM(c.co_prov) AS co_prov,
                            RTRIM(p.prov_des) AS prov_des,
                            RTRIM(p.rif) AS rif,
                            RTRIM(p.direc1) AS prov_dir,
                            RTRIM(p.telefonos) AS telefonos,
                            RTRIM(p.email) AS email,
                            RTRIM(c.co_cond) AS co_cond,
                            ISNULL(NULLIF(RTRIM(cd.cond_des), ''), RTRIM(c.co_cond)) AS cond_des,
                            c.fec_emis,
                            c.fec_venc,
                            c.tasa,
                            RTRIM(c.co_mone) AS co_mone,
                            c.total_bruto,
                            c.monto_imp,
                            c.total_neto,
                            c.status,
                            c.anulado,
                            RTRIM(c.comentario) AS comentario,
                            '${srv.name || srv.id}' AS sede_nombre,
                            '${srv.id}' AS sede_id
                        FROM saOrdenCompra c
                        LEFT JOIN saProveedor p ON c.co_prov = p.co_prov
                        LEFT JOIN saCondicionPago cd ON c.co_cond = cd.co_cond
                        WHERE c.doc_num = @doc_num
                    `);

                if (!headRes.recordset || headRes.recordset.length === 0) continue;
                const orderHeader = headRes.recordset[0];

                // 2. Renglones con cantidades pendientes
                const rengRes = await pool.request()
                    .input('doc_num', sql.Char(20), padProfit(doc_num, 20))
                    .query(`
                        SELECT 
                            r.reng_num,
                            RTRIM(r.doc_num) AS doc_num,
                            RTRIM(r.co_art) AS co_art,
                            RTRIM(a.art_des) AS art_des,
                            RTRIM(a.modelo) AS modelo,
                            RTRIM(a.ref) AS referencia,
                            RTRIM(r.co_uni) AS co_uni,
                            COALESCE(NULLIF(RTRIM(u.des_uni), ''), RTRIM(r.co_uni)) AS unidad,
                            RTRIM(r.co_alma) AS co_alma_original,
                            RTRIM(al.des_alma) AS des_alma_original,
                            r.total_art AS cant_original,
                            r.pendiente AS cant_pendiente,
                            r.cost_unit,
                            r.cost_unit_om,
                            r.tipo_imp,
                            r.porc_imp,
                            r.monto_imp,
                            r.reng_neto,
                            r.rowguid AS rowguid_doc
                        FROM saOrdenCompraReng r
                        LEFT JOIN saArticulo a ON r.co_art = a.co_art
                        LEFT JOIN saUnidad u ON r.co_uni = u.co_uni
                        LEFT JOIN saAlmacen al ON r.co_alma = al.co_alma
                        WHERE r.doc_num = @doc_num AND r.pendiente > 0
                        ORDER BY r.reng_num ASC
                    `);

                return res.json({
                    success: true,
                    data: {
                        ...orderHeader,
                        renglones: rengRes.recordset || []
                    }
                });
            } catch (err) {
                console.error(`❌ [AGENT] Error en sede ${srv.name || srv.id} al consultar detalle de orden pendiente:`, err.message);
            }
        }

        return res.status(404).json({ success: false, message: `Orden de compra ${doc_num} no encontrada.` });
    } catch (error) {
        console.error("❌ [AGENT] Error en GET /ordenes-pendientes/:doc_num:", error);
        res.status(500).json({ success: false, message: 'Error interno del servidor.', error: error.message });
    }
});

// =========================================================================
// 4. CONSULTAR DETALLE DE UNA NOTA DE RECEPCIÓN ESPECÍFICA
// =========================================================================
router.get('/:doc_num', async (req, res) => {
    try {
        const { doc_num } = req.params;
        const { sede } = req.query;

        const servers = getServers();
        const targets = sede ? servers.filter(s => s.id === sede) : servers;

        for (const srv of targets) {
            try {
                const pool = await getPool(srv.id, req.sqlAuth);

                // 1. Cabecera de la Nota de Recepción
                const headRes = await pool.request()
                    .input('doc_num', sql.Char(20), padProfit(doc_num, 20))
                    .query(`
                        SELECT 
                            RTRIM(c.doc_num) AS doc_num,
                            RTRIM(c.descrip) AS descrip,
                            RTRIM(c.nro_fact) AS nro_fact,
                            RTRIM(c.n_control) AS n_control,
                            RTRIM(c.co_prov) AS co_prov,
                            RTRIM(p.prov_des) AS prov_des,
                            RTRIM(p.rif) AS rif,
                            RTRIM(p.direc1) AS prov_dir,
                            RTRIM(p.telefonos) AS telefonos,
                            RTRIM(p.email) AS email,
                            RTRIM(c.co_cond) AS co_cond,
                            ISNULL(NULLIF(RTRIM(cd.cond_des), ''), RTRIM(c.co_cond)) AS cond_des,
                            c.fec_emis,
                            c.fec_venc,
                            c.fec_reg,
                            c.fe_us_in AS fec_us_in,
                            c.fe_us_mo AS fec_us_mo,
                            c.anulado,
                            RTRIM(c.co_mone) AS co_mone,
                            c.tasa,
                            c.total_bruto,
                            c.monto_imp,
                            c.total_neto,
                            c.saldo,
                            RTRIM(c.comentario) AS comentario,
                            RTRIM(c.campo8) AS campo8,
                            RTRIM(c.co_us_in) AS co_us_in,
                            RTRIM(c.co_us_mo) AS co_us_mo,
                            RTRIM(c.co_sucu_in) AS co_sucu_in,
                            (
                                SELECT TOP 1 RTRIM(r.num_doc) 
                                FROM saNotaRecepcionCompraReng r 
                                WHERE r.doc_num = c.doc_num AND r.num_doc IS NOT NULL AND LTRIM(RTRIM(r.num_doc)) <> ''
                            ) AS orden_compra,
                            RTRIM(oc_info.oc_co_us_in) AS oc_co_us_in,
                            RTRIM(oc_info.oc_doc_num) AS oc_doc_num,
                            '${srv.name || srv.id}' AS sede_nombre,
                            '${srv.id}' AS sede_id
                        FROM saNotaRecepcionCompra c
                        LEFT JOIN saProveedor p ON c.co_prov = p.co_prov
                        LEFT JOIN saCondicionPago cd ON c.co_cond = cd.co_cond
                        OUTER APPLY (
                            SELECT TOP 1 oc.doc_num AS oc_doc_num, oc.co_us_in AS oc_co_us_in
                            FROM saOrdenCompra oc
                            WHERE oc.doc_num = (
                                SELECT TOP 1 nrr.num_doc 
                                FROM saNotaRecepcionCompraReng nrr 
                                WHERE nrr.doc_num = c.doc_num AND nrr.num_doc IS NOT NULL AND LTRIM(RTRIM(nrr.num_doc)) <> ''
                            ) OR (c.n_control IS NOT NULL AND oc.doc_num = c.n_control)
                        ) oc_info
                        WHERE c.doc_num = @doc_num
                    `);

                if (!headRes.recordset || headRes.recordset.length === 0) continue;
                const header = headRes.recordset[0];

                // 2. Renglones
                const rengRes = await pool.request()
                    .input('doc_num', sql.Char(20), padProfit(doc_num, 20))
                    .query(`
                        SELECT 
                            r.reng_num,
                            RTRIM(r.doc_num) AS doc_num,
                            RTRIM(r.co_art) AS co_art,
                            RTRIM(a.art_des) AS art_des,
                            RTRIM(a.modelo) AS modelo,
                            RTRIM(a.ref) AS referencia,
                            RTRIM(r.co_uni) AS co_uni,
                            COALESCE(NULLIF(RTRIM(u.des_uni), ''), RTRIM(r.co_uni)) AS unidad,
                            RTRIM(r.co_alma) AS co_alma,
                            RTRIM(al.des_alma) AS almacen_des,
                            r.total_art AS cantidad,
                            r.cost_unit,
                            r.cost_unit_om,
                            r.tipo_imp,
                            r.porc_imp,
                            r.monto_imp,
                            r.reng_neto,
                            r.pendiente,
                            RTRIM(r.tipo_doc) AS tipo_doc,
                            RTRIM(r.num_doc) AS num_doc,
                            r.rowguid_doc,
                            ISNULL(ocr.total_art, r.total_art) AS cant_original,
                            ISNULL(ocr.pendiente + r.total_art, r.total_art) AS cant_pendiente
                        FROM saNotaRecepcionCompraReng r
                        LEFT JOIN saArticulo a ON r.co_art = a.co_art
                        LEFT JOIN saUnidad u ON r.co_uni = u.co_uni
                        LEFT JOIN saAlmacen al ON r.co_alma = al.co_alma
                        LEFT JOIN saOrdenCompraReng ocr ON r.num_doc = ocr.doc_num AND (r.rowguid_doc = ocr.rowguid OR r.co_art = ocr.co_art)
                        WHERE r.doc_num = @doc_num
                        ORDER BY r.reng_num ASC
                    `);

                return res.json({
                    success: true,
                    data: {
                        ...header,
                        renglones: rengRes.recordset || []
                    }
                });
            } catch (err) {
                console.error(`❌ [AGENT] Error en sede ${srv.name || srv.id} al consultar nota de recepción:`, err.message);
            }
        }

        return res.status(404).json({ success: false, message: `Nota de recepción ${doc_num} no encontrada.` });
    } catch (error) {
        console.error("❌ [AGENT] Error en GET /notas-recepcion/:doc_num:", error);
        res.status(500).json({ success: false, message: 'Error interno del servidor.', error: error.message });
    }
});

// =========================================================================
// 5. CREAR / PROCESAR NOTA DE RECEPCIÓN
// =========================================================================
router.post('/', async (req, res) => {
    const { sede } = req.query;
    const data = req.body;
    const auditUser = (req.profitUser || data.co_us_in || 'SYNC2K').substring(0, 6).toUpperCase();

    if (!data.co_prov) {
        return res.status(400).json({ success: false, message: "El código de proveedor (co_prov) es obligatorio." });
    }
    if (!data.renglones || !Array.isArray(data.renglones) || data.renglones.length === 0) {
        return res.status(400).json({ success: false, message: "Debe incluir al menos un renglón para recepcionar." });
    }

    try {
        const results = await executeWrite(sede, req.sqlAuth, async (pool, srv) => {
            const transaction = new sql.Transaction(pool);
            await transaction.begin();

            try {
                // 1. Obtener sucursal y almacén por defecto de la sede
                let defSucu = '01';
                let defAlma = '01';

                try {
                    const sucuRes = await transaction.request().query('SELECT TOP 1 RTRIM(co_sucu) as co_sucu FROM saSucursal');
                    if (sucuRes.recordset?.length > 0) defSucu = sucuRes.recordset[0].co_sucu;
                } catch (e) {}

                try {
                    const almaRes = await transaction.request().query("SELECT TOP 1 RTRIM(co_alma) as co_alma FROM saAlmacen WHERE campo1 = 'DEFAULT' OR co_alma = '01' ORDER BY co_alma ASC");
                    if (almaRes.recordset?.length > 0) defAlma = almaRes.recordset[0].co_alma;
                } catch (e) {}

                const sucuCode = data.co_sucu_in || defSucu;
                const almaDestino = data.co_alma_defecto || defAlma;

                // 2. Configurar Moneda y Tasa
                const resTasa = await transaction.request().query(`
                    SELECT TOP 1 tasa_v FROM saTasa 
                    WHERE LTRIM(RTRIM(co_mone)) IN ('USD', 'US$', 'DOL', '$', 'US') 
                    ORDER BY fecha DESC
                `);
                const currentTasa = resTasa.recordset[0]?.tasa_v || 1;
                const isUSD = data.showUSD === true || String(data.co_mone || '').includes('US');
                const tasaDoc = Number(data.tasa || currentTasa) > 1 ? Number(data.tasa || currentTasa) : currentTasa;
                const docMone = isUSD ? 'USD' : 'BS';

                // 3. Generar o resguardar Correlativo de Nota de Recepción
                let docNum = (data.doc_num || '').trim();
                const isUpdate = !!(docNum && data.isEditing);
                let existingHeader = null;

                if (isUpdate) {
                    // --- MODO EDICIÓN (UPDATE) ---
                    console.log(`🔄 [AGENT] Resguardando datos de Nota de Recepción existente: ${docNum}`);
                    const resPre = await transaction.request()
                        .input('doc_num', sql.Char(20), padProfit(docNum, 20))
                        .query(`SELECT TOP 1 * FROM saNotaRecepcionCompra WHERE doc_num = @doc_num`);

                    if (resPre.recordset.length === 0) {
                        throw new Error(`La nota de recepción ${docNum} no existe para ser editada.`);
                    }

                    existingHeader = resPre.recordset[0];
                    if (existingHeader.anulado) {
                        throw new Error(`La nota de recepción ${docNum} está anulada y no puede ser editada.`);
                    }
                    if (String(existingHeader.status || '').trim() !== '0') {
                        throw new Error(`La nota de recepción ${docNum} ya fue procesada/facturada y no puede ser editada.`);
                    }

                    // Revertir stock y saldos pendientes de renglones anteriores
                    const oldRengRes = await transaction.request()
                        .input('doc_num', sql.Char(20), padProfit(docNum, 20))
                        .query(`SELECT * FROM saNotaRecepcionCompraReng WHERE doc_num = @doc_num`);

                    for (const oldItem of oldRengRes.recordset) {
                        const oldQty = Number(oldItem.total_art || 0);
                        const oldAlma = oldItem.co_alma;
                        const oldArt = oldItem.co_art;
                        const oldOC = (oldItem.num_doc || '').trim();

                        // Restar de ACT y devolver a LLE
                        await transaction.request()
                            .input('art', sql.Char(30), padProfit(oldArt, 30))
                            .input('alma', sql.Char(6), padProfit(oldAlma, 6))
                            .input('qty', sql.Decimal(18, 5), oldQty)
                            .query(`
                                IF EXISTS (SELECT 1 FROM saStockAlmacen WHERE co_art = @art AND co_alma = @alma AND tipo = 'ACT')
                                BEGIN
                                    UPDATE saStockAlmacen 
                                    SET stock = CASE WHEN stock >= @qty THEN stock - @qty ELSE 0 END 
                                    WHERE co_art = @art AND co_alma = @alma AND tipo = 'ACT'
                                END
                                IF EXISTS (SELECT 1 FROM saStockAlmacen WHERE co_art = @art AND co_alma = @alma AND tipo = 'LLE')
                                BEGIN
                                    UPDATE saStockAlmacen 
                                    SET stock = stock + @qty 
                                    WHERE co_art = @art AND co_alma = @alma AND tipo = 'LLE'
                                END
                                ELSE
                                BEGIN
                                    INSERT INTO saStockAlmacen (co_art, co_alma, tipo, stock, revisado, trasnfe) 
                                    VALUES (@art, @alma, 'LLE', @qty, NULL, NULL)
                                END
                            `);

                        // Restaurar saldo pendiente en OC
                        if (oldOC) {
                            if (oldItem.rowguid_doc) {
                                await transaction.request()
                                    .input('qty', sql.Decimal(18, 5), oldQty)
                                    .input('oc_doc', sql.Char(20), padProfit(oldOC, 20))
                                    .input('rowguid_oc', sql.UniqueIdentifier, oldItem.rowguid_doc)
                                    .input('auditUser', sql.Char(6), padProfit(auditUser, 6))
                                    .query(`
                                        UPDATE saOrdenCompraReng
                                        SET pendiente = CASE WHEN (pendiente + @qty) <= total_art THEN (pendiente + @qty) ELSE total_art END,
                                            fe_us_mo = GETDATE(),
                                            co_us_mo = @auditUser
                                        WHERE doc_num = @oc_doc AND rowguid = @rowguid_oc
                                    `);
                            } else {
                                await transaction.request()
                                    .input('qty', sql.Decimal(18, 5), oldQty)
                                    .input('oc_doc', sql.Char(20), padProfit(oldOC, 20))
                                    .input('art', sql.Char(30), padProfit(oldItem.co_art, 30))
                                    .input('auditUser', sql.Char(6), padProfit(auditUser, 6))
                                    .query(`
                                        UPDATE saOrdenCompraReng
                                        SET pendiente = CASE WHEN (pendiente + @qty) <= total_art THEN (pendiente + @qty) ELSE total_art END,
                                            fe_us_mo = GETDATE(),
                                            co_us_mo = @auditUser
                                        WHERE doc_num = @oc_doc AND co_art = @art
                                    `);
                            }
                        }
                    }

                    // Borrar renglones anteriores
                    await transaction.request()
                        .input('doc_num', sql.Char(20), padProfit(docNum, 20))
                        .query(`DELETE FROM saNotaRecepcionCompraReng WHERE doc_num = @doc_num`);
                } else if (!docNum) {
                    const corrRes = await getProximoConsecutivo({
                        runner: transaction,
                        co_tipo_serie: 'NOTA_RECEPCION',
                        co_sucur: defSucu
                    });
                    docNum = corrRes.docNum;
                    console.log(`✨ [AGENT] Nuevo número generado para Nota de Recepción: ${docNum} (Prefijo: '${corrRes.prefijo}', ProxN: ${corrRes.proxN})`);
                }

                console.log(`✨ [AGENT] ${isUpdate ? 'Actualizando' : 'Generando'} Nota de Recepción N°: ${docNum} en sede ${srv.name || srv.id}`);

                // 4. Calcular Totales
                let totalBrutoBs = 0;
                let totalImpBs = 0;

                const validRenglones = [];
                for (const it of data.renglones) {
                    const qty = parseFloat(it.cantidad || it.cant_recibida || 0);
                    if (qty <= 0) continue; // Omitir renglones con cantidad cero

                    const unitCostUSD = parseFloat(it.cost_unit_om || it.cost_unit || it.precio || 0);
                    const unitCostBs = isUSD ? (unitCostUSD * tasaDoc) : unitCostUSD;
                    const pImp = parseFloat(it.porc_imp != null ? it.porc_imp : 0);

                    const subBs = Math.round((qty * unitCostBs) * 100) / 100;
                    const impBs = Math.round(((subBs * pImp) / 100) * 100) / 100;

                    totalBrutoBs += subBs;
                    totalImpBs += impBs;

                    validRenglones.push({
                        ...it,
                        qty,
                        unitCostUSD,
                        unitCostBs,
                        subBs,
                        impBs,
                        pImp
                    });
                }

                if (validRenglones.length === 0) {
                    throw new Error("No hay renglones válidos con cantidad mayor a 0 para procesar.");
                }

                const totalNetoBs = Math.round((totalBrutoBs + totalImpBs) * 100) / 100;
                const tsDate = data.fec_emis ? new Date(`${safeDate(data.fec_emis)}T12:00:00`) : new Date();

                // Obtener días de crédito según condición de pago para vencimiento exacto
                let diasCred = 0;
                try {
                    const condRes = await transaction.request()
                        .input('co_cond', sql.Char(6), padProfit(data.co_cond || 'CONT', 6))
                        .query('SELECT dias_cred FROM saCondicionPago WHERE co_cond = @co_cond');
                    diasCred = Number(condRes.recordset[0]?.dias_cred || 0);
                } catch (e) {
                    diasCred = 0;
                }

                const tsVenc = new Date(tsDate.getTime() + (diasCred * 24 * 60 * 60 * 1000));

                // 5. Insertar o Actualizar Cabecera de Nota de Recepción
                if (isUpdate) {
                    const rH = new sql.Request(transaction);
                    rH.input('sDoc_Num',          sql.Char(20),         padProfit(docNum, 20));
                    rH.input('sDoc_NumOri',       sql.Char(20),         padProfit(docNum, 20));
                    rH.input('sNro_Fact',         sql.Char(20),         padProfit(data.nro_fact || data.n_control || docNum, 20));
                    rH.input('sDescrip',          sql.VarChar(60),      (data.descrip || `RECEPCION OC ${data.doc_num_oc || ''}`).substring(0, 60));
                    rH.input('sCo_Prov',          sql.Char(16),         padProfit(data.co_prov, 16));
                    rH.input('sCo_Cta_Ingr_Egr',  sql.Char(20),         null);
                    rH.input('sCo_Mone',          sql.Char(6),          padProfit(docMone, 6));
                    rH.input('sCo_Cond',          sql.Char(6),          padProfit(data.co_cond || 'CONT', 6));
                    rH.input('sPorc_Desc_Glob',   sql.Char(15),         '0');
                    rH.input('sPorc_Reca',        sql.Char(15),         null);
                    rH.input('sStatus',           sql.Char(1),          '0');
                    rH.input('sN_Control',        sql.Char(20),         padProfit(data.n_control || docNum, 20));
                    rH.input('sdFec_Emis',        sql.SmallDateTime,    existingHeader.fec_emis || tsDate);
                    rH.input('sdFec_Venc',        sql.SmallDateTime,    tsVenc);
                    rH.input('sdFec_Reg',         sql.SmallDateTime,    existingHeader.fec_reg || tsDate);
                    rH.input('deTasa',            sql.Decimal(21, 8),   tasaDoc);
                    rH.input('deSaldo',           sql.Decimal(18, 2),   totalNetoBs);
                    rH.input('deTotal_Bruto',     sql.Decimal(18, 2),   totalBrutoBs);
                    rH.input('deTotal_Neto',      sql.Decimal(18, 2),   totalNetoBs);
                    rH.input('deMonto_Desc_Glob', sql.Decimal(18, 2),   0);
                    rH.input('deMonto_Reca',      sql.Decimal(18, 2),   0);
                    rH.input('deOtros1',          sql.Decimal(18, 2),   0);
                    rH.input('deOtros2',          sql.Decimal(18, 2),   0);
                    rH.input('deOtros3',          sql.Decimal(18, 2),   0);
                    rH.input('deMonto_Imp',       sql.Decimal(18, 2),   totalImpBs);
                    rH.input('deMonto_Imp2',      sql.Decimal(18, 2),   0);
                    rH.input('deMonto_Imp3',      sql.Decimal(18, 2),   0);
                    rH.input('bAnulado',          sql.Bit,              0);
                    rH.input('bImpresa',          sql.Bit,              0);
                    rH.input('sSalestax',         sql.Char(8),          null);
                    rH.input('sDis_Cen',          sql.VarChar(sql.MAX), null);
                    rH.input('sDir_Ent',          sql.VarChar(sql.MAX), data.dir_ent || null);
                    rH.input('sComentario',       sql.VarChar(sql.MAX), (data.comentario || '').trim().substring(0, 500) || null);
                    rH.input('sCampo1',           sql.VarChar(60),      data.campo1 || null);
                    rH.input('sCampo2',           sql.VarChar(60),      data.campo2 || null);
                    rH.input('sCampo3',           sql.VarChar(60),      data.campo3 || null);
                    rH.input('sCampo4',           sql.VarChar(60),      data.campo4 || null);
                    rH.input('sCampo5',           sql.VarChar(60),      data.campo5 || null);
                    rH.input('sCampo6',           sql.VarChar(60),      data.campo6 || null);
                    rH.input('sCampo7',           sql.VarChar(60),      data.campo7 || null);
                    rH.input('sCampo8',           sql.VarChar(60),      'Editado vía API');
                    rH.input('sCo_Us_Mo',         sql.Char(6),          padProfit(auditUser, 6));
                    rH.input('sCo_Sucu_Mo',       sql.Char(6),          padProfit(sucuCode, 6));
                    rH.input('sRevisado',         sql.Char(1),          null);
                    rH.input('sTrasnfe',          sql.Char(1),          null);
                    rH.input('sMaquina',          sql.VarChar(60),      'SYNC2K');
                    rH.input('tsValidador',       sql.VarBinary,        existingHeader.validador);
                    rH.input('sCampos',           sql.VarChar(sql.MAX), null);
                    rH.input('gRowguid',          sql.UniqueIdentifier, existingHeader.rowguid);
                    rH.input('bNac',              sql.Bit,              1);

                    await rH.execute('pActualizarNotaRecepcionCompra');
                } else {
                    const rH = new sql.Request(transaction);
                    rH.input('sDoc_Num',          sql.Char(20),         padProfit(docNum, 20));
                    rH.input('sNro_Fact',         sql.Char(20),         padProfit(data.nro_fact || data.n_control || docNum, 20));
                    rH.input('sDescrip',          sql.VarChar(60),      (data.descrip || `RECEPCION OC ${data.doc_num_oc || ''}`).substring(0, 60));
                    rH.input('sCo_Prov',          sql.Char(16),         padProfit(data.co_prov, 16));
                    rH.input('sCo_Cta_Ingr_Egr',  sql.Char(20),         null);
                    rH.input('sCo_Mone',          sql.Char(6),          padProfit(docMone, 6));
                    rH.input('sCo_Cond',          sql.Char(6),          padProfit(data.co_cond || 'CONT', 6));
                    rH.input('sN_Control',        sql.Char(20),         padProfit(data.n_control || docNum, 20));
                    rH.input('sPorc_Desc_Glob',   sql.Char(15),         '0');
                    rH.input('sdFec_Emis',        sql.SmallDateTime,    tsDate);
                    rH.input('sdFec_Venc',        sql.SmallDateTime,    tsVenc);
                    rH.input('sdFec_Reg',         sql.SmallDateTime,    tsDate);
                    rH.input('bAnulado',          sql.Bit,              0);
                    rH.input('sStatus',           sql.Char(1),          '0'); // 0: Sin procesar / pendiente para factura
                    rH.input('deTasa',            sql.Decimal(21, 8),   tasaDoc);
                    rH.input('sPorc_Reca',        sql.Char(15),         null);
                    rH.input('deSaldo',           sql.Decimal(18, 2),   totalNetoBs);
                    rH.input('deTotal_Bruto',     sql.Decimal(18, 2),   totalBrutoBs);
                    rH.input('deTotal_Neto',      sql.Decimal(18, 2),   totalNetoBs);
                    rH.input('deMonto_Desc_Glob', sql.Decimal(18, 2),   0);
                    rH.input('deMonto_Reca',      sql.Decimal(18, 2),   0);
                    rH.input('deOtros1',          sql.Decimal(18, 2),   0);
                    rH.input('deOtros2',          sql.Decimal(18, 2),   0);
                    rH.input('deOtros3',          sql.Decimal(18, 2),   0);
                    rH.input('deMonto_Imp',       sql.Decimal(18, 2),   totalImpBs);
                    rH.input('deMonto_Imp2',      sql.Decimal(18, 2),   0);
                    rH.input('deMonto_Imp3',      sql.Decimal(18, 2),   0);
                    rH.input('sDir_Ent',          sql.VarChar(sql.MAX), data.dir_ent || null);
                    rH.input('sComentario',       sql.VarChar(sql.MAX), (data.comentario || '').trim().substring(0, 500) || null);
                    rH.input('bImpresa',          sql.Bit,              0);
                    rH.input('sSalestax',         sql.Char(8),          null);
                    rH.input('sDis_Cen',          sql.VarChar(sql.MAX), null);
                    rH.input('sCampo1',           sql.VarChar(60),      data.campo1 || null);
                    rH.input('sCampo2',           sql.VarChar(60),      data.campo2 || null);
                    rH.input('sCampo3',           sql.VarChar(60),      data.campo3 || null);
                    rH.input('sCampo4',           sql.VarChar(60),      data.campo4 || null);
                    rH.input('sCampo5',           sql.VarChar(60),      data.campo5 || null);
                    rH.input('sCampo6',           sql.VarChar(60),      data.campo6 || null);
                    rH.input('sCampo7',           sql.VarChar(60),      data.campo7 || null);
                    rH.input('sCampo8',           sql.VarChar(60),      'Creado vía API');
                    rH.input('sRevisado',         sql.Char(1),          null);
                    rH.input('sTrasnfe',          sql.Char(1),          null);
                    rH.input('sCo_Us_In',         sql.Char(6),          padProfit(auditUser, 6));
                    rH.input('sCo_Sucu_In',       sql.Char(6),          padProfit(sucuCode, 6));
                    rH.input('sMaquina',          sql.VarChar(60),      'SYNC2K');
                    rH.input('bNac',              sql.Bit,              1);

                    await rH.execute('pInsertarNotaRecepcionCompra');
                }

                // 6. Insertar Renglones de la Nota de Recepción (pInsertarRenglonesNotaRecepcionCompra)
                // y Actualizar Stock / Descontar Pendiente en Orden de Compra de Origen
                const touchedOriginOrders = new Set();

                for (let i = 0; i < validRenglones.length; i++) {
                    const item = validRenglones[i];
                    const targetAlma = item.co_alma || almaDestino;
                    const originDocNum = (item.num_doc || data.doc_num_oc || '').trim();

                    let finalUni = String(item.co_uni || '').trim();
                    try {
                        const uniRes = await transaction.request()
                            .input('art', sql.Char(30), padProfit(item.co_art, 30))
                            .input('uni', sql.Char(6), padProfit(finalUni, 6))
                            .query(`
                                SELECT TOP 1 RTRIM(co_uni) as co_uni 
                                FROM saArtUnidad 
                                WHERE co_art = @art 
                                ORDER BY CASE WHEN RTRIM(co_uni) = RTRIM(@uni) THEN 0 WHEN uni_principal = 1 THEN 1 ELSE 2 END
                            `);
                        if (uniRes.recordset?.length > 0) {
                            finalUni = uniRes.recordset[0].co_uni;
                        }
                    } catch (e) {}

                    if (!finalUni) finalUni = '01';
                    const originRengNum = parseInt(item.reng_num_oc || item.reng_num || i + 1);
                    const originRowGuid = item.rowguid_doc || null;

                    if (originDocNum) touchedOriginOrders.add(originDocNum);

                    const rL = new sql.Request(transaction);
                    rL.input('iReng_Num',          sql.Int,              i + 1);
                    rL.input('sDoc_Num',           sql.Char(20),         padProfit(docNum, 20));
                    rL.input('sCo_Art',            sql.Char(30),         padProfit(item.co_art, 30));
                    rL.input('sDes_Art',           sql.VarChar(120),     null);
                    rL.input('sCo_Uni',            sql.Char(6),          padProfit(finalUni, 6));
                    rL.input('sSCo_Uni',           sql.Char(6),          null);
                    rL.input('sCo_Alma',           sql.Char(6),          padProfit(targetAlma, 6));
                    rL.input('sTipo_Imp',          sql.Char(1),          item.tipo_imp || '1');
                    rL.input('sTipo_Imp2',         sql.Char(1),          null);
                    rL.input('sTipo_Imp3',         sql.Char(1),          null);
                    rL.input('sTipo_Doc',          sql.Char(4),          originDocNum ? 'OCOM' : null);
                    rL.input('sPorc_Desc',         sql.Char(15),         null);
                    rL.input('sNum_Doc',           sql.Char(20),         originDocNum ? padProfit(originDocNum, 20) : null);
                    rL.input('gRowGuid_Doc',       sql.UniqueIdentifier, originRowGuid);
                    rL.input('deReng_Neto',        sql.Decimal(18, 2),   item.subBs);
                    rL.input('deCost_Unit',        sql.Decimal(18, 5),   item.unitCostBs);
                    rL.input('deCost_Unit_OM',     sql.Decimal(18, 5),   item.unitCostUSD);
                    rL.input('deTotal_Art',        sql.Decimal(18, 5),   item.qty);
                    rL.input('deSTotal_Art',       sql.Decimal(18, 5),   0);
                    rL.input('deOtros',            sql.Decimal(18, 5),   0);
                    rL.input('dePorc_Imp',         sql.Decimal(18, 5),   item.pImp);
                    rL.input('dePorc_Imp2',        sql.Decimal(18, 5),   0);
                    rL.input('dePorc_Imp3',        sql.Decimal(18, 5),   0);
                    rL.input('deMonto_Imp',        sql.Decimal(18, 5),   item.impBs);
                    rL.input('deMonto_Imp2',       sql.Decimal(18, 5),   0);
                    rL.input('deMonto_Imp3',       sql.Decimal(18, 5),   0);
                    rL.input('dePorc_Gas',         sql.Decimal(18, 2),   0);
                    rL.input('deTotal_Dev',        sql.Decimal(18, 5),   0);
                    rL.input('deMonto_Dev',        sql.Decimal(18, 5),   0);
                    rL.input('dePendiente2',       sql.Decimal(18, 5),   0);
                    rL.input('sComentario',        sql.VarChar(sql.MAX), item.comentario || null);
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
                    rL.input('dePendiente',        sql.Decimal(18, 5),   item.qty); // Pendiente para factura
                    rL.input('iReng_Doc',          sql.Int,              originRengNum);
                    rL.input('sDis_Cen',           sql.VarChar(sql.MAX), null);
                    rL.input('sCo_Sucu_In',        sql.Char(6),          padProfit(sucuCode, 6));
                    rL.input('sCo_Us_In',          sql.Char(6),          padProfit(auditUser, 6));
                    rL.input('sRevisado',          sql.Char(1),          null);
                    rL.input('sTrasnfe',           sql.Char(1),          null);
                    rL.input('sMaquina',           sql.VarChar(60),      'SYNC2K');
                    rL.input('deCosto_Adi1',       sql.Decimal(18, 5),   0);
                    rL.input('deCosto_Adi2',       sql.Decimal(18, 5),   0);
                    rL.input('deCosto_Adi3',       sql.Decimal(18, 5),   0);

                    await rL.execute('pInsertarRenglonesNotaRecepcionCompra');

                    // 7. INGRESO DE STOCK AL ALMACÉN (saStockAlmacen)
                    // Incrementar Stock Actual (tipo = 'ACT')
                    await transaction.request()
                        .input('art', sql.Char(30), padProfit(item.co_art, 30))
                        .input('alma', sql.Char(6), padProfit(targetAlma, 6))
                        .input('qty', sql.Decimal(18, 5), item.qty)
                        .query(`
                            IF EXISTS (SELECT 1 FROM saStockAlmacen WHERE co_art = @art AND co_alma = @alma AND tipo = 'ACT')
                            BEGIN
                                UPDATE saStockAlmacen 
                                SET stock = stock + @qty
                                WHERE co_art = @art AND co_alma = @alma AND tipo = 'ACT'
                            END
                            ELSE
                            BEGIN
                                INSERT INTO saStockAlmacen (co_art, co_alma, tipo, stock, revisado, trasnfe)
                                VALUES (@art, @alma, 'ACT', @qty, NULL, NULL)
                            END
                        `);

                    // Reducir Stock Por Llegar (tipo = 'LLE') si existía
                    await transaction.request()
                        .input('art', sql.Char(30), padProfit(item.co_art, 30))
                        .input('alma', sql.Char(6), padProfit(targetAlma, 6))
                        .input('qty', sql.Decimal(18, 5), item.qty)
                        .query(`
                            IF EXISTS (SELECT 1 FROM saStockAlmacen WHERE co_art = @art AND co_alma = @alma AND tipo = 'LLE')
                            BEGIN
                                UPDATE saStockAlmacen 
                                SET stock = CASE WHEN stock >= @qty THEN stock - @qty ELSE 0 END
                                WHERE co_art = @art AND co_alma = @alma AND tipo = 'LLE'
                            END
                        `);

                    // 8. DESCONTAR PENDIENTE DEL RENGLÓN DE LA ORDEN DE COMPRA DE ORIGEN
                    if (originDocNum) {
                        const rOCLine = transaction.request();
                        rOCLine.input('qty', sql.Decimal(18, 5), item.qty);
                        rOCLine.input('oc_doc', sql.Char(20), padProfit(originDocNum, 20));
                        rOCLine.input('auditUser', sql.Char(6), padProfit(auditUser, 6));

                        if (originRowGuid) {
                            rOCLine.input('rowguid_oc', sql.UniqueIdentifier, originRowGuid);
                            await rOCLine.query(`
                                UPDATE saOrdenCompraReng
                                SET pendiente = CASE WHEN pendiente >= @qty THEN pendiente - @qty ELSE 0 END,
                                    fe_us_mo = GETDATE(),
                                    co_us_mo = @auditUser
                                WHERE doc_num = @oc_doc AND rowguid = @rowguid_oc
                            `);
                        } else {
                            rOCLine.input('reng_oc', sql.Int, originRengNum);
                            await rOCLine.query(`
                                UPDATE saOrdenCompraReng
                                SET pendiente = CASE WHEN pendiente >= @qty THEN pendiente - @qty ELSE 0 END,
                                    fe_us_mo = GETDATE(),
                                    co_us_mo = @auditUser
                                WHERE doc_num = @oc_doc AND reng_num = @reng_oc
                            `);
                        }
                    }
                }

                // 9. ACTUALIZAR ESTADO DE CADA ORDEN DE COMPRA TOCADA
                for (const ocNum of touchedOriginOrders) {
                    console.log(`🔄 [AGENT] Recalculando estado de la Orden de Compra ${ocNum}...`);
                    await transaction.request()
                        .input('oc_doc', sql.Char(20), padProfit(ocNum, 20))
                        .input('auditUser', sql.Char(6), padProfit(auditUser, 6))
                        .query(`
                            DECLARE @totalPendiente DECIMAL(18, 5)
                            DECLARE @totalArt DECIMAL(18, 5)

                            SELECT 
                                @totalPendiente = ISNULL(SUM(pendiente), 0),
                                @totalArt = ISNULL(SUM(total_art), 0)
                            FROM saOrdenCompraReng 
                            WHERE doc_num = @oc_doc

                            UPDATE saOrdenCompra
                            SET status = CASE 
                                WHEN @totalPendiente = 0 THEN '2' -- Procesada Totalmente
                                WHEN @totalPendiente < @totalArt THEN '1' -- Procesada Parcialmente
                                ELSE '0' -- Sin Procesar
                            END,
                            fe_us_mo = GETDATE(),
                            co_us_mo = @auditUser
                            WHERE doc_num = @oc_doc
                        `);
                }

                await transaction.commit();
                console.log(`✅ [AGENT] Nota de Recepción ${docNum} creada con éxito en sede ${srv.name || srv.id}`);

                return {
                    doc_num: docNum,
                    co_prov: data.co_prov,
                    total_neto: totalNetoBs,
                    total_art: validRenglones.reduce((acc, it) => acc + it.qty, 0),
                    almacen_ingreso: almaDestino,
                    sede_id: srv.id,
                    sede_nombre: srv.name || srv.id
                };
            } catch (innerErr) {
                await transaction.rollback();
                throw innerErr;
            }
        });

        return writeResponse(res, results, 'Nota de recepción procesada exitosamente.');
    } catch (error) {
        console.error("❌ [AGENT] Error general al procesar nota de recepción:", error);
        res.status(500).json({ success: false, message: error.message || 'Error al guardar la nota de recepción.' });
    }
});

// =========================================================================
// 6. ANULAR NOTA DE RECEPCIÓN
// =========================================================================
router.post('/:doc_num/anular', async (req, res) => {
    const { doc_num } = req.params;
    const { sede } = req.query;
    const { motivo } = req.body || {};
    const auditUser = (req.profitUser || 'SYNC2K').substring(0, 6).toUpperCase();

    try {
        const results = await executeWrite(sede, req.sqlAuth, async (pool, srv) => {
            const transaction = new sql.Transaction(pool);
            await transaction.begin();

            try {
                // 1. Validar que la Nota de Recepción exista y no esté anulada
                const headRes = await transaction.request()
                    .input('doc_num', sql.Char(20), padProfit(doc_num, 20))
                    .query('SELECT doc_num, anulado, status, comentario FROM saNotaRecepcionCompra WHERE doc_num = @doc_num');

                if (!headRes.recordset || headRes.recordset.length === 0) {
                    throw new Error(`Nota de recepción ${doc_num} no encontrada.`);
                }

                const head = headRes.recordset[0];
                if (head.anulado) {
                    throw new Error(`La nota de recepción ${doc_num} ya se encuentra anulada.`);
                }

                // 2. Obtener los renglones recibidos para revertir inventario y restituir pendientes en la OC
                const rengRes = await transaction.request()
                    .input('doc_num', sql.Char(20), padProfit(doc_num, 20))
                    .query(`
                        SELECT 
                            r.reng_num,
                            RTRIM(r.co_art) AS co_art,
                            RTRIM(r.co_alma) AS co_alma,
                            r.total_art AS qty,
                            RTRIM(r.tipo_doc) AS tipo_doc,
                            RTRIM(r.num_doc) AS num_doc,
                            r.rowguid_doc
                        FROM saNotaRecepcionCompraReng r
                        WHERE r.doc_num = @doc_num
                    `);

                const renglones = rengRes.recordset || [];
                const touchedOriginOrders = new Set();

                for (const item of renglones) {
                    const targetAlma = item.co_alma || '01';
                    const originDocNum = (item.num_doc || '').trim();
                    if (originDocNum) touchedOriginOrders.add(originDocNum);

                    // 3. REVERTIR STOCK (Restar de ACT e Incrementar en LLE)
                    await transaction.request()
                        .input('art', sql.Char(30), padProfit(item.co_art, 30))
                        .input('alma', sql.Char(6), padProfit(targetAlma, 6))
                        .input('qty', sql.Decimal(18, 5), item.qty)
                        .query(`
                            -- Restar de ACT
                            IF EXISTS (SELECT 1 FROM saStockAlmacen WHERE co_art = @art AND co_alma = @alma AND tipo = 'ACT')
                            BEGIN
                                UPDATE saStockAlmacen 
                                SET stock = CASE WHEN stock >= @qty THEN stock - @qty ELSE 0 END
                                WHERE co_art = @art AND co_alma = @alma AND tipo = 'ACT'
                            END

                            -- Restaurar en LLE
                            IF EXISTS (SELECT 1 FROM saStockAlmacen WHERE co_art = @art AND co_alma = @alma AND tipo = 'LLE')
                            BEGIN
                                UPDATE saStockAlmacen 
                                SET stock = stock + @qty
                                WHERE co_art = @art AND co_alma = @alma AND tipo = 'LLE'
                            END
                            ELSE
                            BEGIN
                                INSERT INTO saStockAlmacen (co_art, co_alma, tipo, stock, revisado, trasnfe)
                                VALUES (@art, @alma, 'LLE', @qty, NULL, NULL)
                            END
                        `);

                    // 4. RESTAURAR PENDIENTE EN LA ORDEN DE COMPRA DE ORIGEN
                    if (originDocNum) {
                        const rOC = transaction.request();
                        rOC.input('qty', sql.Decimal(18, 5), item.qty);
                        rOC.input('oc_doc', sql.Char(20), padProfit(originDocNum, 20));
                        rOC.input('auditUser', sql.Char(6), padProfit(auditUser, 6));

                        if (item.rowguid_doc) {
                            rOC.input('rowguid_oc', sql.UniqueIdentifier, item.rowguid_doc);
                            await rOC.query(`
                                UPDATE saOrdenCompraReng
                                SET pendiente = CASE WHEN (pendiente + @qty) <= total_art THEN (pendiente + @qty) ELSE total_art END,
                                    fe_us_mo = GETDATE(),
                                    co_us_mo = @auditUser
                                WHERE doc_num = @oc_doc AND rowguid = @rowguid_oc
                            `);
                        } else {
                            rOC.input('art', sql.Char(30), padProfit(item.co_art, 30));
                            await rOC.query(`
                                UPDATE saOrdenCompraReng
                                SET pendiente = CASE WHEN (pendiente + @qty) <= total_art THEN (pendiente + @qty) ELSE total_art END,
                                    fe_us_mo = GETDATE(),
                                    co_us_mo = @auditUser
                                WHERE doc_num = @oc_doc AND co_art = @art
                            `);
                        }
                    }
                }

                // 5. RECALCULAR ESTADO DE LAS ÓRDENES DE COMPRA TOCADAS
                for (const ocNum of touchedOriginOrders) {
                    await transaction.request()
                        .input('oc_doc', sql.Char(20), padProfit(ocNum, 20))
                        .input('auditUser', sql.Char(6), padProfit(auditUser, 6))
                        .query(`
                            DECLARE @totalPendiente DECIMAL(18, 5)
                            DECLARE @totalArt DECIMAL(18, 5)

                            SELECT 
                                @totalPendiente = ISNULL(SUM(pendiente), 0),
                                @totalArt = ISNULL(SUM(total_art), 0)
                            FROM saOrdenCompraReng 
                            WHERE doc_num = @oc_doc

                            UPDATE saOrdenCompra
                            SET status = CASE 
                                WHEN @totalPendiente = 0 THEN '2' -- Procesada Totalmente
                                WHEN @totalPendiente < @totalArt THEN '1' -- Procesada Parcialmente
                                ELSE '0' -- Sin Procesar
                            END,
                            fe_us_mo = GETDATE(),
                            co_us_mo = @auditUser
                            WHERE doc_num = @oc_doc
                        `);
                }

                // 6. ANULAR CABECERA DE LA NOTA DE RECEPCIÓN
                const cleanComment = (head.comentario || '').trim();
                const voidComment = `${cleanComment ? cleanComment + ' | ' : ''}ANULADO POR API: ${motivo || 'Sin motivo especificado'}`.substring(0, 500);

                await transaction.request()
                    .input('doc_num', sql.Char(20), padProfit(doc_num, 20))
                    .input('auditUser', sql.Char(6), padProfit(auditUser, 6))
                    .input('comment', sql.VarChar(500), voidComment)
                    .query(`
                        UPDATE saNotaRecepcionCompra
                        SET anulado = 1,
                            status = '0',
                            comentario = @comment,
                            campo8 = 'Anulado vía API',
                            fe_us_mo = GETDATE(),
                            co_us_mo = @auditUser
                        WHERE doc_num = @doc_num
                    `);

                await transaction.commit();
                console.log(`🚫 [AGENT] Nota de Recepción ${doc_num} anulada exitosamente en sede ${srv.name || srv.id}`);

                return {
                    doc_num: doc_num,
                    anulado: true,
                    status: '0',
                    sede_id: srv.id,
                    sede_nombre: srv.name || srv.id
                };
            } catch (innerErr) {
                await transaction.rollback();
                throw innerErr;
            }
        });

        return writeResponse(res, results, `Nota de recepción ${doc_num} anulada correctamente.`);
    } catch (error) {
        console.error(`❌ [AGENT] Error al anular nota de recepción ${doc_num}:`, error);
        res.status(500).json({ success: false, message: error.message || 'Error al anular nota de recepción.' });
    }
});

// =========================================================================
// 7. ELIMINAR NOTA DE RECEPCIÓN
// =========================================================================
router.delete('/:doc_num', async (req, res) => {
    const { doc_num } = req.params;
    const { sede } = req.query;
    const auditUser = (req.profitUser || 'SYNC2K').substring(0, 6).toUpperCase();

    try {
        const results = await executeWrite(sede, req.sqlAuth, async (pool, srv) => {
            // 1. Validar que la Nota de Recepción exista y no esté procesada
            const headRes = await pool.request()
                .input('doc_num', sql.Char(20), padProfit(doc_num, 20))
                .query(`
                    SELECT doc_num, anulado, status, validador, rowguid, co_sucu_in 
                    FROM saNotaRecepcionCompra 
                    WHERE doc_num = @doc_num
                `);

            if (!headRes.recordset || headRes.recordset.length === 0) {
                throw new Error(`Nota de recepción ${doc_num} no encontrada.`);
            }

            const head = headRes.recordset[0];
            if (head.anulado) {
                throw new Error(`No se puede eliminar la nota ${doc_num} porque ya se encuentra anulada. Solo se pueden anular o eliminar notas activas.`);
            }

            // Validar si fue procesada en Factura de Compra
            const facCheck = await pool.request()
                .input('doc_num', sql.Char(20), padProfit(doc_num, 20))
                .query(`SELECT TOP 1 doc_num FROM saFacturaCompraReng WHERE num_doc = @doc_num AND tipo_doc = 'NREC'`);

            if (facCheck.recordset?.length > 0) {
                throw new Error(`No se puede eliminar la nota ${doc_num} porque ya fue procesada en la Factura de Compra N° ${facCheck.recordset[0].doc_num.trim()}.`);
            }

            // 2. Obtener los renglones recibidos
            const rengRes = await pool.request()
                .input('doc_num', sql.Char(20), padProfit(doc_num, 20))
                .query(`
                    SELECT 
                        r.reng_num,
                        r.rowguid,
                        RTRIM(r.co_art) AS co_art,
                        RTRIM(r.co_alma) AS co_alma,
                        r.total_art AS qty,
                        RTRIM(r.tipo_doc) AS tipo_doc,
                        RTRIM(r.num_doc) AS num_doc,
                        r.rowguid_doc
                    FROM saNotaRecepcionCompraReng r
                    WHERE r.doc_num = @doc_num
                `);

            const renglones = rengRes.recordset || [];
            const touchedOriginOrders = new Set();

            const transaction = new sql.Transaction(pool);
            await transaction.begin();

            try {
                const resSucu = await pool.request().query(`SELECT TOP 1 RTRIM(co_sucur) AS co_sucur FROM saSucursal`);
                const defSucu = resSucu.recordset[0]?.co_sucur || head.co_sucu_in || '01';

                for (const item of renglones) {
                    const targetAlma = item.co_alma || '01';
                    const originDocNum = (item.num_doc || '').trim();
                    if (originDocNum) touchedOriginOrders.add(originDocNum);

                    // 3. REVERTIR STOCK (Restar de ACT e Incrementar en LLE)
                    await transaction.request()
                        .input('art', sql.Char(30), padProfit(item.co_art, 30))
                        .input('alma', sql.Char(6), padProfit(targetAlma, 6))
                        .input('qty', sql.Decimal(18, 5), item.qty)
                        .query(`
                            -- Restar de ACT
                            IF EXISTS (SELECT 1 FROM saStockAlmacen WHERE co_art = @art AND co_alma = @alma AND tipo = 'ACT')
                            BEGIN
                                UPDATE saStockAlmacen 
                                SET stock = CASE WHEN stock >= @qty THEN stock - @qty ELSE 0 END
                                WHERE co_art = @art AND co_alma = @alma AND tipo = 'ACT'
                            END

                            -- Restaurar en LLE
                            IF EXISTS (SELECT 1 FROM saStockAlmacen WHERE co_art = @art AND co_alma = @alma AND tipo = 'LLE')
                            BEGIN
                                UPDATE saStockAlmacen 
                                SET stock = stock + @qty
                                WHERE co_art = @art AND co_alma = @alma AND tipo = 'LLE'
                            END
                            ELSE
                            BEGIN
                                INSERT INTO saStockAlmacen (co_art, co_alma, tipo, stock, revisado, trasnfe)
                                VALUES (@art, @alma, 'LLE', @qty, NULL, NULL)
                            END
                        `);

                    // 4. RESTAURAR PENDIENTE EN LA ORDEN DE COMPRA DE ORIGEN
                    if (originDocNum) {
                        const rOC = transaction.request();
                        rOC.input('qty', sql.Decimal(18, 5), item.qty);
                        rOC.input('oc_doc', sql.Char(20), padProfit(originDocNum, 20));
                        rOC.input('auditUser', sql.Char(6), padProfit(auditUser, 6));

                        if (item.rowguid_doc) {
                            rOC.input('rowguid_oc', sql.UniqueIdentifier, item.rowguid_doc);
                            await rOC.query(`
                                UPDATE saOrdenCompraReng
                                SET pendiente = CASE WHEN (pendiente + @qty) <= total_art THEN (pendiente + @qty) ELSE total_art END,
                                    fe_us_mo = GETDATE(),
                                    co_us_mo = @auditUser
                                WHERE doc_num = @oc_doc AND rowguid = @rowguid_oc
                            `);
                        } else {
                            rOC.input('art', sql.Char(30), padProfit(item.co_art, 30));
                            await rOC.query(`
                                UPDATE saOrdenCompraReng
                                SET pendiente = CASE WHEN (pendiente + @qty) <= total_art THEN (pendiente + @qty) ELSE total_art END,
                                    fe_us_mo = GETDATE(),
                                    co_us_mo = @auditUser
                                WHERE doc_num = @oc_doc AND co_art = @art
                            `);
                        }
                    }

                    // 5. Eliminar renglón vía SP
                    const rL = new sql.Request(transaction);
                    rL.input('iReng_NumOri', sql.Int, item.reng_num);
                    rL.input('sDoc_NumOri', sql.Char(20), padProfit(doc_num, 20));
                    rL.input('sMaquina', sql.VarChar(60), 'SYNC2K');
                    rL.input('sCo_Us_Mo', sql.Char(6), padProfit(auditUser, 6));
                    rL.input('sCo_Sucu_Mo', sql.Char(6), padProfit(defSucu, 6));
                    rL.input('gRowguid', sql.UniqueIdentifier, item.rowguid);
                    await rL.execute('pEliminarRenglonesNotaRecepcionCompra');
                }

                // 6. RECALCULAR ESTADO DE LAS ÓRDENES DE COMPRA TOCADAS
                for (const ocNum of touchedOriginOrders) {
                    await transaction.request()
                        .input('oc_doc', sql.Char(20), padProfit(ocNum, 20))
                        .input('auditUser', sql.Char(6), padProfit(auditUser, 6))
                        .query(`
                            DECLARE @totalPendiente DECIMAL(18, 5)
                            DECLARE @totalArt DECIMAL(18, 5)

                            SELECT 
                                @totalPendiente = ISNULL(SUM(pendiente), 0),
                                @totalArt = ISNULL(SUM(total_art), 0)
                            FROM saOrdenCompraReng 
                            WHERE doc_num = @oc_doc

                            UPDATE saOrdenCompra
                            SET status = CASE 
                                WHEN @totalPendiente = 0 THEN '2' -- Procesada Totalmente
                                WHEN @totalPendiente < @totalArt THEN '1' -- Procesada Parcialmente
                                ELSE '0' -- Sin Procesar
                            END,
                            fe_us_mo = GETDATE(),
                            co_us_mo = @auditUser
                            WHERE doc_num = @oc_doc
                        `);
                }

                // 7. Eliminar Cabecera vía SP
                const rH = new sql.Request(transaction);
                rH.input('sDoc_NumOri', sql.Char(20), padProfit(doc_num, 20));
                rH.input('sMaquina', sql.VarChar(60), 'SYNC2K');
                rH.input('sCo_Us_Mo', sql.Char(6), padProfit(auditUser, 6));
                rH.input('sCo_Sucu_Mo', sql.Char(6), padProfit(defSucu, 6));
                rH.input('tsvalidador', sql.VarBinary, head.validador);
                rH.input('gRowguid', sql.UniqueIdentifier, head.rowguid);
                await rH.execute('pEliminarNotaRecepcionCompra');

                await transaction.commit();
                console.log(`🗑️ [AGENT] Nota de Recepción ${doc_num} eliminada físicamente con éxito.`);

                return {
                    doc_num: doc_num,
                    deleted: true,
                    sede_id: srv.id,
                    sede_nombre: srv.name || srv.id
                };
            } catch (innerErr) {
                await transaction.rollback();
                throw innerErr;
            }
        });

        return writeResponse(res, results, `Nota de recepción ${doc_num} eliminada correctamente.`);
    } catch (error) {
        console.error(`❌ [AGENT] Error al eliminar nota de recepción ${doc_num}:`, error);
        res.status(500).json({ success: false, message: error.message || 'Error al eliminar nota de recepción.' });
    }
});

module.exports = router;

