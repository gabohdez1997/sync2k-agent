const express = require('express');
const router = express.Router();
const { sql, getPool, getServers, getExchangeRate } = require('../db');
const { executeWrite, writeResponse, paginatedResponse, padProfit } = require('../helpers/multiSede');
const { getProximoConsecutivo } = require('../helpers/consecutivos');

console.log("🚚 [AGENT] Iniciando Módulo de Notas de Despacho de Venta (saNotaDespachoVenta)");

/**
 * Helper para formatear fecha a YYYY-MM-DD
 */
function safeDate(val) {
    if (!val) return null;
    if (val instanceof Date) return val.toISOString().split('T')[0];
    return String(val).split('T')[0];
}

// =========================================================================
// 1. LISTAR NOTAS DE DESPACHO (HISTORIAL)
// =========================================================================
router.get('/', async (req, res) => {
    try {
        const page  = parseInt(req.query.page)  || 1;
        const limit = parseInt(req.query.limit) || 12;
        const { sede, doc_num, factura, co_cli, fec_d, fec_h, search, status } = req.query;
        
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
                if (factura) {
                    request.input('factura', sql.VarChar, `%${factura}%`);
                    whereClauses.push("EXISTS (SELECT 1 FROM saNotaDespachoVentaReng r WHERE r.doc_num = c.doc_num AND r.num_doc LIKE @factura)");
                }
                if (co_cli) {
                    request.input('co_cli_search', sql.VarChar, `%${co_cli}%`);
                    whereClauses.push("(c.co_cli LIKE @co_cli_search OR cl.cli_des LIKE @co_cli_search OR cl.rif LIKE @co_cli_search)");
                }
                if (search) {
                    request.input('search_all', sql.VarChar, `%${search}%`);
                    whereClauses.push(`(
                        c.doc_num LIKE @search_all 
                        OR cl.cli_des LIKE @search_all 
                        OR c.co_cli LIKE @search_all 
                        OR cl.rif LIKE @search_all
                        OR c.n_control LIKE @search_all
                        OR c.descrip LIKE @search_all
                        OR EXISTS (SELECT 1 FROM saNotaDespachoVentaReng r WHERE r.doc_num = c.doc_num AND r.num_doc LIKE @search_all)
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
                        RTRIM(c.co_cli) AS co_cli,
                        RTRIM(cl.cli_des) AS cli_des,
                        RTRIM(cl.rif) AS rif,
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
                        c.status,
                        RTRIM(c.co_us_in) AS co_us_in,
                        RTRIM(c.co_sucu_in) AS co_sucu_in,
                        RTRIM(c.comentario) AS comentario,
                        (
                            SELECT TOP 1 RTRIM(r.num_doc) 
                            FROM saNotaDespachoVentaReng r 
                            WHERE r.doc_num = c.doc_num AND r.num_doc IS NOT NULL AND RTRIM(r.num_doc) <> ''
                        ) AS factura_origen,
                        (
                            SELECT COUNT(*) 
                            FROM saNotaDespachoVentaReng r 
                            WHERE r.doc_num = c.doc_num
                        ) AS total_renglones,
                        (
                            SELECT ISNULL(SUM(r.total_art), 0) 
                            FROM saNotaDespachoVentaReng r 
                            WHERE r.doc_num = c.doc_num
                        ) AS total_unidades,
                        '${srv.name || srv.id}' AS sede_nombre,
                        '${srv.id}' AS sede_id
                    FROM saNotaDespachoVenta c
                    LEFT JOIN saCliente cl ON c.co_cli = cl.co_cli
                    WHERE ${whereSQL}
                    ORDER BY c.fec_emis DESC, c.doc_num DESC
                `;

                const result = await request.query(querySQL);
                return result.recordset || [];
            } catch (err) {
                console.error(`❌ [AGENT] Error en sede ${srv.name || srv.id} al listar notas de despacho:`, err.message);
                return [];
            }
        }));

        const flattened = allData.flat();
        flattened.sort((a, b) => new Date(b.fec_emis).getTime() - new Date(a.fec_emis).getTime());

        return paginatedResponse(res, flattened, page, limit);
    } catch (error) {
        console.error("❌ [AGENT] Error en GET /notas-despacho:", error);
        res.status(500).json({ success: false, message: 'Error interno del servidor.', error: error.message });
    }
});

// =========================================================================
// 2. BUSCAR FACTURAS DE VENTA PENDIENTES DE DESPACHO
// =========================================================================
router.get('/facturas-pendientes', async (req, res) => {
    try {
        const { sede, search, co_cli } = req.query;
        const servers = getServers();
        const targets = sede ? servers.filter(s => s.id === sede) : servers;

        const allInvoices = await Promise.all(targets.map(async (srv) => {
            try {
                const pool = await getPool(srv.id, req.sqlAuth);
                const request = pool.request();
                let whereClauses = [
                    "c.anulado = 0",
                    "c.status IN ('0', '1')", // Sin procesar o parcialmente procesada
                    "EXISTS (SELECT 1 FROM saFacturaVentaReng r WHERE r.doc_num = c.doc_num AND r.pendiente > 0)"
                ];

                if (search) {
                    request.input('search', sql.VarChar, `%${search}%`);
                    whereClauses.push("(c.doc_num LIKE @search OR cl.cli_des LIKE @search OR c.co_cli LIKE @search OR cl.rif LIKE @search OR c.descrip LIKE @search OR c.n_control LIKE @search)");
                }
                if (co_cli) {
                    request.input('co_cli', sql.VarChar, `%${co_cli}%`);
                    whereClauses.push("(c.co_cli LIKE @co_cli OR cl.cli_des LIKE @co_cli OR cl.rif LIKE @co_cli)");
                }

                const whereSQL = whereClauses.join(" AND ");

                const querySQL = `
                    SELECT 
                        RTRIM(c.doc_num) AS doc_num,
                        RTRIM(c.descrip) AS descrip,
                        RTRIM(c.co_cli) AS co_cli,
                        RTRIM(cl.cli_des) AS cli_des,
                        RTRIM(cl.rif) AS rif,
                        RTRIM(cl.direc1) AS cli_dir,
                        RTRIM(cl.telefonos) AS telefonos,
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
                        (SELECT COUNT(*) FROM saFacturaVentaReng r WHERE r.doc_num = c.doc_num) AS total_renglones,
                        (SELECT COUNT(*) FROM saFacturaVentaReng r WHERE r.doc_num = c.doc_num AND r.pendiente > 0) AS renglones_pendientes,
                        (SELECT ISNULL(SUM(r.total_art), 0) FROM saFacturaVentaReng r WHERE r.doc_num = c.doc_num) AS cant_total,
                        (SELECT ISNULL(SUM(r.pendiente), 0) FROM saFacturaVentaReng r WHERE r.doc_num = c.doc_num) AS cant_pendiente,
                        '${srv.name || srv.id}' AS sede_nombre,
                        '${srv.id}' AS sede_id
                    FROM saFacturaVenta c
                    LEFT JOIN saCliente cl ON c.co_cli = cl.co_cli
                    LEFT JOIN saCondicionPago cd ON c.co_cond = cd.co_cond
                    WHERE ${whereSQL}
                    ORDER BY c.fec_emis DESC, c.doc_num DESC
                `;

                const result = await request.query(querySQL);
                return result.recordset || [];
            } catch (err) {
                console.error(`❌ [AGENT] Error en sede ${srv.name || srv.id} al consultar facturas pendientes:`, err.message);
                return [];
            }
        }));

        const flattened = allInvoices.flat();
        flattened.sort((a, b) => new Date(b.fec_emis).getTime() - new Date(a.fec_emis).getTime());

        res.json({ success: true, data: flattened });
    } catch (error) {
        console.error("❌ [AGENT] Error en GET /facturas-pendientes:", error);
        res.status(500).json({ success: false, message: 'Error interno del servidor.', error: error.message });
    }
});

// =========================================================================
// 3. CONSULTAR RENGLONES DE UNA FACTURA PARA DESPACHO
// =========================================================================
router.get('/facturas-pendientes/:doc_num', async (req, res) => {
    try {
        const { doc_num } = req.params;
        const { sede } = req.query;

        const servers = getServers();
        const targets = sede ? servers.filter(s => s.id === sede) : servers;

        for (const srv of targets) {
            try {
                const pool = await getPool(srv.id, req.sqlAuth);

                // 1. Cabecera de la Factura
                const headRes = await pool.request()
                    .input('doc_num', sql.Char(20), padProfit(doc_num, 20))
                    .query(`
                        SELECT 
                            RTRIM(c.doc_num) AS doc_num,
                            RTRIM(c.descrip) AS descrip,
                            RTRIM(c.co_cli) AS co_cli,
                            RTRIM(cl.cli_des) AS cli_des,
                            RTRIM(cl.rif) AS rif,
                            RTRIM(cl.direc1) AS cli_dir,
                            RTRIM(cl.telefonos) AS telefonos,
                            RTRIM(cl.email) AS email,
                            RTRIM(c.co_cond) AS co_cond,
                            ISNULL(NULLIF(RTRIM(cd.cond_des), ''), RTRIM(c.co_cond)) AS cond_des,
                            c.fec_emis,
                            c.fec_venc,
                            c.tasa,
                            RTRIM(c.co_mone) AS co_mone,
                            RTRIM(c.co_ven) AS co_ven,
                            c.total_bruto,
                            c.monto_imp,
                            c.total_neto,
                            c.status,
                            c.anulado,
                            RTRIM(c.comentario) AS comentario,
                            '${srv.name || srv.id}' AS sede_nombre,
                            '${srv.id}' AS sede_id
                        FROM saFacturaVenta c
                        LEFT JOIN saCliente cl ON c.co_cli = cl.co_cli
                        LEFT JOIN saCondicionPago cd ON c.co_cond = cd.co_cond
                        WHERE c.doc_num = @doc_num
                    `);

                if (!headRes.recordset || headRes.recordset.length === 0) continue;
                const invoiceHeader = headRes.recordset[0];

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
                            r.prec_vta,
                            r.prec_vta_om,
                            r.tipo_imp,
                            r.porc_imp,
                            r.monto_imp,
                            r.reng_neto,
                            r.rowguid AS rowguid_doc
                        FROM saFacturaVentaReng r
                        LEFT JOIN saArticulo a ON r.co_art = a.co_art
                        LEFT JOIN saUnidad u ON r.co_uni = u.co_uni
                        LEFT JOIN saAlmacen al ON r.co_alma = al.co_alma
                        WHERE r.doc_num = @doc_num AND r.pendiente > 0
                        ORDER BY r.reng_num ASC
                    `);

                return res.json({
                    success: true,
                    data: {
                        ...invoiceHeader,
                        renglones: rengRes.recordset || []
                    }
                });
            } catch (err) {
                console.error(`❌ [AGENT] Error en sede ${srv.name || srv.id} al consultar detalle de factura pendiente:`, err.message);
            }
        }

        return res.status(404).json({ success: false, message: `Factura de venta ${doc_num} no encontrada.` });
    } catch (error) {
        console.error("❌ [AGENT] Error en GET /facturas-pendientes/:doc_num:", error);
        res.status(500).json({ success: false, message: 'Error interno del servidor.', error: error.message });
    }
});

// =========================================================================
// 4. CONSULTAR DETALLE DE UNA NOTA DE DESPACHO ESPECÍFICA
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

                const headRes = await pool.request()
                    .input('doc_num', sql.Char(20), padProfit(doc_num, 20))
                    .query(`
                        SELECT 
                            RTRIM(c.doc_num) AS doc_num,
                            RTRIM(c.descrip) AS descrip,
                            RTRIM(c.co_cli) AS co_cli,
                            RTRIM(cl.cli_des) AS cli_des,
                            RTRIM(cl.rif) AS rif,
                            RTRIM(cl.direc1) AS cli_dir,
                            RTRIM(cl.telefonos) AS telefonos,
                            RTRIM(cl.email) AS email,
                            RTRIM(c.co_cond) AS co_cond,
                            ISNULL(NULLIF(RTRIM(cd.cond_des), ''), RTRIM(c.co_cond)) AS cond_des,
                            c.fec_emis,
                            c.fec_venc,
                            c.fec_reg,
                            c.fe_us_in AS fec_us_in,
                            c.fe_us_mo AS fec_us_mo,
                            c.anulado,
                            c.status,
                            RTRIM(c.n_control) AS n_control,
                            RTRIM(c.comentario) AS comentario,
                            RTRIM(c.co_us_in) AS co_us_in,
                            RTRIM(c.co_sucu_in) AS co_sucu_in,
                            '${srv.name || srv.id}' AS sede_nombre,
                            '${srv.id}' AS sede_id
                        FROM saNotaDespachoVenta c
                        LEFT JOIN saCliente cl ON c.co_cli = cl.co_cli
                        LEFT JOIN saCondicionPago cd ON c.co_cond = cd.co_cond
                        WHERE c.doc_num = @doc_num
                    `);

                if (!headRes.recordset || headRes.recordset.length === 0) continue;
                const header = headRes.recordset[0];

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
                            RTRIM(al.des_alma) AS des_alma,
                            r.total_art AS cant_despachada,
                            r.pendiente AS cant_pendiente,
                            RTRIM(r.num_doc) AS doc_num_factura,
                            RTRIM(r.tipo_doc) AS tipo_doc
                        FROM saNotaDespachoVentaReng r
                        LEFT JOIN saArticulo a ON r.co_art = a.co_art
                        LEFT JOIN saUnidad u ON r.co_uni = u.co_uni
                        LEFT JOIN saAlmacen al ON r.co_alma = al.co_alma
                        WHERE r.doc_num = @doc_num
                        ORDER BY r.reng_num ASC
                    `);

                return res.json({
                    success: true,
                    data: {
                        ...header,
                        factura_origen: rengRes.recordset && rengRes.recordset[0] ? rengRes.recordset[0].doc_num_factura : '',
                        renglones: rengRes.recordset || []
                    }
                });
            } catch (err) {
                console.error(`❌ [AGENT] Error en sede ${srv.name || srv.id} al consultar nota de despacho ${doc_num}:`, err.message);
            }
        }

        return res.status(404).json({ success: false, message: `Nota de despacho ${doc_num} no encontrada.` });
    } catch (error) {
        console.error("❌ [AGENT] Error en GET /notas-despacho/:doc_num:", error);
        res.status(500).json({ success: false, message: 'Error interno del servidor.', error: error.message });
    }
});

// =========================================================================
// 5. GUARDAR / PROCESAR NOTA DE DESPACHO
// =========================================================================
router.post('/', async (req, res) => {
    try {
        const payload = req.body;
        const sedeParam = req.query.sede || payload.sede_id || null;

        if (!payload.co_cli) {
            return res.status(400).json({ success: false, message: 'El cliente (co_cli) es requerido.' });
        }
        if (!payload.renglones || !payload.renglones.length) {
            return res.status(400).json({ success: false, message: 'Debe incluir al menos un renglón para despachar.' });
        }

        const validLines = payload.renglones.filter(r => (Number(r.cant_despachada) || Number(r.cant_recibida) || Number(r.total_art) || 0) > 0);
        if (!validLines.length) {
            return res.status(400).json({ success: false, message: 'Todos los renglones tienen cantidad cero a despachar.' });
        }

        const outcome = await executeWrite(sedeParam, req.sqlAuth, async (pool, srv) => {
            const auditUser = (req.profitUser || req.sqlAuth?.user || '01').substring(0, 6).toUpperCase();
            const coSucu = (payload.co_sucu || srv.co_sucu || '01').substring(0, 6);

            // 1. Obtener próximo consecutivo para saNotaDespachoVenta
            const consecutivoInfo = await getProximoConsecutivo({
                runner: pool,
                co_tipo_serie: 'V009',
                co_consecutivos: ['NDES_NUM', 'NDES', 'DESP_NUM', 'DESP'],
                co_sucur: coSucu,
                table: 'saNotaDespachoVenta',
                col: 'doc_num'
            });

            const docNum = consecutivoInfo.docNum;
            console.log(`📦 [DESPACHO] Asignando correlativo de Despacho: ${docNum} para sede: ${srv.name}`);

            const ts = new Date();
            const fecEmis = payload.fec_emis ? new Date(`${safeDate(payload.fec_emis)}T00:00:00`) : ts;
            const fecVenc = payload.fec_venc ? new Date(`${safeDate(payload.fec_venc)}T00:00:00`) : fecEmis;

            // 2. Insertar Encabezado
            const headReq = new sql.Request(pool);
            headReq.input('sDoc_Num',        sql.Char(20), padProfit(docNum, 20));
            headReq.input('sDescrip',        sql.VarChar(60), String(payload.descrip || `DESPACHO FACT: ${payload.factura_origen || ''}`).trim().substring(0, 60));
            headReq.input('sCo_Cli',         sql.Char(16), padProfit(payload.co_cli, 16));
            headReq.input('sCo_Tran',        sql.Char(6), padProfit(payload.co_tran || '01', 6));
            headReq.input('sCo_Mone',        sql.Char(6), padProfit(payload.co_mone || 'BS', 6));
            headReq.input('sCo_Ven',         sql.Char(6), padProfit(payload.co_ven || '01', 6));
            headReq.input('sCo_Cond',        sql.Char(6), padProfit(payload.co_cond || '01', 6));
            headReq.input('sdFec_Emis',      sql.SmallDateTime, fecEmis);
            headReq.input('sdFec_Venc',      sql.SmallDateTime, fecVenc);
            headReq.input('sdFec_Reg',       sql.SmallDateTime, ts);
            headReq.input('bAnulado',        sql.Bit, 0);
            headReq.input('sStatus',         sql.Char(1), '0');
            headReq.input('sN_Control',      sql.Char(20), padProfit(payload.n_control || docNum, 20));
            headReq.input('bVen_Ter',        sql.Bit, 0);
            headReq.input('deTasa',          sql.Decimal(18, 5), Number(payload.tasa) || 1);
            headReq.input('sPorc_Desc_Glob', sql.Char(15), '0');
            headReq.input('deMonto_Desc_Glob', sql.Decimal(18, 5), 0);
            headReq.input('sPorc_Reca',      sql.Char(15), '0');
            headReq.input('deMonto_Reca',    sql.Decimal(18, 5), 0);
            headReq.input('deSaldo',         sql.Decimal(18, 5), 0);
            headReq.input('deTotal_Bruto',   sql.Decimal(18, 5), Number(payload.total_bruto) || 0);
            headReq.input('deMonto_Imp',     sql.Decimal(18, 5), Number(payload.monto_imp) || 0);
            headReq.input('deMonto_Imp2',    sql.Decimal(18, 5), 0);
            headReq.input('deMonto_Imp3',    sql.Decimal(18, 5), 0);
            headReq.input('deOtros1',        sql.Decimal(18, 5), 0);
            headReq.input('deOtros2',        sql.Decimal(18, 5), 0);
            headReq.input('deOtros3',        sql.Decimal(18, 5), 0);
            headReq.input('deTotal_Neto',    sql.Decimal(18, 5), Number(payload.total_neto) || 0);
            headReq.input('sDis_Cen',        sql.VarChar(sql.MAX), payload.dis_cen || '<InformacionContable><Carpeta01><CuentaContable>1.1.05.001</CuentaContable></Carpeta01></InformacionContable>');
            headReq.input('sComentario',     sql.VarChar(sql.MAX), payload.comentario || 'Despacho procesado vía API');
            headReq.input('sDir_Ent',        sql.VarChar(sql.MAX), payload.dir_ent || payload.cli_dir || '');
            headReq.input('bContrib',        sql.Bit, payload.contrib ? 1 : 0);
            headReq.input('bImpresa',        sql.Bit, 0);
            headReq.input('sSalestax',       sql.Char(8), null);
            headReq.input('sImpfis',         sql.Char(20), null);
            headReq.input('sImpfisfac',      sql.VarChar(20), null);
            headReq.input('sCampo1',         sql.VarChar(60), payload.campo1 || null);
            headReq.input('sCampo2',         sql.VarChar(60), payload.campo2 || null);
            headReq.input('sCampo3',         sql.VarChar(60), payload.campo3 || null);
            headReq.input('sCampo4',         sql.VarChar(60), payload.campo4 || null);
            headReq.input('sCampo5',         sql.VarChar(60), payload.campo5 || null);
            headReq.input('sCampo6',         sql.VarChar(60), payload.campo6 || null);
            headReq.input('sCampo7',         sql.VarChar(60), payload.campo7 || null);
            headReq.input('sCampo8',         sql.VarChar(60), payload.campo8 || null);
            headReq.input('sCo_Us_In',       sql.Char(6), auditUser);
            headReq.input('sCo_Sucu_In',     sql.Char(6), coSucu);
            headReq.input('sRevisado',       sql.Char(1), '0');
            headReq.input('sTrasnfe',        sql.Char(1), '0');
            headReq.input('sMaquina',        sql.VarChar(60), 'SYNC2K');

            await headReq.execute('pInsertarNotaDespachoVenta');

            // 3. Insertar Renglones
            let validAlmacenes = [];
            try {
                const almaListRes = await pool.request().query("SELECT RTRIM(co_alma) as co_alma, campo1 FROM saAlmacen ORDER BY co_alma ASC");
                validAlmacenes = (almaListRes.recordset || []).map(r => r.co_alma.trim());
            } catch (e) {}

            const requestedDefAlma = (payload.defaultWarehouse || '').trim();
            const defAlma = validAlmacenes.find(a => a === requestedDefAlma)
                || validAlmacenes.find(a => a === coSucu)
                || validAlmacenes.find(a => a === (srv.co_sucu || '').trim())
                || validAlmacenes[0]
                || '01';

            let rengNum = 1;
            for (const line of validLines) {
                const cantDesp = Number(line.cant_despachada || line.cant_recibida || line.total_art || 0);
                const cantPendOriginal = Number(line.cant_pendiente || cantDesp);
                const cantPendRestante = Math.max(0, cantPendOriginal - cantDesp);

                let targetAlma = (line.co_alma || requestedDefAlma || '').trim();
                if (!validAlmacenes.includes(targetAlma)) {
                    console.warn(`⚠️ [AGENT] Almacén '${targetAlma}' no existe en sede ${srv.name}. Reasignando a '${defAlma}'.`);
                    targetAlma = defAlma;
                }

                const rengReq = new sql.Request(pool);
                rengReq.input('iReng_Num',          sql.Int, rengNum);
                rengReq.input('sDoc_Num',           sql.Char(20), padProfit(docNum, 20));
                rengReq.input('sCo_Art',            sql.Char(30), padProfit(line.co_art, 30));
                rengReq.input('sDes_Art',           sql.VarChar(120), String(line.des_art || line.art_des || '').substring(0, 120));
                rengReq.input('sCo_Uni',            sql.Char(6), padProfit(line.co_uni || 'UNID', 6));
                rengReq.input('sSco_Uni',           sql.Char(6), padProfit(line.sco_uni || line.co_uni || 'UNID', 6));
                rengReq.input('sCo_Alma',           sql.Char(6), padProfit(targetAlma, 6));
                rengReq.input('sCo_Precio',         sql.Char(6), padProfit(line.co_precio || '01', 6));
                rengReq.input('sTipo_Imp',          sql.Char(1), line.tipo_imp || '1');
                rengReq.input('sTipo_Imp2',         sql.Char(1), null);
                rengReq.input('sTipo_Imp3',         sql.Char(1), null);
                rengReq.input('deTotal_Art',        sql.Decimal(18, 5), cantDesp);
                rengReq.input('deSTotal_Art',       sql.Decimal(18, 5), cantDesp);
                rengReq.input('dePrec_Vta',         sql.Decimal(18, 5), Number(line.prec_vta) || 0);
                rengReq.input('sPorc_Desc',         sql.VarChar(15), String(line.porc_desc || '0'));
                rengReq.input('deMonto_Desc',       sql.Decimal(18, 5), Number(line.monto_desc) || 0);
                rengReq.input('deReng_Neto',        sql.Decimal(18, 5), Number(line.reng_neto) || 0);
                rengReq.input('dePendiente',        sql.Decimal(18, 5), cantPendRestante);
                rengReq.input('dePendiente2',       sql.Decimal(18, 5), 0);
                rengReq.input('deMonto_Desc_Glob',  sql.Decimal(18, 5), 0);
                rengReq.input('deMonto_reca_Glob',  sql.Decimal(18, 5), 0);
                rengReq.input('deOtros1_glob',      sql.Decimal(18, 5), 0);
                rengReq.input('deOtros2_glob',      sql.Decimal(18, 5), 0);
                rengReq.input('deOtros3_glob',      sql.Decimal(18, 5), 0);
                rengReq.input('deMonto_imp_afec_glob', sql.Decimal(18, 5), 0);
                rengReq.input('deMonto_imp2_afec_glob', sql.Decimal(18, 5), 0);
                rengReq.input('deMonto_imp3_afec_glob', sql.Decimal(18, 5), 0);
                rengReq.input('sTipo_Doc',          sql.Char(4), 'FACT');
                rengReq.input('gRowguid_Doc',       sql.UniqueIdentifier, line.rowguid_doc || line.rowguid || null);
                rengReq.input('sNum_Doc',           sql.VarChar(20), String(line.doc_num_factura || line.num_doc || payload.factura_origen || '').trim().substring(0, 20));
                rengReq.input('dePorc_Imp',         sql.Decimal(18, 5), Number(line.porc_imp) || 0);
                rengReq.input('dePorc_Imp2',        sql.Decimal(18, 5), 0);
                rengReq.input('dePorc_Imp3',        sql.Decimal(18, 5), 0);
                rengReq.input('deMonto_Imp',        sql.Decimal(18, 5), Number(line.monto_imp) || 0);
                rengReq.input('deMonto_Imp2',       sql.Decimal(18, 5), 0);
                rengReq.input('deMonto_Imp3',       sql.Decimal(18, 5), 0);
                rengReq.input('deOtros',            sql.Decimal(18, 5), 0);
                rengReq.input('deTotal_Dev',        sql.Decimal(18, 5), 0);
                rengReq.input('deMonto_Dev',        sql.Decimal(18, 5), 0);
                rengReq.input('sComentario',        sql.VarChar(sql.MAX), line.comentario || '');
                rengReq.input('sDis_Cen',           sql.VarChar(sql.MAX), line.dis_cen || '');
                rengReq.input('sCo_Sucu_In',        sql.Char(6), coSucu);
                rengReq.input('sCo_Us_In',          sql.Char(6), auditUser);
                rengReq.input('sREVISADO',          sql.Char(1), '0');
                rengReq.input('sTRASNFE',           sql.Char(1), '0');
                rengReq.input('sMaquina',           sql.VarChar(60), 'SYNC2K');

                await rengReq.execute('pInsertarRenglonesNotaDespachoVenta');
                rengNum++;
            }

            // 4. Actualizar estatus de las facturas involucradas
            const facturasAfectadas = [...new Set(validLines.map(l => String(l.doc_num_factura || l.num_doc || payload.factura_origen || '').trim()).filter(Boolean))];
            for (const numFact of facturasAfectadas) {
                try {
                    await pool.request()
                        .input('doc_num', sql.Char(20), padProfit(numFact, 20))
                        .query(`
                            DECLARE @total_art DECIMAL(18,5), @pendiente DECIMAL(18,5);
                            SELECT @total_art = ISNULL(SUM(total_art), 0), @pendiente = ISNULL(SUM(pendiente), 0)
                            FROM saFacturaVentaReng WHERE doc_num = @doc_num;

                            UPDATE saFacturaVenta 
                            SET status = CASE 
                                WHEN @pendiente <= 0 THEN '2'
                                WHEN @pendiente < @total_art THEN '1'
                                ELSE '0'
                            END
                            WHERE doc_num = @doc_num;
                        `);
                } catch (stErr) {
                    console.warn(`[DESPACHO] Advertencia actualizando estatus de factura ${numFact}:`, stErr.message);
                }
            }

            return {
                doc_num: docNum,
                total_art: validLines.reduce((acc, l) => acc + (Number(l.cant_despachada || l.cant_recibida || l.total_art) || 0), 0),
                total_neto: Number(payload.total_neto) || 0
            };
        });

        return writeResponse(res, outcome, `Sede "${sedeParam}" no encontrada.`);
    } catch (error) {
        console.error("❌ [AGENT] Error en POST /notas-despacho:", error);
        res.status(500).json({ success: false, message: 'Error al procesar la nota de despacho.', error: error.message });
    }
});

// =========================================================================
// 6. ANULAR NOTA DE DESPACHO
// =========================================================================
router.post('/:doc_num/anular', async (req, res) => {
    try {
        const { doc_num } = req.params;
        const sedeParam = req.query.sede || null;

        const outcome = await executeWrite(sedeParam, req.sqlAuth, async (pool, srv) => {
            const auditUser = (req.profitUser || req.sqlAuth?.user || '01').substring(0, 6).toUpperCase();

            // 1. Obtener renglones del despacho para restaurar cantidades pendientes en la factura
            const rengRes = await pool.request()
                .input('doc_num', sql.Char(20), padProfit(doc_num, 20))
                .query(`
                    SELECT RTRIM(num_doc) AS num_doc, co_art, total_art, rowguid_doc
                    FROM saNotaDespachoVentaReng 
                    WHERE doc_num = @doc_num AND tipo_doc = 'FACT'
                `);

            const renglones = rengRes.recordset || [];

            // 2. Marcar como anulado en saNotaDespachoVenta
            await pool.request()
                .input('doc_num', sql.Char(20), padProfit(doc_num, 20))
                .input('auditUser', sql.Char(6), auditUser)
                .query(`
                    UPDATE saNotaDespachoVenta 
                    SET anulado = 1,
                        status = '3',
                        fe_us_mo = GETDATE(),
                        co_us_mo = @auditUser
                    WHERE doc_num = @doc_num;
                `);

            // 3. Restaurar pendientes en saFacturaVentaReng y recalcular status
            for (const r of renglones) {
                if (r.num_doc) {
                    await pool.request()
                        .input('num_doc', sql.Char(20), padProfit(r.num_doc, 20))
                        .input('co_art', sql.Char(30), padProfit(r.co_art, 30))
                        .input('cant', sql.Decimal(18, 5), Number(r.total_art) || 0)
                        .query(`
                            UPDATE saFacturaVentaReng
                            SET pendiente = pendiente + @cant
                            WHERE doc_num = @num_doc AND co_art = @co_art;
                        `);
                }
            }

            const facturas = [...new Set(renglones.map(r => r.num_doc).filter(Boolean))];
            for (const numFact of facturas) {
                await pool.request()
                    .input('doc_num', sql.Char(20), padProfit(numFact, 20))
                    .query(`
                        DECLARE @total_art DECIMAL(18,5), @pendiente DECIMAL(18,5);
                        SELECT @total_art = ISNULL(SUM(total_art), 0), @pendiente = ISNULL(SUM(pendiente), 0)
                        FROM saFacturaVentaReng WHERE doc_num = @doc_num;

                        UPDATE saFacturaVenta 
                        SET status = CASE 
                            WHEN @pendiente <= 0 THEN '2'
                            WHEN @pendiente < @total_art THEN '1'
                            ELSE '0'
                        END
                        WHERE doc_num = @doc_num;
                    `);
            }

            return { doc_num, anulado: true };
        });

        return writeResponse(res, outcome, `Sede "${sedeParam}" no encontrada.`);
    } catch (error) {
        console.error("❌ [AGENT] Error en POST /notas-despacho/:doc_num/anular:", error);
        res.status(500).json({ success: false, message: 'Error al anular la nota de despacho.', error: error.message });
    }
});

// =========================================================================
// 7. ELIMINAR NOTA DE DESPACHO
// =========================================================================
router.delete('/:doc_num', async (req, res) => {
    try {
        const { doc_num } = req.params;
        const sedeParam = req.query.sede || null;

        const outcome = await executeWrite(sedeParam, req.sqlAuth, async (pool) => {
            const auditUser = (req.profitUser || req.sqlAuth?.user || '01').substring(0, 6).toUpperCase();

            const check = await pool.request()
                .input('doc_num', sql.Char(20), padProfit(doc_num, 20))
                .query("SELECT validador FROM saNotaDespachoVenta WHERE doc_num = @doc_num");

            if (!check.recordset.length) {
                return { skipped: true, message: 'La nota de despacho no existe en esta sede' };
            }

            // Anular primero para restaurar pendientes
            const rengRes = await pool.request()
                .input('doc_num', sql.Char(20), padProfit(doc_num, 20))
                .query("SELECT RTRIM(num_doc) AS num_doc, co_art, total_art FROM saNotaDespachoVentaReng WHERE doc_num = @doc_num");

            for (const r of (rengRes.recordset || [])) {
                if (r.num_doc) {
                    await pool.request()
                        .input('num_doc', sql.Char(20), padProfit(r.num_doc, 20))
                        .input('co_art', sql.Char(30), padProfit(r.co_art, 30))
                        .input('cant', sql.Decimal(18, 5), Number(r.total_art) || 0)
                        .query("UPDATE saFacturaVentaReng SET pendiente = pendiente + @cant WHERE doc_num = @num_doc AND co_art = @co_art");
                }
            }

            const delReq = new sql.Request(pool);
            delReq.input('sDoc_NumOri',   sql.Char(20), padProfit(doc_num, 20));
            delReq.input('tsValidador',   sql.VarBinary, check.recordset[0].validador);
            delReq.input('sMaquina',      sql.VarChar(60), 'SYNC2K');
            delReq.input('sCo_Us_Mo',     sql.Char(6), auditUser);
            delReq.input('sCo_Sucu_Mo',   sql.Char(6), '01');
            delReq.input('gRowguid',      sql.UniqueIdentifier, null);

            await delReq.execute('pEliminarNotaDespachoVenta');
            return { deleted: true, doc_num };
        });

        return writeResponse(res, outcome, `Sede "${sedeParam}" no encontrada.`);
    } catch (error) {
        console.error("❌ [AGENT] Error en DELETE /notas-despacho/:doc_num:", error);
        res.status(500).json({ success: false, message: 'Error al eliminar la nota de despacho.', error: error.message });
    }
});

module.exports = router;
