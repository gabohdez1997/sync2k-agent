const express = require('express');
const router = express.Router();
const { sql, getPool, getServers } = require('../db');
const { executeWrite, writeResponse, paginatedResponse, padProfit } = require('../helpers/multiSede');
const { getProximoConsecutivo } = require('../helpers/consecutivos');
const { getProximoConsecutivoRetencion, revertirConsecutivoRetencionIfLast } = require('../helpers/consecutivoRetenciones');
const { syncSaldoBanco, generateUniqueMovNum, insertMovimientoBancoSP, isCurrencyUSD, round2 } = require('../helpers/bancoSaldo');

/**
 * Parsea fechas YYYY-MM-DD al mediodía local para evitar desfases de zona horaria (UTC-4).
 */
function parseLocalNoonDate(val, fallbackDate) {
    if (!val) return fallbackDate || new Date();
    const s = String(val).trim();
    const m = s.match(/^(\d{4}-\d{2}-\d{2})/);
    if (m) {
        return new Date(`${m[1]}T12:00:00`);
    }
    const parsed = new Date(val);
    return Number.isNaN(parsed.getTime()) ? (fallbackDate || new Date()) : parsed;
}

/**
 * @swagger
 * tags:
 *   name: Pagos
 *   description: Gestión de Pagos a Proveedores y recibos de caja/banco de compras
 */

// --- OBTENER LISTADO DE PAGOS ---
router.get('/', async (req, res) => {
    try {
        const page = parseInt(req.query.page) || 1;
        const limit = parseInt(req.query.limit) || 12;
        const { sede, co_prov, co_us_in, fec_d, fec_h, search } = req.query;

        const servers = getServers();
        const targets = sede ? servers.filter(s => s.id === sede) : servers;

        const allData = await Promise.all(targets.map(async (srv) => {
            try {
                const pool = await getPool(srv.id, req.sqlAuth);
                const request = pool.request();
                let whereClauses = ["1=1"];

                if (co_prov) {
                    request.input('co_prov_search', sql.VarChar, `%${co_prov}%`);
                    whereClauses.push("(p.co_prov LIKE @co_prov_search OR pr.prov_des LIKE @co_prov_search OR pr.rif LIKE @co_prov_search)");
                }
                if (search) {
                    request.input('search_all', sql.VarChar, `%${search}%`);
                    whereClauses.push("(p.cob_num LIKE @search_all OR p.co_prov LIKE @search_all OR pr.prov_des LIKE @search_all OR pr.rif LIKE @search_all)");
                }
                if (co_us_in) {
                    request.input('co_us_in_filter', sql.VarChar, co_us_in.trim().toUpperCase());
                    whereClauses.push("LTRIM(RTRIM(p.co_us_in)) = @co_us_in_filter");
                }
                if (fec_d) {
                    request.input('fec_d', sql.SmallDateTime, fec_d);
                    whereClauses.push("p.fe_us_in >= @fec_d");
                }
                if (fec_h) {
                    request.input('fec_h', sql.SmallDateTime, fec_h);
                    whereClauses.push("p.fe_us_in < DATEADD(day, 1, @fec_h)");
                }

                const whereSQL = whereClauses.join(" AND ");

                const result = await request.query(`
                    SELECT RTRIM(p.cob_num) AS cob_num, RTRIM(p.recibo) AS recibo, RTRIM(p.descrip) AS descrip,
                           RTRIM(p.co_prov) AS co_prov, RTRIM(pr.prov_des) AS prov_des, RTRIM(pr.rif) AS rif,
                           p.fe_us_in AS fecha, p.anulado,
                           ISNULL((SELECT SUM(mont_doc) FROM saPagoTPReng WHERE cob_num = p.cob_num), 0) AS monto,
                           RTRIM(p.co_mone) AS co_mone, 
                           CASE WHEN p.tasa <= 1.000001 THEN 
                                ISNULL((SELECT TOP 1 t.tasa_v FROM saTasa t WHERE LTRIM(RTRIM(t.co_mone)) IN ('USD', 'US$', 'US') AND CONVERT(VARCHAR(10), t.fecha, 120) <= CONVERT(VARCHAR(10), p.fecha, 120) ORDER BY t.fecha DESC), 1.0)
                           ELSE p.tasa END AS tasa,
                           RTRIM(p.co_us_in) AS co_us_in,
                           CASE WHEN EXISTS (
                               SELECT 1 FROM saPagoRetenIvaReng ri INNER JOIN saPagoDocReng pdr ON ri.rowguid_reng_cob = pdr.rowguid WHERE pdr.cob_num = p.cob_num
                           ) OR EXISTS (
                               SELECT 1 FROM saPagoDocReng pdr WHERE pdr.cob_num = p.cob_num AND (pdr.co_tipo_doc = 'IVAN' OR pdr.monto_retencion_iva > 0)
                           ) THEN 1 ELSE 0 END AS has_retencion_iva,
                           CASE WHEN EXISTS (
                               SELECT 1 FROM saPagoRentenReng rn INNER JOIN saPagoDocReng pdr ON rn.rowguid_reng_cob = pdr.rowguid WHERE pdr.cob_num = p.cob_num
                           ) OR EXISTS (
                               SELECT 1 FROM saPagoDocReng pdr WHERE pdr.cob_num = p.cob_num AND (pdr.co_tipo_doc = 'ISLR' OR pdr.monto_retencion > 0)
                           ) THEN 1 ELSE 0 END AS has_retencion_islr,
                           ISNULL(
                               SUBSTRING(
                                   (SELECT ', ' + RTRIM(r.co_tipo_doc) + ' ' + RTRIM(r.nro_doc) + ':' + CAST(ISNULL(d.total_neto, 0) AS VARCHAR)
                                    FROM saPagoDocReng r
                                    LEFT JOIN saDocumentoCompra d ON r.co_tipo_doc = d.co_tipo_doc 
                                                                 AND r.nro_doc = d.nro_doc
                                    WHERE r.cob_num = p.cob_num 
                                      AND LTRIM(RTRIM(r.co_tipo_doc)) IN ('FACT', 'NDEB', 'N/DB', 'GIRO', 'AJPA', 'AJPM', 'N/CR', 'AJNA', 'AJNM', 'ADEL')
                                    ORDER BY r.reng_num
                                    FOR XML PATH('')), 
                                   3, 
                                   200
                               ), 
                               '---'
                           ) AS documentos_asociados
                    FROM saPago p
                    LEFT JOIN saProveedor pr ON p.co_prov = pr.co_prov
                    WHERE ${whereSQL}
                    ORDER BY p.fe_us_in DESC, p.cob_num DESC
                `);

                return result.recordset.map(r => ({ ...r, sede_id: srv.id, sede_nombre: srv.name }));
            } catch (e) {
                console.error(`[PAGOS] Error en sede ${srv.id}:`, e.message);
                return [];
            }
        }));

        const combined = [].concat(...allData);
        combined.sort((a, b) => new Date(b.fecha) - new Date(a.fecha));
        return paginatedResponse(res, combined, page, limit);
    } catch (error) {
        res.status(500).json({ success: false, message: 'Error al consultar Pagos.', error: error.message });
    }
});

// --- OBTENER FACTURAS DE COMPRA PENDIENTES CON SALDO ---
router.get('/facturas/pendientes', async (req, res) => {
    try {
        const { search, co_prov, sede } = req.query;
        const page = parseInt(req.query.page) || 1;
        const limit = parseInt(req.query.limit) || 50;

        const servers = getServers();
        const targets = sede ? servers.filter(s => s.id === sede) : servers;

        const allData = await Promise.all(targets.map(async (srv) => {
            try {
                const pool = await getPool(srv.id, req.sqlAuth);
                const request = pool.request();
                let whereClauses = [
                    "d.saldo > 0",
                    "d.anulado = 0",
                    "RTRIM(d.co_tipo_doc) IN ('FACT', 'NDEB', 'N/DB', 'GIRO', 'AJPA', 'AJPM', 'N/CR', 'AJNA', 'AJNM', 'ADEL')"
                ];

                if (co_prov) {
                    request.input('co_prov', sql.VarChar, co_prov.trim());
                    whereClauses.push("LTRIM(RTRIM(d.co_prov)) = @co_prov");
                }

                if (search) {
                    request.input('search', sql.VarChar, `%${search.trim()}%`);
                    whereClauses.push(`(
                        d.nro_doc LIKE @search 
                        OR d.nro_fact LIKE @search
                        OR d.observa LIKE @search
                        OR d.co_prov LIKE @search 
                        OR pr.prov_des LIKE @search 
                        OR pr.rif LIKE @search
                    )`);
                }

                const whereSQL = whereClauses.join(" AND ");
                const result = await request.query(`
                    SELECT TOP 100 
                           RTRIM(d.co_tipo_doc) AS co_tipo_doc, 
                           RTRIM(d.nro_doc) AS nro_doc, 
                           RTRIM(ISNULL(d.nro_fact, d.nro_doc)) AS nro_fact,
                           d.fec_emis, d.fec_venc, 
                           d.total_neto, d.total_bruto, d.saldo, d.monto_imp,
                           RTRIM(d.co_mone) AS co_mone,
                           CASE WHEN d.tasa <= 1.000001 THEN 
                                 ISNULL((SELECT TOP 1 t.tasa_v FROM saTasa t WHERE LTRIM(RTRIM(t.co_mone)) IN ('USD', 'US$', 'US') AND CONVERT(VARCHAR(10), t.fecha, 120) <= CONVERT(VARCHAR(10), d.fec_emis, 120) ORDER BY t.fecha DESC), 1.0)
                           ELSE d.tasa END AS tasa,
                           RTRIM(d.n_control) AS n_control,
                           d.rowguid,
                           RTRIM(d.co_prov) AS co_prov,
                           RTRIM(pr.prov_des) AS prov_des,
                           RTRIM(pr.rif) AS rif,
                           pr.contribu_e, pr.porc_esp,
                           RTRIM(ISNULL(pr.co_tab, '7')) AS co_tab,
                           RTRIM(ISNULL(t.tab_des, '')) AS tab_des,
                           RTRIM(ISNULL(pr.tipo_per, '3')) AS tipo_per,
                           ISNULL(pr.sujeto_obj_retenISLR_auto, 0) AS sujeto_obj_retenISLR_auto,
                           RTRIM(d.co_us_in) AS co_us_in,
                           RTRIM(ISNULL(d.doc_orig, '')) AS doc_orig,
                           RTRIM(ISNULL(d.nro_orig, '')) AS nro_orig,
                           RTRIM(ISNULL(d.observa, '')) AS observa,
                           RTRIM(ISNULL(d.mov_ban, '')) AS mov_ban,
                           RTRIM(ISNULL(td.descrip, '')) AS des_tipo_doc,
                           RTRIM(ISNULL(td.tipo_mov, '')) AS tipo_mov,
                           RTRIM(ISNULL(d.co_cta_ingr_egr, '')) AS co_cta_ingr_egr,
                           RTRIM(ISNULL(cie.descrip, '')) AS des_cta_ingr_egr,
                           CASE 
                               WHEN UPPER(RTRIM(d.co_tipo_doc)) IN ('N/CR', 'NCR', 'N/DB', 'NDEB') THEN
                                   COALESCE(
                                       NULLIF(RTRIM(afec.nro_fact_origen), ''),
                                       NULLIF(RTRIM(d.nro_orig), ''),
                                       '0'
                                   )
                               ELSE '0'
                           END AS nro_fact_afectado,
                           ISNULL(d.otros1, 0) AS otros1,
                           ISNULL(d.otros2, 0) AS otros2,
                           ISNULL(CASE
                               WHEN reng_calc.has_reng = 1 THEN ISNULL(reng_calc.exento_reng, 0)
                               WHEN ISNULL(d.monto_imp, 0) = 0 THEN ISNULL(NULLIF(d.otros2, 0), ISNULL(NULLIF(d.total_bruto, 0), d.total_neto))
                               ELSE ISNULL(d.otros2, 0)
                           END, 0) AS monto_exento,
                           ISNULL(CASE
                               WHEN reng_calc.has_reng = 1 THEN ISNULL(reng_calc.base_gravada_reng, 0)
                               WHEN ISNULL(d.monto_imp, 0) = 0 THEN 0
                               ELSE (d.total_neto - d.monto_imp - ISNULL(d.otros1, 0) - ISNULL(d.otros2, 0) - ISNULL(d.otros3, 0))
                           END, 0) AS base_imponible,
                           COALESCE(
                               NULLIF(reng_calc.alicuota_reng, 0),
                               CASE 
                                   WHEN d.porc_imp IN (8, 16, 31) THEN d.porc_imp
                                   WHEN ISNULL(d.monto_imp, 0) > 0 THEN 16
                                   ELSE 0
                               END
                           ) AS porc_imp,
                           ISNULL((SELECT SUM(pdr.monto_retencion_iva) 
                                   FROM saPagoDocReng pdr 
                                   INNER JOIN saPago p ON pdr.cob_num = p.cob_num 
                                   WHERE p.anulado = 0 
                                     AND LTRIM(RTRIM(pdr.co_tipo_doc)) = LTRIM(RTRIM(d.co_tipo_doc)) 
                                     AND LTRIM(RTRIM(pdr.nro_doc)) = LTRIM(RTRIM(d.nro_doc))), 0) AS ya_reten_iva_bs,
                           ISNULL((SELECT SUM(pdr.monto_retencion) 
                                   FROM saPagoDocReng pdr 
                                   INNER JOIN saPago p ON pdr.cob_num = p.cob_num 
                                   WHERE p.anulado = 0 
                                     AND LTRIM(RTRIM(pdr.co_tipo_doc)) = LTRIM(RTRIM(d.co_tipo_doc)) 
                                     AND LTRIM(RTRIM(pdr.nro_doc)) = LTRIM(RTRIM(d.nro_doc))), 0) AS ya_reten_islr_bs,
                           ISNULL((SELECT TOP 1 COALESCE(NULLIF(RTRIM(dc.num_comprobante), ''), RTRIM(dc.nro_doc), '') 
                                   FROM saPagoDocReng pdr 
                                   INNER JOIN saPago p ON pdr.cob_num = p.cob_num 
                                   INNER JOIN saDocumentoCompra dc ON dc.doc_orig = 'PAGO' AND LTRIM(RTRIM(dc.nro_orig)) = LTRIM(RTRIM(p.cob_num)) AND UPPER(RTRIM(dc.co_tipo_doc)) = 'IVAN' 
                                   WHERE p.anulado = 0 
                                     AND LTRIM(RTRIM(pdr.co_tipo_doc)) = LTRIM(RTRIM(d.co_tipo_doc)) 
                                     AND LTRIM(RTRIM(pdr.nro_doc)) = LTRIM(RTRIM(d.nro_doc))), '') AS nro_comp_iva,
                           ISNULL((SELECT TOP 1 RTRIM(dc.nro_doc) 
                                   FROM saPagoDocReng pdr 
                                   INNER JOIN saPago p ON pdr.cob_num = p.cob_num 
                                   INNER JOIN saDocumentoCompra dc ON dc.doc_orig = 'PAGO' AND LTRIM(RTRIM(dc.nro_orig)) = LTRIM(RTRIM(p.cob_num)) AND UPPER(RTRIM(dc.co_tipo_doc)) = 'ISLR' 
                                   WHERE p.anulado = 0 
                                     AND LTRIM(RTRIM(pdr.co_tipo_doc)) = LTRIM(RTRIM(d.co_tipo_doc)) 
                                     AND LTRIM(RTRIM(pdr.nro_doc)) = LTRIM(RTRIM(d.nro_doc))), '') AS nro_comp_islr
                    FROM saDocumentoCompra d
                    INNER JOIN saProveedor pr ON d.co_prov = pr.co_prov
                    LEFT JOIN saTipoDocumento td ON d.co_tipo_doc = td.co_tipo_doc
                    LEFT JOIN saCuentaIngEgr cie ON d.co_cta_ingr_egr = cie.co_cta_ingr_egr
                    LEFT JOIN saTabuladorIslr t ON LTRIM(RTRIM(pr.co_tab)) = LTRIM(RTRIM(t.co_tab))
                    OUTER APPLY (
                        SELECT
                            MAX(src.has_reng) AS has_reng,
                            SUM(src.exento_reng) AS exento_reng,
                            SUM(src.base_gravada_reng) AS base_gravada_reng,
                            MAX(src.alicuota_reng) AS alicuota_reng
                        FROM (
                            SELECT
                                1 AS has_reng,
                                CASE WHEN ISNULL(r.porc_imp, 0) = 0 THEN ISNULL(r.reng_neto, 0) ELSE 0 END AS exento_reng,
                                CASE WHEN ISNULL(r.porc_imp, 0) > 0 THEN ISNULL(r.reng_neto, 0) ELSE 0 END AS base_gravada_reng,
                                CASE WHEN ISNULL(r.porc_imp, 0) > 0 THEN r.porc_imp ELSE NULL END AS alicuota_reng
                            FROM saFacturaCompraReng r
                            WHERE UPPER(LTRIM(RTRIM(d.co_tipo_doc))) = 'FACT'
                              AND (
                                  LTRIM(RTRIM(r.doc_num)) = LTRIM(RTRIM(d.nro_doc))
                                  OR (UPPER(LTRIM(RTRIM(ISNULL(d.doc_orig, '')))) IN ('FACT', 'COMP') AND NULLIF(RTRIM(d.nro_orig), '') IS NOT NULL AND LTRIM(RTRIM(r.doc_num)) = LTRIM(RTRIM(d.nro_orig)))
                              )
                            UNION ALL
                            SELECT
                                1 AS has_reng,
                                CASE WHEN ISNULL(dr.porc_imp, 0) = 0 THEN ISNULL(dr.reng_neto, 0) ELSE 0 END AS exento_reng,
                                CASE WHEN ISNULL(dr.porc_imp, 0) > 0 THEN ISNULL(dr.reng_neto, 0) ELSE 0 END AS base_gravada_reng,
                                CASE WHEN ISNULL(dr.porc_imp, 0) > 0 THEN dr.porc_imp ELSE NULL END AS alicuota_reng
                            FROM saDevolucionProveedorReng dr
                            WHERE UPPER(LTRIM(RTRIM(d.co_tipo_doc))) IN ('N/CR', 'NCR')
                              AND (
                                  (NULLIF(RTRIM(d.nro_orig), '') IS NOT NULL AND LTRIM(RTRIM(dr.doc_num)) = LTRIM(RTRIM(d.nro_orig)))
                                  OR LTRIM(RTRIM(dr.doc_num)) = LTRIM(RTRIM(d.nro_doc))
                              )
                        ) src
                    ) reng_calc
                    OUTER APPLY (
                        SELECT TOP 1
                            COALESCE(
                                NULLIF(RTRIM(d_afec.nro_fact), ''),
                                NULLIF(RTRIM(d_dev.nro_fact), ''),
                                NULLIF(RTRIM(d_afec.nro_doc), '')
                            ) AS nro_fact_origen
                        FROM (SELECT 1 AS _x) _dummy
                        LEFT JOIN saDocumentoCompra d_afec ON (
                            UPPER(LTRIM(RTRIM(d.co_tipo_doc))) IN ('N/CR', 'NCR', 'N/DB', 'NDEB')
                            AND NULLIF(RTRIM(d.nro_orig), '') IS NOT NULL
                            AND LTRIM(RTRIM(d_afec.nro_doc)) = LTRIM(RTRIM(d.nro_orig))
                            AND UPPER(LTRIM(RTRIM(d_afec.co_tipo_doc))) NOT IN ('IVAN', 'ISLR', 'N/CR', 'NCR')
                        )
                        LEFT JOIN saDevolucionProveedorReng dpr ON (
                            UPPER(LTRIM(RTRIM(d.co_tipo_doc))) IN ('N/CR', 'NCR')
                            AND d_afec.nro_doc IS NULL
                            AND (
                                (NULLIF(RTRIM(d.nro_orig), '') IS NOT NULL AND LTRIM(RTRIM(dpr.doc_num)) = LTRIM(RTRIM(d.nro_orig)))
                                OR LTRIM(RTRIM(dpr.doc_num)) = LTRIM(RTRIM(d.nro_doc))
                            )
                        )
                        LEFT JOIN saDocumentoCompra d_dev ON (
                            LTRIM(RTRIM(d_dev.nro_doc)) = LTRIM(RTRIM(dpr.num_doc))
                            AND UPPER(LTRIM(RTRIM(d_dev.co_tipo_doc))) IN ('FACT', 'N/DB', 'NDEB')
                        )
                    ) afec
                    WHERE ${whereSQL}
                    ORDER BY d.fec_emis DESC
                `);

                return result.recordset.map(r => ({
                    ...r,
                    sede_id: srv.id,
                    sede_nombre: srv.name
                }));
            } catch (e) {
                console.error(`[PAGOS PENDIENTES] Error en sede ${srv.id}:`, e.message);
                return [];
            }
        }));

        const combined = [].concat(...allData);
        combined.sort((a, b) => new Date(b.fec_emis) - new Date(a.fec_emis));

        const start = (page - 1) * limit;
        const paginated = combined.slice(start, start + limit);

        res.status(200).json({
            success: true,
            count: combined.length,
            data: paginated
        });
    } catch (error) {
        res.status(500).json({ success: false, message: 'Error al consultar documentos pendientes de compra.', error: error.message });
    }
});

// --- OBTENER CONCEPTOS DE RETENCIÓN DE ISLR (CON TABULADOR Y SUSTRAENDO) ---
router.get('/conceptos-islr', async (req, res) => {
    try {
        const { sede, co_tab } = req.query;
        const servers = getServers();
        const target = (sede ? servers.find(s => s.id === sede) : null) || servers[0];

        if (!target) {
            return res.status(404).json({ success: false, message: 'Sede no configurada.' });
        }

        const pool = await getPool(target.id, req.sqlAuth);
        const request = pool.request();
        let query = '';

        if (co_tab && String(co_tab).trim() !== '') {
            request.input('co_tab', sql.Char(6), padProfit(String(co_tab).trim(), 6));
            query = `
                SELECT 
                    RTRIM(co_tab) AS co_tab,
                    RTRIM(co_islr) AS co_islr,
                    RTRIM(islr_des) AS islr_des,
                    RTRIM(ISNULL(islr_deslarga, islr_des)) AS islr_deslarga,
                    ISNULL(porc_imp, 100.0) AS porc_imp,
                    ISNULL(porc_ret, 0.0) AS porc_ret,
                    ISNULL(sustraen, 0.0) AS sustraen
                FROM v_saConISLR_saTabuladorISLR
                WHERE LTRIM(RTRIM(co_tab)) = LTRIM(RTRIM(@co_tab))
                ORDER BY co_islr
            `;
        } else {
            query = `
                SELECT 
                    RTRIM(co_tab) AS co_tab,
                    RTRIM(co_islr) AS co_islr,
                    RTRIM(islr_des) AS islr_des,
                    RTRIM(ISNULL(islr_deslarga, islr_des)) AS islr_deslarga,
                    ISNULL(porc_imp, 100.0) AS porc_imp,
                    ISNULL(porc_ret, 0.0) AS porc_ret,
                    ISNULL(sustraen, 0.0) AS sustraen
                FROM v_saConISLR_saTabuladorISLR
                ORDER BY co_tab, co_islr
            `;
        }

        const [result, utRes, tabRes] = await Promise.all([
            request.query(query),
            pool.request().query(`SELECT TOP 1 valor, CONVERT(VARCHAR(10), co_fec, 120) AS co_fec FROM saUnidadTributaria ORDER BY co_fec DESC`).catch(() => ({ recordset: [{ valor: 43 }] })),
            pool.request().query(`SELECT RTRIM(co_tab) AS co_tab, RTRIM(tab_des) AS tab_des, RTRIM(tipo_per) AS tipo_per FROM saTabuladorIslr ORDER BY co_tab`).catch(() => ({ recordset: [] }))
        ]);

        res.status(200).json({
            success: true,
            data: result.recordset,
            unidad_tributaria: Number(utRes.recordset[0]?.valor || 43),
            tabuladores: tabRes.recordset
        });
    } catch (error) {
        res.status(500).json({ success: false, message: 'Error al consultar conceptos de ISLR.', error: error.message });
    }
});

// --- OBTENER DETALLE DE UN PAGO ---
router.get('/:cob_num', async (req, res) => {
    try {
        const cob_num = (req.params.cob_num || '').trim();
        const sede = req.query.sede || req.headers['x-branch-id'];
        const servers = getServers();
        const targets = sede ? servers.filter(s => s.id === sede) : servers;

        if (targets.length === 0)
            return res.status(404).json({ success: false, message: `Sede "${sede}" no encontrada.` });

        const results = await Promise.all(targets.map(async (srv) => {
            try {
                const pool = await getPool(srv.id, req.sqlAuth);

                const resEnc = await pool.request().input('cob_num', sql.VarChar, cob_num).query(`
                        SELECT RTRIM(p.cob_num) AS cob_num, RTRIM(p.recibo) AS recibo, RTRIM(p.descrip) AS descrip,
                               RTRIM(p.co_prov) AS co_prov, RTRIM(pr.prov_des) AS prov_des, RTRIM(pr.rif) AS rif,
                               ISNULL(pr.porc_esp, 0) AS porc_esp, pr.contribu_e,
                               RTRIM(ISNULL(pr.tipo_per, '3')) AS tipo_per, RTRIM(ISNULL(pr.co_tab, '')) AS co_tab,
                               RTRIM(ISNULL(pr.direc1, '')) AS direc1, '' AS direc2, RTRIM(ISNULL(pr.telefonos, '')) AS telefonos, RTRIM(ISNULL(pr.email, '')) AS email,
                               ISNULL(p.fecha, p.fe_us_in) AS fecha, p.anulado,
                               ISNULL((SELECT SUM(mont_doc) FROM saPagoTPReng WHERE cob_num = p.cob_num), 0) AS monto,
                               RTRIM(p.co_mone) AS co_mone, 
                               CASE WHEN p.tasa <= 1.000001 THEN 
                                    ISNULL((SELECT TOP 1 t.tasa_v FROM saTasa t WHERE LTRIM(RTRIM(t.co_mone)) IN ('USD', 'US$', 'US') AND CONVERT(VARCHAR(10), t.fecha, 120) <= CONVERT(VARCHAR(10), ISNULL(p.fecha, p.fe_us_in), 120) ORDER BY t.fecha DESC), 1.0)
                               ELSE p.tasa END AS tasa,
                               RTRIM(p.co_us_in) AS co_us_in
                        FROM saPago p
                        LEFT JOIN saProveedor pr ON p.co_prov = pr.co_prov
                        WHERE LTRIM(RTRIM(p.cob_num)) = LTRIM(RTRIM(@cob_num))
                    `);

                const [resReng, resTP, resIva, resIslr] = await Promise.all([
                    pool.request().input('cob_num', sql.VarChar, cob_num).query(`
                        SELECT r.reng_num, RTRIM(r.co_tipo_doc) AS co_tipo_doc, RTRIM(r.nro_doc) AS nro_doc,
                               COALESCE(NULLIF(RTRIM(f.nro_fact), ''), NULLIF(RTRIM(d.nro_fact), ''), NULLIF(RTRIM(r.nro_fact), ''), '') AS nro_fact,
                               r.mont_cob, r.monto_retencion_iva, r.monto_retencion,
                               r.rowguid, r.rowguid_reng_ori,
                               COALESCE(f.fec_emis, d.fec_emis) AS fec_emis,
                               COALESCE(NULLIF(RTRIM(f.n_control), ''), NULLIF(RTRIM(d.n_control), ''), '') AS n_control,
                               RTRIM(ISNULL(d.nro_orig, '')) AS nro_orig,
                               COALESCE(NULLIF(f.total_bruto, 0), NULLIF(d.total_bruto, 0), 0) AS total_bruto,
                               COALESCE(NULLIF(f.monto_imp, 0), NULLIF(d.monto_imp, 0), 0) AS monto_imp,
                               COALESCE(NULLIF(f.total_neto, 0), NULLIF(d.total_neto, 0), 0) AS total_neto,
                               ISNULL(CASE
                                   WHEN reng_calc.has_reng = 1 THEN ISNULL(reng_calc.exento_reng, 0)
                                   WHEN COALESCE(NULLIF(f.total_neto, 0), NULLIF(d.total_neto, 0), 0) > 0 AND (COALESCE(NULLIF(f.total_neto, 0), NULLIF(d.total_neto, 0), 0) - COALESCE(NULLIF(f.monto_imp, 0), NULLIF(d.monto_imp, 0), 0) - ISNULL(reng_calc.base_gravada_reng, 0)) > 1 THEN
                                       (COALESCE(NULLIF(f.total_neto, 0), NULLIF(d.total_neto, 0), 0) - COALESCE(NULLIF(f.monto_imp, 0), NULLIF(d.monto_imp, 0), 0) - ISNULL(reng_calc.base_gravada_reng, 0))
                                   WHEN ISNULL(COALESCE(f.monto_imp, d.monto_imp, 0), 0) = 0 THEN ISNULL(NULLIF(d.otros2, 0), ISNULL(NULLIF(f.total_bruto, d.total_bruto), d.total_neto))
                                   ELSE ISNULL(d.otros2, 0)
                               END, 0) AS monto_exento,
                               ISNULL(CASE
                                   WHEN reng_calc.has_reng = 1 THEN ISNULL(reng_calc.base_gravada_reng, 0)
                                   WHEN ISNULL(COALESCE(f.monto_imp, d.monto_imp, 0), 0) = 0 THEN 0
                                   ELSE (COALESCE(NULLIF(f.total_neto, 0), d.total_neto) - COALESCE(NULLIF(f.monto_imp, 0), d.monto_imp) - ISNULL(d.otros1, 0) - ISNULL(d.otros2, 0) - ISNULL(d.otros3, 0))
                               END, 0) AS base_imponible,
                               COALESCE(
                                   NULLIF(reng_calc.alicuota_reng, 0),
                                   CASE 
                                       WHEN d.porc_imp IN (8, 16, 31) THEN d.porc_imp
                                       WHEN ISNULL(COALESCE(f.monto_imp, d.monto_imp, 0), 0) > 0 THEN 16
                                       ELSE 0
                                   END
                               ) AS porc_imp,
                               CASE 
                                   WHEN UPPER(RTRIM(r.co_tipo_doc)) IN ('N/CR', 'NCR', 'N/DB', 'NDEB') THEN
                                       COALESCE(NULLIF(RTRIM(afec.nro_fact_origen), ''), NULLIF(RTRIM(d.nro_orig), ''), '0')
                                   ELSE '0'
                               END AS nro_fact_afectado
                        FROM saPagoDocReng r
                        LEFT JOIN saDocumentoCompra d ON LTRIM(RTRIM(r.co_tipo_doc)) = LTRIM(RTRIM(d.co_tipo_doc)) 
                                                     AND LTRIM(RTRIM(r.nro_doc)) = LTRIM(RTRIM(d.nro_doc))
                        LEFT JOIN saFacturaCompra f ON (
                            LTRIM(RTRIM(f.doc_num)) = LTRIM(RTRIM(r.nro_doc))
                            OR (NULLIF(RTRIM(d.nro_fact), '') IS NOT NULL AND LTRIM(RTRIM(f.nro_fact)) = LTRIM(RTRIM(d.nro_fact)))
                            OR (NULLIF(RTRIM(r.nro_fact), '') IS NOT NULL AND LTRIM(RTRIM(f.nro_fact)) = LTRIM(RTRIM(r.nro_fact)))
                            OR (NULLIF(RTRIM(d.nro_orig), '') IS NOT NULL AND LTRIM(RTRIM(f.doc_num)) = LTRIM(RTRIM(d.nro_orig)))
                        )
                        OUTER APPLY (
                            SELECT
                                MAX(src.has_reng) AS has_reng,
                                SUM(src.exento_reng) AS exento_reng,
                                SUM(src.base_gravada_reng) AS base_gravada_reng,
                                MAX(src.alicuota_reng) AS alicuota_reng
                            FROM (
                                SELECT
                                    1 AS has_reng,
                                    CASE WHEN ISNULL(fr.porc_imp, 0) = 0 THEN ISNULL(fr.reng_neto, 0) ELSE 0 END AS exento_reng,
                                    CASE WHEN ISNULL(fr.porc_imp, 0) > 0 THEN ISNULL(fr.reng_neto, 0) ELSE 0 END AS base_gravada_reng,
                                    CASE WHEN ISNULL(fr.porc_imp, 0) > 0 THEN fr.porc_imp ELSE NULL END AS alicuota_reng
                                FROM saFacturaCompraReng fr
                                WHERE (
                                    (f.doc_num IS NOT NULL AND LTRIM(RTRIM(fr.doc_num)) = LTRIM(RTRIM(f.doc_num)))
                                    OR LTRIM(RTRIM(fr.doc_num)) = LTRIM(RTRIM(r.nro_doc))
                                    OR (d.nro_doc IS NOT NULL AND LTRIM(RTRIM(fr.doc_num)) = LTRIM(RTRIM(d.nro_doc)))
                                    OR (UPPER(LTRIM(RTRIM(ISNULL(d.doc_orig, '')))) IN ('FACT', 'COMP') AND NULLIF(RTRIM(d.nro_orig), '') IS NOT NULL AND LTRIM(RTRIM(fr.doc_num)) = LTRIM(RTRIM(d.nro_orig)))
                                )
                                UNION ALL
                                SELECT
                                    1 AS has_reng,
                                    CASE WHEN ISNULL(dr.porc_imp, 0) = 0 THEN ISNULL(dr.reng_neto, 0) ELSE 0 END AS exento_reng,
                                    CASE WHEN ISNULL(dr.porc_imp, 0) > 0 THEN ISNULL(dr.reng_neto, 0) ELSE 0 END AS base_gravada_reng,
                                    CASE WHEN ISNULL(dr.porc_imp, 0) > 0 THEN dr.porc_imp ELSE NULL END AS alicuota_reng
                                FROM saDevolucionProveedorReng dr
                                WHERE UPPER(LTRIM(RTRIM(d.co_tipo_doc))) IN ('N/CR', 'NCR')
                                  AND (
                                      (NULLIF(RTRIM(d.nro_orig), '') IS NOT NULL AND LTRIM(RTRIM(dr.doc_num)) = LTRIM(RTRIM(d.nro_orig)))
                                      OR LTRIM(RTRIM(dr.doc_num)) = LTRIM(RTRIM(d.nro_doc))
                                  )
                            ) src
                        ) reng_calc
                        OUTER APPLY (
                            SELECT TOP 1
                                COALESCE(
                                    NULLIF(RTRIM(d_afec.nro_fact), ''),
                                    NULLIF(RTRIM(d_dev.nro_fact), ''),
                                    NULLIF(RTRIM(d_afec.nro_doc), '')
                                ) AS nro_fact_origen
                            FROM (SELECT 1 AS _x) _dummy
                            LEFT JOIN saDocumentoCompra d_afec ON (
                                UPPER(LTRIM(RTRIM(d.co_tipo_doc))) IN ('N/CR', 'NCR', 'N/DB', 'NDEB')
                                AND NULLIF(RTRIM(d.nro_orig), '') IS NOT NULL
                                AND LTRIM(RTRIM(d_afec.nro_doc)) = LTRIM(RTRIM(d.nro_orig))
                                AND UPPER(LTRIM(RTRIM(d_afec.co_tipo_doc))) NOT IN ('IVAN', 'ISLR', 'N/CR', 'NCR')
                            )
                            LEFT JOIN saDevolucionProveedorReng dpr ON (
                                UPPER(LTRIM(RTRIM(d.co_tipo_doc))) IN ('N/CR', 'NCR')
                                AND d_afec.nro_doc IS NULL
                                AND (
                                    (NULLIF(RTRIM(d.nro_orig), '') IS NOT NULL AND LTRIM(RTRIM(dpr.doc_num)) = LTRIM(RTRIM(d.nro_orig)))
                                    OR LTRIM(RTRIM(dpr.doc_num)) = LTRIM(RTRIM(d.nro_doc))
                                )
                            )
                            LEFT JOIN saDocumentoCompra d_dev ON (
                                LTRIM(RTRIM(d_dev.nro_doc)) = LTRIM(RTRIM(dpr.num_doc))
                                AND UPPER(LTRIM(RTRIM(d_dev.co_tipo_doc))) IN ('FACT', 'N/DB', 'NDEB')
                            )
                        ) afec
                        WHERE LTRIM(RTRIM(r.cob_num)) = LTRIM(RTRIM(@cob_num))
                        ORDER BY r.reng_num
                    `),
                    pool.request().input('cob_num', sql.VarChar, cob_num).query(`
                        SELECT tp.reng_num, RTRIM(tp.forma_pag) AS forma_pag, tp.mont_doc,
                               RTRIM(tp.cod_caja) AS cod_caja, RTRIM(cj.descrip) AS caja_des,
                               RTRIM(tp.cod_cta) AS cod_cta, RTRIM(cb.num_cta) AS cta_des,
                               RTRIM(cb.co_ban) AS co_ban, RTRIM(b.des_ban) AS ban_des,
                               RTRIM(tp.num_doc) AS num_doc, tp.fecha_che,
                               RTRIM(tp.mov_num_c) AS mov_num_c, RTRIM(tp.mov_num_b) AS mov_num_b
                        FROM saPagoTPReng tp
                        LEFT JOIN saCaja cj ON tp.cod_caja = cj.cod_caja
                        LEFT JOIN saCuentaBancaria cb ON tp.cod_cta = cb.cod_cta
                        LEFT JOIN saBanco b ON cb.co_ban = b.co_ban
                        WHERE LTRIM(RTRIM(tp.cob_num)) = LTRIM(RTRIM(@cob_num))
                        ORDER BY tp.reng_num
                    `),
                    pool.request().input('cob_num', sql.VarChar, cob_num).query(`
                        SELECT ri.reng_num,
                               COALESCE(doc_reng.rowguid, ri.rowguid_reng_cob) AS rowguid_reng_cob,
                               COALESCE(NULLIF(RTRIM(ri.num_comprobante), ''), NULLIF(RTRIM(ivan_doc.num_comprobante), ''), RTRIM(ivan_doc.nro_doc), '') AS num_comprobante,
                               ISNULL(ivan_doc.fec_emis, p.fecha) AS fec_emis_ivan,
                               COALESCE(NULLIF(orig.total_neto, 0), NULLIF(ri.monto_documento, 0), 0) AS monto_documento,
                               CASE
                                   WHEN reng_calc.has_reng = 1 THEN ISNULL(reng_calc.base_gravada_reng, 0)
                                   WHEN ISNULL(orig.monto_imp, 0) = 0 AND orig.nro_doc IS NOT NULL THEN 0
                                   WHEN orig.nro_doc IS NOT NULL AND ISNULL(orig.monto_imp, 0) > 0 THEN
                                       (orig.total_neto - orig.monto_imp - ISNULL(orig.otros1, 0) - ISNULL(orig.otros2, 0) - ISNULL(orig.otros3, 0))
                                   ELSE ISNULL(ri.base_imponible, 0)
                               END AS base_imponible,
                               ri.monto_ret_imp,
                               COALESCE(
                                   NULLIF(reng_calc.alicuota_reng, 0),
                                   CASE
                                       WHEN orig.porc_imp IN (8, 16, 31) THEN orig.porc_imp
                                       WHEN ISNULL(orig.monto_imp, 0) > 0 THEN 16.0
                                       WHEN ri.alicuota > 0 THEN ri.alicuota
                                       ELSE 0.0
                                   END
                               ) AS alicuota,
                               ISNULL(orig.monto_imp, ROUND(ISNULL(ri.base_imponible, 0) * (ISNULL(ri.alicuota, 16.0) / 100.0), 2)) AS monto_imp_causado,
                               CASE
                                   WHEN reng_calc.has_reng = 1 THEN ISNULL(reng_calc.exento_reng, 0)
                                   WHEN ISNULL(orig.monto_imp, 0) = 0 AND orig.nro_doc IS NOT NULL THEN ISNULL(NULLIF(orig.otros2, 0), ISNULL(NULLIF(orig.total_bruto, 0), orig.total_neto))
                                   WHEN ISNULL(orig.otros2, 0) > 0 THEN orig.otros2
                                   ELSE ISNULL(ri.monto_excento, 0)
                               END AS monto_excento,
                               RTRIM(ri.periodo_impositivo) AS periodo_impositivo,
                               COALESCE(fact_orig.fec_emis, orig.fec_emis, ri.fecha_documento, p.fecha) AS fecha_documento,
                               COALESCE(NULLIF(RTRIM(fact_orig.n_control), ''), NULLIF(RTRIM(orig.n_control), ''), NULLIF(RTRIM(ri.numero_control_documento), ''), '---') AS numero_control_documento,
                               COALESCE(NULLIF(RTRIM(orig.co_tipo_doc), ''), NULLIF(RTRIM(doc_reng.co_tipo_doc), ''), NULLIF(RTRIM(ri.tipo_documento), ''), 'FACT') AS tipo_documento,
                               ISNULL(RTRIM(ri.tipo_operacion), 'C') AS tipo_operacion,
                               CASE
                                   WHEN UPPER(LTRIM(RTRIM(COALESCE(orig.co_tipo_doc, doc_reng.co_tipo_doc, ri.tipo_documento)))) IN ('N/CR', 'NCR', 'N/DB', 'NDEB') THEN
                                       COALESCE(
                                           NULLIF(RTRIM(afec.nro_fact_origen), ''),
                                           NULLIF(RTRIM(orig.nro_orig), ''),
                                           NULLIF(RTRIM(CASE WHEN RTRIM(ri.numero_documento_afectado) <> RTRIM(COALESCE(orig.nro_fact, '')) THEN ri.numero_documento_afectado ELSE NULL END), ''),
                                           '0'
                                       )
                                   ELSE '0'
                               END AS numero_documento_afectado,
                               RTRIM(COALESCE(doc_reng.nro_doc, orig.nro_doc, '')) AS nro_doc,
                               RTRIM(COALESCE(NULLIF(RTRIM(fact_orig.nro_fact), ''), NULLIF(RTRIM(orig.nro_fact), ''), NULLIF(RTRIM(doc_reng.nro_fact), ''), NULLIF(RTRIM(ri.numero_documento), ''), RTRIM(orig.nro_doc), RTRIM(doc_reng.nro_doc), '')) AS nro_fact,
                               RTRIM(COALESCE(NULLIF(RTRIM(orig.nro_fact), ''), NULLIF(RTRIM(doc_reng.nro_fact), ''), NULLIF(RTRIM(ri.numero_documento), ''), RTRIM(orig.nro_doc), RTRIM(doc_reng.nro_doc), '')) AS numero_documento,
                               RTRIM(COALESCE(orig.co_tipo_doc, doc_reng.co_tipo_doc, ri.tipo_documento, 'FACT')) AS co_tipo_doc,
                               CASE
                                   WHEN ISNULL(pr.porc_esp, 0) >= 88 THEN 100
                                   WHEN ISNULL(pr.porc_esp, 0) > 0 THEN 75
                                   WHEN ISNULL(orig.monto_imp, 0) > 0 AND (ri.monto_ret_imp / orig.monto_imp) >= 0.88 THEN 100
                                   ELSE 75
                               END AS porc_ret
                        FROM saPagoRetenIvaReng ri
                        INNER JOIN saPagoDocReng pdr ON ri.rowguid_reng_cob = pdr.rowguid
                        INNER JOIN saPago p ON pdr.cob_num = p.cob_num
                        LEFT JOIN saProveedor pr ON p.co_prov = pr.co_prov
                        OUTER APPLY (
                            SELECT TOP 1 r_doc.*
                            FROM saPagoDocReng r_doc
                            WHERE (
                                UPPER(LTRIM(RTRIM(pdr.co_tipo_doc))) NOT IN ('IVAN', 'ISLR', 'AJPA', 'AJNA', 'AJNM', 'AJPM', 'ADEL')
                                AND r_doc.rowguid = pdr.rowguid
                            )
                            OR (
                                UPPER(LTRIM(RTRIM(pdr.co_tipo_doc))) = 'IVAN'
                                AND r_doc.cob_num = pdr.cob_num
                                AND UPPER(LTRIM(RTRIM(r_doc.co_tipo_doc))) NOT IN ('IVAN', 'ISLR', 'AJPA', 'AJNA', 'AJNM', 'AJPM', 'ADEL')
                            )
                            ORDER BY 
                                CASE 
                                    WHEN UPPER(LTRIM(RTRIM(pdr.co_tipo_doc))) NOT IN ('IVAN', 'ISLR', 'AJPA', 'AJNA', 'AJNM', 'AJPM', 'ADEL') AND r_doc.rowguid = pdr.rowguid THEN 0
                                    WHEN pdr.rowguid_reng_ori IS NOT NULL AND r_doc.rowguid = pdr.rowguid_reng_ori THEN 1
                                    WHEN NULLIF(RTRIM(ri.numero_documento), '') IS NOT NULL AND LTRIM(RTRIM(r_doc.nro_fact)) = LTRIM(RTRIM(ri.numero_documento)) THEN 2
                                    WHEN NULLIF(RTRIM(ri.numero_documento), '') IS NOT NULL AND LTRIM(RTRIM(r_doc.nro_doc)) = LTRIM(RTRIM(ri.numero_documento)) THEN 3
                                    WHEN UPPER(LTRIM(RTRIM(r_doc.co_tipo_doc))) = 'FACT' THEN 4
                                    ELSE 5
                                END,
                                r_doc.reng_num
                        ) doc_reng
                        LEFT JOIN saDocumentoCompra orig ON LTRIM(RTRIM(doc_reng.co_tipo_doc)) = LTRIM(RTRIM(orig.co_tipo_doc)) 
                                                        AND LTRIM(RTRIM(doc_reng.nro_doc)) = LTRIM(RTRIM(orig.nro_doc))
                        LEFT JOIN saFacturaCompra fact_orig ON (
                            LTRIM(RTRIM(fact_orig.doc_num)) = LTRIM(RTRIM(doc_reng.nro_doc))
                            OR (NULLIF(RTRIM(orig.nro_fact), '') IS NOT NULL AND LTRIM(RTRIM(fact_orig.nro_fact)) = LTRIM(RTRIM(orig.nro_fact)))
                            OR (NULLIF(RTRIM(doc_reng.nro_fact), '') IS NOT NULL AND LTRIM(RTRIM(fact_orig.nro_fact)) = LTRIM(RTRIM(doc_reng.nro_fact)))
                            OR (NULLIF(RTRIM(orig.nro_orig), '') IS NOT NULL AND LTRIM(RTRIM(fact_orig.doc_num)) = LTRIM(RTRIM(orig.nro_orig)))
                        )
                        OUTER APPLY (
                            SELECT TOP 1 dc_iv.fec_emis, dc_iv.num_comprobante, dc_iv.nro_doc
                            FROM saDocumentoCompra dc_iv
                            WHERE UPPER(RTRIM(dc_iv.co_tipo_doc)) = 'IVAN'
                              AND (
                                  (UPPER(LTRIM(RTRIM(pdr.co_tipo_doc))) = 'IVAN' AND LTRIM(RTRIM(dc_iv.nro_doc)) = LTRIM(RTRIM(pdr.nro_doc)))
                                  OR (dc_iv.doc_orig = 'PAGO' AND LTRIM(RTRIM(dc_iv.nro_orig)) = LTRIM(RTRIM(p.cob_num)))
                              )
                            ORDER BY CASE WHEN UPPER(LTRIM(RTRIM(pdr.co_tipo_doc))) = 'IVAN' AND LTRIM(RTRIM(dc_iv.nro_doc)) = LTRIM(RTRIM(pdr.nro_doc)) THEN 0 ELSE 1 END
                        ) ivan_doc
                        OUTER APPLY (
                            SELECT
                                MAX(src.has_reng) AS has_reng,
                                SUM(src.exento_reng) AS exento_reng,
                                SUM(src.base_gravada_reng) AS base_gravada_reng,
                                MAX(src.alicuota_reng) AS alicuota_reng
                            FROM (
                                SELECT
                                    1 AS has_reng,
                                    CASE WHEN ISNULL(fr.porc_imp, 0) = 0 THEN ISNULL(fr.reng_neto, 0) ELSE 0 END AS exento_reng,
                                    CASE WHEN ISNULL(fr.porc_imp, 0) > 0 THEN ISNULL(fr.reng_neto, 0) ELSE 0 END AS base_gravada_reng,
                                    CASE WHEN ISNULL(fr.porc_imp, 0) > 0 THEN fr.porc_imp ELSE NULL END AS alicuota_reng
                                FROM saFacturaCompraReng fr
                                WHERE (
                                    (fact_orig.doc_num IS NOT NULL AND LTRIM(RTRIM(fr.doc_num)) = LTRIM(RTRIM(fact_orig.doc_num)))
                                    OR LTRIM(RTRIM(fr.doc_num)) = LTRIM(RTRIM(doc_reng.nro_doc))
                                    OR (orig.nro_doc IS NOT NULL AND LTRIM(RTRIM(fr.doc_num)) = LTRIM(RTRIM(orig.nro_doc)))
                                    OR (UPPER(LTRIM(RTRIM(ISNULL(orig.doc_orig, '')))) IN ('FACT', 'COMP') AND NULLIF(RTRIM(orig.nro_orig), '') IS NOT NULL AND LTRIM(RTRIM(fr.doc_num)) = LTRIM(RTRIM(orig.nro_orig)))
                                )
                                UNION ALL
                                SELECT
                                    1 AS has_reng,
                                    CASE WHEN ISNULL(dr.porc_imp, 0) = 0 THEN ISNULL(dr.reng_neto, 0) ELSE 0 END AS exento_reng,
                                    CASE WHEN ISNULL(dr.porc_imp, 0) > 0 THEN ISNULL(dr.reng_neto, 0) ELSE 0 END AS base_gravada_reng,
                                    CASE WHEN ISNULL(dr.porc_imp, 0) > 0 THEN dr.porc_imp ELSE NULL END AS alicuota_reng
                                FROM saDevolucionProveedorReng dr
                                WHERE UPPER(LTRIM(RTRIM(orig.co_tipo_doc))) IN ('N/CR', 'NCR')
                                  AND (
                                      (NULLIF(RTRIM(orig.nro_orig), '') IS NOT NULL AND LTRIM(RTRIM(dr.doc_num)) = LTRIM(RTRIM(orig.nro_orig)))
                                      OR LTRIM(RTRIM(dr.doc_num)) = LTRIM(RTRIM(orig.nro_doc))
                                  )
                            ) src
                        ) reng_calc
                        OUTER APPLY (
                            SELECT TOP 1
                                COALESCE(
                                    NULLIF(RTRIM(d_afec.nro_fact), ''),
                                    NULLIF(RTRIM(d_dev.nro_fact), ''),
                                    NULLIF(RTRIM(d_afec.nro_doc), '')
                                ) AS nro_fact_origen
                            FROM (SELECT 1 AS _x) _dummy
                            LEFT JOIN saDocumentoCompra d_afec ON (
                                UPPER(LTRIM(RTRIM(orig.co_tipo_doc))) IN ('N/CR', 'NCR', 'N/DB', 'NDEB')
                                AND NULLIF(RTRIM(orig.nro_orig), '') IS NOT NULL
                                AND LTRIM(RTRIM(d_afec.nro_doc)) = LTRIM(RTRIM(orig.nro_orig))
                                AND UPPER(LTRIM(RTRIM(d_afec.co_tipo_doc))) NOT IN ('IVAN', 'ISLR', 'N/CR', 'NCR')
                            )
                            LEFT JOIN saDevolucionProveedorReng dpr ON (
                                UPPER(LTRIM(RTRIM(orig.co_tipo_doc))) IN ('N/CR', 'NCR')
                                AND d_afec.nro_doc IS NULL
                                AND (
                                    (NULLIF(RTRIM(orig.nro_orig), '') IS NOT NULL AND LTRIM(RTRIM(dpr.doc_num)) = LTRIM(RTRIM(orig.nro_orig)))
                                    OR LTRIM(RTRIM(dpr.doc_num)) = LTRIM(RTRIM(orig.nro_doc))
                                )
                            )
                            LEFT JOIN saDocumentoCompra d_dev ON (
                                LTRIM(RTRIM(d_dev.nro_doc)) = LTRIM(RTRIM(dpr.num_doc))
                                AND UPPER(LTRIM(RTRIM(d_dev.co_tipo_doc))) IN ('FACT', 'N/DB', 'NDEB')
                            )
                        ) afec
                        WHERE LTRIM(RTRIM(pdr.cob_num)) = LTRIM(RTRIM(@cob_num))
                        UNION ALL
                        SELECT r.reng_num, r.rowguid_reng_ori AS rowguid_reng_cob, RTRIM(COALESCE(NULLIF(RTRIM(d.num_comprobante), ''), RTRIM(d.nro_doc), '')) AS num_comprobante,
                               ISNULL(d.fec_emis, p.fecha) AS fec_emis_ivan,
                               ISNULL(orig.total_neto, orig.total_bruto) AS monto_documento, 
                               CASE
                                   WHEN reng_calc.has_reng = 1 THEN ISNULL(reng_calc.base_gravada_reng, 0)
                                   WHEN ISNULL(orig.monto_imp, 0) = 0 THEN 0
                                   ELSE (orig.total_neto - orig.monto_imp - ISNULL(orig.otros1, 0) - ISNULL(orig.otros2, 0) - ISNULL(orig.otros3, 0))
                               END AS base_imponible, 
                               d.total_neto AS monto_ret_imp, 
                               COALESCE(
                                   NULLIF(reng_calc.alicuota_reng, 0),
                                   CASE
                                       WHEN orig.porc_imp IN (8, 16, 31) THEN orig.porc_imp
                                       WHEN ISNULL(orig.monto_imp, 0) > 0 THEN 16.0
                                       ELSE 0.0
                                   END
                               ) AS alicuota,
                               ISNULL(orig.monto_imp, 0) AS monto_imp_causado,
                               CASE
                                   WHEN reng_calc.has_reng = 1 THEN ISNULL(reng_calc.exento_reng, 0)
                                   WHEN ISNULL(orig.monto_imp, 0) = 0 THEN ISNULL(NULLIF(orig.otros2, 0), ISNULL(NULLIF(orig.total_bruto, 0), orig.total_neto))
                                   ELSE ISNULL(orig.otros2, 0)
                               END AS monto_excento,
                               CONVERT(VARCHAR(6), ISNULL(d.fec_emis, p.fecha), 112) AS periodo_impositivo,
                               ISNULL(orig.fec_emis, p.fecha) AS fecha_documento,
                               ISNULL(NULLIF(RTRIM(orig.n_control), ''), '---') AS numero_control_documento,
                               RTRIM(orig.co_tipo_doc) AS tipo_documento,
                               'C' AS tipo_operacion,
                               CASE
                                   WHEN UPPER(LTRIM(RTRIM(COALESCE(orig.co_tipo_doc, pdr.co_tipo_doc)))) IN ('N/CR', 'NCR', 'N/DB', 'NDEB') THEN
                                       COALESCE(NULLIF(RTRIM(afec.nro_fact_origen), ''), NULLIF(RTRIM(orig.nro_orig), ''), '0')
                                   ELSE '0'
                               END AS numero_documento_afectado,
                               RTRIM(pdr.nro_doc) AS nro_doc,
                               RTRIM(COALESCE(NULLIF(RTRIM(orig.nro_fact), ''), NULLIF(RTRIM(pdr.nro_fact), ''), RTRIM(pdr.nro_doc))) AS nro_fact,
                               RTRIM(COALESCE(NULLIF(RTRIM(orig.nro_fact), ''), NULLIF(RTRIM(pdr.nro_fact), ''), RTRIM(pdr.nro_doc))) AS numero_documento,
                               RTRIM(orig.co_tipo_doc) AS co_tipo_doc,
                               CASE 
                                   WHEN ISNULL(pr.porc_esp, 0) >= 88 THEN 100
                                   WHEN ISNULL(pr.porc_esp, 0) > 0 THEN 75
                                   WHEN ISNULL(orig.monto_imp, 0) > 0 AND (d.total_neto / orig.monto_imp) >= 0.88 THEN 100
                                   ELSE 75
                               END AS porc_ret
                        FROM saPagoDocReng r
                        INNER JOIN saDocumentoCompra d ON LTRIM(RTRIM(r.co_tipo_doc)) = LTRIM(RTRIM(d.co_tipo_doc)) 
                                                      AND LTRIM(RTRIM(r.nro_doc)) = LTRIM(RTRIM(d.nro_doc))
                        INNER JOIN saPago p ON r.cob_num = p.cob_num
                        LEFT JOIN saProveedor pr ON p.co_prov = pr.co_prov
                        LEFT JOIN saPagoDocReng pdr ON (r.rowguid_reng_ori = pdr.rowguid OR (pdr.cob_num = r.cob_num AND UPPER(LTRIM(RTRIM(pdr.co_tipo_doc))) NOT IN ('IVAN', 'ISLR', 'AJPA', 'AJNA', 'AJNM', 'AJPM', 'ADEL')))
                        LEFT JOIN saDocumentoCompra orig ON LTRIM(RTRIM(pdr.co_tipo_doc)) = LTRIM(RTRIM(orig.co_tipo_doc)) 
                                                        AND LTRIM(RTRIM(pdr.nro_doc)) = LTRIM(RTRIM(orig.nro_doc))
                        OUTER APPLY (
                            SELECT
                                MAX(src.has_reng) AS has_reng,
                                SUM(src.exento_reng) AS exento_reng,
                                SUM(src.base_gravada_reng) AS base_gravada_reng,
                                MAX(src.alicuota_reng) AS alicuota_reng
                            FROM (
                                SELECT
                                    1 AS has_reng,
                                    CASE WHEN ISNULL(fr.porc_imp, 0) = 0 THEN ISNULL(fr.reng_neto, 0) ELSE 0 END AS exento_reng,
                                    CASE WHEN ISNULL(fr.porc_imp, 0) > 0 THEN ISNULL(fr.reng_neto, 0) ELSE 0 END AS base_gravada_reng,
                                    CASE WHEN ISNULL(fr.porc_imp, 0) > 0 THEN fr.porc_imp ELSE NULL END AS alicuota_reng
                                FROM saFacturaCompraReng fr
                                WHERE UPPER(LTRIM(RTRIM(orig.co_tipo_doc))) = 'FACT'
                                  AND (
                                      LTRIM(RTRIM(fr.doc_num)) = LTRIM(RTRIM(orig.nro_doc))
                                      OR (UPPER(LTRIM(RTRIM(ISNULL(orig.doc_orig, '')))) IN ('FACT', 'COMP') AND NULLIF(RTRIM(orig.nro_orig), '') IS NOT NULL AND LTRIM(RTRIM(fr.doc_num)) = LTRIM(RTRIM(orig.nro_orig)))
                                  )
                                UNION ALL
                                SELECT
                                    1 AS has_reng,
                                    CASE WHEN ISNULL(dr.porc_imp, 0) = 0 THEN ISNULL(dr.reng_neto, 0) ELSE 0 END AS exento_reng,
                                    CASE WHEN ISNULL(dr.porc_imp, 0) > 0 THEN ISNULL(dr.reng_neto, 0) ELSE 0 END AS base_gravada_reng,
                                    CASE WHEN ISNULL(dr.porc_imp, 0) > 0 THEN dr.porc_imp ELSE NULL END AS alicuota_reng
                                FROM saDevolucionProveedorReng dr
                                WHERE UPPER(LTRIM(RTRIM(orig.co_tipo_doc))) IN ('N/CR', 'NCR')
                                  AND (
                                      (NULLIF(RTRIM(orig.nro_orig), '') IS NOT NULL AND LTRIM(RTRIM(dr.doc_num)) = LTRIM(RTRIM(orig.nro_orig)))
                                      OR LTRIM(RTRIM(dr.doc_num)) = LTRIM(RTRIM(orig.nro_doc))
                                  )
                            ) src
                        ) reng_calc
                        OUTER APPLY (
                            SELECT TOP 1
                                COALESCE(
                                    NULLIF(RTRIM(d_afec.nro_fact), ''),
                                    NULLIF(RTRIM(d_dev.nro_fact), ''),
                                    NULLIF(RTRIM(d_afec.nro_doc), '')
                                ) AS nro_fact_origen
                            FROM (SELECT 1 AS _x) _dummy
                            LEFT JOIN saDocumentoCompra d_afec ON (
                                UPPER(LTRIM(RTRIM(orig.co_tipo_doc))) IN ('N/CR', 'NCR', 'N/DB', 'NDEB')
                                AND NULLIF(RTRIM(orig.nro_orig), '') IS NOT NULL
                                AND LTRIM(RTRIM(d_afec.nro_doc)) = LTRIM(RTRIM(orig.nro_orig))
                                AND UPPER(LTRIM(RTRIM(d_afec.co_tipo_doc))) NOT IN ('IVAN', 'ISLR', 'N/CR', 'NCR')
                            )
                            LEFT JOIN saDevolucionProveedorReng dpr ON (
                                UPPER(LTRIM(RTRIM(orig.co_tipo_doc))) IN ('N/CR', 'NCR')
                                AND d_afec.nro_doc IS NULL
                                AND (
                                    (NULLIF(RTRIM(orig.nro_orig), '') IS NOT NULL AND LTRIM(RTRIM(dpr.doc_num)) = LTRIM(RTRIM(orig.nro_orig)))
                                    OR LTRIM(RTRIM(dpr.doc_num)) = LTRIM(RTRIM(orig.nro_doc))
                                )
                            )
                            LEFT JOIN saDocumentoCompra d_dev ON (
                                LTRIM(RTRIM(d_dev.nro_doc)) = LTRIM(RTRIM(dpr.num_doc))
                                AND UPPER(LTRIM(RTRIM(d_dev.co_tipo_doc))) IN ('FACT', 'N/DB', 'NDEB')
                            )
                        ) afec
                        WHERE LTRIM(RTRIM(r.cob_num)) = LTRIM(RTRIM(@cob_num))
                          AND LTRIM(RTRIM(r.co_tipo_doc)) = 'IVAN'
                          AND NOT EXISTS (
                              SELECT 1 FROM saPagoRetenIvaReng ri2
                              INNER JOIN saPagoDocReng pdr2 ON ri2.rowguid_reng_cob = pdr2.rowguid
                              WHERE LTRIM(RTRIM(pdr2.cob_num)) = LTRIM(RTRIM(@cob_num))
                          )
                    `).catch(err => {
                        console.warn('[PAGOS] Advertencia al consultar retenciones IVA para ' + cob_num + ':', err.message);
                        return { recordset: [] };
                    }),
                    pool.request().input('cob_num', sql.VarChar, cob_num).query(`
                        SELECT rn.reng_num,
                               COALESCE(doc_reng.rowguid, rn.rowguid_reng_cob) AS rowguid_reng_cob,
                               RTRIM(rn.co_islr) AS co_islr,
                               rn.monto, rn.monto_reten, rn.monto_obj, rn.porc_retn, CASE WHEN rn.monto_obj * (rn.porc_retn / 100.0) > rn.monto_reten THEN ROUND(rn.monto_obj * (rn.porc_retn / 100.0) - rn.monto_reten, 2) ELSE 0.0 END AS sustraendo,
                               RTRIM(COALESCE(doc_reng.nro_doc, orig.nro_doc, '')) AS nro_doc,
                               RTRIM(COALESCE(NULLIF(RTRIM(orig.nro_fact), ''), NULLIF(RTRIM(doc_reng.nro_fact), ''), RTRIM(doc_reng.nro_doc), '')) AS nro_fact,
                               RTRIM(COALESCE(orig.co_tipo_doc, doc_reng.co_tipo_doc, 'FACT')) AS co_tipo_doc,
                               ISNULL(RTRIM(d.nro_doc), '') AS nro_doc_islr,
                               ISNULL(RTRIM(d.num_comprobante), '') AS num_comprobante,
                               ISNULL(d.fec_emis, p.fecha) AS fec_emis_islr,
                               ISNULL(orig.fec_emis, p.fecha) AS fecha_documento,
                               ISNULL(NULLIF(RTRIM(orig.n_control), ''), '---') AS numero_control_documento,
                               ISNULL(orig.total_neto, ISNULL(orig.total_bruto, rn.monto)) AS monto_documento,
                               ISNULL(NULLIF(RTRIM(orig.observa), ''), RTRIM(p.descrip)) AS doc_descrip,
                               ISNULL((SELECT TOP 1 RTRIM(islr_des) FROM saConISLR WHERE LTRIM(RTRIM(co_islr)) = LTRIM(RTRIM(rn.co_islr))), 'Retención de ISLR') AS islr_des
                        FROM saPagoRentenReng rn
                        INNER JOIN saPagoDocReng pdr ON rn.rowguid_reng_cob = pdr.rowguid
                        INNER JOIN saPago p ON pdr.cob_num = p.cob_num
                        OUTER APPLY (
                            SELECT TOP 1 r_doc.*
                            FROM saPagoDocReng r_doc
                            WHERE (
                                UPPER(LTRIM(RTRIM(pdr.co_tipo_doc))) NOT IN ('IVAN', 'ISLR', 'AJPA', 'AJNA', 'AJNM', 'AJPM', 'ADEL')
                                AND r_doc.rowguid = pdr.rowguid
                            )
                            OR (
                                UPPER(LTRIM(RTRIM(pdr.co_tipo_doc))) = 'ISLR'
                                AND r_doc.cob_num = pdr.cob_num
                                AND UPPER(LTRIM(RTRIM(r_doc.co_tipo_doc))) NOT IN ('IVAN', 'ISLR', 'AJPA', 'AJNA', 'AJNM', 'AJPM', 'ADEL')
                            )
                            ORDER BY 
                                CASE 
                                    WHEN UPPER(LTRIM(RTRIM(pdr.co_tipo_doc))) NOT IN ('IVAN', 'ISLR', 'AJPA', 'AJNA', 'AJNM', 'AJPM', 'ADEL') AND r_doc.rowguid = pdr.rowguid THEN 0
                                    WHEN pdr.rowguid_reng_ori IS NOT NULL AND r_doc.rowguid = pdr.rowguid_reng_ori THEN 1
                                    WHEN UPPER(LTRIM(RTRIM(r_doc.co_tipo_doc))) = 'FACT' THEN 2
                                    ELSE 3
                                END,
                                r_doc.reng_num
                        ) doc_reng
                        LEFT JOIN saDocumentoCompra orig ON LTRIM(RTRIM(doc_reng.co_tipo_doc)) = LTRIM(RTRIM(orig.co_tipo_doc)) 
                                                        AND LTRIM(RTRIM(doc_reng.nro_doc)) = LTRIM(RTRIM(orig.nro_doc))
                        OUTER APPLY (
                            SELECT TOP 1 dc_is.nro_doc, dc_is.num_comprobante, dc_is.fec_emis
                            FROM saDocumentoCompra dc_is
                            WHERE UPPER(RTRIM(dc_is.co_tipo_doc)) = 'ISLR'
                              AND (
                                  (UPPER(LTRIM(RTRIM(pdr.co_tipo_doc))) = 'ISLR' AND LTRIM(RTRIM(dc_is.nro_doc)) = LTRIM(RTRIM(pdr.nro_doc)))
                                  OR (dc_is.doc_orig = 'PAGO' AND LTRIM(RTRIM(dc_is.nro_orig)) = LTRIM(RTRIM(p.cob_num)))
                              )
                            ORDER BY CASE WHEN UPPER(LTRIM(RTRIM(pdr.co_tipo_doc))) = 'ISLR' AND LTRIM(RTRIM(dc_is.nro_doc)) = LTRIM(RTRIM(pdr.nro_doc)) THEN 0 ELSE 1 END
                        ) d
                        WHERE LTRIM(RTRIM(pdr.cob_num)) = LTRIM(RTRIM(@cob_num))
                        UNION ALL
                        SELECT r.reng_num, r.rowguid_reng_ori AS rowguid_reng_cob, '055' AS co_islr,
                               orig.total_bruto AS monto, 
                               d.total_neto AS monto_reten, 
                               orig.total_bruto - ISNULL(orig.otros1, 0) AS monto_obj,
                               CASE WHEN orig.total_bruto - ISNULL(orig.otros1, 0) > 0 THEN ROUND((d.total_neto / (orig.total_bruto - ISNULL(orig.otros1, 0))) * 100, 2) ELSE 2.00 END AS porc_retn,
                               0 AS sustraendo,
                               RTRIM(pdr.nro_doc) AS nro_doc,
                               RTRIM(COALESCE(NULLIF(RTRIM(orig.nro_fact), ''), NULLIF(RTRIM(pdr.nro_fact), ''), RTRIM(pdr.nro_doc))) AS nro_fact,
                               RTRIM(pdr.co_tipo_doc) AS co_tipo_doc,
                               ISNULL(RTRIM(d.nro_doc), '') AS nro_doc_islr,
                               ISNULL(RTRIM(d.num_comprobante), '') AS num_comprobante,
                               ISNULL(d.fec_emis, p.fecha) AS fec_emis_islr,
                               ISNULL(orig.fec_emis, p.fecha) AS fecha_documento,
                               ISNULL(NULLIF(RTRIM(orig.n_control), ''), '---') AS numero_control_documento,
                               ISNULL(orig.total_neto, ISNULL(orig.total_bruto, d.total_bruto)) AS monto_documento,
                               ISNULL(NULLIF(RTRIM(orig.observa), ''), RTRIM(p.descrip)) AS doc_descrip,
                               'Pagos a Empresas Contratistas o Subcontratistas' AS islr_des
                        FROM saPagoDocReng r
                        INNER JOIN saDocumentoCompra d ON LTRIM(RTRIM(r.co_tipo_doc)) = LTRIM(RTRIM(d.co_tipo_doc)) 
                                                      AND LTRIM(RTRIM(r.nro_doc)) = LTRIM(RTRIM(d.nro_doc))
                        INNER JOIN saPago p ON r.cob_num = p.cob_num
                        LEFT JOIN saPagoDocReng pdr ON (r.rowguid_reng_ori = pdr.rowguid OR (pdr.cob_num = r.cob_num AND UPPER(LTRIM(RTRIM(pdr.co_tipo_doc))) NOT IN ('IVAN', 'ISLR', 'AJPA', 'AJNA', 'AJNM', 'AJPM', 'ADEL')))
                        LEFT JOIN saDocumentoCompra orig ON LTRIM(RTRIM(pdr.co_tipo_doc)) = LTRIM(RTRIM(orig.co_tipo_doc)) 
                                                        AND LTRIM(RTRIM(pdr.nro_doc)) = LTRIM(RTRIM(orig.nro_doc))
                        WHERE LTRIM(RTRIM(r.cob_num)) = LTRIM(RTRIM(@cob_num))
                          AND LTRIM(RTRIM(r.co_tipo_doc)) = 'ISLR'
                          AND NOT EXISTS (
                              SELECT 1 FROM saPagoRentenReng rn2
                              INNER JOIN saPagoDocReng pdr2 ON rn2.rowguid_reng_cob = pdr2.rowguid
                              WHERE LTRIM(RTRIM(pdr2.cob_num)) = LTRIM(RTRIM(@cob_num))
                          )
                    `).catch(err => {
                        console.warn('[PAGOS] Advertencia al consultar retenciones ISLR para ' + cob_num + ':', err.message);
                        return { recordset: [] };
                    })
                ]);

                if (!resEnc.recordset.length) return null;

                let finalRetIslr = resIslr.recordset || [];
                if (finalRetIslr.length === 0 && resReng.recordset && resReng.recordset.length > 0) {
                    const facturasConIslr = resReng.recordset.filter(r =>
                        Number(r.monto_retencion || 0) > 0 && r.co_tipo_doc !== 'IVAN' && r.co_tipo_doc !== 'ISLR'
                    );
                    const islrDoc = resReng.recordset.find(r => r.co_tipo_doc === 'ISLR');

                    if (facturasConIslr.length > 0) {
                        finalRetIslr = facturasConIslr.map((f, idx) => {
                            const docBruto = Number(f.total_neto || f.mont_cob || 0);
                            const retIslr = Number(f.monto_retencion || 0);
                            const exentoVal = Number(f.monto_exento || f.exento_reng || f.otros1 || 0);
                            const gravadoBase = Number(f.base_imponible || f.base_gravada_reng || (docBruto - (Number(f.monto_imp) || 0) - exentoVal) || docBruto);

                            // Detectar si la retención fue por Flete (Concepto 072 al 3% sobre el monto exento/flete)
                            const isFlete = exentoVal > 0 && (
                                Math.abs(retIslr - Number((exentoVal * 0.03).toFixed(2))) <= 0.05 ||
                                Math.abs((retIslr / exentoVal) - 0.03) <= 0.005
                            );

                            const coIslr = isFlete ? '072' : '055';
                            const islrDes = isFlete ? 'FLETES PAGADOS A PERSONAS JURÍDICAS O NATURALES' : 'Pagos a Empresas Contratistas o Subcontratistas';
                            const base = isFlete ? exentoVal : (gravadoBase > 0 ? gravadoBase : docBruto);
                            const porc = isFlete ? 3.0 : (base > 0 ? Number(((retIslr / base) * 100).toFixed(2)) : 2.0);

                            return {
                                reng_num: idx + 1,
                                co_islr: coIslr,
                                islr_des: islrDes,
                                monto: docBruto,
                                monto_obj: base,
                                monto_reten: retIslr,
                                porc_retn: porc,
                                sustraendo: 0,
                                num_comprobante: islrDoc?.nro_doc || resEnc.recordset[0]?.recibo || resEnc.recordset[0]?.cob_num,
                                nro_doc_islr: islrDoc?.nro_doc || '',
                                fec_emis_islr: islrDoc?.fec_emis || resEnc.recordset[0]?.fecha,
                                fecha_documento: f.fec_emis || resEnc.recordset[0]?.fecha,
                                numero_control_documento: f.n_control || '---',
                                nro_doc: f.nro_doc,
                                nro_fact: f.nro_fact || f.nro_doc,
                                co_tipo_doc: f.co_tipo_doc || 'FACT',
                                monto_documento: docBruto,
                                doc_descrip: 'Retención de ISLR'
                            };
                        });
                    } else if (islrDoc) {
                        const retIslr = Number(islrDoc.mont_cob || 0);
                        finalRetIslr = [{
                            reng_num: 1,
                            co_islr: '055',
                            islr_des: 'Pagos a Empresas Contratistas o Subcontratistas',
                            monto: retIslr,
                            monto_obj: retIslr,
                            monto_reten: retIslr,
                            porc_retn: 2.0,
                            sustraendo: 0,
                            num_comprobante: islrDoc.nro_doc,
                            nro_doc_islr: islrDoc.nro_doc,
                            fecha_documento: resEnc.recordset[0]?.fecha,
                            numero_control_documento: '---',
                            nro_doc: islrDoc.nro_doc,
                            nro_fact: '',
                            co_tipo_doc: 'ISLR',
                            monto_documento: retIslr,
                            doc_descrip: 'Retención de ISLR'
                        }];
                    }
                }

                // Normalización de comprobantes ISLR existentes
                if (finalRetIslr.length > 0) {
                    finalRetIslr.forEach(r => {
                        // Formatear comprobante fiscal SENIAT (14 dígitos: YYYYMM + 8 dígitos)
                        const rawComp = r.num_comprobante || r.nro_doc_islr || resEnc.recordset[0]?.recibo || resEnc.recordset[0]?.cob_num;
                        if (rawComp) {
                            const islrDateVal = r.fec_emis_islr || resEnc.recordset[0]?.fecha || new Date();
                            const dObj = new Date(islrDateVal);
                            const pYear = isNaN(dObj.getFullYear()) ? new Date().getFullYear() : dObj.getFullYear();
                            const pMonth = isNaN(dObj.getMonth()) ? String(new Date().getMonth() + 1).padStart(2, "0") : String(dObj.getMonth() + 1).padStart(2, "0");
                            const periodStr = `${pYear}${pMonth}`;
                            const compStr = String(rawComp).trim();
                            if (/^20\d{12}$/.test(compStr)) {
                                r.num_comprobante = compStr;
                            } else {
                                const digits = compStr.replace(/\D/g, '');
                                if (digits.length === 14 && /^20\d{12}$/.test(digits)) {
                                    r.num_comprobante = digits;
                                } else if (digits.length > 0) {
                                    const seq8 = digits.length >= 8 ? digits.slice(-8) : digits.padStart(8, '0');
                                    r.num_comprobante = `${periodStr}${seq8}`;
                                }
                            }
                        }

                        if (resReng.recordset) {
                            const matchedFact = resReng.recordset.find(f =>
                                (r.rowguid_reng_cob && f.rowguid === r.rowguid_reng_cob) ||
                                (f.nro_doc && (f.nro_doc === r.nro_doc || f.nro_fact === r.nro_doc)) ||
                                (f.nro_fact && (f.nro_fact === r.nro_fact || f.nro_doc === r.nro_fact))
                            );
                            if (matchedFact) {
                                const exentoVal = Number(matchedFact.monto_exento || matchedFact.exento_reng || matchedFact.otros1 || 0);
                                const retMonto = Number(r.monto_reten || 0);
                                const isFlete = (r.co_islr === '072') || (exentoVal > 0 && (
                                    Math.abs(retMonto - Number((exentoVal * 0.03).toFixed(2))) <= 0.05 ||
                                    Math.abs((retMonto / exentoVal) - 0.03) <= 0.005
                                ));
                                if (isFlete) {
                                    r.co_islr = '072';
                                    r.islr_des = 'FLETES PAGADOS A PERSONAS JURÍDICAS O NATURALES';
                                    r.monto_obj = exentoVal > 0 ? exentoVal : Number(r.monto_obj || (retMonto / 0.03));
                                    r.porc_retn = 3.0;
                                }
                            }
                        }
                    });
                }

                return {
                    ...resEnc.recordset[0],
                    renglones: resReng.recordset,
                    formas_pago: resTP.recordset,
                    retenciones_iva: resIva.recordset,
                    retenciones_islr: finalRetIslr,
                    sede_id: srv.id,
                    sede_nombre: srv.name
                };
            } catch (e) {
                return { sede_id: srv.id, sede_nombre: srv.name, error: e.message };
            }
        }));

        const errors = results.filter(r => r && r.error);
        const found = results.filter(r => r && !r.error);

        if (!found.length) {
            if (errors.length) {
                console.error('[PAGOS GET :cob_num ERROR]:', errors);
                return res.status(500).json({
                    success: false,
                    message: 'Error al consultar pago en la sede: ' + errors[0].error,
                    error: errors[0].error,
                    errors
                });
            }
            return res.status(404).json({ success: false, message: 'Pago no encontrado.' });
        }

        res.status(200).json({ success: true, count: found.length, data: results.filter(r => r !== null) });
    } catch (error) {
        res.status(500).json({ success: false, message: 'Error al consultar Pago.', error: error.message });
    }
});

// Helper compartido para revertir saldo de documentos pagados al anular o eliminar un pago
async function revertirSaldosDocumentosPago({ transaction, renglones, cobNum, auditUser }) {
    for (const line of renglones) {
        const totalRebaje = Number(line.mont_cob || 0);
        const tipoDoc = line.co_tipo_doc.trim().toUpperCase();
        if (tipoDoc === 'IVAN' || tipoDoc === 'ISLR') continue;

        if (totalRebaje > 0) {
            await transaction.request()
                .input('co_tipo_doc', sql.Char(6), padProfit(line.co_tipo_doc, 6))
                .input('nro_doc', sql.Char(20), padProfit(line.nro_doc, 20))
                .input('cob_num', sql.Char(20), padProfit(cobNum, 20))
                .input('rebaje', sql.Decimal(18, 2), totalRebaje)
                .input('auditUser', sql.Char(6), padProfit(auditUser, 6))
                .query(`
                    UPDATE saDocumentoCompra
                    SET saldo = CASE WHEN saldo + @rebaje > total_neto THEN total_neto ELSE saldo + @rebaje END,
                        fe_us_mo = GETDATE(),
                        co_us_mo = @auditUser
                    WHERE LTRIM(RTRIM(co_tipo_doc)) = LTRIM(RTRIM(@co_tipo_doc))
                      AND LTRIM(RTRIM(nro_doc)) = LTRIM(RTRIM(@nro_doc))
                      AND NOT (UPPER(LTRIM(RTRIM(ISNULL(doc_orig, '')))) = 'PAGO' AND LTRIM(RTRIM(ISNULL(nro_orig, ''))) = LTRIM(RTRIM(@cob_num)))
                `);

            if (tipoDoc === 'FACT') {
                await transaction.request()
                    .input('nro_doc', sql.Char(20), padProfit(line.nro_doc, 20))
                    .input('rebaje', sql.Decimal(18, 2), totalRebaje)
                    .input('auditUser', sql.Char(6), padProfit(auditUser, 6))
                    .query(`
                        UPDATE saFacturaCompra
                        SET saldo = CASE WHEN saldo + @rebaje > total_neto THEN total_neto ELSE saldo + @rebaje END,
                            fe_us_mo = GETDATE(),
                            co_us_mo = @auditUser
                        WHERE LTRIM(RTRIM(doc_num)) = LTRIM(RTRIM(@nro_doc))
                    `);
            }
        }
    }
}

// --- ANULAR PAGO ---
router.post('/:cob_num/anular', async (req, res) => {
    try {
        const { cob_num } = req.params;
        const { sede } = req.query;

        const outcome = await executeWrite(sede || null, req.sqlAuth, async (pool) => {
            // Verificar si el pago existe y no está anulado
            const resPago = await pool.request()
                .input('cob_num', sql.Char(20), padProfit(cob_num, 20))
                .query(`
                    SELECT anulado, RTRIM(co_prov) AS co_prov, RTRIM(co_sucu_in) AS co_sucu_in
                    FROM saPago
                    WHERE LTRIM(RTRIM(cob_num)) = LTRIM(RTRIM(@cob_num))
                `);
            if (!resPago.recordset.length) throw new Error('El pago no existe.');

            const pago = resPago.recordset[0];
            if (pago.anulado) {
                throw new Error(`El pago ${cob_num} ya está anulado.`);
            }

            // Verificar si algún anticipo/ajuste generado por este pago ya fue utilizado en otro pago activo
            const resUsedAdv = await pool.request()
                .input('cob_num', sql.Char(20), padProfit(cob_num, 20))
                .query(`
                    SELECT TOP 1 RTRIM(dc.co_tipo_doc) AS co_tipo_doc, RTRIM(dc.nro_doc) AS nro_doc, RTRIM(p2.cob_num) AS used_cob_num
                    FROM saDocumentoCompra dc
                    INNER JOIN saPagoDocReng r2
                        ON LTRIM(RTRIM(r2.co_tipo_doc)) = LTRIM(RTRIM(dc.co_tipo_doc))
                       AND LTRIM(RTRIM(r2.nro_doc)) = LTRIM(RTRIM(dc.nro_doc))
                    INNER JOIN saPago p2 ON p2.cob_num = r2.cob_num
                    WHERE dc.DOC_ORIG = 'PAGO'
                      AND LTRIM(RTRIM(dc.NRO_ORIG)) = LTRIM(RTRIM(@cob_num))
                      AND UPPER(LTRIM(RTRIM(dc.co_tipo_doc))) IN ('ADEL', 'AJPA', 'AJNA', 'AJNM', 'AJPM')
                      AND p2.anulado = 0
                      AND LTRIM(RTRIM(p2.cob_num)) <> LTRIM(RTRIM(@cob_num))
                `);
            if (resUsedAdv.recordset.length > 0) {
                const u = resUsedAdv.recordset[0];
                throw new Error(`Este pago generó el documento ${u.co_tipo_doc} ${u.nro_doc} y ya fue aplicado en el Pago activo N° ${u.used_cob_num}. Anule primero ese pago.`);
            }

            // Capturar cuentas bancarias afectadas para recalcular saSaldoBanco
            const resBanCtas = await pool.request()
                .input('cob_num', sql.Char(20), padProfit(cob_num, 20))
                .query(`
                    SELECT DISTINCT RTRIM(cod_cta) AS cod_cta
                    FROM saMovimientoBanco
                    WHERE LTRIM(RTRIM(cob_pag)) = LTRIM(RTRIM(@cob_num))
                      AND origen = 'PAG'
                      AND cod_cta IS NOT NULL
                `);

            // Obtener renglones de documentos pagados
            const resReng = await pool.request()
                .input('cob_num', sql.Char(20), padProfit(cob_num, 20))
                .query(`
                    SELECT RTRIM(co_tipo_doc) AS co_tipo_doc, RTRIM(nro_doc) AS nro_doc, mont_cob, monto_retencion_iva, monto_retencion
                    FROM saPagoDocReng
                    WHERE LTRIM(RTRIM(cob_num)) = LTRIM(RTRIM(@cob_num))
                `);

            const transaction = new sql.Transaction(pool);
            await transaction.begin();

            try {
                const auditUser = (req.profitUser || 'API').substring(0, 10).toUpperCase();

                // 1. Anular cabecera de Pago
                await transaction.request()
                    .input('cob_num', sql.Char(20), padProfit(cob_num, 20))
                    .input('auditUser', sql.Char(6), padProfit(auditUser, 6))
                    .query(`
                        UPDATE saPago
                        SET anulado = 1,
                            fe_us_mo = GETDATE(),
                            co_us_mo = @auditUser
                        WHERE LTRIM(RTRIM(cob_num)) = LTRIM(RTRIM(@cob_num))
                    `);

                // 2. Anular documentos generados por este pago (IVAN, ISLR, AJPA, AJNA, ADEL)
                await transaction.request()
                    .input('cob_num', sql.Char(20), padProfit(cob_num, 20))
                    .input('auditUser', sql.Char(6), padProfit(auditUser, 6))
                    .query(`
                        UPDATE saDocumentoCompra
                        SET anulado = 1,
                            saldo = 0,
                            fe_us_mo = GETDATE(),
                            co_us_mo = @auditUser
                        WHERE DOC_ORIG = 'PAGO' AND LTRIM(RTRIM(NRO_ORIG)) = LTRIM(RTRIM(@cob_num))
                    `);

                // 3. Revertir saldo de los documentos de compra pagados (excluye docs originados por este pago)
                await revertirSaldosDocumentosPago({
                    transaction,
                    renglones: resReng.recordset,
                    cobNum: cob_num,
                    auditUser
                });

                // 4. Anular movimientos de caja asociados
                await transaction.request()
                    .input('cob_num', sql.Char(20), padProfit(cob_num, 20))
                    .input('auditUser', sql.Char(6), padProfit(auditUser, 6))
                    .query(`
                        UPDATE saMovimientoCaja
                        SET anulado = 1,
                            fe_us_mo = GETDATE(),
                            co_us_mo = @auditUser
                        WHERE LTRIM(RTRIM(doc_num)) = LTRIM(RTRIM(@cob_num)) AND origen = 'PAG'
                    `);

                // 5. Anular movimientos de banco asociados y recalcular saSaldoBanco
                await transaction.request()
                    .input('cob_num', sql.Char(20), padProfit(cob_num, 20))
                    .input('auditUser', sql.Char(6), padProfit(auditUser, 6))
                    .query(`
                        UPDATE saMovimientoBanco
                        SET anulado = 1,
                            fe_us_mo = GETDATE(),
                            co_us_mo = @auditUser
                        WHERE LTRIM(RTRIM(cob_pag)) = LTRIM(RTRIM(@cob_num)) AND origen = 'PAG'
                    `);

                for (const ctaRow of resBanCtas.recordset) {
                    if (ctaRow.cod_cta) {
                        await syncSaldoBanco(transaction, ctaRow.cod_cta);
                    }
                }

                await transaction.commit();
                return { success: true, cob_num: cob_num };
            } catch (err) {
                if (transaction._aborted === false) await transaction.rollback();
                throw err;
            }
        });

        return writeResponse(res, outcome);
    } catch (error) {
        res.status(500).json({ success: false, message: 'Error al anular pago.', error: error.message });
    }
});

// --- ELIMINAR PAGO DEFINITIVAMENTE ---
const eliminarPagoHandler = async (req, res) => {
    try {
        const cob_num = (req.params.cob_num || '').trim();
        const sede = req.query.sede || req.headers['x-branch-id'];

        const outcome = await executeWrite(sede || null, req.sqlAuth, async (pool, srv) => {
            // 1. Verificar si el pago existe
            const resPago = await pool.request()
                .input('cob_num', sql.Char(20), padProfit(cob_num, 20))
                .query(`
                    SELECT anulado, RTRIM(co_prov) AS co_prov, monto
                    FROM saPago
                    WHERE LTRIM(RTRIM(cob_num)) = LTRIM(RTRIM(@cob_num))
                `);
            if (!resPago.recordset.length) throw new Error(`El pago ${cob_num} no existe.`);

            const pago = resPago.recordset[0];

            // Verificar si algún anticipo/ajuste generado por este pago está referenciado en otro pago (por FK_saPagoDocReng_saDocumentoCompra)
            const resUsedAdvDel = await pool.request()
                .input('cob_num', sql.Char(20), padProfit(cob_num, 20))
                .query(`
                    SELECT TOP 1 RTRIM(dc.co_tipo_doc) AS co_tipo_doc, RTRIM(dc.nro_doc) AS nro_doc, RTRIM(r2.cob_num) AS used_cob_num
                    FROM saDocumentoCompra dc
                    INNER JOIN saPagoDocReng r2
                        ON LTRIM(RTRIM(r2.co_tipo_doc)) = LTRIM(RTRIM(dc.co_tipo_doc))
                       AND LTRIM(RTRIM(r2.nro_doc)) = LTRIM(RTRIM(dc.nro_doc))
                    WHERE dc.DOC_ORIG = 'PAGO'
                      AND LTRIM(RTRIM(dc.NRO_ORIG)) = LTRIM(RTRIM(@cob_num))
                      AND UPPER(LTRIM(RTRIM(dc.co_tipo_doc))) IN ('ADEL', 'AJPA', 'AJNA', 'AJNM', 'AJPM')
                      AND LTRIM(RTRIM(r2.cob_num)) <> LTRIM(RTRIM(@cob_num))
                `);
            if (resUsedAdvDel.recordset.length > 0) {
                const u = resUsedAdvDel.recordset[0];
                throw new Error(`No se puede eliminar este pago porque su documento ${u.co_tipo_doc} ${u.nro_doc} está referenciado en el Pago N° ${u.used_cob_num}. Elimine primero ese pago.`);
            }

            // Capturar cuentas bancarias afectadas para recalcular saSaldoBanco
            const resBanCtasDel = await pool.request()
                .input('cob_num', sql.Char(20), padProfit(cob_num, 20))
                .query(`
                    SELECT DISTINCT RTRIM(cod_cta) AS cod_cta
                    FROM saMovimientoBanco
                    WHERE LTRIM(RTRIM(cob_pag)) = LTRIM(RTRIM(@cob_num))
                      AND origen = 'PAG'
                      AND cod_cta IS NOT NULL
                `);

            // 1.1 Consultar las retenciones (IVAN e ISLR) generadas por este pago antes de borrarlas
            const retIvaDocs = await pool.request()
                .input('cob_num', sql.Char(20), padProfit(cob_num, 20))
                .query(`
                    SELECT DISTINCT RTRIM(ri.num_comprobante) AS num_comprobante
                    FROM saPagoRetenIvaReng ri
                    INNER JOIN saPagoDocReng pdr ON ri.rowguid_reng_cob = pdr.rowguid
                    WHERE LTRIM(RTRIM(pdr.cob_num)) = LTRIM(RTRIM(@cob_num))
                    UNION
                    SELECT DISTINCT RTRIM(num_comprobante) AS num_comprobante
                    FROM saDocumentoCompra
                    WHERE DOC_ORIG = 'PAGO' 
                      AND LTRIM(RTRIM(NRO_ORIG)) = LTRIM(RTRIM(@cob_num)) 
                      AND UPPER(RTRIM(co_tipo_doc)) = 'IVAN'
                `);

            const retIslrDocs = await pool.request()
                .input('cob_num', sql.Char(20), padProfit(cob_num, 20))
                .query(`
                    SELECT DISTINCT RTRIM(nro_doc) AS nro_doc
                    FROM saDocumentoCompra
                    WHERE DOC_ORIG = 'PAGO' 
                      AND LTRIM(RTRIM(NRO_ORIG)) = LTRIM(RTRIM(@cob_num)) 
                      AND UPPER(RTRIM(co_tipo_doc)) = 'ISLR'
                `);

            // 2. Obtener renglones de documentos pagados
            const resReng = await pool.request()
                .input('cob_num', sql.Char(20), padProfit(cob_num, 20))
                .query(`
                    SELECT RTRIM(co_tipo_doc) AS co_tipo_doc, RTRIM(nro_doc) AS nro_doc, mont_cob
                    FROM saPagoDocReng
                    WHERE LTRIM(RTRIM(cob_num)) = LTRIM(RTRIM(@cob_num))
                `);

            const transaction = new sql.Transaction(pool);
            await transaction.begin();

            try {
                const auditUser = (req.profitUser || 'API').substring(0, 10).toUpperCase();

                // 3. Si el pago NO estaba anulado, revertir el saldo de las facturas / documentos de compra
                // (Omitiendo documentos de retención/ajuste propios del pago)
                if (!pago.anulado) {
                    await revertirSaldosDocumentosPago({
                        transaction,
                        renglones: resReng.recordset,
                        cobNum: cob_num,
                        auditUser
                    });
                }

                // 4. Desvincular FK mov_ban en documentos propios del pago y eliminar movimientos de caja y banco creados por este pago
                await transaction.request()
                    .input('cob_num', sql.Char(20), padProfit(cob_num, 20))
                    .query(`
                        UPDATE saDocumentoCompra
                        SET mov_ban = NULL
                        WHERE DOC_ORIG = 'PAGO'
                          AND LTRIM(RTRIM(NRO_ORIG)) = LTRIM(RTRIM(@cob_num))
                          AND mov_ban IS NOT NULL;

                        DELETE FROM saMovimientoCaja
                        WHERE LTRIM(RTRIM(doc_num)) = LTRIM(RTRIM(@cob_num)) AND origen = 'PAG';

                        DELETE FROM saMovimientoBanco
                        WHERE LTRIM(RTRIM(cob_pag)) = LTRIM(RTRIM(@cob_num)) AND origen = 'PAG';
                    `);

                for (const ctaRow of resBanCtasDel.recordset) {
                    if (ctaRow.cod_cta) {
                        await syncSaldoBanco(transaction, ctaRow.cod_cta);
                    }
                }

                // 5. Eliminar renglones de retención de IVA e ISLR asociados al pago (hijos de saPagoDocReng)
                await transaction.request()
                    .input('cob_num', sql.Char(20), padProfit(cob_num, 20))
                    .query(`
                        DELETE ri
                        FROM saPagoRetenIvaReng ri
                        INNER JOIN saPagoDocReng pdr ON ri.rowguid_reng_cob = pdr.rowguid
                        WHERE LTRIM(RTRIM(pdr.cob_num)) = LTRIM(RTRIM(@cob_num));

                        DELETE rn
                        FROM saPagoRentenReng rn
                        INNER JOIN saPagoDocReng pdr ON rn.rowguid_reng_cob = pdr.rowguid
                        WHERE LTRIM(RTRIM(pdr.cob_num)) = LTRIM(RTRIM(@cob_num));
                    `);

                // 6. Eliminar instrumentos de pago (saPagoTPReng) y giros
                await transaction.request()
                    .input('cob_num', sql.Char(20), padProfit(cob_num, 20))
                    .query(`
                        DELETE FROM saPagoTPReng WHERE LTRIM(RTRIM(cob_num)) = LTRIM(RTRIM(@cob_num));
                        DELETE FROM saGiroCompra WHERE LTRIM(RTRIM(cob_num)) = LTRIM(RTRIM(@cob_num));
                    `);

                // 7. Eliminar renglones de documentos (saPagoDocReng)
                // DEBE eliminarse antes de saDocumentoCompra para evitar violación de FK_saPagoDocReng_saDocumentoCompra
                await transaction.request()
                    .input('cob_num', sql.Char(20), padProfit(cob_num, 20))
                    .query(`
                        DELETE FROM saPagoDocReng
                        WHERE LTRIM(RTRIM(cob_num)) = LTRIM(RTRIM(@cob_num));
                    `);

                // 8. Eliminar cabecera (saPago)
                await transaction.request()
                    .input('cob_num', sql.Char(20), padProfit(cob_num, 20))
                    .query(`
                        DELETE FROM saPago
                        WHERE LTRIM(RTRIM(cob_num)) = LTRIM(RTRIM(@cob_num));
                    `);

                // 9. Ahora sí: Eliminar documentos fiscales de retención creados por este pago en saDocumentoCompra (IVAN, ISLR)
                await transaction.request()
                    .input('cob_num', sql.Char(20), padProfit(cob_num, 20))
                    .query(`
                        DELETE FROM saDocumentoCompra
                        WHERE DOC_ORIG = 'PAGO' AND LTRIM(RTRIM(NRO_ORIG)) = LTRIM(RTRIM(@cob_num));
                    `);

                await transaction.commit();

                // 10. Si el pago contenía comprobantes de retención, verificar si correspondían al último emitido y revertir contador
                for (const row of retIvaDocs.recordset) {
                    if (row.num_comprobante) {
                        try {
                            await revertirConsecutivoRetencionIfLast({
                                runner: pool,
                                co_tipo_serie: 'IVAN_COMPRA',
                                deletedNum: row.num_comprobante,
                                currentSrvId: srv?.id,
                                sqlAuth: req.sqlAuth
                            });
                        } catch (eRevIva) {
                            console.warn(`[ELIMINAR PAGO] Advertencia al verificar/revertir correlativo de retención IVA:`, eRevIva.message);
                        }
                    }
                }

                for (const row of retIslrDocs.recordset) {
                    if (row.nro_doc) {
                        try {
                            await revertirConsecutivoRetencionIfLast({
                                runner: pool,
                                co_tipo_serie: 'ISLR_COMPRA',
                                deletedNum: row.nro_doc,
                                currentSrvId: srv?.id,
                                sqlAuth: req.sqlAuth
                            });
                        } catch (eRevIslr) {
                            console.warn(`[ELIMINAR PAGO] Advertencia al verificar/revertir correlativo de retención ISLR:`, eRevIslr.message);
                        }
                    }
                }

                return { success: true, cob_num: cob_num, message: `Pago ${cob_num} eliminado exitosamente.` };
            } catch (err) {
                if (transaction._aborted === false) await transaction.rollback();
                throw err;
            }
        });

        return writeResponse(res, outcome);
    } catch (error) {
        res.status(500).json({ success: false, message: 'Error al eliminar pago.', error: error.message });
    }
};

router.delete('/:cob_num', eliminarPagoHandler);
router.post('/:cob_num/eliminar', eliminarPagoHandler);

// --- GUARDAR PAGO ---
router.post('/', async (req, res) => {
    const data = req.body;
    const isDirectAdvance = Boolean(data.is_advance);
    console.log("📥 [AGENT] Recibiendo Pago (SAVE):", JSON.stringify({ co_prov: data.co_prov, is_advance: isDirectAdvance, advance_doc_type: data.advance_doc_type, renglones: data.renglones?.length, formas_pago: data.formas_pago?.length }, null, 2));

    if (!data.co_prov || !Array.isArray(data.formas_pago)) {
        return res.status(400).json({ success: false, message: 'Campos obligatorios: co_prov, formas_pago' });
    }
    if (!isDirectAdvance && !Array.isArray(data.renglones)) {
        return res.status(400).json({ success: false, message: 'Campos obligatorios: renglones (o is_advance=true)' });
    }
    const rawRenglones = isDirectAdvance ? [] : (Array.isArray(data.renglones) ? data.renglones : []);
    if (isDirectAdvance) {
        const sumFp = data.formas_pago.reduce((acc, fp) => acc + Math.abs(Number(fp.mont_doc || fp.monto || 0)), 0);
        if (sumFp <= 0.01) {
            return res.status(400).json({ success: false, message: 'Para registrar un anticipo directo debe indicar al menos un instrumento de pago con monto mayor a 0.' });
        }
    }

    const outcome = await executeWrite(req.query.sede || null, req.sqlAuth, async (pool, srv) => {
        const reqCtaInput = data.co_cta_ingr_egr ? String(data.co_cta_ingr_egr).trim() : '';

        // Cargar Catálogos y parámetros reales de Compras (nunca heredar default_cta_ingr_egr de ventas/I-001)
        const [resSucu, resAllCtasIE, resParEmp, resProvCta, resCaja] = await Promise.all([
            pool.request().query(`SELECT TOP 1 RTRIM(co_sucur) AS co_sucur FROM saSucursal`),
            pool.request().query(`SELECT RTRIM(co_cta_ingr_egr) AS co_cta_ingr_egr, RTRIM(ISNULL(descrip, '')) AS descrip FROM saCuentaIngEgr ORDER BY co_cta_ingr_egr ASC`),
            pool.request()
                .query(`SELECT TOP 1 RTRIM(ISNULL(c_cta_ing_egr, '')) AS c_cta_ing_egr FROM par_emp`)
                .catch(() => ({ recordset: [] })),
            pool.request()
                .input('co_prov_cta', sql.Char(16), padProfit(data.co_prov, 16))
                .query(`SELECT TOP 1 RTRIM(ISNULL(co_cta_ingr_egr, '')) AS co_cta_ingr_egr FROM saProveedor WHERE LTRIM(RTRIM(co_prov)) = LTRIM(RTRIM(@co_prov_cta))`)
                .catch(() => ({ recordset: [] })),
            pool.request().query(`
                SELECT TOP 1 RTRIM(cod_caja) AS cod_caja
                FROM saCaja
                ORDER BY CASE WHEN RTRIM(cod_caja) = '02' THEN 0 ELSE 1 END, cod_caja
            `)
        ]);

        const allCtas = (resAllCtasIE.recordset || []).map(r => String(r.co_cta_ingr_egr || '').trim()).filter(Boolean);
        const validCtasMap = new Map();
        for (const c of allCtas) {
            validCtasMap.set(c.toUpperCase(), c);
        }
        const findValidCta = (cand) => {
            const clean = String(cand || '').trim();
            if (!clean) return null;
            return validCtasMap.get(clean.toUpperCase()) || null;
        };

        const parCtaValid = findValidCta(resParEmp.recordset?.[0]?.c_cta_ing_egr);
        const branchEgresoValid = findValidCta(srv.sql_config?.profit_defaults?.default_cta_egreso || srv.profit_defaults?.default_cta_egreso);
        const provCtaRaw = findValidCta(resProvCta.recordset?.[0]?.co_cta_ingr_egr);
        const provCtaValid = (provCtaRaw && !/^I-/i.test(provCtaRaw)) ? provCtaRaw : null;

        // Consultar las cuentas definidas en los documentos seleccionados en renglones
        const docCtasFound = [];
        if (!isDirectAdvance && rawRenglones.length > 0) {
            for (const line of rawRenglones) {
                const lTipo = String(line.co_tipo_doc || '').trim();
                const lNro = String(line.nro_doc || '').trim();
                if (!lTipo || !lNro) continue;
                try {
                    const rDocCta = await pool.request()
                        .input('co_tipo_doc', sql.Char(6), padProfit(lTipo, 6))
                        .input('nro_doc', sql.Char(20), padProfit(lNro, 20))
                        .query(`
                            SELECT TOP 1 RTRIM(ISNULL(co_cta_ingr_egr, '')) AS co_cta_ingr_egr
                            FROM saDocumentoCompra
                            WHERE LTRIM(RTRIM(co_tipo_doc)) = LTRIM(RTRIM(@co_tipo_doc))
                              AND LTRIM(RTRIM(nro_doc)) = LTRIM(RTRIM(@nro_doc))
                        `);
                    const validDocCta = findValidCta(rDocCta.recordset?.[0]?.co_cta_ingr_egr);
                    if (validDocCta && !docCtasFound.some(c => c.toUpperCase() === validDocCta.toUpperCase())) {
                        docCtasFound.push(validDocCta);
                    }
                } catch (_) {
                    // Ignorar fallo individual de lectura de cuenta del documento
                }
            }
        }

        let defCtaIE = findValidCta(reqCtaInput);
        let ctaSource = defCtaIE ? 'payload' : '';

        if (!defCtaIE && docCtasFound.length > 0) {
            if (docCtasFound.length === 1) {
                defCtaIE = docCtasFound[0];
            } else {
                defCtaIE =
                    (parCtaValid && docCtasFound.find(c => c.toUpperCase() === parCtaValid.toUpperCase())) ||
                    docCtasFound.find(c => /^(P-|G-|PG-|02)/i.test(c)) ||
                    docCtasFound.find(c => !/^I-/i.test(c)) ||
                    docCtasFound[0];
            }
            if (defCtaIE) ctaSource = 'documentos_renglones';
        }

        if (!defCtaIE && parCtaValid) {
            defCtaIE = parCtaValid;
            ctaSource = 'par_emp.c_cta_ing_egr';
        }

        if (!defCtaIE && branchEgresoValid) {
            defCtaIE = branchEgresoValid;
            ctaSource = 'branch.default_cta_egreso';
        }

        if (!defCtaIE && provCtaValid) {
            defCtaIE = provCtaValid;
            ctaSource = 'saProveedor';
        }

        if (!defCtaIE) {
            defCtaIE =
                findValidCta('P-001') ||
                findValidCta('02') ||
                allCtas.find(c => !/^I-/i.test(c)) ||
                findValidCta('01') ||
                allCtas[0] ||
                'P-001';
            ctaSource = 'catalog_fallback';
        }

        console.log(`🏦 [PAGOS] Cuenta Ing/Egr resuelta para pago: "${defCtaIE}" (origen: ${ctaSource})`);

        const allCtasRows = (resAllCtasIE.recordset || [])
            .map(r => ({ code: String(r.co_cta_ingr_egr || '').trim(), descrip: String(r.descrip || '').trim() }))
            .filter(r => r.code);
        const findCtaByRegex = (rx, excludeRx = null) =>
            allCtasRows.find(r => rx.test(r.descrip) && (!excludeRx || !excludeRx.test(r.descrip)))?.code || null;

        const ctaRetIva =
            findValidCta('P-005') ||
            findCtaByRegex(/IVA.*RET.*PROV|RET.*IVA.*PROV/i) ||
            findCtaByRegex(/IVA.*RET|RET.*IVA/i, /CLIENT/i) ||
            findValidCta('PG-003') ||
            defCtaIE;
        const ctaRetIslr =
            findValidCta('P-003') ||
            findCtaByRegex(/I\.?S\.?L\.?R\.?.*RET.*PROV|RET.*I\.?S\.?L\.?R\.?.*PROV/i) ||
            findValidCta('PG-001') ||
            defCtaIE;
        const ctaRetTimbre =
            findCtaByRegex(/TIMBRE|1\s*[X\*]\s*1000|1X1000/i) ||
            findValidCta('G-031') ||
            findCtaByRegex(/OTROS\s+IMPUESTOS/i) ||
            defCtaIE;

        console.log(`🏛️ [PAGOS] Cuentas retenciones: IVA="${ctaRetIva}", ISLR="${ctaRetIslr}", TIMBRE="${ctaRetTimbre}"`);

        const defSucu = resSucu.recordset[0]?.co_sucur || '01';
        const defCaja = resCaja.recordset[0]?.cod_caja || '02';

        const auditUser = (req.profitUser || req.sqlAuth?.user || 'API').substring(0, 10).toUpperCase();
        const pagoFechaStr = (data.fecha && String(data.fecha).match(/^\d{4}-\d{2}-\d{2}$/))
            ? String(data.fecha).trim()
            : new Date().toISOString().split('T')[0];
        const tsDate = new Date(`${pagoFechaStr}T12:00:00`);

        const branchCodes = srv.profit_branch_codes || [];
        const defaultCodeObj = branchCodes.find(b => b.is_default === true) || branchCodes[0] || { code: defSucu };
        const sucuCode = defaultCodeObj.code;

        const transaction = new sql.Transaction(pool);
        await transaction.begin();

        try {
            // 1. Obtener correlativo de Pago (PAGO / C020)
            const corrRes = await getProximoConsecutivo({
                runner: transaction,
                co_tipo_serie: 'PAGO',
                co_sucur: sucuCode
            });
            const cobNum = corrRes.docNum;
            console.log(`✨ [AGENT] Nuevo número de pago generado: ${cobNum} (Prefijo: '${corrRes.prefijo}', ProxN: ${corrRes.proxN})`);

            // 1.1 Determinar moneda y montos de cabecera
            let paymentMone = data.co_mone || 'USD';
            if (paymentMone.trim().toUpperCase() === 'US$') paymentMone = 'USD';

            let totalAbonoBs = 0;
            rawRenglones.forEach((line) => {
                totalAbonoBs += Number(line.mont_cob || 0);
            });

            let totalFormasPagoBs = 0;
            data.formas_pago.forEach((fp) => {
                totalFormasPagoBs += Math.abs(Number(fp.mont_doc || fp.monto || 0));
            });

            let finalMontoHeader = 0;
            if (data.formas_pago && data.formas_pago.length > 0) {
                finalMontoHeader = paymentMone === 'USD'
                    ? Math.round((totalFormasPagoBs / Number(data.tasa || 1)) * 100) / 100
                    : totalFormasPagoBs;
            } else {
                finalMontoHeader = 0;
            }

            const defaultHeaderDesc = isDirectAdvance
                ? `ANTICIPO PROVEEDOR ${data.co_prov}`
                : `PAGO PROVEEDOR ${data.co_prov}`;

            // 2. Insertar Cabecera de Pago en saPago
            const rH = new sql.Request(transaction);
            rH.input('sCob_Num', sql.Char(20), padProfit(cobNum, 20));
            rH.input('sRecibo', sql.Char(15), null);
            rH.input('sCo_Prov', sql.Char(16), padProfit(data.co_prov, 16));
            rH.input('sCo_Mone', sql.Char(6), padProfit(paymentMone, 6));
            rH.input('deTasa', sql.Decimal(21, 8), Number(data.tasa || 1));
            rH.input('sdFecha', sql.SmallDateTime, tsDate);
            rH.input('bAnulado', sql.Bit, 0);
            rH.input('deMonto', sql.Decimal(18, 2), finalMontoHeader);
            rH.input('sDis_cen', sql.VarChar(sql.MAX), null);
            rH.input('sDescrip', sql.VarChar(60), (data.descrip || defaultHeaderDesc).substring(0, 60));
            rH.input('sCampo1', sql.VarChar(60), null);
            rH.input('sCampo2', sql.VarChar(60), null);
            rH.input('sCampo3', sql.VarChar(60), null);
            rH.input('sCampo4', sql.VarChar(60), null);
            rH.input('sCampo5', sql.VarChar(60), null);
            rH.input('sCampo6', sql.VarChar(60), null);
            rH.input('sCampo7', sql.VarChar(60), null);
            rH.input('sCampo8', sql.VarChar(60), null);
            rH.input('sCo_Us_In', sql.Char(6), padProfit(auditUser, 6));
            rH.input('sCo_Sucu_In', sql.Char(6), padProfit(sucuCode, 6));
            rH.input('sMaquina', sql.VarChar(60), 'WEB');
            rH.input('sRevisado', sql.Char(1), null);
            rH.input('sTrasnfe', sql.Char(1), null);

            await rH.execute('pInsertarPago');

            // Mapas para relacionar los renglones (Padre-Hijo)
            const rengDocGuidMap = new Map(); // `${tipo}::${nro_doc}` (y `nro_doc` para padres) -> rowguid de saPagoDocReng
            const parentTypes = ['FACT', 'NDEB', 'N/DB', 'GIRO'];
            const creditDocTypes = ['N/CR', 'NCR', 'AJNA', 'AJNM', 'ADEL', 'AJPA', 'AJPM'];
            const parentLines = rawRenglones.filter(r => parentTypes.includes(r.co_tipo_doc.trim().toUpperCase()));
            const childLines = rawRenglones.filter(r => !parentTypes.includes(r.co_tipo_doc.trim().toUpperCase()));

            // 2.3 Procesar Retención de Timbre Fiscal 1x1000 (AJNA)
            let createdTimbreAjnaBs = 0;
            if (!isDirectAdvance && data.timbre_fiscal && (data.timbre_fiscal.aplicar || Number(data.timbre_fiscal.monto_bs) > 0.01)) {
                const timbreMontoBs = Math.round((Number(data.timbre_fiscal.monto_bs) || 0) * 100) / 100;
                if (timbreMontoBs > 0.01) {
                    const corrAj = await getProximoConsecutivo({
                        runner: transaction,
                        co_tipo_serie: 'C008',
                        co_consecutivos: ['DOC_COM_AJNA', 'C008', 'AJNA_NUM', 'AJNA'],
                        co_sucur: sucuCode,
                        table: 'saDocumentoCompra',
                        col: 'nro_doc'
                    });
                    const ajDocNum = corrAj.docNum;
                    const ajObserva = `RETENCION TIMBRE FISCAL 1X1000 PAGO N° ${cobNum}`.substring(0, 120);

                    await transaction.request()
                        .input('co_tipo_doc', sql.Char(6), padProfit('AJNA', 6))
                        .input('nro_doc', sql.Char(20), padProfit(ajDocNum, 20))
                        .input('co_prov', sql.Char(16), padProfit(data.co_prov, 16))
                        .input('co_mone', sql.Char(6), padProfit(paymentMone, 6))
                        .input('tasa', sql.Decimal(21, 8), Number(data.tasa || 1))
                        .input('co_cta_ingr_egr', sql.Char(20), padProfit(ctaRetTimbre, 20))
                        .input('observa', sql.VarChar(120), ajObserva)
                        .input('fec_doc', sql.SmallDateTime, tsDate)
                        .input('doc_orig', sql.Char(6), padProfit('PAGO', 6))
                        .input('tipo_origen', sql.Int, 0)
                        .input('nro_orig', sql.VarChar(20), cobNum)
                        .input('total_bruto', sql.Decimal(18, 2), timbreMontoBs)
                        .input('total_neto', sql.Decimal(18, 2), timbreMontoBs)
                        .input('saldo', sql.Decimal(18, 2), timbreMontoBs)
                        .input('co_us_in', sql.Char(6), padProfit(auditUser, 6))
                        .input('co_sucu_in', sql.Char(6), padProfit(sucuCode, 6))
                        .query(`
                            INSERT INTO saDocumentoCompra (
                                co_tipo_doc, nro_doc, co_prov, co_mone, tasa, co_cta_ingr_egr, observa,
                                fec_reg, fec_emis, fec_venc, anulado, aut, pagar,
                                doc_orig, tipo_origen, nro_orig, saldo, total_bruto,
                                total_neto, monto_imp, monto_imp2, monto_imp3, porc_imp, porc_imp2, porc_imp3,
                                otros1, otros2, otros3, adicional, co_us_in, co_sucu_in, fe_us_in,
                                co_us_mo, co_sucu_mo, fe_us_mo, rowguid,
                                monto_desc_glob, monto_reca
                            ) VALUES (
                                @co_tipo_doc, @nro_doc, @co_prov, @co_mone, @tasa, @co_cta_ingr_egr, @observa,
                                @fec_doc, @fec_doc, @fec_doc, 0, 1, 0,
                                @doc_orig, @tipo_origen, @nro_orig, @saldo, @total_bruto,
                                @total_neto, 0, 0, 0, 0, 0, 0,
                                0, 0, 0, 0.00, @co_us_in, @co_sucu_in, GETDATE(),
                                @co_us_in, @co_sucu_in, GETDATE(), NEWID(),
                                0, 0
                            )
                        `);

                    createdTimbreAjnaBs = timbreMontoBs;
                    childLines.push({
                        co_tipo_doc: 'AJNA',
                        nro_doc: ajDocNum,
                        mont_cob: timbreMontoBs,
                        monto_retencion_iva: 0,
                        monto_retencion: 0,
                        parent_doc: parentLines[0]?.nro_doc
                    });

                    console.log(`✨ [PAGOS] Retención Timbre Fiscal 1x1000 generada como AJNA ${ajDocNum} (${timbreMontoBs} Bs) para pago ${cobNum}`);
                }
            }

            const sortedRenglones = [...parentLines, ...childLines];
            let nextRengNum = 1;

            // 3. Insertar Renglones de Documentos de Compra (saPagoDocReng)
            for (let i = 0; i < sortedRenglones.length; i++) {
                const line = sortedRenglones[i];
                const docTypeUpper = line.co_tipo_doc.trim().toUpperCase();

                let parentGuid = null;
                if (!parentTypes.includes(docTypeUpper)) {
                    const lookupKey = line.parent_doc ? line.parent_doc.trim() : line.nro_doc?.trim();
                    parentGuid = rengDocGuidMap.get(lookupKey) || null;
                }

                // Consultar saDocumentoCompra para obtener datos y saldo actual
                let docSaldo = 0;
                let docTasa = 1;
                let docMone = 'BS';
                let docNroFact = line.nro_fact || line.nro_doc;
                let docNControl = '';
                let docNroOrig = '';
                let docFecEmis = tsDate;
                let docTotalNeto = 0;
                let docBaseImponible = 0;
                let docMontoExento = 0;
                let docAlicuota = 16;
                let docNroFactOrigen = '0';

                const queryTypes = ['FACT', 'NDEB', 'N/DB', 'GIRO', 'AJPA', 'AJPM', 'N/CR', 'NCR', 'AJNA', 'AJNM', 'ADEL'];
                if (queryTypes.includes(docTypeUpper)) {
                    const docInfo = await transaction.request()
                        .input('co_tipo_doc', sql.Char(6), padProfit(line.co_tipo_doc, 6))
                        .input('nro_doc', sql.Char(20), padProfit(line.nro_doc, 20))
                        .query(`
                            SELECT RTRIM(d.co_mone) AS co_mone, d.tasa, d.saldo, 
                                   RTRIM(ISNULL(d.nro_fact, d.nro_doc)) AS nro_fact,
                                   RTRIM(ISNULL(d.n_control, '')) AS n_control,
                                   RTRIM(ISNULL(d.nro_orig, '')) AS nro_orig,
                                   RTRIM(ISNULL(d.doc_orig, '')) AS doc_orig,
                                   d.fec_emis,
                                   CONVERT(VARCHAR(10), d.fec_emis, 120) AS fec_emis_str,
                                   ISNULL(d.total_bruto, 0) AS total_bruto,
                                   ISNULL(d.total_neto, 0) AS total_neto,
                                   ISNULL(d.monto_imp, 0) AS monto_imp,
                                   ISNULL(d.porc_imp, 0) AS porc_imp,
                                   ISNULL(d.otros1, 0) AS otros1,
                                   ISNULL(d.otros2, 0) AS otros2,
                                   ISNULL(d.otros3, 0) AS otros3,
                                   ISNULL(CASE
                                       WHEN reng_calc.has_reng = 1 THEN ISNULL(reng_calc.exento_reng, 0)
                                       WHEN ISNULL(d.monto_imp, 0) = 0 THEN ISNULL(NULLIF(d.otros2, 0), ISNULL(NULLIF(d.total_bruto, 0), d.total_neto))
                                       ELSE ISNULL(d.otros2, 0)
                                   END, 0) AS monto_exento_calc,
                                   ISNULL(CASE
                                       WHEN reng_calc.has_reng = 1 THEN ISNULL(reng_calc.base_gravada_reng, 0)
                                       WHEN ISNULL(d.monto_imp, 0) = 0 THEN 0
                                       ELSE (d.total_neto - d.monto_imp - ISNULL(d.otros1, 0) - ISNULL(d.otros2, 0) - ISNULL(d.otros3, 0))
                                   END, 0) AS base_imponible_calc,
                                   COALESCE(
                                       NULLIF(reng_calc.alicuota_reng, 0),
                                       CASE 
                                           WHEN d.porc_imp IN (8, 16, 31) THEN d.porc_imp
                                           WHEN ISNULL(d.monto_imp, 0) > 0 THEN 16
                                           ELSE 0
                                       END
                                   ) AS alicuota_calc,
                                   CASE 
                                       WHEN UPPER(RTRIM(d.co_tipo_doc)) IN ('N/CR', 'NCR', 'N/DB', 'NDEB') THEN
                                           COALESCE(NULLIF(RTRIM(afec.nro_fact_origen), ''), NULLIF(RTRIM(d.nro_orig), ''), '0')
                                       ELSE '0'
                                   END AS nro_fact_afectado_calc
                            FROM saDocumentoCompra d
                            OUTER APPLY (
                                SELECT
                                    MAX(src.has_reng) AS has_reng,
                                    SUM(src.exento_reng) AS exento_reng,
                                    SUM(src.base_gravada_reng) AS base_gravada_reng,
                                    MAX(src.alicuota_reng) AS alicuota_reng
                                FROM (
                                    SELECT
                                        1 AS has_reng,
                                        CASE WHEN ISNULL(fr.porc_imp, 0) = 0 THEN ISNULL(fr.reng_neto, 0) ELSE 0 END AS exento_reng,
                                        CASE WHEN ISNULL(fr.porc_imp, 0) > 0 THEN ISNULL(fr.reng_neto, 0) ELSE 0 END AS base_gravada_reng,
                                        CASE WHEN ISNULL(fr.porc_imp, 0) > 0 THEN fr.porc_imp ELSE NULL END AS alicuota_reng
                                    FROM saFacturaCompraReng fr
                                    WHERE UPPER(LTRIM(RTRIM(d.co_tipo_doc))) = 'FACT'
                                      AND (
                                          LTRIM(RTRIM(fr.doc_num)) = LTRIM(RTRIM(d.nro_doc))
                                          OR (UPPER(LTRIM(RTRIM(ISNULL(d.doc_orig, '')))) IN ('FACT', 'COMP') AND NULLIF(RTRIM(d.nro_orig), '') IS NOT NULL AND LTRIM(RTRIM(fr.doc_num)) = LTRIM(RTRIM(d.nro_orig)))
                                      )
                                    UNION ALL
                                    SELECT
                                        1 AS has_reng,
                                        CASE WHEN ISNULL(dr.porc_imp, 0) = 0 THEN ISNULL(dr.reng_neto, 0) ELSE 0 END AS exento_reng,
                                        CASE WHEN ISNULL(dr.porc_imp, 0) > 0 THEN ISNULL(dr.reng_neto, 0) ELSE 0 END AS base_gravada_reng,
                                        CASE WHEN ISNULL(dr.porc_imp, 0) > 0 THEN dr.porc_imp ELSE NULL END AS alicuota_reng
                                    FROM saDevolucionProveedorReng dr
                                    WHERE UPPER(LTRIM(RTRIM(d.co_tipo_doc))) IN ('N/CR', 'NCR')
                                      AND (
                                          (NULLIF(RTRIM(d.nro_orig), '') IS NOT NULL AND LTRIM(RTRIM(dr.doc_num)) = LTRIM(RTRIM(d.nro_orig)))
                                          OR LTRIM(RTRIM(dr.doc_num)) = LTRIM(RTRIM(d.nro_doc))
                                      )
                                ) src
                            ) reng_calc
                            OUTER APPLY (
                                SELECT TOP 1
                                    COALESCE(
                                        NULLIF(RTRIM(d_afec.nro_fact), ''),
                                        NULLIF(RTRIM(d_dev.nro_fact), ''),
                                        NULLIF(RTRIM(d_afec.nro_doc), '')
                                    ) AS nro_fact_origen
                                FROM (SELECT 1 AS _x) _dummy
                                LEFT JOIN saDocumentoCompra d_afec ON (
                                    UPPER(LTRIM(RTRIM(d.co_tipo_doc))) IN ('N/CR', 'NCR', 'N/DB', 'NDEB')
                                    AND NULLIF(RTRIM(d.nro_orig), '') IS NOT NULL
                                    AND LTRIM(RTRIM(d_afec.nro_doc)) = LTRIM(RTRIM(d.nro_orig))
                                    AND UPPER(LTRIM(RTRIM(d_afec.co_tipo_doc))) NOT IN ('IVAN', 'ISLR', 'N/CR', 'NCR')
                                )
                                LEFT JOIN saDevolucionProveedorReng dpr ON (
                                    UPPER(LTRIM(RTRIM(d.co_tipo_doc))) IN ('N/CR', 'NCR')
                                    AND d_afec.nro_doc IS NULL
                                    AND (
                                        (NULLIF(RTRIM(d.nro_orig), '') IS NOT NULL AND LTRIM(RTRIM(dpr.doc_num)) = LTRIM(RTRIM(d.nro_orig)))
                                        OR LTRIM(RTRIM(dpr.doc_num)) = LTRIM(RTRIM(d.nro_doc))
                                    )
                                )
                                LEFT JOIN saDocumentoCompra d_dev ON (
                                    LTRIM(RTRIM(d_dev.nro_doc)) = LTRIM(RTRIM(dpr.num_doc))
                                    AND UPPER(LTRIM(RTRIM(d_dev.co_tipo_doc))) IN ('FACT', 'N/DB', 'NDEB')
                                )
                            ) afec
                            WHERE LTRIM(RTRIM(d.co_tipo_doc)) = LTRIM(RTRIM(@co_tipo_doc))
                              AND LTRIM(RTRIM(d.nro_doc)) = LTRIM(RTRIM(@nro_doc))
                        `);
                    if (docInfo.recordset.length > 0) {
                        const di = docInfo.recordset[0];
                        docSaldo = Number(di.saldo || 0);
                        docTasa = Number(di.tasa || 1);
                        if (docTasa <= 1.000001 && Number(data.tasa || 1) > 1) {
                            docTasa = Number(data.tasa);
                        }
                        docMone = di.co_mone || 'BS';
                        docNroFact = di.nro_fact || docNroFact;
                        docNControl = di.n_control || '';
                        docNroOrig = di.nro_orig || '';
                        if (di.fec_emis) docFecEmis = new Date(di.fec_emis);
                        docTotalNeto = Number(di.total_neto || 0);
                        docBaseImponible = Number(di.base_imponible_calc || 0);
                        docMontoExento = Number(di.monto_exento_calc || 0);
                        docAlicuota = Number(di.alicuota_calc ?? 16);
                        docNroFactOrigen = di.nro_fact_afectado_calc || '0';
                    }
                }

                let finalMontCob = Math.abs(Number(line.mont_cob));
                let adjustedMontoRetencionIva = Number(line.monto_retencion_iva || 0);
                let adjustedMontoRetencion = Number(line.monto_retencion || 0);
                const docTimbreBs = !creditDocTypes.includes(docTypeUpper)
                    ? Math.abs(Number(line.monto_timbre_bs || 0))
                    : 0;

                let totalRebaje = finalMontCob + adjustedMontoRetencionIva + adjustedMontoRetencion + docTimbreBs;
                if (totalRebaje > docSaldo && docSaldo > 0) {
                    const excess = totalRebaje - docSaldo;
                    finalMontCob = Math.max(0, finalMontCob - excess);
                    totalRebaje = Math.min(docSaldo, finalMontCob + adjustedMontoRetencionIva + adjustedMontoRetencion + docTimbreBs);
                } else if (!creditDocTypes.includes(docTypeUpper) && docSaldo > 0 && (docSaldo - totalRebaje) > 0) {
                    const diffPendingBs = docSaldo - totalRebaje;
                    const expectedTimbreFallback = docTimbreBs === 0 && parentLines.length === 1 ? createdTimbreAjnaBs : 0;
                    if (
                        (expectedTimbreFallback > 0 && Math.abs(diffPendingBs - expectedTimbreFallback) <= 0.05) ||
                        diffPendingBs <= 0.05
                    ) {
                        totalRebaje = docSaldo;
                    }
                }

                // Evitar insertar renglones en 0 cuando un crédito/anticipo no consume nada (ej. ya cubierto por otro anterior)
                if (creditDocTypes.includes(docTypeUpper) && totalRebaje <= 0.001) {
                    continue;
                }

                const rengNum = nextRengNum++;
                const guidResult = await transaction.request().query('SELECT NEWID() AS guid');
                const lineGuid = guidResult.recordset[0].guid;
                const cleanLineNro = line.nro_doc?.trim();
                rengDocGuidMap.set(`${docTypeUpper}::${cleanLineNro}`, lineGuid);
                if (parentTypes.includes(docTypeUpper)) {
                    rengDocGuidMap.set(cleanLineNro, lineGuid);
                }

                // Insertar renglón en saPagoDocReng
                await transaction.request()
                    .input('reng_num', sql.Int, rengNum)
                    .input('cob_num', sql.Char(20), padProfit(cobNum, 20))
                    .input('co_tipo_doc', sql.Char(6), padProfit(line.co_tipo_doc, 6))
                    .input('nro_doc', sql.Char(20), padProfit(line.nro_doc, 20))
                    .input('nro_fact', sql.Char(20), padProfit(docNroFact, 20))
                    .input('mont_cob', sql.Decimal(18, 2), totalRebaje)
                    .input('monto_retencion_iva', sql.Decimal(18, 5), adjustedMontoRetencionIva)
                    .input('monto_retencion', sql.Decimal(18, 2), adjustedMontoRetencion)
                    .input('rowguid_reng_ori', sql.UniqueIdentifier, parentGuid)
                    .input('co_sucu_in', sql.Char(6), padProfit(sucuCode, 6))
                    .input('co_us_in', sql.Char(6), padProfit(auditUser, 6))
                    .input('rowguid', sql.UniqueIdentifier, lineGuid)
                    .query(`
                        INSERT INTO saPagoDocReng (
                            reng_num, cob_num, co_tipo_doc, nro_doc, nro_fact, mont_cob,
                            dppago_porc_desc, dppago_monto, monto_retencion_iva, monto_retencion,
                            tipo_doc, num_doc, rowguid_reng_ori,
                            co_sucu_in, co_us_in, fe_us_in, co_sucu_mo, co_us_mo, fe_us_mo,
                            trasnfe, revisado, rowguid
                        ) VALUES (
                            @reng_num, @cob_num, @co_tipo_doc, @nro_doc, @nro_fact, @mont_cob,
                            0.00, 0.00, @monto_retencion_iva, @monto_retencion,
                            NULL, NULL, @rowguid_reng_ori,
                            @co_sucu_in, @co_us_in, GETDATE(), @co_sucu_in, @co_us_in, GETDATE(),
                            NULL, NULL, @rowguid
                        )
                    `);

                // Rebajar saldo en saDocumentoCompra
                await transaction.request()
                    .input('co_tipo_doc', sql.Char(6), padProfit(line.co_tipo_doc, 6))
                    .input('nro_doc', sql.Char(20), padProfit(line.nro_doc, 20))
                    .input('rebaje', sql.Decimal(18, 2), totalRebaje)
                    .input('user', sql.Char(6), padProfit(auditUser, 6))
                    .query(`
                        UPDATE saDocumentoCompra
                        SET saldo = saldo - @rebaje,
                            fe_us_mo = GETDATE(),
                            co_us_mo = @user
                        WHERE LTRIM(RTRIM(co_tipo_doc)) = LTRIM(RTRIM(@co_tipo_doc))
                          AND LTRIM(RTRIM(nro_doc)) = LTRIM(RTRIM(@nro_doc))
                    `);

                // Si es FACT, también rebajar en saFacturaCompra
                if (line.co_tipo_doc.trim().toUpperCase() === 'FACT') {
                    await transaction.request()
                        .input('nro_doc', sql.Char(20), padProfit(line.nro_doc, 20))
                        .input('rebaje', sql.Decimal(18, 2), totalRebaje)
                        .input('user', sql.Char(6), padProfit(auditUser, 6))
                        .query(`
                            UPDATE saFacturaCompra
                            SET saldo = saldo - @rebaje,
                                fe_us_mo = GETDATE(),
                                co_us_mo = @user
                            WHERE LTRIM(RTRIM(doc_num)) = LTRIM(RTRIM(@nro_doc))
                        `);
                }

                // 3.1 Generar documento IVAN (Retención de IVA en compras)
                if (adjustedMontoRetencionIva > 0) {
                    const retIvaMatch = data.retenciones_iva?.find(r => r.nro_doc_asoc?.trim() === line.nro_doc?.trim());
                    const customComp = (retIvaMatch?.num_comprobante || line.num_comprobante || line.num_comprobante_iva || '').trim();

                    let ivanNum = '';
                    let comprobanteSeniat = '';
                    let periodStr = '';

                    if (customComp) {
                        // Comprobante manual / antiguo proporcionado por el usuario
                        if (/^\d{14}$/.test(customComp)) {
                            comprobanteSeniat = customComp;
                            periodStr = customComp.substring(0, 6);
                        } else if (/^\d{1,8}$/.test(customComp)) {
                            const pYear = tsDate.getFullYear();
                            const pMonth = String(tsDate.getMonth() + 1).padStart(2, '0');
                            periodStr = `${pYear}${pMonth}`;
                            comprobanteSeniat = `${periodStr}${customComp.padStart(8, '0')}`;
                        } else {
                            comprobanteSeniat = customComp.substring(0, 14);
                            if (/^\d{6}/.test(customComp)) {
                                periodStr = customComp.substring(0, 6);
                            } else {
                                const pYear = tsDate.getFullYear();
                                const pMonth = String(tsDate.getMonth() + 1).padStart(2, '0');
                                periodStr = `${pYear}${pMonth}`;
                            }
                        }

                        // Obtener prefijo de serie C016 (ej. 'G3')
                        let seriePrefix = 'G3';
                        try {
                            const serieInfoRes = await transaction.request().query(`
                                SELECT TOP 1 LTRIM(RTRIM(ISNULL(prefijo, ''))) AS prefijo 
                                FROM saSerieTipo 
                                WHERE co_tipo_serie = 'C016'
                            `);
                            if (serieInfoRes.recordset[0]?.prefijo) {
                                seriePrefix = serieInfoRes.recordset[0].prefijo;
                            }
                        } catch (ePrefix) {
                            console.warn('[PAGOS AGENT] No se pudo leer prefijo C016:', ePrefix.message);
                        }

                        const numOnly = customComp.replace(/\D/g, '');
                        const seq8 = numOnly.length >= 8 ? numOnly.slice(-8) : numOnly.padStart(8, '0');

                        const candidate1 = `${seriePrefix}${seq8}`;
                        const candidate2 = seq8;
                        const candidate3 = customComp.substring(0, 20);

                        const checkCandidate = async (cand) => {
                            const cRes = await transaction.request()
                                .input('candDoc', sql.Char(20), padProfit(cand, 20))
                                .query(`SELECT TOP 1 nro_doc FROM saDocumentoCompra WHERE co_tipo_doc = 'IVAN' AND LTRIM(RTRIM(nro_doc)) = LTRIM(RTRIM(@candDoc))`);
                            return cRes.recordset.length === 0;
                        };

                        if (await checkCandidate(candidate1)) {
                            ivanNum = candidate1;
                        } else if (await checkCandidate(candidate2)) {
                            ivanNum = candidate2;
                        } else if (await checkCandidate(candidate3)) {
                            ivanNum = candidate3;
                        } else {
                            ivanNum = `${seriePrefix}${seq8}`.substring(0, 18) + 'A';
                        }
                        console.log(`📌 [PAGOS AGENT] Retención IVA manual asignada: Doc=${ivanNum}, Comprobante=${comprobanteSeniat}, Periodo=${periodStr} (Sin consumir correlativo)`);
                    } else {
                        // Modo automático: obtener correlativo y avanzar contador oficial
                        const corrIvan = await getProximoConsecutivoRetencion({
                            runner: transaction,
                            co_tipo_serie: 'IVAN_COMPRA',
                            co_sucur: sucuCode,
                            currentSrvId: srv.id,
                            sqlAuth: req.sqlAuth
                        });
                        ivanNum = corrIvan.docNum;

                        // Formatear comprobante fiscal SENIAT: YYYYMM + 8 dígitos
                        const pYear = tsDate.getFullYear();
                        const pMonth = String(tsDate.getMonth() + 1).padStart(2, "0");
                        periodStr = `${pYear}${pMonth}`;
                        comprobanteSeniat = `${periodStr}${String(corrIvan.proxN).padStart(8, '0')}`;
                        console.log(`✨ [PAGOS AGENT] Retención IVA auto-generada: Doc=${ivanNum}, Comprobante=${comprobanteSeniat}, Periodo=${periodStr}`);
                    }

                    await transaction.request()
                        .input('co_tipo_doc', sql.Char(6), padProfit('IVAN', 6))
                        .input('nro_doc', sql.Char(20), padProfit(ivanNum, 20))
                        .input('co_prov', sql.Char(16), padProfit(data.co_prov, 16))
                        .input('co_mone', sql.Char(6), padProfit(docMone, 6))
                        .input('tasa', sql.Decimal(21, 8), docTasa)
                        .input('co_cta_ingr_egr', sql.Char(20), padProfit(ctaRetIva, 20))
                        .input('observa', sql.VarChar(120), `PAGO N° ${cobNum} de proveedor ${data.co_prov}`)
                        .input('doc_orig', sql.Char(6), padProfit('PAGO', 6))
                        .input('tipo_origen', sql.Int, 0)
                        .input('nro_orig', sql.VarChar(20), cobNum)
                        .input('total_bruto', sql.Decimal(18, 2), adjustedMontoRetencionIva)
                        .input('total_neto', sql.Decimal(18, 2), adjustedMontoRetencionIva)
                        .input('saldo', sql.Decimal(18, 2), 0.00)
                        .input('fec_doc', sql.SmallDateTime, tsDate)
                        .input('co_us_in', sql.Char(6), padProfit(auditUser, 6))
                        .input('co_sucu_in', sql.Char(6), padProfit(sucuCode, 6))
                        .input('num_comprobante', sql.Char(14), comprobanteSeniat.substring(0, 14))
                        .query(`
                            INSERT INTO saDocumentoCompra (
                                co_tipo_doc, nro_doc, co_prov, co_mone, tasa, co_cta_ingr_egr, observa,
                                fec_reg, fec_emis, fec_venc, anulado, aut, pagar,
                                doc_orig, tipo_origen, nro_orig, saldo, total_bruto,
                                total_neto, monto_imp, monto_imp2, monto_imp3, porc_imp, porc_imp2, porc_imp3,
                                otros1, otros2, otros3, adicional, co_us_in, co_sucu_in, fe_us_in,
                                co_us_mo, co_sucu_mo, fe_us_mo, rowguid,
                                monto_desc_glob, monto_reca, num_comprobante, tipo_imp
                            ) VALUES (
                                @co_tipo_doc, @nro_doc, @co_prov, @co_mone, @tasa, @co_cta_ingr_egr, @observa,
                                @fec_doc, @fec_doc, @fec_doc, 0, 1, 0,
                                @doc_orig, @tipo_origen, @nro_orig, @saldo, @total_bruto,
                                @total_neto, 0, 0, 0, 0, 0, 0,
                                0, 0, 0, 0.00, @co_us_in, @co_sucu_in, GETDATE(),
                                @co_us_in, @co_sucu_in, GETDATE(), NEWID(),
                                0, 0, @num_comprobante, '7'
                            )
                        `);

                    // Insertar renglón de IVAN en saPagoDocReng
                    const ivanRengNum = nextRengNum++;
                    const ivanRengGuidResult = await transaction.request().query('SELECT NEWID() AS guid');
                    const ivanRengGuid = ivanRengGuidResult.recordset[0].guid;

                    await transaction.request()
                        .input('reng_num', sql.Int, ivanRengNum)
                        .input('cob_num', sql.Char(20), padProfit(cobNum, 20))
                        .input('co_tipo_doc', sql.Char(6), padProfit('IVAN', 6))
                        .input('nro_doc', sql.Char(20), padProfit(ivanNum, 20))
                        .input('mont_cob', sql.Decimal(18, 2), adjustedMontoRetencionIva)
                        .input('rowguid_reng_ori', sql.UniqueIdentifier, lineGuid)
                        .input('co_sucu_in', sql.Char(6), padProfit(sucuCode, 6))
                        .input('co_us_in', sql.Char(6), padProfit(auditUser, 6))
                        .input('rowguid', sql.UniqueIdentifier, ivanRengGuid)
                        .query(`
                            INSERT INTO saPagoDocReng (
                                reng_num, cob_num, co_tipo_doc, nro_doc, mont_cob,
                                dppago_porc_desc, dppago_monto, monto_retencion_iva, monto_retencion,
                                rowguid_reng_ori, co_sucu_in, co_us_in, fe_us_in, co_sucu_mo, co_us_mo, fe_us_mo, rowguid
                            ) VALUES (
                                @reng_num, @cob_num, @co_tipo_doc, @nro_doc, @mont_cob,
                                0.00, 0.00, 0.00, 0.00,
                                @rowguid_reng_ori, @co_sucu_in, @co_us_in, GETDATE(), @co_sucu_in, @co_us_in, GETDATE(), @rowguid
                            )
                        `);

                    // Insertar en saPagoRetenIvaReng
                    const rifComprador = retIvaMatch?.rif_comprador || data.co_prov;
                    const hasDbBreakdown = docBaseImponible > 0 || docMontoExento > 0;
                    const baseImponible = hasDbBreakdown ? docBaseImponible : Number(retIvaMatch?.base_imponible || 0);
                    const montoExcentoVal = hasDbBreakdown ? docMontoExento : Number(retIvaMatch?.monto_excento || 0);
                    const montoDoc = docTotalNeto > 0 ? docTotalNeto : Number(retIvaMatch?.monto_documento || totalRebaje);
                    const alicuota = docAlicuota > 0 ? docAlicuota : Number(retIvaMatch?.alicuota || 16);
                    const isNcLine = ['N/CR', 'NCR'].includes(docTypeUpper);
                    const isNdLine = ['N/DB', 'NDEB'].includes(docTypeUpper);
                    const retDocType = isNcLine ? 'N/CR' : (isNdLine ? 'N/DB' : 'FACT');
                    const retDocAfectado = (isNcLine || isNdLine)
                        ? (docNroFactOrigen && docNroFactOrigen !== '0'
                            ? docNroFactOrigen
                            : (retIvaMatch?.numero_documento_afectado || retIvaMatch?.nro_doc_afectado || docNroOrig || '0'))
                        : '0';

                    await transaction.request()
                        .input('reng_num', sql.Int, 1)
                        .input('rowguid_reng_cob', sql.UniqueIdentifier, ivanRengGuid)
                        .input('rif_contribuyente', sql.Char(10), (srv.rif || 'J401750354').substring(0, 10))
                        .input('periodo_impositivo', sql.Decimal(6), Number(periodStr))
                        .input('fecha_documento', sql.SmallDateTime, docFecEmis)
                        .input('tipo_documento', sql.Char(4), padProfit(retDocType, 4))
                        .input('rif_comprador', sql.Char(10), rifComprador.substring(0, 10))
                        .input('numero_documento', sql.Char(20), padProfit(docNroFact, 20))
                        .input('numero_control_documento', sql.Char(20), padProfit(docNControl, 20))
                        .input('monto_documento', sql.Decimal(15, 2), montoDoc)
                        .input('base_imponible', sql.Decimal(15, 2), baseImponible)
                        .input('monto_ret_imp', sql.Decimal(15, 2), adjustedMontoRetencionIva)
                        .input('numero_documento_afectado', sql.Char(20), padProfit(retDocAfectado, 20))
                        .input('num_comprobante', sql.Char(14), comprobanteSeniat.substring(0, 14))
                        .input('monto_excento', sql.Decimal(15, 2), montoExcentoVal)
                        .input('alicuota', sql.Decimal(5, 2), alicuota)
                        .input('reten_tercero', sql.Bit, 0)
                        .input('numero_expediente', sql.Char(15), '0              ')
                        .input('co_us_in', sql.Char(6), padProfit(auditUser, 6))
                        .input('co_sucu_in', sql.Char(6), padProfit(sucuCode, 6))
                        .query(`
                            INSERT INTO saPagoRetenIvaReng (
                                reng_num, rowguid_reng_cob, rif_contribuyente, periodo_impositivo,
                                fecha_documento, tipo_operacion, tipo_documento, rif_comprador,
                                numero_documento, numero_control_documento, monto_documento,
                                base_imponible, monto_ret_imp, numero_documento_afectado,
                                num_comprobante, monto_excento, alicuota, reten_tercero,
                                numero_expediente, co_us_in, co_sucu_in, fe_us_in, co_us_mo, co_sucu_mo, fe_us_mo,
                                revisado, trasnfe, rowguid
                            ) VALUES (
                                @reng_num, @rowguid_reng_cob, @rif_contribuyente, @periodo_impositivo,
                                @fecha_documento, 'C', @tipo_documento, @rif_comprador,
                                @numero_documento, @numero_control_documento, @monto_documento,
                                @base_imponible, @monto_ret_imp, @numero_documento_afectado,
                                @num_comprobante, @monto_excento, @alicuota, @reten_tercero,
                                @numero_expediente, @co_us_in, @co_sucu_in, GETDATE(), @co_us_in, @co_sucu_in, GETDATE(),
                                NULL, NULL, NEWID()
                            )
                        `);
                }

                // 3.2 Generar documento ISLR (Retención de ISLR en compras)
                if (adjustedMontoRetencion > 0) {
                    const corrIslr = await getProximoConsecutivoRetencion({
                        runner: transaction,
                        co_tipo_serie: 'ISLR_COMPRA',
                        co_sucur: sucuCode,
                        currentSrvId: srv.id,
                        sqlAuth: req.sqlAuth
                    });
                    const islrNum = corrIslr.docNum;

                    // Formatear comprobante fiscal SENIAT: YYYYMM + 8 dígitos
                    const islrYear = tsDate.getFullYear();
                    const islrMonth = String(tsDate.getMonth() + 1).padStart(2, "0");
                    const islrPeriodStr = `${islrYear}${islrMonth}`;
                    const islrComprobanteSeniat = `${islrPeriodStr}${String(corrIslr.proxN).padStart(8, '0')}`;
                    console.log(`✨ [PAGOS AGENT] Retención ISLR auto-generada: Doc=${islrNum}, Comprobante=${islrComprobanteSeniat}, Periodo=${islrPeriodStr}`);

                    await transaction.request()
                        .input('co_tipo_doc', sql.Char(6), padProfit('ISLR', 6))
                        .input('nro_doc', sql.Char(20), padProfit(islrNum, 20))
                        .input('co_prov', sql.Char(16), padProfit(data.co_prov, 16))
                        .input('co_mone', sql.Char(6), padProfit(docMone, 6))
                        .input('tasa', sql.Decimal(21, 8), docTasa)
                        .input('co_cta_ingr_egr', sql.Char(20), padProfit(ctaRetIslr, 20))
                        .input('observa', sql.VarChar(120), `PAGO N° ${cobNum} de proveedor ${data.co_prov}`)
                        .input('doc_orig', sql.Char(6), padProfit('PAGO', 6))
                        .input('tipo_origen', sql.Int, 0)
                        .input('nro_orig', sql.VarChar(20), cobNum)
                        .input('total_bruto', sql.Decimal(18, 2), adjustedMontoRetencion)
                        .input('total_neto', sql.Decimal(18, 2), adjustedMontoRetencion)
                        .input('saldo', sql.Decimal(18, 2), 0.00)
                        .input('fec_doc', sql.SmallDateTime, tsDate)
                        .input('co_us_in', sql.Char(6), padProfit(auditUser, 6))
                        .input('co_sucu_in', sql.Char(6), padProfit(sucuCode, 6))
                        .input('num_comprobante', sql.Char(14), islrComprobanteSeniat.substring(0, 14))
                        .query(`
                            INSERT INTO saDocumentoCompra (
                                co_tipo_doc, nro_doc, co_prov, co_mone, tasa, co_cta_ingr_egr, observa,
                                fec_reg, fec_emis, fec_venc, anulado, aut, pagar,
                                doc_orig, tipo_origen, nro_orig, saldo, total_bruto,
                                total_neto, monto_imp, monto_imp2, monto_imp3, porc_imp, porc_imp2, porc_imp3,
                                otros1, otros2, otros3, adicional, co_us_in, co_sucu_in, fe_us_in,
                                co_us_mo, co_sucu_mo, fe_us_mo, rowguid,
                                monto_desc_glob, monto_reca, num_comprobante, tipo_imp
                            ) VALUES (
                                @co_tipo_doc, @nro_doc, @co_prov, @co_mone, @tasa, @co_cta_ingr_egr, @observa,
                                @fec_doc, @fec_doc, @fec_doc, 0, 1, 0,
                                @doc_orig, @tipo_origen, @nro_orig, @saldo, @total_bruto,
                                @total_neto, 0, 0, 0, 0, 0, 0,
                                0, 0, 0, 0.00, @co_us_in, @co_sucu_in, GETDATE(),
                                @co_us_in, @co_sucu_in, GETDATE(), NEWID(),
                                0, 0, @num_comprobante, '7'
                            )
                        `);

                    // Insertar renglón de ISLR en saPagoDocReng
                    const islrRengNum = nextRengNum++;
                    const islrRengGuidResult = await transaction.request().query('SELECT NEWID() AS guid');
                    const islrRengGuid = islrRengGuidResult.recordset[0].guid;

                    await transaction.request()
                        .input('reng_num', sql.Int, islrRengNum)
                        .input('cob_num', sql.Char(20), padProfit(cobNum, 20))
                        .input('co_tipo_doc', sql.Char(6), padProfit('ISLR', 6))
                        .input('nro_doc', sql.Char(20), padProfit(islrNum, 20))
                        .input('mont_cob', sql.Decimal(18, 2), adjustedMontoRetencion)
                        .input('rowguid_reng_ori', sql.UniqueIdentifier, lineGuid)
                        .input('co_sucu_in', sql.Char(6), padProfit(sucuCode, 6))
                        .input('co_us_in', sql.Char(6), padProfit(auditUser, 6))
                        .input('rowguid', sql.UniqueIdentifier, islrRengGuid)
                        .query(`
                            INSERT INTO saPagoDocReng (
                                reng_num, cob_num, co_tipo_doc, nro_doc, mont_cob,
                                dppago_porc_desc, dppago_monto, monto_retencion_iva, monto_retencion,
                                rowguid_reng_ori, co_sucu_in, co_us_in, fe_us_in, co_sucu_mo, co_us_mo, fe_us_mo, rowguid
                            ) VALUES (
                                @reng_num, @cob_num, @co_tipo_doc, @nro_doc, @mont_cob,
                                0.00, 0.00, 0.00, 0.00,
                                @rowguid_reng_ori, @co_sucu_in, @co_us_in, GETDATE(), @co_sucu_in, @co_us_in, GETDATE(), @rowguid
                            )
                        `);

                    // Insertar en saPagoRentenReng (soporta múltiples conceptos por documento)
                    try {
                        const docRetList = (data.retenciones_islr || []).filter(r => r.nro_doc_asoc?.trim() === line.nro_doc?.trim());
                        if (docRetList.length > 0) {
                            for (let islrIdx = 0; islrIdx < docRetList.length; islrIdx++) {
                                const retItem = docRetList[islrIdx];
                                const coIslr = retItem.co_islr || '055';
                                const baseIslr = Number(retItem.monto_obj || 0);
                                const montoReten = Number(retItem.monto_reten || (docRetList.length === 1 ? adjustedMontoRetencion : 0));
                                const porcIslr = Number(retItem.porc_retn || 2);
                                const sustraendo = Number(retItem.sustraendo || 0);

                                await transaction.request()
                                    .input('reng_num', sql.Int, islrIdx + 1)
                                    .input('rowguid_reng_cob', sql.UniqueIdentifier, islrRengGuid)
                                    .input('co_islr', sql.Char(6), padProfit(coIslr, 6))
                                    .input('monto', sql.Decimal(18, 5), baseIslr)
                                    .input('monto_reten', sql.Decimal(18, 5), montoReten)
                                    .input('monto_obj', sql.Decimal(18, 5), baseIslr)
                                    .input('sustraendo', sql.Decimal(18, 5), sustraendo)
                                    .input('porc_retn', sql.Decimal(18, 5), porcIslr)
                                    .input('co_us_in', sql.Char(6), padProfit(auditUser, 6))
                                    .input('co_sucu_in', sql.Char(6), padProfit(sucuCode, 6))
                                    .query(`
                                        INSERT INTO saPagoRentenReng (
                                            reng_num, rowguid_reng_cob, co_islr, monto, monto_reten, monto_obj,
                                            sustraendo, porc_retn, automatica, co_us_in, co_sucu_in, fe_us_in, co_us_mo, co_sucu_mo, fe_us_mo,
                                            revisado, trasnfe, rowguid, rowguid_fact
                                        ) VALUES (
                                            @reng_num, @rowguid_reng_cob, @co_islr, @monto, @monto_reten, @monto_obj,
                                            @sustraendo, @porc_retn, 0, @co_us_in, @co_sucu_in, GETDATE(), @co_us_in, @co_sucu_in, GETDATE(),
                                            NULL, NULL, NEWID(), NULL
                                        )
                                    `);
                            }
                        } else {
                            const coIslr = '055';
                            const baseIslr = Number(docSaldo - (Number(line.monto_imp) || 0));
                            await transaction.request()
                                .input('reng_num', sql.Int, 1)
                                .input('rowguid_reng_cob', sql.UniqueIdentifier, islrRengGuid)
                                .input('co_islr', sql.Char(6), padProfit(coIslr, 6))
                                .input('monto', sql.Decimal(18, 5), baseIslr)
                                .input('monto_reten', sql.Decimal(18, 5), adjustedMontoRetencion)
                                .input('monto_obj', sql.Decimal(18, 5), baseIslr)
                                .input('sustraendo', sql.Decimal(18, 5), 0)
                                .input('porc_retn', sql.Decimal(18, 5), 2)
                                .input('co_us_in', sql.Char(6), padProfit(auditUser, 6))
                                .input('co_sucu_in', sql.Char(6), padProfit(sucuCode, 6))
                                .query(`
                                    INSERT INTO saPagoRentenReng (
                                        reng_num, rowguid_reng_cob, co_islr, monto, monto_reten, monto_obj,
                                        sustraendo, porc_retn, automatica, co_us_in, co_sucu_in, fe_us_in, co_us_mo, co_sucu_mo, fe_us_mo,
                                        revisado, trasnfe, rowguid, rowguid_fact
                                    ) VALUES (
                                        @reng_num, @rowguid_reng_cob, @co_islr, @monto, @monto_reten, @monto_obj,
                                        @sustraendo, @porc_retn, 0, @co_us_in, @co_sucu_in, GETDATE(), @co_us_in, @co_sucu_in, GETDATE(),
                                        NULL, NULL, NEWID(), NULL
                                    )
                                `);
                        }
                    } catch (errIslrReng) {
                        console.error('[PAGOS] Error insertando saPagoRentenReng:', errIslrReng);
                        throw errIslrReng;
                    }
                }
            }

            // 3.3 Procesar Ajuste por Saldo a Favor / Diferencia de Compra (AJPA / Serie C010)
            let surplusDocCreated = null;
            if (!isDirectAdvance && data.ajuste_diferencia && (data.ajuste_diferencia.tipo === 'AJPA' || data.ajuste_diferencia.tipo === 'AJNA')) {
                const ajTipo = data.ajuste_diferencia.tipo === 'AJNA' ? 'AJNA' : 'AJPA';
                const coTipoSerie = ajTipo === 'AJNA' ? 'C008' : 'C010';
                const coConsecutivos = ajTipo === 'AJNA'
                    ? ['DOC_COM_AJNA', 'C008', 'AJNA_NUM', 'AJNA']
                    : ['DOC_COM_AJPA', 'C010', 'AJPA_NUM', 'AJPA'];

                const ajMontoBs = Math.abs(Number(data.ajuste_diferencia.monto_bs) || 0);
                if (ajMontoBs > 0.01) {
                    const corrAj = await getProximoConsecutivo({
                        runner: transaction,
                        co_tipo_serie: coTipoSerie,
                        co_consecutivos: coConsecutivos,
                        co_sucur: sucuCode,
                        table: 'saDocumentoCompra',
                        col: 'nro_doc',
                        filter_col: 'co_tipo_doc',
                        filter_val: ajTipo
                    });
                    const ajDocNum = corrAj.docNum;
                    const ajObserva = (`PAGO N° ${cobNum} de proveedor ${data.co_prov}`).substring(0, 120);

                    await transaction.request()
                        .input('co_tipo_doc', sql.Char(6), padProfit(ajTipo, 6))
                        .input('nro_doc', sql.Char(20), padProfit(ajDocNum, 20))
                        .input('co_prov', sql.Char(16), padProfit(data.co_prov, 16))
                        .input('co_mone', sql.Char(6), padProfit(paymentMone, 6))
                        .input('tasa', sql.Decimal(21, 8), Number(data.tasa || 1))
                        .input('co_cta_ingr_egr', sql.Char(20), padProfit(defCtaIE, 20))
                        .input('observa', sql.VarChar(120), ajObserva)
                        .input('fec_doc', sql.SmallDateTime, tsDate)
                        .input('doc_orig', sql.Char(6), padProfit('PAGO', 6))
                        .input('tipo_origen', sql.Int, 0)
                        .input('nro_orig', sql.VarChar(20), cobNum)
                        .input('total_bruto', sql.Decimal(18, 2), ajMontoBs)
                        .input('total_neto', sql.Decimal(18, 2), ajMontoBs)
                        .input('saldo', sql.Decimal(18, 2), ajMontoBs)
                        .input('co_us_in', sql.Char(6), padProfit(auditUser, 6))
                        .input('co_sucu_in', sql.Char(6), padProfit(sucuCode, 6))
                        .query(`
                            INSERT INTO saDocumentoCompra (
                                co_tipo_doc, nro_doc, co_prov, co_mone, tasa, co_cta_ingr_egr, observa,
                                fec_reg, fec_emis, fec_venc, anulado, aut, pagar,
                                doc_orig, tipo_origen, nro_orig, saldo, total_bruto,
                                total_neto, monto_imp, monto_imp2, monto_imp3, porc_imp, porc_imp2, porc_imp3,
                                otros1, otros2, otros3, adicional, co_us_in, co_sucu_in, fe_us_in,
                                co_us_mo, co_sucu_mo, fe_us_mo, rowguid,
                                monto_desc_glob, monto_reca
                            ) VALUES (
                                @co_tipo_doc, @nro_doc, @co_prov, @co_mone, @tasa, @co_cta_ingr_egr, @observa,
                                @fec_doc, @fec_doc, @fec_doc, 0, 1, 0,
                                @doc_orig, @tipo_origen, @nro_orig, @saldo, @total_bruto,
                                @total_neto, 0, 0, 0, 0, 0, 0,
                                0, 0, 0, 0.00, @co_us_in, @co_sucu_in, GETDATE(),
                                @co_us_in, @co_sucu_in, GETDATE(), NEWID(),
                                0, 0
                            )
                        `);

                    const ajRengNum = nextRengNum++;
                    const ajRengGuidResult = await transaction.request().query('SELECT NEWID() AS guid');
                    const ajRengGuid = ajRengGuidResult.recordset[0].guid;

                    await transaction.request()
                        .input('reng_num', sql.Int, ajRengNum)
                        .input('cob_num', sql.Char(20), padProfit(cobNum, 20))
                        .input('co_tipo_doc', sql.Char(6), padProfit(ajTipo, 6))
                        .input('nro_doc', sql.Char(20), padProfit(ajDocNum, 20))
                        .input('mont_cob', sql.Decimal(18, 2), ajMontoBs)
                        .input('co_sucu_in', sql.Char(6), padProfit(sucuCode, 6))
                        .input('co_us_in', sql.Char(6), padProfit(auditUser, 6))
                        .input('rowguid', sql.UniqueIdentifier, ajRengGuid)
                        .query(`
                            INSERT INTO saPagoDocReng (
                                reng_num, cob_num, co_tipo_doc, nro_doc, mont_cob,
                                dppago_porc_desc, dppago_monto, monto_retencion_iva, monto_retencion,
                                co_sucu_in, co_us_in, fe_us_in, co_sucu_mo, co_us_mo, fe_us_mo, rowguid
                            ) VALUES (
                                @reng_num, @cob_num, @co_tipo_doc, @nro_doc, @mont_cob,
                                0.00, 0.00, 0.00, 0.00,
                                @co_sucu_in, @co_us_in, GETDATE(), @co_sucu_in, @co_us_in, GETDATE(), @rowguid
                            )
                        `);
                    surplusDocCreated = { co_tipo_doc: ajTipo, nro_doc: ajDocNum };
                    console.log(`✨ [PAGOS] ${ajTipo} generado con éxito: ${ajDocNum} por ${ajMontoBs} Bs.`);
                }
            }

            // 4. Insertar Formas de Pago (saPagoTPReng) y crear egresos en Caja/Banco
            const validFormasPago = (data.formas_pago || []).filter(tp => Number(tp.mont_doc || 0) > 0);
            const activeFormasPago = validFormasPago.length > 0
                ? validFormasPago
                : [{
                    forma_pag: 'EF',
                    cod_caja: defCaja,
                    cod_cta: null,
                    co_ban: null,
                    co_tar: null,
                    num_doc: null,
                    mont_doc: 0,
                    fecha_che: null
                }];

            const touchedBankAccounts = new Set();
            let firstMovNumB = null;
            let firstFpRef = null;

            for (let i = 0; i < activeFormasPago.length; i++) {
                const tp = activeFormasPago[i];
                const rengNum = i + 1;
                let movNumC = null;
                let movNumB = null;
                const tpFechaChe = parseLocalNoonDate(tp.fecha_che, tsDate);
                const safeDocNum = String(tp.num_doc || '').trim().substring(0, 20);
                const isBankForma = tp.forma_pag === 'TE' || tp.forma_pag === 'TR' || tp.forma_pag === 'DP' || tp.forma_pag === 'CH' || tp.forma_pag === 'TP' || tp.forma_pag === 'ND';
                if (!firstFpRef && safeDocNum) {
                    firstFpRef = safeDocNum;
                }

                if (tp.forma_pag === 'EF') {
                    // Obtener la moneda de la caja
                    let isUSDcaja = false;
                    const resCajaInfo = await transaction.request()
                        .input('codCaja', sql.Char(6), padProfit(tp.cod_caja || defCaja, 6))
                        .query('SELECT RTRIM(co_mone) AS co_mone FROM saCaja WHERE cod_caja = @codCaja');
                    if (resCajaInfo.recordset[0] && isCurrencyUSD(resCajaInfo.recordset[0].co_mone)) {
                        isUSDcaja = true;
                    }

                    const rate = Number(data.tasa || 1);
                    const rawMonto = Number(tp.mont_doc);
                    const finalMontoCaja = isUSDcaja ? round2(rawMonto / rate) : round2(rawMonto);

                    if (finalMontoCaja > 0) {
                        const corrCaja = await getProximoConsecutivo({
                            runner: transaction,
                            co_tipo_serie: 'B001',
                            co_consecutivos: ['MOVC_NUM', 'B001'],
                            co_sucur: sucuCode,
                            table: 'saMovimientoCaja',
                            col: 'mov_num'
                        });
                        movNumC = corrCaja.docNum;

                        // Crear Movimiento de Caja tipo 'E' (Egreso)
                        const rMovC = new sql.Request(transaction);
                        rMovC.input('sMov_Num', sql.Char(20), padProfit(movNumC, 20));
                        rMovC.input('sdFecha', sql.SmallDateTime, tsDate);
                        rMovC.input('sDescrip', sql.VarChar(60), (`${isDirectAdvance ? 'ANTICIPO PAGO' : 'EGRESO PAGO'} ${cobNum} - ${data.co_prov}`).substring(0, 60));
                        rMovC.input('sCod_Caja', sql.Char(6), padProfit(tp.cod_caja || defCaja, 6));
                        rMovC.input('deTasa', sql.Decimal(21, 8), isUSDcaja ? rate : 1);
                        rMovC.input('sTipo_Mov', sql.Char(2), 'E'); // Egreso
                        rMovC.input('sForma_Pag', sql.Char(2), tp.forma_pag);
                        rMovC.input('sNum_Pago', sql.VarChar(20), safeDocNum || null);
                        rMovC.input('sCo_Ban', sql.Char(6), null);
                        rMovC.input('sCo_Tar', sql.Char(6), null);
                        rMovC.input('sCo_Cta_Ingr_Egr', sql.Char(20), padProfit(defCtaIE, 20));
                        rMovC.input('deMonto', sql.Decimal(18, 2), finalMontoCaja);
                        rMovC.input('bSaldo_Ini', sql.Bit, 0);
                        rMovC.input('sOrigen', sql.Char(3), 'PAG');
                        rMovC.input('sDoc_Num', sql.VarChar(20), cobNum.substring(0, 20));
                        rMovC.input('sDep_Num', sql.VarChar(20), null);
                        rMovC.input('bAnulado', sql.Bit, 0);
                        rMovC.input('bDepositado', sql.Bit, 0);
                        rMovC.input('bConciliado', sql.Bit, 0);
                        rMovC.input('bTransferido', sql.Bit, 0);
                        rMovC.input('sdFecha_Che', sql.SmallDateTime, tsDate);
                        rMovC.input('sCo_Us_In', sql.Char(6), padProfit(auditUser, 6));
                        rMovC.input('sCo_Sucu_In', sql.Char(6), padProfit(sucuCode, 6));
                        rMovC.input('sRevisado', sql.Char(1), null);
                        rMovC.input('sTrasnfe', sql.Char(1), null);

                        await rMovC.execute('pInsertarMovimientoCaja');
                    }
                } else if (isBankForma) {
                    // Obtener la moneda de la cuenta bancaria
                    let isUSDcuenta = false;
                    const resCtaInfo = await transaction.request()
                        .input('codCta', sql.Char(6), padProfit(tp.cod_cta, 6))
                        .query('SELECT RTRIM(co_mone) AS co_mone FROM saCuentaBancaria WHERE cod_cta = @codCta');
                    if (resCtaInfo.recordset[0] && isCurrencyUSD(resCtaInfo.recordset[0].co_mone)) {
                        isUSDcuenta = true;
                    }

                    const rate = Number(data.tasa || 1);
                    const rawMonto = Number(tp.mont_doc);
                    const finalMontoBanco = isUSDcuenta ? round2(rawMonto / rate) : round2(rawMonto);

                    if (finalMontoBanco > 0) {
                        movNumB = await generateUniqueMovNum(transaction, sucuCode);

                        let tipoOp = 'TR'; // Transferencia
                        if (tp.forma_pag === 'CH') tipoOp = 'CH';
                        else if (tp.forma_pag === 'ND') tipoOp = 'ND';

                        const movDesc = `${isDirectAdvance ? 'ANTICIPO PAGO' : 'EGRESO PAGO'} ${cobNum} - ${data.co_prov}`.substring(0, 160);
                        await insertMovimientoBancoSP(transaction, {
                            mov_num: movNumB,
                            descrip: movDesc,
                            cod_cta: tp.cod_cta,
                            fecha: tpFechaChe,
                            fecha_che: tpFechaChe,
                            tasa: isUSDcuenta ? rate : 1,
                            tipo_op: tipoOp,
                            doc_num: safeDocNum || movNumB,
                            monto: finalMontoBanco,
                            co_cta_ingr_egr: defCtaIE,
                            origen: 'PAG',
                            cob_pag: cobNum,
                            idb: 0,
                            co_us_in: auditUser,
                            co_sucu_in: sucuCode,
                            maquina: 'WEB'
                        });

                        if (!firstMovNumB) firstMovNumB = movNumB;
                        if (tp.cod_cta) touchedBankAccounts.add(String(tp.cod_cta).trim());
                    }
                }

                // Insertar renglón en saPagoTPReng
                await transaction.request()
                    .input('reng_num', sql.Int, rengNum)
                    .input('cob_num', sql.Char(20), padProfit(cobNum, 20))
                    .input('forma_pag', sql.Char(2), tp.forma_pag === 'EF' ? 'EF' : tp.forma_pag === 'CH' ? 'CH' : 'TR')
                    .input('cod_cta', sql.Char(6), tp.cod_cta ? padProfit(tp.cod_cta, 6) : null)
                    .input('cod_caja', sql.Char(6), tp.cod_caja ? padProfit(tp.cod_caja, 6) : null)
                    .input('mov_num_c', sql.Char(20), movNumC ? padProfit(movNumC, 20) : null)
                    .input('mov_num_b', sql.Char(20), movNumB ? padProfit(movNumB, 20) : null)
                    .input('num_doc', sql.Char(20), isBankForma ? padProfit(safeDocNum, 20) : (safeDocNum ? padProfit(safeDocNum, 20) : null))
                    .input('mont_doc', sql.Decimal(18, 2), Number(tp.mont_doc || 0))
                    .input('fecha_che', sql.SmallDateTime, tpFechaChe)
                    .input('co_sucu_in', sql.Char(6), padProfit(sucuCode, 6))
                    .input('co_us_in', sql.Char(6), padProfit(auditUser, 6))
                    .query(`
                        INSERT INTO saPagoTPReng (
                            reng_num, cob_num, forma_pag, cod_cta, cod_caja,
                            mov_num_c, mov_num_b, num_doc, devuelto, mont_doc, fecha_che,
                            co_sucu_in, co_us_in, fe_us_in, co_sucu_mo, co_us_mo, fe_us_mo,
                            trasnfe, revisado, rowguid
                        ) VALUES (
                            @reng_num, @cob_num, @forma_pag, @cod_cta, @cod_caja,
                            @mov_num_c, @mov_num_b, @num_doc, 0, @mont_doc, @fecha_che,
                            @co_sucu_in, @co_us_in, GETDATE(), @co_sucu_in, @co_us_in, GETDATE(),
                            NULL, NULL, NEWID()
                        )
                    `);
            }

            // Recalcular saSaldoBanco para todas las cuentas bancarias tocadas
            for (const touchedCta of touchedBankAccounts) {
                await syncSaldoBanco(transaction, touchedCta);
            }

            // 4.5 Enlace de mov_ban para ajuste AJPA por excedente o creación de Anticipo Directo
            if (!isDirectAdvance && surplusDocCreated && firstMovNumB) {
                await transaction.request()
                    .input('co_tipo_doc', sql.Char(6), padProfit(surplusDocCreated.co_tipo_doc, 6))
                    .input('nro_doc', sql.Char(20), padProfit(surplusDocCreated.nro_doc, 20))
                    .input('mov_ban', sql.Char(20), padProfit(firstMovNumB, 20))
                    .query(`
                        UPDATE saDocumentoCompra
                        SET mov_ban = @mov_ban
                        WHERE LTRIM(RTRIM(co_tipo_doc)) = LTRIM(RTRIM(@co_tipo_doc))
                          AND LTRIM(RTRIM(nro_doc)) = LTRIM(RTRIM(@nro_doc))
                    `);
            }

            let createdAdvanceDoc = null;
            if (isDirectAdvance && totalFormasPagoBs > 0.01) {
                const reqAdvType = String(data.advance_doc_type || 'ADEL').trim().toUpperCase();
                const advType = ['ADEL', 'AJPA', 'AJNM'].includes(reqAdvType) ? reqAdvType : 'ADEL';
                const advConsecKey = advType === 'AJPA'
                    ? 'DOC_COMPRA_AJPA'
                    : advType === 'AJNM'
                        ? 'DOC_COMPRA_AJNM'
                        : 'DOC_COMPRA_ADEL';

                let advDocNum = null;
                for (let attempt = 0; attempt < 5; attempt++) {
                    const corrAdv = await getProximoConsecutivo({
                        runner: transaction,
                        co_tipo_serie: advConsecKey,
                        co_sucur: sucuCode
                    });
                    const cand = String(corrAdv.docNum || '').trim();
                    if (!cand) continue;
                    const dupChk = await transaction.request()
                        .input('co_tipo_doc', sql.Char(6), padProfit(advType, 6))
                        .input('nro_doc', sql.Char(20), padProfit(cand, 20))
                        .query(`SELECT 1 AS ex FROM saDocumentoCompra WHERE LTRIM(RTRIM(co_tipo_doc)) = LTRIM(RTRIM(@co_tipo_doc)) AND LTRIM(RTRIM(nro_doc)) = LTRIM(RTRIM(@nro_doc))`);
                    if (dupChk.recordset.length === 0) {
                        advDocNum = cand;
                        break;
                    }
                }
                if (!advDocNum) {
                    throw new Error(`No se pudo asignar un número único de documento ${advType} para el anticipo directo.`);
                }

                const advObserva = (data.descrip || `ANTICIPO PAGO N° ${cobNum} - ${data.co_prov}`).substring(0, 120);
                const advMontoBs = round2(totalFormasPagoBs);

                await transaction.request()
                    .input('co_tipo_doc', sql.Char(6), padProfit(advType, 6))
                    .input('nro_doc', sql.Char(20), padProfit(advDocNum, 20))
                    .input('co_prov', sql.Char(16), padProfit(data.co_prov, 16))
                    .input('co_mone', sql.Char(6), padProfit(paymentMone, 6))
                    .input('tasa', sql.Decimal(21, 8), Number(data.tasa || 1))
                    .input('co_cta_ingr_egr', sql.Char(20), padProfit(defCtaIE, 20))
                    .input('observa', sql.VarChar(120), advObserva)
                    .input('fec_doc', sql.SmallDateTime, tsDate)
                    .input('doc_orig', sql.Char(6), padProfit('PAGO', 6))
                    .input('tipo_origen', sql.Int, 0)
                    .input('nro_orig', sql.VarChar(20), cobNum)
                    .input('mov_ban', sql.Char(20), firstMovNumB ? padProfit(firstMovNumB, 20) : null)
                    .input('total_bruto', sql.Decimal(18, 2), advMontoBs)
                    .input('total_neto', sql.Decimal(18, 2), advMontoBs)
                    .input('saldo', sql.Decimal(18, 2), advMontoBs)
                    .input('co_us_in', sql.Char(6), padProfit(auditUser, 6))
                    .input('co_sucu_in', sql.Char(6), padProfit(sucuCode, 6))
                    .query(`
                        INSERT INTO saDocumentoCompra (
                            co_tipo_doc, nro_doc, co_prov, co_mone, tasa, co_cta_ingr_egr, observa,
                            fec_reg, fec_emis, fec_venc, anulado, aut, pagar,
                            doc_orig, tipo_origen, nro_orig, mov_ban, saldo, total_bruto,
                            total_neto, monto_imp, monto_imp2, monto_imp3, porc_imp, porc_imp2, porc_imp3,
                            otros1, otros2, otros3, adicional, co_us_in, co_sucu_in, fe_us_in,
                            co_us_mo, co_sucu_mo, fe_us_mo, rowguid,
                            monto_desc_glob, monto_reca
                        ) VALUES (
                            @co_tipo_doc, @nro_doc, @co_prov, @co_mone, @tasa, @co_cta_ingr_egr, @observa,
                            @fec_doc, @fec_doc, @fec_doc, 0, 1, 0,
                            @doc_orig, @tipo_origen, @nro_orig, @mov_ban, @saldo, @total_bruto,
                            @total_neto, 0, 0, 0, 0, 0, 0,
                            0, 0, 0, 0.00, @co_us_in, @co_sucu_in, GETDATE(),
                            @co_us_in, @co_sucu_in, GETDATE(), NEWID(),
                            0, 0
                        )
                    `);

                const advRengGuidResult = await transaction.request().query('SELECT NEWID() AS guid');
                const advRengGuid = advRengGuidResult.recordset[0].guid;
                await transaction.request()
                    .input('reng_num', sql.Int, nextRengNum++)
                    .input('cob_num', sql.Char(20), padProfit(cobNum, 20))
                    .input('co_tipo_doc', sql.Char(6), padProfit(advType, 6))
                    .input('nro_doc', sql.Char(20), padProfit(advDocNum, 20))
                    .input('mont_cob', sql.Decimal(18, 2), advMontoBs)
                    .input('co_sucu_in', sql.Char(6), padProfit(sucuCode, 6))
                    .input('co_us_in', sql.Char(6), padProfit(auditUser, 6))
                    .input('rowguid', sql.UniqueIdentifier, advRengGuid)
                    .query(`
                        INSERT INTO saPagoDocReng (
                            reng_num, cob_num, co_tipo_doc, nro_doc, mont_cob,
                            dppago_porc_desc, dppago_monto, monto_retencion_iva, monto_retencion,
                            co_sucu_in, co_us_in, fe_us_in, co_sucu_mo, co_us_mo, fe_us_mo, rowguid
                        ) VALUES (
                            @reng_num, @cob_num, @co_tipo_doc, @nro_doc, @mont_cob,
                            0.00, 0.00, 0.00, 0.00,
                            @co_sucu_in, @co_us_in, GETDATE(), @co_sucu_in, @co_us_in, GETDATE(), @rowguid
                        )
                    `);

                createdAdvanceDoc = { co_tipo_doc: advType, nro_doc: advDocNum, saldo: advMontoBs };
                console.log(`✨ [PAGOS] Anticipo Directo ${advType} ${advDocNum} por ${advMontoBs} Bs generado con Pago ${cobNum} (MovBan: ${firstMovNumB || 'N/A'})`);
            }

            await transaction.commit();
            return {
                success: true,
                doc_num: cobNum,
                advance_doc: createdAdvanceDoc,
                mov_ban: firstMovNumB || null
            };
        } catch (err) {
            if (transaction._aborted === false) await transaction.rollback();
            throw err;
        }
    });

    return writeResponse(res, outcome);
});

module.exports = router;
