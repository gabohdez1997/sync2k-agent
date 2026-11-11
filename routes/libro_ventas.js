const express = require('express');
const router = express.Router();
const { sql, getPool, getServers } = require('../db');

/**
 * Helper para verificar qué columnas existen en una tabla
 */
async function getTableColumns(pool, tableName) {
    try {
        const res = await pool.request().query(`
            SELECT COLUMN_NAME 
            FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_NAME = '${tableName}'
        `);
        return new Set(res.recordset.map(c => c.COLUMN_NAME.toLowerCase()));
    } catch {
        return new Set();
    }
}

/**
 * @swagger
 * /api/v1/reportes/libro-ventas:
 *   get:
 *     summary: Reporte fiscal de Libro de Ventas (SENIAT) con documentos fiscales, retenciones de IVA y reporte Z / impresora
 *     tags: [Reportes]
 *     parameters:
 *       - in: query
 *         name: startDate
 *         schema: { type: string, example: '2026-11-01' }
 *       - in: query
 *         name: endDate
 *         schema: { type: string, example: '2026-11-15' }
 *       - in: query
 *         name: sede
 *         schema: { type: string }
 */
router.get('/', async (req, res) => {
    try {
        let sede = req.query.sede || 'default';
        const today = new Date();
        const firstDayOfMonth = new Date(today.getFullYear(), today.getMonth(), 1).toISOString().split('T')[0];
        const todayStr = today.toISOString().split('T')[0];

        const startDate = req.query.startDate || req.query.desde || firstDayOfMonth;
        const endDate = req.query.endDate || req.query.hasta || todayStr;

        const servers = getServers();
        let targetServers = [];

        if (sede === 'all') {
            targetServers = servers;
        } else if (sede === 'default') {
            if (servers && servers.length > 0) {
                targetServers = [servers[0]];
            }
        } else {
            targetServers = servers.filter(s => s.id === sede || s.name === sede);
            if (targetServers.length === 0 && servers.length > 0) {
                targetServers = [servers[0]];
            }
        }

        if (targetServers.length === 0) {
            return res.status(200).json({
                success: true,
                company: null,
                data: [],
                kpis: {
                    total_ventas: 0,
                    total_exento: 0,
                    total_base_general: 0,
                    total_debito_general: 0,
                    total_iva_retenido: 0,
                    total_documentos: 0
                }
            });
        }

        const allDocs = [];
        let primaryCompany = null;

        for (const srv of targetServers) {
            try {
                const pool = await getPool(srv.id, req.sqlAuth);
                const facCols = await getTableColumns(pool, 'saFacturaVenta');
                const devCols = await getTableColumns(pool, 'saDevolucionCliente');
                const docCols = await getTableColumns(pool, 'saDocumentoVenta');

                // 1. Datos de empresa de saParEmp
                if (!primaryCompany) {
                    try {
                        const compRes = await pool.request().query(`
                            SELECT TOP 1 
                                RTRIM(ISNULL(emp_des, '')) AS emp_des,
                                RTRIM(ISNULL(rif, '')) AS rif,
                                RTRIM(ISNULL(direc, '')) AS direc
                            FROM saParEmp
                        `);
                        if (compRes.recordset && compRes.recordset.length > 0) {
                            primaryCompany = compRes.recordset[0];
                        }
                    } catch (eComp) {
                        console.warn('[LIBRO-VENTAS] No se pudo leer saParEmp:', eComp.message);
                    }
                }

                // Construir campos de impresora fiscal según existan en la tabla
                const facImpfisfac = facCols.has('impfisfac') ? "RTRIM(ISNULL(f.impfisfac, ''))" : "''";
                const facImpfis = facCols.has('impfis') ? "RTRIM(ISNULL(f.impfis, ''))" : "''";
                const facNroZ = facCols.has('imp_nro_z') ? "RTRIM(ISNULL(f.imp_nro_z, ''))" : "''";

                const devImpfisfac = devCols.has('impfisfac') ? "RTRIM(ISNULL(dc.impfisfac, ''))" : "''";
                const devImpfis = devCols.has('impfis') ? "RTRIM(ISNULL(dc.impfis, ''))" : "''";
                const devNroZ = devCols.has('imp_nro_z') ? "RTRIM(ISNULL(dc.imp_nro_z, ''))" : (docCols.has('imp_nro_z') ? "(SELECT TOP 1 RTRIM(ISNULL(dv_z.imp_nro_z, '')) FROM saDocumentoVenta dv_z WHERE dv_z.nro_doc = dc.doc_num AND UPPER(RTRIM(dv_z.co_tipo_doc)) IN ('N/CR', 'NCR'))" : "''");

                const docImpfisfac = docCols.has('impfisfac') ? "RTRIM(ISNULL(d.impfisfac, ''))" : "''";
                const docImpfis = docCols.has('impfis') ? "RTRIM(ISNULL(d.impfis, ''))" : "''";
                const docNroZ = docCols.has('imp_nro_z') ? "RTRIM(ISNULL(d.imp_nro_z, ''))" : "''";

                // --- 1. FACTURAS DE VENTA ---
                const qFacturas = `
                    SELECT 
                        'FACT' AS tipo_doc_raw,
                        'FAC' AS tipo_doc,
                        RTRIM(f.doc_num) AS doc_num,
                        RTRIM(ISNULL(f.n_control, '')) AS n_control,
                        f.fec_emis,
                        RTRIM(f.co_cli) AS co_cli,
                        CASE 
                            WHEN f.anulado = 1 OR RTRIM(ISNULL(f.status, '0')) = '3' THEN 'ANULADA'
                            ELSE RTRIM(ISNULL(c.cli_des, f.descrip))
                        END AS cli_des,
                        RTRIM(ISNULL(c.rif, '')) AS rif,
                        f.anulado,
                        RTRIM(ISNULL(f.status, '0')) AS status,
                        ${facImpfisfac} AS impfisfac,
                        ${facImpfis} AS serial_imp,
                        ${facNroZ} AS reporte_z,
                        CASE 
                            WHEN ${facImpfisfac} <> '' THEN ${facImpfisfac}
                            ELSE RTRIM(f.doc_num)
                        END AS comp_fiscal_desde,
                        CASE 
                            WHEN ${facImpfisfac} <> '' THEN ${facImpfisfac}
                            ELSE RTRIM(f.doc_num)
                        END AS comp_fiscal_hasta,
                        '' AS doc_afectado,
                        '' AS tipo_doc_afectado,
                        -- Si está anulada, montos fiscales en 0 para no alterar base
                        CASE WHEN f.anulado = 1 OR RTRIM(ISNULL(f.status, '0')) = '3' THEN 0 ELSE ISNULL(f.total_bruto, 0) END AS total_bruto,
                        CASE WHEN f.anulado = 1 OR RTRIM(ISNULL(f.status, '0')) = '3' THEN 0 ELSE ISNULL(f.monto_imp, 0) END AS monto_imp,
                        CASE WHEN f.anulado = 1 OR RTRIM(ISNULL(f.status, '0')) = '3' THEN 0 ELSE ISNULL(f.total_neto, 0) END AS total_neto,
                        f.tasa,
                        RTRIM(ISNULL(f.co_mone, 'BS')) AS co_mone,
                        -- Desglose de renglones
                        CASE WHEN f.anulado = 1 OR RTRIM(ISNULL(f.status, '0')) = '3' THEN 0 ELSE ISNULL(reng.ventas_exentas, CASE WHEN ISNULL(f.monto_imp, 0) = 0 THEN f.total_bruto ELSE 0 END) END AS ventas_exentas,
                        CASE WHEN f.anulado = 1 OR RTRIM(ISNULL(f.status, '0')) = '3' THEN 0 ELSE ISNULL(reng.base_general, CASE WHEN ISNULL(f.monto_imp, 0) > 0 THEN f.total_bruto ELSE 0 END) END AS base_general,
                        CASE WHEN f.anulado = 1 OR RTRIM(ISNULL(f.status, '0')) = '3' THEN 0 ELSE ISNULL(reng.imp_general, f.monto_imp) END AS imp_general,
                        CASE WHEN f.anulado = 1 OR RTRIM(ISNULL(f.status, '0')) = '3' THEN 0 ELSE ISNULL(reng.base_reducida, 0) END AS base_reducida,
                        CASE WHEN f.anulado = 1 OR RTRIM(ISNULL(f.status, '0')) = '3' THEN 0 ELSE ISNULL(reng.imp_reducida, 0) END AS imp_reducida,
                        CASE WHEN f.anulado = 1 OR RTRIM(ISNULL(f.status, '0')) = '3' THEN 0 ELSE ISNULL(reng.base_adicional, 0) END AS base_adicional,
                        CASE WHEN f.anulado = 1 OR RTRIM(ISNULL(f.status, '0')) = '3' THEN 0 ELSE ISNULL(reng.imp_adicional, 0) END AS imp_adicional,
                        -- Retención de IVA asociada a la factura
                        RTRIM(ISNULL(ret.num_comprobante_iva, '')) AS num_comprobante_iva,
                        ret.fec_emis_ret,
                        ret.fec_recep_ret,
                        ISNULL(ret.iva_retenido, 0) AS iva_retenido
                    FROM saFacturaVenta f
                    LEFT JOIN saCliente c ON f.co_cli = c.co_cli
                    OUTER APPLY (
                        SELECT 
                            SUM(CASE WHEN ISNULL(r.porc_imp, 0) = 0 THEN r.reng_neto ELSE 0 END) AS ventas_exentas,
                            SUM(CASE WHEN r.porc_imp >= 15 AND r.porc_imp <= 17 THEN r.reng_neto ELSE 0 END) AS base_general,
                            SUM(CASE WHEN r.porc_imp >= 15 AND r.porc_imp <= 17 THEN (r.reng_neto * r.porc_imp / 100.0) ELSE 0 END) AS imp_general,
                            SUM(CASE WHEN r.porc_imp > 0 AND r.porc_imp < 15 THEN r.reng_neto ELSE 0 END) AS base_reducida,
                            SUM(CASE WHEN r.porc_imp > 0 AND r.porc_imp < 15 THEN (r.reng_neto * r.porc_imp / 100.0) ELSE 0 END) AS imp_reducida,
                            SUM(CASE WHEN r.porc_imp > 17 THEN r.reng_neto ELSE 0 END) AS base_adicional,
                            SUM(CASE WHEN r.porc_imp > 17 THEN (r.reng_neto * r.porc_imp / 100.0) ELSE 0 END) AS imp_adicional
                        FROM saFacturaVentaReng r
                        WHERE r.doc_num = f.doc_num
                    ) reng
                    OUTER APPLY (
                        SELECT TOP 1
                            COALESCE(NULLIF(RTRIM(ri.num_comprobante), ''), NULLIF(RTRIM(dv.num_comprobante), ''), RTRIM(dv.nro_doc)) AS num_comprobante_iva,
                            COALESCE(ri.fecha_emision, cb.fecha) AS fec_emis_ret,
                            COALESCE(ri.fecha_recepcion, cb.fecha) AS fec_recep_ret,
                            ISNULL(ri.monto_ret_imp, ISNULL(cdr.monto_retencion_iva, dv.total_neto)) AS iva_retenido
                        FROM saCobroDocReng cdr
                        INNER JOIN saCobro cb ON cdr.cob_num = cb.cob_num AND cb.anulado = 0
                        LEFT JOIN saCobroRetenIvaReng ri ON ri.rowguid_reng_cob = cdr.rowguid
                        LEFT JOIN saCobroDocReng cdr_ret ON cdr_ret.cob_num = cb.cob_num AND cdr_ret.rowguid_reng_ori = cdr.rowguid AND UPPER(RTRIM(cdr_ret.co_tipo_doc)) IN ('IVAN', 'IVAP')
                        LEFT JOIN saDocumentoVenta dv ON dv.doc_orig = 'COBRO' AND dv.nro_orig = cb.cob_num AND UPPER(RTRIM(dv.co_tipo_doc)) IN ('IVAN', 'IVAP')
                        WHERE cdr.nro_doc = f.doc_num AND UPPER(RTRIM(cdr.co_tipo_doc)) = 'FACT'
                        ORDER BY cb.fecha DESC
                    ) ret
                    WHERE CONVERT(VARCHAR(10), f.fec_emis, 120) >= @startDate
                      AND CONVERT(VARCHAR(10), f.fec_emis, 120) <= @endDate
                `;

                // --- 2. DEVOLUCIONES / NOTAS DE CRÉDITO ---
                const qDevoluciones = `
                    SELECT 
                        'N/CR' AS tipo_doc_raw,
                        'NC' AS tipo_doc,
                        RTRIM(dc.doc_num) AS doc_num,
                        RTRIM(ISNULL(dc.n_control, '')) AS n_control,
                        dc.fec_emis,
                        RTRIM(dc.co_cli) AS co_cli,
                        CASE 
                            WHEN dc.anulado = 1 OR RTRIM(ISNULL(dc.status, '0')) = '3' THEN 'ANULADA'
                            ELSE RTRIM(ISNULL(c.cli_des, dc.descrip))
                        END AS cli_des,
                        RTRIM(ISNULL(c.rif, '')) AS rif,
                        dc.anulado,
                        RTRIM(ISNULL(dc.status, '0')) AS status,
                        ${devImpfisfac} AS impfisfac,
                        ${devImpfis} AS serial_imp,
                        ${devNroZ} AS reporte_z,
                        CASE 
                            WHEN ${devImpfisfac} <> '' THEN ${devImpfisfac}
                            ELSE RTRIM(dc.doc_num)
                        END AS comp_fiscal_desde,
                        CASE 
                            WHEN ${devImpfisfac} <> '' THEN ${devImpfisfac}
                            ELSE RTRIM(dc.doc_num)
                        END AS comp_fiscal_hasta,
                        RTRIM(ISNULL(dc.nro_doc, '')) AS doc_afectado,
                        'FACT' AS tipo_doc_afectado,
                        CASE WHEN dc.anulado = 1 OR RTRIM(ISNULL(dc.status, '0')) = '3' THEN 0 ELSE -ABS(ISNULL(dc.total_bruto, 0)) END AS total_bruto,
                        CASE WHEN dc.anulado = 1 OR RTRIM(ISNULL(dc.status, '0')) = '3' THEN 0 ELSE -ABS(ISNULL(dc.monto_imp, 0)) END AS monto_imp,
                        CASE WHEN dc.anulado = 1 OR RTRIM(ISNULL(dc.status, '0')) = '3' THEN 0 ELSE -ABS(ISNULL(dc.total_neto, 0)) END AS total_neto,
                        dc.tasa,
                        RTRIM(ISNULL(dc.co_mone, 'BS')) AS co_mone,
                        CASE WHEN dc.anulado = 1 OR RTRIM(ISNULL(dc.status, '0')) = '3' THEN 0 ELSE -ABS(ISNULL(reng.ventas_exentas, 0)) END AS ventas_exentas,
                        CASE WHEN dc.anulado = 1 OR RTRIM(ISNULL(dc.status, '0')) = '3' THEN 0 ELSE -ABS(ISNULL(reng.base_general, CASE WHEN ISNULL(dc.monto_imp, 0) > 0 THEN dc.total_bruto ELSE 0 END)) END AS base_general,
                        CASE WHEN dc.anulado = 1 OR RTRIM(ISNULL(dc.status, '0')) = '3' THEN 0 ELSE -ABS(ISNULL(reng.imp_general, dc.monto_imp)) END AS imp_general,
                        CASE WHEN dc.anulado = 1 OR RTRIM(ISNULL(dc.status, '0')) = '3' THEN 0 ELSE -ABS(ISNULL(reng.base_reducida, 0)) END AS base_reducida,
                        CASE WHEN dc.anulado = 1 OR RTRIM(ISNULL(dc.status, '0')) = '3' THEN 0 ELSE -ABS(ISNULL(reng.imp_reducida, 0)) END AS imp_reducida,
                        CASE WHEN dc.anulado = 1 OR RTRIM(ISNULL(dc.status, '0')) = '3' THEN 0 ELSE -ABS(ISNULL(reng.base_adicional, 0)) END AS base_adicional,
                        CASE WHEN dc.anulado = 1 OR RTRIM(ISNULL(dc.status, '0')) = '3' THEN 0 ELSE -ABS(ISNULL(reng.imp_adicional, 0)) END AS imp_adicional,
                        '' AS num_comprobante_iva,
                        NULL AS fec_emis_ret,
                        NULL AS fec_recep_ret,
                        0 AS iva_retenido
                    FROM saDevolucionCliente dc
                    LEFT JOIN saCliente c ON dc.co_cli = c.co_cli
                    OUTER APPLY (
                        SELECT 
                            SUM(CASE WHEN ISNULL(r.porc_imp, 0) = 0 THEN r.reng_neto ELSE 0 END) AS ventas_exentas,
                            SUM(CASE WHEN r.porc_imp >= 15 AND r.porc_imp <= 17 THEN r.reng_neto ELSE 0 END) AS base_general,
                            SUM(CASE WHEN r.porc_imp >= 15 AND r.porc_imp <= 17 THEN (r.reng_neto * r.porc_imp / 100.0) ELSE 0 END) AS imp_general,
                            SUM(CASE WHEN r.porc_imp > 0 AND r.porc_imp < 15 THEN r.reng_neto ELSE 0 END) AS base_reducida,
                            SUM(CASE WHEN r.porc_imp > 0 AND r.porc_imp < 15 THEN (r.reng_neto * r.porc_imp / 100.0) ELSE 0 END) AS imp_reducida,
                            SUM(CASE WHEN r.porc_imp > 17 THEN r.reng_neto ELSE 0 END) AS base_adicional,
                            SUM(CASE WHEN r.porc_imp > 17 THEN (r.reng_neto * r.porc_imp / 100.0) ELSE 0 END) AS imp_adicional
                        FROM saDevolucionClienteReng r
                        WHERE r.doc_num = dc.doc_num
                    ) reng
                    WHERE CONVERT(VARCHAR(10), dc.fec_emis, 120) >= @startDate
                      AND CONVERT(VARCHAR(10), dc.fec_emis, 120) <= @endDate
                `;

                // --- 3. NOTAS DE DÉBITO ---
                const qNotasDebito = `
                    SELECT 
                        'N/DB' AS tipo_doc_raw,
                        'ND' AS tipo_doc,
                        RTRIM(d.nro_doc) AS doc_num,
                        RTRIM(ISNULL(d.n_control, '')) AS n_control,
                        d.fec_emis,
                        RTRIM(d.co_cli) AS co_cli,
                        CASE 
                            WHEN d.anulado = 1 THEN 'ANULADA'
                            ELSE RTRIM(ISNULL(c.cli_des, d.observa))
                        END AS cli_des,
                        RTRIM(ISNULL(c.rif, '')) AS rif,
                        d.anulado,
                        '0' AS status,
                        ${docImpfisfac} AS impfisfac,
                        ${docImpfis} AS serial_imp,
                        ${docNroZ} AS reporte_z,
                        CASE 
                            WHEN ${docImpfisfac} <> '' THEN ${docImpfisfac}
                            ELSE RTRIM(d.nro_doc)
                        END AS comp_fiscal_desde,
                        CASE 
                            WHEN ${docImpfisfac} <> '' THEN ${docImpfisfac}
                            ELSE RTRIM(d.nro_doc)
                        END AS comp_fiscal_hasta,
                        RTRIM(ISNULL(d.nro_orig, '')) AS doc_afectado,
                        'FACT' AS tipo_doc_afectado,
                        CASE WHEN d.anulado = 1 THEN 0 ELSE ISNULL(d.total_bruto, 0) END AS total_bruto,
                        CASE WHEN d.anulado = 1 THEN 0 ELSE ISNULL(d.monto_imp, 0) END AS monto_imp,
                        CASE WHEN d.anulado = 1 THEN 0 ELSE ISNULL(d.total_neto, 0) END AS total_neto,
                        d.tasa,
                        RTRIM(ISNULL(d.co_mone, 'BS')) AS co_mone,
                        CASE WHEN d.anulado = 1 THEN 0 ELSE CASE WHEN ISNULL(d.monto_imp, 0) = 0 THEN d.total_bruto ELSE 0 END END AS ventas_exentas,
                        CASE WHEN d.anulado = 1 THEN 0 ELSE CASE WHEN ISNULL(d.monto_imp, 0) > 0 THEN d.total_bruto ELSE 0 END END AS base_general,
                        CASE WHEN d.anulado = 1 THEN 0 ELSE ISNULL(d.monto_imp, 0) END AS imp_general,
                        0 AS base_reducida,
                        0 AS imp_reducida,
                        0 AS base_adicional,
                        0 AS imp_adicional,
                        '' AS num_comprobante_iva,
                        NULL AS fec_emis_ret,
                        NULL AS fec_recep_ret,
                        0 AS iva_retenido
                    FROM saDocumentoVenta d
                    LEFT JOIN saCliente c ON d.co_cli = c.co_cli
                    WHERE UPPER(RTRIM(d.co_tipo_doc)) IN ('N/DB', 'NDEB')
                      AND CONVERT(VARCHAR(10), d.fec_emis, 120) >= @startDate
                      AND CONVERT(VARCHAR(10), d.fec_emis, 120) <= @endDate
                `;

                // --- 4. RETENCIONES DE IVA REGISTRADAS COMO DOCUMENTO FISCAL INDEPENDIENTE ---
                const qRetenciones = `
                    SELECT 
                        'RET/IVA' AS tipo_doc_raw,
                        'RET/IVA' AS tipo_doc,
                        RTRIM(ISNULL(ri.num_comprobante, dv.nro_doc)) AS doc_num,
                        RTRIM(ISNULL(ri.numero_control_documento, dv.n_control)) AS n_control,
                        COALESCE(ri.fecha_recepcion, ri.fecha_emision, cb.fecha, dv.fec_emis) AS fec_emis,
                        RTRIM(ISNULL(cb.co_cli, dv.co_cli)) AS co_cli,
                        RTRIM(ISNULL(c.cli_des, '')) AS cli_des,
                        RTRIM(ISNULL(c.rif, '')) AS rif,
                        ISNULL(cb.anulado, dv.anulado) AS anulado,
                        '0' AS status,
                        '' AS impfisfac,
                        '' AS serial_imp,
                        '' AS reporte_z,
                        '' AS comp_fiscal_desde,
                        '' AS comp_fiscal_hasta,
                        RTRIM(ISNULL(ri.numero_documento_afectado, dv.nro_orig)) AS doc_afectado,
                        'FACT' AS tipo_doc_afectado,
                        0 AS total_bruto,
                        0 AS monto_imp,
                        0 AS total_neto,
                        1 AS tasa,
                        'BS' AS co_mone,
                        0 AS ventas_exentas,
                        0 AS base_general,
                        0 AS imp_general,
                        0 AS base_reducida,
                        0 AS imp_reducida,
                        0 AS base_adicional,
                        0 AS imp_adicional,
                        RTRIM(ISNULL(ri.num_comprobante, dv.num_comprobante)) AS num_comprobante_iva,
                        COALESCE(ri.fecha_emision, cb.fecha, dv.fec_emis) AS fec_emis_ret,
                        COALESCE(ri.fecha_recepcion, cb.fecha, dv.fec_emis) AS fec_recep_ret,
                        COALESCE(ri.monto_ret_imp, cdr.monto_retencion_iva, dv.total_neto) AS iva_retenido
                    FROM saCobroDocReng cdr
                    INNER JOIN saCobro cb ON cdr.cob_num = cb.cob_num AND cb.anulado = 0
                    LEFT JOIN saCobroRetenIvaReng ri ON ri.rowguid_reng_cob = cdr.rowguid
                    LEFT JOIN saDocumentoVenta dv ON dv.doc_orig = 'COBRO' AND dv.nro_orig = cb.cob_num AND UPPER(RTRIM(dv.co_tipo_doc)) IN ('IVAN', 'IVAP')
                    LEFT JOIN saCliente c ON cb.co_cli = c.co_cli
                    WHERE UPPER(RTRIM(cdr.co_tipo_doc)) IN ('IVAN', 'IVAP')
                      AND CONVERT(VARCHAR(10), COALESCE(ri.fecha_recepcion, ri.fecha_emision, cb.fecha, dv.fec_emis), 120) >= @startDate
                      AND CONVERT(VARCHAR(10), COALESCE(ri.fecha_recepcion, ri.fecha_emision, cb.fecha, dv.fec_emis), 120) <= @endDate
                `;

                const fullUnionQuery = `
                    WITH LibroUnion AS (
                        ${qFacturas}
                        UNION ALL
                        ${qDevoluciones}
                        UNION ALL
                        ${qNotasDebito}
                        UNION ALL
                        ${qRetenciones}
                    )
                    SELECT * 
                    FROM LibroUnion
                    ORDER BY fec_emis ASC, doc_num ASC
                `;

                const reqSql = pool.request();
                reqSql.input('startDate', sql.VarChar(10), startDate);
                reqSql.input('endDate', sql.VarChar(10), endDate);

                const resUnion = await reqSql.query(fullUnionQuery);
                const records = resUnion.recordset || [];

                for (const row of records) {
                    allDocs.push({
                        ...row,
                        sede_id: srv.id,
                        sede_nombre: srv.business_name || srv.name
                    });
                }
            } catch (srvErr) {
                console.error(`[LIBRO-VENTAS] Error consultando sede ${srv.name}:`, srvErr.message);
            }
        }

        // Ordenar todos los documentos globalmente por fecha y número de documento
        allDocs.sort((a, b) => {
            const dateA = new Date(a.fec_emis).getTime();
            const dateB = new Date(b.fec_emis).getTime();
            if (dateA !== dateB) return dateA - dateB;
            return String(a.doc_num).localeCompare(String(b.doc_num), undefined, { numeric: true });
        });

        // Asignar Operación Nro. correlativa (1, 2, 3...)
        let operIndex = 1;
        for (const doc of allDocs) {
            doc.oper_nro = operIndex++;
        }

        // Detectar si en los resultados existen reporte_z o serial_imp con contenido
        const hasReporteZ = allDocs.some(d => d.reporte_z && String(d.reporte_z).trim() !== '');
        const hasSerialImp = allDocs.some(d => d.serial_imp && String(d.serial_imp).trim() !== '');

        // Calcular totales acumulados y KPIs
        let totalVentas = 0;
        let totalExento = 0;
        let totalBaseGeneral = 0;
        let totalDebitoGeneral = 0;
        let totalBaseReducida = 0;
        let totalDebitoReducida = 0;
        let totalBaseAdicional = 0;
        let totalDebitoAdicional = 0;
        let totalIvaRetenido = 0;

        let cantFacturas = 0;
        let cantNotasCredito = 0;
        let cantNotasDebito = 0;
        let cantRetenciones = 0;
        let cantAnuladas = 0;

        // Para evitar duplicar iva_retenido si vino en la fila de factura y también en la fila RET/IVA:
        const hasSeparateRetRows = allDocs.some(d => d.tipo_doc === 'RET/IVA');

        for (const doc of allDocs) {
            const isAnulada = Number(doc.anulado) === 1 || String(doc.status) === '3';
            if (isAnulada) {
                cantAnuladas++;
                continue; // no suma a los totales fiscales
            }

            if (doc.tipo_doc === 'FAC') cantFacturas++;
            else if (doc.tipo_doc === 'NC') cantNotasCredito++;
            else if (doc.tipo_doc === 'ND') cantNotasDebito++;
            else if (doc.tipo_doc === 'RET/IVA') cantRetenciones++;

            totalVentas += Number(doc.total_neto || 0);
            totalExento += Number(doc.ventas_exentas || 0);
            totalBaseGeneral += Number(doc.base_general || 0);
            totalDebitoGeneral += Number(doc.imp_general || 0);
            totalBaseReducida += Number(doc.base_reducida || 0);
            totalDebitoReducida += Number(doc.imp_reducida || 0);
            totalBaseAdicional += Number(doc.base_adicional || 0);
            totalDebitoAdicional += Number(doc.imp_adicional || 0);

            if (hasSeparateRetRows) {
                if (doc.tipo_doc === 'RET/IVA') {
                    totalIvaRetenido += Number(doc.iva_retenido || 0);
                }
            } else {
                totalIvaRetenido += Number(doc.iva_retenido || 0);
            }
        }

        const kpis = {
            total_ventas: Math.round(totalVentas * 100) / 100,
            total_exento: Math.round(totalExento * 100) / 100,
            total_base_general: Math.round(totalBaseGeneral * 100) / 100,
            total_debito_general: Math.round(totalDebitoGeneral * 100) / 100,
            total_base_reducida: Math.round(totalBaseReducida * 100) / 100,
            total_debito_reducida: Math.round(totalDebitoReducida * 100) / 100,
            total_base_adicional: Math.round(totalBaseAdicional * 100) / 100,
            total_debito_adicional: Math.round(totalDebitoAdicional * 100) / 100,
            total_base_imponible_total: Math.round((totalBaseGeneral + totalBaseReducida + totalBaseAdicional) * 100) / 100,
            total_debito_fiscal_total: Math.round((totalDebitoGeneral + totalDebitoReducida + totalDebitoAdicional) * 100) / 100,
            total_iva_retenido: Math.round(totalIvaRetenido * 100) / 100,
            total_documentos: allDocs.length,
            cant_facturas: cantFacturas,
            cant_notas_credito: cantNotasCredito,
            cant_notas_debito: cantNotasDebito,
            cant_retenciones: cantRetenciones,
            cant_anuladas: cantAnuladas,
            has_reporte_z: hasReporteZ,
            has_serial_imp: hasSerialImp
        };

        return res.status(200).json({
            success: true,
            startDate,
            endDate,
            sede,
            company: primaryCompany || {
                emp_des: targetServers[0]?.business_name || targetServers[0]?.name || 'EMPRESA',
                rif: targetServers[0]?.rif || '',
                direc: targetServers[0]?.address || ''
            },
            hasReporteZ,
            hasSerialImp,
            kpis,
            data: allDocs
        });

    } catch (error) {
        console.error('[LIBRO-VENTAS ERROR]:', error);
        return res.status(500).json({
            success: false,
            message: 'Error al generar Libro de Ventas: ' + error.message,
            error: error.message
        });
    }
});

module.exports = router;

