const express = require('express');
const router = express.Router();
const { sql, getPool, getServers } = require('../db');

/**
 * Helper para verificar qué columnas existen en una tabla de forma dinámica y segura
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
 * /api/v1/reportes/libro-compras:
 *   get:
 *     summary: Reporte fiscal de Libro de Compras (SENIAT) con facturas de compra, notas de crédito/débito y retenciones de IVA a proveedores
 *     tags: [Reportes]
 *     parameters:
 *       - in: query
 *         name: startDate
 *         schema: { type: string, example: '2026-10-01' }
 *       - in: query
 *         name: endDate
 *         schema: { type: string, example: '2026-10-15' }
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
                    total_compras: 0,
                    total_exento: 0,
                    total_base_general: 0,
                    total_credito_general: 0,
                    total_base_reducida: 0,
                    total_credito_reducida: 0,
                    total_base_adicional: 0,
                    total_credito_adicional: 0,
                    total_base_imponible_total: 0,
                    total_credito_fiscal_total: 0,
                    total_iva_retenido: 0,
                    total_documentos: 0,
                    cant_facturas: 0,
                    cant_notas_credito: 0,
                    cant_notas_debito: 0,
                    cant_retenciones: 0,
                    cant_anuladas: 0
                }
            });
        }

        const allDocs = [];
        let primaryCompany = null;

        for (const srv of targetServers) {
            try {
                const pool = await getPool(srv.id, req.sqlAuth);
                const facCols = await getTableColumns(pool, 'saFacturaCompra');
                const devCols = await getTableColumns(pool, 'saDevolucionProveedor');
                const docCols = await getTableColumns(pool, 'saDocumentoCompra');
                const retCols = await getTableColumns(pool, 'saPagoRetenIvaReng');

                // 1. Datos de empresa (Priorizar configuración de sucursales con fallback a par_emp)
                if (!primaryCompany) {
                    let dbComp = {};
                    try {
                        const parCols = await getTableColumns(pool, 'par_emp');
                        if (parCols.size > 0) {
                            const desCol = parCols.has('emp_des') ? 'emp_des' : (parCols.has('empresa') ? 'empresa' : null);
                            const rifCol = parCols.has('rif') ? 'rif' : (parCols.has('rif_emp') ? 'rif_emp' : null);
                            const dirCol = parCols.has('direc') ? 'direc' : (parCols.has('dir_emp') ? 'dir_emp' : (parCols.has('direccion') ? 'direccion' : null));

                            const selectFields = [
                                desCol ? `RTRIM(ISNULL(${desCol}, '')) AS emp_des` : `'' AS emp_des`,
                                rifCol ? `RTRIM(ISNULL(${rifCol}, '')) AS rif` : `'' AS rif`,
                                dirCol ? `RTRIM(ISNULL(${dirCol}, '')) AS direc` : `'' AS direc`
                            ].join(', ');

                            const compRes = await pool.request().query(`SELECT TOP 1 ${selectFields} FROM par_emp`);
                            if (compRes.recordset && compRes.recordset.length > 0) {
                                dbComp = compRes.recordset[0];
                            }
                        }
                    } catch (eComp) {
                        console.warn('[LIBRO-COMPRAS] No se pudo leer par_emp:', eComp.message);
                    }

                    primaryCompany = {
                        emp_des: (srv.business_name && srv.business_name.trim()) || (dbComp.emp_des && dbComp.emp_des.trim()) || srv.name || 'EMPRESA',
                        rif: (srv.rif && srv.rif.trim()) || (dbComp.rif && dbComp.rif.trim()) || '',
                        direc: (srv.address && srv.address.trim()) || (dbComp.direc && dbComp.direc.trim()) || ''
                    };
                }

                // Campos dinámicos de saPagoRetenIvaReng
                const riFecha = retCols.has('fecha_documento')
                    ? "ri.fecha_documento"
                    : (retCols.has('fecha_emision') ? "ri.fecha_emision" : "p.fecha");
                const riComp = retCols.has('num_comprobante') ? "RTRIM(ISNULL(ri.num_comprobante, ''))" : "''";
                const riMonto = retCols.has('monto_ret_imp') ? "ri.monto_ret_imp" : "NULL";

                // --- 1. FACTURAS DE COMPRA (saFacturaCompra) ---
                const qFacturas = `
                    SELECT 
                        'FACT' AS tipo_doc_raw,
                        'FAC' AS tipo_doc,
                        RTRIM(f.doc_num) AS doc_num,
                        RTRIM(ISNULL(f.nro_fact, f.doc_num)) AS nro_fact,
                        RTRIM(ISNULL(f.n_control, '')) AS n_control,
                        f.fec_emis,
                        RTRIM(f.co_prov) AS co_prov,
                        CASE 
                            WHEN f.anulado = 1 OR RTRIM(ISNULL(f.status, '0')) = '3' THEN 'ANULADA'
                            ELSE RTRIM(ISNULL(p.prov_des, f.descrip))
                        END AS prov_des,
                        RTRIM(ISNULL(p.rif, '')) AS rif,
                        f.anulado,
                        RTRIM(ISNULL(f.status, '0')) AS status,
                        '' AS doc_afectado,
                        '' AS tipo_doc_afectado,
                        '' AS nro_expediente,
                        -- Si está anulada, montos fiscales en 0
                        CASE WHEN f.anulado = 1 OR RTRIM(ISNULL(f.status, '0')) = '3' THEN 0 ELSE ISNULL(f.total_bruto, 0) END AS total_bruto,
                        CASE WHEN f.anulado = 1 OR RTRIM(ISNULL(f.status, '0')) = '3' THEN 0 ELSE ISNULL(f.monto_imp, 0) END AS monto_imp,
                        CASE WHEN f.anulado = 1 OR RTRIM(ISNULL(f.status, '0')) = '3' THEN 0 ELSE (ISNULL(f.total_bruto, 0) + ISNULL(f.monto_imp, 0)) END AS total_neto,
                        ISNULL(f.otros1, 0) AS monto_igtf,
                        f.tasa,
                        RTRIM(ISNULL(f.co_mone, 'BS')) AS co_mone,
                        -- Desglose de renglones
                        CASE WHEN f.anulado = 1 OR RTRIM(ISNULL(f.status, '0')) = '3' THEN 0 ELSE ISNULL(reng.compras_exentas, CASE WHEN ISNULL(f.monto_imp, 0) = 0 THEN f.total_bruto ELSE 0 END) END AS compras_exentas,
                        CASE WHEN f.anulado = 1 OR RTRIM(ISNULL(f.status, '0')) = '3' THEN 0 ELSE ISNULL(reng.base_general, CASE WHEN ISNULL(f.monto_imp, 0) > 0 THEN f.total_bruto ELSE 0 END) END AS base_general,
                        CASE WHEN f.anulado = 1 OR RTRIM(ISNULL(f.status, '0')) = '3' THEN 0 ELSE ISNULL(reng.imp_general, f.monto_imp) END AS imp_general,
                        CASE WHEN f.anulado = 1 OR RTRIM(ISNULL(f.status, '0')) = '3' THEN 0 ELSE ISNULL(reng.base_reducida, 0) END AS base_reducida,
                        CASE WHEN f.anulado = 1 OR RTRIM(ISNULL(f.status, '0')) = '3' THEN 0 ELSE ISNULL(reng.imp_reducida, 0) END AS imp_reducida,
                        CASE WHEN f.anulado = 1 OR RTRIM(ISNULL(f.status, '0')) = '3' THEN 0 ELSE ISNULL(reng.base_adicional, 0) END AS base_adicional,
                        CASE WHEN f.anulado = 1 OR RTRIM(ISNULL(f.status, '0')) = '3' THEN 0 ELSE ISNULL(reng.imp_adicional, 0) END AS imp_adicional,
                        -- Retención de IVA soportada
                        RTRIM(ISNULL(ret.num_comprobante_iva, '')) AS num_comprobante_iva,
                        ret.fec_emis_ret,
                        ret.fec_recep_ret,
                        ISNULL(ret.iva_retenido, 0) AS iva_retenido
                    FROM saFacturaCompra f
                    LEFT JOIN saProveedor p ON f.co_prov = p.co_prov
                    OUTER APPLY (
                        SELECT 
                            SUM(CASE WHEN ISNULL(r.porc_imp, 0) = 0 THEN r.reng_neto ELSE 0 END) AS compras_exentas,
                            SUM(CASE WHEN r.porc_imp >= 15 AND r.porc_imp <= 17 THEN r.reng_neto ELSE 0 END) AS base_general,
                            SUM(CASE WHEN r.porc_imp >= 15 AND r.porc_imp <= 17 THEN (r.reng_neto * r.porc_imp / 100.0) ELSE 0 END) AS imp_general,
                            SUM(CASE WHEN r.porc_imp > 0 AND r.porc_imp < 15 THEN r.reng_neto ELSE 0 END) AS base_reducida,
                            SUM(CASE WHEN r.porc_imp > 0 AND r.porc_imp < 15 THEN (r.reng_neto * r.porc_imp / 100.0) ELSE 0 END) AS imp_reducida,
                            SUM(CASE WHEN r.porc_imp > 17 THEN r.reng_neto ELSE 0 END) AS base_adicional,
                            SUM(CASE WHEN r.porc_imp > 17 THEN (r.reng_neto * r.porc_imp / 100.0) ELSE 0 END) AS imp_adicional
                        FROM saFacturaCompraReng r
                        WHERE r.doc_num = f.doc_num
                    ) reng
                    OUTER APPLY (
                        SELECT TOP 1
                            COALESCE(
                                NULLIF(${riComp}, ''),
                                NULLIF(RTRIM(dc_ret.num_comprobante), ''),
                                RTRIM(dc_ret.nro_doc),
                                RTRIM(pdr.nro_doc),
                                ''
                            ) AS num_comprobante_iva,
                            COALESCE(${riFecha}, p.fecha, dc_ret.fec_emis) AS fec_emis_ret,
                            COALESCE(${riFecha}, p.fecha, dc_ret.fec_emis) AS fec_recep_ret,
                            COALESCE(
                                NULLIF(${riMonto}, 0),
                                NULLIF(pdr.monto_retencion_iva, 0),
                                NULLIF(pdr_ret.mont_cob, 0),
                                dc_ret.total_neto,
                                0
                            ) AS iva_retenido
                        FROM saPagoDocReng pdr
                        INNER JOIN saPago p ON pdr.cob_num = p.cob_num AND p.anulado = 0
                        LEFT JOIN saPagoRetenIvaReng ri ON ri.rowguid_reng_cob = pdr.rowguid
                        LEFT JOIN saPagoDocReng pdr_ret ON pdr_ret.cob_num = p.cob_num AND UPPER(RTRIM(pdr_ret.co_tipo_doc)) = 'IVAN'
                        LEFT JOIN saDocumentoCompra dc_ret ON dc_ret.doc_orig = 'PAGO' 
                                                          AND LTRIM(RTRIM(dc_ret.nro_orig)) = LTRIM(RTRIM(p.cob_num)) 
                                                          AND UPPER(RTRIM(dc_ret.co_tipo_doc)) = 'IVAN'
                        WHERE (
                            LTRIM(RTRIM(pdr.nro_doc)) = LTRIM(RTRIM(f.doc_num)) OR
                            (NULLIF(RTRIM(f.nro_fact), '') IS NOT NULL AND LTRIM(RTRIM(pdr.nro_doc)) = LTRIM(RTRIM(f.nro_fact))) OR
                            (NULLIF(RTRIM(f.nro_fact), '') IS NOT NULL AND LTRIM(RTRIM(pdr.nro_fact)) = LTRIM(RTRIM(f.nro_fact)))
                        )
                        ORDER BY p.fecha DESC
                    ) ret
                    WHERE CONVERT(VARCHAR(10), f.fec_emis, 120) >= @startDate
                      AND CONVERT(VARCHAR(10), f.fec_emis, 120) <= @endDate
                `;

                // --- 2. FACTURAS DIRECTAS EN CUENTAS POR PAGAR (saDocumentoCompra no registradas en saFacturaCompra) ---
                const qDirectDocFacturas = `
                    SELECT 
                        'FACT' AS tipo_doc_raw,
                        'FAC' AS tipo_doc,
                        RTRIM(d.nro_doc) AS doc_num,
                        RTRIM(ISNULL(d.nro_fact, d.nro_doc)) AS nro_fact,
                        RTRIM(ISNULL(d.n_control, '')) AS n_control,
                        d.fec_emis,
                        RTRIM(d.co_prov) AS co_prov,
                        CASE 
                            WHEN d.anulado = 1 THEN 'ANULADA'
                            ELSE RTRIM(ISNULL(p.prov_des, d.observa))
                        END AS prov_des,
                        RTRIM(ISNULL(p.rif, '')) AS rif,
                        d.anulado,
                        '0' AS status,
                        '' AS doc_afectado,
                        '' AS tipo_doc_afectado,
                        '' AS nro_expediente,
                        CASE WHEN d.anulado = 1 THEN 0 ELSE ISNULL(d.total_bruto, 0) END AS total_bruto,
                        CASE WHEN d.anulado = 1 THEN 0 ELSE ISNULL(d.monto_imp, 0) END AS monto_imp,
                        CASE WHEN d.anulado = 1 THEN 0 ELSE (ISNULL(d.total_bruto, 0) + ISNULL(d.monto_imp, 0)) END AS total_neto,
                        0 AS monto_igtf,
                        d.tasa,
                        RTRIM(ISNULL(d.co_mone, 'BS')) AS co_mone,
                        CASE WHEN d.anulado = 1 THEN 0 ELSE ISNULL(d.otros1, CASE WHEN ISNULL(d.monto_imp, 0) = 0 THEN d.total_bruto ELSE 0 END) END AS compras_exentas,
                        CASE WHEN d.anulado = 1 THEN 0 ELSE CASE WHEN ISNULL(d.monto_imp, 0) > 0 THEN (d.total_bruto - ISNULL(d.otros1, 0)) ELSE 0 END END AS base_general,
                        CASE WHEN d.anulado = 1 THEN 0 ELSE ISNULL(d.monto_imp, 0) END AS imp_general,
                        0 AS base_reducida,
                        0 AS imp_reducida,
                        0 AS base_adicional,
                        0 AS imp_adicional,
                        RTRIM(ISNULL(ret.num_comprobante_iva, '')) AS num_comprobante_iva,
                        ret.fec_emis_ret,
                        ret.fec_recep_ret,
                        ISNULL(ret.iva_retenido, 0) AS iva_retenido
                    FROM saDocumentoCompra d
                    LEFT JOIN saProveedor p ON d.co_prov = p.co_prov
                    OUTER APPLY (
                        SELECT TOP 1
                            COALESCE(
                                NULLIF(${riComp}, ''),
                                NULLIF(RTRIM(dc_ret.num_comprobante), ''),
                                RTRIM(dc_ret.nro_doc),
                                RTRIM(pdr.nro_doc),
                                ''
                            ) AS num_comprobante_iva,
                            COALESCE(${riFecha}, p.fecha, dc_ret.fec_emis) AS fec_emis_ret,
                            COALESCE(${riFecha}, p.fecha, dc_ret.fec_emis) AS fec_recep_ret,
                            COALESCE(
                                NULLIF(${riMonto}, 0),
                                NULLIF(pdr.monto_retencion_iva, 0),
                                NULLIF(pdr_ret.mont_cob, 0),
                                dc_ret.total_neto,
                                0
                            ) AS iva_retenido
                        FROM saPagoDocReng pdr
                        INNER JOIN saPago p ON pdr.cob_num = p.cob_num AND p.anulado = 0
                        LEFT JOIN saPagoRetenIvaReng ri ON ri.rowguid_reng_cob = pdr.rowguid
                        LEFT JOIN saPagoDocReng pdr_ret ON pdr_ret.cob_num = p.cob_num AND UPPER(RTRIM(pdr_ret.co_tipo_doc)) = 'IVAN'
                        LEFT JOIN saDocumentoCompra dc_ret ON dc_ret.doc_orig = 'PAGO' 
                                                          AND LTRIM(RTRIM(dc_ret.nro_orig)) = LTRIM(RTRIM(p.cob_num)) 
                                                          AND UPPER(RTRIM(dc_ret.co_tipo_doc)) = 'IVAN'
                        WHERE (
                            LTRIM(RTRIM(pdr.nro_doc)) = LTRIM(RTRIM(d.nro_doc)) OR
                            (NULLIF(RTRIM(d.nro_fact), '') IS NOT NULL AND LTRIM(RTRIM(pdr.nro_doc)) = LTRIM(RTRIM(d.nro_fact))) OR
                            (NULLIF(RTRIM(d.nro_fact), '') IS NOT NULL AND LTRIM(RTRIM(pdr.nro_fact)) = LTRIM(RTRIM(d.nro_fact)))
                        )
                        ORDER BY p.fecha DESC
                    ) ret
                    WHERE UPPER(RTRIM(d.co_tipo_doc)) = 'FACT'
                      AND CONVERT(VARCHAR(10), d.fec_emis, 120) >= @startDate
                      AND CONVERT(VARCHAR(10), d.fec_emis, 120) <= @endDate
                      AND NOT EXISTS (
                          SELECT 1 FROM saFacturaCompra f2 
                          WHERE LTRIM(RTRIM(f2.doc_num)) = LTRIM(RTRIM(d.nro_doc))
                             OR (NULLIF(RTRIM(d.nro_fact), '') IS NOT NULL AND LTRIM(RTRIM(f2.nro_fact)) = LTRIM(RTRIM(d.nro_fact)) AND LTRIM(RTRIM(f2.co_prov)) = LTRIM(RTRIM(d.co_prov)))
                      )
                `;

                // --- 3. DEVOLUCIONES / NOTAS DE CRÉDITO A PROVEEDOR (saDevolucionProveedor) ---
                const qDevoluciones = `
                    SELECT 
                        'N/CR' AS tipo_doc_raw,
                        'NC' AS tipo_doc,
                        RTRIM(dp.doc_num) AS doc_num,
                        RTRIM(ISNULL(dp.nro_fact, dp.doc_num)) AS nro_fact,
                        RTRIM(ISNULL(dp.n_control, '')) AS n_control,
                        dp.fec_emis,
                        RTRIM(dp.co_prov) AS co_prov,
                        CASE 
                            WHEN dp.anulado = 1 OR RTRIM(ISNULL(dp.status, '0')) = '3' THEN 'ANULADA'
                            ELSE RTRIM(ISNULL(p.prov_des, dp.descrip))
                        END AS prov_des,
                        RTRIM(ISNULL(p.rif, '')) AS rif,
                        dp.anulado,
                        RTRIM(ISNULL(dp.status, '0')) AS status,
                        RTRIM(ISNULL(dp.nro_fact, '')) AS doc_afectado,
                        'FACT' AS tipo_doc_afectado,
                        '' AS nro_expediente,
                        CASE WHEN dp.anulado = 1 OR RTRIM(ISNULL(dp.status, '0')) = '3' THEN 0 ELSE -ABS(ISNULL(dp.total_bruto, 0)) END AS total_bruto,
                        CASE WHEN dp.anulado = 1 OR RTRIM(ISNULL(dp.status, '0')) = '3' THEN 0 ELSE -ABS(ISNULL(dp.monto_imp, 0)) END AS monto_imp,
                        CASE WHEN dp.anulado = 1 OR RTRIM(ISNULL(dp.status, '0')) = '3' THEN 0 ELSE -(ISNULL(dp.total_bruto, 0) + ISNULL(dp.monto_imp, 0)) END AS total_neto,
                        -ABS(ISNULL(dp.otros1, 0)) AS monto_igtf,
                        dp.tasa,
                        RTRIM(ISNULL(dp.co_mone, 'BS')) AS co_mone,
                        CASE WHEN dp.anulado = 1 OR RTRIM(ISNULL(dp.status, '0')) = '3' THEN 0 ELSE -ABS(ISNULL(reng.compras_exentas, 0)) END AS compras_exentas,
                        CASE WHEN dp.anulado = 1 OR RTRIM(ISNULL(dp.status, '0')) = '3' THEN 0 ELSE -ABS(ISNULL(reng.base_general, CASE WHEN ISNULL(dp.monto_imp, 0) > 0 THEN dp.total_bruto ELSE 0 END)) END AS base_general,
                        CASE WHEN dp.anulado = 1 OR RTRIM(ISNULL(dp.status, '0')) = '3' THEN 0 ELSE -ABS(ISNULL(reng.imp_general, dp.monto_imp)) END AS imp_general,
                        CASE WHEN dp.anulado = 1 OR RTRIM(ISNULL(dp.status, '0')) = '3' THEN 0 ELSE -ABS(ISNULL(reng.base_reducida, 0)) END AS base_reducida,
                        CASE WHEN dp.anulado = 1 OR RTRIM(ISNULL(dp.status, '0')) = '3' THEN 0 ELSE -ABS(ISNULL(reng.imp_reducida, 0)) END AS imp_reducida,
                        CASE WHEN dp.anulado = 1 OR RTRIM(ISNULL(dp.status, '0')) = '3' THEN 0 ELSE -ABS(ISNULL(reng.base_adicional, 0)) END AS base_adicional,
                        CASE WHEN dp.anulado = 1 OR RTRIM(ISNULL(dp.status, '0')) = '3' THEN 0 ELSE -ABS(ISNULL(reng.imp_adicional, 0)) END AS imp_adicional,
                        '' AS num_comprobante_iva,
                        NULL AS fec_emis_ret,
                        NULL AS fec_recep_ret,
                        0 AS iva_retenido
                    FROM saDevolucionProveedor dp
                    LEFT JOIN saProveedor p ON dp.co_prov = p.co_prov
                    OUTER APPLY (
                        SELECT 
                            SUM(CASE WHEN ISNULL(r.porc_imp, 0) = 0 THEN r.reng_neto ELSE 0 END) AS compras_exentas,
                            SUM(CASE WHEN r.porc_imp >= 15 AND r.porc_imp <= 17 THEN r.reng_neto ELSE 0 END) AS base_general,
                            SUM(CASE WHEN r.porc_imp >= 15 AND r.porc_imp <= 17 THEN (r.reng_neto * r.porc_imp / 100.0) ELSE 0 END) AS imp_general,
                            SUM(CASE WHEN r.porc_imp > 0 AND r.porc_imp < 15 THEN r.reng_neto ELSE 0 END) AS base_reducida,
                            SUM(CASE WHEN r.porc_imp > 0 AND r.porc_imp < 15 THEN (r.reng_neto * r.porc_imp / 100.0) ELSE 0 END) AS imp_reducida,
                            SUM(CASE WHEN r.porc_imp > 17 THEN r.reng_neto ELSE 0 END) AS base_adicional,
                            SUM(CASE WHEN r.porc_imp > 17 THEN (r.reng_neto * r.porc_imp / 100.0) ELSE 0 END) AS imp_adicional
                        FROM saDevolucionProveedorReng r
                        WHERE r.doc_num = dp.doc_num
                    ) reng
                    WHERE CONVERT(VARCHAR(10), dp.fec_emis, 120) >= @startDate
                      AND CONVERT(VARCHAR(10), dp.fec_emis, 120) <= @endDate
                `;

                // --- 4. NOTAS DE CRÉDITO DIRECTAS EN CUENTAS POR PAGAR (saDocumentoCompra N/CR) ---
                const qDirectDocNC = `
                    SELECT 
                        'N/CR' AS tipo_doc_raw,
                        'NC' AS tipo_doc,
                        RTRIM(d.nro_doc) AS doc_num,
                        RTRIM(ISNULL(d.nro_fact, d.nro_doc)) AS nro_fact,
                        RTRIM(ISNULL(d.n_control, '')) AS n_control,
                        d.fec_emis,
                        RTRIM(d.co_prov) AS co_prov,
                        CASE 
                            WHEN d.anulado = 1 THEN 'ANULADA'
                            ELSE RTRIM(ISNULL(p.prov_des, d.observa))
                        END AS prov_des,
                        RTRIM(ISNULL(p.rif, '')) AS rif,
                        d.anulado,
                        '0' AS status,
                        RTRIM(ISNULL(d.nro_orig, '')) AS doc_afectado,
                        'FACT' AS tipo_doc_afectado,
                        '' AS nro_expediente,
                        CASE WHEN d.anulado = 1 THEN 0 ELSE -ABS(ISNULL(d.total_bruto, 0)) END AS total_bruto,
                        CASE WHEN d.anulado = 1 THEN 0 ELSE -ABS(ISNULL(d.monto_imp, 0)) END AS monto_imp,
                        CASE WHEN d.anulado = 1 THEN 0 ELSE -(ISNULL(d.total_bruto, 0) + ISNULL(d.monto_imp, 0)) END AS total_neto,
                        0 AS monto_igtf,
                        d.tasa,
                        RTRIM(ISNULL(d.co_mone, 'BS')) AS co_mone,
                        CASE WHEN d.anulado = 1 THEN 0 ELSE -ABS(ISNULL(d.otros1, CASE WHEN ISNULL(d.monto_imp, 0) = 0 THEN d.total_bruto ELSE 0 END)) END AS compras_exentas,
                        CASE WHEN d.anulado = 1 THEN 0 ELSE -ABS(CASE WHEN ISNULL(d.monto_imp, 0) > 0 THEN (d.total_bruto - ISNULL(d.otros1, 0)) ELSE 0 END) END AS base_general,
                        CASE WHEN d.anulado = 1 THEN 0 ELSE -ABS(ISNULL(d.monto_imp, 0)) END AS imp_general,
                        0 AS base_reducida,
                        0 AS imp_reducida,
                        0 AS base_adicional,
                        0 AS imp_adicional,
                        '' AS num_comprobante_iva,
                        NULL AS fec_emis_ret,
                        NULL AS fec_recep_ret,
                        0 AS iva_retenido
                    FROM saDocumentoCompra d
                    LEFT JOIN saProveedor p ON d.co_prov = p.co_prov
                    WHERE UPPER(RTRIM(d.co_tipo_doc)) IN ('N/CR', 'NCRE')
                      AND CONVERT(VARCHAR(10), d.fec_emis, 120) >= @startDate
                      AND CONVERT(VARCHAR(10), d.fec_emis, 120) <= @endDate
                      AND NOT EXISTS (
                          SELECT 1 FROM saDevolucionProveedor dp2 
                          WHERE LTRIM(RTRIM(dp2.doc_num)) = LTRIM(RTRIM(d.nro_doc))
                      )
                `;

                // --- 5. NOTAS DE DÉBITO EN CUENTAS POR PAGAR (saDocumentoCompra N/DB) ---
                const qNotasDebito = `
                    SELECT 
                        'N/DB' AS tipo_doc_raw,
                        'ND' AS tipo_doc,
                        RTRIM(d.nro_doc) AS doc_num,
                        RTRIM(ISNULL(d.nro_fact, d.nro_doc)) AS nro_fact,
                        RTRIM(ISNULL(d.n_control, '')) AS n_control,
                        d.fec_emis,
                        RTRIM(d.co_prov) AS co_prov,
                        CASE 
                            WHEN d.anulado = 1 THEN 'ANULADA'
                            ELSE RTRIM(ISNULL(p.prov_des, d.observa))
                        END AS prov_des,
                        RTRIM(ISNULL(p.rif, '')) AS rif,
                        d.anulado,
                        '0' AS status,
                        RTRIM(ISNULL(d.nro_orig, '')) AS doc_afectado,
                        'FACT' AS tipo_doc_afectado,
                        '' AS nro_expediente,
                        CASE WHEN d.anulado = 1 THEN 0 ELSE ISNULL(d.total_bruto, 0) END AS total_bruto,
                        CASE WHEN d.anulado = 1 THEN 0 ELSE ISNULL(d.monto_imp, 0) END AS monto_imp,
                        CASE WHEN d.anulado = 1 THEN 0 ELSE (ISNULL(d.total_bruto, 0) + ISNULL(d.monto_imp, 0)) END AS total_neto,
                        0 AS monto_igtf,
                        d.tasa,
                        RTRIM(ISNULL(d.co_mone, 'BS')) AS co_mone,
                        CASE WHEN d.anulado = 1 THEN 0 ELSE ISNULL(d.otros1, CASE WHEN ISNULL(d.monto_imp, 0) = 0 THEN d.total_bruto ELSE 0 END) END AS compras_exentas,
                        CASE WHEN d.anulado = 1 THEN 0 ELSE CASE WHEN ISNULL(d.monto_imp, 0) > 0 THEN (d.total_bruto - ISNULL(d.otros1, 0)) ELSE 0 END END AS base_general,
                        CASE WHEN d.anulado = 1 THEN 0 ELSE ISNULL(d.monto_imp, 0) END AS imp_general,
                        0 AS base_reducida,
                        0 AS imp_reducida,
                        0 AS base_adicional,
                        0 AS imp_adicional,
                        '' AS num_comprobante_iva,
                        NULL AS fec_emis_ret,
                        NULL AS fec_recep_ret,
                        0 AS iva_retenido
                    FROM saDocumentoCompra d
                    LEFT JOIN saProveedor p ON d.co_prov = p.co_prov
                    WHERE UPPER(RTRIM(d.co_tipo_doc)) IN ('N/DB', 'NDEB')
                      AND CONVERT(VARCHAR(10), d.fec_emis, 120) >= @startDate
                      AND CONVERT(VARCHAR(10), d.fec_emis, 120) <= @endDate
                `;

                // --- 6. RETENCIONES DE IVA INDEPENDIENTES (Emitidas en el período sobre compras anteriores) ---
                const qRetenciones = `
                    SELECT 
                        'RET/IVA' AS tipo_doc_raw,
                        'RET/IVA' AS tipo_doc,
                        COALESCE(NULLIF(${riComp}, ''), RTRIM(dc_ret.nro_doc), RTRIM(pdr.nro_doc)) AS doc_num,
                        RTRIM(ISNULL(pdr.nro_fact, pdr.nro_doc)) AS nro_fact,
                        COALESCE(RTRIM(dc_ret.n_control), '') AS n_control,
                        COALESCE(${riFecha}, p.fecha, dc_ret.fec_emis) AS fec_emis,
                        RTRIM(ISNULL(p.co_prov, dc_ret.co_prov)) AS co_prov,
                        RTRIM(ISNULL(pr.prov_des, '')) AS prov_des,
                        RTRIM(ISNULL(pr.rif, '')) AS rif,
                        ISNULL(p.anulado, dc_ret.anulado) AS anulado,
                        '0' AS status,
                        COALESCE(RTRIM(dc_ret.nro_orig), RTRIM(pdr.nro_doc)) AS doc_afectado,
                        'FACT' AS tipo_doc_afectado,
                        '' AS nro_expediente,
                        0 AS total_bruto,
                        0 AS monto_imp,
                        0 AS total_neto,
                        0 AS monto_igtf,
                        1 AS tasa,
                        'BS' AS co_mone,
                        0 AS compras_exentas,
                        0 AS base_general,
                        0 AS imp_general,
                        0 AS base_reducida,
                        0 AS imp_reducida,
                        0 AS base_adicional,
                        0 AS imp_adicional,
                        COALESCE(NULLIF(${riComp}, ''), RTRIM(dc_ret.num_comprobante), RTRIM(dc_ret.nro_doc)) AS num_comprobante_iva,
                        COALESCE(${riFecha}, p.fecha, dc_ret.fec_emis) AS fec_emis_ret,
                        COALESCE(${riFecha}, p.fecha, dc_ret.fec_emis) AS fec_recep_ret,
                        COALESCE(NULLIF(${riMonto}, 0), NULLIF(pdr.monto_retencion_iva, 0), dc_ret.total_neto, 0) AS iva_retenido
                    FROM saPagoDocReng pdr
                    INNER JOIN saPago p ON pdr.cob_num = p.cob_num AND p.anulado = 0
                    LEFT JOIN saPagoRetenIvaReng ri ON ri.rowguid_reng_cob = pdr.rowguid
                    LEFT JOIN saDocumentoCompra dc_ret ON dc_ret.doc_orig = 'PAGO' 
                                                      AND LTRIM(RTRIM(dc_ret.nro_orig)) = LTRIM(RTRIM(p.cob_num)) 
                                                      AND UPPER(RTRIM(dc_ret.co_tipo_doc)) = 'IVAN'
                    LEFT JOIN saProveedor pr ON p.co_prov = pr.co_prov
                    WHERE UPPER(RTRIM(pdr.co_tipo_doc)) IN ('IVAN', 'IVAP')
                      AND CONVERT(VARCHAR(10), COALESCE(${riFecha}, p.fecha, dc_ret.fec_emis), 120) >= @startDate
                      AND CONVERT(VARCHAR(10), COALESCE(${riFecha}, p.fecha, dc_ret.fec_emis), 120) <= @endDate
                `;

                const fullUnionQuery = `
                    WITH LibroUnion AS (
                        ${qFacturas}
                        UNION ALL
                        ${qDirectDocFacturas}
                        UNION ALL
                        ${qDevoluciones}
                        UNION ALL
                        ${qDirectDocNC}
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
                        sede_nombre: srv.name || srv.business_name
                    });
                }
            } catch (srvErr) {
                console.error(`[LIBRO-COMPRAS] Error consultando sede ${srv.name}:`, srvErr.message);
            }
        }

        // Ordenar cronológicamente y correlativamente
        allDocs.sort((a, b) => {
            const dateA = new Date(a.fec_emis).getTime();
            const dateB = new Date(b.fec_emis).getTime();
            if (dateA !== dateB) return dateA - dateB;
            return String(a.doc_num).localeCompare(String(b.doc_num), undefined, { numeric: true });
        });

        // Asignar número de operación
        let op = 1;
        for (const doc of allDocs) {
            doc.oper_nro = op++;
        }

        // Totales y KPIs fiscales (SENIAT)
        let totalCompras = 0;
        let totalExento = 0;
        let totalBaseGeneral = 0;
        let totalCreditoGeneral = 0;
        let totalBaseReducida = 0;
        let totalCreditoReducida = 0;
        let totalBaseAdicional = 0;
        let totalCreditoAdicional = 0;
        let totalIvaRetenido = 0;

        let cantFacturas = 0;
        let cantNotasCredito = 0;
        let cantNotasDebito = 0;
        let cantRetenciones = 0;
        let cantAnuladas = 0;

        const hasSeparateRetRows = allDocs.some(d => d.tipo_doc === 'RET/IVA');

        for (const doc of allDocs) {
            const isAnulada = Number(doc.anulado) === 1 || String(doc.status) === '3';
            if (isAnulada) {
                cantAnuladas++;
                continue;
            }

            if (doc.tipo_doc === 'FAC') cantFacturas++;
            else if (doc.tipo_doc === 'NC') cantNotasCredito++;
            else if (doc.tipo_doc === 'ND') cantNotasDebito++;
            else if (doc.tipo_doc === 'RET/IVA') cantRetenciones++;

            totalCompras += Number(doc.total_neto || 0);
            totalExento += Number(doc.compras_exentas || 0);
            totalBaseGeneral += Number(doc.base_general || 0);
            totalCreditoGeneral += Number(doc.imp_general || 0);
            totalBaseReducida += Number(doc.base_reducida || 0);
            totalCreditoReducida += Number(doc.imp_reducida || 0);
            totalBaseAdicional += Number(doc.base_adicional || 0);
            totalCreditoAdicional += Number(doc.imp_adicional || 0);

            if (hasSeparateRetRows) {
                if (doc.tipo_doc === 'RET/IVA') {
                    totalIvaRetenido += Number(doc.iva_retenido || 0);
                }
            } else {
                totalIvaRetenido += Number(doc.iva_retenido || 0);
            }
        }

        return res.status(200).json({
            success: true,
            company: primaryCompany || {
                emp_des: 'EMPRESA',
                rif: '',
                direc: ''
            },
            data: allDocs,
            kpis: {
                total_compras: Math.round(totalCompras * 100) / 100,
                total_exento: Math.round(totalExento * 100) / 100,
                total_base_general: Math.round(totalBaseGeneral * 100) / 100,
                total_credito_general: Math.round(totalCreditoGeneral * 100) / 100,
                total_base_reducida: Math.round(totalBaseReducida * 100) / 100,
                total_credito_reducida: Math.round(totalCreditoReducida * 100) / 100,
                total_base_adicional: Math.round(totalBaseAdicional * 100) / 100,
                total_credito_adicional: Math.round(totalCreditoAdicional * 100) / 100,
                total_base_imponible_total: Math.round((totalBaseGeneral + totalBaseReducida + totalBaseAdicional) * 100) / 100,
                total_credito_fiscal_total: Math.round((totalCreditoGeneral + totalCreditoReducida + totalCreditoAdicional) * 100) / 100,
                total_iva_retenido: Math.round(totalIvaRetenido * 100) / 100,
                total_documentos: allDocs.length,
                cant_facturas: cantFacturas,
                cant_notas_credito: cantNotasCredito,
                cant_notas_debito: cantNotasDebito,
                cant_retenciones: cantRetenciones,
                cant_anuladas: cantAnuladas
            }
        });
    } catch (err) {
        console.error('[LIBRO-COMPRAS] Error fatal:', err);
        return res.status(500).json({
            success: false,
            message: 'Error al generar el Libro de Compras: ' + err.message
        });
    }
});

module.exports = router;
