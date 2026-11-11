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
 *         schema: { type: string, example: '2026-08-01' }
 *       - in: query
 *         name: endDate
 *         schema: { type: string, example: '2026-08-15' }
 *       - in: query
 *         name: sede
 *         schema: { type: string }
 *       - in: query
 *         name: onlyWithRetentions
 *         schema: { type: boolean, default: true }
 */
router.get('/', async (req, res) => {
    try {
        let sede = req.query.sede || 'default';
        const today = new Date();
        const firstDayOfMonth = new Date(today.getFullYear(), today.getMonth(), 1).toISOString().split('T')[0];
        const todayStr = today.toISOString().split('T')[0];

        const startDate = req.query.startDate || req.query.desde || firstDayOfMonth;
        const endDate = req.query.endDate || req.query.hasta || todayStr;
        const onlyWithRetentions = req.query.onlyWithRetentions !== 'false';

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

        let allDocs = [];
        let primaryCompany = null;

        for (const srv of targetServers) {
            try {
                const pool = await getPool(srv.id, req.sqlAuth);
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
                const riBase = retCols.has('base_imponible') ? "ri.base_imponible" : "NULL";
                const riExcento = retCols.has('monto_excento') ? "ri.monto_excento" : "NULL";
                const riDoc = retCols.has('monto_documento') ? "ri.monto_documento" : "NULL";
                const riAlicuota = retCols.has('alicuota') ? "ri.alicuota" : "16.0";
                const riControl = retCols.has('numero_control_documento') ? "RTRIM(ISNULL(ri.numero_control_documento, ''))" : "''";
                const riDocAfectado = retCols.has('numero_documento_afectado') ? "RTRIM(ISNULL(ri.numero_documento_afectado, ''))" : "''";

                // --- 1. FACTURAS Y DOCUMENTOS CON PAGO / RETENCIÓN DEL PERÍODO FISCAL ---
                // En la contabilidad fiscal de Contribuyentes Especiales (SENIAT), el período impositivo
                // de las compras lo determina la fecha del pago y la emisión del comprobante de retención (p.fecha).
                const qPagosConRetencion = `
                    SELECT 
                        'FACT' AS tipo_doc_raw,
                        CASE 
                            WHEN UPPER(RTRIM(pdr.co_tipo_doc)) IN ('N/CR', 'NCRE') THEN 'NC'
                            WHEN UPPER(RTRIM(pdr.co_tipo_doc)) IN ('N/DB', 'NDEB') THEN 'ND'
                            ELSE 'FAC'
                        END AS tipo_doc,
                        COALESCE(
                            NULLIF(RTRIM(d.nro_doc), ''),
                            NULLIF(RTRIM(f.doc_num), ''),
                            RTRIM(pdr.nro_doc)
                        ) AS doc_num,
                        COALESCE(
                            NULLIF(RTRIM(d.nro_fact), ''), 
                            NULLIF(RTRIM(f.nro_fact), ''), 
                            NULLIF(RTRIM(d.nro_doc), ''),
                            NULLIF(RTRIM(pdr.nro_fact), ''), 
                            RTRIM(pdr.nro_doc)
                        ) AS nro_fact,
                        COALESCE(
                            NULLIF(RTRIM(d.n_control), ''), 
                            NULLIF(RTRIM(f.n_control), ''), 
                            NULLIF(${riControl}, ''), 
                            ''
                        ) AS n_control,
                        -- FECHA DE LA FACTURA (Columna B en el Libro de Compras oficial)
                        COALESCE(d.fec_emis, f.fec_emis, ${riFecha}, p.fecha) AS fec_emis,
                        COALESCE(d.fec_emis, f.fec_emis, ${riFecha}, p.fecha) AS fec_factura,
                        RTRIM(ISNULL(p.co_prov, d.co_prov)) AS co_prov,
                        CASE 
                            WHEN ISNULL(d.anulado, 0) = 1 OR ISNULL(f.anulado, 0) = 1 THEN 'ANULADA'
                            ELSE RTRIM(ISNULL(pr.prov_des, ISNULL(d.observa, f.descrip)))
                        END AS prov_des,
                        RTRIM(ISNULL(pr.rif, '')) AS rif,
                        ISNULL(d.anulado, ISNULL(f.anulado, 0)) AS anulado,
                        '0' AS status,
                        CASE 
                            WHEN UPPER(RTRIM(pdr.co_tipo_doc)) IN ('N/CR', 'NCRE') THEN RTRIM(ISNULL(d.nro_orig, ${riDocAfectado}))
                            ELSE ''
                        END AS doc_afectado,
                        'FACT' AS tipo_doc_afectado,
                        '' AS nro_expediente,
                        -- Montos con signo según tipo de documento (NC = negativo)
                        (CASE WHEN UPPER(RTRIM(pdr.co_tipo_doc)) IN ('N/CR', 'NCRE') THEN -1.0 ELSE 1.0 END) * 
                        COALESCE(
                            NULLIF(f.total_bruto, 0),
                            NULLIF(d.total_bruto, 0),
                            NULLIF(${riBase} + ISNULL(${riExcento}, 0), 0),
                            0
                        ) AS total_bruto,
                        (CASE WHEN UPPER(RTRIM(pdr.co_tipo_doc)) IN ('N/CR', 'NCRE') THEN -1.0 ELSE 1.0 END) * 
                        COALESCE(
                            NULLIF(f.monto_imp, 0),
                            NULLIF(d.monto_imp, 0),
                            NULLIF(${riBase} * (ISNULL(${riAlicuota}, 16.0) / 100.0), 0),
                            0
                        ) AS monto_imp,
                        (CASE WHEN UPPER(RTRIM(pdr.co_tipo_doc)) IN ('N/CR', 'NCRE') THEN -1.0 ELSE 1.0 END) * 
                        COALESCE(
                            NULLIF(f.total_bruto + f.monto_imp, 0),
                            NULLIF(d.total_neto, 0),
                            NULLIF(d.total_bruto + d.monto_imp, 0),
                            NULLIF(${riDoc}, 0),
                            0
                        ) AS total_neto,
                        0 AS monto_igtf,
                        ISNULL(d.tasa, ISNULL(f.tasa, 1.0)) AS tasa,
                        COALESCE(RTRIM(d.co_mone), RTRIM(f.co_mone), 'BS') AS co_mone,
                        -- Compras sin derecho a crédito (Exentas)
                        (CASE WHEN UPPER(RTRIM(pdr.co_tipo_doc)) IN ('N/CR', 'NCRE') THEN -1.0 ELSE 1.0 END) * 
                        COALESCE(
                            reng.compras_exentas,
                            d.otros1,
                            ${riExcento},
                            CASE WHEN COALESCE(f.monto_imp, d.monto_imp, 0) = 0 THEN COALESCE(f.total_bruto, d.total_bruto, ${riDoc}, 0) ELSE 0 END,
                            0
                        ) AS compras_exentas,
                        -- Base Imponible General (16%)
                        (CASE WHEN UPPER(RTRIM(pdr.co_tipo_doc)) IN ('N/CR', 'NCRE') THEN -1.0 ELSE 1.0 END) * 
                        COALESCE(
                            reng.base_general,
                            CASE WHEN COALESCE(d.monto_imp, 0) > 0 THEN (d.total_bruto - ISNULL(d.otros1, 0)) ELSE NULL END,
                            ${riBase},
                            0
                        ) AS base_general,
                        -- Impuesto General (16%)
                        (CASE WHEN UPPER(RTRIM(pdr.co_tipo_doc)) IN ('N/CR', 'NCRE') THEN -1.0 ELSE 1.0 END) * 
                        COALESCE(
                            reng.imp_general,
                            d.monto_imp,
                            CASE WHEN ISNULL(${riBase}, 0) > 0 THEN (${riBase} * (ISNULL(${riAlicuota}, 16.0) / 100.0)) ELSE NULL END,
                            0
                        ) AS imp_general,
                        (CASE WHEN UPPER(RTRIM(pdr.co_tipo_doc)) IN ('N/CR', 'NCRE') THEN -1.0 ELSE 1.0 END) * ISNULL(reng.base_reducida, 0) AS base_reducida,
                        (CASE WHEN UPPER(RTRIM(pdr.co_tipo_doc)) IN ('N/CR', 'NCRE') THEN -1.0 ELSE 1.0 END) * ISNULL(reng.imp_reducida, 0) AS imp_reducida,
                        (CASE WHEN UPPER(RTRIM(pdr.co_tipo_doc)) IN ('N/CR', 'NCRE') THEN -1.0 ELSE 1.0 END) * ISNULL(reng.base_adicional, 0) AS base_adicional,
                        (CASE WHEN UPPER(RTRIM(pdr.co_tipo_doc)) IN ('N/CR', 'NCRE') THEN -1.0 ELSE 1.0 END) * ISNULL(reng.imp_adicional, 0) AS imp_adicional,
                        -- Comprobante de Retención
                        COALESCE(
                            NULLIF(${riComp}, ''),
                            NULLIF(RTRIM(dc_ret.num_comprobante), ''),
                            RTRIM(dc_ret.nro_doc),
                            ''
                        ) AS num_comprobante_iva,
                        -- Fecha del Pago / Retención
                        COALESCE(p.fecha, dc_ret.fec_emis, ${riFecha}) AS fec_emis_ret,
                        COALESCE(p.fecha, dc_ret.fec_emis, ${riFecha}) AS fec_recep_ret,
                        (CASE WHEN UPPER(RTRIM(pdr.co_tipo_doc)) IN ('N/CR', 'NCRE') THEN -1.0 ELSE 1.0 END) * 
                        COALESCE(
                            NULLIF(${riMonto}, 0),
                            NULLIF(pdr.monto_retencion_iva, 0),
                            CASE WHEN (SELECT COUNT(1) FROM saPagoDocReng WHERE cob_num = p.cob_num AND co_tipo_doc NOT IN ('IVAN', 'ISLR', 'AJPA')) = 1 THEN NULLIF(dc_ret.total_neto, 0) ELSE NULL END,
                            CASE WHEN (NULLIF(RTRIM(ISNULL(dc_ret.num_comprobante, '')), '') IS NOT NULL OR NULLIF(RTRIM(ISNULL(${riComp}, '')), '') IS NOT NULL) 
                                      AND COALESCE(reng.imp_general, d.monto_imp, 0) > 0 
                                 THEN (COALESCE(reng.imp_general, d.monto_imp, 0) * 0.75) 
                                 ELSE 0 
                            END
                        ) AS iva_retenido,
                        1 AS from_pago
                    FROM saPagoDocReng pdr
                    INNER JOIN saPago p ON pdr.cob_num = p.cob_num AND p.anulado = 0
                    LEFT JOIN saProveedor pr ON p.co_prov = pr.co_prov
                    LEFT JOIN saDocumentoCompra d ON LTRIM(RTRIM(d.co_tipo_doc)) = LTRIM(RTRIM(pdr.co_tipo_doc))
                                                 AND LTRIM(RTRIM(d.nro_doc)) = LTRIM(RTRIM(pdr.nro_doc))
                    LEFT JOIN saFacturaCompra f ON (LTRIM(RTRIM(f.doc_num)) = LTRIM(RTRIM(pdr.nro_doc)) 
                                                OR (NULLIF(RTRIM(d.nro_fact), '') IS NOT NULL AND LTRIM(RTRIM(f.nro_fact)) = LTRIM(RTRIM(d.nro_fact)) AND LTRIM(RTRIM(f.co_prov)) = LTRIM(RTRIM(d.co_prov))))
                    LEFT JOIN saPagoRetenIvaReng ri ON ri.rowguid_reng_cob = pdr.rowguid
                    LEFT JOIN saDocumentoCompra dc_ret ON dc_ret.doc_orig = 'PAGO' 
                                                      AND LTRIM(RTRIM(dc_ret.nro_orig)) = LTRIM(RTRIM(p.cob_num)) 
                                                      AND UPPER(RTRIM(dc_ret.co_tipo_doc)) = 'IVAN'
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
                    WHERE UPPER(RTRIM(pdr.co_tipo_doc)) NOT IN ('IVAN', 'ISLR', 'AJPA')
                      AND CONVERT(VARCHAR(10), COALESCE(p.fecha, dc_ret.fec_emis), 120) >= @startDate
                      AND CONVERT(VARCHAR(10), COALESCE(p.fecha, dc_ret.fec_emis), 120) <= @endDate
                      ${onlyWithRetentions ? `
                      AND (
                          NULLIF(RTRIM(ISNULL(${riComp}, '')), '') IS NOT NULL 
                          OR NULLIF(RTRIM(ISNULL(dc_ret.num_comprobante, '')), '') IS NOT NULL 
                          OR NULLIF(RTRIM(ISNULL(dc_ret.nro_doc, '')), '') IS NOT NULL
                          OR ISNULL(${riMonto}, 0) > 0 
                          OR ISNULL(pdr.monto_retencion_iva, 0) > 0
                      )` : ''}
                `;

                // --- 2. FACTURAS DIRECTAS EMITIDAS EN EL PERÍODO (SOLO PARA MODO NO FILTRADO) ---
                const qFacturas = `
                    SELECT 
                        'FACT' AS tipo_doc_raw,
                        'FAC' AS tipo_doc,
                        RTRIM(f.doc_num) AS doc_num,
                        RTRIM(ISNULL(f.nro_fact, f.doc_num)) AS nro_fact,
                        RTRIM(ISNULL(f.n_control, '')) AS n_control,
                        f.fec_emis,
                        f.fec_emis AS fec_factura,
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
                        CASE WHEN f.anulado = 1 OR RTRIM(ISNULL(f.status, '0')) = '3' THEN 0 ELSE ISNULL(f.total_bruto, 0) END AS total_bruto,
                        CASE WHEN f.anulado = 1 OR RTRIM(ISNULL(f.status, '0')) = '3' THEN 0 ELSE ISNULL(f.monto_imp, 0) END AS monto_imp,
                        CASE WHEN f.anulado = 1 OR RTRIM(ISNULL(f.status, '0')) = '3' THEN 0 ELSE (ISNULL(f.total_bruto, 0) + ISNULL(f.monto_imp, 0)) END AS total_neto,
                        ISNULL(f.otros1, 0) AS monto_igtf,
                        f.tasa,
                        RTRIM(ISNULL(f.co_mone, 'BS')) AS co_mone,
                        CASE WHEN f.anulado = 1 OR RTRIM(ISNULL(f.status, '0')) = '3' THEN 0 ELSE ISNULL(reng.compras_exentas, CASE WHEN ISNULL(f.monto_imp, 0) = 0 THEN f.total_bruto ELSE 0 END) END AS compras_exentas,
                        CASE WHEN f.anulado = 1 OR RTRIM(ISNULL(f.status, '0')) = '3' THEN 0 ELSE ISNULL(reng.base_general, CASE WHEN ISNULL(f.monto_imp, 0) > 0 THEN f.total_bruto ELSE 0 END) END AS base_general,
                        CASE WHEN f.anulado = 1 OR RTRIM(ISNULL(f.status, '0')) = '3' THEN 0 ELSE ISNULL(reng.imp_general, f.monto_imp) END AS imp_general,
                        CASE WHEN f.anulado = 1 OR RTRIM(ISNULL(f.status, '0')) = '3' THEN 0 ELSE ISNULL(reng.base_reducida, 0) END AS base_reducida,
                        CASE WHEN f.anulado = 1 OR RTRIM(ISNULL(f.status, '0')) = '3' THEN 0 ELSE ISNULL(reng.imp_reducida, 0) END AS imp_reducida,
                        CASE WHEN f.anulado = 1 OR RTRIM(ISNULL(f.status, '0')) = '3' THEN 0 ELSE ISNULL(reng.base_adicional, 0) END AS base_adicional,
                        CASE WHEN f.anulado = 1 OR RTRIM(ISNULL(f.status, '0')) = '3' THEN 0 ELSE ISNULL(reng.imp_adicional, 0) END AS imp_adicional,
                        '' AS num_comprobante_iva,
                        NULL AS fec_emis_ret,
                        NULL AS fec_recep_ret,
                        0 AS iva_retenido,
                        0 AS from_pago
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
                    WHERE CONVERT(VARCHAR(10), f.fec_emis, 120) >= @startDate
                      AND CONVERT(VARCHAR(10), f.fec_emis, 120) <= @endDate
                      AND NOT EXISTS (
                          SELECT 1 FROM saPagoDocReng pdr2
                          INNER JOIN saPago p2 ON pdr2.cob_num = p2.cob_num
                          WHERE p2.anulado = 0
                            AND UPPER(RTRIM(pdr2.co_tipo_doc)) NOT IN ('IVAN', 'ISLR', 'AJPA')
                            AND CONVERT(VARCHAR(10), p2.fecha, 120) >= @startDate
                            AND CONVERT(VARCHAR(10), p2.fecha, 120) <= @endDate
                            AND (
                                LTRIM(RTRIM(pdr2.nro_doc)) = LTRIM(RTRIM(f.doc_num)) OR
                                (NULLIF(RTRIM(f.nro_fact), '') IS NOT NULL AND LTRIM(RTRIM(pdr2.nro_fact)) = LTRIM(RTRIM(f.nro_fact)))
                            )
                      )
                `;

                // --- 3. DOCUMENTOS DIRECTOS EN CUENTAS POR PAGAR (SOLO PARA MODO NO FILTRADO) ---
                const qDirectDocFacturas = `
                    SELECT 
                        'FACT' AS tipo_doc_raw,
                        'FAC' AS tipo_doc,
                        RTRIM(d.nro_doc) AS doc_num,
                        RTRIM(ISNULL(d.nro_fact, d.nro_doc)) AS nro_fact,
                        RTRIM(ISNULL(d.n_control, '')) AS n_control,
                        d.fec_emis,
                        d.fec_emis AS fec_factura,
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
                        '' AS num_comprobante_iva,
                        NULL AS fec_emis_ret,
                        NULL AS fec_recep_ret,
                        0 AS iva_retenido,
                        0 AS from_pago
                    FROM saDocumentoCompra d
                    LEFT JOIN saProveedor p ON d.co_prov = p.co_prov
                    WHERE UPPER(RTRIM(d.co_tipo_doc)) = 'FACT'
                      AND CONVERT(VARCHAR(10), d.fec_emis, 120) >= @startDate
                      AND CONVERT(VARCHAR(10), d.fec_emis, 120) <= @endDate
                      AND NOT EXISTS (
                          SELECT 1 FROM saFacturaCompra f2 
                          WHERE LTRIM(RTRIM(f2.doc_num)) = LTRIM(RTRIM(d.nro_doc))
                             OR (NULLIF(RTRIM(d.nro_fact), '') IS NOT NULL AND LTRIM(RTRIM(f2.nro_fact)) = LTRIM(RTRIM(d.nro_fact)) AND LTRIM(RTRIM(f2.co_prov)) = LTRIM(RTRIM(d.co_prov)))
                      )
                      AND NOT EXISTS (
                          SELECT 1 FROM saPagoDocReng pdr2
                          INNER JOIN saPago p2 ON pdr2.cob_num = p2.cob_num
                          WHERE p2.anulado = 0
                            AND UPPER(RTRIM(pdr2.co_tipo_doc)) NOT IN ('IVAN', 'ISLR', 'AJPA')
                            AND CONVERT(VARCHAR(10), p2.fecha, 120) >= @startDate
                            AND CONVERT(VARCHAR(10), p2.fecha, 120) <= @endDate
                            AND (
                                LTRIM(RTRIM(pdr2.nro_doc)) = LTRIM(RTRIM(d.nro_doc)) OR
                                (NULLIF(RTRIM(d.nro_fact), '') IS NOT NULL AND LTRIM(RTRIM(pdr2.nro_fact)) = LTRIM(RTRIM(d.nro_fact)))
                            )
                      )
                `;

                // Query SQL a ejecutar según el modo de consulta
                let querySql = '';
                if (onlyWithRetentions) {
                    // Modo estándar de Libro de Compras oficial SENIAT (filtrado por la quincena del pago/retención)
                    querySql = `
                        ${qPagosConRetencion}
                        ORDER BY fec_emis ASC, doc_num ASC
                    `;
                } else {
                    // Modo general completo (todas las facturas emitidas + pagos con retenciones)
                    querySql = `
                        WITH LibroUnion AS (
                            ${qPagosConRetencion}
                            UNION ALL
                            ${qFacturas}
                            UNION ALL
                            ${qDirectDocFacturas}
                        )
                        SELECT * 
                        FROM LibroUnion
                        ORDER BY fec_emis ASC, doc_num ASC
                    `;
                }

                const reqSql = pool.request();
                reqSql.input('startDate', sql.VarChar(10), startDate);
                reqSql.input('endDate', sql.VarChar(10), endDate);

                const resUnion = await reqSql.query(querySql);
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

        // 1. Deduplicación por número de documento y tipo dentro de la sede
        const seen = new Set();
        const dedupedDocs = [];
        for (const doc of allDocs) {
            const key = `${doc.sede_id || ''}_${doc.tipo_doc}_${(doc.doc_num || '').trim()}_${(doc.nro_fact || '').trim()}`;
            if (!seen.has(key)) {
                seen.add(key);
                dedupedDocs.push(doc);
            }
        }
        allDocs = dedupedDocs;

        // 2. Filtro fiscal estricto: Si está activo "Solo con retención", excluir cualquier pago sin comprobante ni monto retenido
        if (onlyWithRetentions) {
            allDocs = allDocs.filter(doc => {
                const isAnulada = Number(doc.anulado) === 1 || String(doc.status) === '3';
                if (isAnulada) return true;

                const hasComp = doc.num_comprobante_iva && String(doc.num_comprobante_iva).trim() !== '';
                const hasRetMonto = Number(doc.iva_retenido || 0) !== 0;

                return hasComp || hasRetMonto;
            });
        }

        // 3. Ordenar cronológicamente por fecha de la factura y número de documento (idéntico al Libro de Compras oficial en Excel)
        allDocs.sort((a, b) => {
            const dateA = new Date(a.fec_emis).getTime();
            const dateB = new Date(b.fec_emis).getTime();
            if (dateA !== dateB) return dateA - dateB;
            return String(a.doc_num).localeCompare(String(b.doc_num), undefined, { numeric: true });
        });

        // 3. Asignar número de operación oficial (correlativo 1, 2, 3...)
        let op = 1;
        for (const doc of allDocs) {
            doc.oper_nro = op++;
        }

        // 4. Totales y KPIs fiscales (SENIAT)
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
            totalIvaRetenido += Number(doc.iva_retenido || 0);
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
