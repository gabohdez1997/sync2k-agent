const express = require('express');
const router = express.Router();
const { sql, getPool, getServers, getExchangeRate } = require('../db');

/**
 * @swagger
 * tags:
 *   name: Reportes
 *   description: Reportes de negocio y análisis en tiempo real
 */

/**
 * @swagger
 * /api/v1/reportes/cxc:
 *   get:
 *     summary: Obtiene cuentas por cobrar pendientes (CxC) con saldos y métricas asociadas
 *     tags: [Reportes]
 *     parameters:
 *       - in: query
 *         name: page
 *         schema: { type: integer, default: 1 }
 *       - in: query
 *         name: limit
 *         schema: { type: integer, default: 10 }
 *       - in: query
 *         name: search
 *         schema: { type: string }
 *       - in: query
 *         name: co_ven
 *         schema: { type: string }
 *       - in: query
 *         name: tipo_doc
 *         schema: { type: string }
 *       - in: query
 *         name: status
 *         schema: { type: string, enum: [all, vencidos, por_vencer] }
 *       - in: query
 *         name: sede
 *         schema: { type: string }
 *     responses:
 *       200:
 *         description: Listado paginado de CxC con métricas
 */
router.get('/cxc', async (req, res) => {
    console.log('============= [REPORTES/CXC HIT] =============');
    console.log('[REPORTES/CXC] QUERY COMPLETO:', JSON.stringify(req.query));
    try {
        const page = parseInt(req.query.page) || 1;
        const limit = parseInt(req.query.limit) || 10;
        const search = (req.query.search || "").trim();
        const co_ven = (req.query.co_ven || "").trim();
        const tipo_doc = (req.query.tipo_doc || "").trim();
        const status = req.query.status || "all"; // all, vencidos, por_vencer
        const sede = req.query.sede || "";

        const servers = getServers();
        const targets = sede ? servers.filter(s => s.id === sede) : servers;

        if (targets.length === 0) {
            return res.status(200).json({
                success: true,
                metrics: {
                    total_outstanding_usd: 0,
                    total_outstanding_bs: 0,
                    total_overdue_usd: 0,
                    total_overdue_bs: 0,
                    total_upcoming_usd: 0,
                    total_upcoming_bs: 0,
                    doc_count: 0
                },
                data: [],
                page,
                limit,
                total_items: 0,
                total_pages: 0
            });
        }

        // Obtener tasa de cambio actual para conversiones
        let currentRate = 50.0; // Tasa fallback
        try {
            const firstPool = await getPool(targets[0].id, req.sqlAuth);
            const rateValue = await getExchangeRate(firstPool);
            if (rateValue && typeof rateValue === 'number' && rateValue > 0) {
                currentRate = rateValue;
            }
            console.log('[REPORTES/CXC] Tasa actual obtenida:', currentRate);
        } catch (e) {
            console.warn('[REPORTES/CXC] No se pudo obtener tasa oficial, usando fallback:', currentRate);
        }

        const allData = await Promise.all(targets.map(async (srv) => {
            try {
                const pool = await getPool(srv.id, req.sqlAuth);
                const r = pool.request();
                
                let whereClauses = ["d.saldo <> 0 AND d.anulado = 0"];

                if (search !== "") {
                    r.input('search', sql.VarChar, `%${search}%`);
                    whereClauses.push("(d.nro_doc LIKE @search OR d.co_cli LIKE @search OR c.cli_des LIKE @search)");
                }

                if (co_ven !== "") {
                    r.input('co_ven', sql.VarChar, co_ven.toUpperCase());
                    whereClauses.push("LTRIM(RTRIM(d.co_ven)) = @co_ven");
                }

                if (tipo_doc !== "" && tipo_doc !== "all") {
                    r.input('tipo_doc', sql.VarChar, tipo_doc.toUpperCase());
                    whereClauses.push("LTRIM(RTRIM(d.co_tipo_doc)) = @tipo_doc");
                }

                if (status === "vencidos") {
                    whereClauses.push("CAST(d.fec_venc AS DATE) < CAST(GETDATE() AS DATE)");
                } else if (status === "por_vencer") {
                    whereClauses.push("CAST(d.fec_venc AS DATE) >= CAST(GETDATE() AS DATE)");
                }

                const whereSQL = whereClauses.join(" AND ");
                const querySQL = `
                    SELECT 
                        RTRIM(d.nro_doc) AS nro_doc, 
                        RTRIM(d.co_tipo_doc) AS co_tipo_doc, 
                        RTRIM(d.co_cli) AS co_cli, 
                        RTRIM(c.cli_des) AS cli_des,
                        d.fec_emis, 
                        d.fec_venc, 
                        d.total_neto, 
                        d.saldo, 
                        d.anulado, 
                        RTRIM(d.co_ven) AS co_ven, 
                        RTRIM(d.co_mone) AS co_mone, 
                        d.tasa AS doc_tasa,
                        RTRIM(d.nro_orig) AS nro_orig,
                        RTRIM(d.doc_orig) AS doc_orig,
                        (
                            SELECT TOP 1 RTRIM(r.tipo_doc)
                            FROM saDevolucionClienteReng r
                            WHERE r.doc_num = d.nro_orig
                        ) AS devol_tipo_doc,
                        (
                            SELECT TOP 1 RTRIM(r.num_doc)
                            FROM saDevolucionClienteReng r
                            WHERE r.doc_num = d.nro_orig
                        ) AS devol_num_doc,
                        (
                            SELECT TOP 1 t.tasa_v
                            FROM saTasa t
                            WHERE LTRIM(RTRIM(t.co_mone)) IN ('USD', 'US$', 'US')
                              AND t.fecha <= d.fec_emis
                            ORDER BY t.fecha DESC
                        ) AS tasa_bcv_fecha
                    FROM saDocumentoVenta d
                    LEFT JOIN saCliente c ON d.co_cli = c.co_cli
                    WHERE ${whereSQL}
                    ORDER BY d.fec_emis DESC
                `;

                const resData = await r.query(querySQL);
                return resData.recordset.map(row => {
                    const rowMone = (row.co_mone || "").trim().toUpperCase();
                    const rowTasa = parseFloat(row.tasa_bcv_fecha) || parseFloat(row.doc_tasa) || currentRate || 1.0;
                    const saldo = parseFloat(row.saldo) || 0.0;
                    const total = parseFloat(row.total_neto) || 0.0;

                    const isCred = (row.co_tipo_doc || "").trim().toUpperCase() === 'N/CR';

                    // El saldo y total siempre se almacenan en BS en saDocumentoVenta (moneda base bolívares en Profit)
                    const saldoBs = isCred ? -saldo : saldo;
                    const saldoUsd = isCred ? -saldo / (rowTasa > 0 ? rowTasa : 1.0) : saldo / (rowTasa > 0 ? rowTasa : 1.0);
                    const totalBs = isCred ? -total : total;
                    const totalUsd = isCred ? -total / (rowTasa > 0 ? rowTasa : 1.0) : total / (rowTasa > 0 ? rowTasa : 1.0);

                    // Calcular días de retraso / vencimiento
                    const today = new Date();
                    const fecVenc = new Date(row.fec_venc);
                    const diffTime = today.getTime() - fecVenc.getTime();
                    const diffDays = Math.ceil(diffTime / (1000 * 60 * 60 * 24));
                    const isVencido = !isCred && diffDays > 0;

                    const docOrig = (row.doc_orig || "").trim().toUpperCase();
                    const nroOrig = (row.nro_orig || "").trim();
                    const devolTipoDoc = (row.devol_tipo_doc || "").trim();
                    const devolNumDoc = (row.devol_num_doc || "").trim();

                    const finalDocOrig = docOrig === 'DEVO' && devolTipoDoc ? devolTipoDoc : docOrig;
                    const finalNroOrig = docOrig === 'DEVO' && devolNumDoc ? devolNumDoc : nroOrig;

                    return {
                        nro_doc: row.nro_doc,
                        co_tipo_doc: row.co_tipo_doc,
                        co_cli: row.co_cli,
                        cli_des: row.cli_des || 'Cliente Desconocido',
                        fec_emis: row.fec_emis,
                        fec_venc: row.fec_venc,
                        co_mone: rowMone,
                        tasa: rowTasa,
                        total_original: isCred ? -total : total,
                        saldo_original: isCred ? -saldo : saldo,
                        total_usd: parseFloat(totalUsd.toFixed(2)),
                        total_bs: parseFloat(totalBs.toFixed(2)),
                        saldo_usd: parseFloat(saldoUsd.toFixed(2)),
                        saldo_bs: parseFloat(saldoBs.toFixed(2)),
                        co_ven: row.co_ven,
                        dias_vencidos: isVencido ? diffDays : 0,
                        vencido: isVencido,
                        sede_id: srv.id,
                        sede_nombre: srv.name,
                        nro_orig: finalNroOrig,
                        doc_orig: finalDocOrig
                    };
                });
            } catch (e) {
                console.error(`[REPORTES/CXC] Error en sede ${srv.id}:`, e.message);
                return [];
            }
        }));

        // Combinar datos de todas las sedes
        const combined = [].concat(...allData);

        // Calcular métricas agregadas globales
        let totalOutstandingUsd = 0;
        let totalOutstandingBs = 0;
        let totalOverdueUsd = 0;
        let totalOverdueBs = 0;
        let totalUpcomingUsd = 0;
        let totalUpcomingBs = 0;

        combined.forEach(doc => {
            totalOutstandingUsd += doc.saldo_usd;
            totalOutstandingBs += doc.saldo_bs;
            if (doc.vencido) {
                totalOverdueUsd += doc.saldo_usd;
                totalOverdueBs += doc.saldo_bs;
            } else {
                totalUpcomingUsd += doc.saldo_usd;
                totalUpcomingBs += doc.saldo_bs;
            }
        });

        // Ordenar combinados por fecha de emisión descendente
        combined.sort((a, b) => new Date(b.fec_emis) - new Date(a.fec_emis));

        // Paginación manual
        const totalItems = combined.length;
        const offset = (page - 1) * limit;
        const paginatedData = combined.slice(offset, offset + limit);

        return res.status(200).json({
            success: true,
            metrics: {
                total_outstanding_usd: parseFloat(totalOutstandingUsd.toFixed(2)),
                total_outstanding_bs: parseFloat(totalOutstandingBs.toFixed(2)),
                total_overdue_usd: parseFloat(totalOverdueUsd.toFixed(2)),
                total_overdue_bs: parseFloat(totalOverdueBs.toFixed(2)),
                total_upcoming_usd: parseFloat(totalUpcomingUsd.toFixed(2)),
                total_upcoming_bs: parseFloat(totalUpcomingBs.toFixed(2)),
                doc_count: totalItems
            },
            data: paginatedData,
            page,
            limit,
            total_items: totalItems,
            total_pages: Math.ceil(totalItems / limit)
        });

    } catch (error) {
        console.error('[REPORTES/CXC GLOBAL ERROR]:', error.message);
        res.status(500).json({ success: false, message: 'Error general al generar reporte CxC.', error: error.message });
    }
});

/**
 * @swagger
 * /api/v1/reportes/cxp:
 *   get:
 *     summary: Obtiene cuentas por pagar pendientes (CxP) con saldos y métricas asociadas
 *     tags: [Reportes]
 *     parameters:
 *       - in: query
 *         name: page
 *         schema: { type: integer, default: 1 }
 *       - in: query
 *         name: limit
 *         schema: { type: integer, default: 10 }
 *       - in: query
 *         name: search
 *         schema: { type: string }
 *       - in: query
 *         name: tipo_doc
 *         schema: { type: string }
 *       - in: query
 *         name: status
 *         schema: { type: string, enum: [all, vencidos, por_vencer] }
 *       - in: query
 *         name: sede
 *         schema: { type: string }
 *     responses:
 *       200:
 *         description: Listado paginado de CxP con métricas
 */
router.get('/cxp', async (req, res) => {
    console.log('============= [REPORTES/CXP HIT] =============');
    console.log('[REPORTES/CXP] QUERY COMPLETO:', JSON.stringify(req.query));
    try {
        const page = parseInt(req.query.page) || 1;
        const limit = parseInt(req.query.limit) || 10;
        const search = (req.query.search || "").trim();
        const tipo_doc = (req.query.tipo_doc || "").trim();
        const status = req.query.status || "all"; // all, vencidos, por_vencer
        const sede = req.query.sede || "";

        const servers = getServers();
        const targets = sede ? servers.filter(s => s.id === sede) : servers;

        if (targets.length === 0) {
            return res.status(200).json({
                success: true,
                metrics: {
                    total_outstanding_usd: 0,
                    total_outstanding_bs: 0,
                    total_overdue_usd: 0,
                    total_overdue_bs: 0,
                    total_upcoming_usd: 0,
                    total_upcoming_bs: 0,
                    doc_count: 0
                },
                data: [],
                page,
                limit,
                total_items: 0,
                total_pages: 0
            });
        }

        // Obtener tasa de cambio actual para conversiones
        let currentRate = 50.0;
        try {
            const firstPool = await getPool(targets[0].id, req.sqlAuth);
            const rateValue = await getExchangeRate(firstPool);
            if (rateValue && typeof rateValue === 'number' && rateValue > 0) {
                currentRate = rateValue;
            }
            console.log('[REPORTES/CXP] Tasa actual obtenida:', currentRate);
        } catch (e) {
            console.warn('[REPORTES/CXP] No se pudo obtener tasa oficial, usando fallback:', currentRate);
        }

        const allData = await Promise.all(targets.map(async (srv) => {
            try {
                const pool = await getPool(srv.id, req.sqlAuth);
                const r = pool.request();
                
                let whereClauses = ["d.saldo <> 0 AND d.anulado = 0"];

                if (search !== "") {
                    r.input('search', sql.VarChar, `%${search}%`);
                    whereClauses.push("(d.nro_doc LIKE @search OR d.co_prov LIKE @search OR p.prov_des LIKE @search)");
                }

                if (tipo_doc !== "" && tipo_doc !== "all") {
                    r.input('tipo_doc', sql.VarChar, tipo_doc.toUpperCase());
                    whereClauses.push("LTRIM(RTRIM(d.co_tipo_doc)) = @tipo_doc");
                }

                if (status === "vencidos") {
                    whereClauses.push("CAST(d.fec_venc AS DATE) < CAST(GETDATE() AS DATE)");
                } else if (status === "por_vencer") {
                    whereClauses.push("CAST(d.fec_venc AS DATE) >= CAST(GETDATE() AS DATE)");
                }

                const whereSQL = whereClauses.join(" AND ");
                const querySQL = `
                    SELECT 
                        RTRIM(d.nro_doc) AS nro_doc, 
                        RTRIM(d.co_tipo_doc) AS co_tipo_doc, 
                        RTRIM(d.co_prov) AS co_prov, 
                        RTRIM(p.prov_des) AS prov_des,
                        d.fec_emis, 
                        d.fec_venc, 
                        d.total_neto, 
                        d.saldo, 
                        d.anulado, 
                        RTRIM(d.co_mone) AS co_mone, 
                        d.tasa AS doc_tasa,
                        RTRIM(d.nro_orig) AS nro_orig,
                        RTRIM(d.doc_orig) AS doc_orig,
                        (
                            SELECT TOP 1 t.tasa_v
                            FROM saTasa t
                            WHERE LTRIM(RTRIM(t.co_mone)) IN ('USD', 'US$', 'US')
                              AND t.fecha <= d.fec_emis
                            ORDER BY t.fecha DESC
                        ) AS tasa_bcv_fecha
                    FROM saDocumentoCompra d
                    LEFT JOIN saProveedor p ON d.co_prov = p.co_prov
                    WHERE ${whereSQL}
                    ORDER BY d.fec_emis DESC
                `;

                const resData = await r.query(querySQL);
                return resData.recordset.map(row => {
                    const rowMone = (row.co_mone || "").trim().toUpperCase();
                    const rowTasa = parseFloat(row.tasa_bcv_fecha) || parseFloat(row.doc_tasa) || currentRate || 1.0;
                    const saldo = parseFloat(row.saldo) || 0.0;
                    const total = parseFloat(row.total_neto) || 0.0;

                    const docType = (row.co_tipo_doc || "").trim().toUpperCase();
                    // FACT, AJPA (and NDEB/N/DB) represent liabilities to pay (negative cash/outstanding)
                    // N/CR, NCR, AJNM (and ADEL/ISLR/IVAN) represent credits in our favor (positive)
                    const isNegative = ['FACT', 'AJPA', 'NDEB', 'N/DB', 'IVAP'].includes(docType);

                    const saldoBs = isNegative ? -saldo : saldo;
                    const saldoUsd = isNegative ? -saldo / (rowTasa > 0 ? rowTasa : 1.0) : saldo / (rowTasa > 0 ? rowTasa : 1.0);
                    const totalBs = isNegative ? -total : total;
                    const totalUsd = isNegative ? -total / (rowTasa > 0 ? rowTasa : 1.0) : total / (rowTasa > 0 ? rowTasa : 1.0);

                    const today = new Date();
                    const fecVenc = new Date(row.fec_venc);
                    const diffTime = today.getTime() - fecVenc.getTime();
                    const diffDays = Math.ceil(diffTime / (1000 * 60 * 60 * 24));
                    const isVencido = isNegative && diffDays > 0;

                    return {
                        nro_doc: row.nro_doc,
                        co_tipo_doc: row.co_tipo_doc,
                        co_prov: row.co_prov,
                        prov_des: row.prov_des || 'Proveedor Desconocido',
                        fec_emis: row.fec_emis,
                        fec_venc: row.fec_venc,
                        co_mone: rowMone,
                        tasa: rowTasa,
                        total_original: isNegative ? -total : total,
                        saldo_original: isNegative ? -saldo : saldo,
                        total_usd: parseFloat(totalUsd.toFixed(2)),
                        total_bs: parseFloat(totalBs.toFixed(2)),
                        saldo_usd: parseFloat(saldoUsd.toFixed(2)),
                        saldo_bs: parseFloat(saldoBs.toFixed(2)),
                        dias_vencidos: isVencido ? diffDays : 0,
                        vencido: isVencido,
                        sede_id: srv.id,
                        sede_nombre: srv.name,
                        nro_orig: (row.nro_orig || "").trim(),
                        doc_orig: (row.doc_orig || "").trim()
                    };
                });
            } catch (e) {
                console.error(`[REPORTES/CXP] Error en sede ${srv.id}:`, e.message);
                return [];
            }
        }));

        const combined = [].concat(...allData);

        let totalOutstandingUsd = 0;
        let totalOutstandingBs = 0;
        let totalOverdueUsd = 0;
        let totalOverdueBs = 0;
        let totalUpcomingUsd = 0;
        let totalUpcomingBs = 0;

        combined.forEach(doc => {
            totalOutstandingUsd += doc.saldo_usd;
            totalOutstandingBs += doc.saldo_bs;
            if (doc.vencido) {
                totalOverdueUsd += doc.saldo_usd;
                totalOverdueBs += doc.saldo_bs;
            } else {
                totalUpcomingUsd += doc.saldo_usd;
                totalUpcomingBs += doc.saldo_bs;
            }
        });

        combined.sort((a, b) => new Date(b.fec_emis) - new Date(a.fec_emis));

        const totalItems = combined.length;
        const offset = (page - 1) * limit;
        const paginatedData = combined.slice(offset, offset + limit);

        return res.status(200).json({
            success: true,
            metrics: {
                total_outstanding_usd: parseFloat(totalOutstandingUsd.toFixed(2)),
                total_outstanding_bs: parseFloat(totalOutstandingBs.toFixed(2)),
                total_overdue_usd: parseFloat(totalOverdueUsd.toFixed(2)),
                total_overdue_bs: parseFloat(totalOverdueBs.toFixed(2)),
                total_upcoming_usd: parseFloat(totalUpcomingUsd.toFixed(2)),
                total_upcoming_bs: parseFloat(totalUpcomingBs.toFixed(2)),
                doc_count: totalItems
            },
            data: paginatedData,
            page,
            limit,
            total_items: totalItems,
            total_pages: Math.ceil(totalItems / limit)
        });

    } catch (error) {
        console.error('[REPORTES/CXP GLOBAL ERROR]:', error.message);
        res.status(500).json({ success: false, message: 'Error general al generar reporte CxP.', error: error.message });
    }
});

router.get('/cuenta-detallada', async (req, res) => {
    console.log('============= [REPORTES/CUENTA-DETALLADA HIT] =============');
    console.log('[REPORTES/CUENTA-DETALLADA] QUERY:', JSON.stringify(req.query));
    try {
        const page = parseInt(req.query.page) || 1;
        const limit = parseInt(req.query.limit) || 10;
        const search = (req.query.search || "").trim();
        const co_ven = (req.query.co_ven || "").trim();
        const status = req.query.status || "all";
        const sede = req.query.sede || "";

        const servers = getServers();
        const targets = sede ? servers.filter(s => s.id === sede) : servers;

        if (targets.length === 0) {
            return res.status(200).json({
                success: true,
                metrics: {
                    total_outstanding_usd: 0,
                    total_outstanding_bs: 0,
                    doc_count: 0
                },
                data: [],
                page,
                limit,
                total_items: 0,
                total_pages: 0
            });
        }

        let currentRate = 50.0;
        try {
            const firstPool = await getPool(targets[0].id, req.sqlAuth);
            const rateValue = await getExchangeRate(firstPool);
            if (rateValue && typeof rateValue === 'number' && rateValue > 0) {
                currentRate = rateValue;
            }
        } catch (e) {
            console.warn('[REPORTES/CUENTA-DETALLADA] Error getting exchange rate:', e.message);
        }

        const positiveTypes = ['FACT', 'N/DB', 'NDEB', 'IVAP', 'AJPA'];
        const negativeTypes = ['N/CR', 'NCR', 'AJNA', 'AJNM', 'IVAN', 'ISLR', 'ADEL'];

        const allData = await Promise.all(targets.map(async (srv) => {
            try {
                const pool = await getPool(srv.id, req.sqlAuth);
                const r = pool.request();
                
                let whereClauses = [];

                if (search !== "") {
                    r.input('search', sql.VarChar, `%${search}%`);
                    whereClauses.push("(d.nro_doc LIKE @search OR d.co_cli LIKE @search OR c.cli_des LIKE @search)");
                }

                if (co_ven !== "") {
                    r.input('co_ven', sql.VarChar, co_ven.toUpperCase());
                    whereClauses.push("LTRIM(RTRIM(d.co_ven)) = @co_ven");
                }

                if (status === "vencidos") {
                    whereClauses.push("CAST(d.fec_venc AS DATE) < CAST(GETDATE() AS DATE)");
                } else if (status === "por_vencer") {
                    whereClauses.push("CAST(d.fec_venc AS DATE) >= CAST(GETDATE() AS DATE)");
                }

                const whereSQL = whereClauses.length > 0 ? "WHERE " + whereClauses.join(" AND ") : "";
                const querySQL = `
                    SELECT 
                        RTRIM(d.nro_doc) AS nro_doc, 
                        RTRIM(d.co_tipo_doc) AS co_tipo_doc, 
                        RTRIM(d.co_cli) AS co_cli, 
                        RTRIM(c.cli_des) AS cli_des,
                        d.fec_emis, 
                        d.fec_venc, 
                        d.total_neto, 
                        d.saldo, 
                        d.anulado, 
                        RTRIM(d.co_ven) AS co_ven, 
                        RTRIM(d.co_mone) AS co_mone, 
                        d.tasa AS doc_tasa,
                        RTRIM(d.nro_orig) AS nro_orig,
                        RTRIM(d.doc_orig) AS doc_orig,
                        (
                            SELECT TOP 1 RTRIM(r.tipo_doc)
                            FROM saDevolucionClienteReng r
                            WHERE r.doc_num = d.nro_orig
                        ) AS devol_tipo_doc,
                        (
                            SELECT TOP 1 RTRIM(r.num_doc)
                            FROM saDevolucionClienteReng r
                            WHERE r.doc_num = d.nro_orig
                        ) AS devol_num_doc,
                        (
                            SELECT TOP 1 t.tasa_v
                            FROM saTasa t
                            WHERE LTRIM(RTRIM(t.co_mone)) IN ('USD', 'US$', 'US')
                              AND t.fecha <= d.fec_emis
                            ORDER BY t.fecha DESC
                        ) AS tasa_bcv_fecha
                    FROM saDocumentoVenta d
                    LEFT JOIN saCliente c ON d.co_cli = c.co_cli
                    ${whereSQL}
                    ORDER BY d.fec_emis DESC
                `;

                const resData = await r.query(querySQL);
                return resData.recordset.map(row => {
                    let rowTasa = parseFloat(row.doc_tasa) || 1.0;
                    if (rowTasa === 1.0) {
                        rowTasa = parseFloat(row.tasa_bcv_fecha) || currentRate || 1.0;
                    }
                    const saldo = parseFloat(row.saldo) || 0.0;
                    const total = parseFloat(row.total_neto) || 0.0;

                    const docType = (row.co_tipo_doc || "").trim().toUpperCase();
                    const isNegative = negativeTypes.includes(docType);

                    const saldoBs = isNegative ? -saldo : saldo;
                    const saldoUsd = isNegative ? -saldo / rowTasa : saldo / rowTasa;
                    const totalBs = isNegative ? -total : total;
                    const totalUsd = isNegative ? -total / rowTasa : total / rowTasa;

                    const today = new Date();
                    const fecVenc = new Date(row.fec_venc);
                    const diffDays = Math.ceil((today.getTime() - fecVenc.getTime()) / (1000 * 60 * 60 * 24));
                    const isVencido = !isNegative && saldo > 0 && diffDays > 0;

                    const docOrig = (row.doc_orig || "").trim().toUpperCase();
                    const nroOrig = (row.nro_orig || "").trim();
                    const devolTipoDoc = (row.devol_tipo_doc || "").trim();
                    const devolNumDoc = (row.devol_num_doc || "").trim();

                    const finalDocOrig = docOrig === 'DEVO' && devolTipoDoc ? devolTipoDoc : docOrig;
                    const finalNroOrig = docOrig === 'DEVO' && devolNumDoc ? devolNumDoc : nroOrig;

                    return {
                        nro_doc: row.nro_doc,
                        co_tipo_doc: row.co_tipo_doc,
                        co_cli: row.co_cli,
                        cli_des: row.cli_des || 'Cliente Desconocido',
                        fec_emis: row.fec_emis,
                        fec_venc: row.fec_venc,
                        co_mone: (row.co_mone || "").trim().toUpperCase(),
                        tasa: rowTasa,
                        total_original: isNegative ? -total : total,
                        saldo_original: isNegative ? -saldo : saldo,
                        total_usd: parseFloat(totalUsd.toFixed(2)),
                        total_bs: parseFloat(totalBs.toFixed(2)),
                        saldo_usd: parseFloat(saldoUsd.toFixed(2)),
                        saldo_bs: parseFloat(saldoBs.toFixed(2)),
                        co_ven: row.co_ven,
                        dias_vencidos: isVencido ? diffDays : 0,
                        vencido: isVencido,
                        anulado: row.anulado === 1 || row.anulado === true,
                        sede_id: srv.id,
                        sede_nombre: srv.name,
                        nro_orig: finalNroOrig ? finalNroOrig : null,
                        doc_orig: finalDocOrig ? finalDocOrig : null
                    };
                });
            } catch (e) {
                console.error(`[REPORTES/CUENTA-DETALLADA] Error en sede ${srv.id}:`, e.message);
                return [];
            }
        }));

        const combined = [].concat(...allData);

        let totalOutstandingUsd = 0;
        let totalOutstandingBs = 0;
        combined.forEach(doc => {
            totalOutstandingUsd += doc.total_usd;
            totalOutstandingBs += doc.total_bs;
        });

        combined.sort((a, b) => new Date(b.fec_emis) - new Date(a.fec_emis));

        const totalItems = combined.length;

        return res.status(200).json({
            success: true,
            metrics: {
                total_outstanding_usd: parseFloat(totalOutstandingUsd.toFixed(2)),
                total_outstanding_bs: parseFloat(totalOutstandingBs.toFixed(2)),
                doc_count: totalItems
            },
            data: combined,
            total_items: totalItems
        });

    } catch (error) {
        console.error('[REPORTES/CUENTA-DETALLADA GLOBAL ERROR]:', error.message);
        res.status(500).json({ success: false, message: 'Error general al generar reporte de cuenta detallada.', error: error.message });
    }
});

module.exports = router;
