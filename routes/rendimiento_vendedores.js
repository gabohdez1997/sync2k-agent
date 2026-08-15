const express = require('express');
const router = express.Router();
const { getPool, getServers } = require('../db');

/**
 * @swagger
 * /api/v1/rendimiento-vendedores:
 *   get:
 *     summary: Obtiene métricas y conteo de documentos de venta por vendedor agrupado dinámicamente según rango (diario, semanal, mensual)
 *     tags: [Reportes]
 *     parameters:
 *       - in: query
 *         name: sede
 *         schema: { type: string }
 *       - in: query
 *         name: startDate
 *         schema: { type: string }
 *       - in: query
 *         name: endDate
 *         schema: { type: string }
 *       - in: query
 *         name: co_ven
 *         description: Código del vendedor para filtrar en el gráfico principal (opcional)
 *         schema: { type: string }
 */
router.get('/', async (req, res) => {
    try {
        let sede = req.query.sede || 'default';
        const startDate = req.query.startDate || new Date(new Date().setDate(new Date().getDate() - 30)).toISOString().split('T')[0];
        const endDate = req.query.endDate || new Date().toISOString().split('T')[0];
        const coVen = (req.query.co_ven || '').trim();

        const servers = getServers();
        if (sede === 'default') {
            if (servers && servers.length > 0) {
                sede = servers[0].id;
            } else {
                return res.status(500).json({ success: false, message: 'No hay servidores SQL configurados.' });
            }
        }

        const pool = await getPool(sede, req.sqlAuth);

        // Extraer año, mes y día para calcular la granularidad
        const sParts = startDate.split('-').map(Number);
        const eParts = endDate.split('-').map(Number);
        const sYear = sParts[0] || new Date().getFullYear();
        const sMonth = sParts[1] || (new Date().getMonth() + 1);
        const sDay = sParts[2] || 1;
        const eYear = eParts[0] || new Date().getFullYear();
        const eMonth = eParts[1] || (new Date().getMonth() + 1);
        const eDay = eParts[2] || 1;

        // Días que tiene el mes de startDate
        const daysInStartMonth = new Date(sYear, sMonth, 0).getDate();

        // Diferencia en días del rango (inclusivo)
        const sDate = new Date(Date.UTC(sYear, sMonth - 1, sDay));
        const eDate = new Date(Date.UTC(eYear, eMonth - 1, eDay));
        const diffDays = Math.max(1, Math.round((eDate.getTime() - sDate.getTime()) / (1000 * 60 * 60 * 24)) + 1);

        // Granularidad según regla:
        // 1. Rango <= días del mes de startDate -> diario (excluyendo días con 0 documentos)
        // 2. Rango > días del mes de startDate y <= 90 días -> semanal
        // 3. Rango > 90 días -> mensual
        let tipoAgrupacion = 'mensual';
        if (diffDays <= daysInStartMonth) {
            tipoAgrupacion = 'diario';
        } else if (diffDays <= 90) {
            tipoAgrupacion = 'semanal';
        } else {
            tipoAgrupacion = 'mensual';
        }

        let query = '';
        if (tipoAgrupacion === 'diario') {
            query = `
                ;WITH Fletes AS (
                    SELECT 
                        CAST(f.fec_emis AS DATE) AS fecha,
                        LTRIM(RTRIM(f.co_ven)) AS co_ven
                    FROM saFacturaVentaReng r
                    JOIN saFacturaVenta f ON r.doc_num = f.doc_num
                    WHERE f.anulado = 0
                      AND f.fec_emis >= @start AND f.fec_emis <= @end
                      AND (r.co_art LIKE '901001%' OR r.co_art LIKE '0901001%' OR r.co_art LIKE '%901001%')
                      AND NOT EXISTS (
                          SELECT 1 
                          FROM saDevolucionClienteReng dr
                          JOIN saDevolucionCliente d ON dr.doc_num = d.doc_num
                          WHERE d.anulado = 0 
                            AND LTRIM(RTRIM(dr.tipo_doc)) = 'FACT' 
                            AND LTRIM(RTRIM(dr.num_doc)) = LTRIM(RTRIM(f.doc_num))
                      )
                ),
                Cortes AS (
                    SELECT 
                        CAST(f.fec_emis AS DATE) AS fecha,
                        LTRIM(RTRIM(f.co_ven)) AS co_ven
                    FROM saFacturaVentaReng r
                    JOIN saFacturaVenta f ON r.doc_num = f.doc_num
                    WHERE f.anulado = 0
                      AND f.fec_emis >= @start AND f.fec_emis <= @end
                      AND (
                          r.co_art LIKE '902001%' OR r.co_art LIKE '0902001%' OR r.co_art LIKE '%902001%'
                          OR r.co_art LIKE '902002%' OR r.co_art LIKE '0902002%' OR r.co_art LIKE '%902002%'
                      )
                      AND NOT EXISTS (
                          SELECT 1 
                          FROM saDevolucionClienteReng dr
                          JOIN saDevolucionCliente d ON dr.doc_num = d.doc_num
                          WHERE d.anulado = 0 
                            AND LTRIM(RTRIM(dr.tipo_doc)) = 'FACT' 
                            AND LTRIM(RTRIM(dr.num_doc)) = LTRIM(RTRIM(f.doc_num))
                      )
                ),
                Documentos AS (
                    SELECT CAST(fec_emis AS DATE) AS fecha, LTRIM(RTRIM(co_ven)) AS co_ven, 'factura' AS tipo
                    FROM saFacturaVenta
                    WHERE anulado = 0 AND fec_emis >= @start AND fec_emis <= @end
                    UNION ALL
                    SELECT CAST(fec_emis AS DATE) AS fecha, LTRIM(RTRIM(co_ven)) AS co_ven, 'devolucion' AS tipo
                    FROM saDevolucionCliente
                    WHERE anulado = 0 AND fec_emis >= @start AND fec_emis <= @end
                    UNION ALL
                    SELECT CAST(fec_emis AS DATE) AS fecha, LTRIM(RTRIM(co_ven)) AS co_ven, 'cotizacion' AS tipo
                    FROM saCotizacionCliente
                    WHERE anulado = 0 AND fec_emis >= @start AND fec_emis <= @end
                    UNION ALL
                    SELECT CAST(fec_emis AS DATE) AS fecha, LTRIM(RTRIM(co_ven)) AS co_ven, 'pedido' AS tipo
                    FROM saPedidoVenta
                    WHERE anulado = 0 AND fec_emis >= @start AND fec_emis <= @end
                    UNION ALL
                    SELECT fecha, co_ven, 'flete' AS tipo
                    FROM Fletes
                    UNION ALL
                    SELECT fecha, co_ven, 'corte' AS tipo
                    FROM Cortes
                )
                SELECT 
                    CONVERT(VARCHAR(10), fecha, 120) AS fecha_str,
                    DAY(fecha) AS dia,
                    MONTH(fecha) AS mes,
                    YEAR(fecha) AS anio,
                    co_ven,
                    SUM(CASE WHEN tipo = 'factura' THEN 1 ELSE 0 END) AS facturas,
                    SUM(CASE WHEN tipo = 'devolucion' THEN 1 ELSE 0 END) AS devoluciones,
                    (SUM(CASE WHEN tipo = 'factura' THEN 1 ELSE 0 END) - SUM(CASE WHEN tipo = 'devolucion' THEN 1 ELSE 0 END)) AS docs_exitosos,
                    SUM(CASE WHEN tipo = 'cotizacion' THEN 1 ELSE 0 END) AS cotizaciones,
                    SUM(CASE WHEN tipo = 'pedido' THEN 1 ELSE 0 END) AS pedidos,
                    SUM(CASE WHEN tipo = 'flete' THEN 1 ELSE 0 END) AS fletes,
                    SUM(CASE WHEN tipo = 'corte' THEN 1 ELSE 0 END) AS cortes
                FROM Documentos
                GROUP BY fecha, co_ven
                HAVING COUNT(*) > 0
                ORDER BY fecha ASC
            `;
        } else if (tipoAgrupacion === 'semanal') {
            query = `
                ;WITH Fletes AS (
                    SELECT 
                        CAST(f.fec_emis AS DATE) AS fecha,
                        LTRIM(RTRIM(f.co_ven)) AS co_ven
                    FROM saFacturaVentaReng r
                    JOIN saFacturaVenta f ON r.doc_num = f.doc_num
                    WHERE f.anulado = 0
                      AND f.fec_emis >= @start AND f.fec_emis <= @end
                      AND (r.co_art LIKE '901001%' OR r.co_art LIKE '0901001%' OR r.co_art LIKE '%901001%')
                      AND NOT EXISTS (
                          SELECT 1 
                          FROM saDevolucionClienteReng dr
                          JOIN saDevolucionCliente d ON dr.doc_num = d.doc_num
                          WHERE d.anulado = 0 
                            AND LTRIM(RTRIM(dr.tipo_doc)) = 'FACT' 
                            AND LTRIM(RTRIM(dr.num_doc)) = LTRIM(RTRIM(f.doc_num))
                      )
                ),
                Cortes AS (
                    SELECT 
                        CAST(f.fec_emis AS DATE) AS fecha,
                        LTRIM(RTRIM(f.co_ven)) AS co_ven
                    FROM saFacturaVentaReng r
                    JOIN saFacturaVenta f ON r.doc_num = f.doc_num
                    WHERE f.anulado = 0
                      AND f.fec_emis >= @start AND f.fec_emis <= @end
                      AND (
                          r.co_art LIKE '902001%' OR r.co_art LIKE '0902001%' OR r.co_art LIKE '%902001%'
                          OR r.co_art LIKE '902002%' OR r.co_art LIKE '0902002%' OR r.co_art LIKE '%902002%'
                      )
                      AND NOT EXISTS (
                          SELECT 1 
                          FROM saDevolucionClienteReng dr
                          JOIN saDevolucionCliente d ON dr.doc_num = d.doc_num
                          WHERE d.anulado = 0 
                            AND LTRIM(RTRIM(dr.tipo_doc)) = 'FACT' 
                            AND LTRIM(RTRIM(dr.num_doc)) = LTRIM(RTRIM(f.doc_num))
                      )
                ),
                Documentos AS (
                    SELECT CAST(fec_emis AS DATE) AS fecha, LTRIM(RTRIM(co_ven)) AS co_ven, 'factura' AS tipo
                    FROM saFacturaVenta
                    WHERE anulado = 0 AND fec_emis >= @start AND fec_emis <= @end
                    UNION ALL
                    SELECT CAST(fec_emis AS DATE) AS fecha, LTRIM(RTRIM(co_ven)) AS co_ven, 'devolucion' AS tipo
                    FROM saDevolucionCliente
                    WHERE anulado = 0 AND fec_emis >= @start AND fec_emis <= @end
                    UNION ALL
                    SELECT CAST(fec_emis AS DATE) AS fecha, LTRIM(RTRIM(co_ven)) AS co_ven, 'cotizacion' AS tipo
                    FROM saCotizacionCliente
                    WHERE anulado = 0 AND fec_emis >= @start AND fec_emis <= @end
                    UNION ALL
                    SELECT CAST(fec_emis AS DATE) AS fecha, LTRIM(RTRIM(co_ven)) AS co_ven, 'pedido' AS tipo
                    FROM saPedidoVenta
                    WHERE anulado = 0 AND fec_emis >= @start AND fec_emis <= @end
                    UNION ALL
                    SELECT fecha, co_ven, 'flete' AS tipo
                    FROM Fletes
                    UNION ALL
                    SELECT fecha, co_ven, 'corte' AS tipo
                    FROM Cortes
                ),
                DocSemana AS (
                    SELECT 
                        DATEADD(day, - ((DATEPART(weekday, fecha) + @@DATEFIRST - 2) % 7), fecha) AS semana_inicio,
                        co_ven,
                        tipo
                    FROM Documentos
                )
                SELECT 
                    CONVERT(VARCHAR(10), semana_inicio, 120) AS semana_inicio,
                    CONVERT(VARCHAR(10), DATEADD(day, 6, semana_inicio), 120) AS semana_fin,
                    DAY(semana_inicio) AS dia_inicio,
                    MONTH(semana_inicio) AS mes_inicio,
                    YEAR(semana_inicio) AS anio_inicio,
                    DAY(DATEADD(day, 6, semana_inicio)) AS dia_fin,
                    MONTH(DATEADD(day, 6, semana_inicio)) AS mes_fin,
                    YEAR(DATEADD(day, 6, semana_inicio)) AS anio_fin,
                    co_ven,
                    SUM(CASE WHEN tipo = 'factura' THEN 1 ELSE 0 END) AS facturas,
                    SUM(CASE WHEN tipo = 'devolucion' THEN 1 ELSE 0 END) AS devoluciones,
                    (SUM(CASE WHEN tipo = 'factura' THEN 1 ELSE 0 END) - SUM(CASE WHEN tipo = 'devolucion' THEN 1 ELSE 0 END)) AS docs_exitosos,
                    SUM(CASE WHEN tipo = 'cotizacion' THEN 1 ELSE 0 END) AS cotizaciones,
                    SUM(CASE WHEN tipo = 'pedido' THEN 1 ELSE 0 END) AS pedidos,
                    SUM(CASE WHEN tipo = 'flete' THEN 1 ELSE 0 END) AS fletes,
                    SUM(CASE WHEN tipo = 'corte' THEN 1 ELSE 0 END) AS cortes
                FROM DocSemana
                GROUP BY semana_inicio, co_ven
                ORDER BY semana_inicio ASC
            `;
        } else {
            // Mensual
            query = `
                ;WITH Fletes AS (
                    SELECT 
                        CAST(f.fec_emis AS DATE) AS fecha,
                        LTRIM(RTRIM(f.co_ven)) AS co_ven
                    FROM saFacturaVentaReng r
                    JOIN saFacturaVenta f ON r.doc_num = f.doc_num
                    WHERE f.anulado = 0
                      AND f.fec_emis >= @start AND f.fec_emis <= @end
                      AND (r.co_art LIKE '901001%' OR r.co_art LIKE '0901001%' OR r.co_art LIKE '%901001%')
                      AND NOT EXISTS (
                          SELECT 1 
                          FROM saDevolucionClienteReng dr
                          JOIN saDevolucionCliente d ON dr.doc_num = d.doc_num
                          WHERE d.anulado = 0 
                            AND LTRIM(RTRIM(dr.tipo_doc)) = 'FACT' 
                            AND LTRIM(RTRIM(dr.num_doc)) = LTRIM(RTRIM(f.doc_num))
                      )
                ),
                Cortes AS (
                    SELECT 
                        CAST(f.fec_emis AS DATE) AS fecha,
                        LTRIM(RTRIM(f.co_ven)) AS co_ven
                    FROM saFacturaVentaReng r
                    JOIN saFacturaVenta f ON r.doc_num = f.doc_num
                    WHERE f.anulado = 0
                      AND f.fec_emis >= @start AND f.fec_emis <= @end
                      AND (
                          r.co_art LIKE '902001%' OR r.co_art LIKE '0902001%' OR r.co_art LIKE '%902001%'
                          OR r.co_art LIKE '902002%' OR r.co_art LIKE '0902002%' OR r.co_art LIKE '%902002%'
                      )
                      AND NOT EXISTS (
                          SELECT 1 
                          FROM saDevolucionClienteReng dr
                          JOIN saDevolucionCliente d ON dr.doc_num = d.doc_num
                          WHERE d.anulado = 0 
                            AND LTRIM(RTRIM(dr.tipo_doc)) = 'FACT' 
                            AND LTRIM(RTRIM(dr.num_doc)) = LTRIM(RTRIM(f.doc_num))
                      )
                ),
                Documentos AS (
                    SELECT CAST(fec_emis AS DATE) AS fecha, LTRIM(RTRIM(co_ven)) AS co_ven, 'factura' AS tipo
                    FROM saFacturaVenta
                    WHERE anulado = 0 AND fec_emis >= @start AND fec_emis <= @end
                    UNION ALL
                    SELECT CAST(fec_emis AS DATE) AS fecha, LTRIM(RTRIM(co_ven)) AS co_ven, 'devolucion' AS tipo
                    FROM saDevolucionCliente
                    WHERE anulado = 0 AND fec_emis >= @start AND fec_emis <= @end
                    UNION ALL
                    SELECT CAST(fec_emis AS DATE) AS fecha, LTRIM(RTRIM(co_ven)) AS co_ven, 'cotizacion' AS tipo
                    FROM saCotizacionCliente
                    WHERE anulado = 0 AND fec_emis >= @start AND fec_emis <= @end
                    UNION ALL
                    SELECT CAST(fec_emis AS DATE) AS fecha, LTRIM(RTRIM(co_ven)) AS co_ven, 'pedido' AS tipo
                    FROM saPedidoVenta
                    WHERE anulado = 0 AND fec_emis >= @start AND fec_emis <= @end
                    UNION ALL
                    SELECT fecha, co_ven, 'flete' AS tipo
                    FROM Fletes
                    UNION ALL
                    SELECT fecha, co_ven, 'corte' AS tipo
                    FROM Cortes
                )
                SELECT 
                    YEAR(fecha) AS anio,
                    MONTH(fecha) AS mes,
                    co_ven,
                    SUM(CASE WHEN tipo = 'factura' THEN 1 ELSE 0 END) AS facturas,
                    SUM(CASE WHEN tipo = 'devolucion' THEN 1 ELSE 0 END) AS devoluciones,
                    (SUM(CASE WHEN tipo = 'factura' THEN 1 ELSE 0 END) - SUM(CASE WHEN tipo = 'devolucion' THEN 1 ELSE 0 END)) AS docs_exitosos,
                    SUM(CASE WHEN tipo = 'cotizacion' THEN 1 ELSE 0 END) AS cotizaciones,
                    SUM(CASE WHEN tipo = 'pedido' THEN 1 ELSE 0 END) AS pedidos,
                    SUM(CASE WHEN tipo = 'flete' THEN 1 ELSE 0 END) AS fletes,
                    SUM(CASE WHEN tipo = 'corte' THEN 1 ELSE 0 END) AS cortes
                FROM Documentos
                GROUP BY YEAR(fecha), MONTH(fecha), co_ven
                ORDER BY anio ASC, mes ASC
            `;
        }

        const request = pool.request();
        request.input('start', startDate);
        request.input('end', endDate + ' 23:59:59');
        const result = await request.query(query);

        const monthNames = ['Ene', 'Feb', 'Mar', 'Abr', 'May', 'Jun', 'Jul', 'Ago', 'Sep', 'Oct', 'Nov', 'Dic'];

        const rawRows = result.recordset.map(row => {
            let periodo = '';
            if (tipoAgrupacion === 'diario') {
                const dia = String(row.dia).padStart(2, '0');
                const mes = monthNames[row.mes - 1];
                periodo = `${dia} ${mes}`;
            } else if (tipoAgrupacion === 'semanal') {
                const dIni = String(row.dia_inicio).padStart(2, '0');
                const dFin = String(row.dia_fin).padStart(2, '0');
                if (row.mes_inicio === row.mes_fin) {
                    periodo = `${dIni} - ${dFin} ${monthNames[row.mes_fin - 1]}`;
                } else {
                    periodo = `${dIni} ${monthNames[row.mes_inicio - 1]} - ${dFin} ${monthNames[row.mes_fin - 1]}`;
                }
            } else {
                periodo = `${monthNames[row.mes - 1]} ${row.anio}`;
            }

            return {
                periodo,
                co_ven: (row.co_ven || '').trim(),
                facturas: Number(row.facturas) || 0,
                devoluciones: Number(row.devoluciones) || 0,
                docs_exitosos: Number(row.docs_exitosos) || 0,
                cotizaciones: Number(row.cotizaciones) || 0,
                pedidos: Number(row.pedidos) || 0,
                fletes: Number(row.fletes) || 0,
                cortes: Number(row.cortes) || 0
            };
        });

        // 1. Obtener lista ordenada de todos los períodos únicos presentes en el rango
        const periodosMap = new Map();
        for (const r of rawRows) {
            if (!periodosMap.has(r.periodo)) {
                periodosMap.set(r.periodo, true);
            }
        }
        const allPeriodLabels = Array.from(periodosMap.keys());

        // 2. Timeline para el gráfico superior principal:
        // Si el usuario seleccionó un coVen en el filtro, sumamos solo ese coVen; si no, sumamos todos
        const mainTimelineMap = new Map();
        for (const p of allPeriodLabels) {
            mainTimelineMap.set(p, {
                periodo: p,
                facturas: 0,
                devoluciones: 0,
                docs_exitosos: 0,
                cotizaciones: 0,
                pedidos: 0,
                fletes: 0,
                cortes: 0
            });
        }

        for (const r of rawRows) {
            if (coVen && r.co_ven !== coVen) continue;
            if (mainTimelineMap.has(r.periodo)) {
                const item = mainTimelineMap.get(r.periodo);
                item.facturas += r.facturas;
                item.devoluciones += r.devoluciones;
                item.docs_exitosos += r.docs_exitosos;
                item.cotizaciones += r.cotizaciones;
                item.pedidos += r.pedidos;
                item.fletes += r.fletes;
                item.cortes += r.cortes;
            }
        }

        // Si se filtró por un vendedor específico y es vista diaria, excluimos días donde ese vendedor tuvo 0 docs
        let timeline = Array.from(mainTimelineMap.values());
        if (coVen && tipoAgrupacion === 'diario') {
            timeline = timeline.filter(t => (t.facturas + t.cotizaciones + t.pedidos + t.devoluciones + t.fletes + t.cortes) > 0);
        }

        // Totales acumulados
        const totales = timeline.reduce((acc, m) => {
            acc.facturas += m.facturas;
            acc.devoluciones += m.devoluciones;
            acc.docs_exitosos += m.docs_exitosos;
            acc.cotizaciones += m.cotizaciones;
            acc.pedidos += m.pedidos;
            acc.fletes += m.fletes;
            acc.cortes += m.cortes;
            return acc;
        }, { facturas: 0, devoluciones: 0, docs_exitosos: 0, cotizaciones: 0, pedidos: 0, fletes: 0, cortes: 0 });

        // Lista de vendedores activos en el período cruzados con MasterProfitPro.dbo.MpUsuario
        let vendedores = [];
        try {
            const vendReq = pool.request();
            vendReq.input('start', startDate);
            vendReq.input('end', endDate + ' 23:59:59');
            const vendResult = await vendReq.query(`
                ;WITH VendedoresDocs AS (
                    SELECT co_ven, doc_num FROM saFacturaVenta WHERE anulado = 0 AND fec_emis >= @start AND fec_emis <= @end
                    UNION ALL
                    SELECT co_ven, doc_num FROM saCotizacionCliente WHERE anulado = 0 AND fec_emis >= @start AND fec_emis <= @end
                    UNION ALL
                    SELECT co_ven, doc_num FROM saPedidoVenta WHERE anulado = 0 AND fec_emis >= @start AND fec_emis <= @end
                    UNION ALL
                    SELECT co_ven, doc_num FROM saDevolucionCliente WHERE anulado = 0 AND fec_emis >= @start AND fec_emis <= @end
                )
                SELECT 
                    RTRIM(f.co_ven) AS co_ven,
                    RTRIM(ISNULL(u.desc_usuario, ISNULL(v.ven_des, f.co_ven))) AS ven_des,
                    COUNT(DISTINCT f.doc_num) AS total_docs
                FROM VendedoresDocs f
                LEFT JOIN MasterProfitPro.dbo.MpUsuario u ON LTRIM(RTRIM(f.co_ven)) = LTRIM(RTRIM(u.cod_usuario))
                LEFT JOIN saVendedor v ON LTRIM(RTRIM(f.co_ven)) = LTRIM(RTRIM(v.co_ven))
                WHERE f.co_ven IS NOT NULL AND RTRIM(f.co_ven) <> ''
                GROUP BY f.co_ven, u.desc_usuario, v.ven_des
                ORDER BY total_docs DESC
            `);
            vendedores = vendResult.recordset.map(row => ({
                co_ven: (row.co_ven || '').trim(),
                ven_des: (row.ven_des || row.co_ven || '').trim(),
                total_docs: Number(row.total_docs) || 0
            }));
        } catch (vendErr) {
            console.warn('[rendimiento-vendedores] Error obteniendo lista de vendedores cruzados:', vendErr.message);
        }

        res.json({
            success: true,
            server: sede,
            startDate,
            endDate,
            co_ven: coVen || null,
            tipoAgrupacion,
            diffDays,
            daysInStartMonth,
            totales,
            timeline,
            mensual: timeline, // alias retrocompatible
            periodosComparativa: allPeriodLabels,
            vendedoresTimeline: rawRows,
            vendedores
        });

    } catch (error) {
        console.error(`[GET /rendimiento-vendedores]`, error);
        res.status(500).json({ success: false, message: 'Error en el reporte de rendimiento de vendedores.', error: error.message });
    }
});

module.exports = router;
