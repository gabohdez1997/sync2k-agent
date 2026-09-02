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
                    UNION ALL
                    SELECT CAST(c.fecha AS DATE) AS fecha, LTRIM(RTRIM(c.co_ven)) AS co_ven, 'cobro' AS tipo
                    FROM saCobro c
                    WHERE c.anulado = 0 AND c.fecha >= @start AND c.fecha <= @end AND c.co_ven IS NOT NULL AND RTRIM(c.co_ven) <> ''
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
                    UNION ALL
                    SELECT CAST(c.fecha AS DATE) AS fecha, LTRIM(RTRIM(c.co_ven)) AS co_ven, 'cobro' AS tipo
                    FROM saCobro c
                    WHERE c.anulado = 0 AND c.fecha >= @start AND c.fecha <= @end AND c.co_ven IS NOT NULL AND RTRIM(c.co_ven) <> ''
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
                    UNION ALL
                    SELECT CAST(c.fecha AS DATE) AS fecha, LTRIM(RTRIM(c.co_ven)) AS co_ven, 'cobro' AS tipo
                    FROM saCobro c
                    WHERE c.anulado = 0 AND c.fecha >= @start AND c.fecha <= @end AND c.co_ven IS NOT NULL AND RTRIM(c.co_ven) <> ''
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

        // Consulta de Artículos Distintos por Período y Vendedor (Facturas exitosas, Pedidos y Cotizaciones)
        let artDistintosQuery = '';
        if (tipoAgrupacion === 'diario') {
            artDistintosQuery = `
                ;WITH ArtVendidos AS (
                    SELECT CAST(f.fec_emis AS DATE) AS fecha, LTRIM(RTRIM(f.co_ven)) AS co_ven, r.co_art
                    FROM saFacturaVentaReng r
                    JOIN saFacturaVenta f ON r.doc_num = f.doc_num
                    WHERE f.anulado = 0 AND f.fec_emis >= @start AND f.fec_emis <= @end
                      ${coVen ? 'AND LTRIM(RTRIM(f.co_ven)) = @co_ven' : ''}
                      AND NOT EXISTS (
                          SELECT 1 FROM saDevolucionClienteReng dr
                          JOIN saDevolucionCliente d ON dr.doc_num = d.doc_num
                          WHERE d.anulado = 0 AND LTRIM(RTRIM(dr.tipo_doc)) = 'FACT' AND LTRIM(RTRIM(dr.num_doc)) = LTRIM(RTRIM(f.doc_num))
                      )
                ),
                ArtPedidos AS (
                    SELECT CAST(f.fec_emis AS DATE) AS fecha, LTRIM(RTRIM(f.co_ven)) AS co_ven, r.co_art
                    FROM saPedidoVentaReng r
                    JOIN saPedidoVenta f ON r.doc_num = f.doc_num
                    WHERE f.anulado = 0 AND f.fec_emis >= @start AND f.fec_emis <= @end
                      ${coVen ? 'AND LTRIM(RTRIM(f.co_ven)) = @co_ven' : ''}
                ),
                ArtCotizados AS (
                    SELECT CAST(f.fec_emis AS DATE) AS fecha, LTRIM(RTRIM(f.co_ven)) AS co_ven, r.co_art
                    FROM saCotizacionClienteReng r
                    JOIN saCotizacionCliente f ON r.doc_num = f.doc_num
                    WHERE f.anulado = 0 AND f.fec_emis >= @start AND f.fec_emis <= @end
                      ${coVen ? 'AND LTRIM(RTRIM(f.co_ven)) = @co_ven' : ''}
                )
                SELECT fecha, DAY(fecha) AS dia, MONTH(fecha) AS mes, YEAR(fecha) AS anio, co_ven, 'vendidos' AS tipo, COUNT(DISTINCT co_art) AS cant_art FROM ArtVendidos GROUP BY fecha, co_ven
                UNION ALL
                SELECT fecha, DAY(fecha) AS dia, MONTH(fecha) AS mes, YEAR(fecha) AS anio, co_ven, 'pedidos' AS tipo, COUNT(DISTINCT co_art) AS cant_art FROM ArtPedidos GROUP BY fecha, co_ven
                UNION ALL
                SELECT fecha, DAY(fecha) AS dia, MONTH(fecha) AS mes, YEAR(fecha) AS anio, co_ven, 'cotizados' AS tipo, COUNT(DISTINCT co_art) AS cant_art FROM ArtCotizados GROUP BY fecha, co_ven
            `;
        } else if (tipoAgrupacion === 'semanal') {
            artDistintosQuery = `
                ;WITH ArtVendidos AS (
                    SELECT CAST(f.fec_emis AS DATE) AS fecha, LTRIM(RTRIM(f.co_ven)) AS co_ven, r.co_art
                    FROM saFacturaVentaReng r
                    JOIN saFacturaVenta f ON r.doc_num = f.doc_num
                    WHERE f.anulado = 0 AND f.fec_emis >= @start AND f.fec_emis <= @end
                      ${coVen ? 'AND LTRIM(RTRIM(f.co_ven)) = @co_ven' : ''}
                      AND NOT EXISTS (
                          SELECT 1 FROM saDevolucionClienteReng dr
                          JOIN saDevolucionCliente d ON dr.doc_num = d.doc_num
                          WHERE d.anulado = 0 AND LTRIM(RTRIM(dr.tipo_doc)) = 'FACT' AND LTRIM(RTRIM(dr.num_doc)) = LTRIM(RTRIM(f.doc_num))
                      )
                ),
                ArtPedidos AS (
                    SELECT CAST(f.fec_emis AS DATE) AS fecha, LTRIM(RTRIM(f.co_ven)) AS co_ven, r.co_art
                    FROM saPedidoVentaReng r
                    JOIN saPedidoVenta f ON r.doc_num = f.doc_num
                    WHERE f.anulado = 0 AND f.fec_emis >= @start AND f.fec_emis <= @end
                      ${coVen ? 'AND LTRIM(RTRIM(f.co_ven)) = @co_ven' : ''}
                ),
                ArtCotizados AS (
                    SELECT CAST(f.fec_emis AS DATE) AS fecha, LTRIM(RTRIM(f.co_ven)) AS co_ven, r.co_art
                    FROM saCotizacionClienteReng r
                    JOIN saCotizacionCliente f ON r.doc_num = f.doc_num
                    WHERE f.anulado = 0 AND f.fec_emis >= @start AND f.fec_emis <= @end
                      ${coVen ? 'AND LTRIM(RTRIM(f.co_ven)) = @co_ven' : ''}
                )
                SELECT semana_inicio, DAY(semana_inicio) AS dia_inicio, MONTH(semana_inicio) AS mes_inicio, YEAR(semana_inicio) AS anio_inicio, DAY(DATEADD(day, 6, semana_inicio)) AS dia_fin, MONTH(DATEADD(day, 6, semana_inicio)) AS mes_fin, YEAR(DATEADD(day, 6, semana_inicio)) AS anio_fin, co_ven, 'vendidos' AS tipo, COUNT(DISTINCT co_art) AS cant_art FROM (SELECT DATEADD(day, - ((DATEPART(weekday, fecha) + @@DATEFIRST - 2) % 7), fecha) AS semana_inicio, co_ven, co_art FROM ArtVendidos) x GROUP BY semana_inicio, co_ven
                UNION ALL
                SELECT semana_inicio, DAY(semana_inicio) AS dia_inicio, MONTH(semana_inicio) AS mes_inicio, YEAR(semana_inicio) AS anio_inicio, DAY(DATEADD(day, 6, semana_inicio)) AS dia_fin, MONTH(DATEADD(day, 6, semana_inicio)) AS mes_fin, YEAR(DATEADD(day, 6, semana_inicio)) AS anio_fin, co_ven, 'pedidos' AS tipo, COUNT(DISTINCT co_art) AS cant_art FROM (SELECT DATEADD(day, - ((DATEPART(weekday, fecha) + @@DATEFIRST - 2) % 7), fecha) AS semana_inicio, co_ven, co_art FROM ArtPedidos) x GROUP BY semana_inicio, co_ven
                UNION ALL
                SELECT semana_inicio, DAY(semana_inicio) AS dia_inicio, MONTH(semana_inicio) AS mes_inicio, YEAR(semana_inicio) AS anio_inicio, DAY(DATEADD(day, 6, semana_inicio)) AS dia_fin, MONTH(DATEADD(day, 6, semana_inicio)) AS mes_fin, YEAR(DATEADD(day, 6, semana_inicio)) AS anio_fin, co_ven, 'cotizados' AS tipo, COUNT(DISTINCT co_art) AS cant_art FROM (SELECT DATEADD(day, - ((DATEPART(weekday, fecha) + @@DATEFIRST - 2) % 7), fecha) AS semana_inicio, co_ven, co_art FROM ArtCotizados) x GROUP BY semana_inicio, co_ven
            `;
        } else {
            // Mensual
            artDistintosQuery = `
                ;WITH ArtVendidos AS (
                    SELECT CAST(f.fec_emis AS DATE) AS fecha, LTRIM(RTRIM(f.co_ven)) AS co_ven, r.co_art
                    FROM saFacturaVentaReng r
                    JOIN saFacturaVenta f ON r.doc_num = f.doc_num
                    WHERE f.anulado = 0 AND f.fec_emis >= @start AND f.fec_emis <= @end
                      ${coVen ? 'AND LTRIM(RTRIM(f.co_ven)) = @co_ven' : ''}
                      AND NOT EXISTS (
                          SELECT 1 FROM saDevolucionClienteReng dr
                          JOIN saDevolucionCliente d ON dr.doc_num = d.doc_num
                          WHERE d.anulado = 0 AND LTRIM(RTRIM(dr.tipo_doc)) = 'FACT' AND LTRIM(RTRIM(dr.num_doc)) = LTRIM(RTRIM(f.doc_num))
                      )
                ),
                ArtPedidos AS (
                    SELECT CAST(f.fec_emis AS DATE) AS fecha, LTRIM(RTRIM(f.co_ven)) AS co_ven, r.co_art
                    FROM saPedidoVentaReng r
                    JOIN saPedidoVenta f ON r.doc_num = f.doc_num
                    WHERE f.anulado = 0 AND f.fec_emis >= @start AND f.fec_emis <= @end
                      ${coVen ? 'AND LTRIM(RTRIM(f.co_ven)) = @co_ven' : ''}
                ),
                ArtCotizados AS (
                    SELECT CAST(f.fec_emis AS DATE) AS fecha, LTRIM(RTRIM(f.co_ven)) AS co_ven, r.co_art
                    FROM saCotizacionClienteReng r
                    JOIN saCotizacionCliente f ON r.doc_num = f.doc_num
                    WHERE f.anulado = 0 AND f.fec_emis >= @start AND f.fec_emis <= @end
                      ${coVen ? 'AND LTRIM(RTRIM(f.co_ven)) = @co_ven' : ''}
                )
                SELECT mes_inicio, YEAR(mes_inicio) AS anio, MONTH(mes_inicio) AS mes, co_ven, 'vendidos' AS tipo, COUNT(DISTINCT co_art) AS cant_art FROM (SELECT DATEFROMPARTS(YEAR(fecha), MONTH(fecha), 1) AS mes_inicio, co_ven, co_art FROM ArtVendidos) x GROUP BY mes_inicio, co_ven
                UNION ALL
                SELECT mes_inicio, YEAR(mes_inicio) AS anio, MONTH(mes_inicio) AS mes, co_ven, 'pedidos' AS tipo, COUNT(DISTINCT co_art) AS cant_art FROM (SELECT DATEFROMPARTS(YEAR(fecha), MONTH(fecha), 1) AS mes_inicio, co_ven, co_art FROM ArtPedidos) x GROUP BY mes_inicio, co_ven
                UNION ALL
                SELECT mes_inicio, YEAR(mes_inicio) AS anio, MONTH(mes_inicio) AS mes, co_ven, 'cotizados' AS tipo, COUNT(DISTINCT co_art) AS cant_art FROM (SELECT DATEFROMPARTS(YEAR(fecha), MONTH(fecha), 1) AS mes_inicio, co_ven, co_art FROM ArtCotizados) x GROUP BY mes_inicio, co_ven
            `;
        }

        const artReq = pool.request();
        artReq.input('start', startDate);
        artReq.input('end', endDate + ' 23:59:59');
        if (coVen) artReq.input('co_ven', coVen);
        const artResult = await artReq.query(artDistintosQuery);

        // Consulta de Cobros en USD y BS por Período y Vendedor
        let cobrosQuery = '';
        if (tipoAgrupacion === 'diario') {
            cobrosQuery = `
                ;WITH CobrosReng AS (
                    SELECT 
                        CAST(c.fecha AS DATE) AS fecha,
                        LTRIM(RTRIM(c.co_ven)) AS co_ven,
                        r.mont_doc,
                        CASE 
                            WHEN c.tasa > 1.000001 THEN c.tasa
                            ELSE ISNULL(
                                (SELECT TOP 1 t.tasa_v 
                                 FROM saTasa t 
                                 WHERE LTRIM(RTRIM(t.co_mone)) IN ('USD', 'US$', 'US', '$') 
                                   AND CONVERT(VARCHAR(10), t.fecha, 120) <= CONVERT(VARCHAR(10), c.fecha, 120) 
                                 ORDER BY t.fecha DESC),
                                (SELECT TOP 1 t.tasa_v 
                                 FROM saTasa t 
                                 WHERE LTRIM(RTRIM(t.co_mone)) IN ('USD', 'US$', 'US', '$') 
                                 ORDER BY t.fecha DESC)
                            )
                        END AS tasa,
                        CASE 
                            WHEN RTRIM(r.cod_caja) = '01' THEN 'USD'
                            WHEN RTRIM(r.cod_caja) = '02' THEN 'BS'
                            WHEN UPPER(RTRIM(ISNULL(r.cod_cta, ''))) IN ('ZELLE', 'USDT') THEN 'USD'
                            ELSE 'BS'
                        END AS moneda
                    FROM saCobroTPReng r
                    JOIN saCobro c ON r.cob_num = c.cob_num
                    WHERE c.anulado = 0 
                      AND c.fecha >= @start AND c.fecha <= @end
                      ${coVen ? 'AND LTRIM(RTRIM(c.co_ven)) = @co_ven' : ''}
                      AND c.co_ven IS NOT NULL AND RTRIM(c.co_ven) <> ''
                )
                SELECT fecha, DAY(fecha) AS dia, MONTH(fecha) AS mes, YEAR(fecha) AS anio, co_ven,
                       SUM(CASE WHEN moneda = 'USD' THEN (mont_doc / NULLIF(tasa, 0)) ELSE 0 END) AS cobros_usd,
                       SUM(CASE WHEN moneda = 'BS' THEN mont_doc ELSE 0 END) AS cobros_bs
                FROM CobrosReng
                GROUP BY fecha, co_ven
            `;
        } else if (tipoAgrupacion === 'semanal') {
            cobrosQuery = `
                ;WITH CobrosReng AS (
                    SELECT 
                        CAST(c.fecha AS DATE) AS fecha,
                        LTRIM(RTRIM(c.co_ven)) AS co_ven,
                        r.mont_doc,
                        CASE 
                            WHEN c.tasa > 1.000001 THEN c.tasa
                            ELSE ISNULL(
                                (SELECT TOP 1 t.tasa_v 
                                 FROM saTasa t 
                                 WHERE LTRIM(RTRIM(t.co_mone)) IN ('USD', 'US$', 'US', '$') 
                                   AND CONVERT(VARCHAR(10), t.fecha, 120) <= CONVERT(VARCHAR(10), c.fecha, 120) 
                                 ORDER BY t.fecha DESC),
                                (SELECT TOP 1 t.tasa_v 
                                 FROM saTasa t 
                                 WHERE LTRIM(RTRIM(t.co_mone)) IN ('USD', 'US$', 'US', '$') 
                                 ORDER BY t.fecha DESC)
                            )
                        END AS tasa,
                        CASE 
                            WHEN RTRIM(r.cod_caja) = '01' THEN 'USD'
                            WHEN RTRIM(r.cod_caja) = '02' THEN 'BS'
                            WHEN UPPER(RTRIM(ISNULL(r.cod_cta, ''))) IN ('ZELLE', 'USDT') THEN 'USD'
                            ELSE 'BS'
                        END AS moneda
                    FROM saCobroTPReng r
                    JOIN saCobro c ON r.cob_num = c.cob_num
                    WHERE c.anulado = 0 
                      AND c.fecha >= @start AND c.fecha <= @end
                      ${coVen ? 'AND LTRIM(RTRIM(c.co_ven)) = @co_ven' : ''}
                      AND c.co_ven IS NOT NULL AND RTRIM(c.co_ven) <> ''
                )
                SELECT semana_inicio, DAY(semana_inicio) AS dia_inicio, MONTH(semana_inicio) AS mes_inicio, YEAR(semana_inicio) AS anio_inicio,
                       DAY(DATEADD(day, 6, semana_inicio)) AS dia_fin, MONTH(DATEADD(day, 6, semana_inicio)) AS mes_fin, YEAR(DATEADD(day, 6, semana_inicio)) AS anio_fin,
                       co_ven,
                       SUM(CASE WHEN moneda = 'USD' THEN (mont_doc / NULLIF(tasa, 0)) ELSE 0 END) AS cobros_usd,
                       SUM(CASE WHEN moneda = 'BS' THEN mont_doc ELSE 0 END) AS cobros_bs
                FROM (
                    SELECT DATEADD(day, - ((DATEPART(weekday, fecha) + @@DATEFIRST - 2) % 7), fecha) AS semana_inicio, co_ven, mont_doc, tasa, moneda
                    FROM CobrosReng
                ) x
                GROUP BY semana_inicio, co_ven
            `;
        } else {
            // Mensual
            cobrosQuery = `
                ;WITH CobrosReng AS (
                    SELECT 
                        CAST(c.fecha AS DATE) AS fecha,
                        LTRIM(RTRIM(c.co_ven)) AS co_ven,
                        r.mont_doc,
                        CASE 
                            WHEN c.tasa > 1.000001 THEN c.tasa
                            ELSE ISNULL(
                                (SELECT TOP 1 t.tasa_v 
                                 FROM saTasa t 
                                 WHERE LTRIM(RTRIM(t.co_mone)) IN ('USD', 'US$', 'US', '$') 
                                   AND CONVERT(VARCHAR(10), t.fecha, 120) <= CONVERT(VARCHAR(10), c.fecha, 120) 
                                 ORDER BY t.fecha DESC),
                                (SELECT TOP 1 t.tasa_v 
                                 FROM saTasa t 
                                 WHERE LTRIM(RTRIM(t.co_mone)) IN ('USD', 'US$', 'US', '$') 
                                 ORDER BY t.fecha DESC)
                            )
                        END AS tasa,
                        CASE 
                            WHEN RTRIM(r.cod_caja) = '01' THEN 'USD'
                            WHEN RTRIM(r.cod_caja) = '02' THEN 'BS'
                            WHEN UPPER(RTRIM(ISNULL(r.cod_cta, ''))) IN ('ZELLE', 'USDT') THEN 'USD'
                            ELSE 'BS'
                        END AS moneda
                    FROM saCobroTPReng r
                    JOIN saCobro c ON r.cob_num = c.cob_num
                    WHERE c.anulado = 0 
                      AND c.fecha >= @start AND c.fecha <= @end
                      ${coVen ? 'AND LTRIM(RTRIM(c.co_ven)) = @co_ven' : ''}
                      AND c.co_ven IS NOT NULL AND RTRIM(c.co_ven) <> ''
                )
                SELECT mes_inicio, YEAR(mes_inicio) AS anio, MONTH(mes_inicio) AS mes, co_ven,
                       SUM(CASE WHEN moneda = 'USD' THEN (mont_doc / NULLIF(tasa, 0)) ELSE 0 END) AS cobros_usd,
                       SUM(CASE WHEN moneda = 'BS' THEN mont_doc ELSE 0 END) AS cobros_bs
                FROM (
                    SELECT DATEFROMPARTS(YEAR(fecha), MONTH(fecha), 1) AS mes_inicio, co_ven, mont_doc, tasa, moneda
                    FROM CobrosReng
                ) x
                GROUP BY mes_inicio, co_ven
            `;
        }

        const cobReq = pool.request();
        cobReq.input('start', startDate);
        cobReq.input('end', endDate + ' 23:59:59');
        if (coVen) cobReq.input('co_ven', coVen);
        const cobResult = await cobReq.query(cobrosQuery);

        const monthNames = ['Ene', 'Feb', 'Mar', 'Abr', 'May', 'Jun', 'Jul', 'Ago', 'Sep', 'Oct', 'Nov', 'Dic'];

        // Mapas de artículos distintos por (periodo + co_ven)
        const artVendidosMap = new Map();
        const artPedidosMap = new Map();
        const artCotizadosMap = new Map();

        artResult.recordset.forEach(row => {
            let pStr = '';
            if (tipoAgrupacion === 'diario') {
                const dia = String(row.dia).padStart(2, '0');
                const mes = monthNames[row.mes - 1];
                pStr = `${dia} ${mes}`;
            } else if (tipoAgrupacion === 'semanal') {
                const dIni = String(row.dia_inicio).padStart(2, '0');
                const dFin = String(row.dia_fin).padStart(2, '0');
                if (row.mes_inicio === row.mes_fin) {
                    pStr = `${dIni} - ${dFin} ${monthNames[row.mes_fin - 1]}`;
                } else {
                    pStr = `${dIni} ${monthNames[row.mes_inicio - 1]} - ${dFin} ${monthNames[row.mes_fin - 1]}`;
                }
            } else {
                pStr = `${monthNames[row.mes - 1]} ${row.anio}`;
            }
            const key = `${pStr}_${(row.co_ven || '').trim()}`;
            const cant = Number(row.cant_art) || 0;
            if (row.tipo === 'vendidos') {
                artVendidosMap.set(key, cant);
            } else if (row.tipo === 'pedidos') {
                artPedidosMap.set(key, cant);
            } else if (row.tipo === 'cotizados') {
                artCotizadosMap.set(key, cant);
            }
        });

        // Mapas de cobros (USD y BS) por (periodo + co_ven)
        const cobrosUsdMap = new Map();
        const cobrosBsMap = new Map();

        cobResult.recordset.forEach(row => {
            let pStr = '';
            if (tipoAgrupacion === 'diario') {
                const dia = String(row.dia).padStart(2, '0');
                const mes = monthNames[row.mes - 1];
                pStr = `${dia} ${mes}`;
            } else if (tipoAgrupacion === 'semanal') {
                const dIni = String(row.dia_inicio).padStart(2, '0');
                const dFin = String(row.dia_fin).padStart(2, '0');
                if (row.mes_inicio === row.mes_fin) {
                    pStr = `${dIni} - ${dFin} ${monthNames[row.mes_fin - 1]}`;
                } else {
                    pStr = `${dIni} ${monthNames[row.mes_inicio - 1]} - ${dFin} ${monthNames[row.mes_fin - 1]}`;
                }
            } else {
                pStr = `${monthNames[row.mes - 1]} ${row.anio}`;
            }
            const key = `${pStr}_${(row.co_ven || '').trim()}`;
            cobrosUsdMap.set(key, Number(row.cobros_usd) || 0);
            cobrosBsMap.set(key, Number(row.cobros_bs) || 0);
        });

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

            const cVen = (row.co_ven || '').trim();
            const artKey = `${periodo}_${cVen}`;
            const art_distintos = artVendidosMap.get(artKey) || 0;
            const art_pedidos = artPedidosMap.get(artKey) || 0;
            const art_cotizados = artCotizadosMap.get(artKey) || 0;

            const cobKey = `${periodo}_${cVen}`;
            const cobros_usd = cobrosUsdMap.get(cobKey) || 0;
            const cobros_bs = cobrosBsMap.get(cobKey) || 0;

            return {
                periodo,
                co_ven: cVen,
                facturas: Number(row.facturas) || 0,
                devoluciones: Number(row.devoluciones) || 0,
                docs_exitosos: Number(row.docs_exitosos) || 0,
                cotizaciones: Number(row.cotizaciones) || 0,
                pedidos: Number(row.pedidos) || 0,
                fletes: Number(row.fletes) || 0,
                cortes: Number(row.cortes) || 0,
                art_distintos,
                art_pedidos,
                art_cotizados,
                cobros_usd,
                cobros_bs
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
                cortes: 0,
                art_distintos: 0,
                art_pedidos: 0,
                art_cotizados: 0,
                cobros_usd: 0,
                cobros_bs: 0
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
                item.art_distintos += r.art_distintos;
                item.art_pedidos += r.art_pedidos;
                item.art_cotizados += r.art_cotizados;
                item.cobros_usd += r.cobros_usd;
                item.cobros_bs += r.cobros_bs;
            }
        }

        // Si se filtró por un vendedor específico y es vista diaria, excluimos días donde ese vendedor tuvo 0 docs
        let timeline = Array.from(mainTimelineMap.values());
        if (coVen && tipoAgrupacion === 'diario') {
            timeline = timeline.filter(t => (t.facturas + t.cotizaciones + t.pedidos + t.devoluciones + t.fletes + t.cortes + t.art_distintos + t.art_pedidos + t.art_cotizados + t.cobros_usd + t.cobros_bs) > 0);
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
            acc.art_distintos += m.art_distintos;
            acc.art_pedidos += m.art_pedidos;
            acc.art_cotizados += m.art_cotizados;
            acc.cobros_usd += m.cobros_usd;
            acc.cobros_bs += m.cobros_bs;
            return acc;
        }, { facturas: 0, devoluciones: 0, docs_exitosos: 0, cotizaciones: 0, pedidos: 0, fletes: 0, cortes: 0, art_distintos: 0, art_pedidos: 0, art_cotizados: 0, cobros_usd: 0, cobros_bs: 0 });

        // Rankings de variedad por asesor (Facturas, Pedidos, Cotizaciones)
        let rankingVendedores = [];
        let rankingArtPedidos = [];
        let rankingArtCotizados = [];
        let totalArticulosActivos = 0;
        let totalArticulosDistintosGlobal = 0;
        let totalArtPedidosGlobal = 0;
        let totalArtCotizadosGlobal = 0;

        try {
            // 1. Ranking Artículos Únicos Vendidos
            const rankingQuery = `
                ;WITH ArticulosRango AS (
                    SELECT DISTINCT
                        LTRIM(RTRIM(f.co_ven)) AS co_ven,
                        r.co_art
                    FROM saFacturaVentaReng r
                    JOIN saFacturaVenta f ON r.doc_num = f.doc_num
                    WHERE f.anulado = 0
                      AND f.fec_emis >= @start AND f.fec_emis <= @end
                      ${coVen ? 'AND LTRIM(RTRIM(f.co_ven)) = @co_ven' : ''}
                      AND NOT EXISTS (
                          SELECT 1 
                          FROM saDevolucionClienteReng dr
                          JOIN saDevolucionCliente d ON dr.doc_num = d.doc_num
                          WHERE d.anulado = 0 
                            AND LTRIM(RTRIM(dr.tipo_doc)) = 'FACT' 
                            AND LTRIM(RTRIM(dr.num_doc)) = LTRIM(RTRIM(f.doc_num))
                      )
                )
                SELECT 
                    a.co_ven,
                    RTRIM(ISNULL(u.desc_usuario, ISNULL(v.ven_des, a.co_ven))) AS ven_des,
                    COUNT(a.co_art) AS cant_articulos_unicos,
                    MAX(CASE 
                        WHEN CAST(ISNULL(v.inactivo, 0) AS INT) = 1 OR LTRIM(RTRIM(CAST(ISNULL(v.inactivo, 0) AS VARCHAR(10)))) = '1' THEN 1
                        WHEN UPPER(LTRIM(RTRIM(ISNULL(u.Estado, '')))) = 'I' THEN 1 
                        ELSE 0 
                    END) AS inactivo
                FROM ArticulosRango a
                LEFT JOIN MasterProfitPro.dbo.MpUsuario u ON UPPER(LTRIM(RTRIM(a.co_ven))) = UPPER(LTRIM(RTRIM(u.cod_usuario)))
                LEFT JOIN saVendedor v ON UPPER(LTRIM(RTRIM(a.co_ven))) = UPPER(LTRIM(RTRIM(v.co_ven)))
                GROUP BY a.co_ven, u.desc_usuario, v.ven_des
                ORDER BY COUNT(a.co_art) DESC
            `;
            const rankingReq = pool.request();
            rankingReq.input('start', startDate);
            rankingReq.input('end', endDate + ' 23:59:59');
            if (coVen) rankingReq.input('co_ven', coVen);
            const rankingRes = await rankingReq.query(rankingQuery);
            rankingVendedores = rankingRes.recordset.map(r => ({
                co_ven: r.co_ven,
                ven_des: (r.ven_des || r.co_ven || '').trim().toUpperCase(),
                cant_articulos_unicos: Number(r.cant_articulos_unicos) || 0,
                inactivo: Number(r.inactivo) === 1
            }));

            // 2. Ranking Artículos Únicos Pedidos
            const rankingPedQuery = `
                ;WITH ArticulosRangoPed AS (
                    SELECT DISTINCT
                        LTRIM(RTRIM(f.co_ven)) AS co_ven,
                        r.co_art
                    FROM saPedidoVentaReng r
                    JOIN saPedidoVenta f ON r.doc_num = f.doc_num
                    WHERE f.anulado = 0
                      AND f.fec_emis >= @start AND f.fec_emis <= @end
                      ${coVen ? 'AND LTRIM(RTRIM(f.co_ven)) = @co_ven' : ''}
                )
                SELECT 
                    a.co_ven,
                    RTRIM(ISNULL(u.desc_usuario, ISNULL(v.ven_des, a.co_ven))) AS ven_des,
                    COUNT(a.co_art) AS cant_articulos_unicos,
                    MAX(CASE 
                        WHEN CAST(ISNULL(v.inactivo, 0) AS INT) = 1 OR LTRIM(RTRIM(CAST(ISNULL(v.inactivo, 0) AS VARCHAR(10)))) = '1' THEN 1
                        WHEN UPPER(LTRIM(RTRIM(ISNULL(u.Estado, '')))) = 'I' THEN 1 
                        ELSE 0 
                    END) AS inactivo
                FROM ArticulosRangoPed a
                LEFT JOIN MasterProfitPro.dbo.MpUsuario u ON UPPER(LTRIM(RTRIM(a.co_ven))) = UPPER(LTRIM(RTRIM(u.cod_usuario)))
                LEFT JOIN saVendedor v ON UPPER(LTRIM(RTRIM(a.co_ven))) = UPPER(LTRIM(RTRIM(v.co_ven)))
                GROUP BY a.co_ven, u.desc_usuario, v.ven_des
                ORDER BY COUNT(a.co_art) DESC
            `;
            const rankingPedReq = pool.request();
            rankingPedReq.input('start', startDate);
            rankingPedReq.input('end', endDate + ' 23:59:59');
            if (coVen) rankingPedReq.input('co_ven', coVen);
            const rankingPedRes = await rankingPedReq.query(rankingPedQuery);
            rankingArtPedidos = rankingPedRes.recordset.map(r => ({
                co_ven: r.co_ven,
                ven_des: (r.ven_des || r.co_ven || '').trim().toUpperCase(),
                cant_articulos_unicos: Number(r.cant_articulos_unicos) || 0,
                inactivo: Number(r.inactivo) === 1
            }));

            // 3. Ranking Artículos Únicos Cotizados
            const rankingCotQuery = `
                ;WITH ArticulosRangoCot AS (
                    SELECT DISTINCT
                        LTRIM(RTRIM(f.co_ven)) AS co_ven,
                        r.co_art
                    FROM saCotizacionClienteReng r
                    JOIN saCotizacionCliente f ON r.doc_num = f.doc_num
                    WHERE f.anulado = 0
                      AND f.fec_emis >= @start AND f.fec_emis <= @end
                      ${coVen ? 'AND LTRIM(RTRIM(f.co_ven)) = @co_ven' : ''}
                )
                SELECT 
                    a.co_ven,
                    RTRIM(ISNULL(u.desc_usuario, ISNULL(v.ven_des, a.co_ven))) AS ven_des,
                    COUNT(a.co_art) AS cant_articulos_unicos,
                    MAX(CASE 
                        WHEN CAST(ISNULL(v.inactivo, 0) AS INT) = 1 OR LTRIM(RTRIM(CAST(ISNULL(v.inactivo, 0) AS VARCHAR(10)))) = '1' THEN 1
                        WHEN UPPER(LTRIM(RTRIM(ISNULL(u.Estado, '')))) = 'I' THEN 1 
                        ELSE 0 
                    END) AS inactivo
                FROM ArticulosRangoCot a
                LEFT JOIN MasterProfitPro.dbo.MpUsuario u ON UPPER(LTRIM(RTRIM(a.co_ven))) = UPPER(LTRIM(RTRIM(u.cod_usuario)))
                LEFT JOIN saVendedor v ON UPPER(LTRIM(RTRIM(a.co_ven))) = UPPER(LTRIM(RTRIM(v.co_ven)))
                GROUP BY a.co_ven, u.desc_usuario, v.ven_des
                ORDER BY COUNT(a.co_art) DESC
            `;
            const rankingCotReq = pool.request();
            rankingCotReq.input('start', startDate);
            rankingCotReq.input('end', endDate + ' 23:59:59');
            if (coVen) rankingCotReq.input('co_ven', coVen);
            const rankingCotRes = await rankingCotReq.query(rankingCotQuery);
            rankingArtCotizados = rankingCotRes.recordset.map(r => ({
                co_ven: r.co_ven,
                ven_des: (r.ven_des || r.co_ven || '').trim().toUpperCase(),
                cant_articulos_unicos: Number(r.cant_articulos_unicos) || 0,
                inactivo: Number(r.inactivo) === 1
            }));

            // Total de artículos activos en la base de datos
            const activosRes = await pool.request().query(`
                SELECT COUNT(*) AS total_activos FROM saArticulo WHERE anulado = 0
            `);
            totalArticulosActivos = Number(activosRes.recordset[0]?.total_activos) || 0;

            // Totales globales en el rango
            const globalReq = pool.request();
            globalReq.input('start', startDate);
            globalReq.input('end', endDate + ' 23:59:59');
            if (coVen) globalReq.input('co_ven', coVen);
            const globalRes = await globalReq.query(`
                ;WITH ArticulosGlobal AS (
                    SELECT DISTINCT r.co_art
                    FROM saFacturaVentaReng r
                    JOIN saFacturaVenta f ON r.doc_num = f.doc_num
                    WHERE f.anulado = 0
                      AND f.fec_emis >= @start AND f.fec_emis <= @end
                      ${coVen ? 'AND LTRIM(RTRIM(f.co_ven)) = @co_ven' : ''}
                      AND NOT EXISTS (
                          SELECT 1 
                          FROM saDevolucionClienteReng dr
                          JOIN saDevolucionCliente d ON dr.doc_num = d.doc_num
                          WHERE d.anulado = 0 
                            AND LTRIM(RTRIM(dr.tipo_doc)) = 'FACT' 
                            AND LTRIM(RTRIM(dr.num_doc)) = LTRIM(RTRIM(f.doc_num))
                      )
                ),
                ArtGlobalPed AS (
                    SELECT DISTINCT r.co_art
                    FROM saPedidoVentaReng r
                    JOIN saPedidoVenta f ON r.doc_num = f.doc_num
                    WHERE f.anulado = 0 AND f.fec_emis >= @start AND f.fec_emis <= @end
                      ${coVen ? 'AND LTRIM(RTRIM(f.co_ven)) = @co_ven' : ''}
                ),
                ArtGlobalCot AS (
                    SELECT DISTINCT r.co_art
                    FROM saCotizacionClienteReng r
                    JOIN saCotizacionCliente f ON r.doc_num = f.doc_num
                    WHERE f.anulado = 0 AND f.fec_emis >= @start AND f.fec_emis <= @end
                      ${coVen ? 'AND LTRIM(RTRIM(f.co_ven)) = @co_ven' : ''}
                )
                SELECT 
                    (SELECT COUNT(*) FROM ArticulosGlobal) AS total_global_vendidos,
                    (SELECT COUNT(*) FROM ArtGlobalPed) AS total_global_pedidos,
                    (SELECT COUNT(*) FROM ArtGlobalCot) AS total_global_cotizados
            `);
            totalArticulosDistintosGlobal = Number(globalRes.recordset[0]?.total_global_vendidos) || 0;
            totalArtPedidosGlobal = Number(globalRes.recordset[0]?.total_global_pedidos) || 0;
            totalArtCotizadosGlobal = Number(globalRes.recordset[0]?.total_global_cotizados) || 0;
        } catch (rankErr) {
            console.warn('[rendimiento-vendedores] Error calculando rankings de variedad:', rankErr.message);
        }

        // Rankings de Cobros en USD y BS por Asesor
        let rankingCobrosUsd = [];
        let rankingCobrosBs = [];
        let totalCobrosUsdGlobal = 0;
        let totalCobrosBsGlobal = 0;

        try {
            const rankingCobrosQuery = `
                ;WITH CobrosTotales AS (
                    SELECT 
                        LTRIM(RTRIM(c.co_ven)) AS co_ven,
                        r.mont_doc,
                        CASE 
                            WHEN c.tasa > 1.000001 THEN c.tasa
                            ELSE ISNULL(
                                (SELECT TOP 1 t.tasa_v 
                                 FROM saTasa t 
                                 WHERE LTRIM(RTRIM(t.co_mone)) IN ('USD', 'US$', 'US', '$') 
                                   AND CONVERT(VARCHAR(10), t.fecha, 120) <= CONVERT(VARCHAR(10), c.fecha, 120) 
                                 ORDER BY t.fecha DESC),
                                (SELECT TOP 1 t.tasa_v 
                                 FROM saTasa t 
                                 WHERE LTRIM(RTRIM(t.co_mone)) IN ('USD', 'US$', 'US', '$') 
                                 ORDER BY t.fecha DESC)
                            )
                        END AS tasa,
                        CASE 
                            WHEN RTRIM(r.cod_caja) = '01' THEN 'USD'
                            WHEN RTRIM(r.cod_caja) = '02' THEN 'BS'
                            WHEN UPPER(RTRIM(ISNULL(r.cod_cta, ''))) IN ('ZELLE', 'USDT') THEN 'USD'
                            ELSE 'BS'
                        END AS moneda
                    FROM saCobroTPReng r
                    JOIN saCobro c ON r.cob_num = c.cob_num
                    WHERE c.anulado = 0 
                      AND c.fecha >= @start AND c.fecha <= @end
                      ${coVen ? 'AND LTRIM(RTRIM(c.co_ven)) = @co_ven' : ''}
                      AND c.co_ven IS NOT NULL AND RTRIM(c.co_ven) <> ''
                )
                SELECT 
                    a.co_ven,
                    RTRIM(ISNULL(u.desc_usuario, ISNULL(v.ven_des, a.co_ven))) AS ven_des,
                    SUM(CASE WHEN a.moneda = 'USD' THEN (a.mont_doc / NULLIF(a.tasa, 0)) ELSE 0 END) AS total_usd,
                    SUM(CASE WHEN a.moneda = 'BS' THEN a.mont_doc ELSE 0 END) AS total_bs,
                    MAX(CASE 
                        WHEN CAST(ISNULL(v.inactivo, 0) AS INT) = 1 OR LTRIM(RTRIM(CAST(ISNULL(v.inactivo, 0) AS VARCHAR(10)))) = '1' THEN 1
                        WHEN UPPER(LTRIM(RTRIM(ISNULL(u.Estado, '')))) = 'I' THEN 1 
                        ELSE 0 
                    END) AS inactivo
                FROM CobrosTotales a
                LEFT JOIN MasterProfitPro.dbo.MpUsuario u ON UPPER(LTRIM(RTRIM(a.co_ven))) = UPPER(LTRIM(RTRIM(u.cod_usuario)))
                LEFT JOIN saVendedor v ON UPPER(LTRIM(RTRIM(a.co_ven))) = UPPER(LTRIM(RTRIM(v.co_ven)))
                GROUP BY a.co_ven, u.desc_usuario, v.ven_des
            `;
            const cobRankReq = pool.request();
            cobRankReq.input('start', startDate);
            cobRankReq.input('end', endDate + ' 23:59:59');
            if (coVen) cobRankReq.input('co_ven', coVen);
            const cobRankRes = await cobRankReq.query(rankingCobrosQuery);

            const allCobRows = cobRankRes.recordset.map(r => ({
                co_ven: r.co_ven,
                ven_des: (r.ven_des || r.co_ven || '').trim().toUpperCase(),
                total_usd: Number(r.total_usd) || 0,
                total_bs: Number(r.total_bs) || 0,
                inactivo: Number(r.inactivo) === 1
            }));

            rankingCobrosUsd = [...allCobRows].sort((a, b) => b.total_usd - a.total_usd);
            rankingCobrosBs = [...allCobRows].sort((a, b) => b.total_bs - a.total_bs);

            totalCobrosUsdGlobal = allCobRows.reduce((acc, r) => acc + r.total_usd, 0);
            totalCobrosBsGlobal = allCobRows.reduce((acc, r) => acc + r.total_bs, 0);
        } catch (cobRankErr) {
            console.warn('[rendimiento-vendedores] Error calculando rankings de cobros:', cobRankErr.message);
        }

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
                    UNION ALL
                    SELECT co_ven, cob_num AS doc_num FROM saCobro WHERE anulado = 0 AND fecha >= @start AND fecha <= @end
                    UNION ALL
                    SELECT co_ven, NULL AS doc_num FROM saVendedor
                )
                SELECT 
                    RTRIM(f.co_ven) AS co_ven,
                    RTRIM(ISNULL(u.desc_usuario, ISNULL(v.ven_des, f.co_ven))) AS ven_des,
                    COUNT(DISTINCT f.doc_num) AS total_docs,
                    MAX(CASE 
                        WHEN CAST(ISNULL(v.inactivo, 0) AS INT) = 1 OR LTRIM(RTRIM(CAST(ISNULL(v.inactivo, 0) AS VARCHAR(10)))) = '1' THEN 1
                        WHEN UPPER(LTRIM(RTRIM(ISNULL(u.Estado, '')))) = 'I' THEN 1 
                        ELSE 0 
                    END) AS inactivo
                FROM VendedoresDocs f
                LEFT JOIN MasterProfitPro.dbo.MpUsuario u ON UPPER(LTRIM(RTRIM(f.co_ven))) = UPPER(LTRIM(RTRIM(u.cod_usuario)))
                LEFT JOIN saVendedor v ON UPPER(LTRIM(RTRIM(f.co_ven))) = UPPER(LTRIM(RTRIM(v.co_ven)))
                WHERE f.co_ven IS NOT NULL AND RTRIM(f.co_ven) <> ''
                GROUP BY f.co_ven, u.desc_usuario, v.ven_des
                ORDER BY total_docs DESC
            `);
            vendedores = vendResult.recordset.map(row => ({
                co_ven: (row.co_ven || '').trim(),
                ven_des: (row.ven_des || row.co_ven || '').trim().toUpperCase(),
                inactivo: Number(row.inactivo) === 1,
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
            mensual: timeline,
            periodosComparativa: allPeriodLabels,
            vendedoresTimeline: rawRows,
            vendedores,
            rankingVendedores,
            rankingArtPedidos,
            rankingArtCotizados,
            rankingCobrosUsd,
            rankingCobrosBs,
            totalArticulosActivos,
            totalArticulosDistintosGlobal,
            totalArtPedidosGlobal,
            totalArtCotizadosGlobal,
            totalCobrosUsdGlobal,
            totalCobrosBsGlobal
        });

    } catch (error) {
        console.error(`[GET /rendimiento-vendedores]`, error);
        res.status(500).json({ success: false, message: 'Error en el reporte de rendimiento de vendedores.', error: error.message });
    }
});

module.exports = router;
