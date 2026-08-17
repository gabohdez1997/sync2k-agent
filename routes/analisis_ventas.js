const express = require('express');
const router = express.Router();
const { getPool, getServers } = require('../db');

// Función auxiliar para calcular días hábiles (excluye domingos y feriados fijos VE)
function getBusinessDays(startDateStr, endDateStr) {
    const start = new Date(startDateStr);
    const end = new Date(endDateStr);
    
    if (isNaN(start) || isNaN(end)) return 30;
    if (start > end) return 1;

    const fixedHolidays = [
        '01-01', '04-19', '05-01', '06-24', '07-05', '07-24', '10-12', '12-24', '12-25', '12-31'
    ];

    let count = 0;
    const cur = new Date(start);
    while (cur <= end) {
        const dayOfWeek = cur.getDay(); // 0 = Domingo
        const mmdd = String(cur.getMonth() + 1).padStart(2, '0') + '-' + String(cur.getDate()).padStart(2, '0');
        if (dayOfWeek !== 0 && !fixedHolidays.includes(mmdd)) {
            count++;
        }
        cur.setDate(cur.getDate() + 1);
    }
    return count > 0 ? count : 1;
}

/**
 * @swagger
 * /api/v1/analisis-ventas:
 *   get:
 *     summary: Obtiene el listado de artículos categorizados con stock actual, ventas netas, promedio diario y clasificación ABC/XYZ
 *     tags: [Reportes]
 */
router.get('/', async (req, res) => {
    try {
        let sede = req.query.sede || 'default';
        const startDate = req.query.startDate || new Date(new Date().setDate(new Date().getDate() - 30)).toISOString().split('T')[0];
        const endDate = req.query.endDate || new Date().toISOString().split('T')[0];

        const servers = getServers();
        if (sede === 'default') {
            if (servers && servers.length > 0) {
                sede = servers[0].id;
            } else {
                return res.status(500).json({ success: false, message: 'No hay servidores SQL configurados.' });
            }
        }

        const businessDays = getBusinessDays(startDate, endDate);
        const pool = await getPool(sede, req.sqlAuth);

        const query = `
            ;WITH v_ventas_stock AS (
                SELECT 
                    'VENTA' AS tipo_transaccion, 
                    f.fec_emis AS fecha, 
                    r.co_art, 
                    r.total_art AS cantidad, 
                    r.reng_neto AS monto,
                    0 AS stock_actual
                FROM saFacturaVenta f 
                JOIN saFacturaVentaReng r ON f.doc_num = r.doc_num 
                WHERE f.anulado = 0
                UNION ALL
                SELECT 
                    'DEVOLUCION', 
                    d.fec_emis, 
                    r.co_art, 
                    r.total_art, 
                    r.reng_neto,
                    0
                FROM saDevolucionCliente d 
                JOIN saDevolucionClienteReng r ON d.doc_num = r.doc_num 
                WHERE d.anulado = 0
                UNION ALL
                SELECT 
                    'STOCK', 
                    GETDATE(), 
                    co_art, 
                    0, 
                    0,
                    SUM(ISNULL(stock, 0))
                FROM saStockAlmacen 
                GROUP BY co_art
            )
            SELECT 
                a.co_art,
                MAX(a.art_des) as des_art,
                MAX(a.co_lin) as co_lin,
                MAX(l.lin_des) as des_lin,
                MAX(a.co_subl) as co_subl,
                MAX(sl.subl_des) as des_subl,
                MAX(a.co_cat) as co_cat,
                MAX(c.cat_des) as des_cat,
                RTRIM(COALESCE(MAX(aun.co_uni), '01')) as co_uni,
                RTRIM(COALESCE(MAX(aun.des_uni), MAX(aun.co_uni), 'UND')) as des_uni,
                SUM(CASE WHEN v.tipo_transaccion = 'STOCK' THEN v.stock_actual ELSE 0 END) as stock_actual,
                SUM(CASE WHEN v.tipo_transaccion = 'VENTA' AND v.fecha >= @start AND v.fecha <= @end THEN v.cantidad ELSE 0 END) 
                - SUM(CASE WHEN v.tipo_transaccion = 'DEVOLUCION' AND v.fecha >= @start AND v.fecha <= @end THEN v.cantidad ELSE 0 END) as ventas_netas,
                SUM(CASE WHEN v.tipo_transaccion = 'VENTA' AND v.fecha >= @start AND v.fecha <= @end THEN v.monto ELSE 0 END) 
                - SUM(CASE WHEN v.tipo_transaccion = 'DEVOLUCION' AND v.fecha >= @start AND v.fecha <= @end THEN v.monto ELSE 0 END) as valor_ventas,
                STDEV(CASE WHEN v.tipo_transaccion = 'VENTA' AND v.fecha >= @start AND v.fecha <= @end THEN v.cantidad ELSE NULL END) as desviacion_ventas
            FROM saArticulo a
            LEFT JOIN v_ventas_stock v ON a.co_art = v.co_art
            LEFT JOIN saLineaArticulo l ON a.co_lin = l.co_lin
            LEFT JOIN saSubLinea sl ON a.co_subl = sl.co_subl AND a.co_lin = sl.co_lin
            LEFT JOIN saCategoriaArticulo c ON a.co_cat = c.co_cat
            LEFT JOIN (
                SELECT au.co_art, au.co_uni, u.des_uni,
                       ROW_NUMBER() OVER(PARTITION BY au.co_art ORDER BY au.uni_principal DESC) as rn
                FROM saArtUnidad au
                LEFT JOIN saUnidad u ON LTRIM(RTRIM(au.co_uni)) = LTRIM(RTRIM(u.co_uni))
            ) aun ON a.co_art = aun.co_art AND aun.rn = 1
            WHERE a.anulado = 0 AND a.co_art NOT LIKE '09%'
            GROUP BY a.co_art
            HAVING SUM(CASE WHEN v.tipo_transaccion = 'STOCK' THEN v.stock_actual ELSE 0 END) > 0 
                OR (SUM(CASE WHEN v.tipo_transaccion = 'VENTA' AND v.fecha >= @start AND v.fecha <= @end THEN v.cantidad ELSE 0 END) > 0)
        `;

        const request = pool.request();
        request.input('start', startDate);
        request.input('end', endDate + ' 23:59:59');

        const result = await request.query(query);

        let totalSalesVal = 0;
        let totalUnidadesVendidas = 0;

        let items = result.recordset.map(row => {
            const co_art = (row.co_art || '').trim();
            const des_art = (row.des_art || '').trim();
            const co_lin = (row.co_lin || '').trim();
            const des_lin = (row.des_lin || '').trim();
            const co_subl = (row.co_subl || '').trim();
            const des_subl = (row.des_subl || '').trim();
            const co_cat = (row.co_cat || '').trim();
            const des_cat = (row.des_cat || '').trim();
            const co_uni = (row.co_uni || '').trim();
            const des_uni = (row.des_uni || '').trim();

            const stock_actual = Math.max(0, Number(row.stock_actual) || 0);
            const ventas_netas = Math.max(0, Number(row.ventas_netas) || 0);
            const valor_ventas = Math.max(0, Number(row.valor_ventas) || 0);
            const vpd = businessDays > 0 ? (ventas_netas / businessDays) : 0;
            const stdev = Number(row.desviacion_ventas) || 0;
            const cv = (stdev > 0 && vpd > 0) ? (stdev / vpd) : 0;

            totalSalesVal += valor_ventas;
            totalUnidadesVendidas += ventas_netas;

            return {
                co_art,
                des_art,
                co_lin,
                des_lin,
                co_subl,
                des_subl,
                co_cat,
                des_cat,
                co_uni,
                des_uni,
                stock_actual,
                ventas_netas,
                valor_ventas,
                vpd,
                cv
            };
        });

        // Clasificación ABC / XYZ
        items.sort((a, b) => b.valor_ventas - a.valor_ventas || b.ventas_netas - a.ventas_netas);
        let cumulativeVal = 0;
        items = items.map(item => {
            cumulativeVal += item.valor_ventas;
            const percentage = totalSalesVal > 0 ? (cumulativeVal / totalSalesVal) : 0;

            let abc = 'C';
            if (percentage <= 0.80) abc = 'A';
            else if (percentage <= 0.95) abc = 'B';

            let xyz = 'Z';
            if (item.cv <= 0.20) xyz = 'X';
            else if (item.cv <= 0.60) xyz = 'Y';

            return {
                ...item,
                clasificacion_abc: abc,
                clasificacion_xyz: xyz,
                clase_conjunta: abc + xyz
            };
        });

        res.json({
            success: true,
            server: sede,
            startDate,
            endDate,
            businessDays,
            kpis: {
                total_articulos: items.length,
                total_unidades_vendidas: totalUnidadesVendidas,
                total_valor_ventas: totalSalesVal,
                articulos_con_stock: items.filter(i => i.stock_actual > 0).length,
                articulos_sin_stock: items.filter(i => i.stock_actual <= 0).length
            },
            data: items
        });

    } catch (error) {
        console.error(`[GET /analisis-ventas]`, error);
        res.status(500).json({ success: false, message: 'Error consultando análisis de ventas.', error: error.message });
    }
});

/**
 * @swagger
 * /api/v1/analisis-ventas/article-vendors:
 *   get:
 *     summary: Obtiene la evolución temporal de ventas y desglose de vendedores para un artículo específico
 *     tags: [Reportes]
 *     parameters:
 *       - in: query
 *         name: co_art
 *         required: true
 *         schema: { type: string }
 */
router.get('/article-vendors', async (req, res) => {
    try {
        const co_art = (req.query.co_art || '').trim();
        if (!co_art) {
            return res.status(400).json({ success: false, message: 'El parámetro co_art es obligatorio.' });
        }

        let sede = req.query.sede || 'default';
        const startDate = req.query.startDate || new Date(new Date().setDate(new Date().getDate() - 30)).toISOString().split('T')[0];
        const endDate = req.query.endDate || new Date().toISOString().split('T')[0];

        const servers = getServers();
        if (sede === 'default') {
            if (servers && servers.length > 0) {
                sede = servers[0].id;
            } else {
                return res.status(500).json({ success: false, message: 'No hay servidores SQL configurados.' });
            }
        }

        const pool = await getPool(sede, req.sqlAuth);

        // Calcular granularidad temporal
        const sParts = startDate.split('-').map(Number);
        const eParts = endDate.split('-').map(Number);
        const sYear = sParts[0] || new Date().getFullYear();
        const sMonth = sParts[1] || (new Date().getMonth() + 1);
        const sDay = sParts[2] || 1;
        const eYear = eParts[0] || new Date().getFullYear();
        const eMonth = eParts[1] || (new Date().getMonth() + 1);
        const eDay = eParts[2] || 1;

        const daysInStartMonth = new Date(sYear, sMonth, 0).getDate();
        const sDate = new Date(Date.UTC(sYear, sMonth - 1, sDay));
        const eDate = new Date(Date.UTC(eYear, eMonth - 1, eDay));
        const diffDays = Math.max(1, Math.round((eDate.getTime() - sDate.getTime()) / (1000 * 60 * 60 * 24)) + 1);

        let tipoAgrupacion = 'mensual';
        if (diffDays <= (daysInStartMonth + 1)) {
            tipoAgrupacion = 'diario';
        } else if (diffDays <= 93) {
            tipoAgrupacion = 'semanal';
        } else {
            tipoAgrupacion = 'mensual';
        }

        // 1. Datos básicos del artículo
        const artReq = pool.request();
        artReq.input('co_art', co_art);
        const artRes = await artReq.query(`
            SELECT 
                a.co_art,
                a.art_des,
                a.co_lin,
                l.lin_des,
                a.co_subl,
                sl.subl_des,
                a.co_cat,
                c.cat_des,
                RTRIM(COALESCE(aun.co_uni, '01')) as co_uni,
                RTRIM(COALESCE(aun.des_uni, aun.co_uni, 'UND')) as des_uni,
                (SELECT ISNULL(SUM(stock), 0) FROM saStockAlmacen WHERE co_art = a.co_art) AS stock_actual
            FROM saArticulo a
            LEFT JOIN saLineaArticulo l ON a.co_lin = l.co_lin
            LEFT JOIN saSubLinea sl ON a.co_subl = sl.co_subl AND a.co_lin = sl.co_lin
            LEFT JOIN saCategoriaArticulo c ON a.co_cat = c.co_cat
            LEFT JOIN (
                SELECT au.co_art, au.co_uni, u.des_uni,
                       ROW_NUMBER() OVER(PARTITION BY au.co_art ORDER BY au.uni_principal DESC) as rn
                FROM saArtUnidad au
                LEFT JOIN saUnidad u ON LTRIM(RTRIM(au.co_uni)) = LTRIM(RTRIM(u.co_uni))
            ) aun ON a.co_art = aun.co_art AND aun.rn = 1
            WHERE a.co_art = @co_art
        `);

        const articleInfo = artRes.recordset[0] || {
            co_art,
            art_des: co_art,
            stock_actual: 0
        };

        // 2. Ranking acumulado de vendedores para este artículo en el rango
        const rankingReq = pool.request();
        rankingReq.input('co_art', co_art);
        rankingReq.input('start', startDate);
        rankingReq.input('end', endDate + ' 23:59:59');

        const rankingRes = await rankingReq.query(`
            ;WITH VentasArticulo AS (
                SELECT 
                    LTRIM(RTRIM(f.co_ven)) AS co_ven,
                    r.total_art AS cant,
                    r.reng_neto AS monto,
                    f.doc_num
                FROM saFacturaVenta f
                JOIN saFacturaVentaReng r ON f.doc_num = r.doc_num
                WHERE f.anulado = 0
                  AND r.co_art = @co_art
                  AND f.fec_emis >= @start AND f.fec_emis <= @end
                UNION ALL
                SELECT 
                    LTRIM(RTRIM(d.co_ven)) AS co_ven,
                    -r.total_art AS cant,
                    -r.reng_neto AS monto,
                    d.doc_num
                FROM saDevolucionCliente d
                JOIN saDevolucionClienteReng r ON d.doc_num = r.doc_num
                WHERE d.anulado = 0
                  AND r.co_art = @co_art
                  AND d.fec_emis >= @start AND d.fec_emis <= @end
            )
            SELECT 
                v.co_ven,
                RTRIM(ISNULL(u.desc_usuario, ISNULL(ven.ven_des, v.co_ven))) AS ven_des,
                MAX(CASE 
                    WHEN CAST(ISNULL(ven.inactivo, 0) AS INT) = 1 OR LTRIM(RTRIM(CAST(ISNULL(ven.inactivo, 0) AS VARCHAR(10)))) = '1' THEN 1
                    WHEN UPPER(LTRIM(RTRIM(ISNULL(u.Estado, '')))) = 'I' THEN 1 
                    ELSE 0 
                END) AS inactivo,
                SUM(v.cant) AS cant_vendida,
                SUM(v.monto) AS monto_total,
                COUNT(DISTINCT v.doc_num) AS facturas_count
            FROM VentasArticulo v
            LEFT JOIN MasterProfitPro.dbo.MpUsuario u ON UPPER(LTRIM(RTRIM(v.co_ven))) = UPPER(LTRIM(RTRIM(u.cod_usuario)))
            LEFT JOIN saVendedor ven ON UPPER(LTRIM(RTRIM(v.co_ven))) = UPPER(LTRIM(RTRIM(ven.co_ven)))
            GROUP BY v.co_ven, u.desc_usuario, ven.ven_des
            HAVING SUM(v.cant) > 0
            ORDER BY SUM(v.cant) DESC
        `);

        let totalArtVendidos = 0;
        let totalMontoVendidos = 0;
        const ranking = rankingRes.recordset.map(row => {
            const cant = Number(row.cant_vendida) || 0;
            const monto = Number(row.monto_total) || 0;
            totalArtVendidos += cant;
            totalMontoVendidos += monto;
            return {
                co_ven: (row.co_ven || '').trim(),
                ven_des: (row.ven_des || row.co_ven || '').trim().toUpperCase(),
                inactivo: Number(row.inactivo) === 1,
                cant_vendida: cant,
                monto_total: monto,
                facturas_count: Number(row.facturas_count) || 0
            };
        });

        // Calcular porcentaje de participación
        const rankingWithPct = ranking.map((r, idx) => ({
            ...r,
            pct_participacion: totalArtVendidos > 0 ? Number(((r.cant_vendida / totalArtVendidos) * 100).toFixed(2)) : 0,
            posicion: idx + 1
        }));

        // 3. Evolución temporal de ventas del artículo agrupada por vendedor
        let timeQuery = '';
        if (tipoAgrupacion === 'diario') {
            timeQuery = `
                ;WITH VentasTimeline AS (
                    SELECT 
                        CAST(f.fec_emis AS DATE) AS fecha,
                        LTRIM(RTRIM(f.co_ven)) AS co_ven,
                        r.total_art AS cant,
                        r.reng_neto AS monto
                    FROM saFacturaVenta f
                    JOIN saFacturaVentaReng r ON f.doc_num = r.doc_num
                    WHERE f.anulado = 0
                      AND r.co_art = @co_art
                      AND f.fec_emis >= @start AND f.fec_emis <= @end
                    UNION ALL
                    SELECT 
                        CAST(d.fec_emis AS DATE) AS fecha,
                        LTRIM(RTRIM(d.co_ven)) AS co_ven,
                        -r.total_art AS cant,
                        -r.reng_neto AS monto
                    FROM saDevolucionCliente d
                    JOIN saDevolucionClienteReng r ON d.doc_num = r.doc_num
                    WHERE d.anulado = 0
                      AND r.co_art = @co_art
                      AND d.fec_emis >= @start AND d.fec_emis <= @end
                )
                SELECT 
                    fecha AS periodo_inicio,
                    DAY(fecha) AS dia,
                    MONTH(fecha) AS mes,
                    YEAR(fecha) AS anio,
                    co_ven,
                    SUM(cant) AS cant_vendida,
                    SUM(monto) AS monto_vendido
                FROM VentasTimeline
                GROUP BY fecha, co_ven
                HAVING SUM(cant) > 0
                ORDER BY fecha ASC
            `;
        } else if (tipoAgrupacion === 'semanal') {
            timeQuery = `
                ;WITH VentasTimeline AS (
                    SELECT 
                        DATEADD(day, - ((DATEPART(weekday, f.fec_emis) + @@DATEFIRST - 2) % 7), CAST(f.fec_emis AS DATE)) AS semana_inicio,
                        LTRIM(RTRIM(f.co_ven)) AS co_ven,
                        r.total_art AS cant,
                        r.reng_neto AS monto
                    FROM saFacturaVenta f
                    JOIN saFacturaVentaReng r ON f.doc_num = r.doc_num
                    WHERE f.anulado = 0
                      AND r.co_art = @co_art
                      AND f.fec_emis >= @start AND f.fec_emis <= @end
                    UNION ALL
                    SELECT 
                        DATEADD(day, - ((DATEPART(weekday, d.fec_emis) + @@DATEFIRST - 2) % 7), CAST(d.fec_emis AS DATE)) AS semana_inicio,
                        LTRIM(RTRIM(d.co_ven)) AS co_ven,
                        -r.total_art AS cant,
                        -r.reng_neto AS monto
                    FROM saDevolucionCliente d
                    JOIN saDevolucionClienteReng r ON d.doc_num = r.doc_num
                    WHERE d.anulado = 0
                      AND r.co_art = @co_art
                      AND d.fec_emis >= @start AND d.fec_emis <= @end
                )
                SELECT 
                    semana_inicio AS periodo_inicio,
                    DAY(semana_inicio) AS dia_inicio,
                    MONTH(semana_inicio) AS mes_inicio,
                    YEAR(semana_inicio) AS anio_inicio,
                    DAY(DATEADD(day, 6, semana_inicio)) AS dia_fin,
                    MONTH(DATEADD(day, 6, semana_inicio)) AS mes_fin,
                    YEAR(DATEADD(day, 6, semana_inicio)) AS anio_fin,
                    co_ven,
                    SUM(cant) AS cant_vendida,
                    SUM(monto) AS monto_vendido
                FROM VentasTimeline
                GROUP BY semana_inicio, co_ven
                HAVING SUM(cant) > 0
                ORDER BY semana_inicio ASC
            `;
        } else {
            timeQuery = `
                ;WITH VentasTimeline AS (
                    SELECT 
                        DATEFROMPARTS(YEAR(f.fec_emis), MONTH(f.fec_emis), 1) AS mes_inicio,
                        LTRIM(RTRIM(f.co_ven)) AS co_ven,
                        r.total_art AS cant,
                        r.reng_neto AS monto
                    FROM saFacturaVenta f
                    JOIN saFacturaVentaReng r ON f.doc_num = r.doc_num
                    WHERE f.anulado = 0
                      AND r.co_art = @co_art
                      AND f.fec_emis >= @start AND f.fec_emis <= @end
                    UNION ALL
                    SELECT 
                        DATEFROMPARTS(YEAR(d.fec_emis), MONTH(d.fec_emis), 1) AS mes_inicio,
                        LTRIM(RTRIM(d.co_ven)) AS co_ven,
                        -r.total_art AS cant,
                        -r.reng_neto AS monto
                    FROM saDevolucionCliente d
                    JOIN saDevolucionClienteReng r ON d.doc_num = r.doc_num
                    WHERE d.anulado = 0
                      AND r.co_art = @co_art
                      AND d.fec_emis >= @start AND d.fec_emis <= @end
                )
                SELECT 
                    mes_inicio AS periodo_inicio,
                    YEAR(mes_inicio) AS anio,
                    MONTH(mes_inicio) AS mes,
                    co_ven,
                    SUM(cant) AS cant_vendida,
                    SUM(monto) AS monto_vendido
                FROM VentasTimeline
                GROUP BY mes_inicio, co_ven
                HAVING SUM(cant) > 0
                ORDER BY mes_inicio ASC
            `;
        }

        const timeReq = pool.request();
        timeReq.input('co_art', co_art);
        timeReq.input('start', startDate);
        timeReq.input('end', endDate + ' 23:59:59');
        const timeRes = await timeReq.query(timeQuery);

        const monthNames = ['Ene', 'Feb', 'Mar', 'Abr', 'May', 'Jun', 'Jul', 'Ago', 'Sep', 'Oct', 'Nov', 'Dic'];

        // Agrupar timeline por período
        const periodsMap = new Map();
        timeRes.recordset.forEach(row => {
            let label = '';
            if (tipoAgrupacion === 'diario') {
                const dia = String(row.dia).padStart(2, '0');
                const mes = monthNames[row.mes - 1];
                label = `${dia} ${mes}`;
            } else if (tipoAgrupacion === 'semanal') {
                const dIni = String(row.dia_inicio).padStart(2, '0');
                const dFin = String(row.dia_fin).padStart(2, '0');
                if (row.mes_inicio === row.mes_fin) {
                    label = `${dIni} - ${dFin} ${monthNames[row.mes_fin - 1]}`;
                } else {
                    label = `${dIni} ${monthNames[row.mes_inicio - 1]} - ${dFin} ${monthNames[row.mes_fin - 1]}`;
                }
            } else {
                label = `${monthNames[row.mes - 1]} ${row.anio}`;
            }

            if (!periodsMap.has(label)) {
                periodsMap.set(label, {
                    periodo: label,
                    total_art: 0,
                    total_monto: 0,
                    vendedores: []
                });
            }

            const pObj = periodsMap.get(label);
            const cVen = (row.co_ven || '').trim();
            const venRank = rankingWithPct.find(r => r.co_ven === cVen);
            const venName = venRank ? venRank.ven_des : cVen;
            const cant = Number(row.cant_vendida) || 0;
            const monto = Number(row.monto_vendido) || 0;

            pObj.total_art += cant;
            pObj.total_monto += monto;
            pObj.vendedores.push({
                co_ven: cVen,
                ven_des: venName,
                cant,
                monto
            });
        });

        const timeline = Array.from(periodsMap.values());

        res.json({
            success: true,
            server: sede,
            startDate,
            endDate,
            tipoAgrupacion,
            article: articleInfo,
            totals: {
                total_art_vendidos: totalArtVendidos,
                total_monto_vendidos: totalMontoVendidos,
                vendedores_count: rankingWithPct.length
            },
            ranking: rankingWithPct,
            timeline
        });

    } catch (error) {
        console.error(`[GET /analisis-ventas/article-vendors]`, error);
        res.status(500).json({ success: false, message: 'Error consultando vendedores del artículo.', error: error.message });
    }
});

module.exports = router;
