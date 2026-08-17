const express = require('express');
const router = express.Router();
const fs = require('fs');
const path = require('path');
const { getPool, getServers } = require('../db');

// Función auxiliar para calcular días hábiles (excluye domingos y feriados fijos VE)
function getBusinessDays(startDateStr, endDateStr) {
    const start = new Date(startDateStr);
    const end = new Date(endDateStr);
    
    // Si la fecha es inválida, fallback a 30 días
    if (isNaN(start) || isNaN(end)) return 30;
    if (start > end) return 1;

    // Feriados fijos de Venezuela (Mes 0-11, Día 1-31)
    const fixedHolidays = [
        '01-01', // Año nuevo
        '04-19', // Declaración Independencia
        '05-01', // Día del trabajador
        '06-24', // Batalla de Carabobo
        '07-05', // Independencia
        '07-24', // Natalicio de Bolívar
        '10-12', // Día de la Resistencia Indígena
        '12-24', // Nochebuena
        '12-25', // Navidad
        '12-31'  // Fin de año
    ];

    let count = 0;
    const cur = new Date(start);
    while (cur <= end) {
        const dayOfWeek = cur.getDay(); // 0 = Domingo
        const mmdd = String(cur.getMonth() + 1).padStart(2, '0') + '-' + String(cur.getDate()).padStart(2, '0');
        
        // Contar si NO es domingo y NO es feriado fijo
        if (dayOfWeek !== 0 && !fixedHolidays.includes(mmdd)) {
            count++;
        }
        cur.setDate(cur.getDate() + 1);
    }
    
    return count > 0 ? count : 1; // Evitar divisiones por cero
}

/**
 * @swagger
 * /api/v1/analisis-compras:
 *   get:
 *     summary: Obtiene el análisis dinámico de compras e inventario (ABC/XYZ, ROP, SS)
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
 */
router.get('/', async (req, res) => {
    try {
        let sede = req.query.sede || 'default';
        const startDate = req.query.startDate || new Date(new Date().setDate(new Date().getDate() - 90)).toISOString().split('T')[0];
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

        // Query con CTEs inline - no depende de una vista pre-instalada
        const query = `
            ;WITH v_compras_inventario AS (
                SELECT 'VENTA' AS tipo_transaccion, f.fec_emis AS fecha, r.co_art, r.total_art AS cantidad, 0 AS dias_reposicion, 0 AS stock_actual, 0 AS en_transito
                FROM saFacturaVenta f JOIN saFacturaVentaReng r ON f.doc_num = r.doc_num WHERE f.anulado = 0
                UNION ALL
                SELECT 'DEVOLUCION', d.fec_emis, r.co_art, r.total_art, 0, 0, 0
                FROM saDevolucionCliente d JOIN saDevolucionClienteReng r ON d.doc_num = r.doc_num WHERE d.anulado = 0
                UNION ALL
                SELECT 'RECEPCION', nr.fec_emis, nrr.co_art, nrr.total_art, ISNULL(DATEDIFF(day, oc.fec_emis, nr.fec_emis), 0), 0, 0
                FROM saNotaRecepcionCompra nr JOIN saNotaRecepcionCompraReng nrr ON nr.doc_num = nrr.doc_num
                LEFT JOIN saOrdenCompra oc ON nrr.num_doc = oc.doc_num AND oc.anulado = 0
                WHERE nr.anulado = 0
                UNION ALL
                SELECT 'STOCK', GETDATE(), co_art, 0, 0, SUM(ISNULL(stock, 0)), 0
                FROM saStockAlmacen GROUP BY co_art
                UNION ALL
                SELECT 'TRANSITO', GETDATE(), r.co_art, 0, 0, 0, SUM(ISNULL(r.pendiente, 0))
                FROM saOrdenCompra c JOIN saOrdenCompraReng r ON c.doc_num = r.doc_num
                WHERE c.anulado = 0 AND c.status IN ('0', '1') AND r.pendiente > 0
                GROUP BY r.co_art
            )
            SELECT 
                a.co_art,
                MAX(a.art_des) as des_art,
                MAX(a.co_lin) as co_lin,
                MAX(a.co_cat) as co_cat,
                RTRIM(COALESCE(MAX(aun.co_uni), '01')) as co_uni,
                RTRIM(COALESCE(MAX(aun.des_uni), MAX(aun.co_uni), 'UND')) as des_uni,
                COALESCE(MAX(fact.cost_unit_om), MAX(rec.cost_unit_om), MAX(p2.monto) / 1.30, 0) as costo_actual,
                SUM(CASE WHEN v.tipo_transaccion = 'STOCK' THEN v.stock_actual ELSE 0 END) as stock_almacen,
                SUM(CASE WHEN v.tipo_transaccion = 'TRANSITO' THEN v.en_transito ELSE 0 END) as en_transito,
                SUM(CASE WHEN v.tipo_transaccion = 'VENTA' AND v.fecha >= @start AND v.fecha <= @end THEN v.cantidad ELSE 0 END) 
                - SUM(CASE WHEN v.tipo_transaccion = 'DEVOLUCION' AND v.fecha >= @start AND v.fecha <= @end THEN v.cantidad ELSE 0 END) as ventas_netas,
                AVG(CASE WHEN v.tipo_transaccion = 'RECEPCION' AND v.fecha >= @start AND v.fecha <= @end AND v.dias_reposicion > 0 THEN (v.dias_reposicion * 1.0) ELSE NULL END) as tiempo_reposicion_promedio,
                STDEV(CASE WHEN v.tipo_transaccion = 'VENTA' AND v.fecha >= @start AND v.fecha <= @end THEN v.cantidad ELSE NULL END) as desviacion_ventas
            FROM saArticulo a
            LEFT JOIN v_compras_inventario v ON a.co_art = v.co_art
            LEFT JOIN (
                SELECT au.co_art, au.co_uni, u.des_uni,
                       ROW_NUMBER() OVER(PARTITION BY au.co_art ORDER BY au.uni_principal DESC) as rn
                FROM saArtUnidad au
                LEFT JOIN saUnidad u ON LTRIM(RTRIM(au.co_uni)) = LTRIM(RTRIM(u.co_uni))
            ) aun ON a.co_art = aun.co_art AND aun.rn = 1
            OUTER APPLY (
                SELECT TOP 1 
                    CASE 
                        WHEN RTRIM(n.co_mone) = 'BS' THEN (r.cost_unit / NULLIF((SELECT TOP 1 tasa_v FROM saTasa WHERE (co_mone LIKE 'US%') AND fecha <= n.fec_emis ORDER BY fecha DESC), 0)) 
                    ELSE r.cost_unit_om 
                END AS cost_unit_om
                FROM saFacturaCompraReng r INNER JOIN saFacturaCompra n ON r.doc_num = n.doc_num
                WHERE r.co_art = a.co_art AND n.anulado = 0
                ORDER BY n.fec_emis DESC
            ) fact
            OUTER APPLY (
                SELECT TOP 1 
                    CASE 
                        WHEN RTRIM(n.co_mone) = 'BS' THEN (r.cost_unit / NULLIF((SELECT TOP 1 tasa_v FROM saTasa WHERE (co_mone LIKE 'US%') AND fecha <= n.fec_emis ORDER BY fecha DESC), 0)) 
                    ELSE r.cost_unit_om 
                END AS cost_unit_om
                FROM saNotaRecepcionCompraReng r INNER JOIN saNotaRecepcionCompra n ON r.doc_num = n.doc_num
                WHERE r.co_art = a.co_art AND n.anulado = 0
                ORDER BY n.fec_emis DESC
            ) rec
            OUTER APPLY (
                SELECT TOP 1 monto FROM saArtPrecio 
                WHERE co_art = a.co_art AND (LTRIM(RTRIM(co_precio)) = '02' OR LTRIM(RTRIM(co_precio)) = '2')
                AND Inactivo = 0 AND GETDATE() >= desde AND (hasta IS NULL OR GETDATE() <= hasta)
                ORDER BY desde DESC
            ) p2
            WHERE a.anulado = 0 AND a.co_art NOT LIKE '09%'
            GROUP BY a.co_art
            HAVING SUM(CASE WHEN v.tipo_transaccion = 'STOCK' THEN v.stock_actual ELSE 0 END) > 0 
                OR (SUM(CASE WHEN v.tipo_transaccion = 'VENTA' AND v.fecha >= @start AND v.fecha <= @end THEN v.cantidad ELSE 0 END) > 0)
        `;

        const request = pool.request();
        request.input('start', startDate);
        request.input('end', endDate + ' 23:59:59');
        
        const result = await request.query(query);
        let items = result.recordset;

        // Unidades configuradas dinámicamente o por reglas semánticas
        const customUnitsParam = req.query.allow_decimals_units || '';
        const customUnits = customUnitsParam ? customUnitsParam.split(',').map(s => s.trim().toUpperCase()).filter(Boolean) : [];
        const fractionalCodes = ['06', '07', '08', '10', '25'];
        const fractionalKeywords = [
            'MTS2', 'MTS', 'LTS', 'KG', 'ML',
            'M2', 'M3', 'MT', 'LT', 'KGS', 'KILO', 'KILOS', 'KILOGRAMO', 'KILOGRAMOS',
            'GR', 'GRS', 'GRAMO', 'GRAMOS', 'METRO', 'METROS', 'LITRO', 'LITROS',
            'MILILITRO', 'MILILITROS', 'TON', 'TONELADA', 'CENTIMETRO', 'CM', 'MM', 'PULG', 'PULGADA', 'YARDA'
        ];
        function isFractionalUnit(co_uni, des_uni) {
            const code = String(co_uni || '').trim().toUpperCase();
            const desc = String(des_uni || '').trim().toUpperCase();
            if (customUnits.some(a => a === code || a === desc || desc.includes(a) || code.includes(a))) {
                return true;
            }
            return fractionalCodes.includes(code) || fractionalKeywords.includes(code) || fractionalKeywords.includes(desc);
        }

        // 1. Cálculos Base (VPD, TR, SDR, ROP, SS, Cant. Reponer)
        let totalSalesVal = 0;
        
        items = items.map(item => {
            const vpd = (item.ventas_netas > 0 ? item.ventas_netas : 0) / businessDays;
            // Si no hay datos historicos de TR, asumimos 15 días promedio
            const tr = item.tiempo_reposicion_promedio || 15; 
            const isFrac = isFractionalUnit(item.co_uni, item.des_uni);
            const demandaTR = vpd * tr;
            
            // Normalización de la Desviación Estándar de Demanda Diaria (evita distorsión por facturas al mayor esporádicas)
            let stdDev = item.desviacion_ventas || (vpd * 0.5);
            if (stdDev > (vpd * 1.2)) {
                stdDev = vpd * 1.2;
            }
            if (stdDev < (vpd * 0.2)) {
                stdDev = vpd * 0.2;
            }

            // Safety Stock (SS): factor Z (1.65 para 95% de confianza) * Desviación Diaria * sqrt(TR)
            let ss = 0;
            if (vpd > 0) {
                const rawSS = 1.65 * stdDev * Math.sqrt(tr);
                ss = isFrac ? Number(rawSS.toFixed(2)) : Math.round(rawSS);
            }
            
            // Reorder Point (ROP): (VPD * TR) + SS
            let rop = 0;
            if (vpd > 0) {
                const rawROP = demandaTR + ss;
                if (isFrac) {
                    rop = Number(rawROP.toFixed(2));
                } else {
                    // Para unidades discretas/enteras, el ROP mínimo es 1 unidad o el techo de la demanda en tiempo de reposición + SS
                    rop = Math.max(1, Math.ceil(rawROP));
                }
            }
            
            // Stock Disponible Real (SDR) - solo inventario físico en almacén
            const sdr = (item.stock_almacen || 0);

            // Cantidad sugerida a pedir
            let cantSugerida = 0;
            if (vpd > 0 || (item.ventas_netas || 0) > 0) {
                if (sdr <= rop || sdr <= 0) {
                    const diff = Math.max(rop + ss, rop) - sdr;
                    cantSugerida = isFrac ? Math.max(0.01, Number(diff.toFixed(2))) : Math.max(1, Math.ceil(diff));
                } else if (sdr < (rop + ss)) {
                    const diff = (rop + ss) - sdr;
                    cantSugerida = isFrac ? Number(diff.toFixed(2)) : Math.ceil(diff);
                }
            }
            
            // Valor de ventas para Pareto ABC
            const valorVentas = (item.ventas_netas > 0 ? item.ventas_netas : 0) * (item.costo_actual || 1);
            totalSalesVal += valorVentas;

            // Factor XYZ (Coeficiente de variación)
            const cv = vpd > 0 ? (stdDev / vpd) : 100;

            return {
                ...item,
                is_frac: isFrac,
                vpd,
                tr,
                ss,
                rop,
                sdr,
                cant_sugerida: cantSugerida,
                valor_ventas: valorVentas,
                cv
            };
        });

        // 2. Clasificación ABC (Basada en Valor de Ventas acumulado)
        items.sort((a, b) => b.valor_ventas - a.valor_ventas);
        let cumulativeVal = 0;
        
        items = items.map(item => {
            cumulativeVal += item.valor_ventas;
            const percentage = totalSalesVal > 0 ? (cumulativeVal / totalSalesVal) : 0;
            
            let abc = 'C';
            if (percentage <= 0.80) abc = 'A';
            else if (percentage <= 0.95) abc = 'B';
            
            let xyz = 'Z';
            if (item.cv <= 0.20) xyz = 'X'; // Muy predecible
            else if (item.cv <= 0.60) xyz = 'Y'; // Regular
            
            return {
                ...item,
                clasificacion_abc: abc,
                clasificacion_xyz: xyz,
                clase_conjunta: abc + xyz
            };
        });

        // 3. Ordenar por URGENCIA de Compra
        // Tier 1: SDR <= 0 y ventas > 0 (Quiebre total de stock) o SDR <= ROP
        // Tier 2: SDR <= ROP + SS (Cerca de ruptura - Amarillo)
        // Tier 3: SDR > ROP + SS (Sano / Sobre-stock - Verde)
        const classPriorityMap = {
            'AX': 1, 'AY': 2, 'AZ': 3,
            'BX': 4, 'BY': 5, 'BZ': 6,
            'CX': 7, 'CY': 8, 'CZ': 9
        };

        items.sort((a, b) => {
            // Criterio 1: Nivel de Ruptura (1: Rojo/Quiebre, 2: Amarillo, 3: Verde)
            const aTier = (a.sdr <= a.rop || (a.sdr <= 0 && a.vpd > 0)) ? 1 : (a.sdr <= a.rop + a.ss ? 2 : 3);
            const bTier = (b.sdr <= b.rop || (b.sdr <= 0 && b.vpd > 0)) ? 1 : (b.sdr <= b.rop + b.ss ? 2 : 3);
            if (aTier !== bTier) return aTier - bTier;

            // Criterio 2: Importancia de la clase (AX primero, CZ al final)
            const aPrio = classPriorityMap[a.clase_conjunta] || 99;
            const bPrio = classPriorityMap[b.clase_conjunta] || 99;
            if (aPrio !== bPrio) return aPrio - bPrio;

            // Criterio 3: Mayor déficit de capital
            const aDeficit = (a.cant_sugerida || 0) * (a.costo_actual || 1);
            const bDeficit = (b.cant_sugerida || 0) * (b.costo_actual || 1);
            return bDeficit - aDeficit;
        });

        // 3. KPIs de Resumen
        let capitalInmovilizado = 0;
        let capitalRequerido = 0;
        let alertasSDR = 0;

        items.forEach(item => {
            // Capital inmovilizado: Exceso sobre ROP + SS
            if (item.sdr > (item.rop + item.ss)) {
                capitalInmovilizado += (item.sdr - (item.rop + item.ss)) * (item.costo_actual || 0);
            }
            
            // Alerta si el SDR <= ROP o si SDR <= 0 con demanda
            if (item.sdr <= item.rop || (item.sdr <= 0 && item.vpd > 0)) {
                alertasSDR++;
                capitalRequerido += (item.cant_sugerida || 0) * (item.costo_actual || 0);
            }
        });

        res.json({
            success: true,
            server: sede,
            businessDays,
            kpis: {
                capital_inmovilizado: capitalInmovilizado,
                capital_requerido_urgente: capitalRequerido,
                articulos_en_alerta: alertasSDR
            },
            data: items
        });

    } catch (error) {
        console.error(`[GET /analisis-compras]`, error);
        res.status(500).json({ success: false, message: 'Error en el análisis de compras.', error: error.message });
    }
});

/**
 * @swagger
 * /api/v1/analisis-compras/article-history:
 *   get:
 *     summary: Obtiene el histórico mensual de ventas reales (Facturado - Devoluciones) de los últimos 12 meses para un artículo
 *     tags: [Reportes]
 *     parameters:
 *       - in: query
 *         name: co_art
 *         required: true
 *         schema: { type: string }
 *       - in: query
 *         name: sede
 *         schema: { type: string }
 */
router.get('/article-history', async (req, res) => {
    try {
        const co_art = (req.query.co_art || '').trim();
        if (!co_art) {
            return res.status(400).json({ success: false, message: 'El parámetro co_art es obligatorio.' });
        }

        let sede = req.query.sede || 'default';
        let startDate = req.query.startDate;
        let endDate = req.query.endDate;

        if (!startDate || !endDate) {
            const end = new Date();
            const start = new Date();
            start.setDate(end.getDate() - 365);
            startDate = start.toISOString().split('T')[0];
            endDate = end.toISOString().split('T')[0];
        }

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
        // 1. Rango <= días del mes de startDate (+1) -> diario
        // 2. Rango <= 93 días (3 meses / 90d) -> semanal
        // 3. Rango > 93 días -> mensual
        let tipoAgrupacion = 'mensual';
        if (diffDays <= (daysInStartMonth + 1)) {
            tipoAgrupacion = 'diario';
        } else if (diffDays <= 93) {
            tipoAgrupacion = 'semanal';
        } else {
            tipoAgrupacion = 'mensual';
        }

        let query = '';
        if (tipoAgrupacion === 'diario') {
            query = `
                ;WITH CurrentStock AS (
                    SELECT ISNULL(SUM(stock), 0) AS stock_actual
                    FROM saStockAlmacen
                    WHERE co_art = @co_art
                ),
                Movimientos AS (
                    SELECT CAST(f.fec_emis AS DATE) AS fecha, f.doc_num, r.total_art AS qty, 'factura' AS tipo
                    FROM saFacturaVentaReng r
                    INNER JOIN saFacturaVenta f ON r.doc_num = f.doc_num
                    WHERE r.co_art = @co_art AND f.anulado = 0 AND f.fec_emis >= @start AND f.fec_emis <= @end
                    UNION ALL
                    SELECT CAST(c.fec_emis AS DATE) AS fecha, c.doc_num, d.total_art AS qty, 'devolucion' AS tipo
                    FROM saDevolucionClienteReng d
                    INNER JOIN saDevolucionCliente c ON d.doc_num = c.doc_num
                    WHERE d.co_art = @co_art AND c.anulado = 0 AND c.fec_emis >= @start AND c.fec_emis <= @end
                    UNION ALL
                    SELECT CAST(nr.fec_emis AS DATE) AS fecha, nr.doc_num, nr_r.total_art AS qty, 'recepcion' AS tipo
                    FROM saNotaRecepcionCompraReng nr_r
                    INNER JOIN saNotaRecepcionCompra nr ON nr_r.doc_num = nr.doc_num
                    WHERE nr_r.co_art = @co_art AND nr.anulado = 0 AND nr.fec_emis >= @start AND nr.fec_emis <= @end
                    UNION ALL
                    SELECT CAST(a.fecha AS DATE) AS fecha, a.ajue_num AS doc_num, ar.total_art AS qty,
                           CASE WHEN ar.co_tipo = '01' OR ta.tipo_trans = '0' OR ar.co_tipo LIKE '%ENT%' OR ar.co_tipo LIKE '%IN%' THEN 'ajuste_entrada' ELSE 'ajuste_salida' END AS tipo
                    FROM saAjusteReng ar
                    INNER JOIN saAjuste a ON ar.ajue_num = a.ajue_num
                    LEFT JOIN saTipoAjuste ta ON ar.co_tipo = ta.co_tipo
                    WHERE ar.co_art = @co_art AND a.anulado = 0 AND a.fecha >= @start AND a.fecha <= @end
                ),
                Periodos AS (
                    SELECT 
                        fecha AS periodo_inicio,
                        DATEADD(day, 1, fecha) AS periodo_fin,
                        DAY(fecha) AS dia,
                        MONTH(fecha) AS mes,
                        YEAR(fecha) AS anio,
                        CONVERT(VARCHAR(10), fecha, 120) AS periodo_str,
                        SUM(CASE WHEN tipo = 'factura' THEN qty ELSE 0 END) AS cant_facturada,
                        SUM(CASE WHEN tipo = 'devolucion' THEN qty ELSE 0 END) AS cant_devuelta,
                        COUNT(DISTINCT CASE WHEN tipo = 'factura' THEN doc_num ELSE NULL END) AS docs_facturados,
                        COUNT(DISTINCT CASE WHEN tipo = 'devolucion' THEN doc_num ELSE NULL END) AS docs_devueltos,
                        SUM(CASE WHEN tipo = 'recepcion' THEN qty ELSE 0 END) AS cant_recepcionada,
                        COUNT(DISTINCT CASE WHEN tipo = 'recepcion' THEN doc_num ELSE NULL END) AS docs_recepcion,
                        SUM(CASE WHEN tipo = 'ajuste_entrada' THEN qty ELSE 0 END) AS cant_ajuste_entrada,
                        SUM(CASE WHEN tipo = 'ajuste_salida' THEN qty ELSE 0 END) AS cant_ajuste_salida
                    FROM Movimientos
                    GROUP BY fecha
                )
                SELECT 
                    p.*,
                    (p.cant_facturada - p.cant_devuelta) AS cant_real_vendida,
                    (p.docs_facturados - p.docs_devueltos) AS docs_exitosos,
                    cs.stock_actual,
                    (cs.stock_actual - ISNULL(entradas_post.qty, 0) + ISNULL(salidas_post.qty, 0)) AS stock_inicial_calculado
                FROM Periodos p
                CROSS JOIN CurrentStock cs
                OUTER APPLY (
                    SELECT (
                        ISNULL((SELECT SUM(r.total_art) FROM saNotaRecepcionCompraReng r INNER JOIN saNotaRecepcionCompra nr ON r.doc_num = nr.doc_num WHERE r.co_art = @co_art AND nr.anulado = 0 AND nr.fec_emis >= p.periodo_inicio), 0) +
                        ISNULL((SELECT SUM(r.total_art) FROM saDevolucionClienteReng r INNER JOIN saDevolucionCliente dc ON r.doc_num = dc.doc_num WHERE r.co_art = @co_art AND dc.anulado = 0 AND dc.fec_emis >= p.periodo_inicio), 0) +
                        ISNULL((SELECT SUM(r.total_art) FROM saAjusteReng r INNER JOIN saAjuste a ON r.ajue_num = a.ajue_num LEFT JOIN saTipoAjuste ta ON r.co_tipo = ta.co_tipo WHERE r.co_art = @co_art AND a.anulado = 0 AND (r.co_tipo = '01' OR ta.tipo_trans = '0' OR r.co_tipo LIKE '%ENT%' OR r.co_tipo LIKE '%IN%') AND a.fecha >= p.periodo_inicio), 0)
                    ) AS qty
                ) entradas_post
                OUTER APPLY (
                    SELECT (
                        ISNULL((SELECT SUM(r.total_art) FROM saFacturaVentaReng r INNER JOIN saFacturaVenta fv ON r.doc_num = fv.doc_num WHERE r.co_art = @co_art AND fv.anulado = 0 AND fv.fec_emis >= p.periodo_inicio), 0) +
                        ISNULL((SELECT SUM(r.total_art) FROM saDevolucionProveedorReng r INNER JOIN saDevolucionProveedor dp ON r.doc_num = dp.doc_num WHERE r.co_art = @co_art AND dp.anulado = 0 AND dp.fec_emis >= p.periodo_inicio), 0) +
                        ISNULL((SELECT SUM(r.total_art) FROM saAjusteReng r INNER JOIN saAjuste a ON r.ajue_num = a.ajue_num LEFT JOIN saTipoAjuste ta ON r.co_tipo = ta.co_tipo WHERE r.co_art = @co_art AND a.anulado = 0 AND (r.co_tipo = '02' OR ta.tipo_trans = '1' OR r.co_tipo LIKE '%SAL%' OR r.co_tipo LIKE '%OUT%') AND a.fecha >= p.periodo_inicio), 0)
                    ) AS qty
                ) salidas_post
                ORDER BY p.periodo_inicio ASC
            `;
        } else if (tipoAgrupacion === 'semanal') {
            query = `
                ;WITH CurrentStock AS (
                    SELECT ISNULL(SUM(stock), 0) AS stock_actual
                    FROM saStockAlmacen
                    WHERE co_art = @co_art
                ),
                Movimientos AS (
                    SELECT CAST(f.fec_emis AS DATE) AS fecha, f.doc_num, r.total_art AS qty, 'factura' AS tipo
                    FROM saFacturaVentaReng r
                    INNER JOIN saFacturaVenta f ON r.doc_num = f.doc_num
                    WHERE r.co_art = @co_art AND f.anulado = 0 AND f.fec_emis >= @start AND f.fec_emis <= @end
                    UNION ALL
                    SELECT CAST(c.fec_emis AS DATE) AS fecha, c.doc_num, d.total_art AS qty, 'devolucion' AS tipo
                    FROM saDevolucionClienteReng d
                    INNER JOIN saDevolucionCliente c ON d.doc_num = c.doc_num
                    WHERE d.co_art = @co_art AND c.anulado = 0 AND c.fec_emis >= @start AND c.fec_emis <= @end
                    UNION ALL
                    SELECT CAST(nr.fec_emis AS DATE) AS fecha, nr.doc_num, nr_r.total_art AS qty, 'recepcion' AS tipo
                    FROM saNotaRecepcionCompraReng nr_r
                    INNER JOIN saNotaRecepcionCompra nr ON nr_r.doc_num = nr.doc_num
                    WHERE nr_r.co_art = @co_art AND nr.anulado = 0 AND nr.fec_emis >= @start AND nr.fec_emis <= @end
                    UNION ALL
                    SELECT CAST(a.fecha AS DATE) AS fecha, a.ajue_num AS doc_num, ar.total_art AS qty,
                           CASE WHEN ar.co_tipo = '01' OR ta.tipo_trans = '0' OR ar.co_tipo LIKE '%ENT%' OR ar.co_tipo LIKE '%IN%' THEN 'ajuste_entrada' ELSE 'ajuste_salida' END AS tipo
                    FROM saAjusteReng ar
                    INNER JOIN saAjuste a ON ar.ajue_num = a.ajue_num
                    LEFT JOIN saTipoAjuste ta ON ar.co_tipo = ta.co_tipo
                    WHERE ar.co_art = @co_art AND a.anulado = 0 AND a.fecha >= @start AND a.fecha <= @end
                ),
                DocSemana AS (
                    SELECT 
                        DATEADD(day, - ((DATEPART(weekday, fecha) + @@DATEFIRST - 2) % 7), fecha) AS semana_inicio,
                        doc_num,
                        qty,
                        tipo
                    FROM Movimientos
                ),
                Periodos AS (
                    SELECT 
                        semana_inicio AS periodo_inicio,
                        DATEADD(day, 6, semana_inicio) AS periodo_fin,
                        DAY(semana_inicio) AS dia_inicio,
                        MONTH(semana_inicio) AS mes_inicio,
                        YEAR(semana_inicio) AS anio_inicio,
                        DAY(DATEADD(day, 6, semana_inicio)) AS dia_fin,
                        MONTH(DATEADD(day, 6, semana_inicio)) AS mes_fin,
                        YEAR(DATEADD(day, 6, semana_inicio)) AS anio_fin,
                        CONVERT(VARCHAR(10), semana_inicio, 120) AS periodo_str,
                        SUM(CASE WHEN tipo = 'factura' THEN qty ELSE 0 END) AS cant_facturada,
                        SUM(CASE WHEN tipo = 'devolucion' THEN qty ELSE 0 END) AS cant_devuelta,
                        COUNT(DISTINCT CASE WHEN tipo = 'factura' THEN doc_num ELSE NULL END) AS docs_facturados,
                        COUNT(DISTINCT CASE WHEN tipo = 'devolucion' THEN doc_num ELSE NULL END) AS docs_devueltos,
                        SUM(CASE WHEN tipo = 'recepcion' THEN qty ELSE 0 END) AS cant_recepcionada,
                        COUNT(DISTINCT CASE WHEN tipo = 'recepcion' THEN doc_num ELSE NULL END) AS docs_recepcion,
                        SUM(CASE WHEN tipo = 'ajuste_entrada' THEN qty ELSE 0 END) AS cant_ajuste_entrada,
                        SUM(CASE WHEN tipo = 'ajuste_salida' THEN qty ELSE 0 END) AS cant_ajuste_salida
                    FROM DocSemana
                    GROUP BY semana_inicio
                )
                SELECT 
                    p.*,
                    (p.cant_facturada - p.cant_devuelta) AS cant_real_vendida,
                    (p.docs_facturados - p.docs_devueltos) AS docs_exitosos,
                    cs.stock_actual,
                    (cs.stock_actual - ISNULL(entradas_post.qty, 0) + ISNULL(salidas_post.qty, 0)) AS stock_inicial_calculado
                FROM Periodos p
                CROSS JOIN CurrentStock cs
                OUTER APPLY (
                    SELECT (
                        ISNULL((SELECT SUM(r.total_art) FROM saNotaRecepcionCompraReng r INNER JOIN saNotaRecepcionCompra nr ON r.doc_num = nr.doc_num WHERE r.co_art = @co_art AND nr.anulado = 0 AND nr.fec_emis >= p.periodo_inicio), 0) +
                        ISNULL((SELECT SUM(r.total_art) FROM saDevolucionClienteReng r INNER JOIN saDevolucionCliente dc ON r.doc_num = dc.doc_num WHERE r.co_art = @co_art AND dc.anulado = 0 AND dc.fec_emis >= p.periodo_inicio), 0) +
                        ISNULL((SELECT SUM(r.total_art) FROM saAjusteReng r INNER JOIN saAjuste a ON r.ajue_num = a.ajue_num LEFT JOIN saTipoAjuste ta ON r.co_tipo = ta.co_tipo WHERE r.co_art = @co_art AND a.anulado = 0 AND (r.co_tipo = '01' OR ta.tipo_trans = '0' OR r.co_tipo LIKE '%ENT%' OR r.co_tipo LIKE '%IN%') AND a.fecha >= p.periodo_inicio), 0)
                    ) AS qty
                ) entradas_post
                OUTER APPLY (
                    SELECT (
                        ISNULL((SELECT SUM(r.total_art) FROM saFacturaVentaReng r INNER JOIN saFacturaVenta fv ON r.doc_num = fv.doc_num WHERE r.co_art = @co_art AND fv.anulado = 0 AND fv.fec_emis >= p.periodo_inicio), 0) +
                        ISNULL((SELECT SUM(r.total_art) FROM saDevolucionProveedorReng r INNER JOIN saDevolucionProveedor dp ON r.doc_num = dp.doc_num WHERE r.co_art = @co_art AND dp.anulado = 0 AND dp.fec_emis >= p.periodo_inicio), 0) +
                        ISNULL((SELECT SUM(r.total_art) FROM saAjusteReng r INNER JOIN saAjuste a ON r.ajue_num = a.ajue_num LEFT JOIN saTipoAjuste ta ON r.co_tipo = ta.co_tipo WHERE r.co_art = @co_art AND a.anulado = 0 AND (r.co_tipo = '02' OR ta.tipo_trans = '1' OR r.co_tipo LIKE '%SAL%' OR r.co_tipo LIKE '%OUT%') AND a.fecha >= p.periodo_inicio), 0)
                    ) AS qty
                ) salidas_post
                ORDER BY p.periodo_inicio ASC
            `;
        } else {
            // Mensual
            query = `
                ;WITH CurrentStock AS (
                    SELECT ISNULL(SUM(stock), 0) AS stock_actual
                    FROM saStockAlmacen
                    WHERE co_art = @co_art
                ),
                Movimientos AS (
                    SELECT CAST(f.fec_emis AS DATE) AS fecha, f.doc_num, r.total_art AS qty, 'factura' AS tipo
                    FROM saFacturaVentaReng r
                    INNER JOIN saFacturaVenta f ON r.doc_num = f.doc_num
                    WHERE r.co_art = @co_art AND f.anulado = 0 AND f.fec_emis >= @start AND f.fec_emis <= @end
                    UNION ALL
                    SELECT CAST(c.fec_emis AS DATE) AS fecha, c.doc_num, d.total_art AS qty, 'devolucion' AS tipo
                    FROM saDevolucionClienteReng d
                    INNER JOIN saDevolucionCliente c ON d.doc_num = c.doc_num
                    WHERE d.co_art = @co_art AND c.anulado = 0 AND c.fec_emis >= @start AND c.fec_emis <= @end
                    UNION ALL
                    SELECT CAST(nr.fec_emis AS DATE) AS fecha, nr.doc_num, nr_r.total_art AS qty, 'recepcion' AS tipo
                    FROM saNotaRecepcionCompraReng nr_r
                    INNER JOIN saNotaRecepcionCompra nr ON nr_r.doc_num = nr.doc_num
                    WHERE nr_r.co_art = @co_art AND nr.anulado = 0 AND nr.fec_emis >= @start AND nr.fec_emis <= @end
                    UNION ALL
                    SELECT CAST(a.fecha AS DATE) AS fecha, a.ajue_num AS doc_num, ar.total_art AS qty,
                           CASE WHEN ar.co_tipo = '01' OR ta.tipo_trans = '0' OR ar.co_tipo LIKE '%ENT%' OR ar.co_tipo LIKE '%IN%' THEN 'ajuste_entrada' ELSE 'ajuste_salida' END AS tipo
                    FROM saAjusteReng ar
                    INNER JOIN saAjuste a ON ar.ajue_num = a.ajue_num
                    LEFT JOIN saTipoAjuste ta ON ar.co_tipo = ta.co_tipo
                    WHERE ar.co_art = @co_art AND a.anulado = 0 AND a.fecha >= @start AND a.fecha <= @end
                ),
                Periodos AS (
                    SELECT 
                        DATEFROMPARTS(YEAR(fecha), MONTH(fecha), 1) AS periodo_inicio,
                        DATEADD(month, 1, DATEFROMPARTS(YEAR(fecha), MONTH(fecha), 1)) AS periodo_fin,
                        YEAR(fecha) AS anio,
                        MONTH(fecha) AS mes,
                        CONVERT(VARCHAR(7), DATEFROMPARTS(YEAR(fecha), MONTH(fecha), 1), 120) AS periodo_str,
                        SUM(CASE WHEN tipo = 'factura' THEN qty ELSE 0 END) AS cant_facturada,
                        SUM(CASE WHEN tipo = 'devolucion' THEN qty ELSE 0 END) AS cant_devuelta,
                        COUNT(DISTINCT CASE WHEN tipo = 'factura' THEN doc_num ELSE NULL END) AS docs_facturados,
                        COUNT(DISTINCT CASE WHEN tipo = 'devolucion' THEN doc_num ELSE NULL END) AS docs_devueltos,
                        SUM(CASE WHEN tipo = 'recepcion' THEN qty ELSE 0 END) AS cant_recepcionada,
                        COUNT(DISTINCT CASE WHEN tipo = 'recepcion' THEN doc_num ELSE NULL END) AS docs_recepcion,
                        SUM(CASE WHEN tipo = 'ajuste_entrada' THEN qty ELSE 0 END) AS cant_ajuste_entrada,
                        SUM(CASE WHEN tipo = 'ajuste_salida' THEN qty ELSE 0 END) AS cant_ajuste_salida
                    FROM Movimientos
                    GROUP BY YEAR(fecha), MONTH(fecha), DATEFROMPARTS(YEAR(fecha), MONTH(fecha), 1)
                )
                SELECT 
                    p.*,
                    (p.cant_facturada - p.cant_devuelta) AS cant_real_vendida,
                    (p.docs_facturados - p.docs_devueltos) AS docs_exitosos,
                    cs.stock_actual,
                    (cs.stock_actual - ISNULL(entradas_post.qty, 0) + ISNULL(salidas_post.qty, 0)) AS stock_inicial_calculado
                FROM Periodos p
                CROSS JOIN CurrentStock cs
                OUTER APPLY (
                    SELECT (
                        ISNULL((SELECT SUM(r.total_art) FROM saNotaRecepcionCompraReng r INNER JOIN saNotaRecepcionCompra nr ON r.doc_num = nr.doc_num WHERE r.co_art = @co_art AND nr.anulado = 0 AND nr.fec_emis >= p.periodo_inicio), 0) +
                        ISNULL((SELECT SUM(r.total_art) FROM saDevolucionClienteReng r INNER JOIN saDevolucionCliente dc ON r.doc_num = dc.doc_num WHERE r.co_art = @co_art AND dc.anulado = 0 AND dc.fec_emis >= p.periodo_inicio), 0) +
                        ISNULL((SELECT SUM(r.total_art) FROM saAjusteReng r INNER JOIN saAjuste a ON r.ajue_num = a.ajue_num LEFT JOIN saTipoAjuste ta ON r.co_tipo = ta.co_tipo WHERE r.co_art = @co_art AND a.anulado = 0 AND (r.co_tipo = '01' OR ta.tipo_trans = '0' OR r.co_tipo LIKE '%ENT%' OR r.co_tipo LIKE '%IN%') AND a.fecha >= p.periodo_inicio), 0)
                    ) AS qty
                ) entradas_post
                OUTER APPLY (
                    SELECT (
                        ISNULL((SELECT SUM(r.total_art) FROM saFacturaVentaReng r INNER JOIN saFacturaVenta fv ON r.doc_num = fv.doc_num WHERE r.co_art = @co_art AND fv.anulado = 0 AND fv.fec_emis >= p.periodo_inicio), 0) +
                        ISNULL((SELECT SUM(r.total_art) FROM saDevolucionProveedorReng r INNER JOIN saDevolucionProveedor dp ON r.doc_num = dp.doc_num WHERE r.co_art = @co_art AND dp.anulado = 0 AND dp.fec_emis >= p.periodo_inicio), 0) +
                        ISNULL((SELECT SUM(r.total_art) FROM saAjusteReng r INNER JOIN saAjuste a ON r.ajue_num = a.ajue_num LEFT JOIN saTipoAjuste ta ON r.co_tipo = ta.co_tipo WHERE r.co_art = @co_art AND a.anulado = 0 AND (r.co_tipo = '02' OR ta.tipo_trans = '1' OR r.co_tipo LIKE '%SAL%' OR r.co_tipo LIKE '%OUT%') AND a.fecha >= p.periodo_inicio), 0)
                    ) AS qty
                ) salidas_post
                ORDER BY p.periodo_inicio ASC
            `;
        }

        const request = pool.request();
        request.input('co_art', co_art);
        request.input('start', startDate);
        request.input('end', endDate + ' 23:59:59');

        const result = await request.query(query);
        const monthNames = ['Ene', 'Feb', 'Mar', 'Abr', 'May', 'Jun', 'Jul', 'Ago', 'Sep', 'Oct', 'Nov', 'Dic'];

        const history = result.recordset.map(row => {
            let mes_nombre = '';
            if (tipoAgrupacion === 'diario') {
                const dia = String(row.dia).padStart(2, '0');
                const mes = monthNames[row.mes - 1];
                mes_nombre = `${dia} ${mes}`;
            } else if (tipoAgrupacion === 'semanal') {
                const dIni = String(row.dia_inicio).padStart(2, '0');
                const dFin = String(row.dia_fin).padStart(2, '0');
                if (row.mes_inicio === row.mes_fin) {
                    mes_nombre = `${dIni} - ${dFin} ${monthNames[row.mes_fin - 1]}`;
                } else {
                    mes_nombre = `${dIni} ${monthNames[row.mes_inicio - 1]} - ${dFin} ${monthNames[row.mes_fin - 1]}`;
                }
            } else {
                mes_nombre = `${monthNames[row.mes - 1]} ${row.anio}`;
            }

            return {
                anio: row.anio,
                mes: row.mes,
                periodo: row.periodo_str,
                mes_nombre,
                cant_facturada: Number(row.cant_facturada) || 0,
                cant_devuelta: Number(row.cant_devuelta) || 0,
                cant_real_vendida: Number(row.cant_real_vendida) || 0,
                docs_facturados: Number(row.docs_facturados) || 0,
                docs_devueltos: Number(row.docs_devueltos) || 0,
                docs_exitosos: Math.max(0, Number(row.docs_exitosos) || 0),
                cant_recepcionada: Number(row.cant_recepcionada) || 0,
                docs_recepcion: Number(row.docs_recepcion) || 0,
                cant_ajuste_entrada: Number(row.cant_ajuste_entrada) || 0,
                cant_ajuste_salida: Number(row.cant_ajuste_salida) || 0,
                stock_inicial: Math.max(0, Math.round(Number(row.stock_inicial_calculado) || 0))
            };
        });

        res.json({
            success: true,
            co_art,
            server: sede,
            tipoAgrupacion,
            history
        });
    } catch (error) {
        console.error(`[GET /analisis-compras/article-history]`, error);
        res.status(500).json({ success: false, message: 'Error consultando histórico del artículo.', error: error.message });
    }
});

/**
 * @swagger
 * /api/v1/analisis-compras/sync-view:
 *   post:
 *     summary: Instala o actualiza la vista v_compras_inventario en la base de datos SQL
 *     tags: [Reportes]
 */
router.post('/sync-view', async (req, res) => {
    try {
        let sede = req.query.sede || 'default';
        const servers = getServers();
        if (sede === 'default') {
            if (servers && servers.length > 0) {
                sede = servers[0].id;
            } else {
                return res.status(500).json({ success: false, message: 'No hay servidores SQL configurados.' });
            }
        }

        const pool = await getPool(sede, req.sqlAuth);
        
        const sqlPath = path.join(__dirname, '..', 'SQL', 'v_compras_inventario.sql');
        if (!fs.existsSync(sqlPath)) {
            return res.status(404).json({ success: false, message: 'El archivo SQL no se encontró en el servidor del agente.' });
        }

        // El script contiene "GO", el mssql driver a veces falla si mandas "GO" en un solo query.
        // Lo dividimos por la directiva GO
        const sqlContent = fs.readFileSync(sqlPath, 'utf8');
        const batches = sqlContent.split(/^GO/im);

        for (const batch of batches) {
            const cleanBatch = batch.trim();
            if (cleanBatch.length > 0) {
                await pool.request().query(cleanBatch);
            }
        }

        res.json({ success: true, message: 'Vista v_compras_inventario creada/actualizada exitosamente.' });
    } catch (error) {
        console.error(`[POST /analisis-compras/sync-view]`, error);
        res.status(500).json({ success: false, message: 'Error instalando la vista SQL.', error: error.message });
    }
});

module.exports = router;
