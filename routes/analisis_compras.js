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
                COALESCE(MAX(fact.cost_unit_om), MAX(rec.cost_unit_om), MAX(p2.monto) / 1.30, 0) as costo_actual,
                SUM(CASE WHEN v.tipo_transaccion = 'STOCK' THEN v.stock_actual ELSE 0 END) as stock_almacen,
                SUM(CASE WHEN v.tipo_transaccion = 'TRANSITO' THEN v.en_transito ELSE 0 END) as en_transito,
                SUM(CASE WHEN v.tipo_transaccion = 'VENTA' AND v.fecha >= @start AND v.fecha <= @end THEN v.cantidad ELSE 0 END) 
                - SUM(CASE WHEN v.tipo_transaccion = 'DEVOLUCION' AND v.fecha >= @start AND v.fecha <= @end THEN v.cantidad ELSE 0 END) as ventas_netas,
                AVG(CASE WHEN v.tipo_transaccion = 'RECEPCION' AND v.fecha >= @start AND v.fecha <= @end AND v.dias_reposicion > 0 THEN (v.dias_reposicion * 1.0) ELSE NULL END) as tiempo_reposicion_promedio,
                STDEV(CASE WHEN v.tipo_transaccion = 'VENTA' AND v.fecha >= @start AND v.fecha <= @end THEN v.cantidad ELSE NULL END) as desviacion_ventas
            FROM saArticulo a
            LEFT JOIN v_compras_inventario v ON a.co_art = v.co_art
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

        // 1. Cálculos Base (VPD, TR, SDR, ROP, SS)
        let totalSalesVal = 0;
        
        items = items.map(item => {
            const vpd = (item.ventas_netas > 0 ? item.ventas_netas : 0) / businessDays;
            // Si no hay datos historicos de TR, asumimos 15 días promedio
            const tr = item.tiempo_reposicion_promedio || 15; 
            
            // Normalización de la Desviación Estándar de Demanda Diaria (evita distorsión por facturas al mayor esporádicas)
            let stdDev = item.desviacion_ventas || (vpd * 0.5);
            if (stdDev > (vpd * 1.2)) {
                stdDev = vpd * 1.2;
            }
            if (stdDev < (vpd * 0.2)) {
                stdDev = vpd * 0.2;
            }

            // Safety Stock (SS): factor Z (1.65 para 95% de confianza) * Desviación Diaria * sqrt(TR)
            const ss = Math.round(1.65 * stdDev * Math.sqrt(tr));
            
            // Reorder Point (ROP): (VPD * TR) + SS
            const rop = Math.round((vpd * tr) + ss);
            
            // Stock Disponible Real (SDR) - solo inventario físico en almacén
            const sdr = (item.stock_almacen || 0);
            
            // Valor de ventas para Pareto ABC
            const valorVentas = (item.ventas_netas > 0 ? item.ventas_netas : 0) * (item.costo_actual || 1);
            totalSalesVal += valorVentas;

            // Factor XYZ (Coeficiente de variación)
            const cv = vpd > 0 ? (stdDev / vpd) : 100;

            return {
                ...item,
                vpd,
                tr,
                ss,
                rop,
                sdr,
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
        // Tier 1: SDR <= ROP (Ruptura inminente/actual - Rojo)
        // Tier 2: SDR <= ROP + SS (Cerca de ruptura - Amarillo)
        // Tier 3: SDR > ROP + SS (Sano / Sobre-stock - Verde)
        const classPriorityMap = {
            'AX': 1, 'AY': 2, 'AZ': 3,
            'BX': 4, 'BY': 5, 'BZ': 6,
            'CX': 7, 'CY': 8, 'CZ': 9
        };

        items.sort((a, b) => {
            // Criterio 1: Nivel de Ruptura (1: Rojo, 2: Amarillo, 3: Verde)
            const aTier = a.sdr <= a.rop ? 1 : (a.sdr <= a.rop + a.ss ? 2 : 3);
            const bTier = b.sdr <= b.rop ? 1 : (b.sdr <= b.rop + b.ss ? 2 : 3);
            if (aTier !== bTier) return aTier - bTier;

            // Criterio 2: Importancia de la clase (AX primero, CZ al final)
            const aPrio = classPriorityMap[a.clase_conjunta] || 99;
            const bPrio = classPriorityMap[b.clase_conjunta] || 99;
            if (aPrio !== bPrio) return aPrio - bPrio;

            // Criterio 3: Mayor déficit de capital (ROP - SDR) * costo
            const aDeficit = (a.rop - a.sdr) * (a.costo_actual || 1);
            const bDeficit = (b.rop - b.sdr) * (b.costo_actual || 1);
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
            
            // Alerta si el SDR <= ROP
            if (item.sdr <= item.rop) {
                alertasSDR++;
                const cantReponer = Math.max(0, (item.rop + item.ss) - item.sdr);
                capitalRequerido += cantReponer * (item.costo_actual || 0);
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
