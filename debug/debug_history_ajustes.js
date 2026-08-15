const path = require('path');
require('dotenv').config({ path: path.join(__dirname, '../.env') });
const { getPool, getServers, initServers } = require('../db');

async function run() {
    try {
        await initServers();
        const servers = getServers();
        const pool = await getPool(servers[0].id);
        
        const co_art = '0101001012'; // Alambron with recent adjustment
        const query = `
            ;WITH Numbers AS (
                SELECT 0 AS n
                UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3
                UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6
                UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9
                UNION ALL SELECT 10 UNION ALL SELECT 11
            ),
            Months AS (
                SELECT 
                    n,
                    DATEADD(month, -n, DATEADD(day, 1 - DAY(GETDATE()), CAST(GETDATE() AS DATE))) AS mes_inicio
                FROM Numbers
            ),
            CurrentStock AS (
                SELECT ISNULL(SUM(stock), 0) AS stock_actual
                FROM saStockAlmacen
                WHERE co_art = @co_art
            )
            SELECT 
                YEAR(m.mes_inicio) AS anio,
                MONTH(m.mes_inicio) AS mes,
                CONVERT(VARCHAR(7), m.mes_inicio, 120) AS periodo,
                ISNULL(sales.qty, 0) AS cant_facturada,
                ISNULL(devs.qty, 0) AS cant_devuelta,
                (ISNULL(sales.qty, 0) - ISNULL(devs.qty, 0)) AS cant_real_vendida,
                ISNULL(sales.doc_count, 0) AS docs_facturados,
                ISNULL(devs.doc_count, 0) AS docs_devueltos,
                (ISNULL(sales.doc_count, 0) - ISNULL(devs.doc_count, 0)) AS docs_exitosos,
                ISNULL(recep.qty, 0) AS cant_recepcionada,
                ISNULL(recep.doc_count, 0) AS docs_recepcion,
                ISNULL(ajustes.qty_entrada, 0) AS cant_ajuste_entrada,
                ISNULL(ajustes.qty_salida, 0) AS cant_ajuste_salida,
                cs.stock_actual,
                (cs.stock_actual - ISNULL(entradas_post.qty, 0) + ISNULL(salidas_post.qty, 0)) AS stock_inicial_calculado
            FROM Months m
            CROSS JOIN CurrentStock cs
            OUTER APPLY (
                SELECT 
                    SUM(r.total_art) AS qty,
                    COUNT(DISTINCT f.doc_num) AS doc_count
                FROM saFacturaVentaReng r
                INNER JOIN saFacturaVenta f ON r.doc_num = f.doc_num
                WHERE r.co_art = @co_art
                  AND f.anulado = 0
                  AND f.fec_emis >= m.mes_inicio
                  AND f.fec_emis < DATEADD(month, 1, m.mes_inicio)
            ) sales
            OUTER APPLY (
                SELECT 
                    SUM(d.total_art) AS qty,
                    COUNT(DISTINCT c.doc_num) AS doc_count
                FROM saDevolucionClienteReng d
                INNER JOIN saDevolucionCliente c ON d.doc_num = c.doc_num
                WHERE d.co_art = @co_art
                  AND c.anulado = 0
                  AND c.fec_emis >= m.mes_inicio
                  AND c.fec_emis < DATEADD(month, 1, m.mes_inicio)
            ) devs
            OUTER APPLY (
                SELECT 
                    SUM(nr_r.total_art) AS qty,
                    COUNT(DISTINCT nr.doc_num) AS doc_count
                FROM saNotaRecepcionCompraReng nr_r
                INNER JOIN saNotaRecepcionCompra nr ON nr_r.doc_num = nr.doc_num
                WHERE nr_r.co_art = @co_art
                  AND nr.anulado = 0
                  AND nr.fec_emis >= m.mes_inicio
                  AND nr.fec_emis < DATEADD(month, 1, m.mes_inicio)
            ) recep
            OUTER APPLY (
                SELECT 
                    SUM(CASE WHEN r.co_tipo = '01' OR ta.tipo_trans = '0' OR r.co_tipo LIKE '%ENT%' OR r.co_tipo LIKE '%IN%' THEN r.total_art ELSE 0 END) AS qty_entrada,
                    SUM(CASE WHEN r.co_tipo = '02' OR ta.tipo_trans = '1' OR r.co_tipo LIKE '%SAL%' OR r.co_tipo LIKE '%OUT%' THEN r.total_art ELSE 0 END) AS qty_salida,
                    COUNT(DISTINCT a.ajue_num) AS doc_count
                FROM saAjusteReng r
                INNER JOIN saAjuste a ON r.ajue_num = a.ajue_num
                LEFT JOIN saTipoAjuste ta ON r.co_tipo = ta.co_tipo
                WHERE r.co_art = @co_art
                  AND a.anulado = 0
                  AND a.fecha >= m.mes_inicio
                  AND a.fecha < DATEADD(month, 1, m.mes_inicio)
            ) ajustes
            OUTER APPLY (
                -- Entradas de inventario desde mes_inicio hasta hoy
                SELECT (
                    ISNULL((
                        SELECT SUM(r.total_art)
                        FROM saNotaRecepcionCompraReng r
                        INNER JOIN saNotaRecepcionCompra nr ON r.doc_num = nr.doc_num
                        WHERE r.co_art = @co_art AND nr.anulado = 0 AND nr.fec_emis >= m.mes_inicio
                    ), 0)
                    +
                    ISNULL((
                        SELECT SUM(r.total_art)
                        FROM saDevolucionClienteReng r
                        INNER JOIN saDevolucionCliente dc ON r.doc_num = dc.doc_num
                        WHERE r.co_art = @co_art AND dc.anulado = 0 AND dc.fec_emis >= m.mes_inicio
                    ), 0)
                    +
                    ISNULL((
                        SELECT SUM(r.total_art)
                        FROM saAjusteReng r
                        INNER JOIN saAjuste a ON r.ajue_num = a.ajue_num
                        WHERE r.co_art = @co_art AND a.anulado = 0 
                          AND (r.co_tipo = '01' OR r.co_tipo LIKE '%ENT%' OR r.co_tipo LIKE '%IN%') 
                          AND a.fecha >= m.mes_inicio
                    ), 0)
                ) AS qty
            ) entradas_post
            OUTER APPLY (
                -- Salidas de inventario desde mes_inicio hasta hoy
                SELECT (
                    ISNULL((
                        SELECT SUM(r.total_art)
                        FROM saFacturaVentaReng r
                        INNER JOIN saFacturaVenta fv ON r.doc_num = fv.doc_num
                        WHERE r.co_art = @co_art AND fv.anulado = 0 AND fv.fec_emis >= m.mes_inicio
                    ), 0)
                    +
                    ISNULL((
                        SELECT SUM(r.total_art)
                        FROM saDevolucionProveedorReng r
                        INNER JOIN saDevolucionProveedor dp ON r.doc_num = dp.doc_num
                        WHERE r.co_art = @co_art AND dp.anulado = 0 AND dp.fec_emis >= m.mes_inicio
                    ), 0)
                    +
                    ISNULL((
                        SELECT SUM(r.total_art)
                        FROM saAjusteReng r
                        INNER JOIN saAjuste a ON r.ajue_num = a.ajue_num
                        WHERE r.co_art = @co_art AND a.anulado = 0 
                          AND (r.co_tipo = '02' OR r.co_tipo LIKE '%SAL%' OR r.co_tipo LIKE '%OUT%') 
                          AND a.fecha >= m.mes_inicio
                    ), 0)
                ) AS qty
            ) salidas_post
            ORDER BY m.mes_inicio ASC
        `;

        const request = pool.request();
        request.input('co_art', co_art);
        const res = await request.query(query);
        console.log("Resultado meses:", res.recordset.map(r => ({
            periodo: r.periodo,
            vta: r.cant_real_vendida,
            rec: r.cant_recepcionada,
            aent: r.cant_ajuste_entrada,
            asal: r.cant_ajuste_salida,
            stk_ini: r.stock_inicial_calculado
        })));

        process.exit(0);
    } catch (err) {
        console.error("ERROR:", err);
        process.exit(1);
    }
}

run();
