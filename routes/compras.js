const express = require('express');
const router = express.Router();
const { sql, getPool, getServers } = require('../db');

/**
 * @swagger
 * tags:
 *   name: Compras
 *   description: Módulo de Compras de Profit Plus
 */

/**
 * @swagger
 * /api/v1/compras/articulos:
 *   get:
 *     summary: Maestro de artículos especializado para compras (con logística y stock)
 *     tags: [Compras]
 */
router.get('/articulos', async (req, res) => {
    try {
        const page = parseInt(req.query.page) || 1;
        const limit = parseInt(req.query.limit) || 12;
        const requestedSede = req.query.sede || req.query.sede_id;
        const search = req.query.search || req.query.q || req.query.descripcion || req.query.co_art;
        const linea = req.query.linea;
        const categoria = req.query.categoria;

        let servers = getServers();
        if (requestedSede && requestedSede !== "Todas") {
            servers = servers.filter(srv => srv.id === requestedSede || srv.name === requestedSede);
        }

        if (servers.length === 0) {
            return res.status(200).json({ success: true, data: [], pagination: { total: 0, page, limit, totalPages: 0 } });
        }

        const srv = servers[0];
        const pool = await getPool(srv.id, req.sqlAuth);
        const r = pool.request();

        let whereClauses = ["a.anulado = 0"];
        if (search) {
            r.input('search', sql.VarChar, `%${search}%`);
            whereClauses.push("(a.co_art LIKE @search OR a.art_des LIKE @search OR a.modelo LIKE @search OR a.ref LIKE @search)");
        }
        if (linea) {
            r.input('linea', sql.VarChar, linea);
            whereClauses.push("a.co_lin = @linea");
        }
        if (categoria) {
            r.input('categoria', sql.VarChar, categoria);
            whereClauses.push("a.co_cat = @categoria");
        }

        const whereSQL = whereClauses.join(" AND ");

        const countRes = await r.query(`SELECT COUNT(*) as total FROM saArticulo a WHERE ${whereSQL}`);
        const total = countRes.recordset[0].total;

        // QUERY USANDO FACTURA DE COMPRA (FUENTE DEFINITIVA)
        const querySQL = `
            SELECT 
                RTRIM(a.co_art) AS co_art, 
                RTRIM(a.art_des) AS descripcion,
                RTRIM(a.modelo) AS modelo, 
                RTRIM(a.ref) AS referencia,
                RTRIM(l.lin_des) AS linea, 
                RTRIM(c.cat_des) AS categoria,
                RTRIM(a.tipo_imp) AS tipo_imp,
                -- Logística desde Factura de Compra
                fact.fec_emis AS fecha_ultima_compra,
                fact.cost_unit AS ultimo_costo,
                fact.cost_unit_om AS ultimo_costo_om,
                -- Cantidad por llegar (Órdenes de Compra)
                ISNULL(pend.cantidad_por_llegar, 0) AS cantidad_por_llegar
            FROM saArticulo a
            LEFT JOIN saLineaArticulo l ON a.co_lin = l.co_lin
            LEFT JOIN saCatArticulo c ON a.co_cat = c.co_cat
            OUTER APPLY (
                SELECT TOP 1 
                    n.fec_emis, 
                    r.cost_unit, 
                    -- Si la tasa de la factura es 1 (Bolívares), buscamos la tasa de ese día en saTasa
                    CASE 
                        WHEN (n.tasa <= 1) THEN (r.cost_unit / NULLIF((SELECT TOP 1 tasa_v FROM saTasa WHERE co_mone = 'US' AND fecha <= n.fec_emis ORDER BY fecha DESC), 0))
                        ELSE r.cost_unit_om 
                    END AS cost_unit_om
                FROM saFacturaCompraReng r
                INNER JOIN saFacturaCompra n ON r.doc_num = n.doc_num
                WHERE r.co_art = a.co_art AND n.anulado = 0
                ORDER BY n.fec_emis DESC
            ) fact
            OUTER APPLY (
                SELECT SUM(r.pendiente) AS cantidad_por_llegar
                FROM saOrdenCompraReng r
                INNER JOIN saOrdenCompra o ON r.doc_num = o.doc_num
                WHERE r.co_art = a.co_art AND o.anulado = 0 AND o.status IN ('0', '1') AND r.pendiente > 0
            ) pend
            WHERE ${whereSQL}
            ORDER BY a.art_des ASC
            OFFSET ${(page - 1) * limit} ROWS FETCH NEXT ${limit} ROWS ONLY
        `;

        const resData = await r.query(querySQL);
        const articulos = resData.recordset;

        if (articulos.length > 0) {
            const ids = articulos.map(a => `'${a.co_art.replace(/'/g, "''")}'`).join(',');
            const resStock = await pool.request().query(`
                SELECT RTRIM(s.co_art) AS co_art, RTRIM(s.co_alma) AS co_alma,
                       RTRIM(al.des_alma) AS des_alma,
                       SUM(CASE WHEN RTRIM(s.tipo)='ACT' THEN s.stock ELSE 0 END)
                       - SUM(CASE WHEN RTRIM(s.tipo)='COM' THEN s.stock ELSE 0 END)
                       - SUM(CASE WHEN RTRIM(s.tipo)='DES' THEN s.stock ELSE 0 END) AS stock
                FROM saStockAlmacen s
                LEFT JOIN saAlmacen al ON s.co_alma = al.co_alma
                WHERE LTRIM(RTRIM(s.co_art)) IN (${ids})
                GROUP BY s.co_art, s.co_alma, al.des_alma
                HAVING (SUM(CASE WHEN RTRIM(s.tipo)='ACT' THEN s.stock ELSE 0 END)
                       - SUM(CASE WHEN RTRIM(s.tipo)='COM' THEN s.stock ELSE 0 END)
                       - SUM(CASE WHEN RTRIM(s.tipo)='DES' THEN s.stock ELSE 0 END)) > 0
            `);

            const stockMap = {};
            resStock.recordset.forEach(s => {
                (stockMap[s.co_art] = stockMap[s.co_art] || []).push({
                    co_alma: s.co_alma,
                    des_alma: s.des_alma,
                    stock: s.stock
                });
            });

            articulos.forEach(a => {
                a.disponibilidad = stockMap[a.co_art] || [];
                a.total_stock = a.disponibilidad.reduce((sum, s) => sum + s.stock, 0);
                a.sede_id = srv.id;
                a.sede_nombre = srv.name;
            });
        }

        return res.status(200).json({
            success: true,
            data: articulos,
            pagination: {
                total,
                page,
                limit,
                totalPages: Math.ceil(total / limit)
            }
        });

    } catch (error) {
        console.error('[COMPRAS ARTICULOS] Error:', error.message);
        res.status(500).json({ success: false, message: 'Error en maestro de artículos para compras.', error: error.message });
    }
});

module.exports = router;
