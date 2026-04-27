const express = require('express');
const router = express.Router();
const { sql, getPool, getServers } = require('../db');

router.get('/articulos', async (req, res) => {
    try {
        const page = parseInt(req.query.page) || 1;
        const limit = parseInt(req.query.limit) || 12;
        const search = req.query.search || req.query.q || req.query.descripcion || req.query.co_art;
        const linea = req.query.linea;
        const categoria = req.query.categoria;
        const stockFilter = req.query.in_stock; 
        const soloPendientes = req.query.solo_pendientes === 'true';
        const conCosto = req.query.con_costo; 

        let servers = getServers();
        const srv = servers[0];
        if (!srv) return res.status(200).json({ success: true, data: [], pagination: { total: 0 } });

        const pool = await getPool(srv.id, req.sqlAuth);
        const r = pool.request();

        let whereClauses = ["a.anulado = 0"];
        if (search) { r.input('search', sql.VarChar, `%${search}%`); whereClauses.push("(a.co_art LIKE @search OR a.art_des LIKE @search OR a.modelo LIKE @search OR a.ref LIKE @search)"); }
        if (linea) { r.input('linea', sql.VarChar, linea); whereClauses.push("a.co_lin = @linea"); }
        if (categoria) { r.input('categoria', sql.VarChar, categoria); whereClauses.push("a.co_cat = @categoria"); }

        // ELIMINAMOS EL FILTRO co_sucu ya que el pool ya nos conecta a la DB correcta
        if (stockFilter === 'true') { 
            whereClauses.push(`EXISTS (SELECT 1 FROM saStockAlmacen s2 WHERE s2.co_art = a.co_art GROUP BY s2.co_art HAVING SUM(CASE WHEN RTRIM(s2.tipo)='ACT' THEN s2.stock ELSE 0 END) - SUM(CASE WHEN RTRIM(s2.tipo)='COM' THEN s2.stock ELSE 0 END) - SUM(CASE WHEN RTRIM(s2.tipo)='DES' THEN s2.stock ELSE 0 END) > 0)`); 
        } else if (stockFilter === 'false') {
            whereClauses.push(`NOT EXISTS (SELECT 1 FROM saStockAlmacen s2 WHERE s2.co_art = a.co_art GROUP BY s2.co_art HAVING SUM(CASE WHEN RTRIM(s2.tipo)='ACT' THEN s2.stock ELSE 0 END) - SUM(CASE WHEN RTRIM(s2.tipo)='COM' THEN s2.stock ELSE 0 END) - SUM(CASE WHEN RTRIM(s2.tipo)='DES' THEN s2.stock ELSE 0 END) > 0)`);
        }

        if (soloPendientes) { 
            whereClauses.push(`ISNULL((SELECT SUM(r2.pendiente) FROM saOrdenCompraReng r2 INNER JOIN saOrdenCompra o2 ON r2.doc_num = o2.doc_num WHERE r2.co_art = a.co_art AND o2.anulado = 0 AND o2.status IN ('0', '1')), 0) > 0`); 
        }

        if (conCosto === 'true') { 
            whereClauses.push(`EXISTS (SELECT 1 FROM saFacturaCompraReng r3 INNER JOIN saFacturaCompra n3 ON r3.doc_num = n3.doc_num WHERE r3.co_art = a.co_art AND n3.anulado = 0)`); 
        } else if (conCosto === 'false') {
            whereClauses.push(`NOT EXISTS (SELECT 1 FROM saFacturaCompraReng r3 INNER JOIN saFacturaCompra n3 ON r3.doc_num = n3.doc_num WHERE r3.co_art = a.co_art AND n3.anulado = 0)`);
        }

        const whereSQL = whereClauses.join(" AND ");
        const countRes = await r.query(`SELECT COUNT(*) as total FROM saArticulo a WHERE ${whereSQL}`);
        const total = countRes.recordset[0].total;

        const offset = (page - 1) * limit;
        const querySQL = `
            SELECT 
                RTRIM(a.co_art) AS co_art, RTRIM(a.art_des) AS descripcion,
                RTRIM(a.modelo) AS modelo, RTRIM(a.ref) AS referencia,
                RTRIM(l.lin_des) AS linea, RTRIM(c.cat_des) AS categoria,
                fact.fec_emis AS fecha_ultima_compra,
                fact.cost_unit AS ultimo_costo,
                fact.cost_unit_om AS ultimo_costo_om,
                ISNULL(pend.cantidad_por_llegar, 0) AS cantidad_por_llegar
            FROM saArticulo a
            LEFT JOIN saLineaArticulo l ON a.co_lin = l.co_lin
            LEFT JOIN saCatArticulo c ON a.co_cat = c.co_cat
            OUTER APPLY (
                SELECT TOP 1 n.fec_emis, r.cost_unit, 
                    CASE WHEN RTRIM(n.co_mone) = 'BS' THEN (r.cost_unit / NULLIF((SELECT TOP 1 tasa_v FROM saTasa WHERE (co_mone LIKE 'US%') AND CAST(fecha AS DATE) <= CAST(n.fec_emis AS DATE) ORDER BY fecha DESC), 0)) ELSE r.cost_unit_om END AS cost_unit_om
                FROM saFacturaCompraReng r INNER JOIN saFacturaCompra n ON r.doc_num = n.doc_num
                WHERE r.co_art = a.co_art AND n.anulado = 0
                ORDER BY n.fec_emis DESC
            ) fact
            OUTER APPLY (
                SELECT SUM(r.pendiente) AS cantidad_por_llegar
                FROM saOrdenCompraReng r INNER JOIN saOrdenCompra o ON r.doc_num = o.doc_num
                WHERE r.co_art = a.co_art AND o.anulado = 0 AND o.status IN ('0', '1') AND r.pendiente > 0
            ) pend
            WHERE ${whereSQL}
            ORDER BY a.art_des ASC
            OFFSET ${offset} ROWS FETCH NEXT ${limit} ROWS ONLY
        `;

        const resData = await r.query(querySQL);
        const articulos = resData.recordset;

        if (articulos.length > 0) {
            const ids = articulos.map(a => `'${a.co_art.replace(/'/g, "''")}'`).join(',');
            const resStock = await pool.request().query(`
                SELECT RTRIM(s.co_art) AS co_art, RTRIM(s.co_alma) AS co_alma, RTRIM(al.des_alma) AS des_alma,
                       SUM(CASE WHEN RTRIM(s.tipo)='ACT' THEN s.stock ELSE 0 END) - SUM(CASE WHEN RTRIM(s.tipo)='COM' THEN s.stock ELSE 0 END) - SUM(CASE WHEN RTRIM(s.tipo)='DES' THEN s.stock ELSE 0 END) AS stock
                FROM saStockAlmacen s LEFT JOIN saAlmacen al ON s.co_alma = al.co_alma
                WHERE LTRIM(RTRIM(s.co_art)) IN (${ids})
                GROUP BY s.co_art, s.co_alma, al.des_alma
            `);

            const stockMap = {};
            resStock.recordset.forEach(s => { (stockMap[s.co_art] = stockMap[s.co_art] || []).push({ co_alma: s.co_alma, des_alma: s.des_alma, stock: s.stock }); });

            articulos.forEach(a => {
                a.disponibilidad = stockMap[a.co_art] || [];
                a.total_stock = a.disponibilidad.reduce((sum, s) => sum + s.stock, 0);
                a.sede_nombre = srv.name;
            });
        }

        return res.status(200).json({
            success: true,
            data: articulos,
            pagination: { total, page, limit, totalPages: Math.ceil(total / limit) }
        });

    } catch (error) {
        console.error('[COMPRAS ARTICULOS] Error:', error.message);
        res.status(500).json({ success: false, message: 'Error en consulta de artículos.', error: error.message });
    }
});

module.exports = router;
