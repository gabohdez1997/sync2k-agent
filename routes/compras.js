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
 * /api/v1/compras/articulos/logistica:
 *   get:
 *     summary: Obtener datos logísticos de artículos (Última compra, costo y pendiente)
 *     tags: [Compras]
 *     parameters:
 *       - in: query
 *         name: sede
 *         schema:
 *           type: string
 *         description: ID de la sede a consultar (opcional)
 *       - in: query
 *         name: search
 *         schema:
 *           type: string
 *         description: Filtrar por código o descripción de artículo
 *     responses:
 *       200:
 *         description: Listado consolidado obtenido exitosamente
 *       500:
 *         description: Error del servidor
 */
router.get('/articulos/logistica', async (req, res) => {
    try {
        const requestedSede = req.query.sede || req.query.sede_id;
        const search = req.query.search || req.query.q;

        let servers = getServers();
        if (requestedSede && requestedSede !== "Todas") {
            servers = servers.filter(srv => srv.id === requestedSede || srv.name === requestedSede);
        }

        if (servers.length === 0) {
            return res.status(200).json({ success: true, data: [] });
        }

        const allResults = await Promise.all(servers.map(async (srv) => {
            try {
                const pool = await getPool(srv.id, req.sqlAuth);
                const r = pool.request();

                let whereClause = "WHERE a.anulado = 0";
                if (search) {
                    r.input('search', sql.VarChar, `%${search}%`);
                    whereClause += " AND (a.co_art LIKE @search OR a.art_des LIKE @search)";
                }

                const querySQL = `
                    SELECT 
                        a.co_art,
                        RTRIM(a.art_des) AS art_des,
                        -- Datos de la última Factura de Compra
                        fact.fec_emis AS fecha_ultima_compra,
                        fact.cost_unit AS ultimo_costo,
                        fact.cost_unit_om AS ultimo_costo_om,
                        -- Cantidad por llegar (Órdenes de Compra pendientes)
                        ISNULL(pend.cantidad_por_llegar, 0) AS cantidad_por_llegar
                    FROM saArticulo a
                    OUTER APPLY (
                        SELECT TOP 1 n.fec_emis, r.cost_unit, r.cost_unit_om
                        FROM saFacturaCompraReng r
                        INNER JOIN saFacturaCompra n ON r.doc_num = n.doc_num
                        WHERE r.co_art = a.co_art AND n.anulado = 0
                        ORDER BY n.fec_emis DESC
                    ) fact
                    OUTER APPLY (
                        SELECT SUM(r.pendiente) AS cantidad_por_llegar
                        FROM saOrdenCompraReng r
                        INNER JOIN saOrdenCompra o ON r.doc_num = o.doc_num
                        WHERE r.co_art = a.co_art 
                          AND o.anulado = 0 
                          AND o.status IN ('0', '1')
                          AND r.pendiente > 0
                    ) pend
                    ${whereClause}
                    ORDER BY a.art_des ASC
                `;

                const result = await r.query(querySQL);
                return result.recordset.map(item => ({
                    ...item,
                    sede_id: srv.id,
                    sede_nombre: srv.name
                }));
            } catch (e) {
                console.error(`[GET /compras/articulos/logistica] Error en sede ${srv.id}:`, e.message);
                return [];
            }
        }));

        const flattened = [].concat(...allResults);

        return res.status(200).json({
            success: true,
            count: flattened.length,
            data: flattened
        });

    } catch (error) {
        console.error('[COMPRAS LOGISTICA] Error:', error.message);
        res.status(500).json({ 
            success: false, 
            message: 'Error al consultar datos logísticos de artículos.', 
            error: error.message 
        });
    }
});

module.exports = router;
