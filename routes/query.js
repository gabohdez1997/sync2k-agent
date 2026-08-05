const express = require('express');
const router = express.Router();
const { getPool, getServers } = require('../db');

/**
 * @swagger
 * /api/v1/query:
 *   post:
 *     summary: Ejecuta una consulta SQL en la base de datos de la sede.
 *     tags: [Query]
 *     parameters:
 *       - in: query
 *         name: sede
 *         schema:
 *           type: string
 *         description: ID de la sede a consultar (opcional, si no se envía toma la por defecto).
 *     requestBody:
 *       required: true
 *       content:
 *         application/json:
 *           schema:
 *             type: object
 *             properties:
 *               query:
 *                 type: string
 *     responses:
 *       200:
 *         description: Resultado de la consulta
 */
router.post('/', async (req, res) => {
    try {
        const { query } = req.body;
        let sede = req.query.sede || 'default';

        if (!query) {
            return res.status(400).json({ 
                success: false, 
                message: 'La propiedad "query" es requerida en el cuerpo de la petición.' 
            });
        }

        // Determinar qué sede conectar si se usó 'default'
        const servers = getServers();
        if (sede === 'default') {
            if (servers && servers.length > 0) {
                sede = servers[0].id;
            } else {
                return res.status(500).json({ success: false, message: 'No hay servidores SQL configurados en el agente.' });
            }
        }

        // Obtener el pool de conexión para la sede
        const pool = await getPool(sede, req.sqlAuth);
        
        // Ejecutar query
        const result = await pool.request().query(query);

        res.json({
            success: true,
            server: sede,
            data: result.recordset
        });

    } catch (error) {
        console.error(`[POST /query]`, error);
        res.status(500).json({
            success: false,
            message: 'Error al ejecutar la consulta SQL.',
            error: error.message
        });
    }
});

module.exports = router;
