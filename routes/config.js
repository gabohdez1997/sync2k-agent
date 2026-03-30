const express = require('express');
const router = express.Router();
const { setServers, setMasterConfig, getServers, addOrUpdateServer, removeServer } = require('../db');

/**
 * @swagger
 * tags:
 *   name: Config
 *   description: Configuración dinámica del agente
 */

/**
 * @swagger
 * /api/v1/config/database:
 *   post:
 *     summary: Establece la configuración de la base de datos dinámicamente
 *     tags: [Config]
 *     requestBody:
 *       required: true
 *       content:
 *         application/json:
 *           schema:
 *             type: object
 *             properties:
 *               servers:
 *                 type: array
 *                 items:
 *                   type: object
 *                   properties:
 *                     id: { type: string }
 *                     name: { type: string }
 *                     server: { type: string }
 *                     database: { type: string }
 *                     user: { type: string }
 *                     password: { type: string }
 *               master:
 *                 type: object
 *                 properties:
 *                   server: { type: string }
 *                   database: { type: string }
 *                   user: { type: string }
 *                   password: { type: string }
 *     responses:
 *       200:
 *         description: Configuración aplicada correctamente
 *       500:
 *         description: Error al aplicar la configuración
 */
router.post('/database', async (req, res) => {
    try {
        const { servers, master } = req.body;

        if (servers) {
            await setServers(servers);
        }

        if (master) {
            await setMasterConfig(master);
        }

        res.status(200).json({
            success: true,
            message: 'Configuración de base de datos aplicada correctamente.',
            sedes: getServers().length
        });
    } catch (error) {
        console.error('[POST /config/database]', error);
        res.status(500).json({ success: false, message: 'Error al aplicar la configuración.', error: error.message });
    }
});

/**
 * @swagger
 * /api/v1/config/database/{id}:
 *   patch:
 *     summary: Actualiza o añade una sede específica
 *     tags: [Config]
 *     parameters:
 *       - in: path
 *         name: id
 *         required: true
 *         schema: { type: string }
 *     requestBody:
 *       required: true
 *       content:
 *         application/json:
 *           schema:
 *             type: object
 *             properties:
 *               name: { type: string }
 *               server: { type: string }
 *               database: { type: string }
 *               user: { type: string }
 *               password: { type: string }
 *     responses:
 *       200:
 *         description: Sede actualizada correctamente
 */
router.patch('/database/:id', async (req, res) => {
    try {
        const { id } = req.params;
        const serverData = { ...req.body, id };
        await addOrUpdateServer(serverData);
        res.status(200).json({ success: true, message: `Sede ${id} actualizada correctamente.` });
    } catch (error) {
        console.error('[PATCH /config/database/:id]', error);
        res.status(500).json({ success: false, message: 'Error al actualizar sede.', error: error.message });
    }
});

/**
 * @swagger
 * /api/v1/config/database/{id}:
 *   delete:
 *     summary: Elimina una sede específica
 *     tags: [Config]
 *     parameters:
 *       - in: path
 *         name: id
 *         required: true
 *         schema: { type: string }
 *     responses:
 *       200:
 *         description: Sede eliminada correctamente
 */
router.delete('/database/:id', async (req, res) => {
    try {
        const { id } = req.params;
        await removeServer(id);
        res.status(200).json({ success: true, message: `Sede ${id} eliminada correctamente.` });
    } catch (error) {
        console.error('[DELETE /config/database/:id]', error);
        res.status(500).json({ success: false, message: 'Error al eliminar sede.', error: error.message });
    }
});

/**
 * @swagger
 * /api/v1/config/database:
 *   get:
 *     summary: Obtiene la configuración actual de sedes (solo IDs y nombres)
 *     tags: [Config]
 *     responses:
 *       200:
 *         description: Lista de sedes configuradas
 */
router.get('/database', (req, res) => {
    const servers = getServers().map(s => ({ id: s.id, name: s.name, server: s.server }));
    res.status(200).json({ success: true, servers });
});

module.exports = router;
