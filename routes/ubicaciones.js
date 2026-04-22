const express = require('express');
const router = express.Router();
const { sql, getPool, getServers } = require('../db');
const { executeWrite, writeResponse } = require('../helpers/multiSede');

/**
 * @swagger
 * tags:
 *   name: Ubicaciones
 *   description: Gestión de ubicaciones y artículos por ubicación
 */

/**
 * @swagger
 * /api/v1/ubicaciones:
 *   get:
 *     summary: Obtener listado de ubicaciones de todas las sedes
 *     tags: [Ubicaciones]
 *     parameters:
 *       - in: query
 *         name: sede
 *         schema:
 *           type: string
 *         description: ID o nombre de la sede para filtrar
 *     responses:
 *       200:
 *         description: Listado de ubicaciones obtenido exitosamente
 *       500:
 *         description: Error del servidor
 */
router.get('/', async (req, res) => {
    try {
        const requestedSede = req.query.sede || req.query.sede_id;
        let servers = getServers();
        if (requestedSede && requestedSede !== "Todas") {
            servers = servers.filter(srv => srv.id === requestedSede || srv.name === requestedSede);
        }

        if (!servers.length) return res.json({ success: true, count: 0, data: [] });

        const results = await Promise.all(servers.map(async (srv) => {
            try {
                const pool = await getPool(srv.id, req.sqlAuth);
                const r = pool.request();
                
                let query = `
                    SELECT RTRIM(co_ubicacion) AS id, RTRIM(des_ubicacion) AS descripcion
                    FROM saUbicacion
                `;

                // Filtrar por los códigos de sucursal de Profit asociados a esta sede
                const codes = (srv.profit_branch_codes || []).map(c => typeof c === 'string' ? c : c.code).filter(Boolean);
                if (codes.length > 0) {
                    const params = codes.map((c, i) => `@c${i}`).join(',');
                    codes.forEach((c, i) => r.input(`c${i}`, sql.Char, c));
                    query += ` WHERE co_sucu_in IN (${params})`;
                }

                query += ` ORDER BY des_ubicacion`;
                
                const dbRes = await r.query(query);
                return dbRes.recordset.map(u => ({ ...u, sede_id: srv.id, sede_nombre: srv.name }));
            } catch (e) {
                console.error(`[GET /ubicaciones] Error en sede ${srv.id}:`, e.message);
                return [];
            }
        }));

        const combined = [].concat(...results);
        res.json({ success: true, count: combined.length, data: combined });
    } catch (e) {
        res.status(500).json({ success: false, error: e.message });
    }
});

/**
 * @swagger
 * /api/v1/ubicaciones/{id}/articulos:
 *   get:
 *     summary: Obtener artículos asociados a una ubicación específica
 *     tags: [Ubicaciones]
 *     parameters:
 *       - in: path
 *         name: id
 *         required: true
 *         schema:
 *           type: string
 *         description: ID de la ubicación
 *       - in: query
 *         name: sede
 *         schema:
 *           type: string
 *         description: ID o nombre de la sede para filtrar
 *     responses:
 *       200:
 *         description: Listado de artículos en la ubicación
 *       500:
 *         description: Error del servidor
 */
router.get('/:id/articulos', async (req, res) => {
    try {
        const { id } = req.params;
        const requestedSede = req.query.sede || req.query.sede_id;

        let servers = getServers();
        if (requestedSede && requestedSede !== "Todas") {
            servers = servers.filter(srv => srv.id === requestedSede || srv.name === requestedSede);
        }

        if (!servers.length) return res.json({ success: true, count: 0, data: [] });

        const results = await Promise.all(servers.map(async (srv) => {
            try {
                const pool = await getPool(srv.id, req.sqlAuth);
                const query = `
                    SELECT RTRIM(a.co_art) AS co_art, RTRIM(a.art_des) AS descripcion,
                           RTRIM(a.modelo) AS modelo, RTRIM(a.ref) AS referencia,
                           RTRIM(au.co_ubicacion) AS ubicacion1,
                           RTRIM(au.co_ubicacion2) AS ubicacion2,
                           RTRIM(au.co_ubicacion3) AS ubicacion3
                    FROM saArtUbicacion au
                    INNER JOIN saArticulo a ON au.co_art = a.co_art
                    WHERE a.anulado = 0 AND
                    (LTRIM(RTRIM(au.co_ubicacion)) = @id OR LTRIM(RTRIM(au.co_ubicacion2)) = @id OR LTRIM(RTRIM(au.co_ubicacion3)) = @id)
                `;
                const r = pool.request();
                r.input('id', sql.VarChar, id);
                const dbRes = await r.query(query);
                return dbRes.recordset.map(a => ({ ...a, sede_id: srv.id, sede_nombre: srv.name }));
            } catch (e) {
                console.error(`[GET /ubicaciones/${id}/articulos] Error en sede ${srv.id}:`, e.message);
                return [];
            }
        }));

        const combined = [].concat(...results);
        res.json({ success: true, count: combined.length, data: combined });
    } catch (e) {
        res.status(500).json({ success: false, error: e.message });
    }
});

/**
 * @swagger
 * /api/v1/ubicaciones:
 *   post:
 *     summary: Crear una nueva ubicación
 *     tags: [Ubicaciones]
 *     requestBody:
 *       required: true
 *       content:
 *         application/json:
 *           schema:
 *             type: object
 *             required: [id, descripcion]
 *             properties:
 *               id:
 *                 type: string
 *                 description: Código de la ubicación (máx 6 chars)
 *               descripcion:
 *                 type: string
 *                 description: Nombre descriptivo
 *               sede:
 *                 type: string
 *                 description: ID de la sede donde crearla
 *     responses:
 *       200:
 *         description: Ubicación creada exitosamente
 *       400:
 *         description: Datos faltantes o inválidos
 *       500:
 *         description: Error del servidor
 */
router.post('/', async (req, res) => {
    try {
        const data = req.body;
        if (!data.id || !data.descripcion) {
            return res.status(400).json({ success: false, message: 'Faltan campos id, descripcion' });
        }

        const requestedSede = data.sede || data.sede_id || req.query.sede || req.query.sede_id || null;
        const outcome = await executeWrite(requestedSede, req.sqlAuth, async (pool) => {
            const check = await pool.request().input('id', sql.VarChar, data.id).query('SELECT 1 FROM saUbicacion WHERE LTRIM(RTRIM(co_ubicacion)) = LTRIM(RTRIM(@id))');
            if (check.recordset.length > 0) throw new Error('La ubicación ya existe.');

            const r = new sql.Request(pool);
            r.input('sCo_Ubicacion', sql.Char(6), data.id);
            r.input('sDes_Ubicacion', sql.VarChar(60), data.descripcion);
            r.input('sCo_Us_In', sql.Char(6), '999');
            r.input('sCo_Sucu_In', sql.Char(6), data.co_sucu || data.sucursal || req.query.co_sucu || null);
            r.input('sMaquina', sql.VarChar(60), 'SYNC2K');
            r.input('sRevisado', sql.Char(1), '0');
            r.input('sTrasnfe', sql.Char(1), '0');
            
            // Usando procedimiento por defecto si no lo hay hacemos INSERT directo, pero como es Profit:
            try {
                await r.execute('pInsertarUbicacion'); // Estándar profit
            } catch (pErr) {
                // Fallback a INSERT si el proc no exites/es diferente
                await pool.request()
                    .input('co', sql.Char(6), data.id)
                    .input('des', sql.VarChar(60), data.descripcion)
                    .input('sucu', sql.Char(6), data.co_sucu || data.sucursal || req.query.co_sucu || null)
                    .query('INSERT INTO saUbicacion (co_ubicacion, des_ubicacion, co_us_in, fe_us_in, co_sucu_in) VALUES (@co, @des, \'999\', GETDATE(), @sucu)');
            }
        });

        return writeResponse(res, outcome, `Sede especificada no encontrada.`);
    } catch (e) {
        res.status(500).json({ success: false, error: e.message });
    }
});

/**
 * @swagger
 * /api/v1/ubicaciones/{id}:
 *   put:
 *     summary: Actualizar una ubicación existente
 *     tags: [Ubicaciones]
 *     parameters:
 *       - in: path
 *         name: id
 *         required: true
 *         schema:
 *           type: string
 *         description: ID de la ubicación a modificar
 *     requestBody:
 *       required: true
 *       content:
 *         application/json:
 *           schema:
 *             type: object
 *             required: [descripcion]
 *             properties:
 *               descripcion:
 *                 type: string
 *               sede:
 *                 type: string
 *     responses:
 *       200:
 *         description: Ubicación actualizada
 *       404:
 *         description: Ubicación no encontrada
 *       500:
 *         description: Error del servidor
 */
router.put('/:id', async (req, res) => {
    try {
        const id = req.params.id;
        const data = req.body;

        const requestedSede = data.sede || data.sede_id || req.query.sede || req.query.sede_id || null;
        const outcome = await executeWrite(requestedSede, req.sqlAuth, async (pool) => {
            const r = new sql.Request(pool);
            r.input('id', sql.VarChar, id);
            r.input('des', sql.VarChar(60), data.descripcion);
            r.input('sucu', sql.Char(6), data.co_sucu || data.sucursal || req.query.co_sucu || null);
            
            const check = await r.query('SELECT 1 FROM saUbicacion WHERE LTRIM(RTRIM(co_ubicacion)) = LTRIM(RTRIM(@id))');
            if (check.recordset.length === 0) throw new Error('Ubicación no existe.');
            
            await r.query('UPDATE saUbicacion SET des_ubicacion = @des, co_sucu_mo = @sucu, fe_us_mo = GETDATE(), co_us_mo = \'999\' WHERE LTRIM(RTRIM(co_ubicacion)) = LTRIM(RTRIM(@id))');
        });

        return writeResponse(res, outcome, `Sede especificada no encontrada.`);
    } catch (e) {
        res.status(500).json({ success: false, error: e.message });
    }
});

module.exports = router;
