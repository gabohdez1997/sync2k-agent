const express = require('express');
const router = express.Router();
const { sql, getPool } = require('../db');

/**
 * @swagger
 * /api/v1/catalogos/lineas:
 *   get:
 *     summary: Obtener todas las Líneas de Artículos
 *     description: Retorna el catálogo maestro de líneas.
 *     tags: [Catálogos]
 *     responses:
 *       200:
 *         description: Lista de Líneas obtenida exitosamente.
 */
router.get('/lineas', async (req, res) => {
    try {
        const pool = await getPool();
        const result = await pool.request().query(`
      SELECT 
        RTRIM(co_lin) AS co_lin, 
        RTRIM(lin_des) AS lin_des 
      FROM saLineaArticulo
      ORDER BY lin_des
    `);

        res.status(200).json({
            success: true,
            count: result.recordset.length,
            data: result.recordset
        });
    } catch (error) {
        console.error('Error al obtener Líneas:', error);
        res.status(500).json({ success: false, message: 'Error interno del servidor.', error: error.message });
    }
});

/**
 * @swagger
 * /api/v1/catalogos/sublineas:
 *   get:
 *     summary: Obtener todas las SubLíneas (con filtro opcional)
 *     description: Retorna el catálogo maestro de Sublíneas. Soporta filtro opcional ?co_lin=XXX.
 *     tags: [Catálogos]
 *     parameters:
 *       - in: query
 *         name: co_lin
 *         required: false
 *         schema:
 *           type: string
 *         description: Código de Línea Padre para filtrar opciones
 *     responses:
 *       200:
 *         description: Lista de SubLíneas obtenida exitosamente.
 */
router.get('/sublineas', async (req, res) => {
    try {
        const { co_lin } = req.query;
        const pool = await getPool();
        const request = pool.request();

        let query = `
      SELECT 
        RTRIM(co_subl) AS co_subl, 
        RTRIM(subl_des) AS subl_des,
        RTRIM(co_lin) AS co_lin
      FROM saSubLinea
    `;

        if (co_lin) {
            query += ` WHERE RTRIM(co_lin) = @co_lin `;
            request.input('co_lin', sql.VarChar, co_lin);
        }

        query += ` ORDER BY subl_des`;

        const result = await request.query(query);

        res.status(200).json({
            success: true,
            count: result.recordset.length,
            data: result.recordset
        });
    } catch (error) {
        console.error('Error al obtener Sublíneas:', error);
        res.status(500).json({ success: false, message: 'Error interno del servidor.', error: error.message });
    }
});

/**
 * @swagger
 * /api/v1/catalogos/categorias:
 *   get:
 *     summary: Obtener todas las Categorías de Artículos
 *     description: Retorna el catálogo maestro de categorías.
 *     tags: [Catálogos]
 *     responses:
 *       200:
 *         description: Lista de Categorías obtenida exitosamente.
 */
router.get('/categorias', async (req, res) => {
    try {
        const pool = await getPool();
        const result = await pool.request().query(`
      SELECT 
        RTRIM(co_cat) AS co_cat, 
        RTRIM(cat_des) AS cat_des 
      FROM saCatArticulo
      ORDER BY cat_des
    `);

        res.status(200).json({
            success: true,
            count: result.recordset.length,
            data: result.recordset
        });
    } catch (error) {
        console.error('Error al obtener Categorías:', error);
        res.status(500).json({ success: false, message: 'Error interno del servidor.', error: error.message });
    }
});

/**
 * @swagger
 * /api/v1/catalogos/colores:
 *   get:
 *     summary: Obtener colores de inventario
 *     description: Retorna el catálogo maestro de Colores.
 *     tags: [Catálogos]
 *     responses:
 *       200:
 *         description: Lista de Colores obtenida exitosamente.
 */
router.get('/colores', async (req, res) => {
    try {
        const pool = await getPool();
        const result = await pool.request().query(`
      SELECT 
        RTRIM(co_color) AS co_color, 
        RTRIM(des_color) AS des_color 
      FROM saColor
      ORDER BY des_color
    `);

        res.status(200).json({
            success: true,
            count: result.recordset.length,
            data: result.recordset
        });
    } catch (error) {
        console.error('Error al obtener Colores:', error);
        res.status(500).json({ success: false, message: 'Error interno del servidor.', error: error.message });
    }
});

/**
 * @swagger
 * /api/v1/catalogos/ubicaciones:
 *   get:
 *     summary: Obtener Ubicaciones de Almacenes Físicos
 *     description: Retorna el catálogo maestro de Ubicaciones en los almacenes.
 *     tags: [Catálogos]
 *     responses:
 *       200:
 *         description: Lista de Ubicaciones obtenida exitosamente.
 */
router.get('/ubicaciones', async (req, res) => {
    try {
        const pool = await getPool();
        const result = await pool.request().query(`
      SELECT 
        RTRIM(co_ubicacion) AS co_ubicacion, 
        RTRIM(des_ubicacion) AS des_ubicacion 
      FROM saUbicacion
      ORDER BY des_ubicacion
    `);

        res.status(200).json({
            success: true,
            count: result.recordset.length,
            data: result.recordset
        });
    } catch (error) {
        console.error('Error al obtener Ubicaciones:', error);
        res.status(500).json({ success: false, message: 'Error interno del servidor.', error: error.message });
    }
});

/**
 * @swagger
 * /api/v1/catalogos/vendedores:
 *   get:
 *     summary: Obtener Vendedores
 *     description: Retorna el catálogo maestro de Vendedores.
 *     tags: [Catálogos]
 *     responses:
 *       200:
 *         description: Lista obtenida exitosamente.
 */
router.get('/vendedores', async (req, res) => {
    try {
        const pool = await getPool();
        const result = await pool.request().query(`
      SELECT 
        RTRIM(co_ven) AS co_ven, 
        RTRIM(ven_des) AS ven_des 
      FROM saVendedor
      ORDER BY ven_des
    `);

        res.status(200).json({ success: true, count: result.recordset.length, data: result.recordset });
    } catch (error) {
        console.error('Error al obtener Vendedores:', error);
        res.status(500).json({ success: false, message: 'Error interno.', error: error.message });
    }
});

/**
 * @swagger
 * /api/v1/catalogos/zonas:
 *   get:
 *     summary: Obtener Zonas Geográficas
 *     description: Retorna el catálogo maestro de Zonas.
 *     tags: [Catálogos]
 *     responses:
 *       200:
 *         description: Lista obtenida exitosamente.
 */
router.get('/zonas', async (req, res) => {
    try {
        const pool = await getPool();
        const result = await pool.request().query(`
      SELECT 
        RTRIM(co_zon) AS co_zon, 
        RTRIM(zon_des) AS zon_des 
      FROM saZona
      ORDER BY zon_des
    `);

        res.status(200).json({ success: true, count: result.recordset.length, data: result.recordset });
    } catch (error) {
        console.error('Error al obtener Zonas:', error);
        res.status(500).json({ success: false, message: 'Error interno.', error: error.message });
    }
});

/**
 * @swagger
 * /api/v1/catalogos/segmentos:
 *   get:
 *     summary: Obtener Segmentos de Clientes
 *     description: Retorna el catálogo maestro de Segmentos.
 *     tags: [Catálogos]
 *     responses:
 *       200:
 *         description: Lista obtenida exitosamente.
 */
router.get('/segmentos', async (req, res) => {
    try {
        const pool = await getPool();
        const result = await pool.request().query(`
      SELECT 
        RTRIM(co_seg) AS co_seg, 
        RTRIM(seg_des) AS seg_des 
      FROM saSegmento
      ORDER BY seg_des
    `);

        res.status(200).json({ success: true, count: result.recordset.length, data: result.recordset });
    } catch (error) {
        console.error('Error al obtener Segmentos:', error);
        res.status(500).json({ success: false, message: 'Error interno.', error: error.message });
    }
});

/**
 * @swagger
 * /api/v1/catalogos/monedas:
 *   get:
 *     summary: Obtener Tipos de Monedas
 *     description: Retorna el catálogo maestro de Monedas.
 *     tags: [Catálogos]
 *     responses:
 *       200:
 *         description: Lista obtenida exitosamente.
 */
router.get('/monedas', async (req, res) => {
    try {
        const pool = await getPool();
        const result = await pool.request().query(`
      SELECT 
        RTRIM(m.co_mone) AS co_mone, 
        RTRIM(m.mone_des) AS mone_des,
        CAST(CASE WHEN LTRIM(RTRIM(m.co_mone)) = LTRIM(RTRIM(p.g_moneda)) THEN 1 ELSE 0 END AS BIT) AS is_default
      FROM saMoneda m
      CROSS JOIN (SELECT TOP 1 g_moneda FROM par_emp) p
      ORDER BY m.mone_des
    `);

        res.status(200).json({ success: true, count: result.recordset.length, data: result.recordset });
    } catch (error) {
        console.error('Error al obtener Monedas:', error);
        res.status(500).json({ success: false, message: 'Error interno.', error: error.message });
    }
});

/**
 * @swagger
 * /api/v1/catalogos/condiciones_pago:
 *   get:
 *     summary: Obtener Condiciones de Pago
 *     description: Retorna el catálogo maestro de Condiciones de Pago.
 *     tags: [Catálogos]
 *     responses:
 *       200:
 *         description: Lista obtenida exitosamente.
 */
router.get('/condiciones_pago', async (req, res) => {
    try {
        const pool = await getPool();
        const result = await pool.request().query(`
      SELECT 
        RTRIM(co_cond) AS co_cond, 
        RTRIM(cond_des) AS cond_des,
        CAST(dias_cred AS INT) AS dias_cred
      FROM saCondicionPago
      ORDER BY cond_des
    `);

        res.status(200).json({ success: true, count: result.recordset.length, data: result.recordset });
    } catch (error) {
        console.error('Error al obtener Condiciones de Pago:', error);
        res.status(500).json({ success: false, message: 'Error interno.', error: error.message });
    }
});

/**
 * @swagger
 * /api/v1/catalogos/almacenes:
 *   get:
 *     summary: Obtener Almacenes
 *     description: Retorna el catálogo maestro de Almacenes Generales.
 *     tags: [Catálogos]
 *     responses:
 *       200:
 *         description: Lista obtenida exitosamente.
 */
router.get('/almacenes', async (req, res) => {
    try {
        const pool = await getPool();
        const result = await pool.request().query(`
      SELECT 
        RTRIM(co_alma) AS co_alma, 
        RTRIM(des_alma) AS des_alma 
      FROM saAlmacen
      ORDER BY des_alma
    `);

        res.status(200).json({ success: true, count: result.recordset.length, data: result.recordset });
    } catch (error) {
        console.error('Error al obtener Almacenes:', error);
        res.status(500).json({ success: false, message: 'Error interno.', error: error.message });
    }
});

module.exports = router;
