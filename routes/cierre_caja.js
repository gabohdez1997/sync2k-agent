const express = require('express');
const router = express.Router();
const { auditarCierreCajas } = require('../helpers/cierreCajaAudit');

/**
 * @swagger
 * tags:
 *   name: CierreCaja
 *   description: Auditoría y conciliación de cierres de caja vs Profit Plus
 */

/**
 * @swagger
 * /api/v1/cierre-caja/auditar:
 *   get:
 *     summary: Audita facturas, devoluciones y cobros de los libros Excel contra Profit Plus
 *     tags: [CierreCaja]
 *     parameters:
 *       - in: query
 *         name: fecha
 *         schema:
 *           type: string
 *         description: Fecha de cierre a auditar (formato YYYY-MM-DD). Por defecto la fecha actual o 2026-10-06.
 *     responses:
 *       200:
 *         description: Resultados de la auditoría detallada
 */
router.get('/auditar', async (req, res) => {
    try {
        const fecha = req.query.fecha || new Date().toISOString().split('T')[0];
        const resultados = await auditarCierreCajas({ fecha });
        return res.json({
            success: true,
            data: resultados
        });
    } catch (err) {
        console.error('[CIERRE CAJA] Error en auditoría:', err);
        return res.status(500).json({
            success: false,
            message: 'Error al ejecutar auditoría de cierre de caja: ' + err.message
        });
    }
});

router.post('/auditar', async (req, res) => {
    try {
        const fecha = req.body.fecha || new Date().toISOString().split('T')[0];
        const resultados = await auditarCierreCajas({ fecha });
        return res.json({
            success: true,
            data: resultados
        });
    } catch (err) {
        console.error('[CIERRE CAJA] Error en auditoría:', err);
        return res.status(500).json({
            success: false,
            message: 'Error al ejecutar auditoría de cierre de caja: ' + err.message
        });
    }
});

module.exports = router;
