const express = require('express');
const router = express.Router();
const { sql, getPool } = require('../db');
const { aggregateRead, aggregateUnique, paginatedResponse } = require('../helpers/multiSede');

/**
 * Helper genérico para catálogos simples: agrega, deduplica y ordena.
 */
async function catalogEndpoint(res, query, uniqueKey, sortKey, extraInputs = null) {
    try {
        const data = await aggregateUnique(async (pool) => {
            const req = pool.request();
            if (extraInputs) extraInputs(req);
            const r = await req.query(query);
            return r.recordset;
        }, uniqueKey, sortKey);
        res.status(200).json({ success: true, count: data.length, data });
    } catch (error) {
        res.status(500).json({ success: false, message: 'Error interno.', error: error.message });
    }
}

// ── Líneas ─────────────────────────────────────────────────────────────────
router.get('/lineas', (req, res) =>
    catalogEndpoint(res,
        `SELECT RTRIM(co_lin) AS co_lin, RTRIM(lin_des) AS lin_des FROM saLineaArticulo ORDER BY lin_des`,
        'co_lin', 'lin_des'
    )
);

// ── SubLíneas (filtro opcional ?co_lin=XX) ──────────────────────────────────
router.get('/sublineas', async (req, res) => {
    const { co_lin } = req.query;
    const query = `SELECT RTRIM(co_subl) AS co_subl, RTRIM(subl_des) AS subl_des, RTRIM(co_lin) AS co_lin FROM saSubLinea`
        + (co_lin ? ` WHERE RTRIM(co_lin) = @co_lin` : ` ORDER BY subl_des`);
    try {
        const data = await aggregateUnique(async (pool) => {
            const req = pool.request();
            if (co_lin) req.input('co_lin', sql.VarChar, co_lin);
            return (await req.query(query)).recordset;
        }, 'co_subl', 'subl_des');
        res.status(200).json({ success: true, count: data.length, data });
    } catch (error) {
        res.status(500).json({ success: false, message: 'Error interno.', error: error.message });
    }
});

// ── Categorías ──────────────────────────────────────────────────────────────
router.get('/categorias', (req, res) =>
    catalogEndpoint(res,
        `SELECT RTRIM(co_cat) AS co_cat, RTRIM(cat_des) AS cat_des FROM saCatArticulo`,
        'co_cat', 'cat_des'
    )
);

// ── Colores ─────────────────────────────────────────────────────────────────
router.get('/colores', (req, res) =>
    catalogEndpoint(res,
        `SELECT RTRIM(co_color) AS co_color, RTRIM(des_color) AS des_color FROM saColor`,
        'co_color', 'des_color'
    )
);

// ── Ubicaciones ─────────────────────────────────────────────────────────────
router.get('/ubicaciones', (req, res) =>
    catalogEndpoint(res,
        `SELECT RTRIM(co_ubicacion) AS co_ubicacion, RTRIM(des_ubicacion) AS des_ubicacion FROM saUbicacion`,
        'co_ubicacion', 'des_ubicacion'
    )
);

// ── Vendedores ──────────────────────────────────────────────────────────────
router.get('/vendedores', (req, res) =>
    catalogEndpoint(res,
        `SELECT RTRIM(co_ven) AS co_ven, RTRIM(ven_des) AS ven_des FROM saVendedor`,
        'co_ven', 'ven_des'
    )
);

// ── Zonas ───────────────────────────────────────────────────────────────────
router.get('/zonas', (req, res) =>
    catalogEndpoint(res,
        `SELECT RTRIM(co_zon) AS co_zon, RTRIM(zon_des) AS zon_des FROM saZona`,
        'co_zon', 'zon_des'
    )
);

// ── Segmentos ───────────────────────────────────────────────────────────────
router.get('/segmentos', (req, res) =>
    catalogEndpoint(res,
        `SELECT RTRIM(co_seg) AS co_seg, RTRIM(seg_des) AS seg_des FROM saSegmento`,
        'co_seg', 'seg_des'
    )
);

// ── Monedas ─────────────────────────────────────────────────────────────────
router.get('/monedas', (req, res) =>
    catalogEndpoint(res,
        `SELECT RTRIM(m.co_mone) AS co_mone, RTRIM(m.mone_des) AS mone_des,
                CAST(CASE WHEN LTRIM(RTRIM(m.co_mone)) = LTRIM(RTRIM(p.g_moneda)) THEN 1 ELSE 0 END AS BIT) AS is_default
         FROM saMoneda m CROSS JOIN (SELECT TOP 1 g_moneda FROM par_emp) p`,
        'co_mone', 'mone_des'
    )
);

// ── Condiciones de Pago ─────────────────────────────────────────────────────
router.get('/condiciones_pago', (req, res) =>
    catalogEndpoint(res,
        `SELECT RTRIM(co_cond) AS co_cond, RTRIM(cond_des) AS cond_des, CAST(dias_cred AS INT) AS dias_cred FROM saCondicionPago`,
        'co_cond', 'cond_des'
    )
);

// ── Almacenes ───────────────────────────────────────────────────────────────
// Almacenes se exponen CON sede_id porque pueden diferir entre sedes
router.get('/almacenes', async (req, res) => {
    try {
        const data = await aggregateRead(async (pool, srv) => {
            const r = await pool.request().query(
                `SELECT RTRIM(co_alma) AS co_alma, RTRIM(des_alma) AS des_alma FROM saAlmacen`
            );
            return r.recordset.map(a => ({ ...a, sede_id: srv.id, sede_nombre: srv.name }));
        });
        data.sort((a, b) => a.des_alma.localeCompare(b.des_alma));
        res.status(200).json({ success: true, count: data.length, data });
    } catch (error) {
        res.status(500).json({ success: false, message: 'Error interno.', error: error.message });
    }
});

// ── Precios (lista de listas de precios) ────────────────────────────────────
router.get('/precios', (req, res) =>
    catalogEndpoint(res,
        `SELECT RTRIM(co_precio) AS co_precio, RTRIM(prec_des) AS prec_des FROM saListaPrecio`,
        'co_precio', 'prec_des'
    )
);

// ── Tipos de Cliente ────────────────────────────────────────────────────────
router.get('/tipos_cliente', (req, res) =>
    catalogEndpoint(res,
        `SELECT RTRIM(tip_cli) AS tip_cli, RTRIM(tip_des) AS tip_des FROM saTipoCliente`,
        'tip_cli', 'tip_des'
    )
);

// ── Tasa de cambio ──────────────────────────────────────────────────────────
router.get('/tasa', async (req, res) => {
    try {
        const data = await aggregateRead(async (pool, srv) => {
            const r = await pool.request().query(
                `SELECT TOP 1 RTRIM(co_mone) AS co_mone, tasa_v AS tasa, fecha FROM saTasa
                 WHERE LTRIM(RTRIM(co_mone)) IN ('US$','USD') ORDER BY fecha DESC`
            );
            return r.recordset.map(t => ({ ...t, sede_id: srv.id, sede_nombre: srv.name }));
        });
        res.status(200).json({ success: true, count: data.length, data });
    } catch (error) {
        res.status(500).json({ success: false, message: 'Error interno.', error: error.message });
    }
});

module.exports = router;
