const express = require('express');
const router = express.Router();
const { sql, getPool } = require('../db');
const { aggregateRead, aggregateUnique, paginatedResponse } = require('../helpers/multiSede');

/**
 * @swagger
 * tags:
 *   name: Catalogos
 *   description: Consultas de tablas maestras y catálogos (Líneas, Almacenes, etc.)
 */

/**
 * Helper genérico para catálogos simples: agrega, deduplica y ordena.
 */
async function catalogEndpoint(req, res, query, uniqueKey, sortKey, extraInputs = null) {
    try {
        const data = await aggregateUnique(req.sqlAuth, async (pool) => {
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
/**
 * @swagger
 * /api/v1/catalogos/lineas:
 *   get:
 *     summary: Obtener lista de líneas de artículos
 *     tags: [Catalogos]
 *     responses:
 *       200:
 *         description: Lista de líneas
 */
router.get('/lineas', (req, res) =>
    catalogEndpoint(req, res,
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
        const data = await aggregateUnique(req.sqlAuth, async (pool) => {
            const r = pool.request();
            if (co_lin) r.input('co_lin', sql.VarChar, co_lin);
            return (await r.query(query)).recordset;
        }, 'co_subl', 'subl_des');
        res.status(200).json({ success: true, count: data.length, data });
    } catch (error) {
        res.status(500).json({ success: false, message: 'Error interno.', error: error.message });
    }
});

// ── Categorías ──────────────────────────────────────────────────────────────
router.get('/categorias', (req, res) =>
    catalogEndpoint(req, res,
        `SELECT RTRIM(co_cat) AS co_cat, RTRIM(cat_des) AS cat_des FROM saCatArticulo`,
        'co_cat', 'cat_des'
    )
);

// ── Colores ─────────────────────────────────────────────────────────────────
router.get('/colores', (req, res) =>
    catalogEndpoint(req, res,
        `SELECT RTRIM(co_color) AS co_color, RTRIM(des_color) AS des_color FROM saColor`,
        'co_color', 'des_color'
    )
);

// ── Ubicaciones ─────────────────────────────────────────────────────────────
router.get('/ubicaciones', (req, res) =>
    catalogEndpoint(req, res,
        `SELECT RTRIM(co_ubicacion) AS co_ubicacion, RTRIM(des_ubicacion) AS des_ubicacion FROM saUbicacion`,
        'co_ubicacion', 'des_ubicacion'
    )
);

// ── Vendedores ──────────────────────────────────────────────────────────────
router.get('/vendedores', (req, res) =>
    catalogEndpoint(req, res,
        `SELECT RTRIM(co_ven) AS co_ven, RTRIM(ven_des) AS ven_des FROM saVendedor`,
        'co_ven', 'ven_des'
    )
);

// ── Zonas ───────────────────────────────────────────────────────────────────
router.get('/zonas', async (req, res) => {
    console.log('[CATALOGOS] Consultando zonas...');
    try {
        const data = await aggregateUnique(req.sqlAuth, async (pool) => {
            const r = await pool.request().query(`SELECT RTRIM(co_zon) AS co_zon, RTRIM(zon_des) AS zon_des FROM saZona`);
            return r.recordset;
        }, 'co_zon', 'zon_des');
        console.log(`[CATALOGOS] Zonas encontradas: ${data.length}`);
        res.status(200).json({ success: true, count: data.length, data });
    } catch (error) {
        console.error('[CATALOGOS] Error en zonas:', error.message);
        res.status(500).json({ success: false, message: 'Error interno.', error: error.message });
    }
});

// ── Segmentos ───────────────────────────────────────────────────────────────
router.get('/segmentos', (req, res) =>
    catalogEndpoint(req, res,
        `SELECT RTRIM(co_seg) AS co_seg, RTRIM(seg_des) AS seg_des FROM saSegmento`,
        'co_seg', 'seg_des'
    )
);

// ── Monedas ─────────────────────────────────────────────────────────────────
router.get('/monedas', (req, res) =>
    catalogEndpoint(req, res,
        `SELECT RTRIM(m.co_mone) AS co_mone, RTRIM(m.mone_des) AS mone_des,
                CAST(CASE WHEN LTRIM(RTRIM(m.co_mone)) = LTRIM(RTRIM(p.g_moneda)) THEN 1 ELSE 0 END AS BIT) AS is_default
         FROM saMoneda m CROSS JOIN (SELECT TOP 1 g_moneda FROM par_emp) p`,
        'co_mone', 'mone_des'
    )
);

// ── Condiciones de Pago ─────────────────────────────────────────────────────
router.get('/condiciones_pago', (req, res) =>
    catalogEndpoint(req, res,
        `SELECT RTRIM(co_cond) AS co_cond, RTRIM(cond_des) AS cond_des, CAST(dias_cred AS INT) AS dias_cred FROM saCondicionPago`,
        'co_cond', 'cond_des'
    )
);

// ── Almacenes ───────────────────────────────────────────────────────────────
// Almacenes se exponen CON sede_id porque pueden diferir entre sedes
/**
 * @swagger
 * /api/v1/catalogos/almacenes:
 *   get:
 *     summary: Obtener lista de almacenes (discriminado por sede)
 *     tags: [Catalogos]
 *     responses:
 *       200:
 *         description: Lista de almacenes
 */
router.get('/almacenes', async (req, res) => {
    try {
        const requestedSede = req.query.sede || req.query.sede_id;
        const data = await aggregateRead(req.sqlAuth, async (pool, srv) => {
            // Si se especificó una sede y no es ésta, ignorar
            if (requestedSede && requestedSede !== 'Todas' && srv.id !== requestedSede && srv.name !== requestedSede) {
                return [];
            }

            const r = pool.request();
            let query = `SELECT RTRIM(co_alma) AS co_alma, RTRIM(des_alma) AS des_alma FROM saAlmacen`;
            
            // Filtrar por los códigos de sucursal de Profit asociados a esta sede
            const codes = (srv.profit_branch_codes || []).map(c => typeof c === 'string' ? c : c.code).filter(Boolean);
            if (codes.length > 0) {
                const params = codes.map((c, i) => `@c${i}`).join(',');
                codes.forEach((c, i) => r.input(`c${i}`, sql.Char, c));
                query += ` WHERE co_sucur IN (${params})`;
            }

            const result = await r.query(query);
            return result.recordset.map(a => ({ ...a, sede_id: srv.id, sede_nombre: srv.name }));
        });
        data.sort((a, b) => a.des_alma.localeCompare(b.des_alma));
        res.status(200).json({ success: true, count: data.length, data });
    } catch (error) {
        res.status(500).json({ success: false, message: 'Error interno.', error: error.message });
    }
});

// ── Precios (lista de listas de precios) ────────────────────────────────────
router.get('/precios', (req, res) =>
    catalogEndpoint(req, res,
        `SELECT RTRIM(co_precio) AS co_precio, RTRIM(prec_des) AS prec_des FROM saListaPrecio`,
        'co_precio', 'prec_des'
    )
);

// ── Tipos de Cliente ────────────────────────────────────────────────────────
router.get('/tipos_cliente', (req, res) =>
    catalogEndpoint(req, res,
        `SELECT RTRIM(tip_cli) AS tip_cli, RTRIM(tip_des) AS tip_des FROM saTipoCliente`,
        'tip_cli', 'tip_des'
    )
);

// ── Tasa de cambio ──────────────────────────────────────────────────────────
/**
 * @swagger
 * /api/v1/catalogos/tasa:
 *   get:
 *     summary: Obtener tasa de cambio (BCV) actual por sede
 *     tags: [Catalogos]
 *     responses:
 *       200:
 *         description: Tasa de cambio por sede
 */
router.get('/tasa', async (req, res) => {
    try {
        const data = await aggregateRead(req.sqlAuth, async (pool, srv) => {
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

// ── Tasa BCV (scraping directo desde bcv.org.ve) ────────────────────────────
/**
 * @swagger
 * /api/v1/catalogos/bcv:
 *   get:
 *     summary: Obtener tasa del dólar publicada por el BCV (scraping en tiempo real)
 *     tags: [Catalogos]
 *     responses:
 *       200:
 *         description: Tasa del dólar USD obtenida de bcv.org.ve
 */
router.get('/bcv', async (req, res) => {
    try {
        const https = require('https');

        const html = await new Promise((resolve, reject) => {
            const options = {
                hostname: 'www.bcv.org.ve',
                port: 443,
                path: '/',
                method: 'GET',
                headers: {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                    'Accept-Language': 'es-ES,es;q=0.9'
                },
                rejectUnauthorized: false,
                timeout: 15000
            };

            const request = https.request(options, (response) => {
                let data = '';
                response.on('data', chunk => data += chunk);
                response.on('end', () => resolve(data));
            });

            request.on('error', reject);
            request.on('timeout', () => {
                request.destroy();
                reject(new Error('Timeout al conectar con bcv.org.ve'));
            });

            request.end();
        });

        let tasa = null;
        let fuente = null;

        // Buscar: <div id="dolar" ...> ... <strong> 483,86950000 </strong>
        const dolarMatch = html.match(/id="dolar"[\s\S]*?<strong>\s*([\d.,]+)\s*<\/strong>/);
        if (dolarMatch && dolarMatch[1]) {
            let val = dolarMatch[1].trim();
            if (val.includes('.') && val.includes(',')) {
                val = val.replace(/\./g, '').replace(',', '.');
            } else {
                val = val.replace(',', '.');
            }
            tasa = parseFloat(val);
            fuente = 'id=dolar';
        }

        // Fallback: buscar "USD" seguido de un strong con cifra
        if (!tasa) {
            const usdMatch = html.match(/USD[\s\S]*?<strong>\s*([\d.,]+)\s*<\/strong>/i);
            if (usdMatch && usdMatch[1]) {
                let val = usdMatch[1].trim();
                if (val.includes('.') && val.includes(',')) {
                    val = val.replace(/\./g, '').replace(',', '.');
                } else {
                    val = val.replace(',', '.');
                }
                tasa = parseFloat(val);
                fuente = 'fallback-USD';
            }
        }

        if (tasa && !isNaN(tasa)) {
            console.log(`[BCV] Tasa USD obtenida: ${tasa} (${fuente})`);
            res.status(200).json({
                success: true,
                tasa,
                moneda: 'USD',
                fuente: 'https://www.bcv.org.ve/',
                metodo: fuente,
                fecha: new Date().toISOString()
            });
        } else {
            console.warn('[BCV] No se pudo extraer la tasa del HTML');
            res.status(404).json({
                success: false,
                message: 'No se pudo extraer la tasa del dólar de bcv.org.ve.'
            });
        }
    } catch (error) {
        console.error('[BCV] Error al consultar bcv.org.ve:', error.message);
        res.status(502).json({
            success: false,
            message: `Error al conectar con bcv.org.ve: ${error.message}`
        });
    }
});

module.exports = router;
