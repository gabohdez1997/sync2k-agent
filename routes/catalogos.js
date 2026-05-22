const express = require('express');
const router = express.Router();
const { sql, getPool } = require('../db');
const { aggregateRead, aggregateUnique, paginatedResponse, executeWrite, writeResponse, padProfit } = require('../helpers/multiSede');

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
        `SELECT RTRIM(co_lin) AS co_lin, RTRIM(lin_des) AS lin_des FROM saLineaArticulo`,
        'co_lin', 'co_lin'
    )
);

/**
 * @swagger
 * /api/v1/catalogos/lineas:
 *   post:
 *     summary: Crear nueva línea de artículo (broadcast a todas las sedes)
 *     tags: [Catalogos]
 */
router.post('/lineas', async (req, res) => {
    const { co_lin, lin_des } = req.body;
    if (!co_lin || !lin_des) {
        return res.status(400).json({ success: false, message: 'Código (co_lin) y descripción (lin_des) son obligatorios.' });
    }
    try {
        const user = req.headers['x-profit-user'];
        if (!user || user.trim() === '') {
            return res.status(400).json({ success: false, message: 'Usuario de Profit no proporcionado. Configure su "ID Agente SQL" en la plataforma.' });
        }
        const outcome = await executeWrite(null, req.sqlAuth, async (pool, srv) => {
            const sucursalObj = (srv?.profit_branch_codes || []).find(b => b && b.is_default) || (srv?.profit_branch_codes || [])[0];
            const sucursal = (sucursalObj && typeof sucursalObj === 'object' ? sucursalObj.code : sucursalObj) || '01';

            // Verificar si ya existe
            const check = await pool.request()
                .input('co_lin', sql.Char(6), padProfit(co_lin, 6))
                .query(`SELECT co_lin FROM saLineaArticulo WHERE RTRIM(co_lin) = RTRIM(@co_lin)`);
            if (check.recordset.length > 0) {
                throw new Error(`La línea "${co_lin.trim()}" ya existe en esta sede.`);
            }

            const result = await pool.request()
                .input('co_lin', sql.Char(6), padProfit(co_lin, 6))
                .input('lin_des', sql.VarChar(60), lin_des.trim())
                .input('user', sql.Char(6), padProfit(user, 6))
                .input('sucu', sql.Char(6), padProfit(sucursal, 6))
                .query(`
                    INSERT INTO saLineaArticulo (
                        co_lin, lin_des, dis_cen, co_imun, co_reten, comi_lin, comi_lin2,
                        i_lin_des, va, movil,
                        campo1, campo2, campo3, campo4, campo5, campo6, campo7, campo8,
                        co_us_in, co_sucu_in, fe_us_in, co_us_mo, co_sucu_mo, fe_us_mo,
                        revisado, trasnfe, rowguid, feccom, numcom
                    ) VALUES (
                        @co_lin, @lin_des, NULL, NULL, NULL, 0.00, 0.00,
                        '', '0', '0',
                        NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
                        @user, @sucu, GETDATE(), @user, @sucu, GETDATE(),
                        NULL, NULL, NEWID(), NULL, NULL
                    )
                `);
            
            if (result.rowsAffected[0] === 0) {
                throw new Error(`El registro fue rechazado por la BD (usuario '${user}' inválido u otra regla de negocio).`);
            }
            return { message: 'Línea creada.' };
        });
        return writeResponse(res, outcome);
    } catch (error) {
        res.status(500).json({ success: false, message: 'Error interno.', error: error.message });
    }
});

/**
 * @swagger
 * /api/v1/catalogos/lineas/{co_lin}:
 *   put:
 *     summary: Actualizar descripción de una línea (broadcast a todas las sedes)
 *     tags: [Catalogos]
 */
router.put('/lineas/:co_lin', async (req, res) => {
    const co_lin = req.params.co_lin;
    const { lin_des } = req.body;
    if (!lin_des) {
        return res.status(400).json({ success: false, message: 'La descripción (lin_des) es obligatoria.' });
    }
    try {
        const user = req.headers['x-profit-user'];
        if (!user || user.trim() === '') {
            return res.status(400).json({ success: false, message: 'Usuario de Profit no proporcionado. Configure su "ID Agente SQL" en la plataforma.' });
        }
        const outcome = await executeWrite(null, req.sqlAuth, async (pool, srv) => {
            const sucursalObj = (srv?.profit_branch_codes || []).find(b => b && b.is_default) || (srv?.profit_branch_codes || [])[0];
            const sucursal = (sucursalObj && typeof sucursalObj === 'object' ? sucursalObj.code : sucursalObj) || '01';

            const result = await pool.request()
                .input('co_lin', sql.Char(6), padProfit(co_lin, 6))
                .input('lin_des', sql.VarChar(60), lin_des.trim())
                .input('user', sql.Char(6), padProfit(user, 6))
                .input('sucu', sql.Char(6), padProfit(sucursal, 6))
                .query(`
                    UPDATE saLineaArticulo
                    SET lin_des = @lin_des,
                        co_us_mo = @user,
                        co_sucu_mo = @sucu,
                        fe_us_mo = GETDATE()
                    WHERE RTRIM(co_lin) = RTRIM(@co_lin)
                `);
            
            if (result.rowsAffected[0] === 0) {
                throw new Error(`El registro no fue actualizado (usuario '${user}' inválido, código no existe, u otra regla de negocio).`);
            }
            return { message: 'Línea actualizada.' };
        });
        return writeResponse(res, outcome);
    } catch (error) {
        res.status(500).json({ success: false, message: 'Error interno.', error: error.message });
    }
});

// ── SubLíneas (filtro opcional ?co_lin=XX) ──────────────────────────────────
router.get('/sublineas', async (req, res) => {
    const { co_lin } = req.query;
    const query = `SELECT RTRIM(co_subl) AS co_subl, RTRIM(subl_des) AS subl_des, RTRIM(co_lin) AS co_lin FROM saSubLinea`
        + (co_lin ? ` WHERE RTRIM(co_lin) = @co_lin` : ``);
    try {
        const data = await aggregateUnique(req.sqlAuth, async (pool) => {
            const r = pool.request();
            if (co_lin) r.input('co_lin', sql.VarChar, co_lin);
            return (await r.query(query)).recordset;
        }, 'co_subl', 'co_subl');
        res.status(200).json({ success: true, count: data.length, data });
    } catch (error) {
        res.status(500).json({ success: false, message: 'Error interno.', error: error.message });
    }
});

/**
 * @swagger
 * /api/v1/catalogos/sublineas:
 *   post:
 *     summary: Crear nueva sublínea de artículo (broadcast a todas las sedes)
 *     tags: [Catalogos]
 */
router.post('/sublineas', async (req, res) => {
    const { co_subl, subl_des, co_lin } = req.body;
    if (!co_subl || !subl_des || !co_lin) {
        return res.status(400).json({ success: false, message: 'Código (co_subl), descripción (subl_des) y línea (co_lin) son obligatorios.' });
    }
    try {
        const user = req.headers['x-profit-user'];
        if (!user || user.trim() === '') {
            return res.status(400).json({ success: false, message: 'Usuario de Profit no proporcionado. Configure su "ID Agente SQL" en la plataforma.' });
        }
        const outcome = await executeWrite(null, req.sqlAuth, async (pool, srv) => {
            const sucursalObj = (srv?.profit_branch_codes || []).find(b => b && b.is_default) || (srv?.profit_branch_codes || [])[0];
            const sucursal = (sucursalObj && typeof sucursalObj === 'object' ? sucursalObj.code : sucursalObj) || '01';

            const check = await pool.request()
                .input('co_subl', sql.Char(6), padProfit(co_subl, 6))
                .query(`SELECT co_subl FROM saSubLinea WHERE RTRIM(co_subl) = RTRIM(@co_subl)`);
            if (check.recordset.length > 0) {
                throw new Error(`La sublínea "${co_subl.trim()}" ya existe en esta sede.`);
            }

            const result = await pool.request()
                .input('co_subl', sql.Char(6), padProfit(co_subl, 6))
                .input('subl_des', sql.VarChar(60), subl_des.trim())
                .input('co_lin', sql.Char(6), padProfit(co_lin, 6))
                .input('user', sql.Char(6), padProfit(user, 6))
                .input('sucu', sql.Char(6), padProfit(sucursal, 6))
                .query(`
                    INSERT INTO saSubLinea (
                        co_lin, co_subl, subl_des, co_imun, co_reten, i_subl_des, movil,
                        campo1, campo2, campo3, campo4, campo5, campo6, campo7, campo8,
                        co_us_in, co_sucu_in, fe_us_in, co_us_mo, co_sucu_mo, fe_us_mo,
                        revisado, trasnfe, rowguid
                    ) VALUES (
                        @co_lin, @co_subl, @subl_des, NULL, NULL, NULL, '0',
                        NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
                        @user, @sucu, GETDATE(), @user, @sucu, GETDATE(),
                        NULL, NULL, NEWID()
                    )
                `);
            
            if (result.rowsAffected[0] === 0) {
                throw new Error(`El registro fue rechazado por la BD (usuario ${user} inválido u otra regla de negocio).`);
            }
            return { message: 'Sublínea creada.' };
        });
        return writeResponse(res, outcome);
    } catch (error) {
        res.status(500).json({ success: false, message: 'Error interno.', error: error.message });
    }
});

/**
 * @swagger
 * /api/v1/catalogos/sublineas/{co_subl}:
 *   put:
 *     summary: Actualizar una sublínea (broadcast a todas las sedes)
 *     tags: [Catalogos]
 */
router.put('/sublineas/:co_subl', async (req, res) => {
    const co_subl = req.params.co_subl;
    const { subl_des, co_lin } = req.body;
    if (!subl_des) {
        return res.status(400).json({ success: false, message: 'La descripción (subl_des) es obligatoria.' });
    }
    try {
        const user = req.headers['x-profit-user'];
        if (!user || user.trim() === '') {
            return res.status(400).json({ success: false, message: 'Usuario de Profit no proporcionado. Configure su "ID Agente SQL" en la plataforma.' });
        }
        const outcome = await executeWrite(null, req.sqlAuth, async (pool, srv) => {
            const sucursalObj = (srv?.profit_branch_codes || []).find(b => b && b.is_default) || (srv?.profit_branch_codes || [])[0];
            const sucursal = (sucursalObj && typeof sucursalObj === 'object' ? sucursalObj.code : sucursalObj) || '01';

            const updateFields = [`subl_des = @subl_des`, `co_us_mo = @user`, `co_sucu_mo = @sucu`, `fe_us_mo = GETDATE()`];
            const r = pool.request()
                .input('co_subl', sql.Char(6), padProfit(co_subl, 6))
                .input('subl_des', sql.VarChar(60), subl_des.trim())
                .input('user', sql.Char(6), padProfit(user, 6))
                .input('sucu', sql.Char(6), padProfit(sucursal, 6));

            if (co_lin) {
                r.input('co_lin', sql.Char(6), padProfit(co_lin, 6));
                updateFields.push(`co_lin = @co_lin`);
            }

            const result = await r.query(`UPDATE saSubLinea SET ${updateFields.join(', ')} WHERE RTRIM(co_subl) = RTRIM(@co_subl)`);
            
            if (result.rowsAffected[0] === 0) {
                throw new Error(`El registro no fue actualizado (usuario '${user}' inválido, código no existe, u otra regla de negocio).`);
            }
            return { message: 'Sublínea actualizada.' };
        });
        return writeResponse(res, outcome);
    } catch (error) {
        res.status(500).json({ success: false, message: 'Error interno.', error: error.message });
    }
});

// ── Categorías ──────────────────────────────────────────────────────────────
router.get('/categorias', (req, res) =>
    catalogEndpoint(req, res,
        `SELECT RTRIM(co_cat) AS co_cat, RTRIM(cat_des) AS cat_des FROM saCatArticulo`,
        'co_cat', 'co_cat'
    )
);

/**
 * @swagger
 * /api/v1/catalogos/categorias:
 *   post:
 *     summary: Crear nueva categoría de artículo (broadcast a todas las sedes)
 *     tags: [Catalogos]
 */
router.post('/categorias', async (req, res) => {
    const { co_cat, cat_des } = req.body;
    if (!co_cat || !cat_des) {
        return res.status(400).json({ success: false, message: 'Código (co_cat) y descripción (cat_des) son obligatorios.' });
    }
    try {
        const user = req.headers['x-profit-user'];
        if (!user || user.trim() === '') {
            return res.status(400).json({ success: false, message: 'Usuario de Profit no proporcionado. Configure su "ID Agente SQL" en la plataforma.' });
        }
        const outcome = await executeWrite(null, req.sqlAuth, async (pool, srv) => {
            const sucursalObj = (srv?.profit_branch_codes || []).find(b => b && b.is_default) || (srv?.profit_branch_codes || [])[0];
            const sucursal = (sucursalObj && typeof sucursalObj === 'object' ? sucursalObj.code : sucursalObj) || '01';

            const check = await pool.request()
                .input('co_cat', sql.Char(6), padProfit(co_cat, 6))
                .query(`SELECT co_cat FROM saCatArticulo WHERE RTRIM(co_cat) = RTRIM(@co_cat)`);
            if (check.recordset.length > 0) {
                throw new Error(`La categoría "${co_cat.trim()}" ya existe en esta sede.`);
            }

            const result = await pool.request()
                .input('co_cat', sql.Char(6), padProfit(co_cat, 6))
                .input('cat_des', sql.VarChar(60), cat_des.trim())
                .input('user', sql.Char(6), padProfit(user, 6))
                .input('sucu', sql.Char(6), padProfit(sucursal, 6))
                .query(`
                    INSERT INTO saCatArticulo (
                        co_cat, cat_des, co_imun, co_reten, feccom, numcom, dis_cen, movil,
                        campo1, campo2, campo3, campo4, campo5, campo6, campo7, campo8,
                        co_us_in, co_sucu_in, fe_us_in, co_us_mo, co_sucu_mo, fe_us_mo,
                        revisado, trasnfe, rowguid
                    ) VALUES (
                        @co_cat, @cat_des, NULL, NULL, NULL, NULL, NULL, '0',
                        NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
                        @user, @sucu, GETDATE(), @user, @sucu, GETDATE(),
                        NULL, NULL, NEWID()
                    )
                `);
            
            if (result.rowsAffected[0] === 0) {
                throw new Error(`El registro fue rechazado por la BD (usuario ${user} inválido u otra regla de negocio).`);
            }
            return { message: 'Categoría creada.' };
        });
        return writeResponse(res, outcome);
    } catch (error) {
        res.status(500).json({ success: false, message: 'Error interno.', error: error.message });
    }
});

/**
 * @swagger
 * /api/v1/catalogos/categorias/{co_cat}:
 *   put:
 *     summary: Actualizar descripción de una categoría (broadcast a todas las sedes)
 *     tags: [Catalogos]
 */
router.put('/categorias/:co_cat', async (req, res) => {
    const co_cat = req.params.co_cat;
    const { cat_des } = req.body;
    if (!cat_des) {
        return res.status(400).json({ success: false, message: 'La descripción (cat_des) es obligatoria.' });
    }
    try {
        const user = req.headers['x-profit-user'];
        if (!user || user.trim() === '') {
            return res.status(400).json({ success: false, message: 'Usuario de Profit no proporcionado. Configure su "ID Agente SQL" en la plataforma.' });
        }
        const outcome = await executeWrite(null, req.sqlAuth, async (pool, srv) => {
            const sucursalObj = (srv?.profit_branch_codes || []).find(b => b && b.is_default) || (srv?.profit_branch_codes || [])[0];
            const sucursal = (sucursalObj && typeof sucursalObj === 'object' ? sucursalObj.code : sucursalObj) || '01';

            const result = await pool.request()
                .input('co_cat', sql.Char(6), padProfit(co_cat, 6))
                .input('cat_des', sql.VarChar(60), cat_des.trim())
                .input('user', sql.Char(6), padProfit(user, 6))
                .input('sucu', sql.Char(6), padProfit(sucursal, 6))
                .query(`
                    UPDATE saCatArticulo
                    SET cat_des = @cat_des,
                        co_us_mo = @user,
                        co_sucu_mo = @sucu,
                        fe_us_mo = GETDATE()
                    WHERE RTRIM(co_cat) = RTRIM(@co_cat)
                `);
            
            if (result.rowsAffected[0] === 0) {
                throw new Error(`El registro no fue actualizado (usuario '${user}' inválido, código no existe, u otra regla de negocio).`);
            }
            return { message: 'Categoría actualizada.' };
        });
        return writeResponse(res, outcome);
    } catch (error) {
        res.status(500).json({ success: false, message: 'Error interno.', error: error.message });
    }
});

// ── Unidades de Medida ──────────────────────────────────────────────────────
router.get('/unidades', (req, res) =>
    catalogEndpoint(req, res,
        `SELECT RTRIM(co_uni) AS co_uni, RTRIM(des_uni) AS des_uni FROM saUnidad`,
        'co_uni', 'co_uni'
    )
);

// ── Colores ─────────────────────────────────────────────────────────────────
router.get('/colores', (req, res) =>
    catalogEndpoint(req, res,
        `SELECT RTRIM(co_color) AS co_color, RTRIM(des_color) AS des_color FROM saColor`,
        'co_color', 'co_color'
    )
);

// ── Ubicaciones ─────────────────────────────────────────────────────────────
router.get('/ubicaciones', (req, res) =>
    catalogEndpoint(req, res,
        `SELECT RTRIM(co_ubicacion) AS co_ubicacion, RTRIM(des_ubicacion) AS des_ubicacion FROM saUbicacion`,
        'co_ubicacion', 'co_ubicacion'
    )
);

// ── Procedencias ────────────────────────────────────────────────────────────
router.get('/procedencias', (req, res) =>
    catalogEndpoint(req, res,
        `SELECT RTRIM(cod_proc) AS cod_proc, RTRIM(des_proc) AS des_proc FROM saProcedencia`,
        'cod_proc', 'cod_proc'
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
        `SELECT RTRIM(tip_cli) AS tip_cli, RTRIM(des_tipo) AS des_tipo FROM saTipoCliente`,
        'tip_cli', 'des_tipo'
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
    console.log(`[TASA] Petición recibida. Auth User: ${req.profitUser || 'N/A'}`);
    try {
        const data = await aggregateRead(req.sqlAuth, async (pool, srv) => {
            console.log(`[TASA] Consultando sede: ${srv.name}...`);
            const r = await pool.request().query(
                `SELECT TOP 1 RTRIM(co_mone) AS co_mone, tasa_v AS tasa, fecha FROM saTasa
                 WHERE LTRIM(RTRIM(co_mone)) NOT IN ('BS','VES','VEB','VEF','BS.','BSF') 
                 ORDER BY fecha DESC`
            );
            console.log(`[TASA] Sede ${srv.name} encontró: ${r.recordset.length} registros.`);
            return r.recordset.map(t => ({ ...t, sede_id: srv.id, sede_nombre: srv.name }));
        });
        console.log(`[TASA] Respuesta final: ${data.length} tasas encontradas.`);
        res.status(200).json({ success: true, count: data.length, data });
    } catch (error) {
        console.error('[TASA] Error Crítico:', error.message);
        res.status(500).json({ success: false, message: 'Error interno.', error: error.message });
    }
});

/**
 * @swagger
 * /api/v1/catalogos/tasa:
 *   post:
 *     summary: Actualizar la tasa cambiaria en todas las sedes
 *     tags: [Catalogos]
 *     requestBody:
 *       required: true
 *       content:
 *         application/json:
 *           schema:
 *             type: object
 *             properties:
 *               tasa:
 *                 type: number
 *     responses:
 *       200:
 *         description: Tasa actualizada exitosamente
 */
router.post('/tasa', async (req, res) => {
    let { tasa } = req.body;
    if (!tasa || isNaN(tasa)) {
        return res.status(400).json({ success: false, message: 'Tasa inválida.' });
    }
    
    // Aplicar redondeo matemático a 2 decimales para Profit Plus
    tasa = Math.round(Number(tasa) * 100) / 100;

    try {
        const outcome = await executeWrite(null, req.sqlAuth, async (pool) => {
            // Verificar qué códigos de moneda existen (USD o US$)
            const moneRes = await pool.request().query("SELECT co_mone FROM saMoneda WHERE LTRIM(RTRIM(co_mone)) IN ('USD', 'US$')");
            const codes = moneRes.recordset.map(r => r.co_mone.trim());

            if (codes.length === 0) {
                throw new Error('No se encontró la moneda USD o US$ en la tabla saMoneda.');
            }

            for (const code of codes) {
                // Intentar obtener el usuario de las credenciales SQL (x-sql-auth)
                let user = 'RECON';
                if (req.sqlAuth && req.sqlAuth.user) {
                    user = req.sqlAuth.user;
                } else {
                    user = req.headers['x-profit-user'] || 'RECON';
                }

                await pool.request()
                    .input('tasa', sql.Decimal(18, 6), tasa)
                    .input('mone', sql.Char(6), code)
                    .input('user', sql.VarChar, user)
                    .query(`
                        DECLARE @today DATETIME = CONVERT(VARCHAR(10), GETDATE(), 120);
                        DECLARE @finalUser CHAR(10) = ISNULL(NULLIF(RTRIM(@user), ''), 'RECON');

                        IF EXISTS (SELECT 1 FROM saTasa WHERE LTRIM(RTRIM(co_mone)) = LTRIM(RTRIM(@mone)) AND CONVERT(VARCHAR(10), fecha, 120) = @today)
                        BEGIN
                            UPDATE saTasa 
                            SET tasa_v = @tasa, 
                                tasa_c = @tasa,
                                co_us_mo = @finalUser,
                                fe_us_mo = GETDATE()
                            WHERE LTRIM(RTRIM(co_mone)) = LTRIM(RTRIM(@mone)) AND CONVERT(VARCHAR(10), fecha, 120) = @today
                        END
                        ELSE
                        BEGIN
                            INSERT INTO saTasa (co_mone, tasa_v, tasa_c, fecha, co_us_in, fe_us_in, co_us_mo, fe_us_mo) 
                            VALUES (@mone, @tasa, @tasa, GETDATE(), @finalUser, GETDATE(), @finalUser, GETDATE())
                        END
                    `);
            }
            return { message: 'Tasa actualizada correctamente.' };
        });

        return writeResponse(res, outcome);
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

        // Buscar: <div id="dolar" ...> ... <strong...> 483,86950000 </strong>
        const dolarMatch = html.match(/id="dolar"[\s\S]*?<strong[^>]*>\s*([\d.,]+)\s*<\/strong>/);
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
            const usdMatch = html.match(/USD[\s\S]*?<strong[^>]*>\s*([\d.,]+)/i);
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
