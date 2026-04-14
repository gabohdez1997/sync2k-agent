const express = require('express');
const router = express.Router();
const { sql, getPool, getServers } = require('../db');
const { executeWrite, writeResponse, paginatedResponse } = require('../helpers/multiSede');

// ── Query reutilizable de tasa de cambio ────────────────────────────────────
const QUERY_TASA = `SELECT TOP 1 tasa_v AS tasa_cambio FROM saTasa
                    WHERE LTRIM(RTRIM(co_mone)) IN ('US$','USD') ORDER BY fecha DESC`;

// ── Helper: enriquece artículos con precios y stock ─────────────────────────
async function enrichArticulos(pool, articulos, tasa) {
    if (!articulos.length) return articulos;
    const ids = articulos.map(a => `'${a.co_art.replace(/'/g, "''")}'`).join(',');

    const [resStock, resPrecios] = await Promise.all([
        pool.request().query(`
            SELECT RTRIM(s.co_art) AS co_art, RTRIM(s.co_alma) AS co_alma,
                   RTRIM(a.des_alma) AS des_alma,
                   SUM(CASE WHEN RTRIM(s.tipo)='ACT' THEN s.stock ELSE 0 END)
                   - SUM(CASE WHEN RTRIM(s.tipo)='COM' THEN s.stock ELSE 0 END)
                   - SUM(CASE WHEN RTRIM(s.tipo)='DES' THEN s.stock ELSE 0 END) AS stock
            FROM saStockAlmacen s LEFT JOIN saAlmacen a ON s.co_alma = a.co_alma
            WHERE LTRIM(RTRIM(s.co_art)) IN (${ids})
            GROUP BY s.co_art, s.co_alma, a.des_alma
            HAVING (SUM(CASE WHEN RTRIM(s.tipo)='ACT' THEN s.stock ELSE 0 END)
                   - SUM(CASE WHEN RTRIM(s.tipo)='COM' THEN s.stock ELSE 0 END)
                   - SUM(CASE WHEN RTRIM(s.tipo)='DES' THEN s.stock ELSE 0 END)) > 0
        `),
        pool.request().query(`
            WITH UP AS (
                SELECT RTRIM(co_art) AS co_art, RTRIM(co_precio) AS id_precio,
                       monto AS precio, RTRIM(co_mone) AS moneda,
                       ROW_NUMBER() OVER(PARTITION BY co_art, co_precio ORDER BY desde DESC) AS rn
                FROM saArtPrecio
                WHERE LTRIM(RTRIM(co_art)) IN (${ids})
                  AND Inactivo = 0 AND GETDATE() >= desde AND (hasta IS NULL OR GETDATE() <= hasta)
            )
            SELECT co_art, id_precio, precio, moneda FROM UP WHERE rn = 1
        `)
    ]);

    const stockMap = {};
    resStock.recordset.forEach(s => { (stockMap[s.co_art] = stockMap[s.co_art] || []).push({ co_alma: s.co_alma, des_alma: s.des_alma, stock: s.stock }); });
    const precioMap = {};
    resPrecios.recordset.forEach(p => {
        (precioMap[p.co_art] = precioMap[p.co_art] || []).push({
            id_precio: p.id_precio, precio: p.precio, moneda: p.moneda,
            precio_ves: ((p.moneda || '').includes('US') ? Number((p.precio * tasa).toFixed(2)) : p.precio)
        });
    });

    return articulos.map(a => ({
        ...a,
        tasa_bcv: tasa,
        disponibilidad: stockMap[a.co_art] || [],
        precios: precioMap[a.co_art] || []
    }));
}

/**
 * @swagger
 * tags:
 *   name: Articulos
 *   description: Gestión de artículos y productos
 */

// ────────────────────────────────────────────────────────────────────────────
// 1. GET /api/v1/articulos — Listado paginado de todas las sedes
// ────────────────────────────────────────────────────────────────────────────
/**
 * @swagger
 * /api/v1/articulos:
 *   get:
 *     summary: Obtener listado paginado de artículos de todas las sedes
 *     tags: [Articulos]
 *     parameters:
 *       - in: query
 *         name: page
 *         schema:
 *           type: integer
 *           default: 1
 *         description: Número de página
 *       - in: query
 *         name: limit
 *         schema:
 *           type: integer
 *           default: 10
 *         description: Cantidad de items por página
 *     responses:
 *       200:
 *         description: Listado de artículos obtenido exitosamente
 *       500:
 *         description: Error del servidor
 */
router.get('/', async (req, res) => {
    try {
        const page = parseInt(req.query.page) || 1;
        const limit = parseInt(req.query.limit) || 10;
        const requestedSede = req.query.sede || req.query.sede_id;
        const reqSort = req.query.sort; // Capturar el parámetro de ordenamiento

        // Filtramos servidores
        let servers = getServers();
        if (requestedSede && requestedSede !== "Todas") {
            servers = servers.filter(srv => srv.id === requestedSede || srv.name === requestedSede);
        }

        if (servers.length === 0) {
            return res.status(200).json({ success: true, page, limit, total_items: 0, total_pages: 0, count: 0, data: [] });
        }

        // Configuración de Ordenamiento (Sort por Precio)
        let orderByClause = 'ORDER BY a.art_des ASC';
        let joinPrecioClause = '';
        if (reqSort === 'price_asc') {
            joinPrecioClause = "LEFT JOIN saArtPrecio pr ON a.co_art = pr.co_art AND pr.co_precio = '01'";
            orderByClause = 'ORDER BY pr.monto ASC, a.art_des ASC';
        } else if (reqSort === 'price_desc') {
            joinPrecioClause = "LEFT JOIN saArtPrecio pr ON a.co_art = pr.co_art AND pr.co_precio = '01'";
            orderByClause = 'ORDER BY pr.monto DESC, a.art_des ASC';
        }

        const co_alma = req.query.co_alma;

        const in_stock_all = req.query.in_stock === 'all';

        // 1. Obtener listado básico filtrando nativamente por stock 
        const allData = await Promise.all(servers.map(async (srv) => {
            try {
                const pool = await getPool(srv.id, req.sqlAuth);
                const r = pool.request();
                if (co_alma) r.input('co_alma', sql.VarChar, co_alma);

                const topCount = page * limit;
                const resData = await r.query(
                    `SELECT TOP (${topCount}) RTRIM(a.co_art) AS co_art, RTRIM(a.art_des) AS descripcion,
                             RTRIM(a.tipo) AS tipo, RTRIM(a.modelo) AS modelo, RTRIM(a.ref) AS referencia,
                             RTRIM(l.lin_des) AS linea, RTRIM(sl.subl_des) AS sublinea, RTRIM(c.cat_des) AS categoria,
                             RTRIM(au.co_ubicacion) AS co_ubicacion, RTRIM(u1.des_ubicacion) AS ubicacion,
                             RTRIM(au.co_ubicacion2) AS co_ubicacion2, RTRIM(u2.des_ubicacion) AS ubicacion2,
                             RTRIM(au.co_ubicacion3) AS co_ubicacion3, RTRIM(u3.des_ubicacion) AS ubicacion3,
                             RTRIM(aun.co_uni) AS co_uni, RTRIM(un.des_uni) AS unidad,
                             CAST(CASE WHEN a.art_des LIKE '%TIPO B%' OR c.cat_des LIKE '%TIPO B%' OR sl.subl_des LIKE '%TIPO B%' OR l.lin_des LIKE '%SEGUNDA%' OR sl.subl_des LIKE '%SEGUNDA%' OR c.cat_des LIKE '%SEGUNDA%' OR a.art_des LIKE '%SEGUNDA%' THEN 1 ELSE 0 END AS bit) AS oferta
                             ${joinPrecioClause ? ', ISNULL(pr.monto,0) AS precio_base' : ''}
                      FROM saArticulo a
                      LEFT JOIN saLineaArticulo l ON a.co_lin = l.co_lin
                      LEFT JOIN saSubLinea sl ON a.co_subl = sl.co_subl
                      LEFT JOIN saCatArticulo c ON a.co_cat = c.co_cat
                      LEFT JOIN saArtUbicacion au ON a.co_art = au.co_art ${co_alma ? "AND au.co_alma = @co_alma" : ""}
                      LEFT JOIN saUbicacion u1 ON au.co_ubicacion = u1.co_ubicacion
                      LEFT JOIN saUbicacion u2 ON au.co_ubicacion2 = u2.co_ubicacion
                      LEFT JOIN saUbicacion u3 ON au.co_ubicacion3 = u3.co_ubicacion
                      LEFT JOIN (
                          SELECT co_art, co_uni, 
                                 ROW_NUMBER() OVER(PARTITION BY co_art ORDER BY uni_principal DESC) as rn
                          FROM saArtUnidad
                      ) aun ON LTRIM(RTRIM(a.co_art)) = LTRIM(RTRIM(aun.co_art)) AND aun.rn = 1
                      LEFT JOIN saUnidad un ON LTRIM(RTRIM(aun.co_uni)) = LTRIM(RTRIM(un.co_uni))
                      ${joinPrecioClause}
                      WHERE a.anulado = 0 
                      AND (LTRIM(RTRIM(a.co_lin)) = '09' OR RTRIM(a.tipo) IN ('S', '2') OR ${in_stock_all ? '1=1' : `EXISTS (
                          SELECT 1 FROM saStockAlmacen st 
                          WHERE st.co_art = a.co_art AND st.stock > 0
                          ${co_alma ? ' AND st.co_alma = @co_alma' : ''}
                      )`})
                      ${orderByClause}`
                );
                return resData.recordset.map(a => ({ ...a, sede_id: srv.id, sede_nombre: srv.name }));
            } catch (e) {
                console.error(`[GET /] Error en sede ${srv.id}:`, e.message);
                return [];
            }
        }));

        const combined = [].concat(...allData);

        // Re-ordenar combinaciones inter-servidor según precios directos
        if (reqSort === 'price_asc') {
            combined.sort((a, b) => (a.precio_base || 0) - (b.precio_base || 0));
        } else if (reqSort === 'price_desc') {
            combined.sort((a, b) => (b.precio_base || 0) - (a.precio_base || 0));
        } else {
            combined.sort((a, b) => a.descripcion.localeCompare(b.descripcion));
        }

        // 2. Paginar los resultados YA filtrados y ordenados
        const total = combined.length;
        const paginated = combined.slice((page - 1) * limit, page * limit);

        // 3. Enriquecer solo los artículos de la página actual
        const enrichedItems = [];
        const itemsBySede = paginated.reduce((acc, item) => {
            acc[item.sede_id] = acc[item.sede_id] || [];
            acc[item.sede_id].push(item);
            return acc;
        }, {});

        await Promise.all(Object.entries(itemsBySede).map(async ([sedeId, items]) => {
            try {
                const pool = await getPool(sedeId, req.sqlAuth);
                const resTasa = await pool.request().query(QUERY_TASA);
                const tasa = resTasa.recordset[0]?.tasa_cambio || 1;
                const enriched = await enrichArticulos(pool, items, tasa);
                enrichedItems.push(...enriched);
            } catch (e) {
                console.error(`[GET /] Error enriqueciendo sede ${sedeId}:`, e.message);
                enrichedItems.push(...items.map(i => ({ ...i, error_enriquecimiento: e.message })));
            }
        }));

        // Mantener orden luego del enriquecimiento paralelo asíncrono, usando el arreglo `.precios` oficial devuelto.
        if (reqSort === 'price_asc') {
            enrichedItems.sort((a, b) => {
                const pA = (a.precios && a.precios.length > 0) ? a.precios[0].precio : 0;
                const pB = (b.precios && b.precios.length > 0) ? b.precios[0].precio : 0;
                return pA - pB;
            });
        } else if (reqSort === 'price_desc') {
            enrichedItems.sort((a, b) => {
                const pA = (a.precios && a.precios.length > 0) ? a.precios[0].precio : 0;
                const pB = (b.precios && b.precios.length > 0) ? b.precios[0].precio : 0;
                return pB - pA;
            });
        } else {
            enrichedItems.sort((a, b) => a.descripcion.localeCompare(b.descripcion));
        }

        return res.status(200).json({
            success: true,
            page,
            limit,
            total_items: total,
            total_pages: Math.ceil(total / limit),
            count: enrichedItems.length,
            data: enrichedItems
        });

    } catch (error) {
        res.status(500).json({ success: false, message: 'Error al consultar artículos.', error: error.message });
    }
});



// ────────────────────────────────────────────────────────────────────────────
// 2. GET /api/v1/articulos/search — Búsqueda con filtros
// ────────────────────────────────────────────────────────────────────────────
/**
 * @swagger
 * /api/v1/articulos/search:
 *   get:
 *     summary: Búsqueda de artículos con filtros (co_art, descripcion, modelo, etc.)
 *     tags: [Articulos]
 *     parameters:
 *       - in: query
 *         name: co_art
 *         schema:
 *           type: string
 *       - in: query
 *         name: descripcion
 *         schema:
 *           type: string
 *       - in: query
 *         name: page
 *         schema:
 *           type: integer
 *           default: 1
 *       - in: query
 *         name: limit
 *         schema:
 *           type: integer
 *           default: 30
 *     responses:
 *       200:
 *         description: Resultados de la búsqueda
 *       400:
 *         description: Falta parámetro de búsqueda
 *       500:
 *         description: Error del servidor
 */
router.get('/search', async (req, res) => {
    try {
        const page = parseInt(req.query.page) || 1;
        const limit = parseInt(req.query.limit) || 30;
        const requestedSede = req.query.sede || req.query.sede_id;
        const reqSort = req.query.sort;

        const FIELD_MAP = {
            co_art: 'a.co_art', descripcion: 'a.art_des', modelo: 'a.modelo',
            referencia: 'a.ref', tipo: 'a.tipo', linea: 'a.co_lin',
            sublinea: 'a.co_subl', categoria: 'a.co_cat', proveedor: 'a.co_prov',
            linea_nombre: 'l.lin_des', sublinea_nombre: 'sl.subl_des', categoria_nombre: 'c.cat_des',
            co_ubicacion: 'au.co_ubicacion', ubicacion: 'u1.des_ubicacion',
            co_ubicacion2: 'au.co_ubicacion2', ubicacion2: 'u2.des_ubicacion',
            co_ubicacion3: 'au.co_ubicacion3', ubicacion3: 'u3.des_ubicacion'
        };

        const filters = Object.entries(req.query)
            .map(([k, v]) => {
                const isNegative = k.endsWith('!');
                const baseKey = isNegative ? k.slice(0, -1) : k;
                return { originalKey: k, baseKey, value: v, isNegative };
            })
            .filter(({ baseKey, value }) => (FIELD_MAP[baseKey] && value) || (baseKey === 'oferta' && value))
            .map(({ originalKey, baseKey, value, isNegative }) => {
                if (baseKey === 'oferta') {
                    let isOferta = value === 'true' || value === '1';
                    if (isNegative) isOferta = !isOferta;
                    return { param: baseKey, isOferta };
                }
                return { param: isNegative ? `${baseKey}_neg` : baseKey, column: FIELD_MAP[baseKey], value, isNegative };
            });

        if (!filters.length && !req.query.sede && !req.query.sort) {
            return res.status(400).json({ success: false, message: 'Especifique al menos un parámetro de búsqueda.' });
        }

        const co_alma = req.query.co_alma;
        const in_stock_all = req.query.in_stock === 'all';

        const ofertaCondition = `(a.art_des LIKE '%TIPO B%' OR c.cat_des LIKE '%TIPO B%' OR sl.subl_des LIKE '%TIPO B%' OR l.lin_des LIKE '%SEGUNDA%' OR sl.subl_des LIKE '%SEGUNDA%' OR c.cat_des LIKE '%SEGUNDA%' OR a.art_des LIKE '%SEGUNDA%')`;
        const normalFilters = filters.filter(f => !f.hasOwnProperty('isOferta'));
        const ofertaFilter = filters.find(f => f.hasOwnProperty('isOferta'));

        let whereClause = 'WHERE a.anulado = 0 ';
        if (!in_stock_all) {
            whereClause += " AND (LTRIM(RTRIM(a.co_lin)) = '09' OR RTRIM(a.tipo) IN ('S','2') OR EXISTS (SELECT 1 FROM saStockAlmacen st WHERE st.co_art = a.co_art AND st.stock > 0 ";
            if (co_alma) whereClause += ' AND st.co_alma = @co_alma ';
            whereClause += ')) ';
        }

        if (normalFilters.length > 0) {
            whereClause += normalFilters.map(f => {
                if (f.isNegative) {
                    return `AND ISNULL(${f.column}, '') NOT LIKE '%' + @${f.param} + '%'`;
                }
                return `AND ${f.column} LIKE '%' + @${f.param} + '%'`;
            }).join(' ');
        }

        if (ofertaFilter) {
            whereClause += ofertaFilter.isOferta ? ` AND ${ofertaCondition}` : ` AND NOT ${ofertaCondition}`;
        }

        // --- LÓGICA DE ORDENAMIENTO ---
        let orderByClause = 'ORDER BY a.art_des ASC';
        let joinPrecioClause = '';
        if (reqSort === 'price_asc') {
            joinPrecioClause = "LEFT JOIN saArtPrecio pr ON a.co_art = pr.co_art AND pr.co_precio = '01'";
            orderByClause = 'ORDER BY pr.monto ASC, a.art_des ASC';
        } else if (reqSort === 'price_desc') {
            joinPrecioClause = "LEFT JOIN saArtPrecio pr ON a.co_art = pr.co_art AND pr.co_precio = '01'";
            orderByClause = 'ORDER BY pr.monto DESC, a.art_des ASC';
        }

        // --- SEDES ---
        let servers = getServers();
        if (requestedSede && requestedSede !== "Todas") {
            servers = servers.filter(srv => srv.id === requestedSede || srv.name === requestedSede);
        }

        if (servers.length === 0) {
            return res.status(200).json({ success: true, page, limit, total_items: 0, total_pages: 0, count: 0, data: [] });
        }

        // 1. Obtener listado básico filtrado en SQL
        const allData = await Promise.all(servers.map(async (srv) => {
            try {
                const pool = await getPool(srv.id, req.sqlAuth);
                const r = pool.request();
                normalFilters.forEach(f => r.input(f.param, sql.VarChar, f.value));
                if (co_alma) r.input('co_alma', sql.VarChar, co_alma);

                const resData = await r.query(
                    `SELECT RTRIM(a.co_art) AS co_art, RTRIM(a.art_des) AS descripcion,
                            RTRIM(a.tipo) AS tipo, RTRIM(a.modelo) AS modelo, RTRIM(a.ref) AS referencia,
                            RTRIM(l.lin_des) AS linea, RTRIM(sl.subl_des) AS sublinea, RTRIM(c.cat_des) AS categoria,
                            RTRIM(au.co_ubicacion) AS co_ubicacion, RTRIM(u1.des_ubicacion) AS ubicacion,
                            RTRIM(au.co_ubicacion2) AS co_ubicacion2, RTRIM(u2.des_ubicacion) AS ubicacion2,
                            RTRIM(au.co_ubicacion3) AS co_ubicacion3, RTRIM(u3.des_ubicacion) AS ubicacion3,
                            RTRIM(aun.co_uni) AS co_uni, RTRIM(un.des_uni) AS unidad,
                            CAST(CASE WHEN a.art_des LIKE '%TIPO B%' OR c.cat_des LIKE '%TIPO B%' OR sl.subl_des LIKE '%TIPO B%' OR l.lin_des LIKE '%SEGUNDA%' OR sl.subl_des LIKE '%SEGUNDA%' OR c.cat_des LIKE '%SEGUNDA%' OR a.art_des LIKE '%SEGUNDA%' THEN 1 ELSE 0 END AS bit) AS oferta
                            ${joinPrecioClause ? ', ISNULL(pr.monto,0) AS precio_base' : ''}
                     FROM saArticulo a
                     LEFT JOIN saLineaArticulo l ON a.co_lin = l.co_lin
                     LEFT JOIN saSubLinea sl ON a.co_subl = sl.co_subl
                     LEFT JOIN saCatArticulo c ON a.co_cat = c.co_cat
                     LEFT JOIN saArtUbicacion au ON a.co_art = au.co_art ${co_alma ? "AND au.co_alma = @co_alma" : ""}
                     LEFT JOIN saUbicacion u1 ON au.co_ubicacion = u1.co_ubicacion
                     LEFT JOIN saUbicacion u2 ON au.co_ubicacion2 = u2.co_ubicacion
                     LEFT JOIN saUbicacion u3 ON au.co_ubicacion3 = u3.co_ubicacion
                     LEFT JOIN (
                          SELECT co_art, co_uni, 
                                 ROW_NUMBER() OVER(PARTITION BY co_art ORDER BY uni_principal DESC) as rn
                          FROM saArtUnidad
                     ) aun ON LTRIM(RTRIM(a.co_art)) = LTRIM(RTRIM(aun.co_art)) AND aun.rn = 1
                     LEFT JOIN saUnidad un ON LTRIM(RTRIM(aun.co_uni)) = LTRIM(RTRIM(un.co_uni))
                     ${joinPrecioClause}
                     ${whereClause} 
                     ${orderByClause}`
                );
                return resData.recordset.map(a => ({ ...a, sede_id: srv.id, sede_nombre: srv.name }));
            } catch (e) {
                console.error(`[GET /search] Error en sede ${srv.id}:`, e.message);
                return [];
            }
        }));

        let combined = [].concat(...allData);
        let total = combined.length;

        // 2. Orden Global (Cross-Server)
        if (reqSort === 'price_asc') {
            combined.sort((a, b) => (a.precio_base || 0) - (b.precio_base || 0));
        } else if (reqSort === 'price_desc') {
            combined.sort((a, b) => (b.precio_base || 0) - (a.precio_base || 0));
        } else {
            combined.sort((a, b) => a.descripcion.localeCompare(b.descripcion));
        }

        // 3. Paginación
        const paginated = combined.slice((page - 1) * limit, page * limit);

        // 4. Enriquecimiento paralelo solo de la página actual
        const finalItems = [];
        const itemsBySede = paginated.reduce((acc, item) => {
            acc[item.sede_id] = acc[item.sede_id] || [];
            acc[item.sede_id].push(item);
            return acc;
        }, {});

        await Promise.all(Object.entries(itemsBySede).map(async ([sedeId, items]) => {
            try {
                const pool = await getPool(sedeId, req.sqlAuth);
                const resTasa = await pool.request().query(QUERY_TASA);
                const tasa = resTasa.recordset[0]?.tasa_cambio || 1;
                const enriched = await enrichArticulos(pool, items, tasa);
                finalItems.push(...enriched);
            } catch (e) {
                finalItems.push(...items.map(i => ({ ...i, error_enriquecimiento: e.message })));
            }
        }));

        // Mantener orden final exacto según precios enriquecidos
        if (reqSort === 'price_asc') {
            finalItems.sort((a, b) => {
                const pA = (a.precios && a.precios.length > 0) ? a.precios[0].precio : 0;
                const pB = (b.precios && b.precios.length > 0) ? b.precios[0].precio : 0;
                return pA - pB;
            });
        } else if (reqSort === 'price_desc') {
            finalItems.sort((a, b) => {
                const pA = (a.precios && a.precios.length > 0) ? a.precios[0].precio : 0;
                const pB = (b.precios && b.precios.length > 0) ? b.precios[0].precio : 0;
                return pB - pA;
            });
        } else {
            finalItems.sort((a, b) => a.descripcion.localeCompare(b.descripcion));
        }

        return res.status(200).json({
            success: true,
            page,
            limit,
            total_items: total,
            total_pages: Math.ceil(total / limit),
            count: finalItems.length,
            data: finalItems
        });
    } catch (error) {
        res.status(500).json({ success: false, message: 'Error en búsqueda de artículos.', error: error.message });
    }
});




// ────────────────────────────────────────────────────────────────────────────
// 3. GET /api/v1/articulos/:co_art — Detalle completo por sede
// ────────────────────────────────────────────────────────────────────────────
/**
 * @swagger
 * /api/v1/articulos/{co_art}:
 *   get:
 *     summary: Detalle completo de un artículo por su código (en todas las sedes)
 *     tags: [Articulos]
 *     parameters:
 *       - in: path
 *         name: co_art
 *         required: true
 *         schema:
 *           type: string
 *         description: Código del artículo
 *     responses:
 *       200:
 *         description: Detalle del artículo
 *       404:
 *         description: Artículo no encontrado
 *       500:
 *         description: Error del servidor
 */
router.get('/:co_art', async (req, res) => {
    try {
        const { co_art } = req.params;
        const servers = getServers();

        const results = await Promise.all(servers.map(async (srv) => {
            try {
                const pool = await getPool(srv.id, req.sqlAuth);

                const [resArt, resStock, resPre, resTasa] = await Promise.all([
                    pool.request().input('co_art', sql.VarChar, co_art).query(
                        `SELECT RTRIM(a.co_art) AS co_art, RTRIM(a.art_des) AS descripcion,
                                a.anulado, RTRIM(a.tipo) AS tipo_articulo,
                                RTRIM(l.lin_des) AS linea, RTRIM(sl.subl_des) AS sublinea, RTRIM(c.cat_des) AS categoria,
                                RTRIM(au.co_ubicacion) AS co_ubicacion, RTRIM(u1.des_ubicacion) AS ubicacion,
                                RTRIM(au.co_ubicacion2) AS co_ubicacion2, RTRIM(u2.des_ubicacion) AS ubicacion2,
                                RTRIM(au.co_ubicacion3) AS co_ubicacion3, RTRIM(u3.des_ubicacion) AS ubicacion3,
                                RTRIM(aun.co_uni) AS co_uni, RTRIM(un.des_uni) AS unidad,
                                CAST(CASE WHEN a.art_des LIKE '%TIPO B%' OR c.cat_des LIKE '%TIPO B%' OR sl.subl_des LIKE '%TIPO B%' OR l.lin_des LIKE '%SEGUNDA%' OR sl.subl_des LIKE '%SEGUNDA%' OR c.cat_des LIKE '%SEGUNDA%' OR a.art_des LIKE '%SEGUNDA%' THEN 1 ELSE 0 END AS bit) AS oferta
                         FROM saArticulo a
                         LEFT JOIN saLineaArticulo l ON a.co_lin = l.co_lin
                         LEFT JOIN saSubLinea sl ON a.co_subl = sl.co_subl
                         LEFT JOIN saCatArticulo c ON a.co_cat = c.co_cat
                         LEFT JOIN saArtUbicacion au ON a.co_art = au.co_art
                         LEFT JOIN saUbicacion u1 ON au.co_ubicacion = u1.co_ubicacion
                         LEFT JOIN saUbicacion u2 ON au.co_ubicacion2 = u2.co_ubicacion
                         LEFT JOIN saUbicacion u3 ON au.co_ubicacion3 = u3.co_ubicacion
                         LEFT JOIN (
                             SELECT co_art, co_uni, 
                                    ROW_NUMBER() OVER(PARTITION BY co_art ORDER BY uni_principal DESC) as rn
                             FROM saArtUnidad
                         ) aun ON LTRIM(RTRIM(a.co_art)) = LTRIM(RTRIM(aun.co_art)) AND aun.rn = 1
                         LEFT JOIN saUnidad un ON LTRIM(RTRIM(aun.co_uni)) = LTRIM(RTRIM(un.co_uni))
                         WHERE LTRIM(RTRIM(a.co_art)) = LTRIM(RTRIM(@co_art))`
                    ),
                    pool.request().input('co_art', sql.VarChar, co_art).query(
                        `SELECT RTRIM(co_alma) AS co_alma,
                                SUM(CASE WHEN RTRIM(tipo)='ACT' THEN stock ELSE 0 END)
                                - SUM(CASE WHEN RTRIM(tipo)='COM' THEN stock ELSE 0 END)
                                - SUM(CASE WHEN RTRIM(tipo)='DES' THEN stock ELSE 0 END) AS stock
                         FROM saStockAlmacen WHERE LTRIM(RTRIM(co_art)) = LTRIM(RTRIM(@co_art))
                         GROUP BY co_alma
                         HAVING (SUM(CASE WHEN RTRIM(tipo)='ACT' THEN stock ELSE 0 END)
                                - SUM(CASE WHEN RTRIM(tipo)='COM' THEN stock ELSE 0 END)
                                - SUM(CASE WHEN RTRIM(tipo)='DES' THEN stock ELSE 0 END)) > 0`
                    ),
                    pool.request().input('co_art', sql.VarChar, co_art).query(
                        `WITH UP AS (SELECT RTRIM(co_precio) AS id_precio, monto AS precio,
                                RTRIM(co_mone) AS moneda,
                                ROW_NUMBER() OVER(PARTITION BY co_precio ORDER BY desde DESC) AS rn
                         FROM saArtPrecio WHERE LTRIM(RTRIM(co_art)) = LTRIM(RTRIM(@co_art))
                           AND Inactivo=0 AND GETDATE()>=desde AND (hasta IS NULL OR GETDATE()<=hasta))
                         SELECT id_precio, precio, moneda FROM UP WHERE rn=1 ORDER BY id_precio`
                    ),
                    pool.request().query(QUERY_TASA)
                ]);

                if (!resArt.recordset.length) return null;
                const tasa = resTasa.recordset[0]?.tasa_cambio || 1;

                return {
                    sede_id: srv.id,
                    sede_nombre: srv.name,
                    ...resArt.recordset[0],
                    tasa_cambio: tasa,
                    disponibilidad: resStock.recordset,
                    total_stock: resStock.recordset.reduce((s, r) => s + r.stock, 0),
                    precios: resPre.recordset.map(p => ({
                        ...p,
                        precio_ves: (p.moneda || '').includes('US') ? Number((p.precio * tasa).toFixed(2)) : p.precio
                    }))
                };
            } catch (e) {
                return { sede_id: srv.id, sede_nombre: srv.name, error: e.message };
            }
        }));

        const found = results.filter(r => r && !r.error);
        if (!found.length) {
            return res.status(404).json({
                success: false,
                message: 'Artículo no encontrado en ninguna sede.',
                sedes: results.filter(r => r?.error)
            });
        }

        res.status(200).json({
            success: true,
            count: found.length,
            total_stock_global: found.reduce((s, r) => s + r.total_stock, 0),
            data: results.filter(r => r !== null)
        });

    } catch (error) {
        res.status(500).json({ success: false, message: 'Error al consultar artículo.', error: error.message });
    }
});

// ────────────────────────────────────────────────────────────────────────────
// 4. POST /api/v1/articulos — Crear artículo (targeted o broadcast)
// 
// Query param: ?sede=ID  → solo esa sede
// Sin param             → todas las sedes (broadcast)
// ────────────────────────────────────────────────────────────────────────────
/**
 * @swagger
 * /api/v1/articulos:
 *   post:
 *     summary: Crear un nuevo artículo
 *     tags: [Articulos]
 *     parameters:
 *       - in: query
 *         name: sede
 *         schema:
 *           type: string
 *         description: ID de la sede (opcional para broadcast)
 *     requestBody:
 *       required: true
 *       content:
 *         application/json:
 *           schema:
 *             type: object
 *             required: [co_art, art_des]
 *             properties:
 *               co_art: { type: string }
 *               art_des: { type: string }
 *               tipo: { type: string, default: 'V' }
 *               modelo: { type: string }
 *               ref: { type: string }
 *     responses:
 *       200:
 *         description: Artículo creado exitosamente
 *       400:
 *         description: Datos inválidos
 *       500:
 *         description: Error del servidor
 */
router.post('/', async (req, res) => {
    try {
        const data = req.body;
        if (!data.co_art || !data.art_des)
            return res.status(400).json({ success: false, message: 'Campos obligatorios: co_art, art_des' });

        const outcome = await executeWrite(req.query.sede || null, req.sqlAuth, async (pool) => {
            const f = new Date();
            const [resLin, resSubl, resCat, resCol, resUbic] = await Promise.all([
                pool.request().query('SELECT TOP 1 RTRIM(co_lin) AS id FROM saLineaArticulo'),
                pool.request().query('SELECT TOP 1 RTRIM(co_subl) AS id FROM saSubLinea'),
                pool.request().query('SELECT TOP 1 RTRIM(co_cat) AS id FROM saCatArticulo'),
                pool.request().query('SELECT TOP 1 RTRIM(co_color) AS id FROM saColor'),
                pool.request().query('SELECT TOP 1 RTRIM(co_ubicacion) AS id FROM saUbicacion')
            ]);

            const r = new sql.Request(pool);
            r.input('sCo_Art', sql.Char(30), data.co_art);
            r.input('sdFecha_Reg', sql.SmallDateTime, f);
            r.input('sArt_Des', sql.VarChar(120), data.art_des);
            r.input('sTipo', sql.Char(1), data.tipo || 'V');
            r.input('bAnulado', sql.Bit, 0);
            r.input('sdFecha_Inac', sql.SmallDateTime, f);
            r.input('sCo_Lin', sql.Char(6), data.co_lin || resLin.recordset[0]?.id || null);
            r.input('sCo_Subl', sql.Char(6), data.co_subl || resSubl.recordset[0]?.id || null);
            r.input('sCo_Cat', sql.Char(6), data.co_cat || resCat.recordset[0]?.id || null);
            r.input('sCo_Color', sql.Char(6), data.co_color || resCol.recordset[0]?.id || null);
            r.input('sCo_Ubicacion', sql.Char(6), data.co_ubicacion || resUbic.recordset[0]?.id || 'CONT1A');
            r.input('sItem', sql.VarChar(10), data.item || null);
            r.input('sModelo', sql.VarChar(20), data.modelo || '');
            r.input('sRef', sql.VarChar(20), data.ref || '');
            r.input('bGenerico', sql.Bit, 0);
            r.input('bManeja_Serial', sql.Bit, 0);
            r.input('bManeja_Lote', sql.Bit, 0);
            r.input('bManeja_Lote_Venc', sql.Bit, 0);
            r.input('deMargen_Min', sql.Decimal(18, 5), 0);
            r.input('deMargen_Max', sql.Decimal(18, 5), 0);
            r.input('sTipo_Imp', sql.Char(1), data.tipo_imp || '1');
            r.input('sTipo_Imp2', sql.Char(1), '7');
            r.input('sTipo_Imp3', sql.Char(1), '7');
            r.input('sCo_Reten', sql.Char(6), null);
            r.input('sCod_Proc', sql.Char(6), null);
            r.input('sGarantia', sql.VarChar(30), '');
            r.input('deVolumen', sql.Decimal(18, 5), 0);
            r.input('dePeso', sql.Decimal(18, 5), 0);
            r.input('deStock_Min', sql.Decimal(18, 5), 0);
            r.input('deStock_Max', sql.Decimal(18, 5), 0);
            r.input('deStock_Pedido', sql.Decimal(18, 5), 0);
            r.input('iRelac_Unidad', sql.Int, 1);
            r.input('dePunt_Ven', sql.Decimal(18, 5), 0);
            r.input('dePunt_Cli', sql.Decimal(18, 5), 0);
            r.input('deLic_Mon_Ilc', sql.Decimal(18, 5), 0);
            r.input('deLic_Capacidad', sql.Decimal(18, 5), 0);
            r.input('deLic_Grado_Al', sql.Decimal(18, 5), 0);
            r.input('sLic_Tipo', sql.Char(1), null);
            r.input('bPrec_Om', sql.Bit, 0);
            r.input('sComentario', sql.VarChar(sql.MAX), null);
            r.input('sTipo_Cos', sql.Char(4), '1');
            r.input('dePorc_Margen_Minimo', sql.Decimal(18, 5), 0);
            r.input('dePorc_Margen_Maximo', sql.Decimal(18, 5), 0);
            r.input('deMont_Comi', sql.Decimal(18, 5), 0);
            r.input('dePorc_Arancel', sql.Decimal(18, 5), 0);
            r.input('sI_Art_Des', sql.VarChar(120), null);
            r.input('sDis_Cen', sql.VarChar(sql.MAX), null);
            r.input('sReten_Iva_Tercero', sql.Char(16), null);
            r.input('sCampo1', sql.VarChar(60), '');
            r.input('sCampo2', sql.VarChar(60), '');
            r.input('sCampo3', sql.VarChar(60), '');
            r.input('sCampo4', sql.VarChar(60), '');
            r.input('sCampo5', sql.VarChar(60), '');
            r.input('sCampo6', sql.VarChar(60), '');
            r.input('sCampo7', sql.VarChar(60), '');
            r.input('sCampo8', sql.VarChar(60), '');
            r.input('sCo_Us_In', sql.Char(6), '999');
            r.input('sCo_Sucu_In', sql.Char(6), null);
            r.input('sMaquina', sql.VarChar(60), 'SYNC2K');
            r.input('sRevisado', sql.Char(1), '0');
            r.input('sTrasnfe', sql.Char(1), '0');
            await r.execute('pInsertarArticulo');
        });

        return writeResponse(res, outcome, `Sede "${req.query.sede}" no encontrada.`);
    } catch (error) {
        res.status(500).json({ success: false, message: 'Error interno.', error: error.message });
    }
});

// ────────────────────────────────────────────────────────────────────────────
// 5. PUT /api/v1/articulos/:co_art — Editar artículo (targeted o broadcast)
// ────────────────────────────────────────────────────────────────────────────
/**
 * @swagger
 * /api/v1/articulos/{co_art}:
 *   put:
 *     summary: Actualizar un artículo existente
 *     tags: [Articulos]
 *     parameters:
 *       - in: path
 *         name: co_art
 *         required: true
 *         schema:
 *           type: string
 *       - in: query
 *         name: sede
 *         schema:
 *           type: string
 *     requestBody:
 *       content:
 *         application/json:
 *           schema:
 *             type: object
 *             properties:
 *               art_des: { type: string }
 *               tipo: { type: string }
 *               modelo: { type: string }
 *     responses:
 *       200:
 *         description: Artículo actualizado
 *       404:
 *         description: Artículo no encontrado
 */
router.put('/:co_art', async (req, res) => {
    try {
        const coArtOri = req.params.co_art;
        const data = req.body;

        const outcome = await executeWrite(req.query.sede || null, req.sqlAuth, async (pool) => {
            const check = await pool.request().input('co_art', sql.VarChar, coArtOri).query(
                `SELECT validador, RTRIM(co_lin) AS co_lin, RTRIM(co_subl) AS co_subl,
                        RTRIM(co_cat) AS co_cat, RTRIM(co_color) AS co_color,
                        RTRIM(co_ubicacion) AS co_ubicacion, tipo_imp, tipo_cos
                 FROM saArticulo WHERE LTRIM(RTRIM(co_art)) = LTRIM(RTRIM(@co_art))`
            );
            if (!check.recordset.length) throw new Error('El artículo no existe en esta sede.');

            const row = check.recordset[0];
            const f = new Date();
            const r = new sql.Request(pool);
            r.input('sCo_Art', sql.Char(30), data.co_art || coArtOri);
            r.input('sCo_ArtOri', sql.Char(30), coArtOri);
            r.input('sdFecha_Reg', sql.SmallDateTime, f);
            r.input('sArt_Des', sql.VarChar(120), data.art_des || 'Artículo Modificado API');
            r.input('sTipo', sql.Char(1), data.tipo || 'V');
            r.input('bAnulado', sql.Bit, 0);
            r.input('sdFecha_Inac', sql.SmallDateTime, f);
            r.input('sCo_Lin', sql.Char(6), data.co_lin || row.co_lin);
            r.input('sCo_Subl', sql.Char(6), data.co_subl || row.co_subl);
            r.input('sCo_Cat', sql.Char(6), data.co_cat || row.co_cat);
            r.input('sCo_Color', sql.Char(6), data.co_color || row.co_color);
            r.input('sCo_Ubicacion', sql.Char(6), data.co_ubicacion || row.co_ubicacion);
            r.input('sItem', sql.VarChar(10), data.item || null);
            r.input('sModelo', sql.VarChar(20), data.modelo || '');
            r.input('sRef', sql.VarChar(20), data.ref || '');
            r.input('bGenerico', sql.Bit, 0);
            r.input('bManeja_Serial', sql.Bit, 0);
            r.input('bManeja_Lote', sql.Bit, 0);
            r.input('bManeja_Lote_Venc', sql.Bit, 0);
            r.input('deMargen_Min', sql.Decimal(18, 5), 0);
            r.input('deMargen_Max', sql.Decimal(18, 5), 0);
            r.input('sTipo_Imp', sql.Char(1), data.tipo_imp || row.tipo_imp || '1');
            r.input('sTipo_Imp2', sql.Char(1), '7');
            r.input('sTipo_Imp3', sql.Char(1), '7');
            r.input('sCo_Reten', sql.Char(6), null);
            r.input('sCod_Proc', sql.Char(6), null);
            r.input('sGarantia', sql.VarChar(30), '');
            r.input('deVolumen', sql.Decimal(18, 5), 0);
            r.input('dePeso', sql.Decimal(18, 5), 0);
            r.input('deStock_Min', sql.Decimal(18, 5), 0);
            r.input('deStock_Max', sql.Decimal(18, 5), 0);
            r.input('deStock_Pedido', sql.Decimal(18, 5), 0);
            r.input('iRelac_Unidad', sql.Int, 1);
            r.input('dePunt_Ven', sql.Decimal(18, 5), 0);
            r.input('dePunt_Cli', sql.Decimal(18, 5), 0);
            r.input('deLic_Mon_Ilc', sql.Decimal(18, 5), 0);
            r.input('deLic_Capacidad', sql.Decimal(18, 5), 0);
            r.input('deLic_Grado_Al', sql.Decimal(18, 5), 0);
            r.input('sLic_Tipo', sql.Char(1), null);
            r.input('bPrec_Om', sql.Bit, 0);
            r.input('sComentario', sql.VarChar(sql.MAX), null);
            r.input('sTipo_Cos', sql.Char(4), data.tipo_cos || row.tipo_cos || '1');
            r.input('dePorc_Margen_Minimo', sql.Decimal(18, 5), 0);
            r.input('dePorc_Margen_Maximo', sql.Decimal(18, 5), 0);
            r.input('deMont_Comi', sql.Decimal(18, 5), 0);
            r.input('dePorc_Arancel', sql.Decimal(18, 5), 0);
            r.input('sDis_Cen', sql.VarChar(sql.MAX), null);
            r.input('sReten_Iva_Tercero', sql.Char(16), null);
            r.input('sCampo1', sql.VarChar(60), '');
            r.input('sCampo2', sql.VarChar(60), '');
            r.input('sCampo3', sql.VarChar(60), '');
            r.input('sCampo4', sql.VarChar(60), '');
            r.input('sCampo5', sql.VarChar(60), '');
            r.input('sCampo6', sql.VarChar(60), '');
            r.input('sCampo7', sql.VarChar(60), '');
            r.input('sCampo8', sql.VarChar(60), '');
            r.input('sCo_Us_Mo', sql.Char(6), '999');
            r.input('sCo_Sucu_Mo', sql.Char(6), null);
            r.input('sMaquina', sql.VarChar(60), 'SYNC2K');
            r.input('sCampos', sql.VarChar(sql.MAX), '');
            r.input('sRevisado', sql.Char(1), '0');
            r.input('sTrasnfe', sql.Char(1), '0');
            r.input('tsValidador', sql.VarBinary, row.validador);
            r.input('gRowguid', sql.UniqueIdentifier, null);
            await r.execute('pActualizarArticulo');
        });

        return writeResponse(res, outcome, `Sede "${req.query.sede}" no encontrada.`);
    } catch (error) {
        res.status(500).json({ success: false, message: 'Error interno.', error: error.message });
    }
});

// ────────────────────────────────────────────────────────────────────────────
// 6. DELETE /api/v1/articulos/:co_art — Eliminar artículo (targeted o broadcast)
// ────────────────────────────────────────────────────────────────────────────
/**
 * @swagger
 * /api/v1/articulos/{co_art}:
 *   delete:
 *     summary: Eliminar un artículo
 *     tags: [Articulos]
 *     parameters:
 *       - in: path
 *         name: co_art
 *         required: true
 *         schema:
 *           type: string
 *       - in: query
 *         name: sede
 *         schema:
 *           type: string
 *     responses:
 *       200:
 *         description: Artículo eliminado
 *       404:
 *         description: Artículo no encontrado
 */
router.delete('/:co_art', async (req, res) => {
    try {
        const { co_art } = req.params;

        const outcome = await executeWrite(req.query.sede || null, req.sqlAuth, async (pool) => {
            const check = await pool.request().input('co_art', sql.VarChar, co_art).query(
                `SELECT validador FROM saArticulo WHERE LTRIM(RTRIM(co_art)) = LTRIM(RTRIM(@co_art))`
            );
            if (!check.recordset.length) throw new Error('El artículo no existe en esta sede.');

            const r = new sql.Request(pool);
            r.input('sCo_ArtOri', sql.Char(30), co_art);
            r.input('tsValidador', sql.VarBinary, check.recordset[0].validador);
            r.input('sMaquina', sql.VarChar(60), 'SYNC2K');
            r.input('sCo_Us_Mo', sql.Char(6), '999');
            r.input('sCo_Sucu_Mo', sql.Char(6), null);
            r.input('gRowguid', sql.UniqueIdentifier, null);
            await r.execute('pEliminarArticulo');
        });

        return writeResponse(res, outcome, `Sede "${req.query.sede}" no encontrada.`);
    } catch (error) {
        res.status(500).json({ success: false, message: 'Error interno.', error: error.message });
    }
});

// ────────────────────────────────────────────────────────────────────────────
// 6. PUT /api/v1/articulos/:co_art/ubicaciones — Actualizar ubicaciones del artículo
// ────────────────────────────────────────────────────────────────────────────
/**
 * @swagger
 * /api/v1/articulos/{co_art}/ubicaciones:
 *   put:
 *     summary: Asociar múltiples ubicaciones a un artículo (saArtUbicacion)
 *     description: Actualiza los campos co_ubicacion, co_ubicacion2 y co_ubicacion3 para un artículo en un almacén específico de una sede determinada.
 *     tags: [Articulos]
 *     parameters:
 *       - in: path
 *         name: co_art
 *         required: true
 *         schema:
 *           type: string
 *         description: Código del artículo
 *     requestBody:
 *       required: true
 *       content:
 *         application/json:
 *           schema:
 *             type: object
 *             required: [sede]
 *             properties:
 *               sede:
 *                 type: string
 *                 description: ID o Nombre de la sede (base de datos)
 *               co_alma:
 *                 type: string
 *                 default: "01"
 *                 description: Código del almacén/depósito
 *               co_ubicacion:
 *                 type: string
 *                 description: Ubicación principal (ej. Estante A-1)
 *               co_ubicacion2:
 *                 type: string
 *                 description: Segunda ubicación
 *               co_ubicacion3:
 *                 type: string
 *                 description: Tercera ubicación
 *     responses:
 *       200:
 *         description: Ubicaciones actualizadas exitosamente
 *       400:
 *         description: El parámetro "sede" es obligatorio o los datos son inválidos
 *       404:
 *         description: Artículo o almacén no encontrado en la sede especificada
 *       500:
 *         description: Error interno del servidor
 */
router.put('/:co_art/ubicaciones', async (req, res) => {
    try {
        const { co_art } = req.params;
        const { 
            sede, 
            co_alma = '01'
        } = req.body;
        
        // Capturar valores permitiendo null/vacío, pero sabiendo si fueron provistos
        const hasU1 = req.body.hasOwnProperty('co_ubicacion');
        const hasU2 = req.body.hasOwnProperty('co_ubicacion2');
        const hasU3 = req.body.hasOwnProperty('co_ubicacion3');
        
        const u1 = hasU1 ? req.body.co_ubicacion : null;
        const u2 = hasU2 ? req.body.co_ubicacion2 : null;
        const u3 = hasU3 ? req.body.co_ubicacion3 : null;

        if (!sede) {
            return res.status(400).json({ success: false, message: 'El parámetro "sede" es obligatorio en el cuerpo de la petición (body).' });
        }

        const outcome = await executeWrite(sede, req.sqlAuth, async (pool) => {
            const r = new sql.Request(pool);
            const cleanCoArt = co_art.trim();
            const cleanCoAlma = co_alma.trim();
            
            const finalU1 = (typeof u1 === 'string' && u1.trim() !== '') ? u1.trim() : null;
            const finalU2 = (typeof u2 === 'string' && u2.trim() !== '') ? u2.trim() : null;
            const finalU3 = (typeof u3 === 'string' && u3.trim() !== '') ? u3.trim() : null;

            r.input('co_art', sql.Char(30), cleanCoArt);
            r.input('co_alma', sql.Char(6), cleanCoAlma);
            r.input('u1', sql.VarChar(20), finalU1);
            r.input('u2', sql.VarChar(20), finalU2);
            r.input('u3', sql.VarChar(20), finalU3);
            r.input('hasU1', sql.Bit, hasU1 ? 1 : 0);
            r.input('hasU2', sql.Bit, hasU2 ? 1 : 0);
            r.input('hasU3', sql.Bit, hasU3 ? 1 : 0);
            
            const auditUser = (req.sqlAuth && req.sqlAuth.user) ? req.sqlAuth.user : (req.body.usuario_id || '999');
            r.input('user', sql.VarChar(10), auditUser);

            const artCheck = await r.query('SELECT 1 FROM saArticulo WHERE LTRIM(RTRIM(co_art)) = LTRIM(RTRIM(@co_art))');
            if (artCheck.recordset.length === 0) throw new Error(`El artículo "${cleanCoArt}" no existe en esta sede.`);

            const almaCheck = await r.query('SELECT 1 FROM saAlmacen WHERE LTRIM(RTRIM(co_alma)) = LTRIM(RTRIM(@co_alma))');
            if (almaCheck.recordset.length === 0) throw new Error(`El almacén "${cleanCoAlma}" no existe en esta sede.`);

            const auCheck = await r.query('SELECT 1 FROM saArtUbicacion WHERE LTRIM(RTRIM(co_art)) = LTRIM(RTRIM(@co_art)) AND LTRIM(RTRIM(co_alma)) = LTRIM(RTRIM(@co_alma))');
            
            if (auCheck.recordset.length > 0) {
                const isAllEmpty = finalU1 === null && finalU2 === null && finalU3 === null;
                
                if (isAllEmpty) {
                    await r.query(`DELETE FROM saArtUbicacion WHERE LTRIM(RTRIM(co_art)) = @co_art AND LTRIM(RTRIM(co_alma)) = @co_alma`);
                } else {
                    if (finalU1 === null) {
                        throw new Error('La ubicación principal es obligatoria en Profit Plus. Para eliminarla, cambie todas a "Ninguna".');
                    }
                    // UPDATE: Solo actualizamos si se proporcionan valores (incluso si son nulos vía Ninguna)
                    await r.query(`
                        UPDATE saArtUbicacion 
                        SET co_ubicacion = CASE WHEN @hasU1 = 1 THEN @u1 ELSE co_ubicacion END, 
                            co_ubicacion2 = CASE WHEN @hasU2 = 1 THEN @u2 ELSE co_ubicacion2 END, 
                            co_ubicacion3 = CASE WHEN @hasU3 = 1 THEN @u3 ELSE co_ubicacion3 END,
                            fe_us_mo = GETDATE(),
                            co_us_mo = @user
                        WHERE LTRIM(RTRIM(co_art)) = @co_art AND LTRIM(RTRIM(co_alma)) = @co_alma
                    `);
                }
            } else {
                if (finalU1 === null && finalU2 === null && finalU3 === null) {
                    // Nada que hacer, no existía y se manda a borrar
                    return;
                }
                if (finalU1 === null) {
                    throw new Error('La ubicación principal es obligatoria en Profit Plus al crear una asociación nueva.');
                }
                // INSERT
                await r.query(`
                    INSERT INTO saArtUbicacion (
                        co_art, co_alma, co_ubicacion, co_ubicacion2, co_ubicacion3, 
                        orden, co_us_in, fe_us_in, co_us_mo, fe_us_mo
                    )
                    VALUES (
                        @co_art, @co_alma, @u1, @u2, @u3, 
                        100, @user, GETDATE(), @user, GETDATE()
                    )
                `);
            }
            return { co_art: cleanCoArt, co_alma: cleanCoAlma, success: true };
        });

        return writeResponse(res, outcome, `Sede "${sede}" no encontrada o error en la operación.`);
    } catch (e) {
        console.error(`[PUT /:co_art/ubicaciones] Error:`, e.message);
        res.status(500).json({ success: false, error: e.message });
    }
});


module.exports = router;
