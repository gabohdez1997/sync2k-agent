const express = require('express');
const router = express.Router();
const { sql, getPool, getServers, getExchangeRate } = require('../db');
const { executeWrite, writeResponse, paginatedResponse, resolveServer } = require('../helpers/multiSede');

// ── Helper: enriquece artículos con precios, stock y último costo ─────────────
async function enrichArticulos(pool, articulos, tasa, authorizedAlmacenes = null) {
    if (!articulos.length) return articulos;
    const ids = articulos.map(a => `'${a.co_art.replace(/'/g, "''")}'`).join(',');

    let authCondition = "";
    if (authorizedAlmacenes) {
        const almas = Array.isArray(authorizedAlmacenes) ? authorizedAlmacenes : authorizedAlmacenes.split(',');
        const list = almas.map(a => `'${a.trim().replace(/'/g, "''")}'`).filter(a => a !== "''").join(',');
        if (list) authCondition = ` AND s.co_alma IN (${list})`;
    }

    const [resStock, resPrecios, resCostos, resUbicaciones] = await Promise.all([
        pool.request().query(`
            SELECT RTRIM(s.co_art) AS co_art, RTRIM(s.co_alma) AS co_alma,
                   RTRIM(a.des_alma) AS des_alma,
                   (SUM(ISNULL(CASE WHEN RTRIM(s.tipo)='ACT' THEN s.stock ELSE 0 END, 0)) -
                    SUM(ISNULL(CASE WHEN RTRIM(s.tipo)='COM' THEN s.stock ELSE 0 END, 0))) AS stock
            FROM saStockAlmacen s LEFT JOIN saAlmacen a ON s.co_alma = a.co_alma
            WHERE LTRIM(RTRIM(s.co_art)) IN (${ids}) ${authCondition}
            GROUP BY s.co_art, s.co_alma, a.des_alma
        `),
        pool.request().query(`
            WITH UP AS (
                SELECT RTRIM(p.co_art) AS co_art, RTRIM(p.co_precio) AS id_precio,
                       p.monto AS precio, RTRIM(p.co_mone) AS moneda,
                       ISNULL(m.monto_min, 0) AS margen,
                       ROW_NUMBER() OVER(PARTITION BY p.co_art, p.co_precio ORDER BY p.desde DESC) AS rn
                FROM saArtPrecio p
                LEFT JOIN saArtMargen m ON p.co_art = m.co_art AND p.co_precio = m.co_precio
                WHERE LTRIM(RTRIM(p.co_art)) IN (${ids})
                  AND p.Inactivo = 0 AND GETDATE() >= p.desde AND (p.hasta IS NULL OR GETDATE() <= p.hasta)
            )
            SELECT co_art, id_precio, precio, moneda, margen FROM UP WHERE rn = 1
        `),
        pool.request().query(`
            SELECT r.co_art,
                   r.cost_unit_om,
                   r.cost_unit,
                   r.fec_emis AS fecha_ultima_compra,
                   r.co_mone
            FROM (
                SELECT RTRIM(fr.co_art) AS co_art,
                       CASE 
                            WHEN RTRIM(fn.co_mone) = 'BS' THEN (fr.cost_unit / NULLIF((SELECT TOP 1 tasa_v FROM saTasa WHERE (co_mone LIKE 'US%') AND fecha <= fn.fec_emis ORDER BY fecha DESC), 0)) 
                            ELSE fr.cost_unit_om 
                       END AS cost_unit_om,
                       fr.cost_unit,
                       fn.fec_emis,
                       RTRIM(fn.co_mone) AS co_mone,
                       ROW_NUMBER() OVER(PARTITION BY fr.co_art ORDER BY fn.fec_emis DESC) as rn
                FROM saFacturaCompraReng fr 
                INNER JOIN saFacturaCompra fn ON fr.doc_num = fn.doc_num
                WHERE LTRIM(RTRIM(fr.co_art)) IN (${ids}) AND fn.anulado = 0
            ) r
            WHERE r.rn = 1
        `).catch(err => {
            console.error('[enrichArticulos] Error fetching last purchase cost:', err.message);
            return { recordset: [] };
        }),
        pool.request().query(`
            SELECT RTRIM(co_art) AS co_art, RTRIM(co_alma) AS co_alma,
                   RTRIM(ISNULL(co_ubicacion, '')) AS co_ubicacion,
                   RTRIM(ISNULL(co_ubicacion2, '')) AS co_ubicacion2,
                   RTRIM(ISNULL(co_ubicacion3, '')) AS co_ubicacion3
            FROM saArtUbicacion
            WHERE LTRIM(RTRIM(co_art)) IN (${ids})
        `).catch(err => {
            console.error('[enrichArticulos] Error fetching ubicaciones:', err.message);
            return { recordset: [] };
        })
    ]);

    const stockMap = {};
    resStock.recordset.forEach(s => { 
        (stockMap[s.co_art] = stockMap[s.co_art] || []).push({ 
            co_alma: s.co_alma, 
            des_alma: s.des_alma, 
            stock: s.stock 
        }); 
    });

    const precioMap = {};
    resPrecios.recordset.forEach(p => {
        (precioMap[p.co_art] = precioMap[p.co_art] || []).push({
            id_precio: p.id_precio, precio: p.precio, moneda: p.moneda, margen: p.margen,
            precio_ves: ((p.moneda || '').includes('US') ? Number((p.precio * tasa).toFixed(2)) : p.precio)
        });
    });

    const costMap = {};
    if (resCostos && resCostos.recordset) {
        resCostos.recordset.forEach(c => {
            costMap[c.co_art] = {
                ultimo_costo_om: Number(c.cost_unit_om) || 0,
                ultimo_costo: Number(c.cost_unit) || 0,
                fecha_ultima_compra: c.fecha_ultima_compra
            };
        });
    }

    const ubicacionesMap = {};
    if (resUbicaciones && resUbicaciones.recordset) {
        resUbicaciones.recordset.forEach(u => {
            if (!ubicacionesMap[u.co_art]) ubicacionesMap[u.co_art] = {};
            ubicacionesMap[u.co_art][u.co_alma] = {
                co_alma: u.co_alma,
                co_ubicacion: u.co_ubicacion || '',
                co_ubicacion2: u.co_ubicacion2 || '',
                co_ubicacion3: u.co_ubicacion3 || ''
            };
        });
    }

    return articulos.map(a => {
        const pList = precioMap[a.co_art] || [];
        const p2Obj = pList.find(p => p.id_precio === '02' || p.id_precio === '2') || pList[1] || pList[0];
        const p2 = Number(p2Obj?.precio) || 0;
        const m2 = Number(p2Obj?.margen) || 0;
        
        let costo_estimado = 0;
        if (p2 > 0 && m2 > 0) {
            costo_estimado = m2 > 1 ? Number((p2 / (1 + (m2 / 100))).toFixed(2)) : Number((p2 / m2).toFixed(2));
        }

        const costInfo = costMap[a.co_art] || {};
        const ultCosto = costInfo.ultimo_costo_om || 0;
        const finalCosto = ultCosto > 0 ? ultCosto : (costo_estimado > 0 ? costo_estimado : (p2 > 0 ? p2 : 0));

        const artUbicacionesMap = ubicacionesMap[a.co_art] || {};
        const artUbicacionesList = Object.values(artUbicacionesMap);

        return {
            ...a,
            tasa_bcv: tasa,
            disponibilidad: stockMap[a.co_art] || [],
            precios: pList,
            ultimo_costo_om: ultCosto,
            fecha_ultima_compra: costInfo.fecha_ultima_compra || null,
            costo_estimado: costo_estimado,
            costo_sugerido_usd: finalCosto,
            costo_sugerido_ves: Number((finalCosto * tasa).toFixed(2)),
            ubicaciones_map: artUbicacionesMap,
            ubicaciones: artUbicacionesList
        };
    });
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
        const limit = parseInt(req.query.limit) || 12;
        const requestedSede = req.query.sede || req.query.sede_id;
        const reqSort = req.query.sort;

        let servers = getServers();
        if (requestedSede && requestedSede !== "Todas") {
            servers = servers.filter(srv => srv.id === requestedSede || srv.name === requestedSede);
        }

        if (servers.length === 0) {
            return res.status(200).json({ success: true, page, limit, total_items: 0, total_pages: 0, count: 0, data: [] });
        }

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
        const search = req.query.search || req.query.q;
        const linea = req.query.linea;
        const categoria = req.query.categoria;
        const ubicacion = req.query.co_ubicacion;

        const con_imagen = req.query.con_imagen === 'true' || req.query.has_image === 'true' || req.query.con_img === 'true';

        // Decidir si hacemos búsqueda global o paginación perezosa por rendimiento.
        const isGlobalNeeded = !!(search || linea || categoria || ubicacion || (reqSort && reqSort.startsWith('price')) || in_stock_all || con_imagen);

        // 1. Obtener listado simultáneamente de todos los servidores (aunque sea uno solo ahora)
        let globalTotal = 0;
        const allResults = await Promise.all(servers.map(async (srv) => {
            try {
                const pool = await getPool(srv.id, req.sqlAuth);
                const r = pool.request();

                let whereClauses = ["a.anulado = 0"];
                if (con_imagen) {
                    whereClauses.push("LTRIM(RTRIM(ISNULL(a.campo7, ''))) != ''");
                }
                const co_alma = req.query.co_alma;
                const authAlmacenes = req.query.authorized_almacenes;

                if (search) {
                    r.input('search', sql.VarChar, `%${search}%`);
                    whereClauses.push("(a.co_art LIKE @search OR a.art_des LIKE @search OR a.modelo LIKE @search OR a.ref LIKE @search)");
                }
                if (linea) {
                    r.input('linea', sql.VarChar, linea);
                    whereClauses.push("a.co_lin = @linea");
                }
                if (categoria) {
                    r.input('categoria', sql.VarChar, categoria);
                    whereClauses.push("a.co_cat = @categoria");
                }
                if (ubicacion) {
                    r.input('ubic', sql.VarChar, ubicacion);
                    whereClauses.push("(au.co_ubicacion = @ubic OR au.co_ubicacion2 = @ubic OR au.co_ubicacion3 = @ubic)");
                }
                const whereSQL = whereClauses.join(" AND ");

                let authStockFilter = "";
                if (co_alma) {
                    r.input('co_alma', sql.VarChar, co_alma);
                    authStockFilter = " AND st.co_alma = @co_alma ";
                } else if (authAlmacenes) {
                    const almas = authAlmacenes.split(',').map(a => `'${a.trim().replace(/'/g, "''")}'`).join(',');
                    if (almas) authStockFilter = ` AND st.co_alma IN (${almas}) `;
                }

                // LÓGICA DE STOCK ROBUSTA: Usar ISNULL para evitar que valores nulos oculten artículos
                const stockCondition = in_stock_all ? "" : ` AND (
                    LTRIM(RTRIM(a.co_lin)) = '09' OR 
                    RTRIM(a.tipo) IN ('S', '2') OR 
                    EXISTS (
                        SELECT 1 FROM saStockAlmacen st 
                        WHERE st.co_art = a.co_art 
                        ${authStockFilter}
                        GROUP BY st.co_art
                        HAVING (SUM(ISNULL(CASE WHEN RTRIM(tipo)='ACT' THEN stock ELSE 0 END, 0)) - 
                                SUM(ISNULL(CASE WHEN RTRIM(tipo)='COM' THEN stock ELSE 0 END, 0))) > 0
                    )
                )`;

                // Conteo real espejado con la data
                const fromClause = `FROM saArticulo a 
                                   LEFT JOIN (
                                       SELECT co_art, co_ubicacion, co_ubicacion2, co_ubicacion3,
                                              ROW_NUMBER() OVER(PARTITION BY co_art ORDER BY co_ubicacion) as rn
                                       FROM saArtUbicacion
                                       ${co_alma ? "WHERE co_alma = @co_alma" : ""}
                                   ) au ON a.co_art = au.co_art AND au.rn = 1`;

                const resCount = await r.query(`SELECT COUNT(DISTINCT a.co_art) as total ${fromClause} WHERE ${whereSQL} ${stockCondition}`);
                globalTotal += resCount.recordset[0]?.total || 0;

                const topClause = isGlobalNeeded ? "" : `TOP (${page * limit})`;

                const querySQL = `SELECT ${topClause} RTRIM(a.co_art) AS co_art, RTRIM(a.art_des) AS descripcion,
                             RTRIM(a.tipo) AS tipo, RTRIM(a.modelo) AS modelo, RTRIM(a.ref) AS referencia,
                             RTRIM(a.co_lin) AS co_lin, RTRIM(a.co_subl) AS co_subl, RTRIM(l.lin_des) AS linea, RTRIM(c.cat_des) AS categoria,
                             RTRIM(au.co_ubicacion) AS co_ubicacion,
                             RTRIM(aun.co_uni) AS co_uni, RTRIM(ISNULL(un.des_uni, aun.co_uni)) AS unidad,
                             RTRIM(a.tipo_imp) AS tipo_imp, RTRIM(a.campo7) AS campo7,
                             CAST(CASE WHEN a.art_des LIKE '%TIPO B%' OR c.cat_des LIKE '%TIPO B%' OR a.art_des LIKE '%SEGUNDA%' THEN 1 ELSE 0 END AS bit) AS oferta
                             ${joinPrecioClause ? ', ISNULL(pr.monto,0) AS precio_base' : ''}
                      ${fromClause}
                      LEFT JOIN saLineaArticulo l ON a.co_lin = l.co_lin
                      LEFT JOIN saCatArticulo c ON a.co_cat = c.co_cat
                      LEFT JOIN (
                          SELECT co_art, co_uni, 
                                 ROW_NUMBER() OVER(PARTITION BY co_art ORDER BY uni_principal DESC) as rn
                          FROM saArtUnidad
                      ) aun ON a.co_art = aun.co_art AND aun.rn = 1
                      LEFT JOIN saUnidad un ON aun.co_uni = un.co_uni
                      ${joinPrecioClause}
                      WHERE ${whereSQL} ${stockCondition}
                      ${orderByClause}`;

                const resData = await r.query(querySQL);
                return (resData.recordset || []).map(a => ({ ...a, sede_id: srv.id, sede_nombre: srv.name }));
            } catch (e) {
                console.error(`[GET /] Error en sede ${srv.id}:`, e.message);
                return [{ error_sql: e.message, query_fallido: true }];
            }
        }));

        // 2. Combinar resultados (Si solo hay uno, es directo)
        const combined = [].concat(...allResults);

        // 3. Ordenar ORDEN GLOBAL basado en precios si se requiere
        if (reqSort === 'price_asc') {
            combined.sort((a, b) => (Number(a.precio_base) || 0) - (Number(b.precio_base) || 0));
        } else if (reqSort === 'price_desc') {
            combined.sort((a, b) => (Number(b.precio_base) || 0) - (Number(a.precio_base) || 0));
        } else if (isGlobalNeeded) {
            combined.sort((a, b) => (a.descripcion || "").localeCompare(b.descripcion || ""));
        }

        // 4. Paginar
        // Usar globalTotal para que el conteo sea siempre el real de la DB (7,188)
        const total = isGlobalNeeded ? Math.max(combined.length, globalTotal) : globalTotal;
        const paginated = combined.slice((page - 1) * limit, page * limit);

        // 5. Enriquecer solo los artículos de la página actual
        const enrichedItems = [];
        const itemsBySede = paginated.reduce((acc, item) => {
            acc[item.sede_id] = acc[item.sede_id] || [];
            acc[item.sede_id].push(item);
            return acc;
        }, {});

        await Promise.all(Object.entries(itemsBySede).map(async ([sedeId, items]) => {
            try {
                const pool = await getPool(sedeId, req.sqlAuth);
                const tasa = await getExchangeRate(pool);
                const enriched = await enrichArticulos(pool, items, tasa);
                enrichedItems.push(...enriched);
            } catch (e) {
                console.error(`[GET /] Error enriqueciendo sede ${sedeId}:`, e.message);
                enrichedItems.push(...items.map(i => ({ ...i, error_enriquecimiento: e.message })));
            }
        }));

        // Mantener orden
        if (reqSort === 'price_asc') {
            enrichedItems.sort((a, b) => (Number(a.precio_base) || 0) - (Number(b.precio_base) || 0));
        } else if (reqSort === 'price_desc') {
            enrichedItems.sort((a, b) => (Number(b.precio_base) || 0) - (Number(a.precio_base) || 0));
        } else if (isGlobalNeeded) {
            enrichedItems.sort((a, b) => (a.descripcion || "").localeCompare(b.descripcion || ""));
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
            co_ubicacion3: 'au.co_ubicacion3', ubicacion3: 'u3.des_ubicacion',
            campo7: 'a.campo7'
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

        var globalSearch = req.query.search || req.query.q;

        if (!filters.length && !req.query.sede && !req.query.sort && !globalSearch && !req.query.con_imagen) {
            return res.status(400).json({ success: false, message: 'Especifique al menos un parámetro de búsqueda.' });
        }
        const co_alma = req.query.co_alma;
        const authAlmacenes = req.query.authorized_almacenes;
        const in_stock_all = req.query.in_stock === 'all';
        const con_imagen = req.query.con_imagen === 'true' || req.query.has_image === 'true' || req.query.con_img === 'true';

        let authStockFilter = "";
        if (co_alma) {
            authStockFilter = " AND st.co_alma = @co_alma ";
        } else if (authAlmacenes) {
            const almas = authAlmacenes.split(',').map(a => `'${a.trim().replace(/'/g, "''")}'`).join(',');
            if (almas) authStockFilter = ` AND st.co_alma IN (${almas}) `;
        }

        const ofertaCondition = `(a.art_des LIKE '%TIPO B%' OR c.cat_des LIKE '%TIPO B%' OR sl.subl_des LIKE '%TIPO B%' OR l.lin_des LIKE '%SEGUNDA%' OR sl.subl_des LIKE '%SEGUNDA%' OR c.cat_des LIKE '%SEGUNDA%' OR a.art_des LIKE '%SEGUNDA%')`;
        const normalFilters = filters.filter(f => !f.hasOwnProperty('isOferta'));
        const ofertaFilter = filters.find(f => f.hasOwnProperty('isOferta'));

        let whereClause = 'WHERE a.anulado = 0 ';
        if (con_imagen) {
            whereClause += " AND LTRIM(RTRIM(ISNULL(a.campo7, ''))) != '' ";
        }
        if (!in_stock_all) {
            whereClause += ` AND (LTRIM(RTRIM(a.co_lin)) = '09' OR RTRIM(a.tipo) IN ('S','2') OR EXISTS (SELECT 1 FROM saStockAlmacen st WHERE st.co_art = a.co_art ${authStockFilter} GROUP BY st.co_art HAVING SUM(ISNULL(CASE WHEN RTRIM(st.tipo)='ACT' THEN st.stock ELSE 0 END, 0)) - SUM(ISNULL(CASE WHEN RTRIM(st.tipo)='COM' THEN st.stock ELSE 0 END, 0)) > 0)) `;
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

        if (globalSearch) {
            whereClause += ` AND (a.co_art LIKE '%' + @globalSearch + '%' OR a.art_des LIKE '%' + @globalSearch + '%' OR a.modelo LIKE '%' + @globalSearch + '%' OR a.ref LIKE '%' + @globalSearch + '%') `;
        }

        // --- LÓGICA DE ORDENAMIENTO ---
        let orderByClause = 'ORDER BY a.art_des ASC';
        let joinPrecioClause = '';
        if (reqSort === 'price_asc') {
            joinPrecioClause = "LEFT JOIN saArtPrecio pr ON LTRIM(RTRIM(a.co_art)) = LTRIM(RTRIM(pr.co_art)) AND LTRIM(RTRIM(pr.co_precio)) = '01'";
            orderByClause = 'ORDER BY pr.monto ASC, a.art_des ASC';
        } else if (reqSort === 'price_desc') {
            joinPrecioClause = "LEFT JOIN saArtPrecio pr ON LTRIM(RTRIM(a.co_art)) = LTRIM(RTRIM(pr.co_art)) AND LTRIM(RTRIM(pr.co_precio)) = '01'";
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
                if (globalSearch) r.input('globalSearch', sql.VarChar, globalSearch);

                const resData = await r.query(
                    `SELECT RTRIM(a.co_art) AS co_art, RTRIM(a.art_des) AS descripcion,
                            RTRIM(a.tipo) AS tipo, RTRIM(a.modelo) AS modelo, RTRIM(a.ref) AS referencia,
                            RTRIM(a.co_lin) AS co_lin, RTRIM(a.co_subl) AS co_subl,
                            RTRIM(l.lin_des) AS linea, RTRIM(sl.subl_des) AS sublinea, RTRIM(c.cat_des) AS categoria,
                            RTRIM(au.co_ubicacion) AS co_ubicacion, RTRIM(u1.des_ubicacion) AS ubicacion,
                            RTRIM(au.co_ubicacion2) AS co_ubicacion2, RTRIM(u2.des_ubicacion) AS ubicacion2,
                            RTRIM(au.co_ubicacion3) AS co_ubicacion3, RTRIM(u3.des_ubicacion) AS ubicacion3,
                            RTRIM(aun.co_uni) AS co_uni, RTRIM(un.des_uni) AS unidad,
                            RTRIM(a.campo7) AS campo7,
                            CAST(CASE WHEN a.art_des LIKE '%TIPO B%' OR c.cat_des LIKE '%TIPO B%' OR sl.subl_des LIKE '%TIPO B%' OR l.lin_des LIKE '%SEGUNDA%' OR sl.subl_des LIKE '%SEGUNDA%' OR c.cat_des LIKE '%SEGUNDA%' OR a.art_des LIKE '%SEGUNDA%' THEN 1 ELSE 0 END AS bit) AS oferta
                            ${joinPrecioClause ? ', ISNULL(pr.monto,0) AS precio_base' : ''}
                     FROM saArticulo a
                     LEFT JOIN saLineaArticulo l ON a.co_lin = l.co_lin
                     LEFT JOIN saSubLinea sl ON a.co_subl = sl.co_subl
                     LEFT JOIN saCatArticulo c ON a.co_cat = c.co_cat
                      LEFT JOIN (
                          SELECT co_art, co_alma, co_ubicacion, co_ubicacion2, co_ubicacion3,
                                 ROW_NUMBER() OVER(PARTITION BY co_art ORDER BY co_alma ASC) as rn
                          FROM saArtUbicacion
                          ${co_alma ? "WHERE co_alma = @co_alma" : ""}
                      ) au ON a.co_art = au.co_art AND au.rn = 1
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

        // 2. DEDUPLICAR Cruces inter-servidor
        const combinedRaw = [].concat(...allData);
        const uniqueMap = new Map();
        combinedRaw.forEach(item => {
            const co = (item.co_art || "").trim();
            if (!uniqueMap.has(co)) uniqueMap.set(co, item);
        });
        const combined = Array.from(uniqueMap.values());

        // 3. Orden Global (Cross-Server)
        if (reqSort === 'price_asc') {
            combined.sort((a, b) => (Number(a.precio_base) || 0) - (Number(b.precio_base) || 0));
        } else if (reqSort === 'price_desc') {
            combined.sort((a, b) => (Number(b.precio_base) || 0) - (Number(a.precio_base) || 0));
        } else {
            combined.sort((a, b) => a.descripcion.localeCompare(b.descripcion));
        }

        let total = combined.length;

        // 4. Paginación
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
                const tasa = await getExchangeRate(pool);
                const enriched = await enrichArticulos(pool, items, tasa, authAlmacenes);
                finalItems.push(...enriched);
            } catch (e) {
                finalItems.push(...items.map(i => ({ ...i, error_enriquecimiento: e.message })));
            }
        }));

        // Mantener el orden que ya definimos arriba (muy importante)
        if (reqSort === 'price_asc') {
            finalItems.sort((a, b) => (a.precio_base || 0) - (b.precio_base || 0));
        } else if (reqSort === 'price_desc') {
            finalItems.sort((a, b) => (b.precio_base || 0) - (a.precio_base || 0));
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
// 2.3 GET /api/v1/articulos/export-all — Exportar todos los artículos de la sede
// ────────────────────────────────────────────────────────────────────────────
router.get('/export-all', async (req, res) => {
    try {
        const srv = resolveServer(req);
        if (!srv) return res.status(404).json({ success: false, message: 'No hay sede disponible.' });

        const pool = await getPool(srv.id, req.sqlAuth);
        
        // Consultar artículos con todas sus unidades y datos de catálogos asociados
        const [artRes, artUniRes, linRes, sublRes, catRes, colRes, ubiRes, uniRes] = await Promise.all([
            pool.request().query(
                `SELECT RTRIM(a.co_art) AS co_art, RTRIM(a.art_des) AS art_des, RTRIM(a.tipo) AS tipo,
                        RTRIM(a.co_lin) AS co_lin, RTRIM(a.co_subl) AS co_subl, RTRIM(a.co_cat) AS co_cat,
                        RTRIM(a.co_color) AS co_color, RTRIM(a.co_ubicacion) AS co_ubicacion,
                        RTRIM(a.item) AS item, RTRIM(a.modelo) AS modelo, RTRIM(a.ref) AS ref,
                        a.anulado, a.tipo_imp, a.peso, a.volumen, a.stock_min, a.stock_max,
                        RTRIM(a.campo1) AS campo1, RTRIM(a.campo7) AS campo7
                 FROM saArticulo a`
            ),
            pool.request().query(
                `SELECT RTRIM(au.co_art) AS co_art, RTRIM(au.co_uni) AS co_uni, au.relacion, au.equivalencia,
                        au.uni_principal, au.uso_venta, au.uso_compra, au.uni_secundaria, au.uso_secundaria,
                        RTRIM(u.des_uni) AS des_uni
                 FROM saArtUnidad au
                 LEFT JOIN saUnidad u ON au.co_uni = u.co_uni`
            ),
            pool.request().query('SELECT RTRIM(co_lin) AS co_lin, RTRIM(lin_des) AS lin_des FROM saLineaArticulo'),
            pool.request().query('SELECT RTRIM(co_lin) AS co_lin, RTRIM(co_subl) AS co_subl, RTRIM(subl_des) AS subl_des FROM saSubLinea'),
            pool.request().query('SELECT RTRIM(co_cat) AS co_cat, RTRIM(cat_des) AS cat_des FROM saCatArticulo'),
            pool.request().query('SELECT RTRIM(co_color) AS co_color, RTRIM(des_color) AS des_color FROM saColor'),
            pool.request().query('SELECT RTRIM(co_ubicacion) AS co_ubicacion, RTRIM(des_ubicacion) AS des_ubicacion FROM saUbicacion'),
            pool.request().query('SELECT RTRIM(co_uni) AS co_uni, RTRIM(des_uni) AS des_uni FROM saUnidad')
        ]);

        // Mapear unidades por artículo
        const unitsByArt = new Map();
        for (const u of artUniRes.recordset) {
            const key = (u.co_art || '').trim().toUpperCase();
            if (!unitsByArt.has(key)) unitsByArt.set(key, []);
            unitsByArt.get(key).push(u);
        }

        const linMap = new Map(linRes.recordset.map(l => [(l.co_lin || '').trim().toUpperCase(), l]));
        const sublMap = new Map(sublRes.recordset.map(s => [`${(s.co_lin || '').trim().toUpperCase()}__${(s.co_subl || '').trim().toUpperCase()}`, s]));
        const catMap = new Map(catRes.recordset.map(c => [(c.co_cat || '').trim().toUpperCase(), c]));
        const colMap = new Map(colRes.recordset.map(c => [(c.co_color || '').trim().toUpperCase(), c]));
        const ubiMap = new Map(ubiRes.recordset.map(u => [(u.co_ubicacion || '').trim().toUpperCase(), u]));

        const enrichedArticles = artRes.recordset.map(a => {
            const co_art_key = (a.co_art || '').trim().toUpperCase();
            const co_lin_key = (a.co_lin || '').trim().toUpperCase();
            const co_subl_key = `${co_lin_key}__${(a.co_subl || '').trim().toUpperCase()}`;
            const co_cat_key = (a.co_cat || '').trim().toUpperCase();
            const co_col_key = (a.co_color || '').trim().toUpperCase();
            const co_ubi_key = (a.co_ubicacion || '').trim().toUpperCase();

            return {
                ...a,
                unidades: unitsByArt.get(co_art_key) || [],
                _linea: linMap.get(co_lin_key) || null,
                _sublinea: sublMap.get(co_subl_key) || null,
                _categoria: catMap.get(co_cat_key) || null,
                _color: colMap.get(co_col_key) || null,
                _ubicacion: ubiMap.get(co_ubi_key) || null
            };
        });

        return res.status(200).json({
            success: true,
            sede_id: srv.id,
            sede_nombre: srv.name,
            count: enrichedArticles.length,
            data: enrichedArticles
        });
    } catch (error) {
        console.error('[ARTICULOS EXPORT ERROR]:', error);
        res.status(500).json({ success: false, message: 'Error exportando artículos', error: error.message });
    }
});

/**
 * Asegura que las tablas maestras (Líneas, Sublíneas, Categorías, Unidades, Colores, Ubicaciones)
 * tengan creados los registros que requiere el artículo a insertar.
 */
async function ensureCatalogDependencies(pool, item, auditUser, defaultAlmacen) {
    const co_lin = (item.co_lin || '').trim();
    if (co_lin) {
        const linDes = (item._linea?.lin_des || item.lin_des || `LINEA ${co_lin}`).trim();
        try {
            await pool.request()
                .input('co_lin', sql.Char(6), padProfit(co_lin, 6))
                .input('lin_des', sql.VarChar(60), linDes)
                .input('user', sql.Char(6), padProfit(auditUser || 'PROFIT', 6))
                .input('sucu', sql.Char(6), padProfit(defaultAlmacen || '01', 6))
                .query(`
                    IF NOT EXISTS (SELECT 1 FROM saLineaArticulo WHERE LTRIM(RTRIM(co_lin)) = LTRIM(RTRIM(@co_lin)))
                    BEGIN
                        INSERT INTO saLineaArticulo (co_lin, lin_des, co_us_in, fe_us_in, co_sucu_in, rowguid)
                        VALUES (@co_lin, @lin_des, @user, GETDATE(), @sucu, NEWID())
                    END
                `);
        } catch (eLin) {
            console.warn(`[SYNC] No se pudo asegurar saLineaArticulo ${co_lin}:`, eLin.message);
        }
    }

    const co_subl = (item.co_subl || '').trim();
    if (co_lin && co_subl) {
        const sublDes = (item._sublinea?.subl_des || item.subl_des || `SUBLINEA ${co_subl}`).trim();
        try {
            await pool.request()
                .input('co_lin', sql.Char(6), padProfit(co_lin, 6))
                .input('co_subl', sql.Char(6), padProfit(co_subl, 6))
                .input('subl_des', sql.VarChar(60), sublDes)
                .input('user', sql.Char(6), padProfit(auditUser || 'PROFIT', 6))
                .input('sucu', sql.Char(6), padProfit(defaultAlmacen || '01', 6))
                .query(`
                    IF NOT EXISTS (SELECT 1 FROM saSubLinea WHERE LTRIM(RTRIM(co_lin)) = LTRIM(RTRIM(@co_lin)) AND LTRIM(RTRIM(co_subl)) = LTRIM(RTRIM(@co_subl)))
                    BEGIN
                        INSERT INTO saSubLinea (co_lin, co_subl, subl_des, co_us_in, fe_us_in, co_sucu_in, rowguid)
                        VALUES (@co_lin, @co_subl, @subl_des, @user, GETDATE(), @sucu, NEWID())
                    END
                `);
        } catch (eSubl) {
            console.warn(`[SYNC] No se pudo asegurar saSubLinea ${co_subl}:`, eSubl.message);
        }
    }

    const co_cat = (item.co_cat || '').trim();
    if (co_cat) {
        const catDes = (item._categoria?.cat_des || item.cat_des || `CATEGORIA ${co_cat}`).trim();
        try {
            await pool.request()
                .input('co_cat', sql.Char(6), padProfit(co_cat, 6))
                .input('cat_des', sql.VarChar(60), catDes)
                .input('user', sql.Char(6), padProfit(auditUser || 'PROFIT', 6))
                .input('sucu', sql.Char(6), padProfit(defaultAlmacen || '01', 6))
                .query(`
                    IF NOT EXISTS (SELECT 1 FROM saCatArticulo WHERE LTRIM(RTRIM(co_cat)) = LTRIM(RTRIM(@co_cat)))
                    BEGIN
                        INSERT INTO saCatArticulo (co_cat, cat_des, co_us_in, fe_us_in, co_sucu_in, rowguid)
                        VALUES (@co_cat, @cat_des, @user, GETDATE(), @sucu, NEWID())
                    END
                `);
        } catch (eCat) {
            console.warn(`[SYNC] No se pudo asegurar saCatArticulo ${co_cat}:`, eCat.message);
        }
    }

    const co_color = (item.co_color || '').trim();
    if (co_color) {
        const colDes = (item._color?.des_color || item.des_color || `COLOR ${co_color}`).trim();
        try {
            await pool.request()
                .input('co_color', sql.Char(6), padProfit(co_color, 6))
                .input('des_color', sql.VarChar(60), colDes)
                .input('user', sql.Char(6), padProfit(auditUser || 'PROFIT', 6))
                .input('sucu', sql.Char(6), padProfit(defaultAlmacen || '01', 6))
                .query(`
                    IF NOT EXISTS (SELECT 1 FROM saColor WHERE LTRIM(RTRIM(co_color)) = LTRIM(RTRIM(@co_color)))
                    BEGIN
                        INSERT INTO saColor (co_color, des_color, co_us_in, fe_us_in, co_sucu_in, rowguid)
                        VALUES (@co_color, @des_color, @user, GETDATE(), @sucu, NEWID())
                    END
                `);
        } catch (eCol) {
            console.warn(`[SYNC] No se pudo asegurar saColor ${co_color}:`, eCol.message);
        }
    }

    const co_ubi = (item.co_ubicacion || '').trim();
    if (co_ubi) {
        const ubiDes = (item._ubicacion?.des_ubicacion || item.des_ubicacion || `UBICACION ${co_ubi}`).trim();
        try {
            await pool.request()
                .input('co_ubi', sql.Char(6), padProfit(co_ubi, 6))
                .input('des_ubi', sql.VarChar(60), ubiDes)
                .input('user', sql.Char(6), padProfit(auditUser || 'PROFIT', 6))
                .input('sucu', sql.Char(6), padProfit(defaultAlmacen || '01', 6))
                .query(`
                    IF NOT EXISTS (SELECT 1 FROM saUbicacion WHERE LTRIM(RTRIM(co_ubicacion)) = LTRIM(RTRIM(@co_ubi)))
                    BEGIN
                        INSERT INTO saUbicacion (co_ubicacion, des_ubicacion, co_us_in, fe_us_in, co_sucu_in, rowguid)
                        VALUES (@co_ubi, @des_ubi, @user, GETDATE(), @sucu, NEWID())
                    END
                `);
        } catch (eUbi) {
            console.warn(`[SYNC] No se pudo asegurar saUbicacion ${co_ubi}:`, eUbi.message);
        }
    }
}

/**
 * Asegura que todas las unidades de medida (saUnidad) y sus asociaciones (saArtUnidad) existan.
 */
async function ensureArticleUnits(pool, co_art, unidadesList, defaultUni, auditUser, defaultAlmacen) {
    const cleanArt = String(co_art || '').trim();
    if (!cleanArt) return;

    const unitsToProcess = (Array.isArray(unidadesList) && unidadesList.length > 0)
        ? unidadesList
        : [{ co_uni: defaultUni || '01', uni_principal: 1, relacion: 1, equivalencia: 1, uso_venta: 1, uso_compra: 1 }];

    for (const u of unitsToProcess) {
        const cleanUni = String(u.co_uni || defaultUni || '01').trim();
        if (!cleanUni) continue;

        // 1. Asegurar en saUnidad
        const desUni = (u.des_uni || cleanUni).trim();
        try {
            await pool.request()
                .input('co_uni', sql.Char(6), padProfit(cleanUni, 6))
                .input('des_uni', sql.VarChar(60), desUni)
                .input('user', sql.Char(6), padProfit(auditUser || 'PROFIT', 6))
                .input('sucu', sql.Char(6), padProfit(defaultAlmacen || '01', 6))
                .query(`
                    IF NOT EXISTS (SELECT 1 FROM saUnidad WHERE LTRIM(RTRIM(co_uni)) = LTRIM(RTRIM(@co_uni)))
                    BEGIN
                        INSERT INTO saUnidad (co_uni, des_uni, co_us_in, fe_us_in, co_sucu_in, rowguid)
                        VALUES (@co_uni, @des_uni, @user, GETDATE(), @sucu, NEWID())
                    END
                `);
        } catch (eUni) {
            console.warn(`[SYNC] No se pudo asegurar saUnidad ${cleanUni}:`, eUni.message);
        }

        // 2. Asegurar en saArtUnidad
        try {
            await pool.request()
                .input('art', sql.Char(30), padProfit(cleanArt, 30))
                .input('uni', sql.Char(6), padProfit(cleanUni, 6))
                .input('relacion', sql.Bit, u.relacion ? 1 : 0)
                .input('equivalencia', sql.Decimal(18, 5), Number(u.equivalencia != null ? u.equivalencia : 1))
                .input('uni_princ', sql.Bit, u.uni_principal ? 1 : 0)
                .input('uso_vta', sql.Bit, u.uso_venta != null ? (u.uso_venta ? 1 : 0) : 1)
                .input('uso_cmp', sql.Bit, u.uso_compra != null ? (u.uso_compra ? 1 : 0) : 1)
                .input('user', sql.Char(6), padProfit(auditUser || 'PROFIT', 6))
                .input('sucu', sql.Char(6), padProfit(defaultAlmacen || '01', 6))
                .query(`
                    IF NOT EXISTS (SELECT 1 FROM saArtUnidad WHERE LTRIM(RTRIM(co_art)) = LTRIM(RTRIM(@art)) AND LTRIM(RTRIM(co_uni)) = LTRIM(RTRIM(@uni)))
                    BEGIN
                        INSERT INTO saArtUnidad (
                            co_art, co_uni, relacion, equivalencia, uso_venta, uso_compra,
                            uni_principal, uso_principal, uni_secundaria, uso_secundaria,
                            uso_numDecimales, num_decimales,
                            co_us_in, co_sucu_in, fe_us_in, co_us_mo, co_sucu_mo, fe_us_mo, rowguid
                        ) VALUES (
                            @art, @uni, @relacion, @equivalencia, @uso_vta, @uso_cmp,
                            @uni_princ, 1, 0, 0,
                            0, 0,
                            @user, @sucu, GETDATE(), @user, @sucu, GETDATE(), NEWID()
                        )
                    END
                `);
        } catch (eArtUni) {
            console.warn(`[SYNC] No se pudo asegurar saArtUnidad para ${cleanArt} (${cleanUni}):`, eArtUni.message);
        }
    }
}

// ────────────────────────────────────────────────────────────────────────────
// 2.4 POST /api/v1/articulos/import-batch — Importar lote de artículos faltantes
// ────────────────────────────────────────────────────────────────────────────
router.post('/import-batch', async (req, res) => {
    try {
        const { items } = req.body;
        if (!Array.isArray(items) || items.length === 0) {
            return res.status(200).json({ success: true, migrated: 0, errors: [] });
        }

        const srv = resolveServer(req);
        if (!srv) return res.status(404).json({ success: false, message: 'No hay sede disponible.' });

        const pool = await getPool(srv.id, req.sqlAuth);
        const [existingRes, linRes, sublRes, catRes, colRes, ubiRes, resSuc, resUni] = await Promise.all([
            pool.request().query('SELECT RTRIM(co_art) AS co_art FROM saArticulo'),
            pool.request().query('SELECT RTRIM(co_lin) AS id FROM saLineaArticulo'),
            pool.request().query('SELECT RTRIM(co_lin) AS co_lin, RTRIM(co_subl) AS co_subl FROM saSubLinea'),
            pool.request().query('SELECT RTRIM(co_cat) AS id FROM saCatArticulo'),
            pool.request().query('SELECT RTRIM(co_color) AS id FROM saColor'),
            pool.request().query('SELECT RTRIM(co_ubicacion) AS id FROM saUbicacion'),
            pool.request().query("SELECT TOP 1 RTRIM(co_sucur) AS co_sucur FROM saSucursal ORDER BY CASE WHEN RTRIM(co_sucur) = '01' THEN 0 ELSE 1 END, co_sucur"),
            pool.request().query("SELECT TOP 1 RTRIM(co_uni) AS co_uni FROM saUnidad ORDER BY CASE WHEN RTRIM(co_uni) = '01' THEN 0 WHEN RTRIM(co_uni) = 'UND' THEN 1 ELSE 2 END, co_uni")
        ]);

        const existingSet = new Set(existingRes.recordset.map(r => (r.co_art || '').trim().toUpperCase()));
        const linSet = new Set(linRes.recordset.map(r => (r.id || '').trim().toUpperCase()));
        const sublSet = new Set(sublRes.recordset.map(r => `${(r.co_lin || '').trim().toUpperCase()}__${(r.co_subl || '').trim().toUpperCase()}`));
        const sublByLin = new Map();
        for (const s of sublRes.recordset) {
            const lin = (s.co_lin || '').trim().toUpperCase();
            if (!sublByLin.has(lin)) sublByLin.set(lin, (s.co_subl || '').trim().toUpperCase());
        }
        const catSet = new Set(catRes.recordset.map(r => (r.id || '').trim().toUpperCase()));
        const colSet = new Set(colRes.recordset.map(r => (r.id || '').trim().toUpperCase()));
        const ubiSet = new Set(ubiRes.recordset.map(r => (r.id || '').trim().toUpperCase()));

        const configuredSucu = (srv.profit_branch_codes || []).find(b => b.is_default)?.code 
            || (srv.profit_branch_codes || [])[0]?.code 
            || (srv.profit_branch_codes || [])[0];
        const defaultAlmacen = configuredSucu || resSuc.recordset[0]?.co_sucur || '01';

        const defaultLin = linRes.recordset[0]?.id || '01';
        const defaultCat = catRes.recordset[0]?.id || '01';
        const defaultCol = colRes.recordset[0]?.id || '01';
        const defaultUbi = ubiRes.recordset[0]?.id || '01';
        const defaultUni = resUni.recordset[0]?.co_uni || '01';
        const auditUser = (req.profitUser || req.sqlAuth?.user || '01').substring(0, 10).toUpperCase();

        let migratedCount = 0;
        const errors = [];

        for (const item of items) {
            const co_art = (item.co_art || '').trim().toUpperCase();
            if (!co_art) continue;

            try {
                // 1. Asegurar dependencias de catálogos (Líneas, Sublíneas, Categorías, Colores, Ubicaciones)
                await ensureCatalogDependencies(pool, item, auditUser, defaultAlmacen);

                // 2. Si el artículo ya existe, asegurar que sus unidades en saArtUnidad estén presentes
                if (existingSet.has(co_art)) {
                    await ensureArticleUnits(pool, co_art, item.unidades, defaultUni, auditUser, defaultAlmacen);
                    continue;
                }

                const dataToInsert = { ...item };

                const cleanLin = (dataToInsert.co_lin || '').trim().toUpperCase();
                dataToInsert.co_lin = linSet.has(cleanLin) || (cleanLin && item._linea) ? dataToInsert.co_lin : defaultLin;

                const targetLin = (dataToInsert.co_lin || '').trim().toUpperCase();
                const targetSubl = (dataToInsert.co_subl || '').trim().toUpperCase();
                if (sublSet.has(`${targetLin}__${targetSubl}`) || (targetSubl && item._sublinea)) {
                    dataToInsert.co_subl = dataToInsert.co_subl;
                } else {
                    dataToInsert.co_subl = sublByLin.get(targetLin) || '01';
                }

                const cleanCat = (dataToInsert.co_cat || '').trim().toUpperCase();
                dataToInsert.co_cat = catSet.has(cleanCat) || (cleanCat && item._categoria) ? dataToInsert.co_cat : defaultCat;

                const cleanCol = (dataToInsert.co_color || '').trim().toUpperCase();
                dataToInsert.co_color = colSet.has(cleanCol) || (cleanCol && item._color) ? dataToInsert.co_color : defaultCol;

                const cleanUbi = (dataToInsert.co_ubicacion || '').trim().toUpperCase();
                dataToInsert.co_ubicacion = ubiSet.has(cleanUbi) || (cleanUbi && item._ubicacion) ? dataToInsert.co_ubicacion : defaultUbi;

                const f = new Date();
                const r = new sql.Request(pool);
                r.input('sCo_Art', sql.Char(30), dataToInsert.co_art);
                r.input('sdFecha_Reg', sql.SmallDateTime, f);
                r.input('sArt_Des', sql.VarChar(120), dataToInsert.art_des || 'NUEVO ARTÍCULO');
                r.input('sTipo', sql.Char(1), dataToInsert.tipo || 'V');
                r.input('bAnulado', sql.Bit, dataToInsert.anulado ? 1 : 0);
                r.input('sdFecha_Inac', sql.SmallDateTime, f);
                r.input('sCo_Lin', sql.Char(6), dataToInsert.co_lin);
                r.input('sCo_Subl', sql.Char(6), dataToInsert.co_subl);
                r.input('sCo_Cat', sql.Char(6), dataToInsert.co_cat);
                r.input('sCo_Color', sql.Char(6), dataToInsert.co_color);
                r.input('sCo_Ubicacion', sql.Char(6), dataToInsert.co_ubicacion);
                r.input('sItem', sql.VarChar(10), dataToInsert.item || null);
                r.input('sModelo', sql.VarChar(20), dataToInsert.modelo || '');
                r.input('sRef', sql.VarChar(20), dataToInsert.ref || null);
                r.input('bGenerico', sql.Bit, 0);
                r.input('bManeja_Serial', sql.Bit, 0);
                r.input('bManeja_Lote', sql.Bit, 0);
                r.input('bManeja_Lote_Venc', sql.Bit, 0);
                r.input('deMargen_Min', sql.Decimal(18, 5), 0);
                r.input('deMargen_Max', sql.Decimal(18, 5), 0);
                r.input('sTipo_Imp', sql.Char(1), dataToInsert.tipo_imp || '1');
                r.input('sTipo_Imp2', sql.Char(1), null);
                r.input('sTipo_Imp3', sql.Char(1), null);
                r.input('sCo_Reten', sql.Char(6), null);
                r.input('sCod_Proc', sql.Char(6), null);
                r.input('sGarantia', sql.VarChar(30), '');
                r.input('deVolumen', sql.Decimal(18, 5), Number(dataToInsert.volumen) || 0);
                r.input('dePeso', sql.Decimal(18, 5), Number(dataToInsert.peso) || 0);
                r.input('deStock_Min', sql.Decimal(18, 5), Number(dataToInsert.stock_min) || 0);
                r.input('deStock_Max', sql.Decimal(18, 5), Number(dataToInsert.stock_max) || 0);
                r.input('deStock_Pedido', sql.Decimal(18, 5), 0);
                r.input('iRelac_Unidad', sql.Int, 1);
                r.input('dePunt_Ven', sql.Decimal(18, 5), 0);
                r.input('dePunt_Cli', sql.Decimal(18, 5), 0);
                r.input('deLic_Mon_Ilc', sql.Decimal(18, 5), 0);
                r.input('deLic_Capacidad', sql.Decimal(18, 5), 0);
                r.input('deLic_Grado_Al', sql.Decimal(18, 5), 0);
                r.input('sLic_Tipo', sql.Char(1), null);
                r.input('bPrec_Om', sql.Bit, 0);
                r.input('sComentario', sql.VarChar(sql.MAX), 'Migrado vía Sincronización');
                r.input('sTipo_Cos', sql.Char(4), '1');
                r.input('dePorc_Margen_Minimo', sql.Decimal(18, 5), 0);
                r.input('dePorc_Margen_Maximo', sql.Decimal(18, 5), 0);
                r.input('deMont_Comi', sql.Decimal(18, 5), 0);
                r.input('dePorc_Arancel', sql.Decimal(18, 5), 0);
                r.input('sI_Art_Des', sql.VarChar(120), null);
                r.input('sDis_Cen', sql.VarChar(sql.MAX), null);
                r.input('sReten_Iva_Tercero', sql.Char(16), null);
                r.input('sCampo1', sql.VarChar(60), dataToInsert.campo1 || null);
                r.input('sCampo2', sql.VarChar(60), null);
                r.input('sCampo3', sql.VarChar(60), null);
                r.input('sCampo4', sql.VarChar(60), null);
                r.input('sCampo5', sql.VarChar(60), null);
                r.input('sCampo6', sql.VarChar(60), null);
                r.input('sCampo7', sql.VarChar(60), dataToInsert.campo7 || null);
                r.input('sCampo8', sql.VarChar(60), null);
                r.input('sCo_Us_In', sql.Char(6), auditUser);
                r.input('sCo_Sucu_In', sql.Char(6), defaultAlmacen);
                r.input('sMaquina', sql.VarChar(60), 'SYNC2K');
                r.input('sRevisado', sql.Char(1), null);
                r.input('sTrasnfe', sql.Char(1), null);

                await r.execute('pInsertarArticulo');

                // 3. Asegurar unidades del artículo en saArtUnidad
                await ensureArticleUnits(pool, dataToInsert.co_art, item.unidades, defaultUni, auditUser, defaultAlmacen);

                if (dataToInsert.campo7) {
                    await pool.request()
                        .input('art', sql.Char(30), dataToInsert.co_art)
                        .input('img', sql.VarChar(250), dataToInsert.campo7)
                        .query('UPDATE saArticulo SET campo7 = @img WHERE LTRIM(RTRIM(co_art)) = LTRIM(RTRIM(@art))');
                }

                if (dataToInsert.anulado) {
                    await pool.request()
                        .input('art', sql.Char(30), dataToInsert.co_art)
                        .query('UPDATE saArticulo SET anulado = 1, fecha_inac = GETDATE() WHERE LTRIM(RTRIM(co_art)) = LTRIM(RTRIM(@art))');
                }

                existingSet.add(co_art);
                migratedCount++;
            } catch (err) {
                errors.push(`Artículo ${co_art} (${item.art_des}): ${err.message}`);
            }
        }

        return res.status(200).json({
            success: true,
            sede_id: srv.id,
            sede_nombre: srv.name,
            migrated: migratedCount,
            errors
        });
    } catch (error) {
        console.error('[ARTICULOS IMPORT BATCH ERROR]:', error);
        res.status(500).json({ success: false, message: 'Error importando lote de artículos', error: error.message });
    }
});

// ────────────────────────────────────────────────────────────────────────────
// 2.5 GET /api/v1/articulos/next-code — Obtener el siguiente código disponible
// ────────────────────────────────────────────────────────────────────────────
/**
 * @swagger
 * /api/v1/articulos/next-code:
 *   get:
 *     summary: Obtener el próximo código disponible para un prefijo
 *     tags: [Articulos]
 *     parameters:
 *       - in: query
 *         name: prefix
 *         required: true
 *         schema:
 *           type: string
 *     responses:
 *       200:
 *         description: Próximo código
 */
router.get('/next-code', async (req, res) => {
    try {
        const prefix = req.query.prefix;
        if (!prefix) return res.status(400).json({ success: false, message: 'Prefijo requerido' });

        const servers = getServers();
        const serverToUse = servers.find(s => s.id === req.query.sede_id) || servers[0];
        if (!serverToUse) return res.status(500).json({ success: false, message: 'No hay sedes conectadas.' });

        const pool = await getPool(serverToUse.id, req.sqlAuth); // Usamos la sede disponible
        const result = await pool.request()
            .input('prefix', sql.VarChar, prefix + '%')
            .query(`SELECT MAX(LTRIM(RTRIM(co_art))) as last_code FROM saArticulo WHERE LTRIM(RTRIM(co_art)) LIKE @prefix`);

        let nextSeq = 1;
        const lastCode = result.recordset[0]?.last_code;
        if (lastCode) {
            const seqStr = lastCode.substring(prefix.length);
            const parsed = parseInt(seqStr, 10);
            if (!isNaN(parsed)) nextSeq = parsed + 1;
        }

        const nextCode = prefix + String(nextSeq).padStart(3, '0');
        res.status(200).json({ success: true, nextCode });
    } catch (error) {
        res.status(500).json({ success: false, message: 'Error interno.', error: error.message });
    }
});

// 2.8 POST /api/v1/articulos/bulk — Consulta masiva de artículos por lista de códigos
// ────────────────────────────────────────────────────────────────────────────
/**
 * @swagger
 * /api/v1/articulos/bulk:
 *   post:
 *     summary: Consulta masiva de artículos por lista de códigos
 *     tags: [Articulos]
 *     requestBody:
 *       required: true
 *       content:
 *         application/json:
 *           schema:
 *             type: object
 *             properties:
 *               codes:
 *                 type: array
 *                 items:
 *                   type: string
 *     responses:
 *       200:
 *         description: Lista de artículos encontrados
 */
router.post('/bulk', async (req, res) => {
    try {
        const rawCodes = req.body.codes || [];
        if (!Array.isArray(rawCodes) || rawCodes.length === 0) {
            return res.status(400).json({ success: false, message: 'Debe enviar un arreglo de códigos en "codes".' });
        }

        const codes = Array.from(new Set(rawCodes.map(c => String(c || '').trim()).filter(Boolean)));
        if (codes.length === 0) {
            return res.status(400).json({ success: false, message: 'No se encontraron códigos válidos.' });
        }

        const requestedSede = req.body.sede || req.body.sede_id || req.query.sede || req.query.sede_id;
        let servers = getServers();
        if (requestedSede && requestedSede !== "Todas") {
            servers = servers.filter(srv => srv.id === requestedSede || srv.name === requestedSede);
        }

        const idsEscaped = codes.map(c => `'${c.replace(/'/g, "''")}'`).join(',');

        const results = await Promise.all(servers.map(async (srv) => {
            try {
                const pool = await getPool(srv.id, req.sqlAuth);
                const r = pool.request();

                const querySQL = `
                    SELECT RTRIM(a.co_art) AS co_art, RTRIM(a.art_des) AS art_des, RTRIM(a.art_des) AS descripcion,
                           a.anulado, RTRIM(a.tipo) AS tipo, RTRIM(a.modelo) AS modelo, RTRIM(a.ref) AS referencia,
                           RTRIM(a.co_lin) AS co_lin, RTRIM(l.lin_des) AS linea,
                           RTRIM(a.co_cat) AS co_cat, RTRIM(c.cat_des) AS categoria,
                           RTRIM(aun.co_uni) AS co_uni, RTRIM(ISNULL(un.des_uni, aun.co_uni)) AS unidad,
                           RTRIM(a.tipo_imp) AS tipo_imp, RTRIM(a.campo7) AS campo7,
                           0 AS costo,
                           0 AS costo_bs
                    FROM saArticulo a
                    LEFT JOIN saLineaArticulo l ON a.co_lin = l.co_lin
                    LEFT JOIN saCatArticulo c ON a.co_cat = c.co_cat
                    LEFT JOIN (
                        SELECT co_art, co_uni, 
                               ROW_NUMBER() OVER(PARTITION BY co_art ORDER BY uni_principal DESC) as rn
                        FROM saArtUnidad
                    ) aun ON LTRIM(RTRIM(a.co_art)) = LTRIM(RTRIM(aun.co_art)) AND aun.rn = 1
                    LEFT JOIN saUnidad un ON LTRIM(RTRIM(aun.co_uni)) = LTRIM(RTRIM(un.co_uni))
                    WHERE LTRIM(RTRIM(a.co_art)) IN (${idsEscaped})
                `;

                const resData = await r.query(querySQL);
                const articulos = (resData.recordset || []).map(a => ({ ...a, sede_id: srv.id, sede_nombre: srv.name }));

                const tasa = await getExchangeRate(pool);
                const enriched = await enrichArticulos(pool, articulos, tasa, req.body.authorized_almacenes || req.query.authorized_almacenes);

                return enriched.map(item => {
                    const totalStock = (item.disponibilidad || []).reduce((acc, d) => acc + (Number(d.stock) || 0), 0);
                    return {
                        ...item,
                        stock: totalStock,
                        stock_global: totalStock
                    };
                });
            } catch (e) {
                console.error(`[POST /articulos/bulk] Error en sede ${srv.id}:`, e.message);
                return [];
            }
        }));

        const flattened = results.flat();
        return res.status(200).json({
            success: true,
            count: flattened.length,
            data: flattened
        });
    } catch (error) {
        console.error('[POST /articulos/bulk] Error general:', error);
        return res.status(500).json({ success: false, message: 'Error en consulta masiva de artículos.', error: error.message });
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
        const requestedSede = req.query.sede || req.query.sede_id;
        let servers = getServers();
        if (requestedSede && requestedSede !== "Todas") {
            servers = servers.filter(srv => srv.id === requestedSede || srv.name === requestedSede);
        }

        const results = await Promise.all(servers.map(async (srv) => {
            try {
                const pool = await getPool(srv.id, req.sqlAuth);

                const [resArt, resStock, resPre] = await Promise.all([
                    pool.request().input('co_art', sql.VarChar, co_art).query(
                        `SELECT RTRIM(a.co_art) AS co_art, RTRIM(a.art_des) AS descripcion,
                                a.anulado, RTRIM(a.tipo) AS tipo_articulo,
                                RTRIM(a.modelo) AS modelo, RTRIM(a.ref) AS ref, RTRIM(a.cod_proc) AS cod_proc,
                                RTRIM(a.co_lin) AS co_lin, RTRIM(l.lin_des) AS linea, 
                                RTRIM(a.co_subl) AS co_subl, RTRIM(sl.subl_des) AS sublinea, 
                                RTRIM(a.co_cat) AS co_cat, RTRIM(c.cat_des) AS categoria,
                                RTRIM(a.co_color) AS co_color,
                                RTRIM(au.co_ubicacion) AS co_ubicacion, RTRIM(u1.des_ubicacion) AS ubicacion,
                                RTRIM(au.co_ubicacion2) AS co_ubicacion2, RTRIM(u2.des_ubicacion) AS ubicacion2,
                                RTRIM(au.co_ubicacion3) AS co_ubicacion3, RTRIM(u3.des_ubicacion) AS ubicacion3,
                                RTRIM(aun.co_uni) AS co_uni, RTRIM(un.des_uni) AS unidad,
                                RTRIM(a.tipo_imp) AS tipo_imp,
                                ISNULL(a.peso, 0) AS peso,
                                ISNULL(a.volumen, 0) AS volumen,
                                ISNULL(a.stock_min, 0) AS stock_min,
                                ISNULL(a.stock_max, 0) AS stock_max,
                                RTRIM(a.garantia) AS garantia,
                                RTRIM(a.comentario) AS comentario,
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
                        `SELECT RTRIM(s.co_alma) AS co_alma, RTRIM(alm.des_alma) AS des_alma,
                                (SUM(ISNULL(CASE WHEN RTRIM(s.tipo)='ACT' THEN s.stock ELSE 0 END, 0)) -
                                 SUM(ISNULL(CASE WHEN RTRIM(s.tipo)='COM' THEN s.stock ELSE 0 END, 0))) AS stock
                          FROM saStockAlmacen s
                          LEFT JOIN saAlmacen alm ON s.co_alma = alm.co_alma
                          WHERE LTRIM(RTRIM(s.co_art)) = LTRIM(RTRIM(@co_art))
                          GROUP BY s.co_alma, alm.des_alma
                          HAVING (SUM(ISNULL(CASE WHEN RTRIM(s.tipo)='ACT' THEN s.stock ELSE 0 END, 0)) -
                                  SUM(ISNULL(CASE WHEN RTRIM(s.tipo)='COM' THEN s.stock ELSE 0 END, 0))) > 0`
                    ),
                    pool.request().input('co_art', sql.VarChar, co_art).query(
                        `WITH UP AS (
                            SELECT RTRIM(p.co_precio) AS id_precio, p.monto AS precio,
                                   RTRIM(p.co_mone) AS moneda, ISNULL(m.monto_min, 0) AS margen,
                                   ROW_NUMBER() OVER(PARTITION BY p.co_precio ORDER BY p.desde DESC) AS rn
                            FROM saArtPrecio p
                            LEFT JOIN saArtMargen m ON p.co_art = m.co_art AND p.co_precio = m.co_precio
                            WHERE LTRIM(RTRIM(p.co_art)) = LTRIM(RTRIM(@co_art))
                              AND p.Inactivo=0 AND GETDATE()>=p.desde AND (p.hasta IS NULL OR GETDATE()<=p.hasta)
                         )
                         SELECT id_precio, precio, moneda, margen FROM UP WHERE rn=1 ORDER BY id_precio`
                    )
                ]);

                if (!resArt.recordset.length) return null;
                const tasa = await getExchangeRate(pool);

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

        const outcome = await executeWrite(req.query.sede || null, req.sqlAuth, async (pool, srv) => {
            const f = new Date();
            const [resLin, resSubl, resCat, resCol, resSuc] = await Promise.all([
                pool.request().query('SELECT TOP 1 RTRIM(co_lin) AS id FROM saLineaArticulo'),
                pool.request().query('SELECT TOP 1 RTRIM(co_subl) AS id FROM saSubLinea'),
                pool.request().query('SELECT TOP 1 RTRIM(co_cat) AS id FROM saCatArticulo'),
                pool.request().query('SELECT TOP 1 RTRIM(co_color) AS id FROM saColor'),
                pool.request().query("SELECT TOP 1 RTRIM(co_sucur) AS co_sucur FROM saSucursal ORDER BY CASE WHEN RTRIM(co_sucur) = '01' THEN 0 ELSE 1 END, co_sucur")
            ]);

            const configuredSucu = (srv.profit_branch_codes || []).find(b => b.is_default)?.code 
                || (srv.profit_branch_codes || [])[0]?.code 
                || (srv.profit_branch_codes || [])[0];
            const defaultAlmacen = configuredSucu || resSuc.recordset[0]?.co_sucur || '01';

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
            r.input('sCo_Ubicacion', sql.Char(6), data.co_ubicacion || '01    ');
            r.input('sItem', sql.VarChar(10), data.item || null);
            r.input('sModelo', sql.VarChar(20), data.modelo || '');
            r.input('sRef', sql.VarChar(20), data.ref || null);
            r.input('bGenerico', sql.Bit, 0);
            r.input('bManeja_Serial', sql.Bit, 0);
            r.input('bManeja_Lote', sql.Bit, 0);
            r.input('bManeja_Lote_Venc', sql.Bit, 0);
            r.input('deMargen_Min', sql.Decimal(18, 5), 0);
            r.input('deMargen_Max', sql.Decimal(18, 5), 0);
            r.input('sTipo_Imp', sql.Char(1), data.tipo_imp || '1');
            r.input('sTipo_Imp2', sql.Char(1), null);
            r.input('sTipo_Imp3', sql.Char(1), null);
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
            r.input('sCampo1', sql.VarChar(60), null);
            r.input('sCampo2', sql.VarChar(60), null);
            r.input('sCampo3', sql.VarChar(60), null);
            r.input('sCampo4', sql.VarChar(60), null);
            r.input('sCampo5', sql.VarChar(60), null);
            r.input('sCampo6', sql.VarChar(60), null);
            r.input('sCampo7', sql.VarChar(60), null);
            r.input('sCampo8', sql.VarChar(60), null);
            r.input('sCo_Us_In', sql.Char(6), '999');
            r.input('sCo_Sucu_In', sql.Char(6), defaultAlmacen);
            r.input('sMaquina', sql.VarChar(60), 'SYNC2K');
            r.input('sRevisado', sql.Char(1), null);
            r.input('sTrasnfe', sql.Char(1), null);
            await r.execute('pInsertarArticulo');

            // Refuerzo para asegurar valores por defecto y tipos de impuesto
            await pool.request()
                .input('co_art', sql.Char(30), data.co_art)
                .input('ubic', sql.Char(6), data.co_ubicacion || '01    ')
                .input('sucu', sql.Char(6), defaultAlmacen)
                .query(`
                    UPDATE saArticulo 
                    SET co_ubicacion = @ubic,
                        co_sucu_in = ISNULL(co_sucu_in, @sucu),
                        co_sucu_mo = @sucu,
                        tipo_imp2 = NULL,
                        tipo_imp3 = NULL,
                        relac_unidad = 0,
                        revisado = NULL,
                        trasnfe = NULL,
                        campo1 = NULL, campo2 = NULL, campo3 = NULL, campo4 = NULL,
                        campo5 = NULL, campo6 = NULL, campo7 = NULL, campo8 = NULL
                    WHERE LTRIM(RTRIM(co_art)) = LTRIM(RTRIM(@co_art))
                `);
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
        console.log(`[PUT /articulos/:co_art] Petición recibida para UPSERT: ${coArtOri}`);

        const outcome = await executeWrite(req.query.sede || null, req.sqlAuth, async (pool, srv) => {
            // Obtener almacén por defecto de la configuración de la sede
            const defaultAlmacen = (srv.profit_branch_codes || []).find(b => b.is_default)?.code || (srv.profit_branch_codes || [])[0]?.code || '01';
            console.log(`[UPSERT] Ejecutando en base de datos conectada...`);

            // Descubrir código de moneda para dólares (USD, US$, etc.)
            const resUSD = await pool.request().query(
                `SELECT TOP 1 RTRIM(co_mone) AS co_mone FROM saMoneda WHERE LTRIM(RTRIM(co_mone)) IN ('US$','USD','DOL','$','US') OR mone_des LIKE '%Dolar%'`
            );
            const usdCode = resUSD.recordset[0]?.co_mone || 'USD';
            const check = await pool.request().input('co_art', sql.VarChar, coArtOri).query(
                `SELECT validador, RTRIM(co_lin) AS co_lin, RTRIM(co_subl) AS co_subl,
                        RTRIM(co_cat) AS co_cat, RTRIM(co_color) AS co_color,
                        RTRIM(co_ubicacion) AS co_ubicacion, tipo_imp, tipo_cos
                 FROM saArticulo WHERE LTRIM(RTRIM(co_art)) = LTRIM(RTRIM(@co_art))`
            );

            const isNew = check.recordset.length === 0;
            console.log(`[UPSERT] ¿Es artículo nuevo?: ${isNew}`);

            const row = isNew ? {} : check.recordset[0];
            const f = new Date();
            const r = new sql.Request(pool);

            // Si es nuevo y no manda línea, intentamos agarrar la primera disponible para evitar errores NOT NULL
            let defaultLin = data.co_lin, defaultSubl = data.co_subl, defaultCat = data.co_cat, defaultColor = data.co_color, defaultUbic = data.co_ubicacion;
            if (isNew) {
                const [resDefaults] = await Promise.all([
                    pool.request().query(`
                        SELECT TOP 1 
                            RTRIM(co_lin) as co_lin, 
                            RTRIM(co_subl) as co_subl, 
                            RTRIM(co_cat) as co_cat, 
                            RTRIM(co_color) as co_color 
                        FROM saArticulo
                    `)
                ]);
                const defs = resDefaults.recordset[0] || {};
                defaultLin = data.co_lin || defs.co_lin || '01';
                defaultSubl = data.co_subl || defs.co_subl || '01';
                defaultCat = data.co_cat || defs.co_cat || '01';
                defaultColor = data.co_color || defs.co_color || '01';
                defaultUbic = data.co_ubicacion || '01    ';
            }

            r.input('sCo_Art', sql.Char(30), data.co_art || coArtOri);
            if (!isNew) r.input('sCo_ArtOri', sql.Char(30), coArtOri);
            r.input('sdFecha_Reg', sql.SmallDateTime, f);
            r.input('sArt_Des', sql.VarChar(120), data.art_des || (isNew ? 'NUEVO ARTÍCULO' : 'Artículo Modificado API'));
            r.input('sTipo', sql.Char(1), data.tipo || 'V');
            r.input('bAnulado', sql.Bit, 0);
            r.input('sdFecha_Inac', sql.SmallDateTime, f);
            r.input('sCo_Lin', sql.Char(6), isNew ? defaultLin : (data.co_lin || row.co_lin));
            r.input('sCo_Subl', sql.Char(6), isNew ? defaultSubl : (data.co_subl || row.co_subl));
            r.input('sCo_Cat', sql.Char(6), isNew ? defaultCat : (data.co_cat || row.co_cat));
            r.input('sCo_Color', sql.Char(6), isNew ? defaultColor : (data.co_color || row.co_color));
            r.input('sCo_Ubicacion', sql.Char(6), isNew ? defaultUbic : (data.co_ubicacion || row.co_ubicacion));
            r.input('sItem', sql.VarChar(10), data.item || null);
            r.input('sModelo', sql.VarChar(20), data.modelo || '');
            r.input('sRef', sql.VarChar(20), data.ref || null);
            r.input('bGenerico', sql.Bit, 0);
            r.input('bManeja_Serial', sql.Bit, data.bManeja_Serial ? 1 : 0);
            r.input('bManeja_Lote', sql.Bit, data.bManeja_Lote ? 1 : 0);
            r.input('bManeja_Lote_Venc', sql.Bit, 0);
            r.input('deMargen_Min', sql.Decimal(18, 5), 0);
            r.input('deMargen_Max', sql.Decimal(18, 5), 0);
            r.input('sTipo_Imp', sql.Char(1), data.tipo_imp || row.tipo_imp || '1');
            r.input('sTipo_Imp2', sql.Char(1), null);
            r.input('sTipo_Imp3', sql.Char(1), null);
            r.input('sCo_Reten', sql.Char(6), null);
            r.input('sCod_Proc', sql.Char(6), data.cod_proc || null);
            r.input('sGarantia', sql.VarChar(30), data.garantia?.toString() || '');
            r.input('deVolumen', sql.Decimal(18, 5), data.volumen ? Number(data.volumen) : 0);
            r.input('dePeso', sql.Decimal(18, 5), data.peso ? Number(data.peso) : 0);
            r.input('deStock_Min', sql.Decimal(18, 5), data.stock_min ? Number(data.stock_min) : 0);
            r.input('deStock_Max', sql.Decimal(18, 5), data.stock_max ? Number(data.stock_max) : 0);
            r.input('deStock_Pedido', sql.Decimal(18, 5), 0);
            r.input('iRelac_Unidad', sql.Int, 1);
            r.input('dePunt_Ven', sql.Decimal(18, 5), 0);
            r.input('dePunt_Cli', sql.Decimal(18, 5), 0);
            r.input('deLic_Mon_Ilc', sql.Decimal(18, 5), 0);
            r.input('deLic_Capacidad', sql.Decimal(18, 5), 0);
            r.input('deLic_Grado_Al', sql.Decimal(18, 5), 0);
            r.input('sLic_Tipo', sql.Char(1), null);
            r.input('bPrec_Om', sql.Bit, 0);
            r.input('sComentario', sql.VarChar(sql.MAX), isNew ? 'Creado via API' : 'Editado via API');
            r.input('sTipo_Cos', sql.Char(4), isNew ? '1' : (data.tipo_cos || row.tipo_cos || '1'));
            r.input('dePorc_Margen_Minimo', sql.Decimal(18, 5), 0);
            r.input('dePorc_Margen_Maximo', sql.Decimal(18, 5), 0);
            r.input('deMont_Comi', sql.Decimal(18, 5), 0);
            r.input('dePorc_Arancel', sql.Decimal(18, 5), 0);
            if (isNew) r.input('sI_Art_Des', sql.VarChar(120), null);
            r.input('sDis_Cen', sql.VarChar(sql.MAX), null);
            r.input('sReten_Iva_Tercero', sql.Char(16), null);
            r.input('sCampo1', sql.VarChar(60), null);
            r.input('sCampo2', sql.VarChar(60), null);
            r.input('sCampo3', sql.VarChar(60), null);
            r.input('sCampo4', sql.VarChar(60), null);
            r.input('sCampo5', sql.VarChar(60), null);
            r.input('sCampo6', sql.VarChar(60), null);
            r.input('sCampo7', sql.VarChar(60), null);
            r.input('sCampo8', sql.VarChar(60), null);

            const auditUser = (req.profitUser || req.sqlAuth?.user || '999').substring(0, 10).toUpperCase();

            if (isNew) {
                r.input('sCo_Us_In', sql.Char(6), auditUser);
                r.input('sCo_Sucu_In', sql.Char(6), defaultAlmacen);
            } else {
                r.input('sCo_Us_Mo', sql.Char(6), auditUser);
                r.input('sCo_Sucu_Mo', sql.Char(6), defaultAlmacen);
                r.input('tsValidador', sql.VarBinary(8), row.validador);
                r.input('gRowguid', sql.UniqueIdentifier, null);
            }

            r.input('sMaquina', sql.VarChar(60), 'SYNC2K');
            if (!isNew) r.input('sCampos', sql.VarChar(sql.MAX), '');
            r.input('sRevisado', sql.Char(1), null);
            r.input('sTrasnfe', sql.Char(1), null);

            if (isNew) {
                console.log(`[UPSERT] Ejecutando pInsertarArticulo...`);
                await r.execute('pInsertarArticulo');
            } else {
                console.log(`[UPSERT] Ejecutando pActualizarArticulo...`);
                await r.execute('pActualizarArticulo');
            }

            // Corregir columnas que el SP no mapea correctamente
            const artId = data.co_art || coArtOri;
            await pool.request()
                .input('co_art', sql.Char(30), artId)
                .input('revisado', sql.Char(1), '0')
                .input('trasnfe', sql.Char(1), '0')
                .input('tipo_imp2', sql.Char(1), null)
                .input('tipo_imp3', sql.Char(1), null)
                .input('garantia', sql.VarChar(30), data.garantia?.toString() || '0')
                .input('ref', sql.VarChar(20), data.ref || null)
                .input('fecha_inac', sql.SmallDateTime, null)
                .input('sucu', sql.Char(6), defaultAlmacen)
                .input('user', sql.Char(6), auditUser)
                .input('is_new', sql.Bit, isNew ? 1 : 0)
                .input('ubic', sql.Char(6), isNew ? defaultUbic : (data.co_ubicacion || null))
                .input('comentario', sql.VarChar(sql.MAX), isNew ? 'Creado via API' : 'Editado via API')
                .query(`
                    UPDATE saArticulo SET
                        revisado   = NULL,
                        trasnfe    = NULL,
                        tipo_imp2  = NULL,
                        tipo_imp3  = NULL,
                        garantia   = ISNULL(garantia, @garantia),
                        ref        = ISNULL(ref, @ref),
                        fecha_inac = CASE WHEN anulado = 0 THEN NULL ELSE fecha_inac END,
                        co_sucu_in = ISNULL(co_sucu_in, @sucu),
                        co_sucu_mo = @sucu,
                        co_us_mo   = @user,
                        fe_us_mo   = GETDATE(),
                        relac_unidad = 0,
                        comentario = @comentario,
                        campo1 = NULL, campo2 = NULL, campo3 = NULL, campo4 = NULL,
                        campo5 = NULL, campo6 = NULL, campo7 = NULL, campo8 = NULL,
                        co_ubicacion = CASE 
                            WHEN @is_new = 1 THEN ISNULL(@ubic, co_ubicacion)
                            WHEN @ubic IS NOT NULL THEN @ubic
                            ELSE co_ubicacion 
                        END
                    WHERE LTRIM(RTRIM(co_art)) = LTRIM(RTRIM(@co_art))
                `);


            console.log(`[UPSERT] Actualizando saArtUnidad...`);
            // --- 2. Guardar Unidad de Medida Primaria (saArtUnidad) ---
            if (data.co_uni) {
                const uCheck = await pool.request()
                    .input('co_art', sql.Char(30), data.co_art || coArtOri)
                    .input('co_uni', sql.Char(6), data.co_uni)
                    .query('SELECT 1 FROM saArtUnidad WHERE LTRIM(RTRIM(co_art)) = LTRIM(RTRIM(@co_art)) AND LTRIM(RTRIM(co_uni)) = LTRIM(RTRIM(@co_uni))');

                // Si la unidad enviada no está enlazada al artículo, la insertamos y la marcamos como principal
                if (uCheck.recordset.length === 0) {
                    await pool.request()
                        .input('co_art', sql.Char(30), data.co_art || coArtOri)
                        .input('co_uni', sql.Char(6), data.co_uni)
                        .input('sucu', sql.Char(6), defaultAlmacen)
                        .input('user', sql.Char(6), auditUser)
                        .query(`
                            UPDATE saArtUnidad SET uni_principal = 0, co_sucu_mo = @sucu, co_us_mo = @user, fe_us_mo = GETDATE() WHERE LTRIM(RTRIM(co_art)) = LTRIM(RTRIM(@co_art));
                            INSERT INTO saArtUnidad (co_art, co_uni, relacion, equivalencia, uso_venta, uso_compra, uni_principal, uso_principal, uni_secundaria, uso_secundaria, uso_numDecimales, num_decimales, co_us_in, fe_us_in, co_us_mo, fe_us_mo, co_sucu_in, co_sucu_mo)
                            VALUES (@co_art, @co_uni, 1, 1, 1, 1, 1, 1, 0, 0, 0, 2, @user, GETDATE(), @user, GETDATE(), @sucu, @sucu);
                        `);
                } else {
                    // Si ya estaba enlazada, solo nos aseguramos de que sea la principal
                    await pool.request()
                        .input('co_art', sql.Char(30), data.co_art || coArtOri)
                        .input('co_uni', sql.Char(6), data.co_uni)
                        .input('sucu', sql.Char(6), defaultAlmacen)
                        .input('user', sql.Char(6), auditUser)
                        .query(`
                            UPDATE saArtUnidad SET uni_principal = 0, co_sucu_mo = @sucu, co_us_mo = @user, fe_us_mo = GETDATE() WHERE LTRIM(RTRIM(co_art)) = LTRIM(RTRIM(@co_art));
                            UPDATE saArtUnidad SET uni_principal = 1, co_sucu_mo = @sucu, co_us_mo = @user, fe_us_mo = GETDATE() WHERE LTRIM(RTRIM(co_art)) = LTRIM(RTRIM(@co_art)) AND LTRIM(RTRIM(co_uni)) = LTRIM(RTRIM(@co_uni));
                        `);
                }
            }

            // --- 3. Guardar Precios y Márgenes (saArtPrecio) ---
            // Revisamos los tipos de precio 1, 2, 3, 4 y 5
            for (let i = 1; i <= 5; i++) {
                const margen = data[`margen_${i}`];
                const precio = data[`precio_${i}`];

                if ((margen !== undefined && margen !== null && margen !== '') ||
                    (precio !== undefined && precio !== null && precio !== '')) {
                    const numMargen = margen !== undefined && margen !== null && margen !== '' ? Number(margen) : 0;
                    const numPrecio = precio !== undefined && precio !== null && precio !== '' ? Number(precio) : 0;
                    const precioId = String(i); // '1', '2', '3', '4', '5' (según saTipoPrecio)

                    const activePriceRes = await pool.request()
                        .input('co_art', sql.Char(30), data.co_art || coArtOri)
                        .input('co_precio', sql.Char(6), precioId)
                        .query(`
                            SELECT TOP 1 desde, co_alma_calculado 
                            FROM saArtPrecio 
                            WHERE LTRIM(RTRIM(co_art)) = LTRIM(RTRIM(@co_art)) AND LTRIM(RTRIM(co_precio)) = @co_precio
                            ORDER BY desde DESC
                        `);

                    if (activePriceRes.recordset.length === 0) {
                        // Insertar precio
                        await pool.request()
                            .input('co_art', sql.Char(30), data.co_art || coArtOri)
                            .input('co_precio', sql.Char(6), precioId)
                            .input('margen', sql.Decimal(18, 5), numMargen)
                            .input('monto', sql.Decimal(18, 5), numPrecio)
                            .input('sucu', sql.Char(6), defaultAlmacen)
                            .input('user', sql.Char(6), auditUser)
                            .input('mone', sql.Char(6), usdCode)
                            .query(`
                                INSERT INTO saArtPrecio (
                                    co_art, co_precio, co_mone, desde, hasta, Inactivo, monto, precioOm, 
                                    co_us_in, fe_us_in, co_us_mo, fe_us_mo, co_sucu_in, co_sucu_mo,
                                    montoadi1, montoadi2, montoadi3, montoadi4, montoadi5
                                )
                                VALUES (
                                    @co_art, @co_precio, @mone, GETDATE(), NULL, 0, @monto, 1, 
                                    @user, GETDATE(), @user, GETDATE(), @sucu, @sucu,
                                    0.0, 0.0, 0.0, 0.0, 0.0
                                );
                                
                                INSERT INTO saArtMargen (co_art, co_precio, monto_min, monto_max, co_us_in, fe_us_in, co_us_mo, fe_us_mo)
                                VALUES (@co_art, @co_precio, @margen, @margen, @user, GETDATE(), @user, GETDATE());
                            `);
                    } else {
                        // Actualizar precio existente
                        const originalDesde = activePriceRes.recordset[0].desde;
                        const originalAlma = activePriceRes.recordset[0].co_alma_calculado;

                        const updateRes = await pool.request()
                            .input('co_art', sql.Char(30), data.co_art || coArtOri)
                            .input('co_precio', sql.Char(6), precioId)
                            .input('margen', sql.Decimal(18, 5), numMargen)
                            .input('monto', sql.Decimal(18, 5), numPrecio)
                            .input('sucu', sql.Char(6), defaultAlmacen)
                            .input('user', sql.Char(6), auditUser)
                            .input('mone', sql.Char(6), usdCode)
                            .input('originalDesde', sql.SmallDateTime, originalDesde)
                            .input('originalAlma', sql.Char(6), originalAlma)
                            .query(`
                                UPDATE saArtPrecio SET
                                    monto = @monto,
                                    precioOm = 1,
                                    hasta = NULL,
                                    co_mone = @mone,
                                    co_sucu_mo = @sucu,
                                    co_us_mo = @user,
                                    fe_us_mo = GETDATE(),
                                    montoadi1 = 0.0,
                                    montoadi2 = 0.0,
                                    montoadi3 = 0.0,
                                    montoadi4 = 0.0,
                                    montoadi5 = 0.0
                                WHERE LTRIM(RTRIM(co_art)) = LTRIM(RTRIM(@co_art)) 
                                  AND LTRIM(RTRIM(co_precio)) = @co_precio
                                  AND desde = @originalDesde
                                  AND (co_alma_calculado = @originalAlma OR (co_alma_calculado IS NULL AND @originalAlma IS NULL));

                                IF EXISTS (SELECT 1 FROM saArtMargen WHERE LTRIM(RTRIM(co_art)) = LTRIM(RTRIM(@co_art)) AND LTRIM(RTRIM(co_precio)) = @co_precio)
                                BEGIN
                                    UPDATE saArtMargen 
                                    SET monto_min = @margen, monto_max = @margen, co_us_mo = @user, fe_us_mo = GETDATE()
                                    WHERE LTRIM(RTRIM(co_art)) = LTRIM(RTRIM(@co_art)) AND LTRIM(RTRIM(co_precio)) = @co_precio
                                END
                                ELSE
                                BEGIN
                                    INSERT INTO saArtMargen (co_art, co_precio, monto_min, monto_max, co_us_in, fe_us_in, co_us_mo, fe_us_mo)
                                    VALUES (@co_art, @co_precio, @margen, @margen, @user, GETDATE(), @user, GETDATE());
                                END
                            `);

                        if (updateRes.rowsAffected[0] === 0) {
                            console.log(`⚠️ [AGENT] UPDATE estricto no afectó filas. Intentando UPDATE general por co_art y co_precio...`);
                            const fallbackUpdateRes = await pool.request()
                                .input('co_art', sql.Char(30), data.co_art || coArtOri)
                                .input('co_precio', sql.Char(6), precioId)
                                .input('monto', sql.Decimal(18, 5), numPrecio)
                                .input('sucu', sql.Char(6), defaultAlmacen)
                                .input('user', sql.Char(6), auditUser)
                                .input('mone', sql.Char(6), usdCode)
                                .query(`
                                    UPDATE saArtPrecio SET
                                        monto = @monto,
                                        precioOm = 1,
                                        hasta = NULL,
                                        co_mone = @mone,
                                        co_sucu_mo = @sucu,
                                        co_us_mo = @user,
                                        fe_us_mo = GETDATE()
                                    WHERE LTRIM(RTRIM(co_art)) = LTRIM(RTRIM(@co_art)) 
                                      AND LTRIM(RTRIM(co_precio)) = @co_precio;
                                `);

                            if (fallbackUpdateRes.rowsAffected[0] === 0) {
                                console.log(`🚀 [AGENT] Ningún UPDATE afectó filas. Creando precio (INSERT)...`);
                                await pool.request()
                                    .input('co_art', sql.Char(30), data.co_art || coArtOri)
                                    .input('co_precio', sql.Char(6), precioId)
                                    .input('margen', sql.Decimal(18, 5), numMargen)
                                    .input('monto', sql.Decimal(18, 5), numPrecio)
                                    .input('sucu', sql.Char(6), defaultAlmacen)
                                    .input('user', sql.Char(6), auditUser)
                                    .input('mone', sql.Char(6), usdCode)
                                    .query(`
                                        INSERT INTO saArtPrecio (
                                            co_art, co_precio, co_mone, desde, hasta, Inactivo, monto, precioOm, 
                                            co_us_in, fe_us_in, co_us_mo, fe_us_mo, co_sucu_in, co_sucu_mo,
                                            montoadi1, montoadi2, montoadi3, montoadi4, montoadi5
                                        )
                                        VALUES (
                                            @co_art, @co_precio, @mone, GETDATE(), NULL, 0, @monto, 1, 
                                            @user, GETDATE(), @user, GETDATE(), @sucu, @sucu,
                                            0.0, 0.0, 0.0, 0.0, 0.0
                                        );
                                        
                                        IF NOT EXISTS (SELECT 1 FROM saArtMargen WHERE LTRIM(RTRIM(co_art)) = LTRIM(RTRIM(@co_art)) AND LTRIM(RTRIM(co_precio)) = @co_precio)
                                        BEGIN
                                            INSERT INTO saArtMargen (co_art, co_precio, monto_min, monto_max, co_us_in, fe_us_in, co_us_mo, fe_us_mo)
                                            VALUES (@co_art, @co_precio, @margen, @margen, @user, GETDATE(), @user, GETDATE());
                                        END
                                    `);
                            }
                        }
                    }
                }
            }
            console.log(`[UPSERT] Operación completada exitosamente.`);
        });

        return writeResponse(res, outcome, `Sede "${req.query.sede}" no encontrada.`);
    } catch (error) {
        console.error(`[PUT /articulos/:co_art] Error Catastrófico:`, error);
        res.status(500).json({ success: false, message: 'Error interno en UPSERT.', error: error.message });
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

// ────────────────────────────────────────────────────────────────────────────
// 6. DELETE /api/v1/articulos/:co_art — Eliminar articulo (targeted o broadcast)
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
 *         schema: { type: string }
 *       - in: query
 *         name: sede
 *         schema: { type: string }
 *     responses:
 *       200:
 *         description: Artículo eliminado
 */
router.delete('/:co_art', async (req, res) => {
    try {
        const { co_art } = req.params;

        const outcome = await executeWrite(req.query.sede || null, req.sqlAuth, async (pool, srv) => {
            const auditUser = (req.profitUser || req.sqlAuth?.user || '01').substring(0, 10).toUpperCase();

            const check = await pool.request().input('co_art', sql.VarChar, co_art).query(
                `SELECT validador FROM saArticulo WHERE LTRIM(RTRIM(co_art)) = LTRIM(RTRIM(@co_art))`
            );
            if (!check.recordset.length) throw new Error('El artículo no existe en esta sede.');

            const defaultAlmacen = (srv?.profit_branch_codes || []).find(b => b.is_default)?.code || (srv?.profit_branch_codes || [])[0]?.code || '01';

            const r = new sql.Request(pool);
            r.input('sCo_ArtOri', sql.Char(30), co_art);
            r.input('tsValidador', sql.VarBinary(8), check.recordset[0].validador);
            r.input('sMaquina', sql.VarChar(60), 'SYNC2K');
            r.input('sCo_Us_Mo', sql.Char(6), auditUser);
            r.input('sCo_Sucu_Mo', sql.Char(6), defaultAlmacen);
            r.input('gRowguid', sql.UniqueIdentifier, null);

            try {
                await r.execute('pEliminarArticulo');
            } catch (err) {
                // If it's a foreign key constraint error, it means the article is in use
                if (err.message && err.message.includes('REFERENCE constraint')) {
                    throw new Error('El artículo no se puede eliminar porque ya tiene documentos o movimientos asociados en Profit Plus.');
                }
                throw err;
            }
        });

        return writeResponse(res, outcome, `Sede "${req.query.sede}" no encontrada.`);
    } catch (error) {
        res.status(500).json({ success: false, message: 'Error al eliminar el artículo.', error: error.message });
    }
});


// ────────────────────────────────────────────────────────────────────────────
// 7. PUT /api/v1/articulos/:co_art/imagen — Actualizar imagen (campo7)
// ────────────────────────────────────────────────────────────────────────────
/**
 * @swagger
 * /api/v1/articulos/{co_art}/imagen:
 *   put:
 *     summary: Actualizar la URL de la imagen del artículo (campo7)
 *     tags: [Articulos]
 *     parameters:
 *       - in: path
 *         name: co_art
 *         required: true
 *         schema: { type: string }
 *     requestBody:
 *       required: true
 *       content:
 *         application/json:
 *           schema:
 *             type: object
 *             properties:
 *               imageUrl:
 *                 type: string
 *                 description: URL pública de la imagen en Supabase (o cualquier storage)
 *     responses:
 *       200:
 *         description: Imagen actualizada exitosamente
 */
router.put('/:co_art/imagen', async (req, res) => {
    try {
        const { co_art } = req.params;
        const { imageUrl } = req.body;

        if (imageUrl === undefined) {
            return res.status(400).json({ success: false, message: 'El parámetro "imageUrl" es obligatorio.' });
        }

        // Si no se especifica sede, el broadcast es automático a todas las sedes activas
        const outcome = await executeWrite(req.query.sede || null, req.sqlAuth, async (pool) => {
            const auditUser = (req.profitUser || req.sqlAuth?.user || 'API').substring(0, 10).toUpperCase();

            const r = new sql.Request(pool);
            r.input('co_art', sql.Char(30), co_art.trim());
            r.input('imageUrl', sql.VarChar(250), imageUrl); // campo7 es varchar(250) usualmente
            r.input('auditUser', sql.Char(6), auditUser);

            const result = await r.query(`
                UPDATE saArticulo 
                SET campo7 = @imageUrl, 
                    fe_us_mo = GETDATE(),
                    co_us_mo = @auditUser
                WHERE LTRIM(RTRIM(co_art)) = LTRIM(RTRIM(@co_art));

                SELECT RTRIM(campo7) AS saved_campo7 FROM saArticulo WHERE LTRIM(RTRIM(co_art)) = LTRIM(RTRIM(@co_art));
            `);

            if (result.rowsAffected[0] === 0) {
                throw new Error('El artículo no existe en esta sede.');
            }

            const savedValue = result.recordset ? result.recordset[0].saved_campo7 : null;
            console.log(`[PUT /:co_art/imagen] Artículo ${co_art} actualizado. Valor guardado en DB: "${savedValue}"`);

            return { co_art, success: true, saved_campo7: savedValue };
        });

        return writeResponse(res, outcome, `Sede "${req.query.sede}" no encontrada o error en la operación.`);
    } catch (error) {
        console.error(`[PUT /:co_art/imagen] Error:`, error.message);
        res.status(500).json({ success: false, message: 'Error al actualizar la imagen del artículo.', error: error.message });
    }
});

// ────────────────────────────────────────────────────────────────────────────
// POST /api/v1/articulos/sync — Sincronización multisede de artículos
// ────────────────────────────────────────────────────────────────────────────
router.post('/sync', async (req, res) => {
    try {
        const servers = getServers();
        if (!servers || servers.length < 2) {
            return res.status(200).json({
                success: true,
                message: 'Se requiere al menos 2 sedes activas para sincronizar.',
                total_synced: 0,
                summary: []
            });
        }

        // 1. Obtener todos los artículos, unidades y catálogos de cada servidor
        const serverArticles = {};
        const allUniqueArticles = new Map();

        for (const srv of servers) {
            try {
                const pool = await getPool(srv.id, req.sqlAuth);
                const [artRes, artUniRes, linRes, sublRes, catRes, colRes, ubiRes] = await Promise.all([
                    pool.request().query(
                        `SELECT RTRIM(a.co_art) AS co_art, RTRIM(a.art_des) AS art_des, RTRIM(a.tipo) AS tipo,
                                RTRIM(a.co_lin) AS co_lin, RTRIM(a.co_subl) AS co_subl, RTRIM(a.co_cat) AS co_cat,
                                RTRIM(a.co_color) AS co_color, RTRIM(a.co_ubicacion) AS co_ubicacion,
                                RTRIM(a.item) AS item, RTRIM(a.modelo) AS modelo, RTRIM(a.ref) AS ref,
                                a.anulado, a.tipo_imp, a.peso, a.volumen, a.stock_min, a.stock_max,
                                RTRIM(a.campo1) AS campo1, RTRIM(a.campo7) AS campo7
                         FROM saArticulo a`
                    ),
                    pool.request().query(
                        `SELECT RTRIM(au.co_art) AS co_art, RTRIM(au.co_uni) AS co_uni, au.relacion, au.equivalencia,
                                au.uni_principal, au.uso_venta, au.uso_compra, au.uni_secundaria, au.uso_secundaria,
                                RTRIM(u.des_uni) AS des_uni
                         FROM saArtUnidad au
                         LEFT JOIN saUnidad u ON au.co_uni = u.co_uni`
                    ),
                    pool.request().query('SELECT RTRIM(co_lin) AS co_lin, RTRIM(lin_des) AS lin_des FROM saLineaArticulo'),
                    pool.request().query('SELECT RTRIM(co_lin) AS co_lin, RTRIM(co_subl) AS co_subl, RTRIM(subl_des) AS subl_des FROM saSubLinea'),
                    pool.request().query('SELECT RTRIM(co_cat) AS co_cat, RTRIM(cat_des) AS cat_des FROM saCatArticulo'),
                    pool.request().query('SELECT RTRIM(co_color) AS co_color, RTRIM(des_color) AS des_color FROM saColor'),
                    pool.request().query('SELECT RTRIM(co_ubicacion) AS co_ubicacion, RTRIM(des_ubicacion) AS des_ubicacion FROM saUbicacion')
                ]);

                const unitsByArt = new Map();
                for (const u of artUniRes.recordset) {
                    const key = (u.co_art || '').trim().toUpperCase();
                    if (!unitsByArt.has(key)) unitsByArt.set(key, []);
                    unitsByArt.get(key).push(u);
                }

                const linMap = new Map(linRes.recordset.map(l => [(l.co_lin || '').trim().toUpperCase(), l]));
                const sublMap = new Map(sublRes.recordset.map(s => [`${(s.co_lin || '').trim().toUpperCase()}__${(s.co_subl || '').trim().toUpperCase()}`, s]));
                const catMap = new Map(catRes.recordset.map(c => [(c.co_cat || '').trim().toUpperCase(), c]));
                const colMap = new Map(colRes.recordset.map(c => [(c.co_color || '').trim().toUpperCase(), c]));
                const ubiMap = new Map(ubiRes.recordset.map(u => [(u.co_ubicacion || '').trim().toUpperCase(), u]));

                const artMap = new Map();
                for (const row of artRes.recordset) {
                    const key = (row.co_art || '').trim().toUpperCase();
                    if (key) {
                        const co_lin_key = (row.co_lin || '').trim().toUpperCase();
                        const co_subl_key = `${co_lin_key}__${(row.co_subl || '').trim().toUpperCase()}`;
                        const co_cat_key = (row.co_cat || '').trim().toUpperCase();
                        const co_col_key = (row.co_color || '').trim().toUpperCase();
                        const co_ubi_key = (row.co_ubicacion || '').trim().toUpperCase();

                        const fullItem = {
                            ...row,
                            unidades: unitsByArt.get(key) || [],
                            _linea: linMap.get(co_lin_key) || null,
                            _sublinea: sublMap.get(co_subl_key) || null,
                            _categoria: catMap.get(co_cat_key) || null,
                            _color: colMap.get(co_col_key) || null,
                            _ubicacion: ubiMap.get(co_ubi_key) || null
                        };

                        artMap.set(key, fullItem);
                        if (!allUniqueArticles.has(key)) {
                            allUniqueArticles.set(key, fullItem);
                        } else if (fullItem.unidades.length > 0 && (allUniqueArticles.get(key).unidades || []).length === 0) {
                            allUniqueArticles.set(key, fullItem);
                        }
                    }
                }
                serverArticles[srv.id] = { server: srv, map: artMap, pool };
            } catch (err) {
                console.warn(`[SYNC ARTICULOS] Error leyendo artículos de sede ${srv.name}:`, err.message);
            }
        }

        // 2. Para cada servidor, detectar cuáles artículos faltan y migrarlos
        const summary = [];
        let totalSynced = 0;
        const auditUser = (req.profitUser || req.sqlAuth?.user || '01').substring(0, 10).toUpperCase();

        for (const srv of servers) {
            const srvData = serverArticles[srv.id];
            if (!srvData) {
                summary.push({
                    sede_id: srv.id,
                    sede_nombre: srv.name,
                    migrated: 0,
                    errors: ['No se pudo conectar a la base de datos de esta sede.']
                });
                continue;
            }

            const { pool, map } = srvData;
            const defaultAlmacen = (srv.profit_branch_codes || []).find(b => b.is_default)?.code || (srv.profit_branch_codes || [])[0]?.code || '01';

            // Cargar defaults del servidor destino
            const [resLin, resCat, resCol, resUbi, resUni] = await Promise.all([
                pool.request().query("SELECT TOP 1 RTRIM(co_lin) AS co_lin FROM saLineaArticulo ORDER BY CASE WHEN RTRIM(co_lin) = '01' THEN 0 ELSE 1 END, co_lin"),
                pool.request().query("SELECT TOP 1 RTRIM(co_cat) AS co_cat FROM saCatArticulo ORDER BY CASE WHEN RTRIM(co_cat) = '01' THEN 0 ELSE 1 END, co_cat"),
                pool.request().query("SELECT TOP 1 RTRIM(co_color) AS co_color FROM saColor ORDER BY CASE WHEN RTRIM(co_color) = '01' THEN 0 ELSE 1 END, co_color"),
                pool.request().query("SELECT TOP 1 RTRIM(co_ubicacion) AS co_ubicacion FROM saUbicacion ORDER BY CASE WHEN RTRIM(co_ubicacion) = '01' THEN 0 ELSE 1 END, co_ubicacion"),
                pool.request().query("SELECT TOP 1 RTRIM(co_uni) AS co_uni FROM saUnidad ORDER BY CASE WHEN RTRIM(co_uni) = '01' THEN 0 WHEN RTRIM(co_uni) = 'UND' THEN 1 ELSE 2 END, co_uni")
            ]);

            const defaultLin = resLin.recordset[0]?.co_lin || '01';
            const defaultCat = resCat.recordset[0]?.co_cat || '01';
            const defaultCol = resCol.recordset[0]?.co_color || '01';
            const defaultUbi = resUbi.recordset[0]?.co_ubicacion || '01';
            const defaultUni = resUni.recordset[0]?.co_uni || '01';

            let migratedCount = 0;
            const errors = [];

            for (const [co_art, article] of allUniqueArticles.entries()) {
                try {
                    // Asegurar dependencias de catálogos
                    await ensureCatalogDependencies(pool, article, auditUser, defaultAlmacen);

                    if (map.has(co_art)) {
                        // El artículo ya existe, asegurar que sus unidades estén en saArtUnidad
                        await ensureArticleUnits(pool, co_art, article.unidades, defaultUni, auditUser, defaultAlmacen);
                        continue;
                    }

                    const dataToInsert = { ...article };

                    // 1. Validar co_lin en destino
                    const linCheck = await pool.request().input('lin', sql.VarChar, dataToInsert.co_lin || '').query(
                        'SELECT TOP 1 co_lin FROM saLineaArticulo WHERE LTRIM(RTRIM(co_lin)) = LTRIM(RTRIM(@lin))'
                    );
                    dataToInsert.co_lin = linCheck.recordset.length ? dataToInsert.co_lin : defaultLin;

                    // 2. Validar co_subl correspondiente a la línea en destino
                    const sublCheck = await pool.request()
                        .input('lin', sql.VarChar, dataToInsert.co_lin)
                        .input('subl', sql.VarChar, dataToInsert.co_subl || '')
                        .query('SELECT TOP 1 co_subl FROM saSubLinea WHERE LTRIM(RTRIM(co_lin)) = LTRIM(RTRIM(@lin)) AND LTRIM(RTRIM(co_subl)) = LTRIM(RTRIM(@subl))');
                    
                    if (sublCheck.recordset.length) {
                        dataToInsert.co_subl = sublCheck.recordset[0].co_subl;
                    } else {
                        const firstSubl = await pool.request()
                            .input('lin', sql.VarChar, dataToInsert.co_lin)
                            .query('SELECT TOP 1 co_subl FROM saSubLinea WHERE LTRIM(RTRIM(co_lin)) = LTRIM(RTRIM(@lin))');
                        dataToInsert.co_subl = firstSubl.recordset[0]?.co_subl || '01';
                    }

                    // 3. Validar co_cat en destino
                    const catCheck = await pool.request().input('cat', sql.VarChar, dataToInsert.co_cat || '').query(
                        'SELECT TOP 1 co_cat FROM saCatArticulo WHERE LTRIM(RTRIM(co_cat)) = LTRIM(RTRIM(@cat))'
                    );
                    dataToInsert.co_cat = catCheck.recordset.length ? dataToInsert.co_cat : defaultCat;

                    // 4. Validar co_color en destino
                    const colCheck = await pool.request().input('col', sql.VarChar, dataToInsert.co_color || '').query(
                        'SELECT TOP 1 co_color FROM saColor WHERE LTRIM(RTRIM(co_color)) = LTRIM(RTRIM(@col))'
                    );
                    dataToInsert.co_color = colCheck.recordset.length ? dataToInsert.co_color : defaultCol;

                    // 5. Validar co_ubicacion en destino
                    const ubiCheck = await pool.request().input('ubi', sql.VarChar, dataToInsert.co_ubicacion || '').query(
                        'SELECT TOP 1 co_ubicacion FROM saUbicacion WHERE LTRIM(RTRIM(co_ubicacion)) = LTRIM(RTRIM(@ubi))'
                    );
                    dataToInsert.co_ubicacion = ubiCheck.recordset.length ? dataToInsert.co_ubicacion : defaultUbi;

                    const f = new Date();
                    const r = new sql.Request(pool);
                    r.input('sCo_Art', sql.Char(30), dataToInsert.co_art);
                    r.input('sdFecha_Reg', sql.SmallDateTime, f);
                    r.input('sArt_Des', sql.VarChar(120), dataToInsert.art_des || 'NUEVO ARTÍCULO');
                    r.input('sTipo', sql.Char(1), dataToInsert.tipo || 'V');
                    r.input('bAnulado', sql.Bit, dataToInsert.anulado ? 1 : 0);
                    r.input('sdFecha_Inac', sql.SmallDateTime, f);
                    r.input('sCo_Lin', sql.Char(6), dataToInsert.co_lin);
                    r.input('sCo_Subl', sql.Char(6), dataToInsert.co_subl);
                    r.input('sCo_Cat', sql.Char(6), dataToInsert.co_cat);
                    r.input('sCo_Color', sql.Char(6), dataToInsert.co_color);
                    r.input('sCo_Ubicacion', sql.Char(6), dataToInsert.co_ubicacion);
                    r.input('sItem', sql.VarChar(10), dataToInsert.item || null);
                    r.input('sModelo', sql.VarChar(20), dataToInsert.modelo || '');
                    r.input('sRef', sql.VarChar(20), dataToInsert.ref || null);
                    r.input('bGenerico', sql.Bit, 0);
                    r.input('bManeja_Serial', sql.Bit, 0);
                    r.input('bManeja_Lote', sql.Bit, 0);
                    r.input('bManeja_Lote_Venc', sql.Bit, 0);
                    r.input('deMargen_Min', sql.Decimal(18, 5), 0);
                    r.input('deMargen_Max', sql.Decimal(18, 5), 0);
                    r.input('sTipo_Imp', sql.Char(1), dataToInsert.tipo_imp || '1');
                    r.input('sTipo_Imp2', sql.Char(1), null);
                    r.input('sTipo_Imp3', sql.Char(1), null);
                    r.input('sCo_Reten', sql.Char(6), null);
                    r.input('sCod_Proc', sql.Char(6), null);
                    r.input('sGarantia', sql.VarChar(30), '');
                    r.input('deVolumen', sql.Decimal(18, 5), Number(dataToInsert.volumen) || 0);
                    r.input('dePeso', sql.Decimal(18, 5), Number(dataToInsert.peso) || 0);
                    r.input('deStock_Min', sql.Decimal(18, 5), Number(dataToInsert.stock_min) || 0);
                    r.input('deStock_Max', sql.Decimal(18, 5), Number(dataToInsert.stock_max) || 0);
                    r.input('deStock_Pedido', sql.Decimal(18, 5), 0);
                    r.input('iRelac_Unidad', sql.Int, 1);
                    r.input('dePunt_Ven', sql.Decimal(18, 5), 0);
                    r.input('dePunt_Cli', sql.Decimal(18, 5), 0);
                    r.input('deLic_Mon_Ilc', sql.Decimal(18, 5), 0);
                    r.input('deLic_Capacidad', sql.Decimal(18, 5), 0);
                    r.input('deLic_Grado_Al', sql.Decimal(18, 5), 0);
                    r.input('sLic_Tipo', sql.Char(1), null);
                    r.input('bPrec_Om', sql.Bit, 0);
                    r.input('sComentario', sql.VarChar(sql.MAX), 'Migrado vía Sincronización');
                    r.input('sTipo_Cos', sql.Char(4), '1');
                    r.input('dePorc_Margen_Minimo', sql.Decimal(18, 5), 0);
                    r.input('dePorc_Margen_Maximo', sql.Decimal(18, 5), 0);
                    r.input('deMont_Comi', sql.Decimal(18, 5), 0);
                    r.input('dePorc_Arancel', sql.Decimal(18, 5), 0);
                    r.input('sI_Art_Des', sql.VarChar(120), null);
                    r.input('sDis_Cen', sql.VarChar(sql.MAX), null);
                    r.input('sReten_Iva_Tercero', sql.Char(16), null);
                    r.input('sCampo1', sql.VarChar(60), dataToInsert.campo1 || null);
                    r.input('sCampo2', sql.VarChar(60), null);
                    r.input('sCampo3', sql.VarChar(60), null);
                    r.input('sCampo4', sql.VarChar(60), null);
                    r.input('sCampo5', sql.VarChar(60), null);
                    r.input('sCampo6', sql.VarChar(60), null);
                    r.input('sCampo7', sql.VarChar(60), dataToInsert.campo7 || null);
                    r.input('sCampo8', sql.VarChar(60), null);
                    r.input('sCo_Us_In', sql.Char(6), auditUser);
                    r.input('sCo_Sucu_In', sql.Char(6), defaultAlmacen);
                    r.input('sMaquina', sql.VarChar(60), 'SYNC2K');
                    r.input('sRevisado', sql.Char(1), null);
                    r.input('sTrasnfe', sql.Char(1), null);

                    await r.execute('pInsertarArticulo');

                    // Asegurar todas las unidades en saArtUnidad
                    await ensureArticleUnits(pool, dataToInsert.co_art, article.unidades, defaultUni, auditUser, defaultAlmacen);

                    // Preservar imagen si la tenía
                    if (dataToInsert.campo7) {
                        await pool.request()
                            .input('art', sql.Char(30), dataToInsert.co_art)
                            .input('img', sql.VarChar(250), dataToInsert.campo7)
                            .query('UPDATE saArticulo SET campo7 = @img WHERE LTRIM(RTRIM(co_art)) = LTRIM(RTRIM(@art))');
                    }

                    // Si el artículo original estaba anulado, asegurar su estado anulado en destino
                    if (dataToInsert.anulado) {
                        await pool.request()
                            .input('art', sql.Char(30), dataToInsert.co_art)
                            .query('UPDATE saArticulo SET anulado = 1, fecha_inac = GETDATE() WHERE LTRIM(RTRIM(co_art)) = LTRIM(RTRIM(@art))');
                    }

                    migratedCount++;
                    totalSynced++;
                    map.set(co_art, dataToInsert);
                } catch (err) {
                    errors.push(`Artículo ${co_art} (${article.art_des}): ${err.message}`);
                }
            }

            summary.push({
                sede_id: srv.id,
                sede_nombre: srv.name,
                migrated: migratedCount,
                errors
            });
        }

        return res.status(200).json({
            success: true,
            total_synced: totalSynced,
            summary,
            message: totalSynced > 0
                ? `Sincronización de artículos completada. Se migraron ${totalSynced} artículos con sus unidades y catálogos asociados.`
                : 'Todas las sucursales ya tienen los artículos y dependencias sincronizadas.'
        });
    } catch (error) {
        console.error('[SYNC ARTICULOS FATAL ERROR]:', error);
        res.status(500).json({
            success: false,
            message: 'Error general al sincronizar artículos.',
            error: error.message || String(error)
        });
    }
});

module.exports = router;
