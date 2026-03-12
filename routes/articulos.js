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

    const stockMap  = {};
    resStock.recordset.forEach(s => { (stockMap[s.co_art]  = stockMap[s.co_art]  || []).push({ co_alma: s.co_alma, des_alma: s.des_alma, stock: s.stock }); });
    const precioMap = {};
    resPrecios.recordset.forEach(p => { (precioMap[p.co_art] = precioMap[p.co_art] || []).push({
        id_precio: p.id_precio, precio: p.precio, moneda: p.moneda,
        precio_ves: (p.moneda.includes('US') ? Number((p.precio * tasa).toFixed(2)) : p.precio)
    }); });

    return articulos.map(a => ({
        ...a,
        tasa_bcv:       tasa,
        disponibilidad: stockMap[a.co_art]  || [],
        precios:        precioMap[a.co_art] || []
    }));
}

// ────────────────────────────────────────────────────────────────────────────
// 1. GET /api/v1/articulos — Listado paginado de todas las sedes
// ────────────────────────────────────────────────────────────────────────────
router.get('/', async (req, res) => {
    try {
        const page  = parseInt(req.query.page)  || 1;
        const limit = parseInt(req.query.limit) || 10;
        const servers = getServers();

        const allData = await Promise.all(servers.map(async (srv) => {
            try {
                const pool = await getPool(srv.id);
                const [resData, resTasa] = await Promise.all([
                    pool.request().query(
                        `SELECT RTRIM(co_art) AS co_art, RTRIM(art_des) AS descripcion,
                                RTRIM(tipo) AS tipo, RTRIM(modelo) AS modelo, RTRIM(ref) AS referencia
                         FROM saArticulo WHERE anulado = 0 ORDER BY art_des`
                    ),
                    pool.request().query(QUERY_TASA)
                ]);
                const tasa = resTasa.recordset[0]?.tasa_cambio || 1;
                let articulos = resData.recordset.map(a => ({ ...a, sede_id: srv.id, sede_nombre: srv.name }));
                articulos = await enrichArticulos(pool, articulos, tasa);
                return articulos;
            } catch (e) { return []; }
        }));

        const combined = [].concat(...allData);
        combined.sort((a, b) => a.descripcion.localeCompare(b.descripcion));
        return paginatedResponse(res, combined, page, limit);

    } catch (error) {
        res.status(500).json({ success: false, message: 'Error al consultar artículos.', error: error.message });
    }
});

// ────────────────────────────────────────────────────────────────────────────
// 2. GET /api/v1/articulos/search — Búsqueda con filtros
// ────────────────────────────────────────────────────────────────────────────
router.get('/search', async (req, res) => {
    try {
        const page  = parseInt(req.query.page)  || 1;
        const limit = parseInt(req.query.limit) || 30;

        const FIELD_MAP = {
            co_art: 'co_art', descripcion: 'art_des', modelo: 'modelo',
            referencia: 'ref',  tipo: 'tipo', linea: 'co_lin',
            sublinea: 'co_subl', categoria: 'co_cat', proveedor: 'co_prov'
        };

        const filters = Object.entries(req.query)
            .filter(([k, v]) => FIELD_MAP[k] && v)
            .map(([k, v]) => ({ param: k, column: FIELD_MAP[k], value: v }));

        if (!filters.length) {
            return res.status(400).json({ success: false, message: 'Especifique al menos un parámetro de búsqueda.' });
        }

        const whereClause = 'WHERE anulado = 0 ' + filters.map(f => `AND ${f.column} LIKE '%' + @${f.param} + '%'`).join(' ');
        const servers = getServers();

        const allData = await Promise.all(servers.map(async (srv) => {
            try {
                const pool = await getPool(srv.id);

                // Función factoría de request con filtros inyectados
                const buildReq = () => {
                    const r = pool.request();
                    filters.forEach(f => r.input(f.param, sql.VarChar, f.value));
                    return r;
                };

                const [resData, resTasa] = await Promise.all([
                    buildReq().query(
                        `SELECT RTRIM(co_art) AS co_art, RTRIM(art_des) AS descripcion,
                                RTRIM(tipo) AS tipo, RTRIM(modelo) AS modelo, RTRIM(ref) AS referencia
                         FROM saArticulo ${whereClause} ORDER BY art_des`
                    ),
                    pool.request().query(QUERY_TASA)
                ]);

                const tasa = resTasa.recordset[0]?.tasa_cambio || 1;
                let articulos = resData.recordset.map(a => ({ ...a, sede_id: srv.id, sede_nombre: srv.name }));
                articulos = await enrichArticulos(pool, articulos, tasa);
                return articulos;
            } catch (e) { return []; }
        }));

        const combined = [].concat(...allData);
        combined.sort((a, b) => a.descripcion.localeCompare(b.descripcion));
        return paginatedResponse(res, combined, page, limit);

    } catch (error) {
        res.status(500).json({ success: false, message: 'Error en búsqueda de artículos.', error: error.message });
    }
});

// ────────────────────────────────────────────────────────────────────────────
// 3. GET /api/v1/articulos/:co_art — Detalle completo por sede
// ────────────────────────────────────────────────────────────────────────────
router.get('/:co_art', async (req, res) => {
    try {
        const { co_art } = req.params;
        const servers = getServers();

        const results = await Promise.all(servers.map(async (srv) => {
            try {
                const pool = await getPool(srv.id);

                const [resArt, resStock, resPre, resTasa] = await Promise.all([
                    pool.request().input('co_art', sql.VarChar, co_art).query(
                        `SELECT RTRIM(co_art) AS co_art, RTRIM(art_des) AS descripcion,
                                anulado, RTRIM(tipo) AS tipo_articulo
                         FROM saArticulo WHERE LTRIM(RTRIM(co_art)) = LTRIM(RTRIM(@co_art))`
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
                        precio_ves: p.moneda.includes('US') ? Number((p.precio * tasa).toFixed(2)) : p.precio
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
router.post('/', async (req, res) => {
    try {
        const data = req.body;
        if (!data.co_art || !data.art_des)
            return res.status(400).json({ success: false, message: 'Campos obligatorios: co_art, art_des' });

        const outcome = await executeWrite(req.query.sede || null, async (pool) => {
            const f = new Date();
            const [resLin, resSubl, resCat, resCol, resUbic] = await Promise.all([
                pool.request().query('SELECT TOP 1 RTRIM(co_lin) AS id FROM saLineaArticulo'),
                pool.request().query('SELECT TOP 1 RTRIM(co_subl) AS id FROM saSubLinea'),
                pool.request().query('SELECT TOP 1 RTRIM(co_cat) AS id FROM saCatArticulo'),
                pool.request().query('SELECT TOP 1 RTRIM(co_color) AS id FROM saColor'),
                pool.request().query('SELECT TOP 1 RTRIM(co_ubicacion) AS id FROM saUbicacion')
            ]);

            const r = new sql.Request(pool);
            r.input('sCo_Art',         sql.Char(30),         data.co_art);
            r.input('sdFecha_Reg',      sql.SmallDateTime,    f);
            r.input('sArt_Des',         sql.VarChar(120),     data.art_des);
            r.input('sTipo',            sql.Char(1),          data.tipo || 'V');
            r.input('bAnulado',         sql.Bit,              0);
            r.input('sdFecha_Inac',     sql.SmallDateTime,    f);
            r.input('sCo_Lin',          sql.Char(6),          data.co_lin    || resLin.recordset[0]?.id  || null);
            r.input('sCo_Subl',         sql.Char(6),          data.co_subl   || resSubl.recordset[0]?.id || null);
            r.input('sCo_Cat',          sql.Char(6),          data.co_cat    || resCat.recordset[0]?.id  || null);
            r.input('sCo_Color',        sql.Char(6),          data.co_color  || resCol.recordset[0]?.id  || null);
            r.input('sCo_Ubicacion',    sql.Char(6),          data.co_ubicacion || resUbic.recordset[0]?.id || 'CONT1A');
            r.input('sItem',            sql.VarChar(10),      data.item    || null);
            r.input('sModelo',          sql.VarChar(20),      data.modelo  || '');
            r.input('sRef',             sql.VarChar(20),      data.ref     || '');
            r.input('bGenerico',        sql.Bit,              0);
            r.input('bManeja_Serial',   sql.Bit,              0);
            r.input('bManeja_Lote',     sql.Bit,              0);
            r.input('bManeja_Lote_Venc',sql.Bit,              0);
            r.input('deMargen_Min',     sql.Decimal(18, 5),   0);
            r.input('deMargen_Max',     sql.Decimal(18, 5),   0);
            r.input('sTipo_Imp',        sql.Char(1),          data.tipo_imp || '1');
            r.input('sTipo_Imp2',       sql.Char(1),          '7');
            r.input('sTipo_Imp3',       sql.Char(1),          '7');
            r.input('sCo_Reten',        sql.Char(6),          null);
            r.input('sCod_Proc',        sql.Char(6),          null);
            r.input('sGarantia',        sql.VarChar(30),      '');
            r.input('deVolumen',        sql.Decimal(18, 5),   0);
            r.input('dePeso',           sql.Decimal(18, 5),   0);
            r.input('deStock_Min',      sql.Decimal(18, 5),   0);
            r.input('deStock_Max',      sql.Decimal(18, 5),   0);
            r.input('deStock_Pedido',   sql.Decimal(18, 5),   0);
            r.input('iRelac_Unidad',    sql.Int,              1);
            r.input('dePunt_Ven',       sql.Decimal(18, 5),   0);
            r.input('dePunt_Cli',       sql.Decimal(18, 5),   0);
            r.input('deLic_Mon_Ilc',    sql.Decimal(18, 5),   0);
            r.input('deLic_Capacidad',  sql.Decimal(18, 5),   0);
            r.input('deLic_Grado_Al',   sql.Decimal(18, 5),   0);
            r.input('sLic_Tipo',        sql.Char(1),          null);
            r.input('bPrec_Om',         sql.Bit,              0);
            r.input('sComentario',      sql.VarChar(sql.MAX), null);
            r.input('sTipo_Cos',        sql.Char(4),          '1');
            r.input('dePorc_Margen_Minimo', sql.Decimal(18,5),0);
            r.input('dePorc_Margen_Maximo', sql.Decimal(18,5),0);
            r.input('deMont_Comi',      sql.Decimal(18, 5),   0);
            r.input('dePorc_Arancel',   sql.Decimal(18, 5),   0);
            r.input('sI_Art_Des',       sql.VarChar(120),     null);
            r.input('sDis_Cen',         sql.VarChar(sql.MAX), null);
            r.input('sReten_Iva_Tercero',sql.Char(16),        null);
            r.input('sCampo1',          sql.VarChar(60),      '');
            r.input('sCampo2',          sql.VarChar(60),      '');
            r.input('sCampo3',          sql.VarChar(60),      '');
            r.input('sCampo4',          sql.VarChar(60),      '');
            r.input('sCampo5',          sql.VarChar(60),      '');
            r.input('sCampo6',          sql.VarChar(60),      '');
            r.input('sCampo7',          sql.VarChar(60),      '');
            r.input('sCampo8',          sql.VarChar(60),      '');
            r.input('sCo_Us_In',        sql.Char(6),          '999');
            r.input('sCo_Sucu_In',      sql.Char(6),          null);
            r.input('sMaquina',         sql.VarChar(60),      'SYNC2K');
            r.input('sRevisado',        sql.Char(1),          '0');
            r.input('sTrasnfe',         sql.Char(1),          '0');
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
router.put('/:co_art', async (req, res) => {
    try {
        const coArtOri = req.params.co_art;
        const data = req.body;

        const outcome = await executeWrite(req.query.sede || null, async (pool) => {
            const check = await pool.request().input('co_art', sql.VarChar, coArtOri).query(
                `SELECT validador, RTRIM(co_lin) AS co_lin, RTRIM(co_subl) AS co_subl,
                        RTRIM(co_cat) AS co_cat, RTRIM(co_color) AS co_color,
                        RTRIM(co_ubicacion) AS co_ubicacion, tipo_imp, tipo_cos
                 FROM saArticulo WHERE LTRIM(RTRIM(co_art)) = LTRIM(RTRIM(@co_art))`
            );
            if (!check.recordset.length) throw new Error('El artículo no existe en esta sede.');

            const row = check.recordset[0];
            const f   = new Date();
            const r   = new sql.Request(pool);
            r.input('sCo_Art',          sql.Char(30),         data.co_art || coArtOri);
            r.input('sCo_ArtOri',       sql.Char(30),         coArtOri);
            r.input('sdFecha_Reg',      sql.SmallDateTime,    f);
            r.input('sArt_Des',         sql.VarChar(120),     data.art_des || 'Artículo Modificado API');
            r.input('sTipo',            sql.Char(1),          data.tipo    || 'V');
            r.input('bAnulado',         sql.Bit,              0);
            r.input('sdFecha_Inac',     sql.SmallDateTime,    f);
            r.input('sCo_Lin',          sql.Char(6),          data.co_lin  || row.co_lin);
            r.input('sCo_Subl',         sql.Char(6),          data.co_subl || row.co_subl);
            r.input('sCo_Cat',          sql.Char(6),          data.co_cat  || row.co_cat);
            r.input('sCo_Color',        sql.Char(6),          data.co_color || row.co_color);
            r.input('sCo_Ubicacion',    sql.Char(6),          data.co_ubicacion || row.co_ubicacion);
            r.input('sItem',            sql.VarChar(10),      data.item   || null);
            r.input('sModelo',          sql.VarChar(20),      data.modelo || '');
            r.input('sRef',             sql.VarChar(20),      data.ref    || '');
            r.input('bGenerico',        sql.Bit,              0);
            r.input('bManeja_Serial',   sql.Bit,              0);
            r.input('bManeja_Lote',     sql.Bit,              0);
            r.input('bManeja_Lote_Venc',sql.Bit,              0);
            r.input('deMargen_Min',     sql.Decimal(18, 5),   0);
            r.input('deMargen_Max',     sql.Decimal(18, 5),   0);
            r.input('sTipo_Imp',        sql.Char(1),          data.tipo_imp || row.tipo_imp || '1');
            r.input('sTipo_Imp2',       sql.Char(1),          '7');
            r.input('sTipo_Imp3',       sql.Char(1),          '7');
            r.input('sCo_Reten',        sql.Char(6),          null);
            r.input('sCod_Proc',        sql.Char(6),          null);
            r.input('sGarantia',        sql.VarChar(30),      '');
            r.input('deVolumen',        sql.Decimal(18, 5),   0);
            r.input('dePeso',           sql.Decimal(18, 5),   0);
            r.input('deStock_Min',      sql.Decimal(18, 5),   0);
            r.input('deStock_Max',      sql.Decimal(18, 5),   0);
            r.input('deStock_Pedido',   sql.Decimal(18, 5),   0);
            r.input('iRelac_Unidad',    sql.Int,              1);
            r.input('dePunt_Ven',       sql.Decimal(18, 5),   0);
            r.input('dePunt_Cli',       sql.Decimal(18, 5),   0);
            r.input('deLic_Mon_Ilc',    sql.Decimal(18, 5),   0);
            r.input('deLic_Capacidad',  sql.Decimal(18, 5),   0);
            r.input('deLic_Grado_Al',   sql.Decimal(18, 5),   0);
            r.input('sLic_Tipo',        sql.Char(1),          null);
            r.input('bPrec_Om',         sql.Bit,              0);
            r.input('sComentario',      sql.VarChar(sql.MAX), null);
            r.input('sTipo_Cos',        sql.Char(4),          data.tipo_cos || row.tipo_cos || '1');
            r.input('dePorc_Margen_Minimo', sql.Decimal(18,5),0);
            r.input('dePorc_Margen_Maximo', sql.Decimal(18,5),0);
            r.input('deMont_Comi',      sql.Decimal(18, 5),   0);
            r.input('dePorc_Arancel',   sql.Decimal(18, 5),   0);
            r.input('sDis_Cen',         sql.VarChar(sql.MAX), null);
            r.input('sReten_Iva_Tercero',sql.Char(16),        null);
            r.input('sCampo1',          sql.VarChar(60),      '');
            r.input('sCampo2',          sql.VarChar(60),      '');
            r.input('sCampo3',          sql.VarChar(60),      '');
            r.input('sCampo4',          sql.VarChar(60),      '');
            r.input('sCampo5',          sql.VarChar(60),      '');
            r.input('sCampo6',          sql.VarChar(60),      '');
            r.input('sCampo7',          sql.VarChar(60),      '');
            r.input('sCampo8',          sql.VarChar(60),      '');
            r.input('sCo_Us_Mo',        sql.Char(6),          '999');
            r.input('sCo_Sucu_Mo',      sql.Char(6),          null);
            r.input('sMaquina',         sql.VarChar(60),      'SYNC2K');
            r.input('sCampos',          sql.VarChar(sql.MAX), '');
            r.input('sRevisado',        sql.Char(1),          '0');
            r.input('sTrasnfe',         sql.Char(1),          '0');
            r.input('tsValidador',      sql.VarBinary,        row.validador);
            r.input('gRowguid',         sql.UniqueIdentifier, null);
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
router.delete('/:co_art', async (req, res) => {
    try {
        const { co_art } = req.params;

        const outcome = await executeWrite(req.query.sede || null, async (pool) => {
            const check = await pool.request().input('co_art', sql.VarChar, co_art).query(
                `SELECT validador FROM saArticulo WHERE LTRIM(RTRIM(co_art)) = LTRIM(RTRIM(@co_art))`
            );
            if (!check.recordset.length) throw new Error('El artículo no existe en esta sede.');

            const r = new sql.Request(pool);
            r.input('sCo_ArtOri',  sql.Char(30),         co_art);
            r.input('tsValidador', sql.VarBinary,         check.recordset[0].validador);
            r.input('sMaquina',    sql.VarChar(60),       'SYNC2K');
            r.input('sCo_Us_Mo',   sql.Char(6),           '999');
            r.input('sCo_Sucu_Mo', sql.Char(6),           null);
            r.input('gRowguid',    sql.UniqueIdentifier,  null);
            await r.execute('pEliminarArticulo');
        });

        return writeResponse(res, outcome, `Sede "${req.query.sede}" no encontrada.`);
    } catch (error) {
        res.status(500).json({ success: false, message: 'Error interno.', error: error.message });
    }
});

module.exports = router;
