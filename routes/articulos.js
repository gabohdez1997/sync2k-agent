const express = require('express');
const router = express.Router();
const { sql, getPool, getServers } = require('../db');

/**
 * 1. Endpoint: Consultar lista de artículos
 * GET /api/v1/articulos
 * 
 * Ejemplo de uso:
 * GET /api/v1/articulos?page=1&limit=10
 * Headers: { "x-api-key": "mi-clave-secreta" }
 */
router.get('/', async (req, res) => {
  try {
    const page = parseInt(req.query.page) || 1;
    const limit = parseInt(req.query.limit) || 10;
    const offset = (page - 1) * limit;

    const pool = await getPool();

    // 1. Query Total
    const queryCount = `SELECT COUNT(*) AS total FROM saArticulo WHERE anulado = 0`;

    // 2. Query Data con Paginación
    const queryArticulos = `
          SELECT 
            RTRIM(co_art) AS co_art, 
            RTRIM(art_des) AS descripcion,
            RTRIM(tipo) AS tipo,
            RTRIM(modelo) AS modelo,
            RTRIM(ref) AS referencia
          FROM saArticulo
          WHERE anulado = 0
          ORDER BY art_des
          OFFSET @offset ROWS 
          FETCH NEXT @limit ROWS ONLY
        `;

    // 3. Tasa de cambio oficial actual
    const queryTasa = `
      SELECT TOP 1 tasa_v AS tasa_cambio
      FROM saTasa
      WHERE LTRIM(RTRIM(co_mone)) IN ('US$', 'USD')
      ORDER BY fecha DESC
    `;

    const requestData = pool.request();
    requestData.input('offset', sql.Int, offset);
    requestData.input('limit', sql.Int, limit);

    const [resCount, resData, resTasa] = await Promise.all([
      pool.request().query(queryCount),
      requestData.query(queryArticulos),
      pool.request().query(queryTasa)
    ]);

    const totalItems = resCount.recordset[0].total;
    const tasaActual = resTasa.recordset.length > 0 ? resTasa.recordset[0].tasa_cambio : 1;
    let articulos = resData.recordset;

    if (articulos.length > 0) {
      // Obtener todos los IDs para lanzar búsquedas en lote de almacenes y precios
      const coArtsList = articulos.map(a => `'${a.co_art.replace(/'/g, "''")}'`).join(',');

      const queryStockBatch = `
        SELECT 
          RTRIM(s.co_art) AS co_art, 
          RTRIM(s.co_alma) AS co_alma, 
          RTRIM(a.des_alma) AS des_alma,
          SUM(CASE WHEN RTRIM(s.tipo) = 'ACT' THEN s.stock ELSE 0 END) -
          SUM(CASE WHEN RTRIM(s.tipo) = 'COM' THEN s.stock ELSE 0 END) -
          SUM(CASE WHEN RTRIM(s.tipo) = 'DES' THEN s.stock ELSE 0 END) AS stock
        FROM saStockAlmacen s
        LEFT JOIN saAlmacen a ON s.co_alma = a.co_alma
        WHERE LTRIM(RTRIM(s.co_art)) IN (${coArtsList})
        GROUP BY s.co_art, s.co_alma, a.des_alma
        HAVING (SUM(CASE WHEN RTRIM(s.tipo) = 'ACT' THEN s.stock ELSE 0 END) - SUM(CASE WHEN RTRIM(s.tipo) = 'COM' THEN s.stock ELSE 0 END) - SUM(CASE WHEN RTRIM(s.tipo) = 'DES' THEN s.stock ELSE 0 END)) > 0
      `;

      const queryPreciosBatch = `
        WITH UltimosPrecios AS (
          SELECT 
            RTRIM(co_art) AS co_art, RTRIM(co_precio) AS id_precio, monto AS precio, RTRIM(co_mone) AS moneda,
            ROW_NUMBER() OVER(PARTITION BY co_art, co_precio ORDER BY desde DESC) as rn
          FROM saArtPrecio
          WHERE LTRIM(RTRIM(co_art)) IN (${coArtsList}) AND Inactivo = 0 AND GETDATE() >= desde AND (hasta IS NULL OR GETDATE() <= hasta)
        )
        SELECT co_art, id_precio, precio, moneda FROM UltimosPrecios WHERE rn = 1
      `;

      const [resStock, resPrecios] = await Promise.all([
        pool.request().query(queryStockBatch),
        pool.request().query(queryPreciosBatch)
      ]);

      // Integrar las 2 colecciones a la data principal
      articulos = articulos.map(art => ({
        ...art,
        precios: resPrecios.recordset
          .filter(p => p.co_art === art.co_art)
          .map(p => ({ id_precio: p.id_precio, precio: p.precio, moneda: p.moneda })),
        disponibilidad_por_almacen: resStock.recordset
          .filter(s => s.co_art === art.co_art)
          .map(s => ({ co_alma: s.co_alma, des_alma: s.des_alma, stock: s.stock }))
      }));
    }

    res.status(200).json({
      success: true,
      tasa_oficial_bcv: tasaActual,
      page: page,
      limit: limit,
      total_items: totalItems,
      total_pages: Math.ceil(totalItems / limit),
      count: articulos.length,
      data: articulos
    });

  } catch (error) {
    console.error('Error al consultar artículos:', error);
    res.status(500).json({
      success: false,
      message: 'Error al consultar la lista de artículos.',
      error: error.message
    });
  }
});

/**
 * 2. Endpoint: Buscar artículos filtrando por campos específicos
 * GET /api/v1/articulos/search
 * 
 * Ejemplo de uso:
 * GET /api/v1/articulos/search?descripcion=tarjeta
 * GET /api/v1/articulos/search?modelo=PCI&linea=01
 * GET /api/v1/articulos/search?referencia=AB123
 * Headers: { "x-api-key": "mi-clave-secreta" }
 */
router.get('/search', async (req, res) => {
  try {
    const page = parseInt(req.query.page) || 1;
    const limit = parseInt(req.query.limit) || 30; // 30 por defecto
    const offset = (page - 1) * limit;

    // Mapeo seguro de parámetros permitidos a sus columnas reales en Profit Plus
    const allowedFields = {
      'co_art': 'co_art',
      'descripcion': 'art_des',
      'modelo': 'modelo',
      'referencia': 'ref',
      'tipo': 'tipo',
      'linea': 'co_lin',
      'sublinea': 'co_subl',
      'categoria': 'co_cat',
      'proveedor': 'co_prov'
    };

    const activeFilters = [];

    // Identificar qué parámetros válidos nos enviaron en el query
    for (const key in req.query) {
      if (key !== 'page' && key !== 'limit' && allowedFields[key] && req.query[key]) {
        activeFilters.push({
          paramPath: key,
          dbColumn: allowedFields[key],
          value: req.query[key]
        });
      }
    }

    if (activeFilters.length === 0) {
      return res.status(400).json({
        success: false,
        message: `Debe especificar al menos un parámetro de búsqueda válido. Permitidos: ${Object.keys(allowedFields).join(', ')}`
      });
    }

    const pool = await getPool();

    // Construir consulta dinámica 
    let queryBase = `
      SELECT 
        RTRIM(co_art) AS co_art, 
        RTRIM(art_des) AS descripcion,
        RTRIM(tipo) AS tipo,
        RTRIM(modelo) AS modelo,
        RTRIM(ref) AS referencia
      FROM saArticulo
      WHERE anulado = 0 
    `;

    let queryCount = `SELECT COUNT(*) AS total FROM saArticulo WHERE anulado = 0`;

    // 3. Tasa de cambio oficial actual
    const queryTasa = `
      SELECT TOP 1 tasa_v AS tasa_cambio
      FROM saTasa
      WHERE LTRIM(RTRIM(co_mone)) IN ('US$', 'USD')
      ORDER BY fecha DESC
    `;

    const request = pool.request();

    // Armar el WHERE dinámico para SQL inyectando parámetros de forma segura
    let whereClause = '';
    activeFilters.forEach(filter => {
      whereClause += ` AND ${filter.dbColumn} LIKE '%' + @${filter.paramPath} + '%'`;
      request.input(filter.paramPath, sql.VarChar, filter.value);
    });

    queryBase += whereClause;
    queryCount += whereClause;

    queryBase += ` ORDER BY art_des OFFSET @offset ROWS FETCH NEXT @limit ROWS ONLY`;

    request.input('offset', sql.Int, offset);
    request.input('limit', sql.Int, limit);

    const [resCount, resultBusqueda, resultTasa] = await Promise.all([
      request.query(queryCount),
      request.query(queryBase),
      pool.request().query(queryTasa)
    ]);

    const totalItems = resCount.recordset[0].total;
    const tasaActual = resultTasa.recordset.length > 0 ? resultTasa.recordset[0].tasa_cambio : 1;
    let articulos = resultBusqueda.recordset;

    if (articulos.length > 0) {
      // Obtener todos los IDs para lanzar búsquedas en lote de almacenes y precios
      const coArtsList = articulos.map(a => `'${a.co_art.replace(/'/g, "''")}'`).join(',');

      const queryStockBatch = `
        SELECT 
          RTRIM(co_art) AS co_art, RTRIM(co_alma) AS co_alma, 
          SUM(CASE WHEN RTRIM(tipo) = 'ACT' THEN stock ELSE 0 END) -
          SUM(CASE WHEN RTRIM(tipo) = 'COM' THEN stock ELSE 0 END) -
          SUM(CASE WHEN RTRIM(tipo) = 'DES' THEN stock ELSE 0 END) AS stock
        FROM saStockAlmacen
        WHERE LTRIM(RTRIM(co_art)) IN (${coArtsList})
        GROUP BY co_art, co_alma
        HAVING (SUM(CASE WHEN RTRIM(tipo) = 'ACT' THEN stock ELSE 0 END) - SUM(CASE WHEN RTRIM(tipo) = 'COM' THEN stock ELSE 0 END) - SUM(CASE WHEN RTRIM(tipo) = 'DES' THEN stock ELSE 0 END)) > 0
      `;

      const queryPreciosBatch = `
        WITH UltimosPrecios AS (
          SELECT 
            RTRIM(co_art) AS co_art, RTRIM(co_precio) AS id_precio, monto AS precio, RTRIM(co_mone) AS moneda,
            ROW_NUMBER() OVER(PARTITION BY co_art, co_precio ORDER BY desde DESC) as rn
          FROM saArtPrecio
          WHERE LTRIM(RTRIM(co_art)) IN (${coArtsList}) AND Inactivo = 0 AND GETDATE() >= desde AND (hasta IS NULL OR GETDATE() <= hasta)
        )
        SELECT co_art, id_precio, precio, moneda FROM UltimosPrecios WHERE rn = 1
      `;

      const [resStock, resPrecios] = await Promise.all([
        pool.request().query(queryStockBatch),
        pool.request().query(queryPreciosBatch)
      ]);

      // Integrar las 2 colecciones a la data principal
      articulos = articulos.map(art => ({
        ...art,
        precios: resPrecios.recordset
          .filter(p => p.co_art === art.co_art)
          .map(p => ({ id_precio: p.id_precio, precio: p.precio, moneda: p.moneda })),
        disponibilidad_por_almacen: resStock.recordset
          .filter(s => s.co_art === art.co_art)
          .map(s => ({ co_alma: s.co_alma, stock: s.stock }))
      }));
    }

    res.status(200).json({
      success: true,
      tasa_oficial_bcv: tasaActual,
      page: page,
      limit: limit,
      total_items: totalItems,
      total_pages: Math.ceil(totalItems / limit),
      count: articulos.length,
      data: articulos
    });

  } catch (error) {
    console.error('Error al buscar artículos:', error);
    res.status(500).json({
      success: false,
      message: 'Error al procesar la búsqueda de artículos.',
      error: error.message
    });
  }
});

/**
 * 2. Endpoint: Consultar stock de un artículo
 * GET /api/v1/articulos/:articulo
 * 
 * Ejemplo de uso:
 * GET /api/v1/articulos/ART-001
 * Headers: { "x-api-key": "mi-clave-secreta" }
 */
router.get('/:articulo', async (req, res) => {
  try {
    const { articulo } = req.params;
    const servers = getServers();

    // Consultamos todos los servidores configurados en paralelo
    const results = await Promise.all(servers.map(async (srv) => {
      try {
        const pool = await getPool(srv.id);
        const request = pool.request().input('co_art', sql.VarChar, articulo);

        const queryArticulo = `
          SELECT 
            RTRIM(co_art) AS co_art, 
            RTRIM(art_des) AS descripcion,
            anulado,
            RTRIM(tipo) AS tipo_articulo
          FROM saArticulo
          WHERE LTRIM(RTRIM(co_art)) = LTRIM(RTRIM(@co_art))
        `;

        const queryStock = `
          SELECT 
            RTRIM(co_alma) AS co_alma, 
            SUM(CASE WHEN RTRIM(tipo) = 'ACT' THEN stock ELSE 0 END) -
            SUM(CASE WHEN RTRIM(tipo) = 'COM' THEN stock ELSE 0 END) -
            SUM(CASE WHEN RTRIM(tipo) = 'DES' THEN stock ELSE 0 END) AS stock
          FROM saStockAlmacen
          WHERE LTRIM(RTRIM(co_art)) = LTRIM(RTRIM(@co_art))
          GROUP BY co_alma
          HAVING (
            SUM(CASE WHEN RTRIM(tipo) = 'ACT' THEN stock ELSE 0 END) -
            SUM(CASE WHEN RTRIM(tipo) = 'COM' THEN stock ELSE 0 END) -
            SUM(CASE WHEN RTRIM(tipo) = 'DES' THEN stock ELSE 0 END)
          ) > 0
        `;

        const queryPrecios = `
          WITH UltimosPrecios AS (
            SELECT 
              RTRIM(co_precio) AS id_precio,
              monto AS precio,
              RTRIM(co_mone) AS moneda,
              ROW_NUMBER() OVER(PARTITION BY co_precio ORDER BY desde DESC) as rn
            FROM saArtPrecio
            WHERE LTRIM(RTRIM(co_art)) = LTRIM(RTRIM(@co_art)) 
              AND Inactivo = 0
              AND GETDATE() >= desde 
              AND (hasta IS NULL OR GETDATE() <= hasta)
          )
          SELECT id_precio, precio, moneda
          FROM UltimosPrecios
          WHERE rn = 1
          ORDER BY id_precio
        `;

        const queryTasa = `
          SELECT TOP 1 tasa_v AS tasa_cambio
          FROM saTasa
          WHERE LTRIM(RTRIM(co_mone)) IN ('US$', 'USD')
          ORDER BY fecha DESC
        `;

        const [resArt, resStock, resPre, resTasa] = await Promise.all([
          request.query(queryArticulo),
          request.query(queryStock),
          request.query(queryPrecios),
          pool.request().query(queryTasa)
        ]);

        if (resArt.recordset.length === 0) return null;

        const tasaActual = resTasa.recordset.length > 0 ? resTasa.recordset[0].tasa_cambio : 1;

        return {
          source_id: srv.id,
          source_name: srv.name,
          existencias: resStock.recordset,
          precios: resPre.recordset.map(p => {
            const monedaFija = p.moneda || '';
            const esDolar = monedaFija.includes('US');
            const esBolivar = monedaFija.includes('BS') || monedaFija.includes('VES');
            return {
              id_precio: p.id_precio,
              precio: p.precio,
              moneda: p.moneda,
              precio_ves: esDolar ? Number((p.precio * tasaActual).toFixed(2)) : (esBolivar ? p.precio : null)
            };
          }),
          tasa_cambio: tasaActual,
          articulo_metadata: resArt.recordset[0]
        };
      } catch (err) {
        console.error(`Error consultando servidor ${srv.id}:`, err.message);
        return { source_id: srv.id, source_name: srv.name, error: err.message };
      }
    }));

    // Consolidar resultados
    const validResults = results.filter(r => r && !r.error);
    
    if (validResults.length === 0) {
      return res.status(404).json({ 
        success: false, 
        message: 'Artículo no encontrado en ninguna de las fuentes de datos configuradas.',
        details: results.filter(r => r && r.error)
      });
    }

    // Usamos el primer resultado válido para la metadata básica
    const baseInfo = validResults[0].articulo_metadata;
    
    const consolidatedResponse = {
      co_art: baseInfo.co_art,
      descripcion: baseInfo.descripcion,
      tipo_articulo: baseInfo.tipo_articulo,
      fuentes_de_datos: results.filter(r => r !== null).map(r => ({
        id: r.source_id,
        nombre: r.source_name,
        error: r.error || null,
        total_stock: r.error ? 0 : r.existencias.reduce((acc, curr) => acc + curr.stock, 0),
        disponibilidad: r.error ? [] : r.existencias,
        precios: r.error ? [] : r.precios,
        tasa_cambio: r.error ? null : r.tasa_cambio
      })),
      total_stock_global: validResults.reduce((acc, r) => acc + r.existencias.reduce((a, c) => a + c.stock, 0), 0)
    };

    res.status(200).json({
      success: true,
      data: consolidatedResponse
    });

  } catch (error) {
    console.error('Error al consultar stock consolidado:', error);
    res.status(500).json({
      success: false,
      message: 'Error al consultar el stock en las bases de datos.',
      error: error.message
    });
  }
});

/**
 * 4. Endpoint: Crear un nuevo artículo
 * POST /api/v1/articulos
 * 
 * Ejemplo de JSON (Cuerpo de la petición):
 * {
 *   "co_art": "ART-001",                   // Obligatorio: Código único del artículo
 *   "art_des": "DESCRIPCION DEL ARTICULO", // Obligatorio: Descripción
 *   "co_ubicacion": "CONT1A",              // Opcional: Código de la nueva ubicación física
 *   "tipo": "V",                           // Opcional: V=Venta, E=Equipo, M=Materia Prima...
 *   "item": "ITEM-123",                    // Opcional: Nro Auxiliar o de Item
 *   "modelo": "GENERICO",                  // Opcional: Modelo
 *   "ref": "REF-ABC",                      // Opcional: Referencia comercial
 *   "co_lin": "01",                        // Opcional: Código de línea (Por defecto: Toma la primera)
 *   "co_subl": "01",                       // Opcional: Código de sublínea (Por defecto: Toma la primera)
 *   "co_cat": "01",                        // Opcional: Código de categoría (Por defecto: Toma la primera)
 *   "co_color": "01",                      // Opcional: Código de color (Por defecto: Toma el primero)
 *   "tipo_imp": "1"                        // Opcional: Código de Impuesto (Por defecto: Toma el primero)
 * }
 */
router.post('/', async (req, res) => {
  try {
    const data = req.body;

    if (!data.co_art || !data.art_des) {
      return res.status(400).json({ success: false, message: 'Faltan campos obligatorios: co_art y art_des' });
    }

    const pool = await getPool();
    const f = new Date(); // Usado solo para sdFecha_Inac y similares

    // Obtener valores por defecto
    const [resLin, resSubl, resCat, resCol, resUbic] = await Promise.all([
      pool.request().query('SELECT TOP 1 RTRIM(co_lin) as id FROM saLineaArticulo'),
      pool.request().query('SELECT TOP 1 RTRIM(co_subl) as id FROM saSubLinea'),
      pool.request().query('SELECT TOP 1 RTRIM(co_cat) as id FROM saCatArticulo'),
      pool.request().query('SELECT TOP 1 RTRIM(co_color) as id FROM saColor'),
      pool.request().query('SELECT TOP 1 RTRIM(co_ubicacion) as id FROM saUbicacion')
    ]);

    const defLin = resLin.recordset.length > 0 ? resLin.recordset[0].id : null;
    const defSubl = resSubl.recordset.length > 0 ? resSubl.recordset[0].id : null;
    const defCat = resCat.recordset.length > 0 ? resCat.recordset[0].id : null;
    const defCol = resCol.recordset.length > 0 ? resCol.recordset[0].id : null;
    const defUbic = resUbic.recordset.length > 0 ? resUbic.recordset[0].id : 'CONT1A';

    const request = new sql.Request(pool);

    // Tratamos los valores de data permitiendo fallbacks correctos
    const valCoLin = data.co_lin ? String(data.co_lin).trim() || defLin : defLin;
    const valCoSubl = data.co_subl ? String(data.co_subl).trim() || defSubl : defSubl;
    const valCoCat = data.co_cat ? String(data.co_cat).trim() || defCat : defCat;
    const valCoColor = data.co_color ? String(data.co_color).trim() || defCol : defCol;
    const valCoUbic = data.co_ubicacion ? String(data.co_ubicacion).trim() || defUbic : defUbic;

    request.input('sCo_Art', sql.Char(30), data.co_art);
    request.input('sdFecha_Reg', sql.SmallDateTime, f);
    request.input('sArt_Des', sql.VarChar(120), data.art_des);
    request.input('sTipo', sql.Char(1), data.tipo || 'V');
    request.input('bAnulado', sql.Bit, 0);
    request.input('sdFecha_Inac', sql.SmallDateTime, f);
    request.input('sCo_Lin', sql.Char(6), valCoLin);
    request.input('sCo_Subl', sql.Char(6), valCoSubl);
    request.input('sCo_Cat', sql.Char(6), valCoCat);
    request.input('sCo_Color', sql.Char(6), valCoColor);
    request.input('sCo_Ubicacion', sql.Char(6), valCoUbic);
    request.input('sItem', sql.VarChar(10), data.item || null);
    request.input('sModelo', sql.VarChar(20), data.modelo || '');
    request.input('sRef', sql.VarChar(20), data.ref || '');
    request.input('bGenerico', sql.Bit, 0);
    request.input('bManeja_Serial', sql.Bit, 0);
    request.input('bManeja_Lote', sql.Bit, 0);
    request.input('bManeja_Lote_Venc', sql.Bit, 0);
    request.input('deMargen_Min', sql.Decimal(18, 5), 0);
    request.input('deMargen_Max', sql.Decimal(18, 5), 0);
    request.input('sTipo_Imp', sql.Char(1), data.tipo_imp || '1');
    request.input('sTipo_Imp2', sql.Char(1), '7');
    request.input('sTipo_Imp3', sql.Char(1), '7');
    request.input('sCo_Reten', sql.Char(6), null);
    request.input('sCod_Proc', sql.Char(6), null);
    request.input('sGarantia', sql.VarChar(30), '');
    request.input('deVolumen', sql.Decimal(18, 5), 0);
    request.input('dePeso', sql.Decimal(18, 5), 0);
    request.input('deStock_Min', sql.Decimal(18, 5), 0);
    request.input('deStock_Max', sql.Decimal(18, 5), 0);
    request.input('deStock_Pedido', sql.Decimal(18, 5), 0);
    request.input('iRelac_Unidad', sql.Int, 1);
    request.input('dePunt_Ven', sql.Decimal(18, 5), 0);
    request.input('dePunt_Cli', sql.Decimal(18, 5), 0);
    request.input('deLic_Mon_Ilc', sql.Decimal(18, 5), 0);
    request.input('deLic_Capacidad', sql.Decimal(18, 5), 0);
    request.input('deLic_Grado_Al', sql.Decimal(18, 5), 0);
    request.input('sLic_Tipo', sql.Char(1), null);
    request.input('bPrec_Om', sql.Bit, 0);
    request.input('sComentario', sql.VarChar(sql.MAX), null);
    request.input('sTipo_Cos', sql.Char(4), '1');
    request.input('dePorc_Margen_Minimo', sql.Decimal(18, 5), 0);
    request.input('dePorc_Margen_Maximo', sql.Decimal(18, 5), 0);
    request.input('deMont_Comi', sql.Decimal(18, 5), 0);
    request.input('dePorc_Arancel', sql.Decimal(18, 5), 0);
    request.input('sI_Art_Des', sql.VarChar(120), null);
    request.input('sDis_Cen', sql.VarChar(sql.MAX), null);
    request.input('sReten_Iva_Tercero', sql.Char(16), null);
    request.input('sCampo1', sql.VarChar(60), '');
    request.input('sCampo2', sql.VarChar(60), '');
    request.input('sCampo3', sql.VarChar(60), '');
    request.input('sCampo4', sql.VarChar(60), '');
    request.input('sCampo5', sql.VarChar(60), '');
    request.input('sCampo6', sql.VarChar(60), '');
    request.input('sCampo7', sql.VarChar(60), '');
    request.input('sCampo8', sql.VarChar(60), '');
    request.input('sCo_Us_In', sql.Char(6), '999');
    request.input('sCo_Sucu_In', sql.Char(6), null);
    request.input('sMaquina', sql.VarChar(60), 'API-APP');
    request.input('sRevisado', sql.Char(1), '0');
    request.input('sTrasnfe', sql.Char(1), '0');

    await request.execute('pInsertarArticulo');
    res.status(201).json({ success: true, message: 'Artículo creado correctamente.', co_art: data.co_art });
  } catch (error) {
    console.error('Error al insertar el artículo:', error);
    let errMsg = error.message;
    if (!errMsg || errMsg === '') {
      if (error.precedingErrors && error.precedingErrors.length > 0) {
        errMsg = error.precedingErrors.map(e => e.message).join(' | ');
      } else if (error.originalError && error.originalError.errors) {
        errMsg = error.originalError.errors.map(e => e.message).join(' | ');
      }
    }
    res.status(500).json({ success: false, message: 'Error en BD.', error: errMsg || error.toString() });
  }
});

/**
 * 5. Endpoint: Editar un artículo existente
 * PUT /api/v1/articulos/:co_art
 * 
 * Ejemplo de JSON (Cuerpo de la petición):
 * NOTA: Solo se deben mandar los campos que se desean actualizar.
 * {
 *   "co_art": "NUEVO-CO-ART",              // Opcional: Cambia el código primario del artículo
 *   "art_des": "NUEVA DESCRIPCION",        // Opcional: Cambia la descripción
 *   "co_ubicacion": "CONT2B",              // Opcional: Cambia la ubicación física en almacén
 *   "tipo": "V",                           // Opcional: V=Venta, E=Equipo...
 *   "modelo": "NUEVO MODELO",              // Opcional: Cambiar Modelo
 *   "ref": "NUEVA REF",                    // Opcional: Cambiar la Referencia
 *   "co_lin": "02"                         // Opcional: Cambiar la Línea... etc.
 * }
 */
router.put('/:co_art', async (req, res) => {
  try {
    const coArtOriginal = req.params.co_art;
    const data = req.body;
    const pool = await getPool();

    // Verificación
    const checkQuery = `
      SELECT 
        RTRIM(co_art) as co_art, 
        validador,
        RTRIM(co_lin) as co_lin,
        RTRIM(co_subl) as co_subl,
        RTRIM(co_cat) as co_cat,
        RTRIM(co_color) as co_color,
        RTRIM(co_ubicacion) as co_ubicacion,
        tipo_imp,
        tipo_cos
      FROM saArticulo 
      WHERE LTRIM(RTRIM(co_art)) = LTRIM(RTRIM(@co_art))
    `;
    const checkReq = pool.request().input('co_art', sql.VarChar, coArtOriginal);
    const exists = await checkReq.query(checkQuery);

    if (exists.recordset.length === 0) {
      return res.status(404).json({ success: false, message: 'El artículo no existe.' });
    }

    const row = exists.recordset[0];
    const validadorBuffer = row.validador;
    const nuevoCoArt = data.co_art ? data.co_art : coArtOriginal;
    const f = new Date(); // Usado solo para sdFecha_Inac y similares

    // Obtener valores por defecto
    const [resLin, resSubl, resCat, resCol, resUbic] = await Promise.all([
      pool.request().query('SELECT TOP 1 RTRIM(co_lin) as id FROM saLineaArticulo'),
      pool.request().query('SELECT TOP 1 RTRIM(co_subl) as id FROM saSubLinea'),
      pool.request().query('SELECT TOP 1 RTRIM(co_cat) as id FROM saCatArticulo'),
      pool.request().query('SELECT TOP 1 RTRIM(co_color) as id FROM saColor'),
      pool.request().query('SELECT TOP 1 RTRIM(co_ubicacion) as id FROM saUbicacion')
    ]);

    const defLin = resLin.recordset.length > 0 ? resLin.recordset[0].id : null;
    const defSubl = resSubl.recordset.length > 0 ? resSubl.recordset[0].id : null;
    const defCat = resCat.recordset.length > 0 ? resCat.recordset[0].id : null;
    const defCol = resCol.recordset.length > 0 ? resCol.recordset[0].id : null;
    const defUbic = resUbic.recordset.length > 0 ? resUbic.recordset[0].id : 'CONT1A';

    const valCoLin = data.co_lin ? String(data.co_lin).trim() || row.co_lin || defLin : (row.co_lin || defLin);
    const valCoSubl = data.co_subl ? String(data.co_subl).trim() || row.co_subl || defSubl : (row.co_subl || defSubl);
    const valCoCat = data.co_cat ? String(data.co_cat).trim() || row.co_cat || defCat : (row.co_cat || defCat);
    const valCoColor = data.co_color ? String(data.co_color).trim() || row.co_color || defCol : (row.co_color || defCol);
    const valCoUbic = data.co_ubicacion ? String(data.co_ubicacion).trim() || row.co_ubicacion || defUbic : (row.co_ubicacion || defUbic);

    const request = new sql.Request(pool);
    request.input('sCo_Art', sql.Char(30), nuevoCoArt);
    request.input('sCo_ArtOri', sql.Char(30), coArtOriginal);
    request.input('sdFecha_Reg', sql.SmallDateTime, f);
    request.input('sArt_Des', sql.VarChar(120), data.art_des || 'Artículo Modificado API');
    request.input('sTipo', sql.Char(1), data.tipo || 'V');
    request.input('bAnulado', sql.Bit, 0);
    request.input('sdFecha_Inac', sql.SmallDateTime, f);
    request.input('sCo_Lin', sql.Char(6), valCoLin);
    request.input('sCo_Subl', sql.Char(6), valCoSubl);
    request.input('sCo_Cat', sql.Char(6), valCoCat);
    request.input('sCo_Color', sql.Char(6), valCoColor);
    request.input('sCo_Ubicacion', sql.Char(6), valCoUbic);
    request.input('sItem', sql.VarChar(10), data.item || null);
    request.input('sModelo', sql.VarChar(20), data.modelo || '');
    request.input('sRef', sql.VarChar(20), data.ref || '');
    request.input('bGenerico', sql.Bit, 0);
    request.input('bManeja_Serial', sql.Bit, 0);
    request.input('bManeja_Lote', sql.Bit, 0);
    request.input('bManeja_Lote_Venc', sql.Bit, 0);
    request.input('deMargen_Min', sql.Decimal(18, 5), 0);
    request.input('deMargen_Max', sql.Decimal(18, 5), 0);
    request.input('sTipo_Imp', sql.Char(1), data.tipo_imp || row.tipo_imp || '1');
    request.input('sTipo_Imp2', sql.Char(1), '7');
    request.input('sTipo_Imp3', sql.Char(1), '7');
    request.input('sCo_Reten', sql.Char(6), null);
    request.input('sCod_Proc', sql.Char(6), null);
    request.input('sGarantia', sql.VarChar(30), '');
    request.input('deVolumen', sql.Decimal(18, 5), 0);
    request.input('dePeso', sql.Decimal(18, 5), 0);
    request.input('deStock_Min', sql.Decimal(18, 5), 0);
    request.input('deStock_Max', sql.Decimal(18, 5), 0);
    request.input('deStock_Pedido', sql.Decimal(18, 5), 0);
    request.input('iRelac_Unidad', sql.Int, 1);
    request.input('dePunt_Ven', sql.Decimal(18, 5), 0);
    request.input('dePunt_Cli', sql.Decimal(18, 5), 0);
    request.input('deLic_Mon_Ilc', sql.Decimal(18, 5), 0);
    request.input('deLic_Capacidad', sql.Decimal(18, 5), 0);
    request.input('deLic_Grado_Al', sql.Decimal(18, 5), 0);
    request.input('sLic_Tipo', sql.Char(1), null);
    request.input('bPrec_Om', sql.Bit, 0);
    request.input('sComentario', sql.VarChar(sql.MAX), null);
    request.input('sTipo_Cos', sql.Char(4), data.tipo_cos || row.tipo_cos || '1');
    request.input('dePorc_Margen_Minimo', sql.Decimal(18, 5), 0);
    request.input('dePorc_Margen_Maximo', sql.Decimal(18, 5), 0);
    request.input('deMont_Comi', sql.Decimal(18, 5), 0);
    request.input('dePorc_Arancel', sql.Decimal(18, 5), 0);
    request.input('sDis_Cen', sql.VarChar(sql.MAX), null);
    request.input('sReten_Iva_Tercero', sql.Char(16), null);
    request.input('sCampo1', sql.VarChar(60), '');
    request.input('sCampo2', sql.VarChar(60), '');
    request.input('sCampo3', sql.VarChar(60), '');
    request.input('sCampo4', sql.VarChar(60), '');
    request.input('sCampo5', sql.VarChar(60), '');
    request.input('sCampo6', sql.VarChar(60), '');
    request.input('sCampo7', sql.VarChar(60), '');
    request.input('sCampo8', sql.VarChar(60), '');
    request.input('sCo_Us_Mo', sql.Char(6), '999');
    request.input('sCo_Sucu_Mo', sql.Char(6), null);
    request.input('sMaquina', sql.VarChar(60), 'API-APP');
    request.input('sCampos', sql.VarChar(sql.MAX), '');
    request.input('sRevisado', sql.Char(1), '0');
    request.input('sTrasnfe', sql.Char(1), '0');
    request.input('tsValidador', sql.VarBinary, validadorBuffer);
    request.input('gRowguid', sql.UniqueIdentifier, null);

    await request.execute('pActualizarArticulo');
    res.status(200).json({ success: true, message: 'Artículo editado correctamente.', co_art: nuevoCoArt });
  } catch (error) {
    console.error('Error al editar el artículo:', error);
    let errMsg = error.message;
    if (!errMsg || errMsg === '') {
      if (error.precedingErrors && error.precedingErrors.length > 0) {
        errMsg = error.precedingErrors.map(e => e.message).join(' | ');
      } else if (error.originalError && error.originalError.errors) {
        errMsg = error.originalError.errors.map(e => e.message).join(' | ');
      }
    }
    res.status(500).json({ success: false, message: 'Error en BD.', error: errMsg || error.toString() });
  }
});
/**
 * @swagger
 * /api/v1/articulos/{co_art}:
 *   delete:
 *     summary: Elimina un artículo
 *     description: Elimina un artículo de manera permanente de la base de datos de Profit Plus.
 *     tags: [Artículos]
 *     parameters:
 *       - in: path
 *         name: co_art
 *         required: true
 *         schema:
 *           type: string
 *         description: Código del artículo a eliminar
 *     responses:
 *       200:
 *         description: Artículo eliminado correctamente
 *       404:
 *         description: Artículo no encontrado
 *       500:
 *         description: Error interno del servidor o de base de datos
 */
router.delete('/:co_art', async (req, res) => {
  try {
    const pool = await getPool();
    const co_art = req.params.co_art;

    // Verificación
    const checkQuery = `SELECT RTRIM(co_art) as co_art, validador FROM saArticulo WHERE LTRIM(RTRIM(co_art)) = LTRIM(RTRIM(@co_art))`;
    const checkReq = pool.request().input('co_art', sql.VarChar, co_art);
    const exists = await checkReq.query(checkQuery);

    if (exists.recordset.length === 0) {
      return res.status(404).json({ success: false, message: 'El artículo no existe o ya fue eliminado.' });
    }

    const validadorBuffer = exists.recordset[0].validador;

    const request = new sql.Request(pool);
    request.input('sCo_ArtOri', sql.Char(30), co_art);
    request.input('tsValidador', sql.VarBinary, validadorBuffer);
    request.input('sMaquina', sql.VarChar(60), 'API-APP');
    request.input('sCo_Us_Mo', sql.Char(6), '999');
    request.input('sCo_Sucu_Mo', sql.Char(6), null);
    request.input('gRowguid', sql.UniqueIdentifier, null);

    await request.execute('pEliminarArticulo');
    res.status(200).json({ success: true, message: 'Artículo eliminado correctamente.' });
  } catch (error) {
    console.error('Error al eliminar el artículo:', error);
    let errMsg = error.message;
    if (!errMsg || errMsg === '') {
      if (error.precedingErrors && error.precedingErrors.length > 0) {
        errMsg = error.precedingErrors.map(e => e.message).join(' | ');
      } else if (error.originalError && error.originalError.errors) {
        errMsg = error.originalError.errors.map(e => e.message).join(' | ');
      }
    }
    res.status(500).json({ success: false, message: 'Error en BD.', error: errMsg || error.toString() });
  }
});

module.exports = router;
