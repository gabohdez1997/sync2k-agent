const express = require('express');
const router = express.Router();
const { sql, getPool } = require('../db');

/**
 * 1. Endpoint: Consultar lista de clientes
 * GET /api/v1/clientes
 * 
 * Ejemplo de uso:
 * GET /api/v1/clientes?page=1&limit=10
 * Headers: { "x-api-key": "mi-clave-secreta" }
 */
router.get('/', async (req, res) => {
    try {
        const page = parseInt(req.query.page) || 1;
        const limit = parseInt(req.query.limit) || 10;
        const offset = (page - 1) * limit;

        const pool = await getPool();

        // 1. Query Total
        const queryCount = `SELECT COUNT(*) AS total FROM saCliente WHERE inactivo = 0`;

        // 2. Query Data paginada
        const queryClientes = `
          SELECT 
            RTRIM(co_cli) AS co_cli, 
            RTRIM(cli_des) AS descripcion
          FROM saCliente
          WHERE inactivo = 0
          ORDER BY cli_des
          OFFSET @offset ROWS 
          FETCH NEXT @limit ROWS ONLY
        `;

        const requestData = pool.request();
        requestData.input('offset', sql.Int, offset);
        requestData.input('limit', sql.Int, limit);

        const [resCount, resData] = await Promise.all([
            pool.request().query(queryCount),
            requestData.query(queryClientes)
        ]);

        const totalItems = resCount.recordset[0].total;

        res.status(200).json({
            success: true,
            page: page,
            limit: limit,
            total_items: totalItems,
            total_pages: Math.ceil(totalItems / limit),
            count: resData.recordset.length,
            data: resData.recordset
        });

    } catch (error) {
        console.error('Error al consultar clientes:', error);
        res.status(500).json({
            success: false,
            message: 'Error al consultar la lista de clientes.',
            error: error.message
        });
    }
});

/**
 * 2. Endpoint: Buscar clientes filtrando por campos específicos
 * GET /api/v1/clientes/search
 * 
 * Ejemplo de uso:
 * GET /api/v1/clientes/search?rif=J-1234
 * GET /api/v1/clientes/search?descripcion=proveedor
 * GET /api/v1/clientes/search?email=gmail
 * GET /api/v1/clientes/search?direccion=caracas&telefonos=123
 * Headers: { "x-api-key": "mi-clave-secreta" }
 */
router.get('/search', async (req, res) => {
    try {
        const page = parseInt(req.query.page) || 1;
        const limit = parseInt(req.query.limit) || 30; // 30 por defecto en búsquedas
        const offset = (page - 1) * limit;

        // Mapeo seguro de parámetros permitidos a sus columnas reales en Profit Plus
        const allowedFields = {
            'co_cli': 'co_cli',
            'descripcion': 'cli_des',
            'rif': 'rif',
            'direccion': 'direc1',
            'telefonos': 'telefonos',
            'email': 'email',
            'vendedor': 'co_ven',
            'zona': 'co_zon',
            'segmento': 'co_seg',
            'tipo': 'tip_cli',
            'moneda': 'co_mone',
            'condicion': 'co_cond'
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
            RTRIM(co_cli) AS co_cli, 
            RTRIM(cli_des) AS descripcion,
            RTRIM(rif) AS rif,
            RTRIM(direc1) AS direccion,
            RTRIM(telefonos) AS telefonos,
            RTRIM(email) AS email
          FROM saCliente
          WHERE inactivo = 0 
        `;

        let queryCount = `SELECT COUNT(*) AS total FROM saCliente WHERE inactivo = 0`;

        const request = pool.request();

        // Armar el WHERE dinámico para SQL inyectando parámetros de forma segura
        let whereClause = '';
        activeFilters.forEach(filter => {
            whereClause += ` AND ${filter.dbColumn} LIKE '%' + @${filter.paramPath} + '%'`;
            request.input(filter.paramPath, sql.VarChar, filter.value);
        });

        queryBase += whereClause;
        queryCount += whereClause;

        queryBase += ` ORDER BY cli_des OFFSET @offset ROWS FETCH NEXT @limit ROWS ONLY`;

        request.input('offset', sql.Int, offset);
        request.input('limit', sql.Int, limit);

        const [resCount, resData] = await Promise.all([
            request.query(queryCount),
            request.query(queryBase)
        ]);

        const totalItems = resCount.recordset[0].total;

        res.status(200).json({
            success: true,
            page: page,
            limit: limit,
            total_items: totalItems,
            total_pages: Math.ceil(totalItems / limit),
            count: resData.recordset.length,
            data: resData.recordset
        });

    } catch (error) {
        console.error('Error al buscar clientes:', error);
        res.status(500).json({
            success: false,
            message: 'Error al procesar la búsqueda de clientes.',
            error: error.message
        });
    }
});

/**
 * 2. Endpoint: Consultar detalle de un cliente específico
 * GET /api/v1/clientes/:co_cli
 * 
 * Ejemplo de uso:
 * GET /api/v1/clientes/J-12345678-9
 * Headers: { "x-api-key": "mi-clave-secreta" }
 */
router.get('/:co_cli', async (req, res) => {
    try {
        const pool = await getPool();
        const coCli = req.params.co_cli;

        const query = `
          SELECT 
            RTRIM(co_cli) AS co_cli, 
            RTRIM(cli_des) AS descripcion,
            RTRIM(rif) AS rif,
            RTRIM(direc1) AS direccion,
            RTRIM(telefonos) AS telefonos,
            RTRIM(email) AS email,
            RTRIM(co_ven) AS co_ven,
            RTRIM(co_zon) AS co_zon,
            inactivo
          FROM saCliente 
          WHERE RTRIM(co_cli) = LTRIM(RTRIM(@co_cli))
        `;

        const request = pool.request();
        request.input('co_cli', sql.VarChar, coCli);
        const result = await request.query(query);

        if (result.recordset.length === 0) {
            return res.status(404).json({ success: false, message: 'Cliente no encontrado.' });
        }

        res.status(200).json({ success: true, data: result.recordset[0] });
    } catch (error) {
        console.error('Error al consultar el detalle del cliente:', error);
        res.status(500).json({ success: false, message: 'Error al consultar el cliente.', error: error.message });
    }
});

/**
 * 3. Endpoint: Crear Cliente vía Stored Procedure
 * POST /api/v1/clientes
 * 
 * Ejemplo de uso:
 * POST /api/v1/clientes
 * Headers: { "x-api-key": "mi-clave-secreta", "Content-Type": "application/json" }
 * Body (JSON):
 * {
 *   // --- OBLIGATORIOS ---
 *   "co_cli": "0002",             // Código único del cliente (máx 16)
 *   "cli_des": "Nuevo Cliente",   // Nombre o Razón Social (máx 60)
 *   
 *   // --- OPCIONALES (Información) ---
 *   "rif": "J-00000000-0",        // RIF del cliente (máx 18)
 *   "direc1": "Calle Principal",  // Dirección fiscal principal
 *   "telefonos": "0212-0000000",  // Números de teléfono (máx 60)
 *   "email": "correo@empresa.com",// Correo electrónico (máx 60)
 *   
 *   // --- OPCIONALES (Claves Foráneas - Profit Plus) ---
 *   // Si no se envían, la API tomará el primer valor predeterminado válido de la BD.
 *   "co_seg": "01",               // Segmento del cliente (máx 6)
 *   "co_zon": "01",               // Zona del cliente (máx 6)
 *   "co_ven": "01",               // Vendedor asignado (máx 6)
 *   "tip_cli": "COR",             // Tipo de cliente (máx 6)
 *   "co_mone": "USD",             // Moneda de facturación (máx 6)
 *   "co_cond": "01",              // Condición de pago (máx 6)
 *   "co_cta_ingr_egr": "A-013"    // Cuenta de Ingresos/Egresos (máx 20)
 * }
 */
router.post('/', async (req, res) => {
    try {
        const data = req.body;

        if (!data.co_cli || !data.cli_des) {
            return res.status(400).json({ success: false, message: 'Faltan campos obligatorios: co_cli, cli_des' });
        }

        const pool = await getPool();
        const f = new Date();

        // Obtener valores por defecto de los catálogos para evitar errores de FKs
        const [resCta, resSeg, resZon, resVen, resTip, resMon, resCond] = await Promise.all([
            pool.request().query('SELECT TOP 1 RTRIM(co_cta_ingr_egr) as id FROM saCuentaIngEgr'),
            pool.request().query('SELECT TOP 1 RTRIM(co_seg) as id FROM saSegmento'),
            pool.request().query('SELECT TOP 1 RTRIM(co_zon) as id FROM saZona'),
            pool.request().query('SELECT TOP 1 RTRIM(co_ven) as id FROM saVendedor'),
            pool.request().query('SELECT TOP 1 RTRIM(tip_cli) as id FROM saTipoCliente'),
            pool.request().query('SELECT TOP 1 RTRIM(co_mone) as id FROM saMoneda'),
            pool.request().query('SELECT TOP 1 RTRIM(co_cond) as id FROM saCondicionPago')
        ]);

        const defCta = resCta.recordset.length > 0 ? resCta.recordset[0].id : null;
        const defSeg = resSeg.recordset.length > 0 ? resSeg.recordset[0].id : null;
        const defZon = resZon.recordset.length > 0 ? resZon.recordset[0].id : null;
        const defVen = resVen.recordset.length > 0 ? resVen.recordset[0].id : null;
        const defTip = resTip.recordset.length > 0 ? resTip.recordset[0].id : null;
        const defMon = resMon.recordset.length > 0 ? resMon.recordset[0].id : null;
        const defCond = resCond.recordset.length > 0 ? resCond.recordset[0].id : null;

        const request = new sql.Request(pool);
        request.input('sCo_Cli', sql.Char(16), data.co_cli);
        request.input('sLogin', sql.Char(20), '');
        request.input('sPassword', sql.Char(20), '');
        request.input('sSalesTax', sql.Char(8), null);
        request.input('sCli_Des', sql.VarChar(60), data.cli_des);
        request.input('sCo_Seg', sql.Char(6), data.co_seg || defSeg);
        request.input('sCo_Zon', sql.Char(6), data.co_zon || defZon);
        request.input('sCo_Ven', sql.Char(6), data.co_ven || defVen);
        request.input('sEstado', sql.Char(1), '1');
        request.input('bInactivo', sql.Bit, 0);
        request.input('bValido', sql.Bit, 0);
        request.input('bSinCredito', sql.Bit, 0);
        request.input('bLunes', sql.Bit, 1);
        request.input('bMartes', sql.Bit, 1);
        request.input('bMiercoles', sql.Bit, 1);
        request.input('bJueves', sql.Bit, 1);
        request.input('bViernes', sql.Bit, 1);
        request.input('bSabado', sql.Bit, 1);
        request.input('bDomingo', sql.Bit, 1);
        request.input('sDirec1', sql.VarChar(sql.MAX), data.direc1 || '');
        request.input('sDirec2', sql.VarChar(sql.MAX), '');
        request.input('sDir_Ent2', sql.VarChar(sql.MAX), '');
        request.input('sHorar_Caja', sql.VarChar(sql.MAX), '');
        request.input('sFrecu_Vist', sql.VarChar(sql.MAX), '');
        request.input('sTelefonos', sql.VarChar(60), data.telefonos || '');
        request.input('sFax', sql.VarChar(60), '');
        request.input('sRespons', sql.VarChar(60), '');
        request.input('sdFecha_Reg', sql.SmallDateTime, f);
        request.input('sTip_Cli', sql.Char(6), data.tip_cli || defTip);
        request.input('sSerialP', sql.Char(30), '');
        request.input('iPuntaje', sql.Int, 0);
        request.input('iId', sql.Int, 0);
        request.input('deMont_Cre', sql.Decimal(18, 5), 0);
        request.input('sCo_Mone', sql.Char(6), data.co_mone || defMon);
        request.input('sCond_Pag', sql.Char(6), data.co_cond || defCond);
        request.input('iPlaz_pag', sql.Int, 0);
        request.input('deDesc_ppago', sql.Decimal(18, 5), 0);
        request.input('deDesc_Glob', sql.Decimal(18, 5), 0);
        request.input('sTipo_Iva', sql.Char(1), '1');
        request.input('deIva', sql.Decimal(18, 5), 0);
        request.input('sRif', sql.VarChar(18), data.rif || '');
        request.input('bContrib', sql.Bit, 1);
        request.input('sDis_cen', sql.VarChar(sql.MAX), '');
        request.input('sNit', sql.VarChar(18), '');
        request.input('sEmail', sql.VarChar(60), data.email || '');
        request.input('sCo_Cta_Ingr_Egr', sql.Char(20), data.co_cta_ingr_egr || defCta);
        request.input('sComentario', sql.VarChar(sql.MAX), 'Creado vía API');
        request.input('sCampo1', sql.VarChar(60), '');
        request.input('sCampo2', sql.VarChar(60), '');
        request.input('sCampo3', sql.VarChar(60), '');
        request.input('sCampo4', sql.VarChar(60), '');
        request.input('sCampo5', sql.VarChar(60), '');
        request.input('sCampo6', sql.VarChar(60), '');
        request.input('sCampo7', sql.VarChar(60), '');
        request.input('sCampo8', sql.VarChar(60), '');
        request.input('sCo_Us_In', sql.Char(6), '999');
        request.input('sMaquina', sql.VarChar(60), 'SYNC2K');
        request.input('sRevisado', sql.Char(1), '0');
        request.input('sTrasnfe', sql.Char(1), '0');
        request.input('sCo_Sucu_In', sql.Char(6), '01');
        request.input('bJuridico', sql.Bit, 0);
        request.input('iTipo_Adi', sql.Int, 1);
        request.input('sMatriz', sql.Char(16), null);
        request.input('sCo_Tab', sql.Char(6), null);
        request.input('sTipo_Per', sql.Char(1), '0');
        request.input('sCo_pais', sql.Char(6), null);
        request.input('sCiudad', sql.VarChar(50), '');
        request.input('sZip', sql.VarChar(20), '');
        request.input('sWebSite', sql.VarChar(200), '');
        request.input('bContribu_E', sql.Bit, 0);
        request.input('bRete_Regis_Doc', sql.Bit, 0);
        request.input('dePorc_Esp', sql.Decimal(18, 5), 0);
        request.input('sN_cr', sql.Char(6), null);
        request.input('sN_db', sql.Char(6), null);
        request.input('sTComp', sql.Char(6), null);
        request.input('sEmail_alterno', sql.VarChar(60), '');

        await request.execute('pInsertarCliente');

        res.status(200).json({ success: true, message: 'Cliente insertado correctamente.', co_cli: data.co_cli });
    } catch (error) {
        console.error('Error al insertar cliente:', error);
        res.status(500).json({ success: false, message: 'Error en base de datos.', error: error.message });
    }
});

/**
 * 4. Endpoint: Actualizar Cliente 
 * PUT /api/v1/clientes/:co_cli
 * 
 * Ejemplo de uso:
 * PUT /api/v1/clientes/0002
 * Headers: { "x-api-key": "mi-clave-secreta", "Content-Type": "application/json" }
 * Body (JSON):
 * {
 *   // NOTA: Todos los campos en PUT son opcionales. 
 *   // Si no se envían, se conserva la información original o se asume el valor predeterminado
 *   
 *   // --- OPCIONALES (Información) ---
 *   "co_cli": "0003",             // Si se envía, cambia el código del cliente (máx 16)
 *   "cli_des": "Cliente Editado", // Nombre o Razón Social editado (máx 60)
 *   "rif": "J-12345678-9",        // RIF del cliente (máx 18)
 *   "direc1": "Otra Calle",       // Dirección fiscal principal
 *   "telefonos": "0212-9999999",  // Números de teléfono (máx 60)
 *   "email": "ventas@cliente.com",// Correo electrónico (máx 60)
 *   
 *   // --- OPCIONALES (Claves Foráneas - Profit Plus) ---
 *   "co_seg": "01",               // Segmento del cliente (máx 6)
 *   "co_zon": "01",               // Zona del cliente (máx 6)
 *   "co_ven": "01",               // Vendedor asignado (máx 6)
 *   "tip_cli": "COR",             // Tipo de cliente (máx 6)
 *   "co_mone": "USD",             // Moneda por defecto (máx 6)
 *   "co_cond": "01",              // Condición de pago (máx 6)
 *   "co_cta_ingr_egr": "A-013"    // Cuenta de Ingresos/Egresos (máx 20)
 * }
 */
router.put('/:co_cli', async (req, res) => {
    try {
        const coCli = req.params.co_cli;
        const data = req.body;

        const pool = await getPool();

        // Verificar existencia y obtener el validador actual
        const checkQuery = `SELECT RTRIM(co_cli) as co_cli, validador FROM saCliente WHERE RTRIM(co_cli) = LTRIM(RTRIM(@co_cli))`;
        const checkReq = pool.request().input('co_cli', sql.VarChar, coCli);
        const exists = await checkReq.query(checkQuery);

        if (exists.recordset.length === 0) {
            return res.status(404).json({ success: false, message: 'El cliente no existe.' });
        }

        const validadorBuffer = exists.recordset[0].validador;

        // Obtener valores por defecto de los catálogos para evitar errores de FKs
        const [resCta, resSeg, resZon, resVen, resTip, resMon, resCond] = await Promise.all([
            pool.request().query('SELECT TOP 1 RTRIM(co_cta_ingr_egr) as id FROM saCuentaIngEgr'),
            pool.request().query('SELECT TOP 1 RTRIM(co_seg) as id FROM saSegmento'),
            pool.request().query('SELECT TOP 1 RTRIM(co_zon) as id FROM saZona'),
            pool.request().query('SELECT TOP 1 RTRIM(co_ven) as id FROM saVendedor'),
            pool.request().query('SELECT TOP 1 RTRIM(tip_cli) as id FROM saTipoCliente'),
            pool.request().query('SELECT TOP 1 RTRIM(co_mone) as id FROM saMoneda'),
            pool.request().query('SELECT TOP 1 RTRIM(co_cond) as id FROM saCondicionPago')
        ]);

        const defCta = resCta.recordset.length > 0 ? resCta.recordset[0].id : null;
        const defSeg = resSeg.recordset.length > 0 ? resSeg.recordset[0].id : null;
        const defZon = resZon.recordset.length > 0 ? resZon.recordset[0].id : null;
        const defVen = resVen.recordset.length > 0 ? resVen.recordset[0].id : null;
        const defTip = resTip.recordset.length > 0 ? resTip.recordset[0].id : null;
        const defMon = resMon.recordset.length > 0 ? resMon.recordset[0].id : null;
        const defCond = resCond.recordset.length > 0 ? resCond.recordset[0].id : null;

        const request = new sql.Request(pool);
        // Si mandan un nuevo co_cli en el body, lo usamos como sCo_Cli (nuevo código), sino usamos el original de la URL.
        const nuevoCoCli = data.co_cli ? data.co_cli : coCli;
        request.input('sCo_Cli', sql.Char(16), nuevoCoCli);
        request.input('sCo_CliOri', sql.Char(16), coCli);
        request.input('sLogin', sql.Char(20), '');
        request.input('sPassword', sql.Char(20), '');
        request.input('sSalesTax', sql.Char(8), null);
        request.input('sCli_Des', sql.VarChar(60), data.cli_des || 'Cliente Editado API');
        request.input('sCo_seg', sql.Char(6), data.co_seg || defSeg);
        request.input('sCo_zon', sql.Char(6), data.co_zon || defZon);
        request.input('sCo_Ven', sql.Char(6), data.co_ven || defVen);
        request.input('sEstado', sql.Char(1), '1');
        request.input('bInactivo', sql.Bit, 0);
        request.input('bValido', sql.Bit, 0);
        request.input('bSinCredito', sql.Bit, 0);
        request.input('bLunes', sql.Bit, 1);
        request.input('bMartes', sql.Bit, 1);
        request.input('bMiercoles', sql.Bit, 1);
        request.input('bJueves', sql.Bit, 1);
        request.input('bViernes', sql.Bit, 1);
        request.input('bSabado', sql.Bit, 1);
        request.input('bDomingo', sql.Bit, 1);
        request.input('sDirec1', sql.VarChar(sql.MAX), data.direc1 || '');
        request.input('sDirec2', sql.VarChar(sql.MAX), '');
        request.input('sDir_Ent2', sql.VarChar(sql.MAX), '');
        request.input('sHorar_Caja', sql.VarChar(sql.MAX), '');
        request.input('sFrecu_Vist', sql.VarChar(sql.MAX), '');
        request.input('sTelefonos', sql.VarChar(60), data.telefonos || '');
        request.input('sFax', sql.VarChar(60), '');
        request.input('sRespons', sql.VarChar(60), '');
        request.input('sdFecha_reg', sql.SmallDateTime, new Date());
        request.input('sTip_Cli', sql.Char(6), data.tip_cli || defTip);
        request.input('sSerialP', sql.Char(30), null);
        request.input('iPuntaje', sql.Int, 0);
        request.input('iId', sql.Int, 0);
        request.input('deMont_cre', sql.Decimal(18, 5), 0);
        request.input('sCo_Mone', sql.Char(6), data.co_mone || defMon);
        request.input('sCond_Pag', sql.Char(6), data.co_cond || defCond);
        request.input('iPlaz_pag', sql.Int, 0);
        request.input('deDesc_ppago', sql.Decimal(18, 5), 0);
        request.input('deDesc_Glob', sql.Decimal(18, 5), 0);
        request.input('sRif', sql.VarChar(18), data.rif || '');
        request.input('bContrib', sql.Bit, 1);
        request.input('sDis_cen', sql.VarChar(sql.MAX), '');
        request.input('sNit', sql.VarChar(18), '');
        request.input('sEmail', sql.VarChar(60), data.email || '');
        request.input('sCo_Cta_Ingr_Egr', sql.Char(20), data.co_cta_ingr_egr || defCta);
        request.input('sComentario', sql.VarChar(sql.MAX), 'Editado vía API');
        request.input('sCampo1', sql.VarChar(60), '');
        request.input('sCampo2', sql.VarChar(60), '');
        request.input('sCampo3', sql.VarChar(60), '');
        request.input('sCampo4', sql.VarChar(60), '');
        request.input('sCampo5', sql.VarChar(60), '');
        request.input('sCampo6', sql.VarChar(60), '');
        request.input('sCampo7', sql.VarChar(60), '');
        request.input('sCampo8', sql.VarChar(60), '');
        request.input('sCo_us_mo', sql.Char(6), '999');
        request.input('sCo_Sucu_Mo', sql.Char(6), '01');
        request.input('sMaquina', sql.VarChar(60), 'SYNC2K');
        request.input('sCampos', sql.VarChar(sql.MAX), null);
        request.input('sRevisado', sql.Char(1), '0');
        request.input('sTrasnfe', sql.Char(1), '0');
        request.input('bJuridico', sql.Bit, 0);
        request.input('iTipo_Adi', sql.Int, 1);
        request.input('sMatriz', sql.Char(16), null);
        request.input('sCo_Tab', sql.Char(6), null);
        request.input('sTipo_Per', sql.Char(1), '0');
        request.input('sCo_pais', sql.VarChar(6), null);
        request.input('sCiudad', sql.VarChar(50), '');
        request.input('sZip', sql.VarChar(20), '');
        request.input('sWebSite', sql.VarChar(200), '');
        request.input('bContribu_E', sql.Bit, 0);
        request.input('bRete_Regis_Doc', sql.Bit, 0);
        request.input('dePorc_Esp', sql.Decimal(18, 5), 0);
        request.input('tsValidador', sql.VarBinary, validadorBuffer);
        request.input('gRowguid', sql.UniqueIdentifier, null);
        request.input('sN_cr', sql.Char(6), null);
        request.input('sN_db', sql.Char(6), null);
        request.input('sTComp', sql.Char(6), null);
        request.input('sEmail_alterno', sql.VarChar(60), '');

        await request.execute('pActualizarCliente');

        res.status(200).json({ success: true, message: 'Cliente actualizado correctamente.', co_cli: nuevoCoCli });

    } catch (error) {
        console.error('Error al actualizar cliente:', error);
        res.status(500).json({ success: false, message: 'Error en base de datos.', error: error.message });
    }
});

/**
 * 5. Endpoint: Eliminar Cliente
 * DELETE /api/v1/clientes/:co_cli
 * 
 * Ejemplo de uso:
 * DELETE /api/v1/clientes/0002
 * Headers: { "x-api-key": "mi-clave-secreta" }
 */
router.delete('/:co_cli', async (req, res) => {
    try {
        const coCli = req.params.co_cli;
        const pool = await getPool();

        // 1. Verificar existencia y extraer validador
        const queryCheck = `SELECT RTRIM(co_cli) as co_cli, validador FROM saCliente WHERE RTRIM(co_cli) = LTRIM(RTRIM(@co_cli))`;
        const checkReq = pool.request();
        checkReq.input('co_cli', sql.VarChar, coCli);
        const exists = await checkReq.query(queryCheck);

        if (exists.recordset.length === 0) {
            return res.status(404).json({ success: false, message: 'El cliente especificado no existe o ya fue eliminado.' });
        }

        const validadorBuffer = exists.recordset[0].validador;

        // 2. Ejecutar Store Procedure con validador
        const request = new sql.Request(pool);
        request.input('sCo_CliOri', sql.Char(16), coCli);
        request.input('tsValidador', sql.VarBinary, validadorBuffer);
        request.input('sMaquina', sql.VarChar(60), 'SYNC2K');
        request.input('sCo_Us_Mo', sql.Char(6), '999');
        request.input('sCo_Sucu_Mo', sql.Char(6), '01');
        request.input('gRowguid', sql.UniqueIdentifier, null);

        await request.execute('pEliminarCliente');

        res.status(200).json({ success: true, message: 'Cliente eliminado correctamente.', co_cli: coCli });

    } catch (error) {
        console.error('Error al eliminar cliente:', error);
        res.status(500).json({ success: false, message: 'No se pudo eliminar el cliente. Es posible que tenga documentos asociados (pedidos, cobros).', error: error.message });
    }
});

module.exports = router;
