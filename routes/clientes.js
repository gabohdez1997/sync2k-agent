const express = require('express');
const router = express.Router();
const { sql, getPool, getServers } = require('../db');
const { executeWrite, writeResponse, paginatedResponse } = require('../helpers/multiSede');

// ── Helper: inputs del STORED PROCEDURE pInsertarCliente ───────────────────
function bindClienteInsert(r, data, defaults, ts = new Date(), auditUser = '999') {
    const d = defaults;
    r.input('sCo_Cli',           sql.Char(16),         data.co_cli);
    r.input('sLogin',            sql.Char(20),          '');
    r.input('sPassword',         sql.Char(20),          '');
    r.input('sSalesTax',         sql.Char(8),           null);
    r.input('sCli_Des',          sql.VarChar(60),       data.cli_des || data.descripcion);
    r.input('sCo_Seg',           sql.Char(6),           '01');
    r.input('sCo_Zon',           sql.Char(6),           data.co_zon   || d.co_zon);
    r.input('sCo_Ven',           sql.VarChar(10),       data.co_ven || d.co_ven || '01');
    r.input('sEstado',           sql.Char(1),           '1');
    r.input('bInactivo',         sql.Bit,               0);
    r.input('bValido',           sql.Bit,               0);
    r.input('bSinCredito',       sql.Bit,               0);
    r.input('bLunes',            sql.Bit,               0);
    r.input('bMartes',           sql.Bit,               0);
    r.input('bMiercoles',        sql.Bit,               0);
    r.input('bJueves',           sql.Bit,               0);
    r.input('bViernes',          sql.Bit,               0);
    r.input('bSabado',           sql.Bit,               0);
    r.input('bDomingo',          sql.Bit,               0);
    r.input('sDirec1',           sql.VarChar(sql.MAX),  data.direc1 || '');
    r.input('sDirec2',           sql.VarChar(sql.MAX),  '');
    r.input('sDir_Ent2',         sql.VarChar(sql.MAX),  '');
    r.input('sHorar_Caja',       sql.VarChar(sql.MAX),  '');
    r.input('sFrecu_Vist',       sql.VarChar(sql.MAX),  '');
    r.input('sTelefonos',        sql.VarChar(60),       data.telefonos || '');
    r.input('sFax',              sql.VarChar(60),       '');
    r.input('sRespons',          sql.VarChar(60),       '');
    r.input('sdFecha_Reg',       sql.SmallDateTime,     ts);
    r.input('sTip_Cli',          sql.Char(6),           data.tip_cli  || d.tip_cli);
    r.input('sSerialP',          sql.Char(30),          '');
    r.input('iPuntaje',          sql.Int,               0);
    r.input('iId',               sql.Int,               0);
    r.input('deMont_Cre',        sql.Decimal(18,5),     0);
    r.input('sCo_Mone',          sql.Char(6),           data.co_mone  || d.co_mone);
    r.input('sCond_Pag',         sql.Char(6),           '01');
    r.input('iPlaz_pag',         sql.Int,               0);
    r.input('deDesc_ppago',      sql.Decimal(18,5),     0);
    r.input('deDesc_Glob',       sql.Decimal(18,5),     0);
    r.input('sTipo_Iva',         sql.Char(1),           '1');
    r.input('deIva',             sql.Decimal(18,5),     0);
    r.input('sRif',              sql.VarChar(18),       data.rif || '');
    r.input('bContrib',          sql.Bit,               data.contribuyente === false ? 0 : 1);
    r.input('sDis_cen',          sql.VarChar(sql.MAX),  '');
    r.input('sNit',              sql.VarChar(18),       '');
    r.input('sEmail',            sql.VarChar(60),       data.email || '');
    r.input('sCo_Cta_Ingr_Egr', sql.Char(20),          '01');
    r.input('sComentario',       sql.VarChar(sql.MAX),  'Creado vía API');
    r.input('sCampo1',           sql.VarChar(60),       '');
    r.input('sCampo2',           sql.VarChar(60),       '');
    r.input('sCampo3',           sql.VarChar(60),       '');
    r.input('sCampo4',           sql.VarChar(60),       '');
    r.input('sCampo5',           sql.VarChar(60),       '');
    r.input('sCampo6',           sql.VarChar(60),       '');
    r.input('sCampo7',           sql.VarChar(60),       '');
    r.input('sCampo8',           sql.VarChar(60),       '');
    r.input('sCo_Us_In',         sql.VarChar(10),       auditUser);
    r.input('sMaquina',          sql.VarChar(60),       'SYNC2K');
    r.input('sRevisado',         sql.Char(1),           '0');
    r.input('sTrasnfe',          sql.Char(1),           '0');
    r.input('sCo_Sucu_In',       sql.Char(6),           '01');
    r.input('bJuridico',         sql.Bit,               0);
    r.input('iTipo_Adi',         sql.Int,               1);
    r.input('sMatriz',           sql.Char(16),          null);
    r.input('sCo_Tab',           sql.Char(6),           null);
    r.input('sTipo_Per',         sql.Char(1),           data.tipo_per || '1');
    r.input('sCo_pais',          sql.Char(6),           'VE');
    r.input('sCiudad',           sql.VarChar(50),       '');
    r.input('sZip',              sql.VarChar(20),       '');
    r.input('sWebSite',          sql.VarChar(200),      '');
    r.input('bContribu_E',       sql.Bit,               data.contribu_e ? 1 : 0);
    r.input('bRete_Regis_Doc',   sql.Bit,               0);
    r.input('dePorc_Esp',        sql.Decimal(18,5),     data.porc_esp || 0);
    r.input('sN_cr',             sql.Char(6),           null);
    r.input('sN_db',             sql.Char(6),           null);
    r.input('sTComp',            sql.Char(6),           null);
    r.input('sEmail_alterno',    sql.VarChar(60),       '');
}

// ── Helper: inputs del STORED PROCEDURE pActualizarCliente ──────────────────
function bindClienteUpdate(r, data, row, ts = new Date(), auditUser = '999') {
    r.input('sCo_Cli',           sql.Char(16),         data.co_cli || row.co_cli);
    r.input('sCo_CliOri',        sql.Char(16),         row.co_cli);
    r.input('sLogin',            sql.Char(20),          '');
    r.input('sPassword',         sql.Char(20),          '');
    r.input('sSalesTax',         sql.Char(8),           null);
    r.input('sCli_Des',          sql.VarChar(60),       data.cli_des || data.descripcion || row.cli_des);
    r.input('sCo_Seg',           sql.Char(6),           '01');
    r.input('sCo_Zon',           sql.Char(6),           data.co_zon   || row.co_zon);
    r.input('sCo_Ven',           sql.VarChar(10),       data.co_ven || row.co_ven || '01');
    r.input('sEstado',           sql.Char(1),           '1');
    r.input('bInactivo',         sql.Bit,               0);
    r.input('bValido',           sql.Bit,               0);
    r.input('bSinCredito',       sql.Bit,               0);
    r.input('bLunes',            sql.Bit,               0);
    r.input('bMartes',           sql.Bit,               0);
    r.input('bMiercoles',        sql.Bit,               0);
    r.input('bJueves',           sql.Bit,               0);
    r.input('bViernes',          sql.Bit,               0);
    r.input('bSabado',           sql.Bit,               0);
    r.input('bDomingo',          sql.Bit,               0);
    r.input('sDirec1',           sql.VarChar(sql.MAX),  data.direc1 ?? row.direc1 ?? '');
    r.input('sDirec2',           sql.VarChar(sql.MAX),  '');
    r.input('sDir_Ent2',         sql.VarChar(sql.MAX),  '');
    r.input('sHorar_Caja',       sql.VarChar(sql.MAX),  '');
    r.input('sFrecu_Vist',       sql.VarChar(sql.MAX),  '');
    r.input('sTelefonos',        sql.VarChar(60),       data.telefonos ?? row.telefonos ?? '');
    r.input('sFax',              sql.VarChar(60),       '');
    r.input('sRespons',          sql.VarChar(60),       '');
    r.input('sdFecha_reg',       sql.SmallDateTime,     ts);
    r.input('sTip_Cli',          sql.Char(6),           data.tip_cli  || row.tip_cli);
    r.input('sSerialP',          sql.Char(30),          null);
    r.input('iPuntaje',          sql.Int,               0);
    r.input('iId',               sql.Int,               0);
    r.input('deMont_cre',        sql.Decimal(18,5),     0);
    r.input('sCo_Mone',          sql.Char(6),           data.co_mone  || row.co_mone);
    r.input('sCond_Pag',         sql.Char(6),           '01');
    r.input('iPlaz_pag',         sql.Int,               0);
    r.input('deDesc_ppago',      sql.Decimal(18,5),     0);
    r.input('deDesc_Glob',       sql.Decimal(18,5),     0);
    r.input('sRif',              sql.VarChar(18),       data.rif ?? row.rif ?? '');
    // bContrib: convertir el booleano correctamente (true→1, false→0)
    r.input('bContrib',          sql.Bit,               data.contribuyente === true || data.contribuyente === 'true' ? 1 : 0);
    r.input('sDis_cen',          sql.VarChar(sql.MAX),  '');
    r.input('sNit',              sql.VarChar(18),       '');
    r.input('sEmail',            sql.VarChar(60),       data.email || '');
    r.input('sCo_Cta_Ingr_Egr',  sql.Char(20),          '01');
    r.input('sComentario',       sql.VarChar(sql.MAX),  'Editado vía API');
    r.input('sCampo1',           sql.VarChar(60),       '');
    r.input('sCampo2',           sql.VarChar(60),       '');
    r.input('sCampo3',           sql.VarChar(60),       '');
    r.input('sCampo4',           sql.VarChar(60),       '');
    r.input('sCampo5',           sql.VarChar(60),       '');
    r.input('sCampo6',           sql.VarChar(60),       '');
    r.input('sCampo7',           sql.VarChar(60),       '');
    r.input('sCampo8',           sql.VarChar(60),       '');
    r.input('sCo_Us_Mo',         sql.VarChar(10),       auditUser);
    r.input('sCo_Sucu_Mo',       sql.Char(6),           '01');
    r.input('sMaquina',          sql.VarChar(60),       'SYNC2K');
    r.input('sCampos',           sql.VarChar(sql.MAX),  null);
    r.input('sRevisado',         sql.Char(1),           '0');
    r.input('sTrasnfe',          sql.Char(1),           '0');
    r.input('bJuridico',         sql.Bit,               0);
    r.input('iTipo_Adi',         sql.Int,               1);
    r.input('sMatriz',           sql.Char(16),          null);
    r.input('sCo_Tab',           sql.Char(6),           null);
    r.input('sTipo_Per',         sql.Char(1),           data.tipo_per || row.tipo_per || '1');
    r.input('sCo_pais',          sql.VarChar(6),        'VE');
    r.input('sCiudad',           sql.VarChar(50),       '');
    r.input('sZip',              sql.VarChar(20),       '');
    r.input('sWebSite',          sql.VarChar(200),      '');
    r.input('bContribu_E',       sql.Bit,               data.contribu_e === true || data.contribu_e === 'true' ? 1 : 0);
    r.input('bRete_Regis_Doc',   sql.Bit,               0);
    r.input('dePorc_Esp',        sql.Decimal(18,5),     Number(data.porc_esp) || 0);
    r.input('tsValidador',       sql.VarBinary(8),      row.validador);
    r.input('gRowguid',          sql.UniqueIdentifier,  null);
    r.input('sN_cr',             sql.Char(6),           null);
    r.input('sN_db',             sql.Char(6),           null);
    r.input('sTComp',            sql.Char(6),           null);
    r.input('sEmail_alterno',    sql.VarChar(60),       '');
}

// ── Helper: carga defaults de FK ───────────────────────────────────────────
async function loadDefaults(pool) {
    const [cta, seg, zon, ven, tip, mon, cond] = await Promise.all([
        pool.request().query('SELECT TOP 1 RTRIM(co_cta_ingr_egr) AS id FROM saCuentaIngEgr'),
        pool.request().query('SELECT TOP 1 RTRIM(co_seg) AS id FROM saSegmento'),
        pool.request().query('SELECT TOP 1 RTRIM(co_zon) AS id FROM saZona'),
        pool.request().query('SELECT TOP 1 RTRIM(co_ven) AS id FROM saVendedor'),
        pool.request().query('SELECT TOP 1 RTRIM(tip_cli) AS id FROM saTipoCliente'),
        pool.request().query('SELECT TOP 1 RTRIM(co_mone) AS id FROM saMoneda ORDER BY CASE WHEN co_mone IN (\'BS\',\'VES\',\'BSF\') THEN 0 ELSE 1 END, co_mone'),
        pool.request().query('SELECT TOP 1 RTRIM(co_cond) AS id FROM saCondicionPago')
    ]);
    return {
        co_cta:  cta.recordset[0]?.id || '01',
        co_seg:  seg.recordset[0]?.id || '01',
        co_zon:  zon.recordset[0]?.id || '01',
        co_ven:  ven.recordset[0]?.id || '01',
        tip_cli: tip.recordset[0]?.id || '01',
        co_mone: mon.recordset[0]?.id || '01',
        co_cond: cond.recordset[0]?.id || '01'
    };
}

/**
 * Valida que el usuario esté registrado como vendedor en saVendedor.
 * Si existe, retorna su código. Si NO existe, lanza un error que bloquea la operación.
 * Esto garantiza integridad: toda modificación de clientes debe ser trazable a un vendedor válido.
 */
async function requireCoVen(pool, profitUser) {
    if (!profitUser) {
        throw new Error('No se recibió código de usuario (x-profit-user). Inicie sesión nuevamente.');
    }
    const check = await pool.request()
        .input('co_ven', sql.VarChar, profitUser)
        .query("SELECT 1 FROM saVendedor WHERE RTRIM(co_ven) = @co_ven");
    if (check.recordset.length === 0) {
        throw new Error(`El usuario "${profitUser}" no está registrado como vendedor en Profit Plus. Contacte al administrador para que lo agregue a la tabla saVendedor.`);
    }
    console.log(`[requireCoVen] ✅ Usuario "${profitUser}" es vendedor válido.`);
    return profitUser;
}

/**
 * @swagger
 * tags:
 *   name: Clientes
 *   description: Gestión de clientes (Rif, Direcciones, etc.)
 */

// ────────────────────────────────────────────────────────────────────────────
// 1. GET /api/v1/clientes — Listado paginado desde todas las sedes
// ────────────────────────────────────────────────────────────────────────────
/**
 * @swagger
 * /api/v1/clientes:
 *   get:
 *     summary: Obtener listado paginado de clientes de todas las sedes
 *     tags: [Clientes]
 *     parameters:
 *       - in: query
 *         name: page
 *         schema: { type: integer, default: 1 }
 *       - in: query
 *         name: limit
 *         schema: { type: integer, default: 10 }
 *       - in: query
 *         name: co_ven
 *         schema: { type: string }
 *     responses:
 *       200:
 *         description: Listado de clientes
 */
router.get('/', async (req, res) => {
    try {
        const page  = parseInt(req.query.page)  || 1;
        const limit = parseInt(req.query.limit) || 10;
        let servers = getServers();
        if (req.query.sede_id) servers = servers.filter(s => s.id === req.query.sede_id);
        else if (req.query.sede) servers = servers.filter(s => s.id === req.query.sede);

        const offset = (page - 1) * limit;
        let globalTotal = 0;

        const allData = await Promise.all(servers.map(async (srv) => {
            try {
                const pool = await getPool(srv.id, req.sqlAuth);
                const co_ven = req.query.co_ven;
                
                let whereSQL = "WHERE cli.inactivo = 0 ";
                const request = pool.request();
                
                if (co_ven) {
                    request.input('co_ven_filter', sql.VarChar, co_ven.trim().toUpperCase());
                    whereSQL += " AND LTRIM(RTRIM(cli.co_ven)) = @co_ven_filter ";
                }

                // Fetch Total Count
                const countRes = await request.query(`SELECT COUNT(*) AS total FROM saCliente cli ${whereSQL}`);
                globalTotal += countRes.recordset[0].total;

                // Fetch Paginated Chunk
                request.input('offset', sql.Int, offset);
                request.input('limit', sql.Int, limit);
                
                const result = await request.query(
                    `SELECT RTRIM(cli.co_cli) AS co_cli, RTRIM(cli.cli_des) AS descripcion,
                            RTRIM(cli.rif) AS rif, RTRIM(cli.direc1) AS direc1,
                            RTRIM(cli.telefonos) AS telefonos, RTRIM(cli.email) AS email,
                            RTRIM(cli.co_zon) AS co_zon, RTRIM(z.zon_des) AS zon_des,
                            cli.contrib, RTRIM(cli.tipo_per) AS tipo_per, cli.contribu_e, cli.porc_esp,
                            RTRIM(cli.co_ven) AS co_ven, RTRIM(v.ven_des) AS ven_des
                     FROM saCliente cli
                     LEFT JOIN saVendedor v ON cli.co_ven = v.co_ven
                     LEFT JOIN saZona z ON cli.co_zon = z.co_zon
                     ${whereSQL} 
                     ORDER BY cli.cli_des
                     OFFSET @offset ROWS FETCH NEXT @limit ROWS ONLY`
                );
                return result.recordset.map(c => ({ ...c, sede_id: srv.id, sede_nombre: srv.name }));
            } catch (e) { return []; }
        }));

        const combined = [].concat(...allData);
        combined.sort((a, b) => a.descripcion.localeCompare(b.descripcion));
        
        // Si hay varios servidores, el combine superará el límite, lo re-recortamos a la página final.
        const items = combined.slice(0, limit);

        return res.json({
            success: true,
            page,
            limit,
            total_items: globalTotal,
            total_pages: Math.ceil(globalTotal / limit),
            data: items
        });
    } catch (error) {
        res.status(500).json({ success: false, message: 'Error interno.', error: error.message });
    }
});

// ────────────────────────────────────────────────────────────────────────────
// 2. GET /api/v1/clientes/search — Búsqueda con filtros
// ────────────────────────────────────────────────────────────────────────────
router.get('/search', async (req, res) => {
    try {
        const page  = parseInt(req.query.page)  || 1;
        const limit = parseInt(req.query.limit) || 30;

        const FIELD_MAP = {
            co_cli: 'co_cli', descripcion: 'cli_des', rif: 'rif',
            direccion: 'direc1', telefonos: 'telefonos', email: 'email',
            co_ven: 'co_ven'
        };

        const filters = Object.entries(req.query)
            .filter(([k, v]) => FIELD_MAP[k] && v)
            .map(([k, v]) => ({ param: k, column: FIELD_MAP[k], value: v }));

        if (!filters.length)
            return res.status(400).json({ success: false, message: 'Especifique al menos un parámetro de búsqueda.' });

        const whereClause = 'WHERE cli.inactivo = 0 ' + filters.map(f => {
            if (f.param === 'co_ven') return `AND LTRIM(RTRIM(cli.${f.column})) = @${f.param}`;
            return `AND cli.${f.column} LIKE '%' + @${f.param} + '%'`;
        }).join(' ');
        let servers = getServers();
        if (req.query.sede_id) servers = servers.filter(s => s.id === req.query.sede_id);
        else if (req.query.sede) servers = servers.filter(s => s.id === req.query.sede);

        const offset = (page - 1) * limit;
        let globalTotal = 0;

        const allData = await Promise.all(servers.map(async (srv) => {
            try {
                const pool = await getPool(srv.id, req.sqlAuth);
                
                // Fetch Total Count
                const countReq = pool.request();
                filters.forEach(f => countReq.input(f.param, sql.VarChar, f.value));
                const countRes = await countReq.query(`SELECT COUNT(*) AS total FROM saCliente cli LEFT JOIN saVendedor v ON cli.co_ven = v.co_ven LEFT JOIN saZona z ON cli.co_zon = z.co_zon ${whereClause}`);
                globalTotal += countRes.recordset[0].total;

                // Fetch Paginated Chunk
                const r = pool.request();
                filters.forEach(f => r.input(f.param, sql.VarChar, f.value));
                r.input('offset', sql.Int, offset);
                r.input('limit', sql.Int, limit);
                
                const result = await r.query(
                    `SELECT RTRIM(cli.co_cli) AS co_cli, RTRIM(cli.cli_des) AS descripcion,
                            RTRIM(cli.rif) AS rif, RTRIM(cli.direc1) AS direc1,
                            RTRIM(cli.telefonos) AS telefonos, RTRIM(cli.email) AS email,
                            RTRIM(cli.co_zon) AS co_zon, RTRIM(z.zon_des) AS zon_des,
                            cli.contrib, RTRIM(cli.tipo_per) AS tipo_per, cli.contribu_e, cli.porc_esp,
                            RTRIM(cli.co_ven) AS co_ven, RTRIM(v.ven_des) AS ven_des
                     FROM saCliente cli
                     LEFT JOIN saVendedor v ON cli.co_ven = v.co_ven
                     LEFT JOIN saZona z ON cli.co_zon = z.co_zon
                     ${whereClause} 
                     ORDER BY cli.cli_des
                     OFFSET @offset ROWS FETCH NEXT @limit ROWS ONLY`
                );
                return result.recordset.map(c => ({ ...c, sede_id: srv.id, sede_nombre: srv.name }));
            } catch (e) { return []; }
        }));

        const combined = [].concat(...allData);
        combined.sort((a, b) => a.descripcion.localeCompare(b.descripcion));
        
        const items = combined.slice(0, limit);

        return res.json({
            success: true,
            page,
            limit,
            total_items: globalTotal,
            total_pages: Math.ceil(globalTotal / limit),
            data: items
        });
    } catch (error) {
        res.status(500).json({ success: false, message: 'Error en búsqueda.', error: error.message });
    }
});

// ────────────────────────────────────────────────────────────────────────────
// 3. GET /api/v1/clientes/:co_cli — Detalle del cliente desde todas las sedes
// ────────────────────────────────────────────────────────────────────────────
/**
 * @swagger
 * /api/v1/clientes/{co_cli}:
 *   get:
 *     summary: Obtener detalle de un cliente específico por su código
 *     tags: [Clientes]
 *     parameters:
 *       - in: path
 *         name: co_cli
 *         required: true
 *         schema: { type: string }
 *     responses:
 *       200:
 *         description: Detalle del cliente
 *       404:
 *         description: Cliente no encontrado
 */
router.get('/:co_cli', async (req, res) => {
    try {
        const { co_cli } = req.params;
        const servers = getServers();

        const results = await Promise.all(servers.map(async (srv) => {
            try {
                const pool = await getPool(srv.id, req.sqlAuth);
                const result = await pool.request().input('co_cli', sql.VarChar, co_cli).query(
                    `SELECT RTRIM(cli.co_cli) AS co_cli, RTRIM(cli.cli_des) AS descripcion,
                            RTRIM(cli.rif) AS rif, RTRIM(cli.direc1) AS direc1,
                            RTRIM(cli.telefonos) AS telefonos, RTRIM(cli.email) AS email,
                            RTRIM(cli.co_ven) AS co_ven, RTRIM(v.ven_des) AS ven_des,
                            RTRIM(cli.co_zon) AS co_zon, RTRIM(z.zon_des) AS zon_des,
                            RTRIM(cli.co_seg) AS co_seg, cli.inactivo, cli.contrib,
                            RTRIM(cli.tipo_per) AS tipo_per, cli.contribu_e, cli.porc_esp
                     FROM saCliente cli
                     LEFT JOIN saVendedor v ON cli.co_ven = v.co_ven
                     LEFT JOIN saZona z ON cli.co_zon = z.co_zon
                     WHERE LTRIM(RTRIM(cli.co_cli)) = LTRIM(RTRIM(@co_cli))`
                );
                if (!result.recordset.length) return null;
                return { ...result.recordset[0], sede_id: srv.id, sede_nombre: srv.name };
            } catch (e) { return { sede_id: srv.id, sede_nombre: srv.name, error: e.message }; }
        }));

        const found = results.filter(r => r && !r.error);
        if (!found.length)
            return res.status(404).json({ success: false, message: 'Cliente no encontrado en ninguna sede.' });

        res.status(200).json({ success: true, count: found.length, data: results.filter(r => r !== null) });
    } catch (error) {
        res.status(500).json({ success: false, message: 'Error interno.', error: error.message });
    }
});

// ────────────────────────────────────────────────────────────────────────────
// 4. POST /api/v1/clientes — Crear cliente (targeted o broadcast)
// ────────────────────────────────────────────────────────────────────────────
router.post('/', async (req, res) => {
    try {
        const data = req.body;
        if (!data.co_cli || (!data.cli_des && !data.descripcion))
            return res.status(400).json({ success: false, message: 'Campos obligatorios: co_cli, descripcion' });

        const outcome = await executeWrite(req.query.sede || null, req.sqlAuth, async (pool) => {
            const defaults  = await loadDefaults(pool);
            const r         = new sql.Request(pool);
            let   auditUser = (req.profitUser || req.sqlAuth?.user || '01').substring(0, 10).toUpperCase();
            
            // Validar que el usuario sea vendedor registrado (obligatorio por FK)
            data.co_ven = await requireCoVen(pool, auditUser);
            
            bindClienteInsert(r, data, defaults, new Date(), auditUser);
            await r.execute('pInsertarCliente');
        });

        return writeResponse(res, outcome, `Sede "${req.query.sede}" no encontrada.`);
    } catch (error) {
        console.error('[CLIENTES POST FATAL ERROR]:', error);
        res.status(500).json({ 
            success: false, 
            message: 'Error al procesar la creación del cliente.', 
            error: error.message || String(error)
        });
    }
});

// ────────────────────────────────────────────────────────────────────────────
// 5. PUT /api/v1/clientes/:co_cli — Actualizar cliente (targeted o broadcast)
// ────────────────────────────────────────────────────────────────────────────
router.put('/:co_cli', async (req, res) => {
    try {
        const { co_cli } = req.params;
        const data = req.body;

        const outcome = await executeWrite(req.query.sede || null, req.sqlAuth, async (pool) => {
            const check = await pool.request().input('co_cli', sql.VarChar, co_cli).query(
                `SELECT validador,
                        RTRIM(co_cli)           AS co_cli,
                        RTRIM(cli_des)          AS cli_des, 
                        RTRIM(co_seg)           AS co_seg,
                        RTRIM(co_zon)           AS co_zon,
                        RTRIM(co_ven)           AS co_ven,
                        RTRIM(tip_cli)          AS tip_cli,
                        RTRIM(co_mone)          AS co_mone,
                        RTRIM(cond_pag)         AS cond_pag,
                        RTRIM(co_cta_ingr_egr)  AS co_cta_ingr_egr,
                        RTRIM(tipo_per)         AS tipo_per,
                        RTRIM(direc1)           AS direc1,
                        RTRIM(telefonos)        AS telefonos,
                        RTRIM(rif)              AS rif,
                        contrib,
                        contribu_e
                 FROM saCliente WHERE LTRIM(RTRIM(co_cli)) = LTRIM(RTRIM(@co_cli))`
            );
            if (!check.recordset.length) throw new Error('El cliente no existe en esta sede.');

            const row = check.recordset[0];

            // Siempre cargar defaults reales de FK para garantizar valores válidos
            const defaults = await loadDefaults(pool);
            row.co_mone = row.co_mone || defaults.co_mone;
            row.tip_cli = row.tip_cli || defaults.tip_cli;
            row.co_zon  = row.co_zon  || defaults.co_zon;
            row.co_ven  = row.co_ven  || defaults.co_ven;

            const r   = new sql.Request(pool);
            const auditUser = (req.profitUser || req.sqlAuth?.user || '01').substring(0, 10).toUpperCase();
            
            // Validar que el usuario sea vendedor registrado (obligatorio por FK)
            data.co_ven = await requireCoVen(pool, auditUser);
            
            // ── DIAGNÓSTICO COMPLETO DEL PAYLOAD ────────────────────────────
            console.log('[PUT DEBUG] ══ INICIO ACTUALIZACIÓN CLIENTE ══');
            console.log('[PUT DEBUG] co_cli:', co_cli);
            console.log('[PUT DEBUG] row.co_ven:', JSON.stringify(row.co_ven), '| defaults.co_ven:', JSON.stringify(defaults.co_ven));
            console.log('[PUT DEBUG] validador:', Buffer.isBuffer(row.validador) ? `Buffer(${row.validador.length})` : row.validador);
            console.log('[PUT DEBUG] PAYLOAD COMPLETO (data):', JSON.stringify({
                cli_des:      data.cli_des || data.descripcion,
                rif:          data.rif,
                telefonos:    data.telefonos,
                email:        data.email,
                direc1:       data.direc1,
                co_zon:       data.co_zon,
                co_ven:       data.co_ven,
                contribuyente: data.contribuyente,
                contribu_e:   data.contribu_e,
                tipo_per:     data.tipo_per,
                porc_esp:     data.porc_esp,
                co_mone:      data.co_mone,
                tip_cli:      data.tip_cli,
            }));
            console.log('[PUT DEBUG] VALORES RESUELTOS (data vs row):', JSON.stringify({
                sCo_Ven:    data.co_ven   || row.co_ven,
                sCo_Zon:    data.co_zon   || row.co_zon,
                bContrib:   data.contribuyente === false ? 0 : 1,
                sTipo_Per:  data.tipo_per  || row.tipo_per || '1',
                bContribu_E: data.contribu_e ? 1 : 0,
                dePorc_Esp: data.porc_esp  || 0,
                sCo_Mone:   data.co_mone  || row.co_mone,
                sTip_Cli:   data.tip_cli  || row.tip_cli,
            }));

            bindClienteUpdate(r, data, row, new Date(), auditUser);
            const spResult = await r.execute('pActualizarCliente');
            
            console.log('[PUT DEBUG] SP returnValue:', spResult.returnValue);
            console.log('[PUT DEBUG] SP output:', spResult.output);
            console.log('[PUT DEBUG] SP recordsets count:', spResult.recordsets?.length);
            if (spResult.recordsets?.length) {
                spResult.recordsets.forEach((rs, i) => console.log(`[PUT DEBUG] recordset[${i}]:`, JSON.stringify(rs)));
            }

            // Validar returnValue del SP (0 = éxito, otro = falla)
            if (spResult.returnValue !== 0 && spResult.returnValue !== undefined) {
                throw new Error(`pActualizarCliente retornó código ${spResult.returnValue}. Verifique los datos enviados.`);
            }
        });

        return writeResponse(res, outcome, `Sede "${req.query.sede}" no encontrada.`);
    } catch (error) {
        console.error('[CLIENTES PUT FATAL ERROR]:', error);
        res.status(500).json({ 
            success: false, 
            message: 'Error al procesar la actualización del cliente.', 
            error: error.message || String(error)
        });
    }
});

// ────────────────────────────────────────────────────────────────────────────
// 6. DELETE /api/v1/clientes/:co_cli — Eliminar cliente (targeted o broadcast)
// ────────────────────────────────────────────────────────────────────────────
/**
 * @swagger
 * /api/v1/clientes/{co_cli}:
 *   delete:
 *     summary: Eliminar (anular) un cliente
 *     tags: [Clientes]
 *     parameters:
 *       - in: path
 *         name: co_cli
 *         required: true
 *         schema: { type: string }
 *       - in: query
 *         name: sede
 *         schema: { type: string }
 *     responses:
 *       200:
 *         description: Cliente eliminado
 */
router.delete('/:co_cli', async (req, res) => {
    try {
        const { co_cli } = req.params;

        const outcome = await executeWrite(req.query.sede || null, req.sqlAuth, async (pool) => {
            const auditUser = (req.profitUser || req.sqlAuth?.user || '01').substring(0, 10).toUpperCase();
            // Validar que el usuario sea vendedor registrado
            await requireCoVen(pool, auditUser);

            const check = await pool.request().input('co_cli', sql.VarChar, co_cli).query(
                `SELECT validador FROM saCliente WHERE LTRIM(RTRIM(co_cli)) = LTRIM(RTRIM(@co_cli))`
            );
            if (!check.recordset.length) throw new Error('El cliente no existe en esta sede.');

            const r = new sql.Request(pool);
            r.input('sCo_CliOri',  sql.Char(16),          co_cli);
            r.input('tsValidador', sql.VarBinary,          check.recordset[0].validador);
            r.input('sMaquina',    sql.VarChar(60),        'SYNC2K');
            r.input('sCo_Us_Mo',   sql.Char(6),            auditUser);
            r.input('sCo_Sucu_Mo', sql.Char(6),            '01');
            r.input('gRowguid',    sql.UniqueIdentifier,   null);
            await r.execute('pEliminarCliente');
        });

        return writeResponse(res, outcome, `Sede "${req.query.sede}" no encontrada.`);
    } catch (error) {
        res.status(500).json({ success: false, message: 'Error interno.', error: error.message });
    }
});

module.exports = router;
