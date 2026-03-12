const express = require('express');
const router = express.Router();
const { sql, getPool, getServers } = require('../db');
const { executeWrite, writeResponse, paginatedResponse } = require('../helpers/multiSede');

// ── Helper: inputs del STORED PROCEDURE pInsertarCliente ───────────────────
function bindClienteInsert(r, data, defaults, ts = new Date()) {
    const d = defaults;
    r.input('sCo_Cli',           sql.Char(16),         data.co_cli);
    r.input('sLogin',            sql.Char(20),          '');
    r.input('sPassword',         sql.Char(20),          '');
    r.input('sSalesTax',         sql.Char(8),           null);
    r.input('sCli_Des',          sql.VarChar(60),       data.cli_des);
    r.input('sCo_Seg',           sql.Char(6),           data.co_seg   || d.co_seg);
    r.input('sCo_Zon',           sql.Char(6),           data.co_zon   || d.co_zon);
    r.input('sCo_Ven',           sql.Char(6),           data.co_ven   || d.co_ven);
    r.input('sEstado',           sql.Char(1),           '1');
    r.input('bInactivo',         sql.Bit,               0);
    r.input('bValido',           sql.Bit,               0);
    r.input('bSinCredito',       sql.Bit,               0);
    r.input('bLunes',            sql.Bit,               1);
    r.input('bMartes',           sql.Bit,               1);
    r.input('bMiercoles',        sql.Bit,               1);
    r.input('bJueves',           sql.Bit,               1);
    r.input('bViernes',          sql.Bit,               1);
    r.input('bSabado',           sql.Bit,               1);
    r.input('bDomingo',          sql.Bit,               1);
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
    r.input('sCond_Pag',         sql.Char(6),           data.co_cond  || d.co_cond);
    r.input('iPlaz_pag',         sql.Int,               0);
    r.input('deDesc_ppago',      sql.Decimal(18,5),     0);
    r.input('deDesc_Glob',       sql.Decimal(18,5),     0);
    r.input('sTipo_Iva',         sql.Char(1),           '1');
    r.input('deIva',             sql.Decimal(18,5),     0);
    r.input('sRif',              sql.VarChar(18),       data.rif || '');
    r.input('bContrib',          sql.Bit,               1);
    r.input('sDis_cen',          sql.VarChar(sql.MAX),  '');
    r.input('sNit',              sql.VarChar(18),       '');
    r.input('sEmail',            sql.VarChar(60),       data.email || '');
    r.input('sCo_Cta_Ingr_Egr', sql.Char(20),          data.co_cta_ingr_egr || d.co_cta);
    r.input('sComentario',       sql.VarChar(sql.MAX),  'Creado vía API');
    r.input('sCampo1',           sql.VarChar(60),       '');
    r.input('sCampo2',           sql.VarChar(60),       '');
    r.input('sCampo3',           sql.VarChar(60),       '');
    r.input('sCampo4',           sql.VarChar(60),       '');
    r.input('sCampo5',           sql.VarChar(60),       '');
    r.input('sCampo6',           sql.VarChar(60),       '');
    r.input('sCampo7',           sql.VarChar(60),       '');
    r.input('sCampo8',           sql.VarChar(60),       '');
    r.input('sCo_Us_In',         sql.Char(6),           '999');
    r.input('sMaquina',          sql.VarChar(60),       'SYNC2K');
    r.input('sRevisado',         sql.Char(1),           '0');
    r.input('sTrasnfe',          sql.Char(1),           '0');
    r.input('sCo_Sucu_In',       sql.Char(6),           '01');
    r.input('bJuridico',         sql.Bit,               0);
    r.input('iTipo_Adi',         sql.Int,               1);
    r.input('sMatriz',           sql.Char(16),          null);
    r.input('sCo_Tab',           sql.Char(6),           null);
    r.input('sTipo_Per',         sql.Char(1),           '0');
    r.input('sCo_pais',          sql.Char(6),           null);
    r.input('sCiudad',           sql.VarChar(50),       '');
    r.input('sZip',              sql.VarChar(20),       '');
    r.input('sWebSite',          sql.VarChar(200),      '');
    r.input('bContribu_E',       sql.Bit,               0);
    r.input('bRete_Regis_Doc',   sql.Bit,               0);
    r.input('dePorc_Esp',        sql.Decimal(18,5),     0);
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
        pool.request().query('SELECT TOP 1 RTRIM(co_mone) AS id FROM saMoneda'),
        pool.request().query('SELECT TOP 1 RTRIM(co_cond) AS id FROM saCondicionPago')
    ]);
    return {
        co_cta:  cta.recordset[0]?.id,
        co_seg:  seg.recordset[0]?.id,
        co_zon:  zon.recordset[0]?.id,
        co_ven:  ven.recordset[0]?.id,
        tip_cli: tip.recordset[0]?.id,
        co_mone: mon.recordset[0]?.id,
        co_cond: cond.recordset[0]?.id
    };
}

// ────────────────────────────────────────────────────────────────────────────
// 1. GET /api/v1/clientes — Listado paginado desde todas las sedes
// ────────────────────────────────────────────────────────────────────────────
router.get('/', async (req, res) => {
    try {
        const page  = parseInt(req.query.page)  || 1;
        const limit = parseInt(req.query.limit) || 10;
        const servers = getServers();

        const allData = await Promise.all(servers.map(async (srv) => {
            try {
                const pool = await getPool(srv.id);
                const result = await pool.request().query(
                    `SELECT RTRIM(co_cli) AS co_cli, RTRIM(cli_des) AS descripcion
                     FROM saCliente WHERE inactivo = 0 ORDER BY cli_des`
                );
                return result.recordset.map(c => ({ ...c, sede_id: srv.id, sede_nombre: srv.name }));
            } catch (e) { return []; }
        }));

        const combined = [].concat(...allData);
        combined.sort((a, b) => a.descripcion.localeCompare(b.descripcion));
        return paginatedResponse(res, combined, page, limit);
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
            direccion: 'direc1', telefonos: 'telefonos', email: 'email'
        };

        const filters = Object.entries(req.query)
            .filter(([k, v]) => FIELD_MAP[k] && v)
            .map(([k, v]) => ({ param: k, column: FIELD_MAP[k], value: v }));

        if (!filters.length)
            return res.status(400).json({ success: false, message: 'Especifique al menos un parámetro de búsqueda.' });

        const whereClause = 'WHERE inactivo = 0 ' + filters.map(f => `AND ${f.column} LIKE '%' + @${f.param} + '%'`).join(' ');
        const servers = getServers();

        const allData = await Promise.all(servers.map(async (srv) => {
            try {
                const pool = await getPool(srv.id);
                const r = pool.request();
                filters.forEach(f => r.input(f.param, sql.VarChar, f.value));
                const result = await r.query(
                    `SELECT RTRIM(co_cli) AS co_cli, RTRIM(cli_des) AS descripcion,
                            RTRIM(rif) AS rif, RTRIM(direc1) AS direccion,
                            RTRIM(telefonos) AS telefonos, RTRIM(email) AS email
                     FROM saCliente ${whereClause} ORDER BY cli_des`
                );
                return result.recordset.map(c => ({ ...c, sede_id: srv.id, sede_nombre: srv.name }));
            } catch (e) { return []; }
        }));

        const combined = [].concat(...allData);
        combined.sort((a, b) => a.descripcion.localeCompare(b.descripcion));
        return paginatedResponse(res, combined, page, limit);
    } catch (error) {
        res.status(500).json({ success: false, message: 'Error en búsqueda.', error: error.message });
    }
});

// ────────────────────────────────────────────────────────────────────────────
// 3. GET /api/v1/clientes/:co_cli — Detalle del cliente desde todas las sedes
// ────────────────────────────────────────────────────────────────────────────
router.get('/:co_cli', async (req, res) => {
    try {
        const { co_cli } = req.params;
        const servers = getServers();

        const results = await Promise.all(servers.map(async (srv) => {
            try {
                const pool = await getPool(srv.id);
                const result = await pool.request().input('co_cli', sql.VarChar, co_cli).query(
                    `SELECT RTRIM(co_cli) AS co_cli, RTRIM(cli_des) AS descripcion,
                            RTRIM(rif) AS rif, RTRIM(direc1) AS direccion,
                            RTRIM(telefonos) AS telefonos, RTRIM(email) AS email,
                            RTRIM(co_ven) AS co_ven, RTRIM(co_zon) AS co_zon,
                            RTRIM(co_seg) AS co_seg, inactivo
                     FROM saCliente WHERE LTRIM(RTRIM(co_cli)) = LTRIM(RTRIM(@co_cli))`
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
        if (!data.co_cli || !data.cli_des)
            return res.status(400).json({ success: false, message: 'Campos obligatorios: co_cli, cli_des' });

        const outcome = await executeWrite(req.query.sede || null, async (pool) => {
            const defaults = await loadDefaults(pool);
            const r = new sql.Request(pool);
            bindClienteInsert(r, data, defaults);
            await r.execute('pInsertarCliente');
        });

        return writeResponse(res, outcome, `Sede "${req.query.sede}" no encontrada.`);
    } catch (error) {
        res.status(500).json({ success: false, message: 'Error interno.', error: error.message });
    }
});

// ────────────────────────────────────────────────────────────────────────────
// 5. PUT /api/v1/clientes/:co_cli — Actualizar cliente (targeted o broadcast)
// ────────────────────────────────────────────────────────────────────────────
router.put('/:co_cli', async (req, res) => {
    try {
        const { co_cli } = req.params;
        const data = req.body;

        const outcome = await executeWrite(req.query.sede || null, async (pool) => {
            const check = await pool.request().input('co_cli', sql.VarChar, co_cli).query(
                `SELECT validador, RTRIM(co_seg) AS co_seg, RTRIM(co_zon) AS co_zon,
                        RTRIM(co_ven) AS co_ven, RTRIM(tip_cli) AS tip_cli,
                        RTRIM(co_mone) AS co_mone, RTRIM(co_cond) AS co_cond,
                        RTRIM(co_cta_ingr_egr) AS co_cta_ingr_egr
                 FROM saCliente WHERE LTRIM(RTRIM(co_cli)) = LTRIM(RTRIM(@co_cli))`
            );
            if (!check.recordset.length) throw new Error('El cliente no existe en esta sede.');

            const row = check.recordset[0];
            const r   = new sql.Request(pool);
            const ts  = new Date();

            r.input('sCo_Cli',           sql.Char(16),         data.co_cli || co_cli);
            r.input('sCo_CliOri',        sql.Char(16),         co_cli);
            r.input('sLogin',            sql.Char(20),          '');
            r.input('sPassword',         sql.Char(20),          '');
            r.input('sSalesTax',         sql.Char(8),           null);
            r.input('sCli_Des',          sql.VarChar(60),       data.cli_des || 'Editado API');
            r.input('sCo_seg',           sql.Char(6),           data.co_seg   || row.co_seg);
            r.input('sCo_zon',           sql.Char(6),           data.co_zon   || row.co_zon);
            r.input('sCo_Ven',           sql.Char(6),           data.co_ven   || row.co_ven);
            r.input('sEstado',           sql.Char(1),           '1');
            r.input('bInactivo',         sql.Bit,               0);
            r.input('bValido',           sql.Bit,               0);
            r.input('bSinCredito',       sql.Bit,               0);
            r.input('bLunes',            sql.Bit,               1);
            r.input('bMartes',           sql.Bit,               1);
            r.input('bMiercoles',        sql.Bit,               1);
            r.input('bJueves',           sql.Bit,               1);
            r.input('bViernes',          sql.Bit,               1);
            r.input('bSabado',           sql.Bit,               1);
            r.input('bDomingo',          sql.Bit,               1);
            r.input('sDirec1',           sql.VarChar(sql.MAX),  data.direc1 || '');
            r.input('sDirec2',           sql.VarChar(sql.MAX),  '');
            r.input('sDir_Ent2',         sql.VarChar(sql.MAX),  '');
            r.input('sHorar_Caja',       sql.VarChar(sql.MAX),  '');
            r.input('sFrecu_Vist',       sql.VarChar(sql.MAX),  '');
            r.input('sTelefonos',        sql.VarChar(60),       data.telefonos || '');
            r.input('sFax',              sql.VarChar(60),       '');
            r.input('sRespons',          sql.VarChar(60),       '');
            r.input('sdFecha_reg',       sql.SmallDateTime,     ts);
            r.input('sTip_Cli',          sql.Char(6),           data.tip_cli  || row.tip_cli);
            r.input('sSerialP',          sql.Char(30),          null);
            r.input('iPuntaje',          sql.Int,               0);
            r.input('iId',               sql.Int,               0);
            r.input('deMont_cre',        sql.Decimal(18,5),     0);
            r.input('sCo_Mone',          sql.Char(6),           data.co_mone  || row.co_mone);
            r.input('sCond_Pag',         sql.Char(6),           data.co_cond  || row.co_cond);
            r.input('iPlaz_pag',         sql.Int,               0);
            r.input('deDesc_ppago',      sql.Decimal(18,5),     0);
            r.input('deDesc_Glob',       sql.Decimal(18,5),     0);
            r.input('sRif',              sql.VarChar(18),       data.rif || '');
            r.input('bContrib',          sql.Bit,               1);
            r.input('sDis_cen',          sql.VarChar(sql.MAX),  '');
            r.input('sNit',              sql.VarChar(18),       '');
            r.input('sEmail',            sql.VarChar(60),       data.email || '');
            r.input('sCo_Cta_Ingr_Egr', sql.Char(20),          data.co_cta_ingr_egr || row.co_cta_ingr_egr);
            r.input('sComentario',       sql.VarChar(sql.MAX),  'Editado vía API');
            r.input('sCampo1',           sql.VarChar(60),       '');
            r.input('sCampo2',           sql.VarChar(60),       '');
            r.input('sCampo3',           sql.VarChar(60),       '');
            r.input('sCampo4',           sql.VarChar(60),       '');
            r.input('sCampo5',           sql.VarChar(60),       '');
            r.input('sCampo6',           sql.VarChar(60),       '');
            r.input('sCampo7',           sql.VarChar(60),       '');
            r.input('sCampo8',           sql.VarChar(60),       '');
            r.input('sCo_us_mo',         sql.Char(6),           '999');
            r.input('sCo_Sucu_Mo',       sql.Char(6),           '01');
            r.input('sMaquina',          sql.VarChar(60),       'SYNC2K');
            r.input('sCampos',           sql.VarChar(sql.MAX),  null);
            r.input('sRevisado',         sql.Char(1),           '0');
            r.input('sTrasnfe',          sql.Char(1),           '0');
            r.input('bJuridico',         sql.Bit,               0);
            r.input('iTipo_Adi',         sql.Int,               1);
            r.input('sMatriz',           sql.Char(16),          null);
            r.input('sCo_Tab',           sql.Char(6),           null);
            r.input('sTipo_Per',         sql.Char(1),           '0');
            r.input('sCo_pais',          sql.VarChar(6),        null);
            r.input('sCiudad',           sql.VarChar(50),       '');
            r.input('sZip',              sql.VarChar(20),       '');
            r.input('sWebSite',          sql.VarChar(200),      '');
            r.input('bContribu_E',       sql.Bit,               0);
            r.input('bRete_Regis_Doc',   sql.Bit,               0);
            r.input('dePorc_Esp',        sql.Decimal(18,5),     0);
            r.input('tsValidador',       sql.VarBinary,         row.validador);
            r.input('gRowguid',          sql.UniqueIdentifier,  null);
            r.input('sN_cr',             sql.Char(6),           null);
            r.input('sN_db',             sql.Char(6),           null);
            r.input('sTComp',            sql.Char(6),           null);
            r.input('sEmail_alterno',    sql.VarChar(60),       '');
            await r.execute('pActualizarCliente');
        });

        return writeResponse(res, outcome, `Sede "${req.query.sede}" no encontrada.`);
    } catch (error) {
        res.status(500).json({ success: false, message: 'Error interno.', error: error.message });
    }
});

// ────────────────────────────────────────────────────────────────────────────
// 6. DELETE /api/v1/clientes/:co_cli — Eliminar cliente (targeted o broadcast)
// ────────────────────────────────────────────────────────────────────────────
router.delete('/:co_cli', async (req, res) => {
    try {
        const { co_cli } = req.params;

        const outcome = await executeWrite(req.query.sede || null, async (pool) => {
            const check = await pool.request().input('co_cli', sql.VarChar, co_cli).query(
                `SELECT validador FROM saCliente WHERE LTRIM(RTRIM(co_cli)) = LTRIM(RTRIM(@co_cli))`
            );
            if (!check.recordset.length) throw new Error('El cliente no existe en esta sede.');

            const r = new sql.Request(pool);
            r.input('sCo_CliOri',  sql.Char(16),          co_cli);
            r.input('tsValidador', sql.VarBinary,          check.recordset[0].validador);
            r.input('sMaquina',    sql.VarChar(60),        'SYNC2K');
            r.input('sCo_Us_Mo',   sql.Char(6),            '999');
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
