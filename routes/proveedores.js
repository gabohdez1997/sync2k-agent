const express = require('express');
const router = express.Router();
const { sql, getPool, getServers } = require('../db');
const { executeWrite, writeResponse, padProfit, aggregateRead, resolveServer } = require('../helpers/multiSede');

// ── Helper: resuelve o valida el co_tab de saTabuladorIslr según tipo_per ───
async function resolveCoTab(pool, co_tab, tipo_per) {
    try {
        // 1. Si enviaron un co_tab específico, validar si existe en la BD
        if (co_tab && typeof co_tab === 'string' && co_tab.trim()) {
            const clean = co_tab.trim();
            const check = await pool.request().input('co_tab', sql.VarChar, clean).query(
                'SELECT TOP 1 RTRIM(co_tab) AS co_tab FROM saTabuladorIslr WHERE LTRIM(RTRIM(co_tab)) = LTRIM(RTRIM(@co_tab))'
            );
            if (check.recordset.length > 0) {
                return check.recordset[0].co_tab;
            }
        }

        // 2. Si no o no existe, buscar el tabulador correspondiente al tipo_per (ej: '3' PJD -> '7', '1' PNR -> '5')
        if (tipo_per) {
            const cleanTipoPer = String(tipo_per).trim();
            const checkByTipo = await pool.request().input('tipo_per', sql.VarChar, cleanTipoPer).query(
                'SELECT TOP 1 RTRIM(co_tab) AS co_tab FROM saTabuladorIslr WHERE LTRIM(RTRIM(tipo_per)) = LTRIM(RTRIM(@tipo_per))'
            );
            if (checkByTipo.recordset.length > 0) {
                return checkByTipo.recordset[0].co_tab;
            }
        }

        // 3. Fallback: cualquier tabulador disponible en la tabla
        const anyTab = await pool.request().query(
            'SELECT TOP 1 RTRIM(co_tab) AS co_tab FROM saTabuladorIslr'
        );
        if (anyTab.recordset.length > 0) {
            return anyTab.recordset[0].co_tab;
        }

        // 4. Si la tabla está vacía, NULL
        return null;
    } catch {
        return null;
    }
}

// ── Helper: inputs del STORED PROCEDURE pInsertarProveedor ───────────────────
function bindProveedorInsert(r, data, defaults, ts = new Date(), auditUser = '999') {
    const d = defaults;
    const co_prov = (data.co_prov || data.rif || '').trim().toUpperCase();
    r.input('sCo_Prov', sql.Char(16), padProfit(co_prov, 16));
    r.input('sProv_des', sql.VarChar(60), String(data.prov_des || data.descripcion || '').trim().substring(0, 60));
    r.input('sCo_seg', sql.Char(6), padProfit(data.co_seg || d.co_seg, 6));
    r.input('sCo_zon', sql.Char(6), padProfit(data.co_zon || d.co_zon, 6));
    r.input('bInactivo', sql.Bit, data.inactivo ? 1 : 0);
    r.input('sDirec1', sql.VarChar(sql.MAX), data.direc1 || data.direccion || '');
    r.input('sDirec2', sql.VarChar(sql.MAX), '');
    r.input('sTelefonos', sql.VarChar(60), data.telefonos || '');
    r.input('sFax', sql.VarChar(60), '');
    r.input('sRespons', sql.VarChar(60), data.respons || '');
    r.input('sdFecha_reg', sql.SmallDateTime, ts);
    r.input('sTip_Pro', sql.Char(6), padProfit(data.tip_pro || d.tip_pro, 6));
    r.input('deMont_cre', sql.Decimal(18, 2), Number(data.mont_cre) || 0);
    r.input('sCo_Mone', sql.Char(6), padProfit(data.co_mone || d.co_mone, 6));
    r.input('sCond_Pag', sql.Char(6), padProfit(data.cond_pag || d.cond_pag || '01', 6));
    r.input('iPlaz_pag', sql.Int, Number(data.plaz_pag) || 0);
    r.input('deDesc_ppago', sql.Decimal(18, 2), Number(data.desc_ppago) || 0);
    r.input('deDesc_Glob', sql.Decimal(18, 2), Number(data.desc_glob) || 0);
    r.input('sRif', sql.VarChar(18), (data.rif || '').trim());
    r.input('bNacional', sql.Bit, data.nacional !== undefined ? (data.nacional ? 1 : 0) : 1);
    r.input('sDis_cen', sql.VarChar(sql.MAX), data.dis_cen || '<InformacionContable><Carpeta01><CuentaContable>1.1.05.001</CuentaContable></Carpeta01></InformacionContable>');
    r.input('sNit', sql.VarChar(18), '');
    r.input('sEmail', sql.VarChar(60), (data.email || '').trim());
    r.input('sCo_Cta_Ingr_Egr', sql.Char(20), padProfit(data.co_cta_ingr_egr || data.co_cta || d.co_cta || '02', 20));
    r.input('sComentario', sql.VarChar(sql.MAX), data.comentario || 'Creado vía API');
    r.input('iTipo_Adi', sql.Int, 1);
    r.input('sMatriz', sql.Char(16), null);
    r.input('sCo_Tab', sql.Char(20), data.co_tab ? padProfit(data.co_tab, 20) : null);
    r.input('sTipo_Per', sql.Char(1), data.tipo_per || '3');
    r.input('sCo_pais', sql.Char(6), padProfit(data.co_pais || 'VE', 6));
    r.input('sCiudad', sql.VarChar(50), data.ciudad || '');
    r.input('sZip', sql.VarChar(10), data.zip || '');
    r.input('sWebSite', sql.VarChar(200), data.website || '');
    r.input('sFormType', sql.Char(30), null);
    r.input('sTaxid', sql.Char(20), null);
    r.input('bContribu_E', sql.Bit, data.contribu_e || data.contribuyente ? 1 : 0);
    r.input('bRete_Regis_Doc', sql.Bit, 0);
    r.input('dePorc_Esp', sql.Decimal(18, 2), Number(data.porc_esp) || 0);
    r.input('sCampo1', sql.VarChar(60), '');
    r.input('sCampo2', sql.VarChar(60), '');
    r.input('sCampo3', sql.VarChar(60), '');
    r.input('sCampo4', sql.VarChar(60), '');
    r.input('sCampo5', sql.VarChar(60), '');
    r.input('sCampo6', sql.VarChar(60), '');
    r.input('sCampo7', sql.VarChar(60), '');
    r.input('sCampo8', sql.VarChar(60), '');
    r.input('sCo_Us_In', sql.Char(6), padProfit(auditUser, 6));
    r.input('sCo_Sucu_In', sql.Char(6), padProfit(d.co_sucu || '01', 6));
    r.input('sMaquina', sql.VarChar(60), 'SYNC2K');
    r.input('sRevisado', sql.Char(1), '0');
    r.input('sTrasnfe', sql.Char(1), '0');
    r.input('sTgasto', sql.Char(2), null);
    r.input('sTComp', sql.Char(2), null);
    r.input('sEmail_alterno', sql.VarChar(120), null);
    r.input('bSujeto_Obj_RetenISLR_Auto', sql.Bit, 0);
}

// ── Helper: inputs del STORED PROCEDURE pActualizarProveedor ────────────────
function bindProveedorUpdate(r, data, row, ts = new Date(), auditUser = '999', defaults = null) {
    const co_prov = (data.co_prov || row.co_prov).trim().toUpperCase();
    r.input('sCo_Prov', sql.Char(16), padProfit(co_prov, 16));
    r.input('sCo_ProvOri', sql.Char(16), padProfit(row.co_prov, 16));
    r.input('sProv_des', sql.VarChar(100), (data.prov_des || data.descripcion || row.prov_des || row.descripcion).trim());
    r.input('sCo_seg', sql.Char(6), padProfit(data.co_seg || row.co_seg || '01', 6));
    r.input('sCo_zon', sql.Char(6), padProfit(data.co_zon || row.co_zon || '01', 6));
    r.input('bInactivo', sql.Bit, data.inactivo !== undefined ? (data.inactivo ? 1 : 0) : (row.inactivo ? 1 : 0));
    r.input('sDirec1', sql.VarChar(sql.MAX), data.direc1 ?? data.direccion ?? row.direc1 ?? '');
    r.input('sDirec2', sql.VarChar(sql.MAX), '');
    r.input('sTelefonos', sql.VarChar(60), data.telefonos ?? row.telefonos ?? '');
    r.input('sFax', sql.VarChar(60), '');
    r.input('sRespons', sql.VarChar(60), data.respons ?? row.respons ?? '');
    r.input('sdFecha_reg', sql.SmallDateTime, row.fecha_reg || ts);
    r.input('sTip_Pro', sql.Char(6), padProfit(data.tip_pro || row.tip_pro, 6));
    r.input('deMont_cre', sql.Decimal(18, 2), Number(data.mont_cre ?? row.mont_cre) || 0);
    r.input('sCo_Mone', sql.Char(6), padProfit(data.co_mone || row.co_mone, 6));
    r.input('sCond_Pag', sql.Char(6), padProfit(data.cond_pag || row.cond_pag || '01', 6));
    r.input('iPlaz_pag', sql.Int, Number(data.plaz_pag ?? row.plaz_pag) || 0);
    r.input('deDesc_ppago', sql.Decimal(18, 2), Number(data.desc_ppago ?? row.desc_ppago) || 0);
    r.input('deDesc_Glob', sql.Decimal(18, 2), Number(data.desc_glob ?? row.desc_glob) || 0);
    r.input('sRif', sql.VarChar(18), (data.rif ?? row.rif ?? '').trim());
    r.input('bNacional', sql.Bit, data.nacional !== undefined ? (data.nacional ? 1 : 0) : (row.nacional ? 1 : 0));
    const existingDisCen = row.dis_cen ? String(row.dis_cen).trim() : '';
    r.input('sDis_cen', sql.VarChar(sql.MAX), existingDisCen || '<InformacionContable><Carpeta01><CuentaContable>1.1.05.001</CuentaContable></Carpeta01></InformacionContable>');
    r.input('sNit', sql.VarChar(18), '');
    r.input('sEmail', sql.VarChar(60), (data.email ?? row.email ?? '').trim());
    r.input('sCo_Cta_Ingr_Egr', sql.Char(20), padProfit(data.co_cta_ingr_egr || (row.co_cta_ingr_egr && row.co_cta_ingr_egr !== '01' ? row.co_cta_ingr_egr : '02'), 20));
    r.input('sComentario', sql.VarChar(sql.MAX), data.comentario || 'Editado vía API');
    r.input('iTipo_Adi', sql.Int, 1);
    r.input('sMatriz', sql.Char(16), null);
    r.input('sCo_Tab', sql.Char(20), row.co_tab ? padProfit(row.co_tab, 20) : null);
    r.input('sTipo_Per', sql.Char(1), data.tipo_per || row.tipo_per || '3');
    r.input('sCo_pais', sql.Char(6), padProfit(data.co_pais || row.co_pais || 'VE', 6));
    r.input('sCiudad', sql.VarChar(50), data.ciudad ?? row.ciudad ?? '');
    r.input('sZip', sql.VarChar(10), data.zip ?? row.zip ?? '');
    r.input('sWebSite', sql.VarChar(200), data.website ?? row.website ?? '');
    r.input('sFormType', sql.Char(30), null);
    r.input('sTaxid', sql.Char(20), null);
    r.input('bContribu_E', sql.Bit, (data.contribu_e || data.contribuyente) ? 1 : 0);
    r.input('bRete_Regis_Doc', sql.Bit, 0);
    r.input('dePorc_Esp', sql.Decimal(18, 2), Number(data.porc_esp ?? row.porc_esp) || 0);
    r.input('sCampo1', sql.VarChar(60), '');
    r.input('sCampo2', sql.VarChar(60), '');
    r.input('sCampo3', sql.VarChar(60), '');
    r.input('sCampo4', sql.VarChar(60), '');
    r.input('sCampo5', sql.VarChar(60), '');
    r.input('sCampo6', sql.VarChar(60), '');
    r.input('sCampo7', sql.VarChar(60), '');
    r.input('sCampo8', sql.VarChar(60), '');
    r.input('sCo_Us_Mo', sql.Char(6), padProfit(auditUser, 6));
    r.input('sCo_Sucu_Mo', sql.Char(6), padProfit(defaults?.co_sucu || '01', 6));
    r.input('sMaquina', sql.VarChar(60), 'SYNC2K');
    r.input('sCampos', sql.VarChar(sql.MAX), null);
    r.input('sRevisado', sql.Char(1), '0');
    r.input('sTrasnfe', sql.Char(1), '0');
    r.input('tsValidador', sql.VarBinary(8), row.validador);
    r.input('gRowguid', sql.UniqueIdentifier, null);
    r.input('sTgasto', sql.Char(2), null);
    r.input('sTComp', sql.Char(2), null);
    r.input('sEmail_alterno', sql.VarChar(120), null);
    r.input('bSujeto_Obj_RetenISLR_Auto', sql.Bit, 0);
}

// ── Helper: carga defaults de FK para Proveedor ─────────────────────────────
async function loadDefaults(pool, srv = null) {
    const defaultSucu = (srv?.profit_branch_codes || []).find(b => b.is_default)?.code 
        || (srv?.profit_branch_codes || [])[0]?.code 
        || (srv?.profit_branch_codes || [])[0] 
        || null;

    const [cta, seg, zon, tip, mon, cond, sucu] = await Promise.all([
        pool.request().query('SELECT TOP 1 RTRIM(co_cta_ingr_egr) AS id FROM saCuentaIngEgr ORDER BY CASE WHEN RTRIM(co_cta_ingr_egr) = \'02\' THEN 0 ELSE 1 END'),
        pool.request().query('SELECT TOP 1 RTRIM(co_seg) AS id FROM saSegmento'),
        pool.request().query('SELECT TOP 1 RTRIM(co_zon) AS id FROM saZona'),
        pool.request().query('SELECT TOP 1 RTRIM(tip_pro) AS id FROM saTipoProveedor ORDER BY CASE WHEN RTRIM(tip_pro) = \'01\' THEN 0 ELSE 1 END, tip_pro'),
        pool.request().query('SELECT TOP 1 RTRIM(co_mone) AS id FROM saMoneda ORDER BY CASE WHEN co_mone IN (\'BS\',\'VES\',\'BSF\') THEN 0 ELSE 1 END, co_mone'),
        pool.request().query('SELECT TOP 1 RTRIM(co_cond) AS id FROM saCondicionPago'),
        pool.request().query('SELECT TOP 1 RTRIM(co_sucur) AS id FROM saSucursal ORDER BY CASE WHEN RTRIM(co_sucur) = \'01\' THEN 0 ELSE 1 END, co_sucur')
    ]);
    return {
        co_cta: cta.recordset[0]?.id || '02',
        co_seg: seg.recordset[0]?.id || '01',
        co_zon: zon.recordset[0]?.id || '01',
        tip_pro: tip.recordset[0]?.id || '01',
        co_mone: mon.recordset[0]?.id || '01',
        cond_pag: cond.recordset[0]?.id || '01',
        co_sucu: defaultSucu || sucu.recordset[0]?.id || '01'
    };
}

/**
 * @swagger
 * tags:
 *   name: Proveedores
 *   description: Gestión de proveedores (Rif, Direcciones, etc.)
 */

// ────────────────────────────────────────────────────────────────────────────
// 1. GET /api/v1/proveedores — Listado paginado desde todas las sedes
// ────────────────────────────────────────────────────────────────────────────
router.get('/', async (req, res) => {
    try {
        const page = parseInt(req.query.page) || 1;
        const limit = parseInt(req.query.limit) || 20;
        let servers = getServers();
        if (req.query.sede_id) servers = servers.filter(s => s.id === req.query.sede_id);
        else if (req.query.sede) servers = servers.filter(s => s.id === req.query.sede);

        const offset = (page - 1) * limit;
        let globalTotal = 0;

        const allData = await Promise.all(servers.map(async (srv) => {
            try {
                const pool = await getPool(srv.id, req.sqlAuth);
                let whereSQL = "WHERE p.inactivo = 0 ";
                const request = pool.request();

                // Fetch Total Count
                const countRes = await request.query(`SELECT COUNT(*) AS total FROM saProveedor p ${whereSQL}`);
                globalTotal += countRes.recordset[0].total;

                // Fetch Paginated Chunk
                request.input('offset', sql.Int, offset);
                request.input('limit', sql.Int, limit);

                const result = await request.query(
                    `SELECT RTRIM(p.co_prov) AS co_prov, RTRIM(p.prov_des) AS descripcion,
                            RTRIM(p.rif) AS rif, RTRIM(p.direc1) AS direc1,
                            RTRIM(p.telefonos) AS telefonos, RTRIM(p.email) AS email,
                            RTRIM(p.respons) AS respons,
                            RTRIM(p.co_seg) AS co_seg,
                            RTRIM(p.co_zon) AS co_zon, RTRIM(z.zon_des) AS zon_des,
                            RTRIM(p.tip_pro) AS tip_pro, RTRIM(tp.des_tipo) AS tip_pro_des,
                            RTRIM(p.cond_pag) AS cond_pag, RTRIM(cp.cond_des) AS cond_des,
                            p.contribu_e, p.porc_esp, RTRIM(p.tipo_per) AS tipo_per,
                            p.inactivo
                     FROM saProveedor p
                     LEFT JOIN saZona z ON p.co_zon = z.co_zon
                     LEFT JOIN saTipoProveedor tp ON p.tip_pro = tp.tip_pro
                     LEFT JOIN saCondicionPago cp ON p.cond_pag = cp.co_cond
                     ${whereSQL} 
                     ORDER BY p.prov_des
                     OFFSET @offset ROWS FETCH NEXT @limit ROWS ONLY`
                );
                return result.recordset.map(c => ({ ...c, sede_id: srv.id, sede_nombre: srv.name }));
            } catch (e) { return []; }
        }));

        const combined = [].concat(...allData);
        combined.sort((a, b) => (a.descripcion || '').localeCompare(b.descripcion || ''));

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
        console.error('[PROVEEDORES GET ERROR]:', error);
        res.status(500).json({ success: false, message: 'Error interno.', error: error.message });
    }
});

// ────────────────────────────────────────────────────────────────────────────
// 2. GET /api/v1/proveedores/search — Búsqueda con filtros
// ────────────────────────────────────────────────────────────────────────────
router.get('/search', async (req, res) => {
    try {
        const page = parseInt(req.query.page) || 1;
        const limit = parseInt(req.query.limit) || 20;

        const q = (req.query.q || req.query.search || req.query.descripcion || req.query.prov_des || '').trim();
        const rif = (req.query.rif || '').trim();
        const co_prov = (req.query.co_prov || '').trim();

        let servers = getServers();
        if (req.query.sede_id) servers = servers.filter(s => s.id === req.query.sede_id);
        else if (req.query.sede) servers = servers.filter(s => s.id === req.query.sede);

        const offset = (page - 1) * limit;
        let globalTotal = 0;

        const allData = await Promise.all(servers.map(async (srv) => {
            try {
                const pool = await getPool(srv.id, req.sqlAuth);
                const countReq = pool.request();
                const dataReq = pool.request();

                let whereConditions = ["p.inactivo = 0"];

                if (co_prov) {
                    countReq.input('co_prov', sql.VarChar, `%${co_prov}%`);
                    dataReq.input('co_prov', sql.VarChar, `%${co_prov}%`);
                    whereConditions.push("p.co_prov LIKE @co_prov");
                }
                if (rif) {
                    countReq.input('rif', sql.VarChar, `%${rif}%`);
                    dataReq.input('rif', sql.VarChar, `%${rif}%`);
                    whereConditions.push("p.rif LIKE @rif");
                }
                if (q) {
                    countReq.input('q', sql.VarChar, `%${q}%`);
                    dataReq.input('q', sql.VarChar, `%${q}%`);
                    whereConditions.push("(p.prov_des LIKE @q OR p.co_prov LIKE @q OR p.rif LIKE @q OR p.telefonos LIKE @q OR p.email LIKE @q OR p.respons LIKE @q)");
                }

                const whereSQL = whereConditions.length ? "WHERE " + whereConditions.join(" AND ") : "";

                const countRes = await countReq.query(
                    `SELECT COUNT(*) AS total FROM saProveedor p ${whereSQL}`
                );
                globalTotal += countRes.recordset[0].total;

                dataReq.input('offset', sql.Int, offset);
                dataReq.input('limit', sql.Int, limit);

                const result = await dataReq.query(
                    `SELECT RTRIM(p.co_prov) AS co_prov, RTRIM(p.prov_des) AS descripcion,
                            RTRIM(p.rif) AS rif, RTRIM(p.direc1) AS direc1,
                            RTRIM(p.telefonos) AS telefonos, RTRIM(p.email) AS email,
                            RTRIM(p.respons) AS respons,
                            RTRIM(p.co_seg) AS co_seg,
                            RTRIM(p.co_zon) AS co_zon, RTRIM(z.zon_des) AS zon_des,
                            RTRIM(p.tip_pro) AS tip_pro, RTRIM(tp.des_tipo) AS tip_pro_des,
                            RTRIM(p.cond_pag) AS cond_pag, RTRIM(cp.cond_des) AS cond_des,
                            p.contribu_e, p.porc_esp, RTRIM(p.tipo_per) AS tipo_per,
                            p.inactivo
                     FROM saProveedor p
                     LEFT JOIN saZona z ON p.co_zon = z.co_zon
                     LEFT JOIN saTipoProveedor tp ON p.tip_pro = tp.tip_pro
                     LEFT JOIN saCondicionPago cp ON p.cond_pag = cp.co_cond
                     ${whereSQL}
                     ORDER BY p.prov_des
                     OFFSET @offset ROWS FETCH NEXT @limit ROWS ONLY`
                );
                return result.recordset.map(c => ({ ...c, sede_id: srv.id, sede_nombre: srv.name }));
            } catch (e) { return []; }
        }));

        const combined = [].concat(...allData);
        combined.sort((a, b) => (a.descripcion || '').localeCompare(b.descripcion || ''));

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
        console.error('[PROVEEDORES SEARCH ERROR]:', error);
        res.status(500).json({ success: false, message: 'Error en búsqueda.', error: error.message });
    }
});

// ────────────────────────────────────────────────────────────────────────────
// 2.1 GET /api/v1/proveedores/export-all — Exportar todos los proveedores de la sede
// ────────────────────────────────────────────────────────────────────────────
router.get('/export-all', async (req, res) => {
    try {
        const srv = resolveServer(req);
        if (!srv) return res.status(404).json({ success: false, message: 'No hay sede disponible.' });

        const pool = await getPool(srv.id, req.sqlAuth);
        const result = await pool.request().query(
            `SELECT RTRIM(co_prov) AS co_prov, RTRIM(prov_des) AS prov_des, RTRIM(rif) AS rif,
                    RTRIM(direc1) AS direc1, RTRIM(direc2) AS direc2, RTRIM(telefonos) AS telefonos,
                    RTRIM(fax) AS fax, RTRIM(respons) AS respons, fecha_reg,
                    RTRIM(tip_pro) AS tip_pro, mont_cre, RTRIM(co_mone) AS co_mone,
                    RTRIM(cond_pag) AS cond_pag, plaz_pag, desc_ppago, desc_glob,
                    nacional, dis_cen, nit, RTRIM(email) AS email, RTRIM(co_cta_ingr_egr) AS co_cta_ingr_egr,
                    comentario, tipo_adi, matriz, RTRIM(co_tab) AS co_tab, RTRIM(tipo_per) AS tipo_per,
                    RTRIM(co_pais) AS co_pais, RTRIM(ciudad) AS ciudad, RTRIM(zip) AS zip,
                    RTRIM(website) AS website, formtype, taxid, contribu_e, rete_regis_doc,
                    porc_esp, inactivo, RTRIM(co_seg) AS co_seg, RTRIM(co_zon) AS co_zon
             FROM saProveedor`
        );

        return res.status(200).json({
            success: true,
            sede_id: srv.id,
            sede_nombre: srv.name,
            count: result.recordset.length,
            data: result.recordset
        });
    } catch (error) {
        console.error('[PROVEEDORES EXPORT ERROR]:', error);
        res.status(500).json({ success: false, message: 'Error exportando proveedores', error: error.message });
    }
});

// ────────────────────────────────────────────────────────────────────────────
// 2.2 POST /api/v1/proveedores/import-batch — Importar lote de proveedores faltantes
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
        const defaults = await loadDefaults(pool, srv);
        const auditUser = (req.profitUser || req.sqlAuth?.user || '01').substring(0, 6).toUpperCase();
        
        let migratedCount = 0;
        const errors = [];

        const [existingRes, segRes, zonRes, tipRes, condRes, monRes] = await Promise.all([
            pool.request().query('SELECT RTRIM(co_prov) AS co_prov FROM saProveedor'),
            pool.request().query('SELECT RTRIM(co_seg) AS id FROM saSegmento'),
            pool.request().query('SELECT RTRIM(co_zon) AS id FROM saZona'),
            pool.request().query('SELECT RTRIM(tip_pro) AS id FROM saTipoProveedor'),
            pool.request().query('SELECT RTRIM(co_cond) AS id FROM saCondicionPago'),
            pool.request().query('SELECT RTRIM(co_mone) AS id FROM saMoneda')
        ]);

        const existingSet = new Set(existingRes.recordset.map(r => (r.co_prov || '').trim().toUpperCase()));
        const segSet = new Set(segRes.recordset.map(r => (r.id || '').trim().toUpperCase()));
        const zonSet = new Set(zonRes.recordset.map(r => (r.id || '').trim().toUpperCase()));
        const tipSet = new Set(tipRes.recordset.map(r => (r.id || '').trim().toUpperCase()));
        const condSet = new Set(condRes.recordset.map(r => (r.id || '').trim().toUpperCase()));
        const monSet = new Set(monRes.recordset.map(r => (r.id || '').trim().toUpperCase()));

        for (const item of items) {
            const co_prov = (item.co_prov || item.rif || '').trim().toUpperCase();
            if (!co_prov || existingSet.has(co_prov)) continue;

            try {
                const dataToInsert = { ...item };

                dataToInsert.co_seg = segSet.has((dataToInsert.co_seg || '').trim().toUpperCase()) ? dataToInsert.co_seg : defaults.co_seg;
                dataToInsert.co_zon = zonSet.has((dataToInsert.co_zon || '').trim().toUpperCase()) ? dataToInsert.co_zon : defaults.co_zon;
                dataToInsert.tip_pro = tipSet.has((dataToInsert.tip_pro || '').trim().toUpperCase()) ? dataToInsert.tip_pro : defaults.tip_pro;
                dataToInsert.cond_pag = condSet.has((dataToInsert.cond_pag || '').trim().toUpperCase()) ? dataToInsert.cond_pag : defaults.cond_pag;
                dataToInsert.co_mone = monSet.has((dataToInsert.co_mone || '').trim().toUpperCase()) ? dataToInsert.co_mone : defaults.co_mone;
                dataToInsert.co_cta_ingr_egr = (dataToInsert.co_cta_ingr_egr && dataToInsert.co_cta_ingr_egr !== '01') ? dataToInsert.co_cta_ingr_egr : (defaults.co_cta || '02');

                const tipo_per = dataToInsert.tipo_per || '3';
                dataToInsert.tipo_per = tipo_per;
                dataToInsert.co_tab = await resolveCoTab(pool, dataToInsert.co_tab, tipo_per);
                dataToInsert.co_pais = dataToInsert.co_pais || 'VE';

                const r = new sql.Request(pool);
                bindProveedorInsert(r, dataToInsert, defaults, new Date(), auditUser);
                await r.execute('pInsertarProveedor');

                if (item.prov_des && item.prov_des.length > 60) {
                    try {
                        await pool.request()
                            .input('prov', sql.Char(16), padProfit(co_prov, 16))
                            .input('des', sql.VarChar(120), String(item.prov_des).trim().substring(0, 120))
                            .query('UPDATE saProveedor SET prov_des = @des WHERE LTRIM(RTRIM(co_prov)) = LTRIM(RTRIM(@prov))');
                    } catch {}
                }

                if (dataToInsert.inactivo) {
                    await pool.request()
                        .input('prov', sql.Char(16), padProfit(co_prov, 16))
                        .query('UPDATE saProveedor SET inactivo = 1 WHERE LTRIM(RTRIM(co_prov)) = LTRIM(RTRIM(@prov))');
                }

                existingSet.add(co_prov);
                migratedCount++;
            } catch (err) {
                errors.push(`Proveedor ${co_prov} (${item.prov_des || item.descripcion}): ${err.message}`);
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
        console.error('[PROVEEDORES IMPORT BATCH ERROR]:', error);
        res.status(500).json({ success: false, message: 'Error importando lote de proveedores', error: error.message });
    }
});

// ────────────────────────────────────────────────────────────────────────────
// 3. GET /api/v1/proveedores/:co_prov — Detalle del proveedor desde todas las sedes
// ────────────────────────────────────────────────────────────────────────────
router.get('/:co_prov', async (req, res) => {
    try {
        const { co_prov } = req.params;
        const servers = getServers();

        const results = await Promise.all(servers.map(async (srv) => {
            try {
                const pool = await getPool(srv.id, req.sqlAuth);
                const result = await pool.request().input('co_prov', sql.VarChar, co_prov).query(
                    `SELECT RTRIM(p.co_prov) AS co_prov, RTRIM(p.prov_des) AS descripcion,
                            RTRIM(p.rif) AS rif, RTRIM(p.direc1) AS direc1,
                            RTRIM(p.telefonos) AS telefonos, RTRIM(p.email) AS email,
                            RTRIM(p.co_zon) AS co_zon, RTRIM(z.zon_des) AS zon_des,
                            RTRIM(p.co_seg) AS co_seg, p.inactivo,
                            RTRIM(p.tip_pro) AS tip_pro, RTRIM(tp.des_tipo) AS tip_pro_des,
                            RTRIM(p.co_mone) AS co_mone, RTRIM(p.cond_pag) AS cond_pag,
                            RTRIM(cp.cond_des) AS cond_des, RTRIM(cp.cond_des) AS cond_pag_des,
                            p.contribu_e, p.porc_esp, RTRIM(p.tipo_per) AS tipo_per,
                            RTRIM(p.ciudad) AS ciudad, RTRIM(p.respons) AS respons
                     FROM saProveedor p
                     LEFT JOIN saZona z ON p.co_zon = z.co_zon
                     LEFT JOIN saTipoProveedor tp ON p.tip_pro = tp.tip_pro
                     LEFT JOIN saCondicionPago cp ON p.cond_pag = cp.co_cond
                     WHERE LTRIM(RTRIM(p.co_prov)) = LTRIM(RTRIM(@co_prov))`
                );
                if (!result.recordset.length) return null;
                return { ...result.recordset[0], sede_id: srv.id, sede_nombre: srv.name };
            } catch (e) { return { sede_id: srv.id, sede_nombre: srv.name, error: e.message }; }
        }));

        const found = results.filter(r => r && !r.error);
        if (!found.length)
            return res.status(404).json({ success: false, message: 'Proveedor no encontrado en ninguna sede.' });

        res.status(200).json({ success: true, count: found.length, data: results.filter(r => r !== null) });
    } catch (error) {
        res.status(500).json({ success: false, message: 'Error interno.', error: error.message });
    }
});

// ────────────────────────────────────────────────────────────────────────────
// 4. POST /api/v1/proveedores — Crear proveedor (targeted o broadcast)
// ────────────────────────────────────────────────────────────────────────────
router.post('/', async (req, res) => {
    try {
        const data = req.body;
        const co_prov = data.co_prov || data.rif;
        const prov_des = data.prov_des || data.descripcion;

        if (!co_prov || !prov_des) {
            return res.status(400).json({ success: false, message: 'Campos obligatorios: co_prov/rif, prov_des/descripcion' });
        }

        const outcome = await executeWrite(req.query.sede || null, req.sqlAuth, async (pool, srv) => {
            const co_prov_clean = (data.co_prov || data.rif || '').trim().toUpperCase();
            const check = await pool.request().input('co_prov', sql.VarChar, co_prov_clean).query(
                'SELECT 1 FROM saProveedor WHERE LTRIM(RTRIM(co_prov)) = LTRIM(RTRIM(@co_prov))'
            );
            if (check.recordset.length > 0) {
                return { skipped: true, message: 'Proveedor ya registrado en esta sede' };
            }

            const defaults = await loadDefaults(pool, srv);
            const r = new sql.Request(pool);
            const auditUser = (req.profitUser || req.sqlAuth?.user || '01').substring(0, 6).toUpperCase();

            const tipo_per = data.tipo_per || '3';
            data.tipo_per = tipo_per;
            data.co_tab = await resolveCoTab(pool, data.co_tab, tipo_per);

            bindProveedorInsert(r, data, defaults, new Date(), auditUser);
            await r.execute('pInsertarProveedor');
        });

        return writeResponse(res, outcome, `Sede "${req.query.sede}" no encontrada.`);
    } catch (error) {
        console.error('[PROVEEDORES POST FATAL ERROR]:', error);
        res.status(500).json({
            success: false,
            message: 'Error al procesar la creación del proveedor.',
            error: error.message || String(error)
        });
    }
});

// ────────────────────────────────────────────────────────────────────────────
// 5. PUT /api/v1/proveedores/:co_prov — Actualizar proveedor (targeted o broadcast)
// ────────────────────────────────────────────────────────────────────────────
router.put('/:co_prov', async (req, res) => {
    try {
        const { co_prov } = req.params;
        const data = req.body;

        const outcome = await executeWrite(req.query.sede || null, req.sqlAuth, async (pool, srv) => {
            const check = await pool.request().input('co_prov', sql.VarChar, co_prov).query(
                `SELECT validador,
                        RTRIM(co_prov)           AS co_prov,
                        RTRIM(prov_des)         AS prov_des, 
                        RTRIM(co_seg)           AS co_seg,
                        RTRIM(co_zon)           AS co_zon,
                        RTRIM(tip_pro)          AS tip_pro,
                        RTRIM(co_mone)          AS co_mone,
                        RTRIM(cond_pag)         AS cond_pag,
                        RTRIM(co_cta_ingr_egr)  AS co_cta_ingr_egr,
                        RTRIM(tipo_per)         AS tipo_per,
                        RTRIM(direc1)           AS direc1,
                        RTRIM(telefonos)        AS telefonos,
                        RTRIM(rif)              AS rif,
                        contribu_e,
                        porc_esp,
                        inactivo,
                        RTRIM(co_tab)           AS co_tab,
                        RTRIM(co_pais)          AS co_pais,
                        CAST(dis_cen AS VARCHAR(MAX)) AS dis_cen
                 FROM saProveedor WHERE LTRIM(RTRIM(co_prov)) = LTRIM(RTRIM(@co_prov))`
            );
            if (!check.recordset.length) throw new Error('El proveedor no existe en esta sede.');

            const row = check.recordset[0];
            const defaults = await loadDefaults(pool, srv);
            row.co_mone = row.co_mone || defaults.co_mone;
            row.tip_pro = row.tip_pro || defaults.tip_pro;
            row.co_zon = row.co_zon || defaults.co_zon;
            row.co_seg = row.co_seg || defaults.co_seg;
            row.co_cta_ingr_egr = (row.co_cta_ingr_egr && row.co_cta_ingr_egr !== '01') ? row.co_cta_ingr_egr : (defaults.co_cta || '02');
            row.cond_pag = row.cond_pag || defaults.cond_pag;
            row.co_pais = row.co_pais || 'VE';
            
            // Resolver co_tab automáticamente según el tipo_per (ej. PJD '3' -> '7', PNR '1' -> '5')
            const candidateTipoPer = data.tipo_per || row.tipo_per || '3';
            const candidateTab = data.co_tab !== undefined ? data.co_tab : row.co_tab;
            row.co_tab = await resolveCoTab(pool, candidateTab, candidateTipoPer);
            row.tipo_per = candidateTipoPer;

            const r = new sql.Request(pool);
            const auditUser = (req.profitUser || req.sqlAuth?.user || '01').substring(0, 6).toUpperCase();

            bindProveedorUpdate(r, data, row, new Date(), auditUser, defaults);
            const spResult = await r.execute('pActualizarProveedor');

            if (spResult.returnValue !== 0 && spResult.returnValue !== undefined) {
                throw new Error(`pActualizarProveedor retornó código ${spResult.returnValue}. Verifique los datos enviados.`);
            }
        });

        return writeResponse(res, outcome, `Sede "${req.query.sede}" no encontrada.`);
    } catch (error) {
        console.error('[PROVEEDORES PUT FATAL ERROR]:', error);
        res.status(500).json({
            success: false,
            message: 'Error al procesar la actualización del proveedor.',
            error: error.message || String(error)
        });
    }
});

// ────────────────────────────────────────────────────────────────────────────
// 6. DELETE /api/v1/proveedores/:co_prov — Eliminar proveedor (targeted o broadcast)
// ────────────────────────────────────────────────────────────────────────────
router.delete('/:co_prov', async (req, res) => {
    try {
        const { co_prov } = req.params;

        const outcome = await executeWrite(req.query.sede || null, req.sqlAuth, async (pool) => {
            const auditUser = (req.profitUser || req.sqlAuth?.user || '01').substring(0, 6).toUpperCase();

            const check = await pool.request().input('co_prov', sql.VarChar, co_prov).query(
                `SELECT validador FROM saProveedor WHERE LTRIM(RTRIM(co_prov)) = LTRIM(RTRIM(@co_prov))`
            );
            if (!check.recordset.length) throw new Error('El proveedor no existe en esta sede.');

            const r = new sql.Request(pool);
            r.input('sCo_ProvOri', sql.Char(16), padProfit(co_prov, 16));
            r.input('tsValidador', sql.VarBinary, check.recordset[0].validador);
            r.input('sMaquina', sql.VarChar(60), 'SYNC2K');
            r.input('sCo_Us_Mo', sql.Char(6), padProfit(auditUser, 6));
            r.input('sCo_Sucu_Mo', sql.Char(6), padProfit('01', 6));
            r.input('gRowguid', sql.UniqueIdentifier, null);
            await r.execute('pEliminarProveedor');
        });

        return writeResponse(res, outcome, `Sede "${req.query.sede}" no encontrada.`);
    } catch (error) {
        console.error('[PROVEEDORES DELETE ERROR]:', error);
        res.status(500).json({ success: false, message: 'Error interno.', error: error.message });
    }
});

// ────────────────────────────────────────────────────────────────────────────
// 7. POST /api/v1/proveedores/sync — Sincronización multisede de proveedores
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

        // 1. Obtener todos los proveedores de cada servidor
        const serverSuppliers = {};
        const allUniqueSuppliers = new Map();

        for (const srv of servers) {
            try {
                const pool = await getPool(srv.id, req.sqlAuth);
                const result = await pool.request().query(
                    `SELECT RTRIM(co_prov) AS co_prov, RTRIM(prov_des) AS prov_des, RTRIM(rif) AS rif,
                            RTRIM(direc1) AS direc1, RTRIM(direc2) AS direc2, RTRIM(telefonos) AS telefonos,
                            RTRIM(fax) AS fax, RTRIM(respons) AS respons, fecha_reg,
                            RTRIM(tip_pro) AS tip_pro, mont_cre, RTRIM(co_mone) AS co_mone,
                            RTRIM(cond_pag) AS cond_pag, plaz_pag, desc_ppago, desc_glob,
                            nacional, dis_cen, nit, RTRIM(email) AS email, RTRIM(co_cta_ingr_egr) AS co_cta_ingr_egr,
                            comentario, tipo_adi, matriz, RTRIM(co_tab) AS co_tab, RTRIM(tipo_per) AS tipo_per,
                            RTRIM(co_pais) AS co_pais, RTRIM(ciudad) AS ciudad, RTRIM(zip) AS zip,
                            RTRIM(website) AS website, formtype, taxid, contribu_e, rete_regis_doc,
                            porc_esp, inactivo, RTRIM(co_seg) AS co_seg, RTRIM(co_zon) AS co_zon
                     FROM saProveedor`
                );
                
                const provMap = new Map();
                for (const row of result.recordset) {
                    const key = (row.co_prov || '').trim().toUpperCase();
                    if (key) {
                        provMap.set(key, row);
                        if (!allUniqueSuppliers.has(key)) {
                            allUniqueSuppliers.set(key, row);
                        }
                    }
                }
                serverSuppliers[srv.id] = { server: srv, map: provMap, pool };
            } catch (err) {
                console.warn(`[SYNC PROVEEDORES] Error leyendo proveedores de sede ${srv.name}:`, err.message);
            }
        }

        // 2. Para cada servidor, detectar cuáles proveedores faltan y migrarlos
        const summary = [];
        let totalSynced = 0;
        const auditUser = (req.profitUser || req.sqlAuth?.user || '01').substring(0, 6).toUpperCase();

        for (const srv of servers) {
            const srvData = serverSuppliers[srv.id];
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
            const defaults = await loadDefaults(pool, srv);
            let migratedCount = 0;
            const errors = [];

            for (const [co_prov, supplier] of allUniqueSuppliers.entries()) {
                if (!map.has(co_prov)) {
                    // El proveedor no existe en esta sede: migrarlo con consistencia referencial
                    try {
                        // Clonar datos
                        const dataToInsert = { ...supplier };

                        // 1. Validar co_seg en destino
                        const segCheck = await pool.request().input('seg', sql.VarChar, dataToInsert.co_seg || '').query(
                            'SELECT TOP 1 co_seg FROM saSegmento WHERE LTRIM(RTRIM(co_seg)) = LTRIM(RTRIM(@seg))'
                        );
                        dataToInsert.co_seg = segCheck.recordset.length ? dataToInsert.co_seg : defaults.co_seg;

                        // 2. Validar co_zon en destino
                        const zonCheck = await pool.request().input('zon', sql.VarChar, dataToInsert.co_zon || '').query(
                            'SELECT TOP 1 co_zon FROM saZona WHERE LTRIM(RTRIM(co_zon)) = LTRIM(RTRIM(@zon))'
                        );
                        dataToInsert.co_zon = zonCheck.recordset.length ? dataToInsert.co_zon : defaults.co_zon;

                        // 3. Validar tip_pro en destino
                        const tipCheck = await pool.request().input('tip', sql.VarChar, dataToInsert.tip_pro || '').query(
                            'SELECT TOP 1 tip_pro FROM saTipoProveedor WHERE LTRIM(RTRIM(tip_pro)) = LTRIM(RTRIM(@tip))'
                        );
                        dataToInsert.tip_pro = tipCheck.recordset.length ? dataToInsert.tip_pro : defaults.tip_pro;

                        // 4. Validar cond_pag en destino
                        const condCheck = await pool.request().input('cond', sql.VarChar, dataToInsert.cond_pag || '').query(
                            'SELECT TOP 1 co_cond FROM saCondicionPago WHERE LTRIM(RTRIM(co_cond)) = LTRIM(RTRIM(@cond))'
                        );
                        dataToInsert.cond_pag = condCheck.recordset.length ? dataToInsert.cond_pag : defaults.cond_pag;

                        // 5. Validar co_mone en destino
                        const monCheck = await pool.request().input('mone', sql.VarChar, dataToInsert.co_mone || '').query(
                            'SELECT TOP 1 co_mone FROM saMoneda WHERE LTRIM(RTRIM(co_mone)) = LTRIM(RTRIM(@mone))'
                        );
                        dataToInsert.co_mone = monCheck.recordset.length ? dataToInsert.co_mone : defaults.co_mone;

                        // 6. Cuenta de egresos
                        dataToInsert.co_cta_ingr_egr = (dataToInsert.co_cta_ingr_egr && dataToInsert.co_cta_ingr_egr !== '01') ? dataToInsert.co_cta_ingr_egr : (defaults.co_cta || '02');

                        // 7. Tabulador ISLR dinámico según tipo_per
                        const tipo_per = dataToInsert.tipo_per || '3';
                        dataToInsert.tipo_per = tipo_per;
                        dataToInsert.co_tab = await resolveCoTab(pool, dataToInsert.co_tab, tipo_per);

                        // 8. País
                        dataToInsert.co_pais = dataToInsert.co_pais || 'VE';

                        const r = new sql.Request(pool);
                        bindProveedorInsert(r, dataToInsert, defaults, new Date(), auditUser);
                        await r.execute('pInsertarProveedor');

                        // Si el proveedor original estaba inactivo, asegurar su estado inactivo en destino
                        if (dataToInsert.inactivo) {
                            await pool.request()
                                .input('prov', sql.Char(16), padProfit(co_prov, 16))
                                .query('UPDATE saProveedor SET inactivo = 1 WHERE LTRIM(RTRIM(co_prov)) = LTRIM(RTRIM(@prov))');
                        }

                        migratedCount++;
                        totalSynced++;
                        map.set(co_prov, dataToInsert); // Actualizar mapa en memoria
                    } catch (err) {
                        errors.push(`Proveedor ${co_prov} (${supplier.prov_des || supplier.descripcion}): ${err.message}`);
                    }
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
                ? `Sincronización completada con éxito. Se migraron ${totalSynced} proveedores.`
                : 'Todas las sucursales ya se encuentran sincronizadas.'
        });
    } catch (error) {
        console.error('[SYNC PROVEEDORES FATAL ERROR]:', error);
        res.status(500).json({
            success: false,
            message: 'Error general al sincronizar proveedores.',
            error: error.message || String(error)
        });
    }
});

module.exports = router;
