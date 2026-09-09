const express = require('express');
const router = express.Router();
const { sql, getPool, getServers, getExchangeRate } = require('../db');
const { executeWrite, writeResponse, paginatedResponse, padProfit } = require('../helpers/multiSede');
const { getProximoConsecutivo } = require('../helpers/consecutivos');

console.log("🧾 [AGENT] Iniciando Módulo de Facturas de Compra (saFacturaCompra)");

function safeDate(val) {
    if (!val) return null;
    if (val instanceof Date) return val.toISOString().split('T')[0];
    return String(val).split('T')[0];
}

// =========================================================================
// 1. LISTAR RECEPCIONES PENDIENTES POR FACTURAR (PARA MODAL DE IMPORTACIÓN)
// =========================================================================
router.get('/recepciones/pendientes', async (req, res) => {
    try {
        const { sede, co_prov, search } = req.query;
        const servers = getServers();
        const targets = sede ? servers.filter(s => s.id === sede) : servers;

        if (targets.length === 0) {
            return res.status(404).json({ success: false, message: `Sede "${sede}" no encontrada.` });
        }

        const allResults = await Promise.all(targets.map(async (srv) => {
            try {
                const pool = await getPool(srv.id, req.sqlAuth);
                const request = pool.request();
                let whereClauses = [
                    "c.anulado = 0",
                    "c.status IN ('0', '1')",
                    "EXISTS (SELECT 1 FROM saNotaRecepcionCompraReng r WHERE r.doc_num = c.doc_num AND ISNULL(r.pendiente, 0) > 0)"
                ];

                if (co_prov) {
                    request.input('co_prov', sql.VarChar, co_prov.trim());
                    whereClauses.push("LTRIM(RTRIM(c.co_prov)) = @co_prov");
                }

                if (search) {
                    request.input('search', sql.VarChar, `%${search.trim()}%`);
                    whereClauses.push(`(
                        c.doc_num LIKE @search 
                        OR p.prov_des LIKE @search 
                        OR c.co_prov LIKE @search 
                        OR p.rif LIKE @search
                        OR c.nro_fact LIKE @search
                        OR c.n_control LIKE @search
                    )`);
                }

                const whereSQL = whereClauses.join(" AND ");

                // 1. Cabeceras de recepciones pendientes
                const resHeaders = await request.query(`
                    SELECT 
                        RTRIM(c.doc_num) AS doc_num,
                        RTRIM(c.descrip) AS descrip,
                        RTRIM(c.co_prov) AS co_prov,
                        RTRIM(p.prov_des) AS prov_des,
                        RTRIM(p.rif) AS rif,
                        RTRIM(p.direc1) AS direc1,
                        RTRIM(p.telefonos) AS telefonos,
                        p.contribu_e,
                        p.porc_esp,
                        c.fec_emis,
                        c.fec_venc,
                        RTRIM(c.co_mone) AS co_mone,
                        c.tasa,
                        c.total_bruto,
                        c.total_neto,
                        RTRIM(c.n_control) AS n_control,
                        RTRIM(c.nro_fact) AS nro_fact,
                        RTRIM(c.status) AS status,
                        RTRIM(c.co_cond) AS co_cond,
                        RTRIM(cd.cond_des) AS cond_des,
                        RTRIM(c.co_sucu_in) AS co_sucu_in,
                        (
                            SELECT TOP 1 RTRIM(al.des_alma)
                            FROM saNotaRecepcionCompraReng r
                            LEFT JOIN saAlmacen al ON r.co_alma = al.co_alma
                            WHERE r.doc_num = c.doc_num
                        ) AS almacen_des,
                        (SELECT COUNT(*) FROM saNotaRecepcionCompraReng r WHERE r.doc_num = c.doc_num AND r.pendiente > 0) AS cant_renglones_pendientes,
                        (SELECT ISNULL(SUM(r.pendiente), 0) FROM saNotaRecepcionCompraReng r WHERE r.doc_num = c.doc_num) AS total_unidades_pendientes
                    FROM saNotaRecepcionCompra c
                    LEFT JOIN saProveedor p ON c.co_prov = p.co_prov
                    LEFT JOIN saCondicionPago cd ON c.co_cond = cd.co_cond
                    WHERE ${whereSQL}
                    ORDER BY c.fec_emis DESC, c.doc_num DESC
                `);

                const headers = resHeaders.recordset;
                if (headers.length === 0) return [];

                // 2. Renglones pendientes de estas recepciones
                const docNums = headers.map(h => `'${h.doc_num}'`).join(',');
                const resRenglones = await pool.request().query(`
                    SELECT 
                        RTRIM(r.doc_num) AS doc_num,
                        r.reng_num,
                        RTRIM(r.co_art) AS co_art,
                        RTRIM(ISNULL(a.art_des, r.des_art)) AS art_des,
                        RTRIM(a.modelo) AS modelo,
                        RTRIM(a.ref) AS referencia,
                        RTRIM(r.co_uni) AS co_uni,
                        RTRIM(u.des_uni) AS unidad,
                        RTRIM(r.co_alma) AS co_alma,
                        RTRIM(al.des_alma) AS des_alma,
                        r.total_art,
                        r.pendiente,
                        r.cost_unit,
                        r.cost_unit_om,
                        RTRIM(r.tipo_imp) AS tipo_imp,
                        r.porc_imp,
                        r.monto_imp,
                        r.reng_neto,
                        r.rowguid
                    FROM saNotaRecepcionCompraReng r
                    LEFT JOIN saArticulo a ON r.co_art = a.co_art
                    LEFT JOIN saUnidad u ON r.co_uni = u.co_uni
                    LEFT JOIN saAlmacen al ON r.co_alma = al.co_alma
                    WHERE r.doc_num IN (${docNums})
                      AND ISNULL(r.pendiente, 0) > 0
                    ORDER BY r.doc_num, r.reng_num
                `);

                const rengMap = {};
                for (const row of resRenglones.recordset) {
                    const d = row.doc_num;
                    if (!rengMap[d]) rengMap[d] = [];
                    rengMap[d].push({
                        ...row,
                        checked: true,
                        cant_facturar: Number(row.pendiente) || 0
                    });
                }

                return headers.map(h => ({
                    ...h,
                    sede_id: srv.id,
                    sede_nombre: srv.name,
                    renglones: rengMap[h.doc_num] || []
                }));

            } catch (err) {
                console.error(`[FACTURAS COMPRAS] Error consultando recepciones pendientes en sede ${srv.id}:`, err.message);
                return [];
            }
        }));

        const combined = [].concat(...allResults);
        return res.json({ success: true, count: combined.length, data: combined });
    } catch (error) {
        res.status(500).json({ success: false, message: 'Error al consultar recepciones pendientes.', error: error.message });
    }
});

// =========================================================================
// 2. LISTAR FACTURAS DE COMPRA (HISTORIAL)
// =========================================================================
router.get('/', async (req, res) => {
    try {
        const page  = parseInt(req.query.page)  || 1;
        const limit = parseInt(req.query.limit) || 12;
        const { sede, doc_num, nro_fact, n_control, co_prov, fec_d, fec_h, search, status } = req.query;
        
        const servers = getServers();
        const targets = sede ? servers.filter(s => s.id === sede) : servers;

        const allData = await Promise.all(targets.map(async (srv) => {
            try {
                const pool = await getPool(srv.id, req.sqlAuth);
                const request = pool.request();
                let whereClauses = ["1=1"];

                if (doc_num) {
                    request.input('doc_num', sql.VarChar, `%${doc_num}%`);
                    whereClauses.push("f.doc_num LIKE @doc_num");
                }
                if (nro_fact) {
                    request.input('nro_fact', sql.VarChar, `%${nro_fact}%`);
                    whereClauses.push("f.nro_fact LIKE @nro_fact");
                }
                if (n_control) {
                    request.input('n_control', sql.VarChar, `%${n_control}%`);
                    whereClauses.push("f.n_control LIKE @n_control");
                }
                if (co_prov) {
                    request.input('co_prov_search', sql.VarChar, `%${co_prov}%`);
                    whereClauses.push("(f.co_prov LIKE @co_prov_search OR p.prov_des LIKE @co_prov_search OR p.rif LIKE @co_prov_search)");
                }
                if (search) {
                    request.input('search_all', sql.VarChar, `%${search}%`);
                    whereClauses.push(`(
                        f.doc_num LIKE @search_all 
                        OR f.nro_fact LIKE @search_all 
                        OR f.n_control LIKE @search_all 
                        OR p.prov_des LIKE @search_all 
                        OR f.co_prov LIKE @search_all 
                        OR p.rif LIKE @search_all
                        OR EXISTS (SELECT 1 FROM saFacturaCompraReng r WHERE r.doc_num = f.doc_num AND r.num_doc LIKE @search_all)
                    )`);
                }
                if (fec_d) {
                    request.input('fec_d', sql.SmallDateTime, fec_d);
                    whereClauses.push("f.fec_emis >= @fec_d");
                }
                if (fec_h) {
                    request.input('fec_h', sql.SmallDateTime, fec_h);
                    whereClauses.push("f.fec_emis < DATEADD(day, 1, @fec_h)");
                }
                if (status !== undefined && status !== null && status !== '' && status !== 'all') {
                    if (status === 'anulado') {
                        whereClauses.push("f.anulado = 1");
                    } else if (status === 'activo') {
                        whereClauses.push("f.anulado = 0");
                    }
                }

                const whereSQL = whereClauses.join(" AND ");

                const querySQL = `
                    SELECT 
                        RTRIM(f.doc_num) AS doc_num,
                        RTRIM(f.nro_fact) AS nro_fact,
                        RTRIM(f.n_control) AS n_control,
                        RTRIM(f.descrip) AS descrip,
                        RTRIM(f.co_prov) AS co_prov,
                        RTRIM(p.prov_des) AS prov_des,
                        RTRIM(p.rif) AS rif,
                        f.fec_emis,
                        f.fec_venc,
                        f.fec_reg,
                        f.fe_us_in AS fec_us_in,
                        f.anulado,
                        RTRIM(f.co_mone) AS co_mone,
                        f.tasa,
                        f.total_bruto,
                        f.monto_imp,
                        f.total_neto,
                        f.saldo,
                        RTRIM(f.co_cond) AS co_cond,
                        RTRIM(cd.cond_des) AS cond_des,
                        RTRIM(f.co_us_in) AS co_us_in,
                        RTRIM(f.co_sucu_in) AS co_sucu_in,
                        (SELECT COUNT(*) FROM saFacturaCompraReng r WHERE r.doc_num = f.doc_num) AS cant_renglones,
                        (SELECT ISNULL(SUM(r.total_art), 0) FROM saFacturaCompraReng r WHERE r.doc_num = f.doc_num) AS total_unidades,
                        (
                            SELECT TOP 1 RTRIM(r.num_doc) 
                            FROM saFacturaCompraReng r 
                            WHERE r.doc_num = f.doc_num AND r.num_doc IS NOT NULL AND LTRIM(RTRIM(r.num_doc)) <> ''
                        ) AS recepcion_origen
                    FROM saFacturaCompra f
                    LEFT JOIN saProveedor p ON f.co_prov = p.co_prov
                    LEFT JOIN saCondicionPago cd ON f.co_cond = cd.co_cond
                    WHERE ${whereSQL}
                    ORDER BY f.fec_emis DESC, f.doc_num DESC
                `;

                const result = await request.query(querySQL);
                return result.recordset.map(c => ({ ...c, sede_id: srv.id, sede_nombre: srv.name }));
            } catch (e) {
                console.error(`[FACTURAS COMPRAS] Error en sede ${srv.id}:`, e.message);
                return [];
            }
        }));

        const combined = [].concat(...allData);
        combined.sort((a, b) => new Date(b.fec_emis) - new Date(a.fec_emis));
        return paginatedResponse(res, combined, page, limit);
    } catch (error) {
        res.status(500).json({ success: false, message: 'Error al consultar Facturas de Compra.', error: error.message });
    }
});

// =========================================================================
// 3. DETALLE DE FACTURA DE COMPRA
// =========================================================================
router.get('/:doc_num', async (req, res) => {
    try {
        const { doc_num } = req.params;
        const { sede } = req.query;
        const servers = getServers();
        const targets = sede ? servers.filter(s => s.id === sede) : servers;

        if (targets.length === 0)
            return res.status(404).json({ success: false, message: `Sede "${sede}" no encontrada.` });

        const results = await Promise.all(targets.map(async (srv) => {
            try {
                const pool = await getPool(srv.id, req.sqlAuth);

                const [resEnc, resReng, currentRate] = await Promise.all([
                    pool.request().input('doc_num', sql.VarChar, doc_num).query(`
                        SELECT 
                            RTRIM(f.doc_num) AS doc_num,
                            RTRIM(f.nro_fact) AS nro_fact,
                            RTRIM(f.n_control) AS n_control,
                            RTRIM(f.descrip) AS descrip,
                            RTRIM(f.co_prov) AS co_prov,
                            RTRIM(p.prov_des) AS prov_des,
                            RTRIM(p.rif) AS rif,
                            RTRIM(p.direc1) AS direc1,
                            RTRIM(p.telefonos) AS telefonos,
                            RTRIM(p.email) AS email,
                            RTRIM(f.co_cond) AS co_cond,
                            RTRIM(cd.cond_des) AS cond_des,
                            f.fec_emis,
                            f.fec_venc,
                            f.fec_reg,
                            f.fe_us_in AS fec_us_in,
                            f.anulado,
                            RTRIM(f.co_mone) AS co_mone,
                            f.tasa,
                            f.total_bruto,
                            f.monto_desc_glob,
                            f.monto_imp,
                            f.total_neto,
                            f.saldo,
                            RTRIM(f.comentario) AS comentario,
                            RTRIM(f.co_us_in) AS co_us_in,
                            RTRIM(f.co_sucu_in) AS co_sucu_in
                        FROM saFacturaCompra f
                        LEFT JOIN saProveedor p ON f.co_prov = p.co_prov
                        LEFT JOIN saCondicionPago cd ON f.co_cond = cd.co_cond
                        WHERE LTRIM(RTRIM(f.doc_num)) = LTRIM(RTRIM(@doc_num))
                    `),
                    pool.request().input('doc_num', sql.VarChar, doc_num).query(`
                        SELECT 
                            r.reng_num,
                            RTRIM(r.co_art) AS co_art,
                            RTRIM(ISNULL(a.art_des, r.des_art)) AS art_des,
                            RTRIM(a.modelo) AS modelo,
                            RTRIM(a.ref) AS referencia,
                            RTRIM(r.co_uni) AS co_uni,
                            RTRIM(u.des_uni) AS unidad,
                            RTRIM(r.co_alma) AS co_alma,
                            RTRIM(al.des_alma) AS des_alma,
                            r.total_art AS cantidad,
                            r.pendiente,
                            r.cost_unit AS costo,
                            r.cost_unit_om AS costo_om,
                            RTRIM(r.tipo_imp) AS tipo_imp,
                            r.porc_imp,
                            r.monto_imp,
                            r.reng_neto AS total_renglon,
                            RTRIM(r.tipo_doc) AS tipo_doc,
                            RTRIM(r.num_doc) AS num_doc,
                            r.rowguid_doc
                        FROM saFacturaCompraReng r
                        LEFT JOIN saArticulo a ON r.co_art = a.co_art
                        LEFT JOIN saUnidad u ON r.co_uni = u.co_uni
                        LEFT JOIN saAlmacen al ON r.co_alma = al.co_alma
                        WHERE LTRIM(RTRIM(r.doc_num)) = LTRIM(RTRIM(@doc_num))
                        ORDER BY r.reng_num
                    `),
                    getExchangeRate(pool)
                ]);

                if (!resEnc.recordset.length) return null;
                return { 
                    ...resEnc.recordset[0], 
                    renglones: resReng.recordset, 
                    tasa_actual: currentRate,
                    sede_id: srv.id, 
                    sede_nombre: srv.name 
                };
            } catch (e) {
                return { sede_id: srv.id, sede_nombre: srv.name, error: e.message };
            }
        }));

        const found = results.filter(r => r && !r.error);
        if (!found.length)
            return res.status(404).json({ success: false, message: 'Factura de compra no encontrada.' });

        res.status(200).json({ success: true, count: found.length, data: results.filter(r => r !== null) });
    } catch (error) {
        res.status(500).json({ success: false, message: 'Error al consultar Factura de Compra.', error: error.message });
    }
});

// =========================================================================
// 4. REGISTRAR FACTURA DE COMPRA
// =========================================================================
router.post('/', async (req, res) => {
    const payload = req.body;
    console.log("📥 [AGENT] Recibiendo Factura de Compra (SAVE):", JSON.stringify({
        co_prov: payload.co_prov,
        nro_fact: payload.nro_fact,
        n_control: payload.n_control,
        renglones: payload.renglones?.length
    }, null, 2));

    if (!payload.co_prov || !payload.nro_fact || !payload.renglones || !payload.renglones.length) {
        return res.status(400).json({
            success: false,
            message: 'Campos obligatorios: co_prov, nro_fact (N° Factura del Proveedor) y renglones.'
        });
    }

    const outcome = await executeWrite(req.query.sede || null, req.sqlAuth, async (pool, srv) => {
        const auditUser = (req.profitUser || req.sqlAuth?.user || 'API').substring(0, 6).toUpperCase();
        const coSucu = (payload.co_sucu || srv.co_sucu || '01').substring(0, 6);

        // 1. Obtener correlativo consecutivo para saFacturaCompra
        const consecutivoInfo = await getProximoConsecutivo({
            runner: pool,
            co_tipo_serie: 'FACTURA_COMPRA',
            co_sucur: coSucu,
            table: 'saFacturaCompra',
            col: 'doc_num'
        });
        const docNum = consecutivoInfo.docNum;
        console.log(`📦 [FACTURA COMPRA] Asignando Correlativo: ${docNum} para sede: ${srv.name}`);

        // 2. Resolver datos maestros y moneda
        const [resMoneda, resUSD, resCond, resTax, resTasa] = await Promise.all([
            pool.request().query(`SELECT TOP 1 RTRIM(g_moneda) AS g_moneda FROM par_emp`),
            pool.request().query(`SELECT TOP 1 RTRIM(co_mone)  AS co_mone   FROM saMoneda WHERE LTRIM(RTRIM(co_mone)) IN ('US$','USD','DOL','$','US') OR mone_des LIKE '%Dolar%'`),
            pool.request().input('co_cond', sql.VarChar, payload.co_cond || '01').query(`SELECT TOP 1 co_cond, dias_cred FROM saCondicionPago WHERE co_cond = @co_cond`),
            pool.request().query(`SELECT TOP 1 RTRIM(co_art) as tax_co_art FROM par_emp`),
            getExchangeRate(pool)
        ]);

        const coMone = payload.co_mone || resUSD.recordset[0]?.co_mone || 'USD';
        const tasaCambio = Number(payload.tasa) > 0 ? Number(payload.tasa) : (Number(resTasa) || 1);
        const diasCred = Number(resCond.recordset[0]?.dias_cred) || 0;

        const ts = new Date();
        const fecEmis = payload.fec_emis ? new Date(`${safeDate(payload.fec_emis)}T00:00:00`) : ts;
        const fecVenc = payload.fec_venc ? new Date(`${safeDate(payload.fec_venc)}T00:00:00`) : new Date(fecEmis.getTime() + (diasCred * 86400000));
        const fecReg = ts;

        // Iniciar Transacción SQL
        const transaction = new sql.Transaction(pool);
        await transaction.begin();

        try {
            // 3. Calcular Totales
            let totalBruto = 0;
            let totalImp = 0;
            let totalNeto = 0;

            for (const item of payload.renglones) {
                const cant = Number(item.total_art || item.cantidad) || 0;
                const costUnit = Number(item.cost_unit || item.costo) || 0;
                const subtotal = cant * costUnit;
                const porcDesc = Number(item.porc_desc) || 0;
                const montoDesc = subtotal * (porcDesc / 100);
                const netoReng = subtotal - montoDesc;
                const porcImp = Number(item.porc_imp) || 0;
                const montoImp = netoReng * (porcImp / 100);

                totalBruto += subtotal;
                totalImp += montoImp;
                totalNeto += (netoReng + montoImp);
            }

            const descGlobalPorc = Number(payload.porc_desc_glob) || 0;
            const descGlobalMonto = totalBruto * (descGlobalPorc / 100);
            totalNeto -= descGlobalMonto;
            const saldo = totalNeto;

            // 4. Insertar Cabecera de Factura de Compra
            const descripDoc = (payload.descrip || `FACTURA COMPRA ${payload.nro_fact}`).substring(0, 60);
            const reqH = new sql.Request(transaction);
            reqH.input('sDoc_Num',          sql.Char(20),       padProfit(docNum, 20));
            reqH.input('sNro_Fact',         sql.Char(20),       padProfit(String(payload.nro_fact).trim(), 20));
            reqH.input('sDescrip',          sql.VarChar(60),    descripDoc);
            reqH.input('sCo_Prov',          sql.Char(16),       padProfit(payload.co_prov, 16));
            reqH.input('sCo_Cta_Ingr_Egr',  sql.Char(20),       null);
            reqH.input('sCo_Mone',          sql.Char(6),        padProfit(coMone, 6));
            reqH.input('sCo_Cond',          sql.Char(6),        padProfit(payload.co_cond || '01', 6));
            reqH.input('sN_Control',        sql.VarChar(20),    payload.n_control ? String(payload.n_control).trim().substring(0, 20) : 'N/A');
            reqH.input('sPorc_Desc_Glob',   sql.VarChar(15),    String(descGlobalPorc));
            reqH.input('sdFec_Emis',        sql.SmallDateTime,  fecEmis);
            reqH.input('sdFec_Venc',        sql.SmallDateTime,  fecVenc);
            reqH.input('sdFec_Reg',         sql.SmallDateTime,  fecReg);
            reqH.input('bAnulado',          sql.Bit,            0);
            reqH.input('sStatus',           sql.Char(1),        '0');
            reqH.input('deTasa',            sql.Decimal(18, 5), tasaCambio);
            reqH.input('sPorc_Reca',        sql.VarChar(15),    null);
            reqH.input('deSaldo',           sql.Decimal(18, 2), saldo);
            reqH.input('deTotal_Bruto',     sql.Decimal(18, 2), totalBruto);
            reqH.input('deTotal_Neto',      sql.Decimal(18, 2), totalNeto);
            reqH.input('deMonto_Desc_Glob', sql.Decimal(18, 2), descGlobalMonto);
            reqH.input('deMonto_Reca',      sql.Decimal(18, 2), 0);
            reqH.input('deOtros1',          sql.Decimal(18, 2), 0);
            reqH.input('deOtros2',          sql.Decimal(18, 2), 0);
            reqH.input('deOtros3',          sql.Decimal(18, 2), 0);
            reqH.input('deMonto_Imp',       sql.Decimal(18, 2), totalImp);
            reqH.input('deMonto_Imp2',      sql.Decimal(18, 2), 0);
            reqH.input('deMonto_Imp3',      sql.Decimal(18, 2), 0);
            reqH.input('sDir_Ent',          sql.VarChar(sql.MAX), null);
            reqH.input('sComentario',       sql.VarChar(sql.MAX), payload.comentario || `Registrado vía Sync2k`);
            reqH.input('bImpresa',          sql.Bit,            0);
            reqH.input('sSalestax',         sql.Char(8),        null);
            reqH.input('sDis_Cen',          sql.VarChar(sql.MAX), null);
            reqH.input('sCampo1',           sql.VarChar(60),    null);
            reqH.input('sCampo2',           sql.VarChar(60),    null);
            reqH.input('sCampo3',           sql.VarChar(60),    null);
            reqH.input('sCampo4',           sql.VarChar(60),    null);
            reqH.input('sCampo5',           sql.VarChar(60),    null);
            reqH.input('sCampo6',           sql.VarChar(60),    null);
            reqH.input('sCampo7',           sql.VarChar(60),    null);
            reqH.input('sCampo8',           sql.VarChar(60),    'Creado vía API Sync2k');
            reqH.input('sRevisado',         sql.Char(1),        null);
            reqH.input('sTrasnfe',          sql.Char(1),        null);
            reqH.input('sCo_Us_In',         sql.Char(6),        padProfit(auditUser, 6));
            reqH.input('sCo_Sucu_In',       sql.Char(6),        padProfit(coSucu, 6));
            reqH.input('sMaquina',          sql.VarChar(60),    'SYNC2K');
            reqH.input('bNac',              sql.Bit,            1);

            await reqH.execute('pInsertarFacturaCompra');

            // 5. Insertar Renglones
            let rengNum = 1;
            const recepcionesAfectadas = new Set();

            for (const item of payload.renglones) {
                const cant = Number(item.total_art || item.cantidad) || 0;
                if (cant <= 0) continue;

                const costUnit = Number(item.cost_unit || item.costo) || 0;
                const costUnitOM = Number(item.cost_unit_om) || (coMone === 'BS' ? (costUnit / (tasaCambio || 1)) : costUnit);
                const subtotal = cant * costUnit;
                const porcDesc = Number(item.porc_desc) || 0;
                const montoDesc = subtotal * (porcDesc / 100);
                const netoReng = subtotal - montoDesc;
                const tipoImp = item.tipo_imp ? String(item.tipo_imp).trim().substring(0, 1) : '1';
                const porcImp = Number(item.porc_imp) || 0;
                const montoImp = netoReng * (porcImp / 100);

                const tipoDocOrigen = (item.tipo_doc || (item.num_doc ? 'NREC' : null));
                const numDocOrigen = item.num_doc ? String(item.num_doc).trim() : null;
                const rengDocOrigen = item.reng_doc ? parseInt(item.reng_doc) : null;
                const rowguidDocOrigen = item.rowguid_doc || null;

                const reqR = new sql.Request(transaction);
                reqR.input('iReng_Num',              sql.Int,            rengNum);
                reqR.input('sDoc_Num',               sql.Char(20),       padProfit(docNum, 20));
                reqR.input('sCo_Art',                sql.Char(30),       padProfit(item.co_art, 30));
                reqR.input('sDes_Art',               sql.VarChar(120),   (item.des_art || item.art_des || '').substring(0, 120));
                reqR.input('sCo_Uni',                sql.Char(6),        padProfit(item.co_uni || '01', 6));
                reqR.input('sSCo_Uni',               sql.Char(6),        null);
                reqR.input('sCo_Alma',               sql.Char(6),        padProfit(item.co_alma || '01', 6));
                reqR.input('sTipo_Imp',              sql.Char(1),        tipoImp);
                reqR.input('sTipo_Imp2',             sql.Char(1),        null);
                reqR.input('sTipo_Imp3',             sql.Char(1),        null);
                reqR.input('sTipo_Doc',              sql.Char(4),        tipoDocOrigen ? padProfit(tipoDocOrigen, 4) : null);
                reqR.input('sPorc_Desc',             sql.Char(15),       String(porcDesc));
                reqR.input('sNum_Doc',               sql.Char(20),       numDocOrigen ? padProfit(numDocOrigen, 20) : null);
                reqR.input('gRowGuid_Doc',           sql.UniqueIdentifier, rowguidDocOrigen);
                reqR.input('deReng_Neto',            sql.Decimal(18, 2), netoReng);
                reqR.input('deCost_Unit',            sql.Decimal(18, 5), costUnit);
                reqR.input('deCost_Unit_OM',         sql.Decimal(18, 5), costUnitOM);
                reqR.input('deTotal_Art',            sql.Decimal(18, 5), cant);
                reqR.input('deSTotal_Art',           sql.Decimal(18, 5), 0);
                reqR.input('deOtros',                sql.Decimal(18, 5), 0);
                reqR.input('dePorc_Imp',             sql.Decimal(18, 5), porcImp);
                reqR.input('dePorc_Imp2',            sql.Decimal(18, 5), 0);
                reqR.input('dePorc_Imp3',            sql.Decimal(18, 5), 0);
                reqR.input('deMonto_Imp',            sql.Decimal(18, 5), montoImp);
                reqR.input('deMonto_Imp2',           sql.Decimal(18, 5), 0);
                reqR.input('deMonto_Imp3',           sql.Decimal(18, 5), 0);
                reqR.input('dePorc_Gas',             sql.Decimal(18, 2), 0);
                reqR.input('deTotal_Dev',            sql.Decimal(18, 5), 0);
                reqR.input('deMonto_Dev',            sql.Decimal(18, 5), 0);
                reqR.input('dePendiente2',           sql.Decimal(18, 5), 0);
                reqR.input('sComentario',            sql.VarChar(sql.MAX), null);
                reqR.input('bLote_Asignado',         sql.Bit,            0);
                reqR.input('deMonto_Desc_Glob',      sql.Decimal(18, 5), 0);
                reqR.input('deMonto_reca_Glob',      sql.Decimal(18, 5), 0);
                reqR.input('deOtros1_glob',          sql.Decimal(18, 5), 0);
                reqR.input('deOtros2_glob',          sql.Decimal(18, 5), 0);
                reqR.input('deOtros3_glob',          sql.Decimal(18, 5), 0);
                reqR.input('deMonto_imp_afec_glob',  sql.Decimal(18, 5), 0);
                reqR.input('deMonto_imp2_afec_glob', sql.Decimal(18, 5), 0);
                reqR.input('deMonto_imp3_afec_glob', sql.Decimal(18, 5), 0);
                reqR.input('deMonto_Desc',           sql.Decimal(18, 5), montoDesc);
                reqR.input('dePendiente',            sql.Decimal(18, 5), cant);
                reqR.input('iReng_Doc',              sql.Int,            rengDocOrigen);
                reqR.input('sDis_Cen',               sql.VarChar(sql.MAX), null);
                reqR.input('sCo_Sucu_In',            sql.Char(6),        padProfit(coSucu, 6));
                reqR.input('sCo_Us_In',              sql.Char(6),        padProfit(auditUser, 6));
                reqR.input('sRevisado',              sql.Char(1),        null);
                reqR.input('sTrasnfe',               sql.Char(1),        null);
                reqR.input('sMaquina',               sql.VarChar(60),    'SYNC2K');
                reqR.input('deCosto_Adi1',           sql.Decimal(18, 5), 0);
                reqR.input('deCosto_Adi2',           sql.Decimal(18, 5), 0);
                reqR.input('deCosto_Adi3',           sql.Decimal(18, 5), 0);
                reqR.input('sCredito_fiscal',        sql.VarChar(30),    'Totalmente Deducible (Art. 34)');

                await reqR.execute('pInsertarRenglonesFacturaCompra');

                // Si viene de Nota de Recepción, descontar pendiente
                if (numDocOrigen) {
                    recepcionesAfectadas.add(numDocOrigen);
                    const updateNrecReq = new sql.Request(transaction);
                    updateNrecReq.input('num_doc', sql.Char(20), padProfit(numDocOrigen, 20));
                    updateNrecReq.input('co_art', sql.Char(30), padProfit(item.co_art, 30));
                    updateNrecReq.input('cant', sql.Decimal(18, 5), cant);
                    if (rengDocOrigen) {
                        updateNrecReq.input('reng_num', sql.Int, rengDocOrigen);
                        await updateNrecReq.query(`
                            UPDATE saNotaRecepcionCompraReng
                            SET pendiente = CASE WHEN pendiente >= @cant THEN pendiente - @cant ELSE 0 END,
                                fe_us_mo = GETDATE()
                            WHERE doc_num = @num_doc AND reng_num = @reng_num;
                        `);
                    } else {
                        await updateNrecReq.query(`
                            UPDATE saNotaRecepcionCompraReng
                            SET pendiente = CASE WHEN pendiente >= @cant THEN pendiente - @cant ELSE 0 END,
                                fe_us_mo = GETDATE()
                            WHERE doc_num = @num_doc AND co_art = @co_art;
                        `);
                    }
                }

                rengNum++;
            }

            // 6. Actualizar status de las recepciones afectadas
            for (const nrecDoc of recepcionesAfectadas) {
                const statusReq = new sql.Request(transaction);
                statusReq.input('doc_num', sql.Char(20), padProfit(nrecDoc, 20));
                await statusReq.query(`
                    DECLARE @total_qty DECIMAL(18,5), @pending_qty DECIMAL(18,5);
                    SELECT @total_qty = ISNULL(SUM(total_art), 0), @pending_qty = ISNULL(SUM(pendiente), 0)
                    FROM saNotaRecepcionCompraReng
                    WHERE doc_num = @doc_num;

                    UPDATE saNotaRecepcionCompra
                    SET status = CASE 
                        WHEN @pending_qty = 0 THEN '2'
                        WHEN @pending_qty < @total_qty THEN '1'
                        ELSE '0'
                    END,
                    fe_us_mo = GETDATE()
                    WHERE doc_num = @doc_num;
                `);
                console.log(`🔄 [FACTURA COMPRA] Estado de Recepción ${nrecDoc} actualizado.`);
            }

            // 7. Insertar Documento en Cuentas por Pagar (saDocumentoCompra)
            try {
                const reqDoc = new sql.Request(transaction);
                reqDoc.input('sCo_Tipo_Doc',     sql.Char(4),        padProfit('FACT', 4));
                reqDoc.input('sNro_Doc',         sql.Char(20),       padProfit(docNum, 20));
                reqDoc.input('sCo_Prov',         sql.Char(16),       padProfit(payload.co_prov, 16));
                reqDoc.input('sCo_Cta_Ingr_Egr', sql.Char(20),       null);
                reqDoc.input('sCo_Mone',         sql.Char(6),        padProfit(coMone, 6));
                reqDoc.input('sNro_Fact',        sql.Char(20),       padProfit(String(payload.nro_fact).trim(), 20));
                reqDoc.input('sN_Control',       sql.Char(20),       payload.n_control ? padProfit(String(payload.n_control).trim(), 20) : padProfit('N/A', 20));
                reqDoc.input('sdFec_Emis',       sql.SmallDateTime,  fecEmis);
                reqDoc.input('sdFec_Venc',       sql.SmallDateTime,  fecVenc);
                reqDoc.input('sdFec_Reg',        sql.SmallDateTime,  fecReg);
                reqDoc.input('sObserva',         sql.VarChar(sql.MAX), `FACT N° ${payload.nro_fact} de proveedor ${payload.co_prov}`.substring(0, 120));
                reqDoc.input('bAnulado',         sql.Bit,            0);
                reqDoc.input('bAut',             sql.Bit,            1);
                reqDoc.input('deTasa',           sql.Decimal(18, 5), tasaCambio);
                reqDoc.input('deTotal_Bruto',    sql.Decimal(18, 2), totalBruto);
                reqDoc.input('deTotal_Neto',     sql.Decimal(18, 2), totalNeto);
                reqDoc.input('deSaldo',          sql.Decimal(18, 2), saldo);
                reqDoc.input('deMonto_Imp',      sql.Decimal(18, 2), totalImp);
                reqDoc.input('deMonto_Imp2',     sql.Decimal(18, 2), 0);
                reqDoc.input('deMonto_Imp3',     sql.Decimal(18, 2), 0);
                reqDoc.input('sPorc_Desc_Glob',  sql.Char(15),       String(descGlobalPorc));
                reqDoc.input('deMonto_Desc_Glob',sql.Decimal(18, 2), descGlobalMonto);
                reqDoc.input('sPorc_Reca',       sql.Char(15),       null);
                reqDoc.input('deMonto_Reca',     sql.Decimal(18, 2), 0);
                reqDoc.input('deOtros1',         sql.Decimal(18, 2), 0);
                reqDoc.input('deOtros2',         sql.Decimal(18, 2), 0);
                reqDoc.input('deOtros3',         sql.Decimal(18, 2), 0);
                reqDoc.input('sCo_Sucu_In',      sql.Char(6),        padProfit(coSucu, 6));
                reqDoc.input('sCo_Us_In',        sql.Char(6),        padProfit(auditUser, 6));
                reqDoc.input('sMaquina',         sql.VarChar(60),    'SYNC2K');

                await reqDoc.execute('pInsertarDocumentoCompra');
                console.log(`💳 [FACTURA COMPRA] Cuentas por Pagar registrada en saDocumentoCompra.`);
            } catch (docErr) {
                console.warn(`⚠️ [FACTURA COMPRA] Advertencia al insertar saDocumentoCompra:`, docErr.message);
                // Si pInsertarDocumentoCompra falla por parámetros específicos de versión, insertamos directo:
                await transaction.request()
                    .input('co_tipo_doc', sql.Char(4), 'FACT')
                    .input('nro_doc', sql.Char(20), padProfit(docNum, 20))
                    .input('nro_fact', sql.Char(20), padProfit(String(payload.nro_fact).trim(), 20))
                    .input('co_prov', sql.Char(16), padProfit(payload.co_prov, 16))
                    .input('co_mone', sql.Char(6), padProfit(coMone, 6))
                    .input('fec_emis', sql.SmallDateTime, fecEmis)
                    .input('fec_venc', sql.SmallDateTime, fecVenc)
                    .input('fec_reg', sql.SmallDateTime, fecReg)
                    .input('tasa', sql.Decimal(18, 5), tasaCambio)
                    .input('total_bruto', sql.Decimal(18, 2), totalBruto)
                    .input('total_neto', sql.Decimal(18, 2), totalNeto)
                    .input('saldo', sql.Decimal(18, 2), saldo)
                    .input('monto_imp', sql.Decimal(18, 2), totalImp)
                    .input('n_control', sql.Char(20), payload.n_control ? padProfit(String(payload.n_control).trim(), 20) : padProfit('N/A', 20))
                    .input('observa', sql.VarChar(120), `FACT N° ${payload.nro_fact} de prov ${payload.co_prov}`.substring(0, 120))
                    .input('co_us_in', sql.Char(6), padProfit(auditUser, 6))
                    .input('co_sucu_in', sql.Char(6), padProfit(coSucu, 6))
                    .query(`
                        IF NOT EXISTS (SELECT 1 FROM saDocumentoCompra WHERE nro_doc = @nro_doc AND co_tipo_doc = 'FACT')
                        BEGIN
                            INSERT INTO saDocumentoCompra (
                                co_tipo_doc, nro_doc, nro_fact, co_prov, co_mone,
                                fec_emis, fec_venc, fec_reg, tasa, total_bruto,
                                total_neto, saldo, monto_imp, n_control, observa,
                                co_us_in, co_sucu_in, fe_us_in, co_us_mo, co_sucu_mo, fe_us_mo,
                                anulado, aut
                            ) VALUES (
                                @co_tipo_doc, @nro_doc, @nro_fact, @co_prov, @co_mone,
                                @fec_emis, @fec_venc, @fec_reg, @tasa, @total_bruto,
                                @total_neto, @saldo, @monto_imp, @n_control, @observa,
                                @co_us_in, @co_sucu_in, GETDATE(), @co_us_in, @co_sucu_in, GETDATE(),
                                0, 1
                            );
                        END
                    `);
            }

            await transaction.commit();
            console.log(`✅ [FACTURA COMPRA] Documento ${docNum} creado con éxito.`);
            return {
                success: true,
                doc_num: docNum,
                nro_fact: payload.nro_fact,
                total_neto: totalNeto
            };
        } catch (err) {
            if (transaction._aborted === false) await transaction.rollback();
            throw err;
        }
    });

    return writeResponse(res, outcome);
});

// =========================================================================
// 5. ANULAR FACTURA DE COMPRA
// =========================================================================
router.post('/:doc_num/anular', async (req, res) => {
    try {
        const { doc_num } = req.params;
        const { sede } = req.query;

        const outcome = await executeWrite(sede || null, req.sqlAuth, async (pool) => {
            const resH = await pool.request().input('doc_num', sql.VarChar, doc_num).query(
                `SELECT rowguid, anulado, RTRIM(co_us_in) AS co_us_in, saldo, total_neto
                 FROM saFacturaCompra
                 WHERE LTRIM(RTRIM(doc_num)) = LTRIM(RTRIM(@doc_num))`
            );
            if (!resH.recordset.length) throw new Error('Factura de compra no existe.');

            const { anulado, saldo, total_neto } = resH.recordset[0];
            if (anulado) {
                throw new Error(`La factura de compra ${doc_num} ya se encuentra anulada.`);
            }

            // Fetch invoice lines to revert pending quantities in reception notes
            const resL = await pool.request().input('doc_num', sql.VarChar, doc_num).query(
                `SELECT reng_num, co_art, total_art, RTRIM(tipo_doc) AS tipo_doc, RTRIM(num_doc) AS num_doc, reng_doc, rowguid_doc 
                 FROM saFacturaCompraReng 
                 WHERE LTRIM(RTRIM(doc_num)) = LTRIM(RTRIM(@doc_num))`
            );

            const transaction = new sql.Transaction(pool);
            await transaction.begin();

            try {
                const auditUser = (req.profitUser || 'API').substring(0, 6).toUpperCase();

                // 1. Anular cabecera de la factura de compra
                await transaction.request()
                    .input('doc_num', sql.Char(20), padProfit(doc_num, 20))
                    .input('auditUser', sql.Char(6), padProfit(auditUser, 6))
                    .query(`
                        UPDATE saFacturaCompra
                        SET anulado = 1,
                            fe_us_mo = GETDATE(),
                            co_us_mo = @auditUser
                        WHERE LTRIM(RTRIM(doc_num)) = LTRIM(RTRIM(@doc_num))
                    `);

                // 2. Revertir pendientes en notas de recepción de compra origen
                const recepcionesAfectadas = new Set();
                for (const line of resL.recordset) {
                    if (line.tipo_doc === 'NREC' && line.num_doc) {
                        recepcionesAfectadas.add(line.num_doc);
                        const rRevert = new sql.Request(transaction);
                        rRevert.input('qty', sql.Decimal(18, 5), line.total_art);
                        rRevert.input('num_doc', sql.Char(20), padProfit(line.num_doc, 20));
                        rRevert.input('co_art', sql.Char(30), padProfit(line.co_art, 30));

                        if (line.reng_doc) {
                            rRevert.input('reng_num', sql.Int, line.reng_doc);
                            await rRevert.query(`
                                UPDATE saNotaRecepcionCompraReng
                                SET pendiente = CASE WHEN pendiente + @qty > total_art THEN total_art ELSE pendiente + @qty END,
                                    fe_us_mo = GETDATE()
                                WHERE doc_num = @num_doc AND reng_num = @reng_num;
                            `);
                        } else {
                            await rRevert.query(`
                                UPDATE saNotaRecepcionCompraReng
                                SET pendiente = CASE WHEN pendiente + @qty > total_art THEN total_art ELSE pendiente + @qty END,
                                    fe_us_mo = GETDATE()
                                WHERE doc_num = @num_doc AND co_art = @co_art;
                            `);
                        }
                    }
                }

                // Actualizar status de las recepciones afectadas
                for (const nrecDoc of recepcionesAfectadas) {
                    const statusReq = new sql.Request(transaction);
                    statusReq.input('doc_num', sql.Char(20), padProfit(nrecDoc, 20));
                    await statusReq.query(`
                        DECLARE @total_qty DECIMAL(18,5), @pending_qty DECIMAL(18,5);
                        SELECT @total_qty = ISNULL(SUM(total_art), 0), @pending_qty = ISNULL(SUM(pendiente), 0)
                        FROM saNotaRecepcionCompraReng
                        WHERE doc_num = @doc_num;

                        UPDATE saNotaRecepcionCompra
                        SET status = CASE 
                            WHEN @pending_qty = 0 THEN '2'
                            WHEN @pending_qty < @total_qty THEN '1'
                            ELSE '0'
                        END,
                        fe_us_mo = GETDATE()
                        WHERE doc_num = @doc_num;
                    `);
                }

                // 3. Anular documento de cuentas por pagar en saDocumentoCompra
                await transaction.request()
                    .input('doc_num', sql.Char(20), padProfit(doc_num, 20))
                    .input('auditUser', sql.Char(6), padProfit(auditUser, 6))
                    .query(`
                        UPDATE saDocumentoCompra
                        SET anulado = 1,
                            fe_us_mo = GETDATE(),
                            co_us_mo = @auditUser
                        WHERE LTRIM(RTRIM(nro_doc)) = LTRIM(RTRIM(@doc_num))
                          AND co_tipo_doc = 'FACT'
                    `);

                await transaction.commit();
                return { success: true, doc_num: doc_num };
            } catch (err) {
                if (transaction._aborted === false) await transaction.rollback();
                throw err;
            }
        });

        return writeResponse(res, outcome);
    } catch (error) {
        res.status(500).json({ success: false, message: 'Error al anular Factura de Compra.', error: error.message });
    }
});

module.exports = router;
