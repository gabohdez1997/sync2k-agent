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
        const { sede, doc_num, nro_fact, n_control, co_prov, co_us_in, fec_d, fec_h, search, status } = req.query;
        
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
                if (co_us_in) {
                    request.input('co_us_in_filter', sql.VarChar, co_us_in.trim().toUpperCase());
                    whereClauses.push("LTRIM(RTRIM(f.co_us_in)) = @co_us_in_filter");
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
        // 1. Resolver datos maestros y moneda
        const [resMoneda, resUSD, resCond, resTasa] = await Promise.all([
            pool.request().query(`SELECT TOP 1 RTRIM(g_moneda) AS g_moneda FROM par_emp`),
            pool.request().query(`SELECT TOP 1 RTRIM(co_mone)  AS co_mone   FROM saMoneda WHERE LTRIM(RTRIM(co_mone)) IN ('US$','USD','DOL','$','US') OR mone_des LIKE '%Dolar%'`),
            pool.request().input('co_cond', sql.VarChar, payload.co_cond || '01').query(`SELECT TOP 1 co_cond, dias_cred FROM saCondicionPago WHERE co_cond = @co_cond`),
            getExchangeRate(pool)
        ]);

        const coMone = resUSD.recordset[0]?.co_mone || 'USD';
        const currentTasa = resTasa || 1;
        let tasaDoc = Number(payload.tasa || currentTasa);
        if (tasaDoc <= 1 && currentTasa > 1) {
            tasaDoc = currentTasa;
        }
        const diasCred = Number(resCond.recordset[0]?.dias_cred) || 0;

        const ts = new Date();
        const fecEmis = payload.fec_emis ? new Date(`${safeDate(payload.fec_emis)}T00:00:00`) : ts;
        let fecVenc = payload.fec_venc ? new Date(`${safeDate(payload.fec_venc)}T00:00:00`) : new Date(fecEmis.getTime() + (diasCred * 86400000));
        if (fecVenc < fecEmis) fecVenc = fecEmis;
        let fecReg = ts;
        if (fecReg < fecEmis) fecReg = fecEmis;

        // 2. Pre-calcular Totales e IVA
        // Al igual que en Factura de Ventas (/dashboard/billing y routes/facturas.js),
        // el documento se emite en USD con su tasa de cambio, pero todos los montos base
        // (cost_unit, reng_neto, total_bruto, total_neto, saldo) se almacenan en Bolívares (BS).
        let totalBruto = 0;
        let totalImp = 0;
        let totalNeto = 0;

        for (const item of payload.renglones) {
            const cant = Number(item.total_art || item.cantidad) || 0;
            if (cant <= 0) continue;

            const unitCostUSD = Number(item.cost_unit_om != null ? item.cost_unit_om : (item.cost_unit || item.costo)) || 0;
            const costUnitBs = Math.round((unitCostUSD * tasaDoc) * 100000) / 100000;
            const costUnit = costUnitBs;

            const subtotal = Math.round((cant * costUnit) * 100) / 100;
            const porcDesc = Number(item.porc_desc) || 0;
            const montoDesc = Math.round((subtotal * (porcDesc / 100)) * 100) / 100;
            const netoReng = subtotal - montoDesc;
            const porcImp = Number(item.porc_imp) || 0;
            const montoImp = Math.round((netoReng * (porcImp / 100)) * 100) / 100;

            totalBruto += subtotal;
            totalImp += montoImp;
            totalNeto += (netoReng + montoImp);
        }

        const descGlobalPorc = Number(payload.porc_desc_glob) || 0;
        const descGlobalMonto = Math.round((totalBruto * (descGlobalPorc / 100)) * 100) / 100;
        totalNeto -= descGlobalMonto;
        const saldo = totalNeto;

        // 3. Determinar Sucursal (co_sucu_in, co_sucu_mo) según IVA (idéntico a /dashboard/billing y facturas.js)
        // Si tiene IVA se guarda con la sucursal por defecto según la sede, sino por la otra sucursal
        const branchCodes = srv.profit_branch_codes || [];
        const defaultCodeObj = branchCodes.find(b => b.is_default === true) || branchCodes[0] || { code: srv.co_sucu || '01' };
        const nonDefaultCodeObj = branchCodes.find(b => b.is_default === false) || defaultCodeObj;

        let sucuCode;
        if (payload.force_sucu) {
            sucuCode = String(payload.force_sucu).trim();
        } else if (payload.co_sucu) {
            sucuCode = String(payload.co_sucu).trim();
        } else {
            sucuCode = totalImp === 0 ? nonDefaultCodeObj.code : defaultCodeObj.code;
        }
        console.log(`🏢 [FACTURA COMPRA] Resolviendo sucursal. IVA = ${totalImp}, force_sucu = ${payload.force_sucu || payload.co_sucu || 'N/A'}. Sucu asignada = ${sucuCode} (Default = ${defaultCodeObj.code}, Non-Default = ${nonDefaultCodeObj.code})`);

        // Iniciar Transacción SQL
        const transaction = new sql.Transaction(pool);
        await transaction.begin();

        try {
            // 4. Obtener correlativo consecutivo para saFacturaCompra según la sucursal
            const consecutivoInfo = await getProximoConsecutivo({
                runner: transaction,
                co_tipo_serie: 'FACTURA_COMPRA',
                co_sucur: sucuCode,
                table: 'saFacturaCompra',
                col: 'doc_num'
            });
            const docNum = consecutivoInfo.docNum;
            console.log(`📦 [FACTURA COMPRA] Asignando Correlativo: ${docNum} para sede: ${srv.name}, sucursal: ${sucuCode}`);

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
            reqH.input('deTasa',            sql.Decimal(18, 5), tasaDoc);
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
            reqH.input('sCo_Sucu_In',       sql.Char(6),        padProfit(sucuCode, 6));
            reqH.input('sMaquina',          sql.VarChar(60),    'SYNC2K');
            reqH.input('bNac',              sql.Bit,            1);

            await reqH.execute('pInsertarFacturaCompra');

            // 4.1 Snapshot de precios y márgenes previos para rollback si update_prices === false
            const coArtsInFactura = [...new Set(payload.renglones.map(r => r.co_art).filter(Boolean))];
            let snapshotPrecios = [];
            if (coArtsInFactura.length > 0) {
                const idsPrecios = coArtsInFactura.map(id => `'${id.replace(/'/g, "''")}'`).join(',');
                try {
                    const snapRes = await pool.request().query(`
                        SELECT RTRIM(p.co_art) AS co_art, RTRIM(p.co_precio) AS co_precio, p.monto, p.co_mone,
                               ISNULL(m.monto_min, 0) AS monto_min, ISNULL(m.monto_max, 0) AS monto_max
                        FROM saArtPrecio p
                        LEFT JOIN saArtMargen m ON p.co_art = m.co_art AND p.co_precio = m.co_precio
                        WHERE LTRIM(RTRIM(p.co_art)) IN (${idsPrecios})
                    `);
                    snapshotPrecios = snapRes.recordset || [];
                    console.log(`📸 [FACTURA COMPRA] Snapshot de precios tomado para ${snapshotPrecios.length} registros.`);
                } catch (snapErr) {
                    console.warn(`⚠️ [FACTURA COMPRA] Error obteniendo snapshot de precios: ${snapErr.message}`);
                }
            }

            // 5. Insertar Renglones
            let rengNum = 1;
            const recepcionesAfectadas = new Set();

            for (const item of payload.renglones) {
                const cant = Number(item.total_art || item.cantidad) || 0;
                if (cant <= 0) continue;

                const unitCostUSD = Number(item.cost_unit_om != null ? item.cost_unit_om : (item.cost_unit || item.costo)) || 0;
                const costUnitBs = Math.round((unitCostUSD * tasaDoc) * 100000) / 100000;
                const costUnitOM = unitCostUSD;
                const costUnit = costUnitBs;

                const subtotal = Math.round((cant * costUnit) * 100) / 100;
                const porcDesc = Number(item.porc_desc) || 0;
                const montoDesc = Math.round((subtotal * (porcDesc / 100)) * 100) / 100;
                const netoReng = subtotal - montoDesc;
                // Profit Plus Compras: '1' = General (16%), '2' = Reducida (8%), '3' = Adicional (31%), '6' = Compra Exenta (0%)
                let tipoImp = item.tipo_imp ? String(item.tipo_imp).trim().substring(0, 1) : '1';
                const porcImp = Number(item.porc_imp) || 0;
                if (porcImp === 0) {
                    tipoImp = (tipoImp === '7') ? '7' : '6'; // '6' = Compra Exenta
                } else if (tipoImp === '5' || !['1', '2', '3'].includes(tipoImp)) {
                    tipoImp = '1';
                }
                const montoImp = Math.round((netoReng * (porcImp / 100)) * 100) / 100;

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
                reqR.input('sCo_Sucu_In',            sql.Char(6),        padProfit(sucuCode, 6));
                reqR.input('sCo_Us_In',              sql.Char(6),        padProfit(auditUser, 6));
                reqR.input('sRevisado',              sql.Char(1),        null);
                reqR.input('sTrasnfe',               sql.Char(1),        null);
                reqR.input('sMaquina',               sql.VarChar(60),    'SYNC2K');
                reqR.input('deCosto_Adi1',           sql.Decimal(18, 5), 0);
                reqR.input('deCosto_Adi2',           sql.Decimal(18, 5), 0);
                reqR.input('deCosto_Adi3',           sql.Decimal(18, 5), 0);
                reqR.input('sCredito_fiscal',        sql.VarChar(30),    'Totalmente Deducible (Art. 34)');

                try {
                    await reqR.execute('pInsertarRenglonesFacturaCompra');
                } catch (spErr) {
                    console.warn(`⚠️ [FACTURA COMPRA] SP pInsertarRenglonesFacturaCompra falló: ${spErr.message}. Insertando directo...`);
                    // Fallback: INSERT directo con NEWID() para rowguid
                    const rowguid = require('crypto').randomUUID();
                    const directReq = new sql.Request(transaction);
                    directReq.input('rowguid', sql.UniqueIdentifier, rowguid);
                    directReq.input('doc_num', sql.Char(20), padProfit(docNum, 20));
                    directReq.input('reng_num', sql.Int, rengNum);
                    directReq.input('co_art', sql.Char(30), padProfit(item.co_art, 30));
                    directReq.input('des_art', sql.VarChar(120), (item.des_art || item.art_des || '').substring(0, 120));
                    directReq.input('co_uni', sql.Char(6), padProfit(item.co_uni || '01', 6));
                    directReq.input('sco_uni', sql.Char(6), null);
                    directReq.input('co_alma', sql.Char(6), padProfit(item.co_alma || '01', 6));
                    directReq.input('tipo_imp_v', sql.Char(1), tipoImp);
                    directReq.input('tipo_imp2', sql.Char(1), null);
                    directReq.input('tipo_imp3', sql.Char(1), null);
                    directReq.input('tipo_doc', sql.Char(4), tipoDocOrigen ? padProfit(tipoDocOrigen, 4) : null);
                    directReq.input('num_doc', sql.Char(20), numDocOrigen ? padProfit(numDocOrigen, 20) : null);
                    directReq.input('rowguid_doc', sql.UniqueIdentifier, rowguidDocOrigen);
                    directReq.input('reng_neto', sql.Decimal(18, 2), netoReng);
                    directReq.input('cost_unit', sql.Decimal(18, 5), costUnit);
                    directReq.input('cost_unit_om', sql.Decimal(18, 5), costUnitOM);
                    directReq.input('total_art', sql.Decimal(18, 5), cant);
                    directReq.input('stotal_art', sql.Decimal(18, 5), 0);
                    directReq.input('otros', sql.Decimal(18, 5), 0);
                    directReq.input('porc_imp', sql.Decimal(18, 5), porcImp);
                    directReq.input('porc_imp2', sql.Decimal(18, 5), 0);
                    directReq.input('porc_imp3', sql.Decimal(18, 5), 0);
                    directReq.input('monto_imp', sql.Decimal(18, 5), montoImp);
                    directReq.input('monto_imp2', sql.Decimal(18, 5), 0);
                    directReq.input('monto_imp3', sql.Decimal(18, 5), 0);
                    directReq.input('porc_gas', sql.Decimal(18, 2), 0);
                    directReq.input('total_dev', sql.Decimal(18, 5), 0);
                    directReq.input('monto_dev', sql.Decimal(18, 5), 0);
                    directReq.input('pendiente2', sql.Decimal(18, 5), 0);
                    directReq.input('comentario', sql.VarChar(sql.MAX), null);
                    directReq.input('lote_asignado', sql.Bit, 0);
                    directReq.input('monto_desc_glob', sql.Decimal(18, 5), 0);
                    directReq.input('monto_reca_glob', sql.Decimal(18, 5), 0);
                    directReq.input('otros1_glob', sql.Decimal(18, 5), 0);
                    directReq.input('otros2_glob', sql.Decimal(18, 5), 0);
                    directReq.input('otros3_glob', sql.Decimal(18, 5), 0);
                    directReq.input('monto_imp_afec_glob', sql.Decimal(18, 5), 0);
                    directReq.input('monto_imp2_afec_glob', sql.Decimal(18, 5), 0);
                    directReq.input('monto_imp3_afec_glob', sql.Decimal(18, 5), 0);
                    directReq.input('monto_desc', sql.Decimal(18, 5), montoDesc);
                    directReq.input('pendiente', sql.Decimal(18, 5), cant);
                    directReq.input('porc_desc', sql.Char(15), String(porcDesc));
                    directReq.input('dis_cen', sql.VarChar(sql.MAX), null);
                    directReq.input('co_sucu_in', sql.Char(6), padProfit(coSucu, 6));
                    directReq.input('co_us_in', sql.Char(6), padProfit(auditUser, 6));
                    directReq.input('maquina', sql.VarChar(60), 'SYNC2K');
                    directReq.input('costo_adi1', sql.Decimal(18, 5), 0);
                    directReq.input('costo_adi2', sql.Decimal(18, 5), 0);
                    directReq.input('costo_adi3', sql.Decimal(18, 5), 0);
                    directReq.input('credito_fiscal', sql.VarChar(30), 'Totalmente Deducible (Art. 34)');

                    await directReq.query(`
                        INSERT INTO saFacturaCompraReng (
                            rowguid, doc_num, reng_num, co_art, des_art, co_uni, sco_uni, co_alma,
                            tipo_imp, tipo_imp2, tipo_imp3, tipo_doc, num_doc, rowguid_doc,
                            reng_neto, cost_unit, cost_unit_om, total_art, stotal_art, otros,
                            porc_imp, porc_imp2, porc_imp3, monto_imp, monto_imp2, monto_imp3,
                            porc_gas, total_dev, monto_dev, pendiente2, comentario, lote_asignado,
                            monto_desc_glob, monto_reca_glob, otros1_glob, otros2_glob, otros3_glob,
                            monto_imp_afec_glob, monto_imp2_afec_glob, monto_imp3_afec_glob,
                            monto_desc, pendiente, porc_desc, dis_cen,
                            co_sucu_in, co_us_in, fe_us_in, co_sucu_mo, co_us_mo, fe_us_mo,
                            revisado, trasnfe, maquina, costo_adi1, costo_adi2, costo_adi3
                        ) VALUES (
                            @rowguid, @doc_num, @reng_num, @co_art, @des_art, @co_uni, @sco_uni, @co_alma,
                            @tipo_imp_v, @tipo_imp2, @tipo_imp3, @tipo_doc, @num_doc, @rowguid_doc,
                            @reng_neto, @cost_unit, @cost_unit_om, @total_art, @stotal_art, @otros,
                            @porc_imp, @porc_imp2, @porc_imp3, @monto_imp, @monto_imp2, @monto_imp3,
                            @porc_gas, @total_dev, @monto_dev, @pendiente2, @comentario, @lote_asignado,
                            @monto_desc_glob, @monto_reca_glob, @otros1_glob, @otros2_glob, @otros3_glob,
                            @monto_imp_afec_glob, @monto_imp2_afec_glob, @monto_imp3_afec_glob,
                            @monto_desc, @pendiente, @porc_desc, @dis_cen,
                            @co_sucu_in, @co_us_in, GETDATE(), @co_sucu_in, @co_us_in, GETDATE(),
                            NULL, NULL, @maquina, @costo_adi1, @costo_adi2, @costo_adi3
                        );

                        -- Insert into Ext table if it exists
                        IF OBJECT_ID('saFacturaCompraRengExt', 'U') IS NOT NULL
                        BEGIN
                            IF NOT EXISTS (SELECT 1 FROM saFacturaCompraRengExt WHERE rowguid_reng = @rowguid)
                            BEGIN
                                INSERT INTO saFacturaCompraRengExt (rowguid_reng, credito_fiscal)
                                VALUES (@rowguid, @credito_fiscal);
                            END
                        END
                    `);
                    console.log(`✅ [FACTURA COMPRA] Renglón ${rengNum} insertado vía INSERT directo.`);
                }

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

            // 5.1 ROLLBACK DE PRECIOS SI EL USUARIO DECIDIÓ NO ACTUALIZARLOS
            // La función nativa de Profit Plus (pInsertarRenglonesFacturaCompra) actualiza costos y precios.
            // Si el usuario decide no hacerlo, ejecutamos un 2do update para hacer rollback a los precios originales.
            if (payload.update_prices === false && snapshotPrecios.length > 0) {
                console.log(`🔄 [FACTURA COMPRA] update_prices = false. Ejecutando 2do UPDATE para rollback de precios a valores originales...`);
                for (const snap of snapshotPrecios) {
                    const rbReq = new sql.Request(transaction);
                    rbReq.input('co_art', sql.Char(30), padProfit(snap.co_art, 30));
                    rbReq.input('co_precio', sql.Char(6), padProfit(snap.co_precio, 6));
                    rbReq.input('monto', sql.Decimal(18, 5), snap.monto);
                    rbReq.input('margen', sql.Decimal(18, 5), snap.monto_min);
                    rbReq.input('user', sql.Char(6), padProfit(auditUser, 6));
                    await rbReq.query(`
                        UPDATE saArtPrecio
                        SET monto = @monto,
                            co_us_mo = @user,
                            fe_us_mo = GETDATE()
                        WHERE LTRIM(RTRIM(co_art)) = LTRIM(RTRIM(@co_art))
                          AND LTRIM(RTRIM(co_precio)) = LTRIM(RTRIM(@co_precio));

                        UPDATE saArtMargen
                        SET monto_min = @margen,
                            monto_max = @margen,
                            co_us_mo = @user,
                            fe_us_mo = GETDATE()
                        WHERE LTRIM(RTRIM(co_art)) = LTRIM(RTRIM(@co_art))
                          AND LTRIM(RTRIM(co_precio)) = LTRIM(RTRIM(@co_precio));
                    `);
                }
                console.log(`✅ [FACTURA COMPRA] Rollback de precios completado exitosamente para ${snapshotPrecios.length} registros.`);
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
                reqDoc.input('sCo_Tipo_Doc',     sql.Char(6),        padProfit('FACT', 6));
                reqDoc.input('sNro_Doc',         sql.Char(20),       padProfit(docNum, 20));
                reqDoc.input('sNro_Fact',        sql.Char(20),       padProfit(String(payload.nro_fact).trim(), 20));
                reqDoc.input('sCo_Mone',         sql.Char(6),        padProfit(coMone, 6));
                reqDoc.input('sCo_Prov',         sql.Char(16),       padProfit(payload.co_prov, 16));
                reqDoc.input('sCo_Cta_Ingr_Egr', sql.Char(20),       null);
                reqDoc.input('sDoc_Orig',        sql.Char(6),        padProfit('FACT', 6));
                reqDoc.input('sMov_Ban',         sql.Char(20),       null);
                reqDoc.input('sNro_Orig',        sql.Char(20),       padProfit(docNum, 20));
                reqDoc.input('sNro_Che',         sql.Char(20),       null);
                reqDoc.input('sPorc_Reca',       sql.Char(15),       null);
                reqDoc.input('sPorc_Desc_Glob',  sql.Char(15),       String(descGlobalPorc));
                reqDoc.input('bAnulado',         sql.Bit,            0);
                reqDoc.input('bAut',             sql.Bit,            1);
                reqDoc.input('iPagar',           sql.Int,            0);
                reqDoc.input('sObserva',         sql.VarChar(120),   `FACT N° ${payload.nro_fact} de proveedor ${payload.co_prov}`.substring(0, 120));
                reqDoc.input('sTipo_Imp',        sql.Char(1),        '1');
                reqDoc.input('sTipo_Imp2',       sql.Char(1),        null);
                reqDoc.input('sTipo_Imp3',       sql.Char(1),        null);
                reqDoc.input('sdFec_Reg',        sql.SmallDateTime,  fecReg);
                reqDoc.input('sdFec_Emis',       sql.SmallDateTime,  fecEmis);
                reqDoc.input('sdFec_Venc',       sql.SmallDateTime,  fecVenc);
                reqDoc.input('deTotal_Neto',     sql.Decimal(18, 2), totalNeto);
                reqDoc.input('deTasa',           sql.Decimal(21, 8), tasaDoc);
                reqDoc.input('dePorc_Imp',       sql.Decimal(18, 5), totalBruto > 0 ? ((totalImp / totalBruto) * 100) : 0);
                reqDoc.input('dePorc_Imp2',      sql.Decimal(18, 5), 0);
                reqDoc.input('dePorc_Imp3',      sql.Decimal(18, 5), 0);
                reqDoc.input('deMonto_Imp',      sql.Decimal(18, 2), totalImp);
                reqDoc.input('deMonto_Imp2',     sql.Decimal(18, 2), 0);
                reqDoc.input('deMonto_Imp3',     sql.Decimal(18, 2), 0);
                reqDoc.input('deTotal_Bruto',    sql.Decimal(18, 2), totalBruto);
                reqDoc.input('deMonto_Desc_Glob',sql.Decimal(18, 2), descGlobalMonto);
                reqDoc.input('deMonto_Reca',     sql.Decimal(18, 2), 0);
                reqDoc.input('deSaldo',          sql.Decimal(18, 2), saldo);
                reqDoc.input('deAdicional',      sql.Decimal(18, 2), 0);
                reqDoc.input('deOtros1',         sql.Decimal(18, 2), 0);
                reqDoc.input('deOtros2',         sql.Decimal(18, 2), 0);
                reqDoc.input('deOtros3',         sql.Decimal(18, 2), 0);
                reqDoc.input('sPro_Pago',        sql.VarChar(sql.MAX), null);
                reqDoc.input('sSalestax',        sql.Char(8),        null);
                reqDoc.input('sProv_Ter',        sql.Char(16),       null);
                reqDoc.input('iReng_Ter',        sql.Int,            0);
                reqDoc.input('iTipo_Origen',     sql.Int,            0);
                reqDoc.input('sNum_Comprobante', sql.Char(14),       null);
                reqDoc.input('sDis_Cen',         sql.VarChar(sql.MAX), null);
                reqDoc.input('sN_Control',       sql.Char(20),       payload.n_control ? padProfit(String(payload.n_control).trim(), 20) : padProfit('N/A', 20));
                reqDoc.input('sCampo1',          sql.VarChar(60),    null);
                reqDoc.input('sCampo2',          sql.VarChar(60),    null);
                reqDoc.input('sCampo3',          sql.VarChar(60),    null);
                reqDoc.input('sCampo4',          sql.VarChar(60),    null);
                reqDoc.input('sCampo5',          sql.VarChar(60),    null);
                reqDoc.input('sCampo6',          sql.VarChar(60),    null);
                reqDoc.input('sCampo7',          sql.VarChar(60),    null);
                reqDoc.input('sCampo8',          sql.VarChar(60),    null);
                reqDoc.input('sRevisado',        sql.Char(1),        null);
                reqDoc.input('sTrasnfe',         sql.Char(1),        null);
                reqDoc.input('sco_sucu_in',      sql.Char(6),        padProfit(sucuCode, 6));
                reqDoc.input('sco_us_in',        sql.Char(6),        padProfit(auditUser, 6));
                reqDoc.input('sMaquina',         sql.VarChar(60),    'SYNC2K');
                reqDoc.input('bNac',             sql.Bit,            0);

                await reqDoc.execute('pInsertarDocumentoCompra');
                console.log(`💳 [FACTURA COMPRA] Cuentas por Pagar registrada en saDocumentoCompra (vía SP).`);
            } catch (docErr) {
                console.warn(`⚠️ [FACTURA COMPRA] Advertencia al insertar saDocumentoCompra vía SP:`, docErr.message);
                console.log(`🔄 [FACTURA COMPRA] Intentando INSERT directo en saDocumentoCompra...`);
                // Fallback direct INSERT con TODOS los campos requeridos por saDocumentoCompra
                const directReq = new sql.Request(transaction);
                directReq.input('co_tipo_doc',    sql.Char(6), padProfit('FACT', 6));
                directReq.input('nro_doc',        sql.Char(20), padProfit(docNum, 20));
                directReq.input('nro_fact',       sql.Char(20), padProfit(String(payload.nro_fact).trim(), 20));
                directReq.input('co_mone',        sql.Char(6), padProfit(coMone, 6));
                directReq.input('co_prov',        sql.Char(16), padProfit(payload.co_prov, 16));
                directReq.input('doc_orig',       sql.Char(6), padProfit('FACT', 6));
                directReq.input('nro_orig',       sql.Char(20), padProfit(docNum, 20));
                directReq.input('monto_reca',     sql.Decimal(18, 2), 0);
                directReq.input('monto_desc_glob',sql.Decimal(18, 2), descGlobalMonto);
                directReq.input('porc_desc_glob', sql.Char(15), String(descGlobalPorc));
                directReq.input('anulado',        sql.Bit, 0);
                directReq.input('aut',            sql.Bit, 1);
                directReq.input('pagar',          sql.Int, 0);
                directReq.input('observa',        sql.VarChar(120), `FACT N° ${payload.nro_fact} de prov ${payload.co_prov}`.substring(0, 120));
                directReq.input('tipo_imp',       sql.Char(1), '1');
                directReq.input('fec_reg',        sql.SmallDateTime, fecReg);
                directReq.input('fec_emis',       sql.SmallDateTime, fecEmis);
                directReq.input('fec_venc',       sql.SmallDateTime, fecVenc);
                directReq.input('porc_imp',       sql.Decimal(18, 5), totalBruto > 0 ? ((totalImp / totalBruto) * 100) : 0);
                directReq.input('porc_imp2',      sql.Decimal(18, 5), 0);
                directReq.input('porc_imp3',      sql.Decimal(18, 5), 0);
                directReq.input('monto_imp',      sql.Decimal(18, 2), totalImp);
                directReq.input('monto_imp2',     sql.Decimal(18, 2), 0);
                directReq.input('monto_imp3',     sql.Decimal(18, 2), 0);
                directReq.input('tasa',           sql.Decimal(21, 8), tasaDoc);
                directReq.input('total_bruto',    sql.Decimal(18, 2), totalBruto);
                directReq.input('total_neto',     sql.Decimal(18, 2), totalNeto);
                directReq.input('saldo',          sql.Decimal(18, 2), saldo);
                directReq.input('adicional',      sql.Decimal(18, 2), 0);
                directReq.input('otros1',         sql.Decimal(18, 2), 0);
                directReq.input('otros2',         sql.Decimal(18, 2), 0);
                directReq.input('otros3',         sql.Decimal(18, 2), 0);
                directReq.input('reng_ter',       sql.Int, 0);
                directReq.input('tipo_origen',    sql.Int, 0);
                directReq.input('n_control',      sql.Char(20), payload.n_control ? padProfit(String(payload.n_control).trim(), 20) : padProfit('N/A', 20));
                directReq.input('co_us_in',       sql.Char(6), padProfit(auditUser, 6));
                directReq.input('co_sucu_in',     sql.Char(6), padProfit(sucuCode, 6));

                await directReq.query(`
                    IF NOT EXISTS (SELECT 1 FROM saDocumentoCompra WHERE nro_doc = @nro_doc AND co_tipo_doc = 'FACT')
                    BEGIN
                        INSERT INTO saDocumentoCompra (
                            co_tipo_doc, nro_doc, nro_fact, co_mone, co_prov, doc_orig, nro_orig,
                            porc_desc_glob, monto_desc_glob, monto_reca, anulado, aut, pagar,
                            observa, tipo_imp, fec_reg, fec_emis, fec_venc, porc_imp, porc_imp2,
                            porc_imp3, monto_imp, monto_imp2, monto_imp3, tasa, total_bruto,
                            total_neto, saldo, adicional, otros1, otros2, otros3, reng_ter,
                            tipo_origen, n_control, co_us_in, co_sucu_in, fe_us_in, co_us_mo,
                            co_sucu_mo, fe_us_mo
                        ) VALUES (
                            @co_tipo_doc, @nro_doc, @nro_fact, @co_mone, @co_prov, @doc_orig, @nro_orig,
                            @porc_desc_glob, @monto_desc_glob, @monto_reca, @anulado, @aut, @pagar,
                            @observa, @tipo_imp, @fec_reg, @fec_emis, @fec_venc, @porc_imp, @porc_imp2,
                            @porc_imp3, @monto_imp, @monto_imp2, @monto_imp3, @tasa, @total_bruto,
                            @total_neto, @saldo, @adicional, @otros1, @otros2, @otros3, @reng_ter,
                            @tipo_origen, @n_control, @co_us_in, @co_sucu_in, GETDATE(), @co_us_in,
                            @co_sucu_in, GETDATE()
                        );
                    END
                `);
                console.log(`💳 [FACTURA COMPRA] Cuentas por Pagar registrada en saDocumentoCompra (vía INSERT directo).`);
            }

            await transaction.commit();
            console.log(`✅ [FACTURA COMPRA] Documento ${docNum} creado con éxito.`);

            // Si el usuario aceptó actualizar precios y tiene broadcast activado, propagar a las demás sedes
            if (payload.update_prices === true && payload.broadcast_prices !== false && Array.isArray(payload.price_updates) && payload.price_updates.length > 0) {
                const otherServers = getServers().filter(s => s.id !== srv.id);
                if (otherServers.length > 0) {
                    console.log(`📡 [FACTURA COMPRA] Propagando precios a ${otherServers.length} sedes adicionales (Broadcast)...`);
                    for (const otherSrv of otherServers) {
                        try {
                            const oPool = await getPool(otherSrv.id, req.sqlAuth);
                            for (const it of payload.price_updates) {
                                const coArt = String(it.co_art || '').trim();
                                if (!coArt || !Array.isArray(it.precios)) continue;
                                for (const p of it.precios) {
                                    const numPrecio = parseInt(p.id_precio, 10);
                                    if (isNaN(numPrecio)) continue;
                                    const precioMonto = Number(p.precio_nuevo != null ? p.precio_nuevo : p.precio) || 0;
                                    const margenMonto = Number(p.margen) || 0;
                                    const precioId = String(numPrecio);
                                    const rOther = oPool.request()
                                        .input('co_art', sql.Char(30), padProfit(coArt, 30))
                                        .input('co_precio', sql.Char(6), precioId)
                                        .input('monto', sql.Decimal(18, 5), precioMonto)
                                        .input('margen', sql.Decimal(18, 5), margenMonto)
                                        .input('user', sql.Char(6), padProfit(auditUser, 6));

                                    await rOther.query(`
                                        DECLARE @real_co_precio CHAR(6);
                                        SELECT TOP 1 @real_co_precio = co_precio 
                                        FROM saTipoPrecio 
                                        WHERE co_precio = @co_precio OR co_precio = RIGHT('0' + LTRIM(RTRIM(@co_precio)), 2);
                                        IF @real_co_precio IS NULL SET @real_co_precio = @co_precio;

                                        IF @monto <= 0
                                        BEGIN
                                            DELETE FROM saArtPrecio WHERE co_art = @co_art AND (co_precio = @real_co_precio OR co_precio = @co_precio);
                                        END
                                        ELSE
                                        BEGIN
                                            UPDATE saArtPrecio SET monto = @monto, fe_us_mo = GETDATE(), co_us_mo = @user
                                            WHERE co_art = @co_art AND (co_precio = @real_co_precio OR co_precio = @co_precio);
                                        END

                                        IF @margen <= 0
                                        BEGIN
                                            DELETE FROM saArtMargen WHERE co_art = @co_art AND (co_precio = @real_co_precio OR co_precio = @co_precio);
                                        END
                                        ELSE
                                        BEGIN
                                            UPDATE saArtMargen SET monto_min = @margen, monto_max = @margen, fe_us_mo = GETDATE(), co_us_mo = @user
                                            WHERE co_art = @co_art AND (co_precio = @real_co_precio OR co_precio = @co_precio);
                                        END
                                    `);
                                }
                            }
                            console.log(`✅ [FACTURA COMPRA] Precios propagados con éxito a sede ${otherSrv.id} (${otherSrv.name}).`);
                        } catch (broadErr) {
                            console.warn(`⚠️ [FACTURA COMPRA] Error al propagar precios a sede ${otherSrv.id}: ${broadErr.message}`);
                        }
                    }
                }
            }

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
                `SELECT reng_num, co_art, total_art, RTRIM(tipo_doc) AS tipo_doc, RTRIM(num_doc) AS num_doc, rowguid_doc 
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
                            saldo = 0,
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

                        if (line.rowguid_doc) {
                            rRevert.input('rowguid_doc', sql.UniqueIdentifier, line.rowguid_doc);
                            await rRevert.query(`
                                UPDATE saNotaRecepcionCompraReng
                                SET pendiente = CASE WHEN pendiente + @qty > total_art THEN total_art ELSE pendiente + @qty END,
                                    fe_us_mo = GETDATE()
                                WHERE rowguid = @rowguid_doc;
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
                            saldo = 0,
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

// =========================================================================
// 6. ELIMINAR FACTURA DE COMPRA
// =========================================================================
router.delete('/:doc_num', async (req, res) => {
    try {
        const { doc_num } = req.params;
        const { sede } = req.query;

        console.log(`🗑️ [FACTURA COMPRA] Petición de eliminación para factura ${doc_num} (sede: ${sede || 'todas/default'})`);

        const outcome = await executeWrite(sede || null, req.sqlAuth, async (pool, srv) => {
            const auditUser = (req.profitUser || req.sqlAuth?.user || 'API').substring(0, 6).toUpperCase();
            const defSucu = (srv.profit_branch_codes || []).find(b => b.is_default)?.code || (srv.profit_branch_codes || [])[0]?.code || '01';

            // 1. Verificar existencia de la factura
            const resH = await pool.request()
                .input('doc_num', sql.VarChar, doc_num)
                .query(`
                    SELECT doc_num, validador, rowguid, anulado, saldo, total_neto,
                           RTRIM(co_sucu_in) AS co_sucu_in, RTRIM(nro_fact) AS nro_fact
                    FROM saFacturaCompra
                    WHERE LTRIM(RTRIM(doc_num)) = LTRIM(RTRIM(@doc_num))
                `);

            if (!resH.recordset.length) {
                throw new Error(`La Factura de Compra "${doc_num}" no existe.`);
            }

            const head = resH.recordset[0];

            // 2. Verificar que no tenga pagos asociados en Cuentas por Pagar
            const resPagos = await pool.request()
                .input('doc_num', sql.Char(20), padProfit(doc_num, 20))
                .query(`
                    SELECT TOP 1 cob_num
                    FROM saPagoDocReng
                    WHERE LTRIM(RTRIM(nro_doc)) = LTRIM(RTRIM(@doc_num))
                      AND co_tipo_doc = 'FACT'
                `);

            if (resPagos.recordset.length > 0) {
                const cobNum = resPagos.recordset[0].cob_num;
                throw new Error(`No se puede eliminar la factura ${doc_num} porque tiene el pago N° ${cobNum?.trim()} asociado en Cuentas por Pagar. Anule o elimine el pago primero.`);
            }

            // 3. Obtener renglones de la factura
            const resL = await pool.request()
                .input('doc_num', sql.VarChar, doc_num)
                .query(`
                    SELECT reng_num, co_art, total_art, RTRIM(tipo_doc) AS tipo_doc,
                           RTRIM(num_doc) AS num_doc, rowguid_doc, rowguid
                    FROM saFacturaCompraReng
                    WHERE LTRIM(RTRIM(doc_num)) = LTRIM(RTRIM(@doc_num))
                `);

            const lines = resL.recordset;

            const transaction = new sql.Transaction(pool);
            await transaction.begin();

            try {
                // 4. Revertir pendientes en notas de recepción origen
                const recepcionesAfectadas = new Set();
                for (const line of lines) {
                    if (line.tipo_doc === 'NREC' && line.num_doc) {
                        recepcionesAfectadas.add(line.num_doc);
                        const rRevert = new sql.Request(transaction);
                        rRevert.input('qty', sql.Decimal(18, 5), line.total_art);
                        rRevert.input('num_doc', sql.Char(20), padProfit(line.num_doc, 20));
                        rRevert.input('co_art', sql.Char(30), padProfit(line.co_art, 30));

                        if (line.rowguid_doc) {
                            rRevert.input('rowguid_doc', sql.UniqueIdentifier, line.rowguid_doc);
                            await rRevert.query(`
                                UPDATE saNotaRecepcionCompraReng
                                SET pendiente = CASE WHEN pendiente + @qty > total_art THEN total_art ELSE pendiente + @qty END,
                                    fe_us_mo = GETDATE()
                                WHERE rowguid = @rowguid_doc;
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

                // 5. Eliminar contraparte en saDocumentoCompra (Cuentas por Pagar)
                await transaction.request()
                    .input('doc_num', sql.Char(20), padProfit(doc_num, 20))
                    .query(`
                        DELETE FROM saDocumentoCompra
                        WHERE LTRIM(RTRIM(nro_doc)) = LTRIM(RTRIM(@doc_num))
                          AND co_tipo_doc = 'FACT';
                    `);

                // 6. Eliminar Renglones de la Factura de Compra
                for (const line of lines) {
                    try {
                        const rDelLine = new sql.Request(transaction);
                        rDelLine.input('iReng_NumOri', sql.Int, line.reng_num);
                        rDelLine.input('sDoc_NumOri', sql.Char(20), padProfit(doc_num, 20));
                        rDelLine.input('sMaquina', sql.VarChar(60), 'SYNC2K');
                        rDelLine.input('sCo_Us_Mo', sql.Char(6), padProfit(auditUser, 6));
                        rDelLine.input('sCo_Sucu_Mo', sql.Char(6), padProfit(defSucu, 6));
                        rDelLine.input('gRowguid', sql.UniqueIdentifier, line.rowguid);
                        await rDelLine.execute('pEliminarRenglonesFacturaCompra');
                    } catch (spLineErr) {
                        console.warn(`⚠️ [FACTURA COMPRA] pEliminarRenglonesFacturaCompra falló renglón ${line.reng_num}: ${spLineErr.message}. Usando DELETE directo.`);
                        await transaction.request()
                            .input('doc_num', sql.Char(20), padProfit(doc_num, 20))
                            .input('reng_num', sql.Int, line.reng_num)
                            .query(`
                                IF OBJECT_ID('saFacturaCompraRengExt', 'U') IS NOT NULL
                                    DELETE FROM saFacturaCompraRengExt WHERE rowguid_reng = (SELECT rowguid FROM saFacturaCompraReng WHERE doc_num = @doc_num AND reng_num = @reng_num);
                                DELETE FROM saFacturaCompraReng WHERE doc_num = @doc_num AND reng_num = @reng_num;
                            `);
                    }
                }

                // 7. Eliminar Cabecera de la Factura de Compra
                try {
                    const rDelHead = new sql.Request(transaction);
                    rDelHead.input('sDoc_NumOri', sql.Char(20), padProfit(doc_num, 20));
                    rDelHead.input('sMaquina', sql.VarChar(60), 'SYNC2K');
                    rDelHead.input('sCo_Us_Mo', sql.Char(6), padProfit(auditUser, 6));
                    rDelHead.input('sCo_Sucu_Mo', sql.Char(6), padProfit(defSucu, 6));
                    if (head.validador) rDelHead.input('tsvalidador', sql.VarBinary, head.validador);
                    rDelHead.input('gRowguid', sql.UniqueIdentifier, head.rowguid);
                    await rDelHead.execute('pEliminarFacturaCompra');
                } catch (spHeadErr) {
                    console.warn(`⚠️ [FACTURA COMPRA] pEliminarFacturaCompra falló: ${spHeadErr.message}. Usando DELETE directo.`);
                    await transaction.request()
                        .input('doc_num', sql.Char(20), padProfit(doc_num, 20))
                        .input('rowguid', sql.UniqueIdentifier, head.rowguid)
                        .query(`
                            IF OBJECT_ID('saFacturaCompraExt', 'U') IS NOT NULL
                                DELETE FROM saFacturaCompraExt WHERE rowguid_doc = @rowguid;
                            DELETE FROM saFacturaCompra WHERE doc_num = @doc_num;
                        `);
                }

                await transaction.commit();
                console.log(`🗑️ [FACTURA COMPRA] Factura ${doc_num} eliminada exitosamente.`);
                return { success: true, doc_num: doc_num, message: `Factura ${doc_num} eliminada exitosamente.` };
            } catch (err) {
                if (transaction._aborted === false) await transaction.rollback();
                throw err;
            }
        });

        return writeResponse(res, outcome);
    } catch (error) {
        console.error('[DELETE /facturas-compras/:doc_num] Error:', error);
        res.status(500).json({ success: false, message: 'Error al eliminar Factura de Compra.', error: error.message });
    }
});

module.exports = router;
