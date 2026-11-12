const express = require('express');
const router = express.Router();
const { sql, getPool, getServers } = require('../db');
const { padProfit } = require('../helpers/multiSede');
const { getProximoConsecutivo } = require('../helpers/consecutivos');

console.log("📑 [AGENT] Iniciando Módulo de Documentos de Compra (saDocumentoCompra)");

function safeDate(val) {
    if (!val) return null;
    if (val instanceof Date) return val.toISOString().split('T')[0];
    return String(val).split('T')[0];
}

// =========================================================================
// 1. CATÁLOGOS AUXILIARES: TIPOS DE DOCUMENTO Y CUENTAS DE GASTO
// =========================================================================
router.get('/tipos', async (req, res) => {
    try {
        const { sede } = req.query;
        const servers = getServers();
        const target = sede ? servers.find(s => s.id === sede) : servers[0];
        if (!target) return res.status(404).json({ success: false, message: 'Sede no encontrada.' });

        const pool = await getPool(target.id, req.sqlAuth);
        const result = await pool.request().query(`
            SELECT 
                RTRIM(co_tipo_doc) AS co_tipo_doc,
                RTRIM(descrip) AS descrip,
                RTRIM(tipo_mov) AS tipo_mov
            FROM saTipoDocumento
            WHERE usar_compras = 1
            ORDER BY CASE 
                WHEN co_tipo_doc = 'FACT' THEN 1 
                WHEN co_tipo_doc = 'N/CR' THEN 2 
                WHEN co_tipo_doc = 'N/DB' THEN 3 
                ELSE 4 
            END, descrip ASC
        `);
        return res.json({ success: true, data: result.recordset });
    } catch (e) {
        console.error('[DOCS COMPRA] Error al consultar tipos:', e.message);
        return res.status(500).json({ success: false, message: e.message });
    }
});

router.get('/cuentas', async (req, res) => {
    try {
        const { sede } = req.query;
        const servers = getServers();
        const target = sede ? servers.find(s => s.id === sede) : servers[0];
        if (!target) return res.status(404).json({ success: false, message: 'Sede no encontrada.' });

        const pool = await getPool(target.id, req.sqlAuth);
        const result = await pool.request().query(`
            SELECT 
                RTRIM(co_cta_ingr_egr) AS co_cta_ingr_egr,
                RTRIM(descrip) AS descrip
            FROM saCuentaIngEgr
            ORDER BY descrip ASC
        `);
        return res.json({ success: true, data: result.recordset });
    } catch (e) {
        console.error('[DOCS COMPRA] Error al consultar cuentas:', e.message);
        return res.status(500).json({ success: false, message: e.message });
    }
});

// =========================================================================
// 2. LISTAR DOCUMENTOS DE COMPRA (saDocumentoCompra) CON FILTROS Y PERMISOS
// =========================================================================
router.get('/', async (req, res) => {
    try {
        const {
            sede,
            page = 1,
            limit = 20,
            search,
            doc_num,
            nro_fact,
            n_control,
            co_prov,
            co_tipo_doc,
            fec_d,
            fec_h,
            status,
            co_us_in,
            order_by = 'fec_emis',
            order_dir = 'DESC'
        } = req.query;

        const servers = getServers();
        const targets = sede ? servers.filter(s => s.id === sede) : servers;
        if (targets.length === 0) {
            return res.status(404).json({ success: false, message: `Sede "${sede}" no encontrada.` });
        }

        const pageNum = Math.max(1, parseInt(page) || 1);
        const limitNum = Math.max(1, Math.min(200, parseInt(limit) || 20));

        let sortCol = 'fec_emis';
        const allowedSort = ['fec_emis', 'fec_venc', 'nro_doc', 'nro_fact', 'total_neto', 'saldo', 'prov_des'];
        if (allowedSort.includes(order_by)) {
            sortCol = order_by;
        }
        const sortDir = String(order_dir).toUpperCase() === 'ASC' ? 'ASC' : 'DESC';

        const allData = await Promise.all(targets.map(async (srv) => {
            try {
                const pool = await getPool(srv.id, req.sqlAuth);
                const request = pool.request();
                let whereClauses = ["1 = 1"];

                if (doc_num) {
                    request.input('doc_num', sql.VarChar, `%${doc_num.trim()}%`);
                    whereClauses.push("d.nro_doc LIKE @doc_num");
                }

                if (nro_fact) {
                    request.input('nro_fact', sql.VarChar, `%${nro_fact.trim()}%`);
                    whereClauses.push("d.nro_fact LIKE @nro_fact");
                }

                if (n_control) {
                    request.input('n_control', sql.VarChar, `%${n_control.trim()}%`);
                    whereClauses.push("d.n_control LIKE @n_control");
                }

                if (co_prov) {
                    request.input('co_prov_search', sql.VarChar, `%${co_prov.trim()}%`);
                    whereClauses.push("(d.co_prov LIKE @co_prov_search OR p.prov_des LIKE @co_prov_search OR p.rif LIKE @co_prov_search)");
                }

                if (co_tipo_doc && co_tipo_doc !== 'all') {
                    request.input('co_tipo_doc', sql.VarChar, co_tipo_doc.trim());
                    whereClauses.push("LTRIM(RTRIM(d.co_tipo_doc)) = @co_tipo_doc");
                }

                // Permisos de terceros: si no tiene permiso de ver otros, filtrar estrictamente por su usuario
                if (co_us_in) {
                    request.input('co_us_in_filter', sql.VarChar, co_us_in.trim().toUpperCase());
                    whereClauses.push("LTRIM(RTRIM(d.co_us_in)) = @co_us_in_filter");
                }

                if (search) {
                    request.input('search_all', sql.VarChar, `%${search.trim()}%`);
                    whereClauses.push(`(
                        d.nro_doc LIKE @search_all 
                        OR d.nro_fact LIKE @search_all 
                        OR d.n_control LIKE @search_all 
                        OR p.prov_des LIKE @search_all 
                        OR d.co_prov LIKE @search_all 
                        OR p.rif LIKE @search_all
                        OR d.observa LIKE @search_all
                    )`);
                }

                if (fec_d) {
                    request.input('fec_d', sql.SmallDateTime, fec_d);
                    whereClauses.push("d.fec_emis >= @fec_d");
                }

                if (fec_h) {
                    request.input('fec_h', sql.SmallDateTime, fec_h);
                    whereClauses.push("d.fec_emis < DATEADD(day, 1, @fec_h)");
                }

                if (status !== undefined && status !== null && status !== '' && status !== 'all') {
                    if (status === 'anulado') {
                        whereClauses.push("d.anulado = 1");
                    } else if (status === 'activo') {
                        whereClauses.push("d.anulado = 0");
                    } else if (status === 'con_saldo') {
                        whereClauses.push("d.anulado = 0 AND d.saldo > 0");
                    } else if (status === 'pagado') {
                        whereClauses.push("d.anulado = 0 AND d.saldo <= 0");
                    }
                }

                const whereSQL = whereClauses.join(" AND ");

                let sqlSort = "d.fec_emis";
                if (sortCol === 'nro_doc') sqlSort = "d.nro_doc";
                else if (sortCol === 'nro_fact') sqlSort = "d.nro_fact";
                else if (sortCol === 'total_neto') sqlSort = "d.total_neto";
                else if (sortCol === 'saldo') sqlSort = "d.saldo";
                else if (sortCol === 'fec_venc') sqlSort = "d.fec_venc";
                else if (sortCol === 'prov_des') sqlSort = "p.prov_des";

                const querySQL = `
                    SELECT 
                        RTRIM(d.co_tipo_doc) AS co_tipo_doc,
                        RTRIM(td.descrip) AS des_tipo_doc,
                        RTRIM(td.tipo_mov) AS tipo_mov,
                        RTRIM(d.nro_doc) AS nro_doc,
                        RTRIM(d.nro_fact) AS nro_fact,
                        RTRIM(d.n_control) AS n_control,
                        RTRIM(d.co_prov) AS co_prov,
                        RTRIM(p.prov_des) AS prov_des,
                        RTRIM(p.rif) AS rif,
                        d.fec_emis,
                        d.fec_venc,
                        d.fec_reg,
                        d.fe_us_in AS fec_us_in,
                        d.anulado,
                        d.aut,
                        d.pagar,
                        RTRIM(d.co_mone) AS co_mone,
                        d.tasa,
                        d.total_bruto,
                        d.porc_imp,
                        d.monto_imp,
                        d.otros1 AS monto_exento,
                        d.total_neto,
                        d.saldo,
                        RTRIM(d.observa) AS observa,
                        RTRIM(d.co_cta_ingr_egr) AS co_cta_ingr_egr,
                        RTRIM(cie.descrip) AS des_cta_ingr_egr,
                        RTRIM(d.co_us_in) AS co_us_in,
                        RTRIM(d.co_sucu_in) AS co_sucu_in,
                        RTRIM(d.doc_orig) AS doc_orig,
                        RTRIM(d.nro_orig) AS nro_orig,
                        (
                            SELECT COUNT(*) 
                            FROM saPagoDocReng pdr 
                            INNER JOIN saPago pag ON pdr.cob_num = pag.cob_num 
                            WHERE pag.anulado = 0 
                              AND LTRIM(RTRIM(pdr.co_tipo_doc)) = LTRIM(RTRIM(d.co_tipo_doc)) 
                              AND LTRIM(RTRIM(pdr.nro_doc)) = LTRIM(RTRIM(d.nro_doc))
                        ) AS pagos_asociados
                    FROM saDocumentoCompra d
                    LEFT JOIN saProveedor p ON d.co_prov = p.co_prov
                    LEFT JOIN saTipoDocumento td ON d.co_tipo_doc = td.co_tipo_doc
                    LEFT JOIN saCuentaIngEgr cie ON d.co_cta_ingr_egr = cie.co_cta_ingr_egr
                    WHERE ${whereSQL}
                    ORDER BY ${sqlSort} ${sortDir}, d.nro_doc ${sortDir}
                `;

                const result = await request.query(querySQL);
                return result.recordset.map(r => ({
                    ...r,
                    sede_id: srv.id,
                    sede_nombre: srv.name
                }));
            } catch (errSrv) {
                console.error(`[DOCS COMPRA] Error en sede ${srv.id}:`, errSrv.message);
                return [];
            }
        }));

        const combined = [].concat(...allData);

        // Sorting global
        const dirMult = sortDir === 'ASC' ? 1 : -1;
        combined.sort((a, b) => {
            let res = 0;
            switch (sortCol) {
                case 'saldo':
                    res = (Number(a.saldo) || 0) - (Number(b.saldo) || 0);
                    break;
                case 'total_neto':
                    res = (Number(a.total_neto) || 0) - (Number(b.total_neto) || 0);
                    break;
                case 'nro_doc':
                    res = String(a.nro_doc).localeCompare(String(b.nro_doc));
                    break;
                case 'nro_fact':
                    res = String(a.nro_fact).localeCompare(String(b.nro_fact));
                    break;
                case 'prov_des':
                    res = String(a.prov_des || '').localeCompare(String(b.prov_des || ''));
                    break;
                case 'fec_venc':
                    res = new Date(a.fec_venc).getTime() - new Date(b.fec_venc).getTime();
                    break;
                case 'fec_emis':
                default:
                    res = new Date(a.fec_emis).getTime() - new Date(b.fec_emis).getTime();
                    break;
            }
            return res * dirMult;
        });

        // Totales globales y métricas
        const totalItems = combined.length;
        const totalPages = Math.ceil(totalItems / limitNum) || 1;
        const startIndex = (pageNum - 1) * limitNum;
        const paginated = combined.slice(startIndex, startIndex + limitNum);

        let totalNetoSum = 0;
        let totalSaldoSum = 0;
        let totalImpSum = 0;
        let totalExentoSum = 0;
        let facturasCount = 0;
        let ncrCount = 0;
        let ndbCount = 0;
        let anuladosCount = 0;

        for (const item of combined) {
            if (item.anulado) {
                anuladosCount++;
            } else {
                const neto = Number(item.total_neto) || 0;
                const saldo = Number(item.saldo) || 0;
                const imp = Number(item.monto_imp) || 0;
                const exento = Number(item.monto_exento) || 0;

                const isCredit = item.tipo_mov === 'CR' || item.co_tipo_doc === 'N/CR';
                if (isCredit) {
                    totalNetoSum -= neto;
                    totalSaldoSum -= saldo;
                    totalImpSum -= imp;
                    totalExentoSum -= exento;
                } else {
                    totalNetoSum += neto;
                    totalSaldoSum += saldo;
                    totalImpSum += imp;
                    totalExentoSum += exento;
                }

                if (item.co_tipo_doc === 'FACT') facturasCount++;
                else if (item.co_tipo_doc === 'N/CR') ncrCount++;
                else if (item.co_tipo_doc === 'N/DB') ndbCount++;
            }
        }

        return res.json({
            success: true,
            data: paginated,
            pagination: {
                total: totalItems,
                pages: totalPages,
                currentPage: pageNum,
                limit: limitNum
            },
            metrics: {
                total_items: totalItems,
                total_neto: totalNetoSum,
                total_saldo: totalSaldoSum,
                total_imp: totalImpSum,
                total_exento: totalExentoSum,
                facturas_count: facturasCount,
                ncr_count: ncrCount,
                ndb_count: ndbCount,
                anulados_count: anuladosCount
            }
        });
    } catch (e) {
        console.error('[DOCS COMPRA] Error general en GET /:', e.message);
        return res.status(500).json({ success: false, message: e.message });
    }
});

// =========================================================================
// 3. CONSULTAR DETALLE DE UN DOCUMENTO ESPECÍFICO
// =========================================================================
router.get('/:co_tipo_doc/:nro_doc', async (req, res) => {
    try {
        const { co_tipo_doc, nro_doc } = req.params;
        const { sede } = req.query;

        const servers = getServers();
        const target = sede ? servers.find(s => s.id === sede) : servers[0];
        if (!target) return res.status(404).json({ success: false, message: 'Sede no encontrada.' });

        const pool = await getPool(target.id, req.sqlAuth);
        const reqDoc = pool.request();
        reqDoc.input('co_tipo_doc', sql.Char(6), padProfit(co_tipo_doc, 6));
        reqDoc.input('nro_doc', sql.Char(20), padProfit(nro_doc, 20));

        const resDoc = await reqDoc.query(`
            SELECT 
                RTRIM(d.co_tipo_doc) AS co_tipo_doc,
                RTRIM(td.descrip) AS des_tipo_doc,
                RTRIM(td.tipo_mov) AS tipo_mov,
                RTRIM(d.nro_doc) AS nro_doc,
                RTRIM(d.nro_fact) AS nro_fact,
                RTRIM(d.n_control) AS n_control,
                RTRIM(d.co_prov) AS co_prov,
                RTRIM(p.prov_des) AS prov_des,
                RTRIM(p.rif) AS rif,
                RTRIM(p.direc1) AS direc1,
                RTRIM(p.telefonos) AS telefonos,
                RTRIM(p.email) AS email,
                p.contribu_e,
                p.porc_esp,
                d.fec_emis,
                d.fec_venc,
                d.fec_reg,
                d.fe_us_in AS fec_us_in,
                d.anulado,
                d.aut,
                d.pagar,
                RTRIM(d.co_mone) AS co_mone,
                d.tasa,
                d.total_bruto,
                d.porc_imp,
                d.monto_imp,
                RTRIM(d.porc_desc_glob) AS porc_desc_glob,
                d.monto_desc_glob,
                RTRIM(d.porc_reca) AS porc_reca,
                d.monto_reca,
                d.otros1 AS otros,
                d.otros1 AS monto_exento,
                d.otros2,
                d.otros3,
                d.total_neto,
                d.saldo,
                RTRIM(d.observa) AS observa,
                RTRIM(d.co_cta_ingr_egr) AS co_cta_ingr_egr,
                RTRIM(cie.descrip) AS des_cta_ingr_egr,
                RTRIM(d.co_us_in) AS co_us_in,
                RTRIM(d.co_sucu_in) AS co_sucu_in,
                RTRIM(d.doc_orig) AS doc_orig,
                RTRIM(d.nro_orig) AS nro_orig
            FROM saDocumentoCompra d
            LEFT JOIN saProveedor p ON d.co_prov = p.co_prov
            LEFT JOIN saTipoDocumento td ON d.co_tipo_doc = td.co_tipo_doc
            LEFT JOIN saCuentaIngEgr cie ON d.co_cta_ingr_egr = cie.co_cta_ingr_egr
            WHERE LTRIM(RTRIM(d.co_tipo_doc)) = LTRIM(RTRIM(@co_tipo_doc))
              AND LTRIM(RTRIM(d.nro_doc)) = LTRIM(RTRIM(@nro_doc))
        `);

        if (resDoc.recordset.length === 0) {
            return res.status(404).json({ success: false, message: 'Documento no encontrado.' });
        }

        const doc = resDoc.recordset[0];

        // Consultar pagos aplicados si existen
        const reqPagos = pool.request();
        reqPagos.input('co_tipo_doc', sql.Char(6), padProfit(co_tipo_doc, 6));
        reqPagos.input('nro_doc', sql.Char(20), padProfit(nro_doc, 20));

        const resPagos = await reqPagos.query(`
            SELECT 
                RTRIM(p.cob_num) AS cob_num,
                p.fec_cob,
                RTRIM(p.descrip) AS descrip,
                r.mont_cob,
                r.monto_retencion_iva,
                r.monto_retencion_islr,
                p.anulado
            FROM saPagoDocReng r
            INNER JOIN saPago p ON r.cob_num = p.cob_num
            WHERE LTRIM(RTRIM(r.co_tipo_doc)) = LTRIM(RTRIM(@co_tipo_doc))
              AND LTRIM(RTRIM(r.nro_doc)) = LTRIM(RTRIM(@nro_doc))
            ORDER BY p.fec_cob DESC
        `);

        return res.json({
            success: true,
            data: {
                ...doc,
                pagos: resPagos.recordset
            }
        });
    } catch (e) {
        console.error('[DOCS COMPRA] Error al consultar detalle:', e.message);
        return res.status(500).json({ success: false, message: e.message });
    }
});

// =========================================================================
// 4. REGISTRAR DOCUMENTO DE COMPRA DIRECTO (saDocumentoCompra)
// =========================================================================
router.post('/', async (req, res) => {
    let pool = null;
    let transaction = null;

    try {
        const { sede } = req.query;
        const payload = req.body;

        const servers = getServers();
        const srv = sede ? servers.find(s => s.id === sede) : servers[0];
        if (!srv) {
            return res.status(404).json({ success: false, message: `Sede "${sede}" no encontrada.` });
        }

        // Validaciones obligatorias
        if (!payload.co_prov || !String(payload.co_prov).trim()) {
            return res.status(400).json({ success: false, message: 'El código de proveedor es obligatorio.' });
        }
        if (!payload.nro_fact || !String(payload.nro_fact).trim()) {
            return res.status(400).json({ success: false, message: 'El número de factura / documento fiscal del proveedor es obligatorio.' });
        }
        if (!payload.fec_emis) {
            return res.status(400).json({ success: false, message: 'La fecha de emisión es obligatoria.' });
        }

        const coTipoDoc = (payload.co_tipo_doc || 'FACT').trim().toUpperCase();
        const nroFact = String(payload.nro_fact).trim().substring(0, 20);
        const nControl = payload.n_control && String(payload.n_control).trim() 
            ? String(payload.n_control).trim().substring(0, 20) 
            : 'N/A';
        const coMone = (payload.co_mone || 'BS').trim().toUpperCase();
        const tasaDoc = Math.max(0.00000001, Number(payload.tasa) || 1);

        // Montos financieros en moneda del documento
        const baseGravada = Math.max(0, Number(payload.total_bruto ?? payload.base_imponible ?? payload.monto_bruto) || 0);
        const porcDescGlob = payload.porc_desc_glob ? String(payload.porc_desc_glob).trim() : '0';
        const montoDescGlob = Math.max(0, Number(payload.monto_desc_glob) || 0);
        const porcReca = payload.porc_reca ? String(payload.porc_reca).trim() : null;
        const montoReca = Math.max(0, Number(payload.monto_reca) || 0);
        const otrosMonto = Number(payload.otros1 ?? payload.otros ?? 0) || 0;
        const exentoMonto = Number(payload.monto_exento ?? 0) || 0;
        const otros1 = Math.max(0, otrosMonto + exentoMonto);
        const porcImp = Math.max(0, Number(payload.porc_imp ?? payload.alicuota_iva ?? 16));
        const montoImp = Math.max(0, Number(payload.monto_imp ?? payload.monto_iva) || 0);
        const totalNeto = Math.max(0, Number(payload.total_neto) || Math.max(0, (baseGravada - montoDescGlob + montoReca) + montoImp + otros1));
        const saldo = totalNeto;

        if (totalNeto <= 0) {
            return res.status(400).json({ success: false, message: 'El monto total neto del documento debe ser mayor a 0.' });
        }

        // Fechas
        const fecEmis = safeDate(payload.fec_emis);
        const fecVenc = safeDate(payload.fec_venc || payload.fec_emis);
        const fecReg = safeDate(payload.fec_reg || payload.fec_emis);

        // Sucursal y auditoría
        const sucuCode = (payload.co_sucu_in || '01').trim();
        let auditUser = req.profitUser || payload.co_us_in || 'PROFIT';
        if (auditUser.length > 6) auditUser = auditUser.substring(0, 6);

        pool = await getPool(srv.id, req.sqlAuth);

        // Verificar si ya existe un documento activo con este mismo nro_fact para este proveedor
        const checkReq = pool.request();
        checkReq.input('co_prov', sql.Char(16), padProfit(payload.co_prov, 16));
        checkReq.input('nro_fact', sql.Char(20), padProfit(nroFact, 20));
        checkReq.input('co_tipo_doc', sql.Char(6), padProfit(coTipoDoc, 6));

        const existing = await checkReq.query(`
            SELECT TOP 1 RTRIM(nro_doc) AS nro_doc, anulado 
            FROM saDocumentoCompra 
            WHERE LTRIM(RTRIM(co_prov)) = LTRIM(RTRIM(@co_prov))
              AND LTRIM(RTRIM(nro_fact)) = LTRIM(RTRIM(@nro_fact))
              AND LTRIM(RTRIM(co_tipo_doc)) = LTRIM(RTRIM(@co_tipo_doc))
              AND anulado = 0
        `);

        if (existing.recordset.length > 0) {
            return res.status(400).json({
                success: false,
                message: `El proveedor ya tiene registrado el documento fiscal N° "${nroFact}" con número interno ${existing.recordset[0].nro_doc}.`
            });
        }

        transaction = new sql.Transaction(pool);
        await transaction.begin();

        // Obtener correlativo consecutivo atómico
        let consecKey = 'DOC_COMPRA_FACT';
        if (coTipoDoc === 'N/CR') consecKey = 'DOC_COMPRA_NCR';
        else if (coTipoDoc === 'N/DB') consecKey = 'DOC_COMPRA_NDB';

        const { docNum } = await getProximoConsecutivo({
            runner: transaction,
            co_tipo_serie: consecKey,
            co_sucur: sucuCode
        });

        console.log(`🧾 [DOCS COMPRA] Consecutivo asignado: ${docNum} (${coTipoDoc}) para proveedor ${payload.co_prov}`);

        const coCtaIngEgr = payload.co_cta_ingr_egr && String(payload.co_cta_ingr_egr).trim()
            ? padProfit(String(payload.co_cta_ingr_egr).trim(), 20)
            : null;
        const observa = (payload.observa || `${coTipoDoc} N° ${nroFact} - PROV ${payload.co_prov}`).substring(0, 120);

        // 1. Intentar ejecución vía Stored Procedure oficial de Profit
        let inserted = false;
        try {
            const reqDoc = new sql.Request(transaction);
            reqDoc.input('sCo_Tipo_Doc',     sql.Char(6),        padProfit(coTipoDoc, 6));
            reqDoc.input('sNro_Doc',         sql.Char(20),       padProfit(docNum, 20));
            reqDoc.input('sNro_Fact',        sql.Char(20),       padProfit(nroFact, 20));
            reqDoc.input('sCo_Mone',         sql.Char(6),        padProfit(coMone, 6));
            reqDoc.input('sCo_Prov',         sql.Char(16),       padProfit(payload.co_prov, 16));
            reqDoc.input('sCo_Cta_Ingr_Egr', sql.Char(20),       coCtaIngEgr);
            reqDoc.input('sDoc_Orig',        sql.Char(6),        null);
            reqDoc.input('sMov_Ban',         sql.Char(20),       null);
            reqDoc.input('sNro_Orig',        sql.Char(20),       null);
            reqDoc.input('sNro_Che',         sql.Char(20),       null);
            reqDoc.input('sPorc_Reca',       sql.Char(15),       porcReca);
            reqDoc.input('sPorc_Desc_Glob',  sql.Char(15),       porcDescGlob);
            reqDoc.input('bAnulado',         sql.Bit,            0);
            reqDoc.input('bAut',             sql.Bit,            0);
            reqDoc.input('iPagar',           sql.Int,            0);
            reqDoc.input('sObserva',         sql.VarChar(120),   observa);
            reqDoc.input('sTipo_Imp',        sql.Char(1),        porcImp > 0 ? '1' : '5');
            reqDoc.input('sTipo_Imp2',       sql.Char(1),        null);
            reqDoc.input('sTipo_Imp3',       sql.Char(1),        null);
            reqDoc.input('sdFec_Reg',        sql.SmallDateTime,  fecReg);
            reqDoc.input('sdFec_Emis',       sql.SmallDateTime,  fecEmis);
            reqDoc.input('sdFec_Venc',       sql.SmallDateTime,  fecVenc);
            reqDoc.input('deTotal_Neto',     sql.Decimal(18, 2), totalNeto);
            reqDoc.input('deTasa',           sql.Decimal(21, 8), tasaDoc);
            reqDoc.input('dePorc_Imp',       sql.Decimal(18, 5), porcImp);
            reqDoc.input('dePorc_Imp2',      sql.Decimal(18, 5), 0);
            reqDoc.input('dePorc_Imp3',      sql.Decimal(18, 5), 0);
            reqDoc.input('deMonto_Imp',      sql.Decimal(18, 2), montoImp);
            reqDoc.input('deMonto_Imp2',     sql.Decimal(18, 2), 0);
            reqDoc.input('deMonto_Imp3',     sql.Decimal(18, 2), 0);
            reqDoc.input('deTotal_Bruto',    sql.Decimal(18, 2), baseGravada);
            reqDoc.input('deMonto_Desc_Glob',sql.Decimal(18, 2), montoDescGlob);
            reqDoc.input('deMonto_Reca',     sql.Decimal(18, 2), montoReca);
            reqDoc.input('deSaldo',          sql.Decimal(18, 2), saldo);
            reqDoc.input('deAdicional',      sql.Decimal(18, 2), 0);
            reqDoc.input('deOtros1',         sql.Decimal(18, 2), otros1);
            reqDoc.input('deOtros2',         sql.Decimal(18, 2), 0);
            reqDoc.input('deOtros3',         sql.Decimal(18, 2), 0);
            reqDoc.input('sPro_Pago',        sql.VarChar(sql.MAX), null);
            reqDoc.input('sSalestax',        sql.Char(8),        null);
            reqDoc.input('sProv_Ter',        sql.Char(16),       null);
            reqDoc.input('iReng_Ter',        sql.Int,            0);
            reqDoc.input('iTipo_Origen',     sql.Int,            0);
            reqDoc.input('sNum_Comprobante', sql.Char(14),       null);
            reqDoc.input('sDis_Cen',         sql.VarChar(sql.MAX), null);
            reqDoc.input('sN_Control',       sql.Char(20),       padProfit(nControl, 20));
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
            reqDoc.input('bNac',             sql.Bit,            coMone === 'BS' ? 1 : 0);

            await reqDoc.execute('pInsertarDocumentoCompra');
            inserted = true;
            console.log(`✅ [DOCS COMPRA] Documento ${docNum} insertado vía SP.`);
        } catch (spErr) {
            console.warn(`⚠️ [DOCS COMPRA] Advertencia SP pInsertarDocumentoCompra:`, spErr.message);
            console.log(`🔄 [DOCS COMPRA] Ejecutando INSERT directo en saDocumentoCompra...`);

            const directReq = new sql.Request(transaction);
            directReq.input('co_tipo_doc',    sql.Char(6), padProfit(coTipoDoc, 6));
            directReq.input('nro_doc',        sql.Char(20), padProfit(docNum, 20));
            directReq.input('nro_fact',       sql.Char(20), padProfit(nroFact, 20));
            directReq.input('co_mone',        sql.Char(6), padProfit(coMone, 6));
            directReq.input('co_prov',        sql.Char(16), padProfit(payload.co_prov, 16));
            directReq.input('co_cta_ingr_egr',sql.Char(20), coCtaIngEgr);
            directReq.input('anulado',        sql.Bit, 0);
            directReq.input('aut',            sql.Bit, 0);
            directReq.input('pagar',          sql.Int, 0);
            directReq.input('observa',        sql.VarChar(120), observa);
            directReq.input('tipo_imp',       sql.Char(1), '1');
            directReq.input('fec_reg',        sql.SmallDateTime, fecReg);
            directReq.input('fec_emis',       sql.SmallDateTime, fecEmis);
            directReq.input('fec_venc',       sql.SmallDateTime, fecVenc);
            directReq.input('porc_imp',       sql.Decimal(18, 5), porcImp);
            directReq.input('porc_imp2',      sql.Decimal(18, 5), 0);
            directReq.input('porc_imp3',      sql.Decimal(18, 5), 0);
            directReq.input('monto_imp',      sql.Decimal(18, 2), montoImp);
            directReq.input('monto_imp2',     sql.Decimal(18, 2), 0);
            directReq.input('monto_imp3',     sql.Decimal(18, 2), 0);
            directReq.input('tasa',           sql.Decimal(21, 8), tasaDoc);
            directReq.input('total_bruto',    sql.Decimal(18, 2), baseGravada);
            directReq.input('total_neto',     sql.Decimal(18, 2), totalNeto);
            directReq.input('saldo',          sql.Decimal(18, 2), saldo);
            directReq.input('porc_desc_glob', sql.VarChar(15),  porcDescGlob);
            directReq.input('monto_desc_glob',sql.Decimal(18, 2), montoDescGlob);
            directReq.input('porc_reca',      sql.VarChar(15),  porcReca);
            directReq.input('monto_reca',     sql.Decimal(18, 2), montoReca);
            directReq.input('otros1',         sql.Decimal(18, 2), otros1);
            directReq.input('otros2',         sql.Decimal(18, 2), 0);
            directReq.input('otros3',         sql.Decimal(18, 2), 0);
            directReq.input('adicional',      sql.Decimal(18, 2), 0);
            directReq.input('reng_ter',       sql.Int, 0);
            directReq.input('tipo_origen',    sql.Int, 0);
            directReq.input('n_control',      sql.Char(20), padProfit(nControl, 20));
            directReq.input('co_us_in',       sql.Char(6), padProfit(auditUser, 6));
            directReq.input('co_sucu_in',     sql.Char(6), padProfit(sucuCode, 6));
            directReq.input('nac',            sql.Bit, coMone === 'BS' ? 1 : 0);

            await directReq.query(`
                INSERT INTO saDocumentoCompra (
                    co_tipo_doc, nro_doc, nro_fact, co_mone, co_prov, co_cta_ingr_egr,
                    porc_desc_glob, monto_desc_glob, porc_reca, monto_reca, anulado, aut, pagar, observa, tipo_imp,
                    fec_reg, fec_emis, fec_venc, porc_imp, porc_imp2, porc_imp3,
                    monto_imp, monto_imp2, monto_imp3, tasa, total_bruto, total_neto,
                    saldo, adicional, otros1, otros2, otros3, reng_ter, tipo_origen,
                    n_control, co_us_in, co_sucu_in, fe_us_in, co_us_mo, co_sucu_mo,
                    fe_us_mo, nac
                ) VALUES (
                    @co_tipo_doc, @nro_doc, @nro_fact, @co_mone, @co_prov, @co_cta_ingr_egr,
                    @porc_desc_glob, @monto_desc_glob, @porc_reca, @monto_reca, @anulado, @aut, @pagar, @observa, @tipo_imp,
                    @fec_reg, @fec_emis, @fec_venc, @porc_imp, @porc_imp2, @porc_imp3,
                    @monto_imp, @monto_imp2, @monto_imp3, @tasa, @total_bruto, @total_neto,
                    @saldo, @adicional, @otros1, @otros2, @otros3, @reng_ter, @tipo_origen,
                    @n_control, @co_us_in, @co_sucu_in, GETDATE(), @co_us_in, @co_sucu_in,
                    GETDATE(), @nac
                );
            `);
            inserted = true;
            console.log(`✅ [DOCS COMPRA] Documento ${docNum} insertado vía direct INSERT.`);
        }

        await transaction.commit();

        return res.json({
            success: true,
            doc_num: docNum,
            nro_doc: docNum,
            co_tipo_doc: coTipoDoc,
            nro_fact: nroFact,
            message: `Documento ${coTipoDoc} ${docNum} registrado con éxito.`
        });
    } catch (e) {
        if (transaction) {
            try { await transaction.rollback(); } catch (_) {}
        }
        console.error('[DOCS COMPRA] Error al registrar documento:', e.message);
        return res.status(500).json({ success: false, message: 'Error al registrar documento: ' + e.message });
    }
});

// =========================================================================
// 5. ANULAR DOCUMENTO DE COMPRA (saDocumentoCompra)
// =========================================================================
router.post('/:co_tipo_doc/:nro_doc/anular', async (req, res) => {
    let pool = null;
    let transaction = null;

    try {
        const { co_tipo_doc, nro_doc } = req.params;
        const { sede } = req.query;

        const servers = getServers();
        const srv = sede ? servers.find(s => s.id === sede) : servers[0];
        if (!srv) return res.status(404).json({ success: false, message: 'Sede no encontrada.' });

        let auditUser = req.profitUser || req.body.co_us_in || 'PROFIT';
        if (auditUser.length > 6) auditUser = auditUser.substring(0, 6);

        pool = await getPool(srv.id, req.sqlAuth);

        // 1. Verificar existencia y estado del documento
        const checkReq = pool.request();
        checkReq.input('co_tipo_doc', sql.Char(6), padProfit(co_tipo_doc, 6));
        checkReq.input('nro_doc', sql.Char(20), padProfit(nro_doc, 20));

        const resDoc = await checkReq.query(`
            SELECT 
                anulado,
                saldo,
                total_neto,
                doc_orig,
                nro_orig
            FROM saDocumentoCompra
            WHERE LTRIM(RTRIM(co_tipo_doc)) = LTRIM(RTRIM(@co_tipo_doc))
              AND LTRIM(RTRIM(nro_doc)) = LTRIM(RTRIM(@nro_doc))
        `);

        if (resDoc.recordset.length === 0) {
            return res.status(404).json({ success: false, message: 'Documento no encontrado.' });
        }

        const doc = resDoc.recordset[0];
        if (doc.anulado) {
            return res.status(400).json({ success: false, message: 'El documento ya se encuentra anulado.' });
        }

        // 2. Verificar si tiene pagos asociados activos
        const checkPagos = pool.request();
        checkPagos.input('co_tipo_doc', sql.Char(6), padProfit(co_tipo_doc, 6));
        checkPagos.input('nro_doc', sql.Char(20), padProfit(nro_doc, 20));

        const resPagos = await checkPagos.query(`
            SELECT TOP 1 p.cob_num 
            FROM saPagoDocReng r
            INNER JOIN saPago p ON r.cob_num = p.cob_num
            WHERE p.anulado = 0
              AND LTRIM(RTRIM(r.co_tipo_doc)) = LTRIM(RTRIM(@co_tipo_doc))
              AND LTRIM(RTRIM(r.nro_doc)) = LTRIM(RTRIM(@nro_doc))
        `);

        if (resPagos.recordset.length > 0) {
            return res.status(400).json({
                success: false,
                message: `El documento tiene pagos activos asociados (Ej. Pago N° ${resPagos.recordset[0].cob_num}). Debe anular primero los pagos antes de anular este documento.`
            });
        }

        transaction = new sql.Transaction(pool);
        await transaction.begin();

        // 3. Proceder a anular en saDocumentoCompra
        const updReq = new sql.Request(transaction);
        updReq.input('co_tipo_doc', sql.Char(6), padProfit(co_tipo_doc, 6));
        updReq.input('nro_doc', sql.Char(20), padProfit(nro_doc, 20));
        updReq.input('auditUser', sql.Char(6), padProfit(auditUser, 6));

        await updReq.query(`
            UPDATE saDocumentoCompra
            SET anulado = 1,
                saldo = 0,
                fe_us_mo = GETDATE(),
                co_us_mo = @auditUser
            WHERE LTRIM(RTRIM(co_tipo_doc)) = LTRIM(RTRIM(@co_tipo_doc))
              AND LTRIM(RTRIM(nro_doc)) = LTRIM(RTRIM(@nro_doc));
        `);

        await transaction.commit();

        return res.json({
            success: true,
            nro_doc,
            co_tipo_doc,
            message: `Documento ${co_tipo_doc} ${nro_doc} anulado exitosamente.`
        });
    } catch (e) {
        if (transaction) {
            try { await transaction.rollback(); } catch (_) {}
        }
        console.error('[DOCS COMPRA] Error al anular documento:', e.message);
        return res.status(500).json({ success: false, message: 'Error al anular documento: ' + e.message });
    }
});

// =========================================================================
// 6. ELIMINAR DOCUMENTO DE COMPRA (saDocumentoCompra)
// =========================================================================
router.delete('/:co_tipo_doc/:nro_doc', async (req, res) => {
    let pool = null;
    let transaction = null;

    try {
        const { co_tipo_doc, nro_doc } = req.params;
        const { sede } = req.query;

        const servers = getServers();
        const srv = sede ? servers.find(s => s.id === sede) : servers[0];
        if (!srv) return res.status(404).json({ success: false, message: 'Sede no encontrada.' });

        pool = await getPool(srv.id, req.sqlAuth);

        // 1. Verificar existencia y si proviene de factura de compra con renglones
        const checkReq = pool.request();
        checkReq.input('co_tipo_doc', sql.Char(6), padProfit(co_tipo_doc, 6));
        checkReq.input('nro_doc', sql.Char(20), padProfit(nro_doc, 20));

        const resDoc = await checkReq.query(`
            SELECT 
                anulado,
                saldo,
                total_neto,
                doc_orig,
                nro_orig
            FROM saDocumentoCompra
            WHERE LTRIM(RTRIM(co_tipo_doc)) = LTRIM(RTRIM(@co_tipo_doc))
              AND LTRIM(RTRIM(nro_doc)) = LTRIM(RTRIM(@nro_doc))
        `);

        if (resDoc.recordset.length === 0) {
            return res.status(404).json({ success: false, message: 'Documento no encontrado.' });
        }

        const doc = resDoc.recordset[0];

        // Si fue generado desde una factura con inventario (saFacturaCompra), no se debe eliminar directamente desde aquí
        if (doc.doc_orig && doc.doc_orig.trim() === 'FACT' && doc.nro_orig && doc.nro_orig.trim()) {
            // Verificar si existe la factura de compra
            const fcCheck = await pool.request()
                .input('doc_num', sql.Char(20), padProfit(doc.nro_orig, 20))
                .query(`SELECT 1 FROM saFacturaCompra WHERE LTRIM(RTRIM(doc_num)) = LTRIM(RTRIM(@doc_num))`);
            if (fcCheck.recordset.length > 0) {
                return res.status(400).json({
                    success: false,
                    message: `Este documento fue generado automáticamente por la Factura de Compra ${doc.nro_orig.trim()}. Debe ser administrado desde el módulo de Facturas de Compra.`
                });
            }
        }

        // 2. Verificar pagos
        const checkPagos = pool.request();
        checkPagos.input('co_tipo_doc', sql.Char(6), padProfit(co_tipo_doc, 6));
        checkPagos.input('nro_doc', sql.Char(20), padProfit(nro_doc, 20));

        const resPagos = await checkPagos.query(`
            SELECT TOP 1 p.cob_num 
            FROM saPagoDocReng r
            INNER JOIN saPago p ON r.cob_num = p.cob_num
            WHERE LTRIM(RTRIM(r.co_tipo_doc)) = LTRIM(RTRIM(@co_tipo_doc))
              AND LTRIM(RTRIM(r.nro_doc)) = LTRIM(RTRIM(@nro_doc))
        `);

        if (resPagos.recordset.length > 0) {
            return res.status(400).json({
                success: false,
                message: `No se puede eliminar el documento porque tiene pagos asociados (Ej. Pago N° ${resPagos.recordset[0].cob_num}).`
            });
        }

        if (!doc.anulado && Number(doc.saldo) < Number(doc.total_neto)) {
            return res.status(400).json({
                success: false,
                message: 'No se puede eliminar el documento porque su saldo es menor al total neto (posee amortizaciones).'
            });
        }

        transaction = new sql.Transaction(pool);
        await transaction.begin();

        const delReq = new sql.Request(transaction);
        delReq.input('co_tipo_doc', sql.Char(6), padProfit(co_tipo_doc, 6));
        delReq.input('nro_doc', sql.Char(20), padProfit(nro_doc, 20));

        await delReq.query(`
            DELETE FROM saDocumentoCompra
            WHERE LTRIM(RTRIM(co_tipo_doc)) = LTRIM(RTRIM(@co_tipo_doc))
              AND LTRIM(RTRIM(nro_doc)) = LTRIM(RTRIM(@nro_doc));
        `);

        await transaction.commit();

        return res.json({
            success: true,
            nro_doc,
            co_tipo_doc,
            message: `Documento ${co_tipo_doc} ${nro_doc} eliminado exitosamente.`
        });
    } catch (e) {
        if (transaction) {
            try { await transaction.rollback(); } catch (_) {}
        }
        console.error('[DOCS COMPRA] Error al eliminar documento:', e.message);
        return res.status(500).json({ success: false, message: 'Error al eliminar documento: ' + e.message });
    }
});

module.exports = router;

