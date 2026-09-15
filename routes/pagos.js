const express = require('express');
const router = express.Router();
const { sql, getPool, getServers } = require('../db');
const { executeWrite, writeResponse, paginatedResponse, padProfit } = require('../helpers/multiSede');
const { getProximoConsecutivo } = require('../helpers/consecutivos');

/**
 * @swagger
 * tags:
 *   name: Pagos
 *   description: Gestión de Pagos a Proveedores y recibos de caja/banco de compras
 */

// --- OBTENER LISTADO DE PAGOS ---
router.get('/', async (req, res) => {
    try {
        const page = parseInt(req.query.page) || 1;
        const limit = parseInt(req.query.limit) || 12;
        const { sede, co_prov, co_us_in, fec_d, fec_h, search } = req.query;

        const servers = getServers();
        const targets = sede ? servers.filter(s => s.id === sede) : servers;

        const allData = await Promise.all(targets.map(async (srv) => {
            try {
                const pool = await getPool(srv.id, req.sqlAuth);
                const request = pool.request();
                let whereClauses = ["1=1"];

                if (co_prov) {
                    request.input('co_prov_search', sql.VarChar, `%${co_prov}%`);
                    whereClauses.push("(p.co_prov LIKE @co_prov_search OR pr.prov_des LIKE @co_prov_search OR pr.rif LIKE @co_prov_search)");
                }
                if (search) {
                    request.input('search_all', sql.VarChar, `%${search}%`);
                    whereClauses.push("(p.cob_num LIKE @search_all OR p.co_prov LIKE @search_all OR pr.prov_des LIKE @search_all OR pr.rif LIKE @search_all)");
                }
                if (co_us_in) {
                    request.input('co_us_in_filter', sql.VarChar, co_us_in.trim().toUpperCase());
                    whereClauses.push("LTRIM(RTRIM(p.co_us_in)) = @co_us_in_filter");
                }
                if (fec_d) {
                    request.input('fec_d', sql.SmallDateTime, fec_d);
                    whereClauses.push("p.fe_us_in >= @fec_d");
                }
                if (fec_h) {
                    request.input('fec_h', sql.SmallDateTime, fec_h);
                    whereClauses.push("p.fe_us_in < DATEADD(day, 1, @fec_h)");
                }

                const whereSQL = whereClauses.join(" AND ");

                const result = await request.query(`
                    SELECT RTRIM(p.cob_num) AS cob_num, RTRIM(p.recibo) AS recibo, RTRIM(p.descrip) AS descrip,
                           RTRIM(p.co_prov) AS co_prov, RTRIM(pr.prov_des) AS prov_des, RTRIM(pr.rif) AS rif,
                           p.fe_us_in AS fecha, p.anulado,
                           ISNULL((SELECT SUM(mont_doc) FROM saPagoTPReng WHERE cob_num = p.cob_num), 0) AS monto,
                           RTRIM(p.co_mone) AS co_mone, 
                           CASE WHEN p.tasa <= 1.000001 THEN 
                                ISNULL((SELECT TOP 1 t.tasa_v FROM saTasa t WHERE LTRIM(RTRIM(t.co_mone)) IN ('USD', 'US$', 'US') AND CONVERT(VARCHAR(10), t.fecha, 120) <= CONVERT(VARCHAR(10), p.fecha, 120) ORDER BY t.fecha DESC), 1.0)
                           ELSE p.tasa END AS tasa,
                           RTRIM(p.co_us_in) AS co_us_in,
                           ISNULL(
                               SUBSTRING(
                                   (SELECT ', ' + RTRIM(r.co_tipo_doc) + ' ' + RTRIM(r.nro_doc) + ':' + CAST(ISNULL(d.total_neto, 0) AS VARCHAR)
                                    FROM saPagoDocReng r
                                    LEFT JOIN saDocumentoCompra d ON r.co_tipo_doc = d.co_tipo_doc 
                                                                 AND r.nro_doc = d.nro_doc
                                    WHERE r.cob_num = p.cob_num 
                                      AND r.co_tipo_doc IN ('FACT  ', 'NDEB  ', 'N/DB  ', 'GIRO  ', 'AJPA  ')
                                    ORDER BY r.reng_num
                                    FOR XML PATH('')), 
                                   3, 
                                   200
                               ), 
                               '---'
                           ) AS documentos_asociados
                    FROM saPago p
                    LEFT JOIN saProveedor pr ON p.co_prov = pr.co_prov
                    WHERE ${whereSQL}
                    ORDER BY p.fe_us_in DESC, p.cob_num DESC
                `);

                return result.recordset.map(r => ({ ...r, sede_id: srv.id, sede_nombre: srv.name }));
            } catch (e) {
                console.error(`[PAGOS] Error en sede ${srv.id}:`, e.message);
                return [];
            }
        }));

        const combined = [].concat(...allData);
        combined.sort((a, b) => new Date(b.fecha) - new Date(a.fecha));
        return paginatedResponse(res, combined, page, limit);
    } catch (error) {
        res.status(500).json({ success: false, message: 'Error al consultar Pagos.', error: error.message });
    }
});

// --- OBTENER FACTURAS DE COMPRA PENDIENTES CON SALDO ---
router.get('/facturas/pendientes', async (req, res) => {
    try {
        const { search, co_prov, sede } = req.query;
        const page = parseInt(req.query.page) || 1;
        const limit = parseInt(req.query.limit) || 50;

        const servers = getServers();
        const targets = sede ? servers.filter(s => s.id === sede) : servers;

        const allData = await Promise.all(targets.map(async (srv) => {
            try {
                const pool = await getPool(srv.id, req.sqlAuth);
                const request = pool.request();
                let whereClauses = [
                    "d.saldo > 0",
                    "d.anulado = 0",
                    "RTRIM(d.co_tipo_doc) IN ('FACT', 'NDEB', 'N/DB', 'GIRO', 'AJPA', 'N/CR')"
                ];

                if (co_prov) {
                    request.input('co_prov', sql.VarChar, co_prov.trim());
                    whereClauses.push("LTRIM(RTRIM(d.co_prov)) = @co_prov");
                }

                if (search) {
                    request.input('search', sql.VarChar, `%${search.trim()}%`);
                    whereClauses.push(`(
                        d.nro_doc LIKE @search 
                        OR d.nro_fact LIKE @search
                        OR d.co_prov LIKE @search 
                        OR pr.prov_des LIKE @search 
                        OR pr.rif LIKE @search
                    )`);
                }

                const whereSQL = whereClauses.join(" AND ");
                const result = await request.query(`
                    SELECT TOP 100 
                           RTRIM(d.co_tipo_doc) AS co_tipo_doc, 
                           RTRIM(d.nro_doc) AS nro_doc, 
                           RTRIM(ISNULL(d.nro_fact, d.nro_doc)) AS nro_fact,
                           d.fec_emis, d.fec_venc, 
                           d.total_neto, d.total_bruto, d.saldo, d.monto_imp,
                           RTRIM(d.co_mone) AS co_mone,
                           CASE WHEN d.tasa <= 1.000001 THEN 
                                 ISNULL((SELECT TOP 1 t.tasa_v FROM saTasa t WHERE LTRIM(RTRIM(t.co_mone)) IN ('USD', 'US$', 'US') AND CONVERT(VARCHAR(10), t.fecha, 120) <= CONVERT(VARCHAR(10), d.fec_emis, 120) ORDER BY t.fecha DESC), 1.0)
                           ELSE d.tasa END AS tasa,
                           RTRIM(d.n_control) AS n_control,
                           d.rowguid,
                           RTRIM(d.co_prov) AS co_prov,
                           RTRIM(pr.prov_des) AS prov_des,
                           RTRIM(pr.rif) AS rif,
                           pr.contribu_e, pr.porc_esp,
                           RTRIM(d.co_us_in) AS co_us_in,
                           ISNULL(d.otros1, 0) AS otros1,
                           ISNULL(CASE WHEN d.porc_imp > 0 THEN (d.total_neto - d.monto_imp) ELSE 0 END, 0) AS base_imponible
                    FROM saDocumentoCompra d
                    INNER JOIN saProveedor pr ON d.co_prov = pr.co_prov
                    WHERE ${whereSQL}
                    ORDER BY d.fec_emis DESC
                `);

                return result.recordset.map(r => ({
                    ...r,
                    sede_id: srv.id,
                    sede_nombre: srv.name
                }));
            } catch (e) {
                console.error(`[PAGOS PENDIENTES] Error en sede ${srv.id}:`, e.message);
                return [];
            }
        }));

        const combined = [].concat(...allData);
        combined.sort((a, b) => new Date(b.fec_emis) - new Date(a.fec_emis));

        const start = (page - 1) * limit;
        const paginated = combined.slice(start, start + limit);

        res.status(200).json({
            success: true,
            count: combined.length,
            data: paginated
        });
    } catch (error) {
        res.status(500).json({ success: false, message: 'Error al consultar documentos pendientes de compra.', error: error.message });
    }
});

// --- OBTENER CONCEPTOS DE RETENCIÓN DE ISLR ---
router.get('/conceptos-islr', async (req, res) => {
    try {
        const { sede } = req.query;
        const servers = getServers();
        const target = (sede ? servers.find(s => s.id === sede) : null) || servers[0];

        if (!target) {
            return res.status(404).json({ success: false, message: 'Sede no configurada.' });
        }

        const pool = await getPool(target.id, req.sqlAuth);
        const result = await pool.request().query(`
            SELECT RTRIM(co_islr) AS co_islr, RTRIM(islr_des) AS islr_des
            FROM saConISLR
            ORDER BY co_islr
        `);

        res.status(200).json({
            success: true,
            data: result.recordset
        });
    } catch (error) {
        res.status(500).json({ success: false, message: 'Error al consultar conceptos de ISLR.', error: error.message });
    }
});

// --- OBTENER DETALLE DE UN PAGO ---
router.get('/:cob_num', async (req, res) => {
    try {
        const { cob_num } = req.params;
        const { sede } = req.query;
        const servers = getServers();
        const targets = sede ? servers.filter(s => s.id === sede) : servers;

        if (targets.length === 0)
            return res.status(404).json({ success: false, message: `Sede "${sede}" no encontrada.` });

        const results = await Promise.all(targets.map(async (srv) => {
            try {
                const pool = await getPool(srv.id, req.sqlAuth);

                const [resEnc, resReng, resTP, resIva, resIslr] = await Promise.all([
                    pool.request().input('cob_num', sql.VarChar, cob_num).query(`
                        SELECT RTRIM(p.cob_num) AS cob_num, RTRIM(p.recibo) AS recibo, RTRIM(p.descrip) AS descrip,
                               RTRIM(p.co_prov) AS co_prov, RTRIM(pr.prov_des) AS prov_des, RTRIM(pr.rif) AS rif,
                               ISNULL(pr.porc_esp, 0) AS porc_esp, pr.contribu_e,
                               p.fe_us_in AS fecha, p.anulado,
                               ISNULL((SELECT SUM(mont_doc) FROM saPagoTPReng WHERE cob_num = p.cob_num), 0) AS monto,
                               RTRIM(p.co_mone) AS co_mone, 
                               CASE WHEN p.tasa <= 1.000001 THEN 
                                    ISNULL((SELECT TOP 1 t.tasa_v FROM saTasa t WHERE LTRIM(RTRIM(t.co_mone)) IN ('USD', 'US$', 'US') AND CONVERT(VARCHAR(10), t.fecha, 120) <= CONVERT(VARCHAR(10), p.fecha, 120) ORDER BY t.fecha DESC), 1.0)
                               ELSE p.tasa END AS tasa,
                               RTRIM(p.co_us_in) AS co_us_in
                        FROM saPago p
                        LEFT JOIN saProveedor pr ON p.co_prov = pr.co_prov
                        WHERE LTRIM(RTRIM(p.cob_num)) = LTRIM(RTRIM(@cob_num))
                    `),
                    pool.request().input('cob_num', sql.VarChar, cob_num).query(`
                        SELECT r.reng_num, RTRIM(r.co_tipo_doc) AS co_tipo_doc, RTRIM(r.nro_doc) AS nro_doc,
                               RTRIM(r.nro_fact) AS nro_fact,
                               r.mont_cob, r.monto_retencion_iva, r.monto_retencion,
                               r.rowguid, r.rowguid_reng_ori,
                               ISNULL(d.monto_imp, 0) AS monto_imp,
                               ISNULL(d.total_neto, 0) AS total_neto
                        FROM saPagoDocReng r
                        LEFT JOIN saDocumentoCompra d ON LTRIM(RTRIM(r.co_tipo_doc)) = LTRIM(RTRIM(d.co_tipo_doc)) 
                                                     AND LTRIM(RTRIM(r.nro_doc)) = LTRIM(RTRIM(d.nro_doc))
                        WHERE LTRIM(RTRIM(r.cob_num)) = LTRIM(RTRIM(@cob_num))
                        ORDER BY r.reng_num
                    `),
                    pool.request().input('cob_num', sql.VarChar, cob_num).query(`
                        SELECT tp.reng_num, RTRIM(tp.forma_pag) AS forma_pag, tp.mont_doc,
                               RTRIM(tp.cod_caja) AS cod_caja, RTRIM(cj.descrip) AS caja_des,
                               RTRIM(tp.cod_cta) AS cod_cta, RTRIM(cb.num_cta) AS cta_des,
                               RTRIM(cb.co_ban) AS co_ban, RTRIM(b.des_ban) AS ban_des,
                               RTRIM(tp.num_doc) AS num_doc, tp.fecha_che,
                               RTRIM(tp.mov_num_c) AS mov_num_c, RTRIM(tp.mov_num_b) AS mov_num_b
                        FROM saPagoTPReng tp
                        LEFT JOIN saCaja cj ON tp.cod_caja = cj.cod_caja
                        LEFT JOIN saCuentaBancaria cb ON tp.cod_cta = cb.cod_cta
                        LEFT JOIN saBanco b ON cb.co_ban = b.co_ban
                        WHERE LTRIM(RTRIM(tp.cob_num)) = LTRIM(RTRIM(@cob_num))
                        ORDER BY tp.reng_num
                    `),
                    pool.request().input('cob_num', sql.VarChar, cob_num).query(`
                        SELECT ri.reng_num, ri.rowguid_reng_cob, RTRIM(ri.num_comprobante) AS num_comprobante,
                               ri.monto_documento, ri.base_imponible, ri.monto_ret_imp, ri.alicuota,
                               RTRIM(ri.numero_documento_afectado) AS numero_documento_afectado
                        FROM saPagoRetenIvaReng ri
                        INNER JOIN saPagoDocReng pdr ON ri.rowguid_reng_cob = pdr.rowguid
                        WHERE LTRIM(RTRIM(pdr.cob_num)) = LTRIM(RTRIM(@cob_num))
                        UNION ALL
                        SELECT r.reng_num, r.rowguid_reng_ori AS rowguid_reng_cob, RTRIM(d.num_comprobante) AS num_comprobante,
                               orig.total_bruto AS monto_documento, 
                               orig.total_bruto - orig.otros1 AS base_imponible, 
                               d.total_neto AS monto_ret_imp, 
                               orig.porc_imp AS alicuota,
                               RTRIM(pdr.nro_doc) AS numero_documento_afectado
                        FROM saPagoDocReng r
                        INNER JOIN saDocumentoCompra d ON LTRIM(RTRIM(r.co_tipo_doc)) = LTRIM(RTRIM(d.co_tipo_doc)) 
                                                      AND LTRIM(RTRIM(r.nro_doc)) = LTRIM(RTRIM(d.nro_doc))
                        INNER JOIN saPagoDocReng pdr ON r.rowguid_reng_ori = pdr.rowguid
                        LEFT JOIN saDocumentoCompra orig ON LTRIM(RTRIM(pdr.co_tipo_doc)) = LTRIM(RTRIM(orig.co_tipo_doc)) 
                                                        AND LTRIM(RTRIM(pdr.nro_doc)) = LTRIM(RTRIM(orig.nro_doc))
                        WHERE LTRIM(RTRIM(r.cob_num)) = LTRIM(RTRIM(@cob_num))
                          AND LTRIM(RTRIM(r.co_tipo_doc)) = 'IVAN'
                    `),
                    pool.request().input('cob_num', sql.VarChar, cob_num).query(`
                        SELECT rn.reng_num, rn.rowguid_reng_cob, RTRIM(rn.co_islr) AS co_islr,
                               rn.monto, rn.monto_reten, rn.monto_obj, rn.porc_retn
                        FROM saPagoRentenReng rn
                        INNER JOIN saPagoDocReng pdr ON rn.rowguid_reng_cob = pdr.rowguid
                        WHERE LTRIM(RTRIM(pdr.cob_num)) = LTRIM(RTRIM(@cob_num))
                        UNION ALL
                        SELECT r.reng_num, r.rowguid_reng_ori AS rowguid_reng_cob, '055' AS co_islr,
                               orig.total_bruto AS monto, 
                               d.total_neto AS monto_reten, 
                               orig.total_bruto - orig.otros1 AS monto_obj,
                               CASE WHEN orig.total_bruto - orig.otros1 > 0 THEN ROUND((d.total_neto / (orig.total_bruto - orig.otros1)) * 100, 2) ELSE 2.00 END AS porc_retn
                        FROM saPagoDocReng r
                        INNER JOIN saDocumentoCompra d ON LTRIM(RTRIM(r.co_tipo_doc)) = LTRIM(RTRIM(d.co_tipo_doc)) 
                                                      AND LTRIM(RTRIM(r.nro_doc)) = LTRIM(RTRIM(d.nro_doc))
                        INNER JOIN saPagoDocReng pdr ON r.rowguid_reng_ori = pdr.rowguid
                        LEFT JOIN saDocumentoCompra orig ON LTRIM(RTRIM(pdr.co_tipo_doc)) = LTRIM(RTRIM(orig.co_tipo_doc)) 
                                                        AND LTRIM(RTRIM(pdr.nro_doc)) = LTRIM(RTRIM(orig.nro_doc))
                        WHERE LTRIM(RTRIM(r.cob_num)) = LTRIM(RTRIM(@cob_num))
                          AND LTRIM(RTRIM(r.co_tipo_doc)) = 'ISLR'
                    `)
                ]);

                if (!resEnc.recordset.length) return null;
                return {
                    ...resEnc.recordset[0],
                    renglones: resReng.recordset,
                    formas_pago: resTP.recordset,
                    retenciones_iva: resIva.recordset,
                    retenciones_islr: resIslr.recordset,
                    sede_id: srv.id,
                    sede_nombre: srv.name
                };
            } catch (e) {
                return { sede_id: srv.id, sede_nombre: srv.name, error: e.message };
            }
        }));

        const found = results.filter(r => r && !r.error);
        if (!found.length)
            return res.status(404).json({ success: false, message: 'Pago no encontrado.' });

        res.status(200).json({ success: true, count: found.length, data: results.filter(r => r !== null) });
    } catch (error) {
        res.status(500).json({ success: false, message: 'Error al consultar Pago.', error: error.message });
    }
});

// --- ANULAR PAGO ---
router.post('/:cob_num/anular', async (req, res) => {
    try {
        const { cob_num } = req.params;
        const { sede } = req.query;

        const outcome = await executeWrite(sede || null, req.sqlAuth, async (pool) => {
            // Verificar si el pago existe y no está anulado
            const resPago = await pool.request()
                .input('cob_num', sql.Char(20), padProfit(cob_num, 20))
                .query(`
                    SELECT anulado, RTRIM(co_prov) AS co_prov, RTRIM(co_sucu_in) AS co_sucu_in
                    FROM saPago
                    WHERE LTRIM(RTRIM(cob_num)) = LTRIM(RTRIM(@cob_num))
                `);
            if (!resPago.recordset.length) throw new Error('El pago no existe.');

            const pago = resPago.recordset[0];
            if (pago.anulado) {
                throw new Error(`El pago ${cob_num} ya está anulado.`);
            }

            // Obtener renglones de documentos pagados
            const resReng = await pool.request()
                .input('cob_num', sql.Char(20), padProfit(cob_num, 20))
                .query(`
                    SELECT RTRIM(co_tipo_doc) AS co_tipo_doc, RTRIM(nro_doc) AS nro_doc, mont_cob, monto_retencion_iva, monto_retencion
                    FROM saPagoDocReng
                    WHERE LTRIM(RTRIM(cob_num)) = LTRIM(RTRIM(@cob_num))
                `);

            const transaction = new sql.Transaction(pool);
            await transaction.begin();

            try {
                const auditUser = (req.profitUser || 'API').substring(0, 10).toUpperCase();

                // 1. Anular cabecera de Pago
                await transaction.request()
                    .input('cob_num', sql.Char(20), padProfit(cob_num, 20))
                    .input('auditUser', sql.Char(6), padProfit(auditUser, 6))
                    .query(`
                        UPDATE saPago
                        SET anulado = 1,
                            fe_us_mo = GETDATE(),
                            co_us_mo = @auditUser
                        WHERE LTRIM(RTRIM(cob_num)) = LTRIM(RTRIM(@cob_num))
                    `);

                // 2. Anular documentos de retención creados por este pago (IVAN, ISLR)
                await transaction.request()
                    .input('cob_num', sql.Char(20), padProfit(cob_num, 20))
                    .input('auditUser', sql.Char(6), padProfit(auditUser, 6))
                    .query(`
                        UPDATE saDocumentoCompra
                        SET anulado = 1,
                            fe_us_mo = GETDATE(),
                            co_us_mo = @auditUser
                        WHERE DOC_ORIG = 'PAGO' AND LTRIM(RTRIM(NRO_ORIG)) = LTRIM(RTRIM(@cob_num))
                    `);

                // 3. Revertir saldo de los documentos de compra pagados
                for (const line of resReng.recordset) {
                    const totalRebaje = Number(line.mont_cob || 0);
                    if (totalRebaje > 0) {
                        await transaction.request()
                            .input('co_tipo_doc', sql.Char(6), padProfit(line.co_tipo_doc, 6))
                            .input('nro_doc', sql.Char(20), padProfit(line.nro_doc, 20))
                            .input('rebaje', sql.Decimal(18, 2), totalRebaje)
                            .input('auditUser', sql.Char(6), padProfit(auditUser, 6))
                            .query(`
                                UPDATE saDocumentoCompra
                                SET saldo = saldo + @rebaje,
                                    fe_us_mo = GETDATE(),
                                    co_us_mo = @auditUser
                                WHERE LTRIM(RTRIM(co_tipo_doc)) = LTRIM(RTRIM(@co_tipo_doc))
                                  AND LTRIM(RTRIM(nro_doc)) = LTRIM(RTRIM(@nro_doc))
                            `);

                        // Si es factura de compra (FACT), revertir saldo también en saFacturaCompra
                        if (line.co_tipo_doc.trim().toUpperCase() === 'FACT') {
                            await transaction.request()
                                .input('nro_doc', sql.Char(20), padProfit(line.nro_doc, 20))
                                .input('rebaje', sql.Decimal(18, 2), totalRebaje)
                                .input('auditUser', sql.Char(6), padProfit(auditUser, 6))
                                .query(`
                                    UPDATE saFacturaCompra
                                    SET saldo = saldo + @rebaje,
                                        fe_us_mo = GETDATE(),
                                        co_us_mo = @auditUser
                                    WHERE LTRIM(RTRIM(doc_num)) = LTRIM(RTRIM(@nro_doc))
                                `);
                        }
                    }
                }

                // 4. Anular movimientos de caja asociados
                await transaction.request()
                    .input('cob_num', sql.Char(20), padProfit(cob_num, 20))
                    .input('auditUser', sql.Char(6), padProfit(auditUser, 6))
                    .query(`
                        UPDATE saMovimientoCaja
                        SET anulado = 1,
                            fe_us_mo = GETDATE(),
                            co_us_mo = @auditUser
                        WHERE LTRIM(RTRIM(doc_num)) = LTRIM(RTRIM(@cob_num)) AND origen = 'PAG'
                    `);

                // 5. Anular movimientos de banco asociados
                await transaction.request()
                    .input('cob_num', sql.Char(20), padProfit(cob_num, 20))
                    .input('auditUser', sql.Char(6), padProfit(auditUser, 6))
                    .query(`
                        UPDATE saMovimientoBanco
                        SET anulado = 1,
                            fe_us_mo = GETDATE(),
                            co_us_mo = @auditUser
                        WHERE LTRIM(RTRIM(cob_pag)) = LTRIM(RTRIM(@cob_num)) AND origen = 'PAG'
                    `);

                await transaction.commit();
                return { success: true, cob_num: cob_num };
            } catch (err) {
                if (transaction._aborted === false) await transaction.rollback();
                throw err;
            }
        });

        return writeResponse(res, outcome);
    } catch (error) {
        res.status(500).json({ success: false, message: 'Error al anular pago.', error: error.message });
    }
});

// --- GUARDAR PAGO ---
router.post('/', async (req, res) => {
    const data = req.body;
    console.log("📥 [AGENT] Recibiendo Pago (SAVE):", JSON.stringify({ co_prov: data.co_prov, renglones: data.renglones?.length, formas_pago: data.formas_pago?.length }, null, 2));

    if (!data.co_prov || !data.renglones || !data.formas_pago) {
        return res.status(400).json({ success: false, message: 'Campos obligatorios: co_prov, renglones, formas_pago' });
    }

    const outcome = await executeWrite(req.query.sede || null, req.sqlAuth, async (pool, srv) => {
        // Cargar Catálogos para valores predeterminados
        const [resSucu, resCtaIE] = await Promise.all([
            pool.request().query(`SELECT TOP 1 RTRIM(co_sucur) AS co_sucur FROM saSucursal`),
            pool.request().query(`SELECT TOP 1 RTRIM(co_cta_ingr_egr) AS co_cta_ingr_egr FROM saCuentaIngEgr`)
        ]);

        const defSucu = resSucu.recordset[0]?.co_sucur || '01';
        const defCtaIE = '01';

        const auditUser = (req.profitUser || req.sqlAuth?.user || 'API').substring(0, 10).toUpperCase();
        const tsDate = new Date();

        const branchCodes = srv.profit_branch_codes || [];
        const defaultCodeObj = branchCodes.find(b => b.is_default === true) || branchCodes[0] || { code: defSucu };
        const sucuCode = defaultCodeObj.code;

        const transaction = new sql.Transaction(pool);
        await transaction.begin();

        try {
            // 1. Obtener correlativo de Pago (PAGO / C020)
            const corrRes = await getProximoConsecutivo({
                runner: transaction,
                co_tipo_serie: 'PAGO',
                co_sucur: sucuCode
            });
            const cobNum = corrRes.docNum;
            console.log(`✨ [AGENT] Nuevo número de pago generado: ${cobNum} (Prefijo: '${corrRes.prefijo}', ProxN: ${corrRes.proxN})`);

            // 1.1 Determinar moneda y montos de cabecera
            let paymentMone = data.co_mone || 'USD';
            if (paymentMone.trim().toUpperCase() === 'US$') paymentMone = 'USD';

            let totalAbonoBs = 0;
            if (data.renglones && Array.isArray(data.renglones)) {
                data.renglones.forEach((line) => {
                    totalAbonoBs += Number(line.mont_cob || 0);
                });
            }

            let totalFormasPagoBs = 0;
            if (data.formas_pago && Array.isArray(data.formas_pago)) {
                data.formas_pago.forEach((fp) => {
                    totalFormasPagoBs += Math.abs(Number(fp.mont_doc || fp.monto || 0));
                });
            }

            let finalMontoHeader = 0;
            if (data.formas_pago && data.formas_pago.length > 0) {
                finalMontoHeader = paymentMone === 'USD' 
                    ? Math.round((totalFormasPagoBs / Number(data.tasa || 1)) * 100) / 100 
                    : totalFormasPagoBs;
            } else {
                finalMontoHeader = Math.max(0, paymentMone === 'USD' 
                    ? Math.round((totalAbonoBs / Number(data.tasa || 1)) * 100) / 100 
                    : totalAbonoBs);
            }

            // 2. Insertar Cabecera de Pago en saPago
            const rH = new sql.Request(transaction);
            rH.input('sCob_Num', sql.Char(20), padProfit(cobNum, 20));
            rH.input('sRecibo', sql.Char(15), null);
            rH.input('sCo_Prov', sql.Char(16), padProfit(data.co_prov, 16));
            rH.input('sCo_Mone', sql.Char(6), padProfit(paymentMone, 6));
            rH.input('deTasa', sql.Decimal(21, 8), Number(data.tasa || 1));
            rH.input('sdFecha', sql.SmallDateTime, tsDate);
            rH.input('bAnulado', sql.Bit, 0);
            rH.input('deMonto', sql.Decimal(18, 2), finalMontoHeader);
            rH.input('sDis_cen', sql.VarChar(sql.MAX), null);
            rH.input('sDescrip', sql.VarChar(60), (data.descrip || `PAGO PROVEEDOR ${data.co_prov}`).substring(0, 60));
            rH.input('sCampo1', sql.VarChar(60), null);
            rH.input('sCampo2', sql.VarChar(60), null);
            rH.input('sCampo3', sql.VarChar(60), null);
            rH.input('sCampo4', sql.VarChar(60), null);
            rH.input('sCampo5', sql.VarChar(60), null);
            rH.input('sCampo6', sql.VarChar(60), null);
            rH.input('sCampo7', sql.VarChar(60), null);
            rH.input('sCampo8', sql.VarChar(60), null);
            rH.input('sCo_Us_In', sql.Char(6), padProfit(auditUser, 6));
            rH.input('sCo_Sucu_In', sql.Char(6), padProfit(sucuCode, 6));
            rH.input('sMaquina', sql.VarChar(60), 'WEB');
            rH.input('sRevisado', sql.Char(1), null);
            rH.input('sTrasnfe', sql.Char(1), null);

            await rH.execute('pInsertarPago');

            // Mapas para relacionar los renglones (Padre-Hijo)
            const rengDocGuidMap = new Map(); // nro_doc -> rowguid de saPagoDocReng
            const parentTypes = ['FACT', 'NDEB', 'N/DB', 'GIRO', 'AJPA'];
            const parentLines = data.renglones.filter(r => parentTypes.includes(r.co_tipo_doc.trim().toUpperCase()));
            const childLines = data.renglones.filter(r => !parentTypes.includes(r.co_tipo_doc.trim().toUpperCase()));
            const sortedRenglones = [...parentLines, ...childLines];
            let nextRengNum = 1;

            // 3. Insertar Renglones de Documentos de Compra (saPagoDocReng)
            for (let i = 0; i < sortedRenglones.length; i++) {
                const line = sortedRenglones[i];
                const rengNum = nextRengNum++;

                let parentGuid = null;
                if (!parentTypes.includes(line.co_tipo_doc.trim().toUpperCase())) {
                    const lookupKey = line.parent_doc ? line.parent_doc.trim() : line.nro_doc?.trim();
                    parentGuid = rengDocGuidMap.get(lookupKey);
                }

                // Consultar saDocumentoCompra para obtener datos y saldo actual
                let docSaldo = 0;
                let docTasa = 1;
                let docMone = 'BS';
                let docNroFact = line.nro_fact || line.nro_doc;
                let docNControl = '';

                const docTypeUpper = line.co_tipo_doc.trim().toUpperCase();
                const queryTypes = ['FACT', 'NDEB', 'N/DB', 'GIRO', 'AJPA', 'N/CR'];
                if (queryTypes.includes(docTypeUpper)) {
                    const docInfo = await transaction.request()
                        .input('co_tipo_doc', sql.Char(6), padProfit(line.co_tipo_doc, 6))
                        .input('nro_doc', sql.Char(20), padProfit(line.nro_doc, 20))
                        .query(`
                            SELECT RTRIM(co_mone) AS co_mone, tasa, saldo, 
                                   RTRIM(ISNULL(nro_fact, nro_doc)) AS nro_fact,
                                   RTRIM(ISNULL(n_control, '')) AS n_control,
                                   CONVERT(VARCHAR(10), fec_emis, 120) AS fec_emis_str
                            FROM saDocumentoCompra
                            WHERE LTRIM(RTRIM(co_tipo_doc)) = LTRIM(RTRIM(@co_tipo_doc))
                              AND LTRIM(RTRIM(nro_doc)) = LTRIM(RTRIM(@nro_doc))
                        `);
                    if (docInfo.recordset.length > 0) {
                        docSaldo = Number(docInfo.recordset[0].saldo || 0);
                        docTasa = Number(docInfo.recordset[0].tasa || 1);
                        docMone = docInfo.recordset[0].co_mone || 'BS';
                        docNroFact = docInfo.recordset[0].nro_fact || docNroFact;
                        docNControl = docInfo.recordset[0].n_control || '';
                    }
                }

                let finalMontCob = Math.abs(Number(line.mont_cob));
                let adjustedMontoRetencionIva = Number(line.monto_retencion_iva || 0);
                let adjustedMontoRetencion = Number(line.monto_retencion || 0);

                let totalRebaje = finalMontCob + adjustedMontoRetencionIva + adjustedMontoRetencion;
                if (totalRebaje > docSaldo && docSaldo > 0) {
                    const excess = totalRebaje - docSaldo;
                    finalMontCob = Math.max(0, finalMontCob - excess);
                    totalRebaje = finalMontCob + adjustedMontoRetencionIva + adjustedMontoRetencion;
                }

                const guidResult = await transaction.request().query('SELECT NEWID() AS guid');
                const lineGuid = guidResult.recordset[0].guid;
                rengDocGuidMap.set(line.nro_doc?.trim(), lineGuid);

                // Insertar renglón en saPagoDocReng
                await transaction.request()
                    .input('reng_num', sql.Int, rengNum)
                    .input('cob_num', sql.Char(20), padProfit(cobNum, 20))
                    .input('co_tipo_doc', sql.Char(6), padProfit(line.co_tipo_doc, 6))
                    .input('nro_doc', sql.Char(20), padProfit(line.nro_doc, 20))
                    .input('nro_fact', sql.Char(20), padProfit(docNroFact, 20))
                    .input('mont_cob', sql.Decimal(18, 2), totalRebaje)
                    .input('monto_retencion_iva', sql.Decimal(18, 5), adjustedMontoRetencionIva)
                    .input('monto_retencion', sql.Decimal(18, 2), adjustedMontoRetencion)
                    .input('rowguid_reng_ori', sql.UniqueIdentifier, parentGuid)
                    .input('co_sucu_in', sql.Char(6), padProfit(sucuCode, 6))
                    .input('co_us_in', sql.Char(6), padProfit(auditUser, 6))
                    .input('rowguid', sql.UniqueIdentifier, lineGuid)
                    .query(`
                        INSERT INTO saPagoDocReng (
                            reng_num, cob_num, co_tipo_doc, nro_doc, nro_fact, mont_cob,
                            dppago_porc_desc, dppago_monto, monto_retencion_iva, monto_retencion,
                            tipo_doc, num_doc, rowguid_reng_ori,
                            co_sucu_in, co_us_in, fe_us_in, co_sucu_mo, co_us_mo, fe_us_mo,
                            trasnfe, revisado, rowguid
                        ) VALUES (
                            @reng_num, @cob_num, @co_tipo_doc, @nro_doc, @nro_fact, @mont_cob,
                            0.00, 0.00, @monto_retencion_iva, @monto_retencion,
                            NULL, NULL, @rowguid_reng_ori,
                            @co_sucu_in, @co_us_in, GETDATE(), @co_sucu_in, @co_us_in, GETDATE(),
                            NULL, NULL, @rowguid
                        )
                    `);

                // Rebajar saldo en saDocumentoCompra
                await transaction.request()
                    .input('co_tipo_doc', sql.Char(6), padProfit(line.co_tipo_doc, 6))
                    .input('nro_doc', sql.Char(20), padProfit(line.nro_doc, 20))
                    .input('rebaje', sql.Decimal(18, 2), totalRebaje)
                    .input('user', sql.Char(6), padProfit(auditUser, 6))
                    .query(`
                        UPDATE saDocumentoCompra
                        SET saldo = saldo - @rebaje,
                            fe_us_mo = GETDATE(),
                            co_us_mo = @user
                        WHERE LTRIM(RTRIM(co_tipo_doc)) = LTRIM(RTRIM(@co_tipo_doc))
                          AND LTRIM(RTRIM(nro_doc)) = LTRIM(RTRIM(@nro_doc))
                    `);

                // Si es FACT, también rebajar en saFacturaCompra
                if (line.co_tipo_doc.trim().toUpperCase() === 'FACT') {
                    await transaction.request()
                        .input('nro_doc', sql.Char(20), padProfit(line.nro_doc, 20))
                        .input('rebaje', sql.Decimal(18, 2), totalRebaje)
                        .input('user', sql.Char(6), padProfit(auditUser, 6))
                        .query(`
                            UPDATE saFacturaCompra
                            SET saldo = saldo - @rebaje,
                                fe_us_mo = GETDATE(),
                                co_us_mo = @user
                            WHERE LTRIM(RTRIM(doc_num)) = LTRIM(RTRIM(@nro_doc))
                        `);
                }

                // 3.1 Generar documento IVAN (Retención de IVA en compras)
                if (adjustedMontoRetencionIva > 0) {
                    const corrIvan = await getProximoConsecutivo({
                        runner: transaction,
                        co_tipo_serie: 'IVAN_COMPRA',
                        co_sucur: sucuCode
                    });
                    const ivanNum = corrIvan.docNum;

                    // Formatear comprobante fiscal SENIAT: YYYYMM + 8 dígitos
                    const today = new Date();
                    const periodStr = today.getFullYear() + String(today.getMonth() + 1).padStart(2, "0");
                    const comprobanteSeniat = `${periodStr}${String(corrIvan.proxN).padStart(8, '0')}`;

                    await transaction.request()
                        .input('co_tipo_doc', sql.Char(6), padProfit('IVAN', 6))
                        .input('nro_doc', sql.Char(20), padProfit(ivanNum, 20))
                        .input('co_prov', sql.Char(16), padProfit(data.co_prov, 16))
                        .input('co_mone', sql.Char(6), padProfit(docMone, 6))
                        .input('tasa', sql.Decimal(21, 8), docTasa)
                        .input('observa', sql.VarChar(120), `PAGO N° ${cobNum} de proveedor ${data.co_prov}`)
                        .input('doc_orig', sql.Char(6), padProfit('PAGO', 6))
                        .input('tipo_origen', sql.Int, 0)
                        .input('nro_orig', sql.VarChar(20), cobNum)
                        .input('total_bruto', sql.Decimal(18, 2), adjustedMontoRetencionIva)
                        .input('total_neto', sql.Decimal(18, 2), adjustedMontoRetencionIva)
                        .input('saldo', sql.Decimal(18, 2), 0.00)
                        .input('co_us_in', sql.Char(6), padProfit(auditUser, 6))
                        .input('co_sucu_in', sql.Char(6), padProfit(sucuCode, 6))
                        .input('num_comprobante', sql.Char(14), comprobanteSeniat.substring(0, 14))
                        .query(`
                            INSERT INTO saDocumentoCompra (
                                co_tipo_doc, nro_doc, co_prov, co_mone, tasa, observa,
                                fec_reg, fec_emis, fec_venc, anulado, aut,
                                doc_orig, tipo_origen, nro_orig, saldo, total_bruto,
                                total_neto, monto_imp, monto_imp2, monto_imp3, porc_imp, porc_imp2, porc_imp3,
                                otros1, otros2, otros3, co_us_in, co_sucu_in, fe_us_in,
                                co_us_mo, co_sucu_mo, fe_us_mo, rowguid,
                                monto_desc_glob, monto_reca, num_comprobante, tipo_imp
                            ) VALUES (
                                @co_tipo_doc, @nro_doc, @co_prov, @co_mone, @tasa, @observa,
                                CONVERT(VARCHAR(10), GETDATE(), 120), CONVERT(VARCHAR(10), GETDATE(), 120), CONVERT(VARCHAR(10), GETDATE(), 120), 0, 1,
                                @doc_orig, @tipo_origen, @nro_orig, @saldo, @total_bruto,
                                @total_neto, 0, 0, 0, 0, 0, 0,
                                0, 0, 0, @co_us_in, @co_sucu_in, GETDATE(),
                                @co_us_in, @co_sucu_in, GETDATE(), NEWID(),
                                0, 0, @num_comprobante, '7'
                            )
                        `);

                    // Insertar renglón de IVAN en saPagoDocReng
                    const ivanRengNum = nextRengNum++;
                    const ivanRengGuidResult = await transaction.request().query('SELECT NEWID() AS guid');
                    const ivanRengGuid = ivanRengGuidResult.recordset[0].guid;

                    await transaction.request()
                        .input('reng_num', sql.Int, ivanRengNum)
                        .input('cob_num', sql.Char(20), padProfit(cobNum, 20))
                        .input('co_tipo_doc', sql.Char(6), padProfit('IVAN', 6))
                        .input('nro_doc', sql.Char(20), padProfit(ivanNum, 20))
                        .input('mont_cob', sql.Decimal(18, 2), adjustedMontoRetencionIva)
                        .input('rowguid_reng_ori', sql.UniqueIdentifier, lineGuid)
                        .input('co_sucu_in', sql.Char(6), padProfit(sucuCode, 6))
                        .input('co_us_in', sql.Char(6), padProfit(auditUser, 6))
                        .input('rowguid', sql.UniqueIdentifier, ivanRengGuid)
                        .query(`
                            INSERT INTO saPagoDocReng (
                                reng_num, cob_num, co_tipo_doc, nro_doc, mont_cob,
                                dppago_porc_desc, dppago_monto, monto_retencion_iva, monto_retencion,
                                rowguid_reng_ori, co_sucu_in, co_us_in, fe_us_in, co_sucu_mo, co_us_mo, fe_us_mo, rowguid
                            ) VALUES (
                                @reng_num, @cob_num, @co_tipo_doc, @nro_doc, @mont_cob,
                                0.00, 0.00, 0.00, 0.00,
                                @rowguid_reng_ori, @co_sucu_in, @co_us_in, GETDATE(), @co_sucu_in, @co_us_in, GETDATE(), @rowguid
                            )
                        `);

                    // Insertar en saPagoRetenIvaReng
                    const retIvaMatch = data.retenciones_iva?.find(r => r.nro_doc_asoc?.trim() === line.nro_doc?.trim());
                    const rifComprador = retIvaMatch?.rif_comprador || data.co_prov;
                    const baseImponible = Number(retIvaMatch?.base_imponible || 0);
                    const montoDoc = Number(retIvaMatch?.monto_documento || totalRebaje);
                    const alicuota = Number(retIvaMatch?.alicuota || 16);

                    await transaction.request()
                        .input('reng_num', sql.Int, 1)
                        .input('rowguid_reng_cob', sql.UniqueIdentifier, lineGuid)
                        .input('rif_contribuyente', sql.Char(10), (srv.rif || 'J401750354').substring(0, 10))
                        .input('periodo_impositivo', sql.Decimal(6), Number(periodStr))
                        .input('fecha_documento', sql.SmallDateTime, tsDate)
                        .input('tipo_documento', sql.Char(4), 'FACT')
                        .input('rif_comprador', sql.Char(10), rifComprador.substring(0, 10))
                        .input('numero_documento', sql.Char(20), padProfit(docNroFact, 20))
                        .input('numero_control_documento', sql.Char(20), padProfit(docNControl, 20))
                        .input('monto_documento', sql.Decimal(15, 2), montoDoc)
                        .input('base_imponible', sql.Decimal(15, 2), baseImponible)
                        .input('monto_ret_imp', sql.Decimal(15, 2), adjustedMontoRetencionIva)
                        .input('numero_documento_afectado', sql.Char(20), '0                   ')
                        .input('num_comprobante', sql.Char(14), comprobanteSeniat.substring(0, 14))
                        .input('monto_excento', sql.Decimal(15, 2), Number(retIvaMatch?.monto_excento || 0))
                        .input('alicuota', sql.Decimal(5, 2), alicuota)
                        .input('reten_tercero', sql.Bit, 0)
                        .input('numero_expediente', sql.Char(15), '0              ')
                        .input('co_us_in', sql.Char(6), padProfit(auditUser, 6))
                        .input('co_sucu_in', sql.Char(6), padProfit(sucuCode, 6))
                        .query(`
                            INSERT INTO saPagoRetenIvaReng (
                                reng_num, rowguid_reng_cob, rif_contribuyente, periodo_impositivo,
                                fecha_documento, tipo_operacion, tipo_documento, rif_comprador,
                                numero_documento, numero_control_documento, monto_documento,
                                base_imponible, monto_ret_imp, numero_documento_afectado,
                                num_comprobante, monto_excento, alicuota, reten_tercero,
                                numero_expediente, co_us_in, co_sucu_in, fe_us_in, co_us_mo, co_sucu_mo, fe_us_mo,
                                revisado, trasnfe, rowguid
                            ) VALUES (
                                @reng_num, @rowguid_reng_cob, @rif_contribuyente, @periodo_impositivo,
                                @fecha_documento, 'C', @tipo_documento, @rif_comprador,
                                @numero_documento, @numero_control_documento, @monto_documento,
                                @base_imponible, @monto_ret_imp, @numero_documento_afectado,
                                @num_comprobante, @monto_excento, @alicuota, @reten_tercero,
                                @numero_expediente, @co_us_in, @co_sucu_in, GETDATE(), @co_us_in, @co_sucu_in, GETDATE(),
                                NULL, NULL, NEWID()
                            )
                        `);
                }

                // 3.2 Generar documento ISLR (Retención de ISLR en compras)
                if (adjustedMontoRetencion > 0) {
                    const corrIslr = await getProximoConsecutivo({
                        runner: transaction,
                        co_tipo_serie: 'ISLR_COMPRA',
                        co_sucur: sucuCode
                    });
                    const islrNum = corrIslr.docNum;

                    await transaction.request()
                        .input('co_tipo_doc', sql.Char(6), padProfit('ISLR', 6))
                        .input('nro_doc', sql.Char(20), padProfit(islrNum, 20))
                        .input('co_prov', sql.Char(16), padProfit(data.co_prov, 16))
                        .input('co_mone', sql.Char(6), padProfit(docMone, 6))
                        .input('tasa', sql.Decimal(21, 8), docTasa)
                        .input('observa', sql.VarChar(120), `PAGO N° ${cobNum} de proveedor ${data.co_prov}`)
                        .input('doc_orig', sql.Char(6), padProfit('PAGO', 6))
                        .input('tipo_origen', sql.Int, 0)
                        .input('nro_orig', sql.VarChar(20), cobNum)
                        .input('total_bruto', sql.Decimal(18, 2), adjustedMontoRetencion)
                        .input('total_neto', sql.Decimal(18, 2), adjustedMontoRetencion)
                        .input('saldo', sql.Decimal(18, 2), 0.00)
                        .input('co_us_in', sql.Char(6), padProfit(auditUser, 6))
                        .input('co_sucu_in', sql.Char(6), padProfit(sucuCode, 6))
                        .query(`
                            INSERT INTO saDocumentoCompra (
                                co_tipo_doc, nro_doc, co_prov, co_mone, tasa, observa,
                                fec_reg, fec_emis, fec_venc, anulado, aut,
                                doc_orig, tipo_origen, nro_orig, saldo, total_bruto,
                                total_neto, monto_imp, monto_imp2, monto_imp3, porc_imp, porc_imp2, porc_imp3,
                                otros1, otros2, otros3, co_us_in, co_sucu_in, fe_us_in,
                                co_us_mo, co_sucu_mo, fe_us_mo, rowguid,
                                monto_desc_glob, monto_reca
                            ) VALUES (
                                @co_tipo_doc, @nro_doc, @co_prov, @co_mone, @tasa, @observa,
                                CONVERT(VARCHAR(10), GETDATE(), 120), CONVERT(VARCHAR(10), GETDATE(), 120), CONVERT(VARCHAR(10), GETDATE(), 120), 0, 1,
                                @doc_orig, @tipo_origen, @nro_orig, @saldo, @total_bruto,
                                @total_neto, 0, 0, 0, 0, 0, 0,
                                0, 0, 0, @co_us_in, @co_sucu_in, GETDATE(),
                                @co_us_in, @co_sucu_in, GETDATE(), NEWID(),
                                0, 0
                            )
                        `);

                    // Insertar renglón de ISLR en saPagoDocReng
                    const islrRengNum = nextRengNum++;
                    const islrRengGuidResult = await transaction.request().query('SELECT NEWID() AS guid');
                    const islrRengGuid = islrRengGuidResult.recordset[0].guid;

                    await transaction.request()
                        .input('reng_num', sql.Int, islrRengNum)
                        .input('cob_num', sql.Char(20), padProfit(cobNum, 20))
                        .input('co_tipo_doc', sql.Char(6), padProfit('ISLR', 6))
                        .input('nro_doc', sql.Char(20), padProfit(islrNum, 20))
                        .input('mont_cob', sql.Decimal(18, 2), adjustedMontoRetencion)
                        .input('rowguid_reng_ori', sql.UniqueIdentifier, lineGuid)
                        .input('co_sucu_in', sql.Char(6), padProfit(sucuCode, 6))
                        .input('co_us_in', sql.Char(6), padProfit(auditUser, 6))
                        .input('rowguid', sql.UniqueIdentifier, islrRengGuid)
                        .query(`
                            INSERT INTO saPagoDocReng (
                                reng_num, cob_num, co_tipo_doc, nro_doc, mont_cob,
                                dppago_porc_desc, dppago_monto, monto_retencion_iva, monto_retencion,
                                rowguid_reng_ori, co_sucu_in, co_us_in, fe_us_in, co_sucu_mo, co_us_mo, fe_us_mo, rowguid
                            ) VALUES (
                                @reng_num, @cob_num, @co_tipo_doc, @nro_doc, @mont_cob,
                                0.00, 0.00, 0.00, 0.00,
                                @rowguid_reng_ori, @co_sucu_in, @co_us_in, GETDATE(), @co_sucu_in, @co_us_in, GETDATE(), @rowguid
                            )
                        `);

                    // Insertar en saPagoRentenReng
                    const retIslrMatch = data.retenciones_islr?.find(r => r.nro_doc_asoc?.trim() === line.nro_doc?.trim());
                    const coIslr = retIslrMatch?.co_islr || '055';
                    const baseIslr = Number(retIslrMatch?.monto_obj || (docSaldo - (Number(line.monto_imp) || 0)));
                    const porcIslr = Number(retIslrMatch?.porc_retn || 2);

                    await transaction.request()
                        .input('reng_num', sql.Int, 1)
                        .input('rowguid_reng_cob', sql.UniqueIdentifier, lineGuid)
                        .input('co_islr', sql.Char(6), padProfit(coIslr, 6))
                        .input('monto', sql.Decimal(18, 5), baseIslr)
                        .input('monto_reten', sql.Decimal(18, 5), adjustedMontoRetencion)
                        .input('monto_obj', sql.Decimal(18, 5), baseIslr)
                        .input('sustraendo', sql.Decimal(18, 5), Number(retIslrMatch?.sustraendo || 0))
                        .input('porc_retn', sql.Decimal(18, 5), porcIslr)
                        .input('co_us_in', sql.Char(6), padProfit(auditUser, 6))
                        .input('co_sucu_in', sql.Char(6), padProfit(sucuCode, 6))
                        .query(`
                            INSERT INTO saPagoRentenReng (
                                reng_num, rowguid_reng_cob, co_islr, monto, monto_reten, monto_obj,
                                sustraendo, porc_retn, automatica, co_us_in, co_sucu_in, fe_us_in, co_us_mo, co_sucu_mo, fe_us_mo,
                                revisado, trasnfe, rowguid, rowguid_fact
                            ) VALUES (
                                @reng_num, @rowguid_reng_cob, @co_islr, @monto, @monto_reten, @monto_obj,
                                @sustraendo, @porc_retn, 0, @co_us_in, @co_sucu_in, GETDATE(), @co_us_in, @co_sucu_in, GETDATE(),
                                NULL, NULL, NEWID(), NULL
                            )
                        `);
                }
            }

            // 4. Insertar Formas de Pago (saPagoTPReng) y crear egresos en Caja/Banco
            const activeFormasPago = data.formas_pago && data.formas_pago.length > 0
                ? data.formas_pago
                : [{
                    forma_pag: 'EF',
                    cod_caja: '02',
                    cod_cta: null,
                    co_ban: null,
                    co_tar: null,
                    num_doc: null,
                    mont_doc: 0,
                    fecha_che: null
                  }];

            for (let i = 0; i < activeFormasPago.length; i++) {
                const tp = activeFormasPago[i];
                const rengNum = i + 1;
                let movNumC = null;
                let movNumB = null;

                if (tp.forma_pag === 'EF') {
                    // Obtener la moneda de la caja
                    let isUSDcaja = false;
                    const resCajaInfo = await transaction.request()
                        .input('codCaja', sql.Char(6), padProfit(tp.cod_caja, 6))
                        .query('SELECT RTRIM(co_mone) AS co_mone FROM saCaja WHERE cod_caja = @codCaja');
                    if (resCajaInfo.recordset[0] && resCajaInfo.recordset[0].co_mone !== 'BS' && resCajaInfo.recordset[0].co_mone !== 'VES') {
                        isUSDcaja = true;
                    }

                    const rate = Number(data.tasa || 1);
                    const rawMonto = Number(tp.mont_doc);
                    const finalMontoCaja = isUSDcaja ? Math.round((rawMonto / rate) * 100) / 100 : rawMonto;

                    if (finalMontoCaja > 0) {
                        const corrCaja = await getProximoConsecutivo({
                            runner: transaction,
                            co_tipo_serie: 'B001',
                            co_consecutivos: ['MOVC_NUM', 'B001'],
                            co_sucur: sucuCode,
                            table: 'saMovimientoCaja',
                            col: 'mov_num'
                        });
                        movNumC = corrCaja.docNum;

                        // Crear Movimiento de Caja tipo 'E' (Egreso)
                        const rMovC = new sql.Request(transaction);
                        rMovC.input('sMov_Num', sql.Char(20), padProfit(movNumC, 20));
                        rMovC.input('sdFecha', sql.SmallDateTime, tsDate);
                        rMovC.input('sDescrip', sql.VarChar(60), (`EGRESO PAGO ${cobNum} - ${data.co_prov}`).substring(0, 60));
                        rMovC.input('sCod_Caja', sql.Char(6), padProfit(tp.cod_caja, 6));
                        rMovC.input('deTasa', sql.Decimal(21, 8), isUSDcaja ? rate : 1);
                        rMovC.input('sTipo_Mov', sql.Char(2), 'E'); // Egreso
                        rMovC.input('sForma_Pag', sql.Char(2), tp.forma_pag);
                        rMovC.input('sNum_Pago', sql.VarChar(20), tp.num_doc ? tp.num_doc.substring(0, 20) : null);
                        rMovC.input('sCo_Ban', sql.Char(6), null);
                        rMovC.input('sCo_Tar', sql.Char(6), null);
                        rMovC.input('sCo_Cta_Ingr_Egr', sql.Char(20), padProfit(defCtaIE, 20));
                        rMovC.input('deMonto', sql.Decimal(18, 2), finalMontoCaja);
                        rMovC.input('bSaldo_Ini', sql.Bit, 0);
                        rMovC.input('sOrigen', sql.Char(3), 'PAG');
                        rMovC.input('sDoc_Num', sql.VarChar(20), cobNum.substring(0, 20));
                        rMovC.input('sDep_Num', sql.VarChar(20), null);
                        rMovC.input('bAnulado', sql.Bit, 0);
                        rMovC.input('bDepositado', sql.Bit, 0);
                        rMovC.input('bConciliado', sql.Bit, 0);
                        rMovC.input('bTransferido', sql.Bit, 0);
                        rMovC.input('sdFecha_Che', sql.SmallDateTime, tsDate);
                        rMovC.input('sCo_Us_In', sql.Char(6), padProfit(auditUser, 6));
                        rMovC.input('sCo_Sucu_In', sql.Char(6), padProfit(sucuCode, 6));
                        rMovC.input('sRevisado', sql.Char(1), null);
                        rMovC.input('sTrasnfe', sql.Char(1), null);

                        await rMovC.execute('pInsertarMovimientoCaja');
                    }
                } else if (tp.forma_pag === 'TE' || tp.forma_pag === 'DP' || tp.forma_pag === 'CH' || tp.forma_pag === 'TP') {
                    // Obtener la moneda de la cuenta bancaria
                    let isUSDcuenta = false;
                    const resCtaInfo = await transaction.request()
                        .input('codCta', sql.Char(6), padProfit(tp.cod_cta, 6))
                        .query('SELECT RTRIM(co_mone) AS co_mone FROM saCuentaBancaria WHERE cod_cta = @codCta');
                    if (resCtaInfo.recordset[0] && resCtaInfo.recordset[0].co_mone !== 'BS' && resCtaInfo.recordset[0].co_mone !== 'VES') {
                        isUSDcuenta = true;
                    }

                    const rate = Number(data.tasa || 1);
                    const rawMonto = Number(tp.mont_doc);
                    const finalMontoBanco = isUSDcuenta ? Math.round((rawMonto / rate) * 100) / 100 : rawMonto;

                    if (finalMontoBanco > 0) {
                        const corrBanco = await getProximoConsecutivo({
                            runner: transaction,
                            co_tipo_serie: 'B002',
                            co_consecutivos: ['MOVB_NUM', 'B002'],
                            co_sucur: sucuCode,
                            table: 'saMovimientoBanco',
                            col: 'mov_num'
                        });
                        movNumB = corrBanco.docNum;

                        let tipoOp = 'TR'; // Transferencia
                        if (tp.forma_pag === 'CH') tipoOp = 'CH'; // Cheque

                        // Crear Movimiento de Banco
                        const rMovB = new sql.Request(transaction);
                        rMovB.input('sMov_Num', sql.Char(20), padProfit(movNumB, 20));
                        rMovB.input('sDescrip', sql.VarChar(160), (`EGRESO PAGO ${cobNum} - ${data.co_prov}`).substring(0, 160));
                        rMovB.input('sCod_Cta', sql.Char(6), padProfit(tp.cod_cta, 6));
                        rMovB.input('sdFecha', sql.SmallDateTime, tsDate);
                        rMovB.input('deTasa', sql.Decimal(21, 8), isUSDcuenta ? rate : 1);
                        rMovB.input('sTipo_Op', sql.Char(2), tipoOp);
                        rMovB.input('sDoc_Num', sql.VarChar(20), (tp.num_doc || '').substring(0, 20));
                        rMovB.input('deMonto', sql.Decimal(18, 2), finalMontoBanco);
                        rMovB.input('sCo_Cta_Ingr_Egr', sql.Char(20), padProfit(defCtaIE, 20));
                        rMovB.input('sOrigen', sql.Char(3), 'PAG');
                        rMovB.input('sCob_Pag', sql.Char(20), padProfit(cobNum, 20));
                        rMovB.input('deIDB', sql.Decimal(18, 2), 0.00);
                        rMovB.input('sDep_Num', sql.Char(20), null);
                        rMovB.input('bAnulado', sql.Bit, 0);
                        rMovB.input('bSaldo_Ini', sql.Bit, 0);
                        rMovB.input('bConciliado', sql.Bit, 0);
                        rMovB.input('bOri_Dep', sql.Bit, 0);
                        rMovB.input('iDep_Con', sql.Int, 0);
                        rMovB.input('sCod_IngBen', sql.Char(6), null);
                        rMovB.input('sdFecha_Che', sql.SmallDateTime, tsDate);
                        rMovB.input('sCo_Us_In', sql.Char(6), padProfit(auditUser, 6));
                        rMovB.input('sCo_Sucu_In', sql.Char(6), padProfit(sucuCode, 6));
                        rMovB.input('sRevisado', sql.Char(1), null);
                        rMovB.input('sTrasnfe', sql.Char(1), null);

                        await rMovB.execute('pInsertarMovimientoBanco');
                    }
                }

                // Insertar renglón en saPagoTPReng
                await transaction.request()
                    .input('reng_num', sql.Int, rengNum)
                    .input('cob_num', sql.Char(20), padProfit(cobNum, 20))
                    .input('forma_pag', sql.Char(2), tp.forma_pag === 'TE' ? 'TP' : tp.forma_pag)
                    .input('cod_cta', sql.Char(6), tp.cod_cta ? padProfit(tp.cod_cta, 6) : null)
                    .input('cod_caja', sql.Char(6), tp.cod_caja ? padProfit(tp.cod_caja, 6) : null)
                    .input('co_ban', sql.Char(6), tp.co_ban ? padProfit(tp.co_ban, 6) : null)
                    .input('mov_num_c', sql.Char(20), movNumC ? padProfit(movNumC, 20) : null)
                    .input('mov_num_b', sql.Char(20), movNumB ? padProfit(movNumB, 20) : null)
                    .input('num_doc', sql.Char(20), tp.num_doc ? padProfit(tp.num_doc, 20) : null)
                    .input('mont_doc', sql.Decimal(18, 2), Number(tp.mont_doc))
                    .input('fecha_che', sql.SmallDateTime, tp.fecha_che ? new Date(tp.fecha_che) : tsDate)
                    .input('co_sucu_in', sql.Char(6), padProfit(sucuCode, 6))
                    .input('co_us_in', sql.Char(6), padProfit(auditUser, 6))
                    .query(`
                        INSERT INTO saPagoTPReng (
                            reng_num, cob_num, forma_pag, cod_cta, cod_caja, co_ban,
                            mov_num_c, mov_num_b, num_doc, mont_doc, fecha_che,
                            co_sucu_in, co_us_in, fe_us_in, co_sucu_mo, co_us_mo, fe_us_mo,
                            trasnfe, revisado, rowguid
                        ) VALUES (
                            @reng_num, @cob_num, @forma_pag, @cod_cta, @cod_caja, @co_ban,
                            @mov_num_c, @mov_num_b, @num_doc, @mont_doc, @fecha_che,
                            @co_sucu_in, @co_us_in, GETDATE(), @co_sucu_in, @co_us_in, GETDATE(),
                            NULL, NULL, NEWID()
                        )
                    `);
            }

            await transaction.commit();
            return { success: true, doc_num: cobNum };
        } catch (err) {
            if (transaction._aborted === false) await transaction.rollback();
            throw err;
        }
    });

    return writeResponse(res, outcome);
});

module.exports = router;
