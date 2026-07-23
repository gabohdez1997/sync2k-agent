const express = require('express');
const router = express.Router();
const { sql, getPool, getServers } = require('../db');
const { executeWrite, writeResponse, paginatedResponse, padProfit } = require('../helpers/multiSede');

/**
 * @swagger
 * tags:
 *   name: Cobros
 *   description: Gestión de Cobros y recibos de caja/banco de clientes
 */

// --- OBTENER LISTADO ---
router.get('/', async (req, res) => {
    try {
        const page = parseInt(req.query.page) || 1;
        const limit = parseInt(req.query.limit) || 12;
        const { sede, co_cli, co_ven, co_us_in, fec_d, fec_h, search } = req.query;

        const servers = getServers();
        const targets = sede ? servers.filter(s => s.id === sede) : servers;

        const allData = await Promise.all(targets.map(async (srv) => {
            try {
                const pool = await getPool(srv.id, req.sqlAuth);
                const request = pool.request();
                let whereClauses = ["1=1"];

                if (co_cli) {
                    request.input('co_cli_search', sql.VarChar, `%${co_cli}%`);
                    whereClauses.push("(c.co_cli LIKE @co_cli_search OR cl.cli_des LIKE @co_cli_search OR cl.rif LIKE @co_cli_search)");
                }
                if (search) {
                    request.input('search_all', sql.VarChar, `%${search}%`);
                    whereClauses.push("(c.cob_num LIKE @search_all OR c.co_cli LIKE @search_all OR cl.cli_des LIKE @search_all OR cl.rif LIKE @search_all)");
                }
                if (co_ven) {
                    request.input('co_ven_filter', sql.VarChar, co_ven.trim().toUpperCase());
                    whereClauses.push("LTRIM(RTRIM(c.co_ven)) = @co_ven_filter");
                }
                if (co_us_in) {
                    request.input('co_us_in_filter', sql.VarChar, co_us_in.trim().toUpperCase());
                    whereClauses.push("LTRIM(RTRIM(c.co_us_in)) = @co_us_in_filter");
                }
                if (fec_d) {
                    request.input('fec_d', sql.VarChar, `${fec_d} 00:00:00`);
                    whereClauses.push("c.fe_us_in >= @fec_d");
                }
                if (fec_h) {
                    request.input('fec_h', sql.VarChar, `${fec_h} 23:59:59`);
                    whereClauses.push("c.fe_us_in <= @fec_h");
                }

                const whereSQL = whereClauses.join(" AND ");

                const result = await request.query(`
                    SELECT RTRIM(c.cob_num) AS cob_num, RTRIM(c.recibo) AS recibo, RTRIM(c.descrip) AS descrip,
                           RTRIM(c.co_cli) AS co_cli, RTRIM(cl.cli_des) AS cli_des,
                           c.fe_us_in AS fecha, c.anulado,
                           ISNULL((SELECT SUM(mont_doc) FROM saCobroTPReng WHERE cob_num = c.cob_num), 0) AS monto,
                           RTRIM(c.co_mone) AS co_mone, 
                           CASE WHEN c.tasa <= 1.000001 THEN 
                                ISNULL((SELECT TOP 1 t.tasa_v FROM saTasa t WHERE LTRIM(RTRIM(t.co_mone)) IN ('USD', 'US$', 'US') AND CONVERT(VARCHAR(10), t.fecha, 120) <= CONVERT(VARCHAR(10), c.fecha, 120) ORDER BY t.fecha DESC), 1.0)
                           ELSE c.tasa END AS tasa,
                           RTRIM(c.co_ven) AS co_ven, RTRIM(c.co_us_in) AS co_us_in,
                           ISNULL(
                               SUBSTRING(
                                   (SELECT ', ' + RTRIM(r.co_tipo_doc) + ' ' + RTRIM(r.nro_doc) + ':' + RTRIM(ISNULL(d.co_sucu_in, '')) + ':' + CAST(ISNULL(d.monto_imp, 0) AS VARCHAR)
                                    FROM saCobroDocReng r
                                    LEFT JOIN saDocumentoVenta d ON r.co_tipo_doc = d.co_tipo_doc 
                                                                AND r.nro_doc = d.nro_doc
                                    WHERE r.cob_num = c.cob_num 
                                      AND r.co_tipo_doc IN ('FACT  ', 'NDEB  ', 'N/DB  ', 'GIRO  ', 'AJPA  ')
                                    ORDER BY r.reng_num
                                    FOR XML PATH('')), 
                                   3, 
                                   200
                               ), 
                               '---'
                           ) AS documentos_asociados
                    FROM saCobro c
                    LEFT JOIN saCliente cl ON c.co_cli = cl.co_cli
                    WHERE ${whereSQL}
                    ORDER BY c.fe_us_in DESC, c.cob_num DESC
                `);

                return result.recordset.map(r => ({ ...r, sede_id: srv.id, sede_nombre: srv.name }));
            } catch (e) {
                console.error(`[COBROS] Error en sede ${srv.id}:`, e.message);
                return [];
            }
        }));

        const combined = [].concat(...allData);
        combined.sort((a, b) => new Date(b.fecha) - new Date(a.fecha));
        return paginatedResponse(res, combined, page, limit);
    } catch (error) {
        res.status(500).json({ success: false, message: 'Error al consultar Cobros.', error: error.message });
    }
});

// --- OBTENER FACTURAS PENDIENTES CON SALDO ---
router.get('/facturas/pendientes', async (req, res) => {
    try {
        const { search, sede } = req.query;
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

                if (search) {
                    request.input('search', sql.VarChar, `%${search.trim()}%`);
                    whereClauses.push(`(
                        d.nro_doc LIKE @search 
                        OR d.co_cli LIKE @search 
                        OR c.cli_des LIKE @search 
                        OR c.rif LIKE @search
                    )`);
                }

                const whereSQL = whereClauses.join(" AND ");
                const result = await request.query(`
                    SELECT TOP 100 
                           RTRIM(d.co_tipo_doc) AS co_tipo_doc, 
                           RTRIM(d.nro_doc) AS nro_doc, 
                           d.fec_emis, d.fec_venc, 
                           d.total_neto, d.saldo, d.monto_imp,
                           RTRIM(d.co_mone) AS co_mone,
                           CASE WHEN d.tasa <= 1.000001 THEN 
                                 ISNULL((SELECT TOP 1 t.tasa_v FROM saTasa t WHERE LTRIM(RTRIM(t.co_mone)) IN ('USD', 'US$', 'US') AND CONVERT(VARCHAR(10), t.fecha, 120) <= CONVERT(VARCHAR(10), d.fec_emis, 120) ORDER BY t.fecha DESC), 1.0)
                           ELSE d.tasa END AS tasa,
                           RTRIM(d.n_control) AS n_control,
                           d.rowguid,
                           RTRIM(d.co_cli) AS co_cli,
                           RTRIM(c.cli_des) AS cli_des,
                           RTRIM(c.rif) AS rif,
                           c.contribu_e, c.porc_esp, c.co_ven,
                           ISNULL(CASE WHEN RTRIM(d.co_tipo_doc) = 'FACT' THEN f.otros1 ELSE d.otros1 END, 0) AS otros1
                    FROM saDocumentoVenta d
                    INNER JOIN saCliente c ON d.co_cli = c.co_cli
                    LEFT JOIN saFacturaVenta f ON RTRIM(d.co_tipo_doc) = 'FACT' AND LTRIM(RTRIM(d.nro_doc)) = LTRIM(RTRIM(f.doc_num))
                    WHERE ${whereSQL}
                    ORDER BY d.fec_emis DESC
                `);;

                return result.recordset.map(r => ({
                    ...r,
                    sede_id: srv.id,
                    sede_nombre: srv.name
                }));
            } catch (e) {
                console.error(`[PENDIENTES] Error en sede ${srv.id}:`, e.message);
                return [];
            }
        }));

        const combined = [].concat(...allData);
        combined.sort((a, b) => new Date(b.fec_emis) - new Date(a.fec_emis));

        // Paginar
        const start = (page - 1) * limit;
        const paginated = combined.slice(start, start + limit);

        res.status(200).json({
            success: true,
            count: combined.length,
            data: paginated
        });
    } catch (error) {
        res.status(500).json({ success: false, message: 'Error al consultar facturas pendientes.', error: error.message });
    }
});

// --- OBTENER DETALLE ---
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
                        SELECT RTRIM(c.cob_num) AS cob_num, RTRIM(c.recibo) AS recibo, RTRIM(c.descrip) AS descrip,
                               RTRIM(c.co_cli) AS co_cli, RTRIM(cl.cli_des) AS cli_des, RTRIM(cl.rif) AS rif,
                               ISNULL(cl.porc_esp, 0) AS porc_esp, cl.contribu_e,
                               c.fe_us_in AS fecha, c.anulado,
                               ISNULL((SELECT SUM(mont_doc) FROM saCobroTPReng WHERE cob_num = c.cob_num), 0) AS monto,
                               RTRIM(c.co_mone) AS co_mone, 
                               CASE WHEN c.tasa <= 1.000001 THEN 
                                    ISNULL((SELECT TOP 1 t.tasa_v FROM saTasa t WHERE LTRIM(RTRIM(t.co_mone)) IN ('USD', 'US$', 'US') AND CONVERT(VARCHAR(10), t.fecha, 120) <= CONVERT(VARCHAR(10), c.fecha, 120) ORDER BY t.fecha DESC), 1.0)
                               ELSE c.tasa END AS tasa,
                               RTRIM(c.co_ven) AS co_ven, RTRIM(v.ven_des) AS ven_des,
                               RTRIM(c.co_us_in) AS co_us_in
                        FROM saCobro c
                        LEFT JOIN saCliente cl ON c.co_cli = cl.co_cli
                        LEFT JOIN saVendedor v ON c.co_ven = v.co_ven
                        WHERE LTRIM(RTRIM(c.cob_num)) = LTRIM(RTRIM(@cob_num))
                    `),
                    pool.request().input('cob_num', sql.VarChar, cob_num).query(`
                        SELECT r.reng_num, RTRIM(r.co_tipo_doc) AS co_tipo_doc, RTRIM(r.nro_doc) AS nro_doc,
                               r.mont_cob, r.monto_retencion_iva, r.monto_retencion,
                               r.rowguid, r.rowguid_reng_ori,
                               ISNULL(d.otros1, 0) AS otros1,
                               ISNULL(d.monto_imp, 0) AS monto_imp,
                               ISNULL(d.total_neto, 0) AS total_neto
                        FROM saCobroDocReng r
                        LEFT JOIN saDocumentoVenta d ON LTRIM(RTRIM(r.co_tipo_doc)) = LTRIM(RTRIM(d.co_tipo_doc)) 
                                                    AND LTRIM(RTRIM(r.nro_doc)) = LTRIM(RTRIM(d.nro_doc))
                        WHERE LTRIM(RTRIM(r.cob_num)) = LTRIM(RTRIM(@cob_num))
                        ORDER BY r.reng_num
                    `),
                    pool.request().input('cob_num', sql.VarChar, cob_num).query(`
                        SELECT tp.reng_num, RTRIM(tp.forma_pag) AS forma_pag, tp.mont_doc,
                               RTRIM(tp.cod_caja) AS cod_caja, RTRIM(cj.descrip) AS caja_des,
                               RTRIM(tp.cod_cta) AS cod_cta, RTRIM(cb.num_cta) AS cta_des,
                               RTRIM(tp.co_ban) AS co_ban, RTRIM(b.des_ban) AS ban_des,
                               RTRIM(tp.co_tar) AS co_tar, RTRIM(t.des_tar) AS tar_des,
                               RTRIM(tp.num_doc) AS num_doc, tp.fecha_che,
                               RTRIM(tp.mov_num_c) AS mov_num_c, RTRIM(tp.mov_num_b) AS mov_num_b
                        FROM saCobroTPReng tp
                        LEFT JOIN saCaja cj ON tp.cod_caja = cj.cod_caja
                        LEFT JOIN saCuentaBancaria cb ON tp.cod_cta = cb.cod_cta
                        LEFT JOIN saBanco b ON tp.co_ban = b.co_ban
                        LEFT JOIN saTarjetaCredito t ON tp.co_tar = t.co_tar
                        WHERE LTRIM(RTRIM(tp.cob_num)) = LTRIM(RTRIM(@cob_num))
                        ORDER BY tp.reng_num
                    `),
                    pool.request().input('cob_num', sql.VarChar, cob_num).query(`
                        SELECT ri.reng_num, ri.rowguid_reng_cob, RTRIM(ri.num_comprobante) AS num_comprobante,
                               ri.monto_documento, ri.base_imponible, ri.monto_ret_imp, ri.alicuota,
                               RTRIM(ri.numero_documento_afectado) AS numero_documento_afectado
                        FROM saCobroRetenIvaReng ri
                        INNER JOIN saCobroDocReng cdr ON ri.rowguid_reng_cob = cdr.rowguid
                        WHERE LTRIM(RTRIM(cdr.cob_num)) = LTRIM(RTRIM(@cob_num))
                        UNION ALL
                        SELECT r.reng_num, r.rowguid_reng_ori AS rowguid_reng_cob, RTRIM(d.num_comprobante) AS num_comprobante,
                               orig.total_bruto AS monto_documento, 
                               orig.total_bruto - orig.otros1 AS base_imponible, 
                               d.total_neto AS monto_ret_imp, 
                               orig.porc_imp AS alicuota,
                               RTRIM(cdr.nro_doc) AS numero_documento_afectado
                        FROM saCobroDocReng r
                        INNER JOIN saDocumentoVenta d ON LTRIM(RTRIM(r.co_tipo_doc)) = LTRIM(RTRIM(d.co_tipo_doc)) 
                                                    AND LTRIM(RTRIM(r.nro_doc)) = LTRIM(RTRIM(d.nro_doc))
                        INNER JOIN saCobroDocReng cdr ON r.rowguid_reng_ori = cdr.rowguid
                        LEFT JOIN saDocumentoVenta orig ON LTRIM(RTRIM(cdr.co_tipo_doc)) = LTRIM(RTRIM(orig.co_tipo_doc)) 
                                                       AND LTRIM(RTRIM(cdr.nro_doc)) = LTRIM(RTRIM(orig.nro_doc))
                        WHERE LTRIM(RTRIM(r.cob_num)) = LTRIM(RTRIM(@cob_num))
                          AND LTRIM(RTRIM(r.co_tipo_doc)) = 'IVAN'
                    `),
                    pool.request().input('cob_num', sql.VarChar, cob_num).query(`
                        SELECT rn.reng_num, rn.rowguid_reng_cob, RTRIM(rn.co_islr) AS co_islr,
                               rn.monto, rn.monto_reten, rn.monto_obj, rn.porc_retn
                        FROM saCobroRentenReng rn
                        INNER JOIN saCobroDocReng cdr ON rn.rowguid_reng_cob = cdr.rowguid
                        WHERE LTRIM(RTRIM(cdr.cob_num)) = LTRIM(RTRIM(@cob_num))
                        UNION ALL
                        SELECT r.reng_num, r.rowguid_reng_ori AS rowguid_reng_cob, '001' AS co_islr,
                               orig.total_bruto AS monto, 
                               d.total_neto AS monto_reten, 
                               orig.total_bruto - orig.otros1 AS monto_obj,
                               CASE WHEN orig.total_bruto - orig.otros1 > 0 THEN ROUND((d.total_neto / (orig.total_bruto - orig.otros1)) * 100, 2) ELSE 2.00 END AS porc_retn
                        FROM saCobroDocReng r
                        INNER JOIN saDocumentoVenta d ON LTRIM(RTRIM(r.co_tipo_doc)) = LTRIM(RTRIM(d.co_tipo_doc)) 
                                                    AND LTRIM(RTRIM(r.nro_doc)) = LTRIM(RTRIM(d.nro_doc))
                        INNER JOIN saCobroDocReng cdr ON r.rowguid_reng_ori = cdr.rowguid
                        LEFT JOIN saDocumentoVenta orig ON LTRIM(RTRIM(cdr.co_tipo_doc)) = LTRIM(RTRIM(orig.co_tipo_doc)) 
                                                       AND LTRIM(RTRIM(cdr.nro_doc)) = LTRIM(RTRIM(orig.nro_doc))
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
            return res.status(404).json({ success: false, message: 'Cobro no encontrado.' });

        res.status(200).json({ success: true, count: found.length, data: results.filter(r => r !== null) });
    } catch (error) {
        res.status(500).json({ success: false, message: 'Error al consultar Cobro.', error: error.message });
    }
});

// --- ANULAR COBRO ---
router.post('/:cob_num/anular', async (req, res) => {
    try {
        const { cob_num } = req.params;
        const { sede } = req.query;

        const outcome = await executeWrite(sede || null, req.sqlAuth, async (pool) => {
            // Check if collection exists and is not already voided
            const resCob = await pool.request()
                .input('cob_num', sql.Char(20), padProfit(cob_num, 20))
                .query(`
                    SELECT anulado, RTRIM(co_cli) AS co_cli, RTRIM(co_sucu_in) AS co_sucu_in
                    FROM saCobro
                    WHERE LTRIM(RTRIM(cob_num)) = LTRIM(RTRIM(@cob_num))
                `);
            if (!resCob.recordset.length) throw new Error('Cobro no existe.');

            const cob = resCob.recordset[0];
            if (cob.anulado) {
                throw new Error(`El cobro ${cob_num} ya está anulado.`);
            }

            // Fetch detail lines of paid documents
            const resReng = await pool.request()
                .input('cob_num', sql.Char(20), padProfit(cob_num, 20))
                .query(`
                    SELECT RTRIM(co_tipo_doc) AS co_tipo_doc, RTRIM(nro_doc) AS nro_doc, mont_cob, monto_retencion_iva, monto_retencion
                    FROM saCobroDocReng
                    WHERE LTRIM(RTRIM(cob_num)) = LTRIM(RTRIM(@cob_num))
                `);

            const transaction = new sql.Transaction(pool);
            await transaction.begin();

            try {
                const auditUser = (req.profitUser || 'API').substring(0, 10).toUpperCase();
                const sucuCode = cob.co_sucu_in || '01';

                // 1. Anular cabecera de Cobro
                await transaction.request()
                    .input('cob_num', sql.Char(20), padProfit(cob_num, 20))
                    .input('auditUser', sql.Char(6), padProfit(auditUser, 6))
                    .query(`
                        UPDATE saCobro
                        SET anulado = 1,
                            fe_us_mo = GETDATE(),
                            co_us_mo = @auditUser
                        WHERE LTRIM(RTRIM(cob_num)) = LTRIM(RTRIM(@cob_num))
                    `);

                // 2. Anular documentos de retención o diferenciales creados por este cobro (IVAN, ISLR, N/CR, N/DB)
                await transaction.request()
                    .input('cob_num', sql.Char(20), padProfit(cob_num, 20))
                    .input('auditUser', sql.Char(6), padProfit(auditUser, 6))
                    .query(`
                        UPDATE saDocumentoVenta
                        SET anulado = 1,
                            fe_us_mo = GETDATE(),
                            co_us_mo = @auditUser
                        WHERE DOC_ORIG = 'COBRO' AND LTRIM(RTRIM(NRO_ORIG)) = LTRIM(RTRIM(@cob_num))
                    `);

                // 3. Revertir saldo de los documentos cobrados
                for (const line of resReng.recordset) {
                    const totalRebaje = Number(line.mont_cob || 0);
                    if (totalRebaje > 0) {
                        await transaction.request()
                            .input('co_tipo_doc', sql.Char(6), padProfit(line.co_tipo_doc, 6))
                            .input('nro_doc', sql.Char(20), padProfit(line.nro_doc, 20))
                            .input('rebaje', sql.Decimal(18, 2), totalRebaje)
                            .input('auditUser', sql.Char(6), padProfit(auditUser, 6))
                            .query(`
                                UPDATE saDocumentoVenta
                                SET saldo = saldo + @rebaje,
                                    fe_us_mo = GETDATE(),
                                    co_us_mo = @auditUser
                                WHERE LTRIM(RTRIM(co_tipo_doc)) = LTRIM(RTRIM(@co_tipo_doc))
                                  AND LTRIM(RTRIM(nro_doc)) = LTRIM(RTRIM(@nro_doc))
                            `);

                        // Si es factura (FACT), revertir saldo también en saFacturaVenta
                        if (line.co_tipo_doc.trim().toUpperCase() === 'FACT') {
                            await transaction.request()
                                .input('nro_doc', sql.Char(20), padProfit(line.nro_doc, 20))
                                .input('rebaje', sql.Decimal(18, 2), totalRebaje)
                                .input('auditUser', sql.Char(6), padProfit(auditUser, 6))
                                .query(`
                                    UPDATE saFacturaVenta
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
                    .input('sNro_Cobro', sql.Char(20), padProfit(cob_num, 20))
                    .input('sCo_Us_Mo', sql.Char(6), padProfit(auditUser, 6))
                    .input('sCo_Sucu_Mo', sql.Char(6), padProfit(sucuCode, 6))
                    .input('sRevisado', sql.Char(1), null)
                    .input('sTrasnfe', sql.Char(1), null)
                    .execute('pv_ActualizarMovCajaAsocCobroAnular');

                // 5. Anular movimientos de banco asociados
                await transaction.request()
                    .input('cob_num', sql.Char(20), padProfit(cob_num, 20))
                    .input('auditUser', sql.Char(6), padProfit(auditUser, 6))
                    .query(`
                        UPDATE saMovimientoBanco
                        SET anulado = 1,
                            fe_us_mo = GETDATE(),
                            co_us_mo = @auditUser
                        WHERE LTRIM(RTRIM(cob_pag)) = LTRIM(RTRIM(@cob_num)) AND origen = 'COB'
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
        res.status(500).json({ success: false, message: 'Error al anular cobro.', error: error.message });
    }
});

// --- GUARDAR COBRO ---
router.post('/', async (req, res) => {
    const data = req.body;
    console.log("📥 [AGENT] Recibiendo Cobro (SAVE):", JSON.stringify({ ...data, renglones: data.renglones?.length, formas_pago: data.formas_pago?.length }, null, 2));

    if (!data.co_cli || !data.renglones || !data.formas_pago) {
        return res.status(400).json({ success: false, message: 'Campos obligatorios: co_cli, renglones, formas_pago' });
    }

    const outcome = await executeWrite(req.query.sede || null, req.sqlAuth, async (pool, srv) => {
        // Cargar Catálogos para valores predeterminados
        const [resVen, resSucu, resCtaIE] = await Promise.all([
            pool.request().query(`SELECT TOP 1 RTRIM(co_ven) AS co_ven FROM saVendedor`),
            pool.request().query(`SELECT TOP 1 RTRIM(co_sucur) AS co_sucur FROM saSucursal`),
            pool.request().query(`SELECT TOP 1 RTRIM(co_cta_ingr_egr) AS co_cta_ingr_egr FROM saCuentaIngEgr`)
        ]);

        const defVen = resVen.recordset[0]?.co_ven || '01';
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
            // 1. Obtener correlativo de Cobro
            const resCorr = await transaction.request().query(`
                UPDATE saSerie
                SET prox_n = prox_n + 1, fe_us_mo = GETDATE()
                OUTPUT INSERTED.prox_n, RTRIM(INSERTED.desde_a) as prefijo
                WHERE co_serie = (
                    SELECT TOP 1 co_serie
                    FROM saConsecutivo
                    WHERE UPPER(LTRIM(RTRIM(co_consecutivo))) = 'COBRO'
                      AND co_serie IS NOT NULL
                )
            `);
            let corrRow = resCorr.recordset[0];
            if (!corrRow || !corrRow.prox_n) {
                throw new Error("No se pudo obtener el correlativo de cobro.");
            }
            const proxN = Number(corrRow.prox_n || 0);
            const cobNum = proxN.toString().padStart(10, '0');
            console.log(`✨ [AGENT] Nuevo número de cobro generado: ${cobNum}`);

            // 2. Insertar Cabecera de Cobro
            const rH = new sql.Request(transaction);
            rH.input('sCob_Num', sql.Char(20), padProfit(cobNum, 20));
            rH.input('sRecibo', sql.Char(15), null);
            rH.input('sCo_cli', sql.Char(16), padProfit(data.co_cli, 16));
            rH.input('sCo_ven', sql.Char(6), padProfit(data.co_ven || defVen, 6));
            let collectionMone = data.co_mone || 'USD';
            if (collectionMone.trim().toUpperCase() === 'US$') {
                collectionMone = 'USD';
            }
            rH.input('sCo_Mone', sql.Char(6), padProfit(collectionMone, 6));
            rH.input('deTasa', sql.Decimal(21, 8), Number(data.tasa || 1));
            rH.input('sdFecha', sql.SmallDateTime, tsDate);
            rH.input('bAnulado', sql.Bit, 0);

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
                finalMontoHeader = collectionMone === 'USD' 
                    ? Math.round((totalFormasPagoBs / Number(data.tasa || 1)) * 100) / 100 
                    : totalFormasPagoBs;
            } else {
                finalMontoHeader = Math.max(0, collectionMone === 'USD' 
                    ? Math.round((totalAbonoBs / Number(data.tasa || 1)) * 100) / 100 
                    : totalAbonoBs);
            }
            rH.input('deMonto', sql.Decimal(18, 2), finalMontoHeader);
            rH.input('sDis_cen', sql.VarChar(sql.MAX), null);
            rH.input('sDescrip', sql.VarChar(60), (data.descrip || 'COBRO DE CLIENTE').substring(0, 60));
            rH.input('sCo_Us_In', sql.Char(6), padProfit(auditUser, 6));
            rH.input('sCo_Sucu_In', sql.Char(6), padProfit(sucuCode, 6));
            rH.input('sRevisado', sql.Char(1), null);
            rH.input('sTrasnfe', sql.Char(1), null);

            await rH.execute('pInsertarCobro');

            // Mapas para relacionar los renglones (Padre-Hijo)
            const rengDocGuidMap = new Map(); // nro_doc -> rowguid de saCobroDocReng
            const insertedRenglones = []; // Arreglo para ordenar y guardar guids finales

            // 3. Insertar Renglones de Documentos (saCobroDocReng)
            // Primero insertamos los registros de débito (FACT, NDEB, etc.) para generar los rowguid Padres
            const parentTypes = ['FACT', 'NDEB', 'N/DB', 'GIRO', 'AJPA'];
            const parentLines = data.renglones.filter(r => parentTypes.includes(r.co_tipo_doc.trim().toUpperCase()));
            const childLines = data.renglones.filter(r => !parentTypes.includes(r.co_tipo_doc.trim().toUpperCase()));
            const sortedRenglones = [...parentLines, ...childLines];
            let nextRengNum = 1;
            for (let i = 0; i < sortedRenglones.length; i++) {
                const line = sortedRenglones[i];
                const rengNum = nextRengNum++;
                const docGuid = sql.UniqueIdentifier;

                // Determinar rowguid_reng_ori para notas de crédito/retenciones asociadas
                let parentGuid = null;
                if (!parentTypes.includes(line.co_tipo_doc.trim().toUpperCase())) {
                    const lookupKey = line.parent_doc ? line.parent_doc.trim() : line.nro_doc?.trim();
                    parentGuid = rengDocGuidMap.get(lookupKey);
                }

                // 3.0. Consultar saDocumentoVenta para obtener tasa, saldo y moneda original de la factura
                let docSaldo = 0;
                let docTasa = 1;
                let docMone = 'BS';
                let docCoCli = data.co_cli;
                let docCoVen = data.co_ven || defVen;

                let docFecEmisStr = '';
                const docTypeUpper = line.co_tipo_doc.trim().toUpperCase();
                const queryTypes = ['FACT', 'NDEB', 'N/DB', 'GIRO', 'AJPA', 'N/CR'];
                if (queryTypes.includes(docTypeUpper)) {
                    const docInfo = await transaction.request()
                        .input('co_tipo_doc', sql.Char(6), padProfit(line.co_tipo_doc, 6))
                        .input('nro_doc', sql.Char(20), padProfit(line.nro_doc, 20))
                        .query(`
                            SELECT RTRIM(co_mone) AS co_mone, tasa, saldo, co_cli, co_ven,
                                   CONVERT(VARCHAR(10), fec_emis, 120) AS fec_emis_str
                            FROM saDocumentoVenta
                            WHERE LTRIM(RTRIM(co_tipo_doc)) = LTRIM(RTRIM(@co_tipo_doc))
                              AND LTRIM(RTRIM(nro_doc)) = LTRIM(RTRIM(@nro_doc))
                        `);
                    if (docInfo.recordset.length > 0) {
                        docSaldo = Number(docInfo.recordset[0].saldo || 0);
                        docTasa = Number(docInfo.recordset[0].tasa || 1);
                        docMone = docInfo.recordset[0].co_mone || 'BS';
                        docCoCli = docInfo.recordset[0].co_cli || docCoCli;
                        docCoVen = docInfo.recordset[0].co_ven || docCoVen;
                        docFecEmisStr = docInfo.recordset[0].fec_emis_str || '';
                    }
                }

                let finalMontCob = Math.abs(Number(line.mont_cob));

                let adjustedMontoRetencionIva = Number(line.monto_retencion_iva || 0);
                let adjustedMontoRetencion = Number(line.monto_retencion || 0);
                let diffBs = 0;

                let totalRebaje = finalMontCob + adjustedMontoRetencionIva + adjustedMontoRetencion;
                const todayStr = new Date().toISOString().split('T')[0];
                const isPreviousDateDoc = docFecEmisStr && docFecEmisStr < todayStr;
                const rateCobro = Number(data.tasa || 1);

                if (isPreviousDateDoc && docSaldo > 0 && docTasa > 0 && rateCobro > docTasa) {
                    // Llevar saldo en Bs a USD según la tasa del documento
                    const saldoUsd = docSaldo / docTasa;
                    // Calcular equivalente en Bs a la tasa actual
                    const montoBsActual = Math.round((saldoUsd * rateCobro) * 100) / 100;
                    // El diferencial en Bs que se registrará en N/DB
                    diffBs = Math.max(0, Math.round((montoBsActual - docSaldo) * 100) / 100);

                    // La factura original se amortiza exactamente por su saldo en Bs original (dejándola en Bs 0,00)
                    finalMontCob = Math.max(0, docSaldo - adjustedMontoRetencionIva - adjustedMontoRetencion);
                    totalRebaje = finalMontCob + adjustedMontoRetencionIva + adjustedMontoRetencion;
                } else {
                    // Control de Saldo Máximo (Capping): El rebaje total no puede exceder el saldo actual del documento
                    if (totalRebaje > docSaldo) {
                        const excess = totalRebaje - docSaldo;
                        finalMontCob = Math.max(0, finalMontCob - excess);
                        diffBs = diffBs + excess;
                        totalRebaje = finalMontCob + adjustedMontoRetencionIva + adjustedMontoRetencion;
                    }
                }

                try {
                    require('fs').appendFileSync('scratch_cobro_log.txt', `\n[${new Date().toISOString()}] nro_doc=${line.nro_doc}, docMone=${docMone}, docTasa=${docTasa}, docSaldo=${docSaldo}, origMontCob=${line.mont_cob}, finalMontCob=${finalMontCob}, diffBs=${diffBs}, data.tasa=${data.tasa}`);
                } catch (e) { }

                const rR = new sql.Request(transaction);

                const guidResult = await transaction.request().query('SELECT NEWID() AS guid');
                const lineGuid = guidResult.recordset[0].guid;
                rengDocGuidMap.set(line.nro_doc?.trim(), lineGuid);

                await transaction.request()
                    .input('reng_num', sql.Int, rengNum)
                    .input('cob_num', sql.Char(20), padProfit(cobNum, 20))
                    .input('co_tipo_doc', sql.Char(6), padProfit(line.co_tipo_doc, 6))
                    .input('nro_doc', sql.Char(20), padProfit(line.nro_doc, 20))
                    .input('mont_cob', sql.Decimal(18, 2), totalRebaje)
                    .input('monto_retencion_iva', sql.Decimal(18, 5), adjustedMontoRetencionIva)
                    .input('monto_retencion', sql.Decimal(18, 2), adjustedMontoRetencion)
                    .input('rowguid_reng_ori', sql.UniqueIdentifier, parentGuid)
                    .input('co_sucu_in', sql.Char(6), padProfit(sucuCode, 6))
                    .input('co_us_in', sql.Char(6), padProfit(auditUser, 6))
                    .input('rowguid', sql.UniqueIdentifier, lineGuid)
                    .query(`
                        INSERT INTO saCobroDocReng (
                            reng_num, cob_num, co_tipo_doc, nro_doc, mont_cob,
                            dpcobro_porc_desc, dpcobro_monto, monto_retencion_iva, monto_retencion,
                            reten_tercero_rowguid_ori, tipo_doc, num_doc, rowguid_reng_ori,
                            tipo_origen, gen_origen, co_sucu_in, co_us_in, fe_us_in, co_sucu_mo, co_us_mo, fe_us_mo,
                            trasnfe, revisado, rowguid
                        ) VALUES (
                            @reng_num, @cob_num, @co_tipo_doc, @nro_doc, @mont_cob,
                            0.00, 0.00, @monto_retencion_iva, @monto_retencion,
                            NULL, NULL, NULL, @rowguid_reng_ori,
                            NULL, NULL, @co_sucu_in, @co_us_in, GETDATE(), @co_sucu_in, @co_us_in, GETDATE(),
                            NULL, NULL, @rowguid
                        )
                    `);

                // 3.1 Rebajar el saldo del documento en saDocumentoVenta usando el monto original ajustado
                totalRebaje = finalMontCob + adjustedMontoRetencionIva + adjustedMontoRetencion;
                await transaction.request()
                    .input('co_tipo_doc', sql.Char(6), padProfit(line.co_tipo_doc, 6))
                    .input('nro_doc', sql.Char(20), padProfit(line.nro_doc, 20))
                    .input('rebaje', sql.Decimal(18, 2), totalRebaje)
                    .input('user', sql.Char(6), padProfit(auditUser, 6))
                    .query(`
                        UPDATE saDocumentoVenta
                        SET saldo = saldo - @rebaje,
                            fe_us_mo = GETDATE(),
                            co_us_mo = @user
                        WHERE LTRIM(RTRIM(co_tipo_doc)) = LTRIM(RTRIM(@co_tipo_doc))
                          AND LTRIM(RTRIM(nro_doc)) = LTRIM(RTRIM(@nro_doc))
                    `);

                // Si el tipo de documento es FACT (Factura), también debemos rebajar el saldo en saFacturaVenta
                if (line.co_tipo_doc.trim().toUpperCase() === 'FACT') {
                    await transaction.request()
                        .input('nro_doc', sql.Char(20), padProfit(line.nro_doc, 20))
                        .input('rebaje', sql.Decimal(18, 2), totalRebaje)
                        .input('user', sql.Char(6), padProfit(auditUser, 6))
                        .query(`
                            UPDATE saFacturaVenta
                            SET saldo = saldo - @rebaje,
                                fe_us_mo = GETDATE(),
                                co_us_mo = @user
                            WHERE LTRIM(RTRIM(doc_num)) = LTRIM(RTRIM(@nro_doc))
                        `);
                }


                // 3.2 Generar documentos de retención de IVA e ISLR en saDocumentoVenta para evitar inconsistencias y permitir la anulación
                if (adjustedMontoRetencionIva > 0) {
                    let ivanNum = null;
                    try {
                        const ivanRes = await transaction.request()
                            .input('sCo_Sucur', sql.Char(6), padProfit(sucuCode, 6))
                            .input('sCo_Consecutivo', sql.Char(16), padProfit('DOC_VEN_IVAN', 16))
                            .execute('pConsecutivoProximo');
                        ivanNum = ivanRes.recordset[0]?.ProximoConsecutivo?.trim();
                    } catch (err) {
                        const ivanResGlobal = await transaction.request()
                            .input('sCo_Sucur', sql.Char(6), '')
                            .input('sCo_Consecutivo', sql.Char(16), padProfit('DOC_VEN_IVAN', 16))
                            .execute('pConsecutivoProximo');
                        ivanNum = ivanResGlobal.recordset[0]?.ProximoConsecutivo?.trim();
                    }

                    if (!ivanNum) {
                        throw new Error('No se pudo obtener el próximo consecutivo para el documento IVAN.');
                    }

                    const retIvaMatch = data.retenciones_iva?.find(r => r.nro_doc_asoc?.trim() === line.nro_doc?.trim());
                    const numComprobante = retIvaMatch?.num_comprobante || '';

                    await transaction.request()
                        .input('co_tipo_doc', sql.Char(6), padProfit('IVAN', 6))
                        .input('nro_doc', sql.Char(20), padProfit(ivanNum, 20))
                        .input('co_cli', sql.Char(16), padProfit(data.co_cli, 16))
                        .input('co_ven', sql.Char(6), padProfit(data.co_ven || defVen, 6))
                        .input('co_mone', sql.Char(6), padProfit(docMone, 6))
                        .input('tasa', sql.Decimal(21, 8), docTasa)
                        .input('observa', sql.VarChar(120), `COBRO N° ${cobNum}`)
                        .input('doc_orig', sql.Char(6), padProfit('COBRO', 6))
                        .input('tipo_origen', sql.Int, 0)
                        .input('nro_orig', sql.VarChar(20), cobNum)
                        .input('total_bruto', sql.Decimal(18, 2), adjustedMontoRetencionIva)
                        .input('total_neto', sql.Decimal(18, 2), adjustedMontoRetencionIva)
                        .input('saldo', sql.Decimal(18, 2), 0.00)
                        .input('co_us_in', sql.Char(6), padProfit(auditUser, 6))
                        .input('co_sucu_in', sql.Char(6), padProfit(sucuCode, 6))
                        .input('num_comprobante', sql.Char(14), numComprobante.substring(0, 14))
                        .query(`
                            INSERT INTO saDocumentoVenta (
                                co_tipo_doc, nro_doc, co_cli, co_ven, co_mone, tasa, observa,
                                fec_reg, fec_emis, fec_venc, anulado, aut, contrib,
                                doc_orig, tipo_origen, nro_orig, saldo, total_bruto,
                                total_neto, monto_imp, monto_imp2, monto_imp3, porc_imp, porc_imp2, porc_imp3,
                                comis1, comis2, comis3, comis4, comis5, comis6, adicional, ven_ter,
                                otros1, otros2, otros3, n_control, co_us_in, co_sucu_in, fe_us_in,
                                co_us_mo, co_sucu_mo, fe_us_mo, rowguid,
                                monto_desc_glob, monto_reca, num_comprobante, tipo_imp
                            ) VALUES (
                                @co_tipo_doc, @nro_doc, @co_cli, @co_ven, @co_mone, @tasa, @observa,
                                CONVERT(VARCHAR(10), GETDATE(), 120), CONVERT(VARCHAR(10), GETDATE(), 120), CONVERT(VARCHAR(10), GETDATE(), 120), 0, 1, 0,
                                @doc_orig, @tipo_origen, @nro_orig, @saldo, @total_bruto,
                                @total_neto, 0, 0, 0, 0, 0, 0,
                                0, 0, 0, 0, 0, 0, 0, 0,
                                0, 0, 0, '', @co_us_in, @co_sucu_in, GETDATE(),
                                @co_us_in, @co_sucu_in, GETDATE(), NEWID(),
                                0, 0, @num_comprobante, 7
                            )
                        `);

                    // Insertar renglón de IVAN en saCobroDocReng para registrar la retención
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
                            INSERT INTO saCobroDocReng (
                                reng_num, cob_num, co_tipo_doc, nro_doc, mont_cob,
                                dpcobro_porc_desc, dpcobro_monto, monto_retencion_iva, monto_retencion,
                                rowguid_reng_ori, co_sucu_in, co_us_in, fe_us_in, co_sucu_mo, co_us_mo, fe_us_mo, rowguid
                            ) VALUES (
                                @reng_num, @cob_num, @co_tipo_doc, @nro_doc, @mont_cob,
                                0.00, 0.00, 0.00, 0.00,
                                @rowguid_reng_ori, @co_sucu_in, @co_us_in, GETDATE(), @co_sucu_in, @co_us_in, GETDATE(), @rowguid
                            )
                        `);
                }

                if (adjustedMontoRetencion > 0) {
                    let islrNum = null;
                    try {
                        const islrRes = await transaction.request()
                            .input('sCo_Sucur', sql.Char(6), padProfit(sucuCode, 6))
                            .input('sCo_Consecutivo', sql.Char(16), padProfit('DOC_VEN_ISLR', 16))
                            .execute('pConsecutivoProximo');
                        islrNum = islrRes.recordset[0]?.ProximoConsecutivo?.trim();
                    } catch (err) {
                        const islrResGlobal = await transaction.request()
                            .input('sCo_Sucur', sql.Char(6), '')
                            .input('sCo_Consecutivo', sql.Char(16), padProfit('DOC_VEN_ISLR', 16))
                            .execute('pConsecutivoProximo');
                        islrNum = islrResGlobal.recordset[0]?.ProximoConsecutivo?.trim();
                    }

                    if (!islrNum) {
                        throw new Error('No se pudo obtener el próximo consecutivo para el documento ISLR.');
                    }

                    await transaction.request()
                        .input('co_tipo_doc', sql.Char(6), padProfit('ISLR', 6))
                        .input('nro_doc', sql.Char(20), padProfit(islrNum, 20))
                        .input('co_cli', sql.Char(16), padProfit(data.co_cli, 16))
                        .input('co_ven', sql.Char(6), padProfit(data.co_ven || defVen, 6))
                        .input('co_mone', sql.Char(6), padProfit(docMone, 6))
                        .input('tasa', sql.Decimal(21, 8), docTasa)
                        .input('observa', sql.VarChar(120), `COBRO N° ${cobNum}`)
                        .input('doc_orig', sql.Char(6), padProfit('COBRO', 6))
                        .input('tipo_origen', sql.Int, 0)
                        .input('nro_orig', sql.VarChar(20), cobNum)
                        .input('total_bruto', sql.Decimal(18, 2), adjustedMontoRetencion)
                        .input('total_neto', sql.Decimal(18, 2), adjustedMontoRetencion)
                        .input('saldo', sql.Decimal(18, 2), 0.00)
                        .input('co_us_in', sql.Char(6), padProfit(auditUser, 6))
                        .input('co_sucu_in', sql.Char(6), padProfit(sucuCode, 6))
                        .query(`
                            INSERT INTO saDocumentoVenta (
                                co_tipo_doc, nro_doc, co_cli, co_ven, co_mone, tasa, observa,
                                fec_reg, fec_emis, fec_venc, anulado, aut, contrib,
                                doc_orig, tipo_origen, nro_orig, saldo, total_bruto,
                                total_neto, monto_imp, monto_imp2, monto_imp3, porc_imp, porc_imp2, porc_imp3,
                                comis1, comis2, comis3, comis4, comis5, comis6, adicional, ven_ter,
                                otros1, otros2, otros3, n_control, co_us_in, co_sucu_in, fe_us_in,
                                co_us_mo, co_sucu_mo, fe_us_mo, rowguid,
                                monto_desc_glob, monto_reca
                            ) VALUES (
                                @co_tipo_doc, @nro_doc, @co_cli, @co_ven, @co_mone, @tasa, @observa,
                                CONVERT(VARCHAR(10), GETDATE(), 120), CONVERT(VARCHAR(10), GETDATE(), 120), CONVERT(VARCHAR(10), GETDATE(), 120), 0, 1, 0,
                                @doc_orig, @tipo_origen, @nro_orig, @saldo, @total_bruto,
                                @total_neto, 0, 0, 0, 0, 0, 0,
                                0, 0, 0, 0, 0, 0, 0, 0,
                                0, 0, 0, '', @co_us_in, @co_sucu_in, GETDATE(),
                                @co_us_in, @co_sucu_in, GETDATE(), NEWID(),
                                0, 0
                            )
                        `);

                    // Insertar renglón de ISLR en saCobroDocReng para registrar la retención
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
                            INSERT INTO saCobroDocReng (
                                reng_num, cob_num, co_tipo_doc, nro_doc, mont_cob,
                                dpcobro_porc_desc, dpcobro_monto, monto_retencion_iva, monto_retencion,
                                rowguid_reng_ori, co_sucu_in, co_us_in, fe_us_in, co_sucu_mo, co_us_mo, fe_us_mo, rowguid
                            ) VALUES (
                                @reng_num, @cob_num, @co_tipo_doc, @nro_doc, @mont_cob,
                                0.00, 0.00, 0.00, 0.00,
                                @rowguid_reng_ori, @co_sucu_in, @co_us_in, GETDATE(), @co_sucu_in, @co_us_in, GETDATE(), @rowguid
                            )
                        `);
                }

                insertedRenglones.push({
                    co_tipo_doc: line.co_tipo_doc,
                    nro_doc: line.nro_doc,
                    rowguid: lineGuid
                });

                // 3.2 Generar Nota de Débito (N/DB) o Crédito (N/CR) por diferencial cambiario
                if (Math.abs(diffBs) > 0.01) {
                    const isDebit = diffBs > 0;
                    const diffDocType = isDebit ? 'N/DB' : 'N/CR';
                    const consecName = isDebit ? 'DOC_VEN_N/DB' : 'DOC_VEN_N/CR';

                    // Obtener correlativo
                    const resCorrDiff = await transaction.request().query(`
                        UPDATE saSerie
                        SET prox_n = prox_n + 1, fe_us_mo = GETDATE()
                        OUTPUT INSERTED.prox_n, RTRIM(INSERTED.desde_a) as prefijo
                        WHERE co_serie = (
                            SELECT TOP 1 co_serie
                            FROM saConsecutivo
                            WHERE UPPER(LTRIM(RTRIM(co_consecutivo))) = '${consecName}'
                              AND co_serie IS NOT NULL
                        )
                    `);
                    let corrDiff = resCorrDiff.recordset[0];
                    if (!corrDiff || !corrDiff.prox_n) {
                        throw new Error(`No se pudo obtener el correlativo para la Nota de ${isDebit ? 'Débito' : 'Crédito'} de diferencial cambiario.`);
                    }
                    const proxDiff = Number(corrDiff.prox_n || 0);
                    const diffDocNum = proxDiff.toString().padStart(10, '0');

                    // Insertar N/DB o N/CR en saDocumentoVenta
                    await transaction.request()
                        .input('co_tipo_doc', sql.Char(6), padProfit(diffDocType, 6))
                        .input('nro_doc', sql.Char(20), padProfit(diffDocNum, 20))
                        .input('co_cli', sql.Char(16), padProfit(docCoCli, 16))
                        .input('co_ven', sql.Char(6), padProfit(docCoVen, 6))
                        .input('co_mone', sql.Char(6), padProfit('BS', 6))
                        .input('tasa', sql.Decimal(21, 8), 1.00)
                        .input('observa', sql.VarChar(120), (`Diferencial Cambiario COBRO N° ${cobNum} ${line.nro_doc.trim()}`).substring(0, 120))
                        .input('doc_orig', sql.Char(6), padProfit('COBRO', 6))
                        .input('tipo_origen', sql.Int, 2)
                        .input('nro_orig', sql.VarChar(20), cobNum)
                        .input('total_bruto', sql.Decimal(18, 2), Math.abs(diffBs))
                        .input('total_neto', sql.Decimal(18, 2), Math.abs(diffBs))
                        .input('saldo', sql.Decimal(18, 2), 0.00)
                        .input('co_us_in', sql.Char(6), padProfit(auditUser, 6))
                        .input('co_sucu_in', sql.Char(6), padProfit(sucuCode, 6))
                        .query(`
                            INSERT INTO saDocumentoVenta (
                                co_tipo_doc, nro_doc, co_cli, co_ven, co_mone, tasa, observa,
                                fec_reg, fec_emis, fec_venc, anulado, aut, contrib,
                                doc_orig, tipo_origen, nro_orig, saldo, total_bruto,
                                total_neto, monto_imp, monto_imp2, monto_imp3, porc_imp, porc_imp2, porc_imp3,
                                comis1, comis2, comis3, comis4, comis5, comis6, adicional, ven_ter,
                                otros1, otros2, otros3, n_control, co_us_in, co_sucu_in, fe_us_in,
                                co_us_mo, co_sucu_mo, fe_us_mo, rowguid,
                                monto_desc_glob, monto_reca
                            ) VALUES (
                                @co_tipo_doc, @nro_doc, @co_cli, @co_ven, @co_mone, @tasa, @observa,
                                CONVERT(VARCHAR(10), GETDATE(), 120), CONVERT(VARCHAR(10), GETDATE(), 120), CONVERT(VARCHAR(10), GETDATE(), 120), 0, 1, 0,
                                @doc_orig, @tipo_origen, @nro_orig, @saldo, @total_bruto,
                                @total_neto, 0, 0, 0, 0, 0, 0,
                                0, 0, 0, 0, 0, 0, 0, 0,
                                0, 0, 0, @nro_doc, @co_us_in, @co_sucu_in, GETDATE(),
                                @co_us_in, @co_sucu_in, GETDATE(), NEWID(),
                                0.00, 0.00
                            )
                        `);

                    // Insertar renglón en saCobroDocReng
                    const diffRengNum = nextRengNum++;
                    const diffRengGuidResult = await transaction.request().query('SELECT NEWID() AS guid');
                    const diffRengGuid = diffRengGuidResult.recordset[0].guid;

                    await transaction.request()
                        .input('reng_num', sql.Int, diffRengNum)
                        .input('cob_num', sql.Char(20), padProfit(cobNum, 20))
                        .input('co_tipo_doc', sql.Char(6), padProfit(diffDocType, 6))
                        .input('nro_doc', sql.Char(20), padProfit(diffDocNum, 20))
                        .input('mont_cob', sql.Decimal(18, 2), Math.abs(diffBs))
                        .input('co_sucu_in', sql.Char(6), padProfit(sucuCode, 6))
                        .input('co_us_in', sql.Char(6), padProfit(auditUser, 6))
                        .input('rowguid', sql.UniqueIdentifier, diffRengGuid)
                        .query(`
                            INSERT INTO saCobroDocReng (
                                reng_num, cob_num, co_tipo_doc, nro_doc, mont_cob,
                                dpcobro_porc_desc, dpcobro_monto, monto_retencion_iva, monto_retencion,
                                co_sucu_in, co_us_in, fe_us_in, co_sucu_mo, co_us_mo, fe_us_mo, rowguid
                            ) VALUES (
                                @reng_num, @cob_num, @co_tipo_doc, @nro_doc, @mont_cob,
                                0.00, 0.00, 0.00, 0.00,
                                @co_sucu_in, @co_us_in, GETDATE(), @co_sucu_in, @co_us_in, GETDATE(), @rowguid
                            )
                        `);
                }
            }

            // Usar la caja '02' (Bolívares) por defecto para el renglón dummy de cobro neto a 0,00
            const dummyCaja = '02';

            const activeFormasPago = data.formas_pago && data.formas_pago.length > 0
                ? data.formas_pago
                : [{
                    forma_pag: 'EF',
                    cod_caja: dummyCaja,
                    cod_cta: null,
                    co_ban: null,
                    co_tar: null,
                    num_doc: null,
                    mont_doc: 0,
                    fecha_che: null
                  }];

            // 4. Insertar Formas de Pago (saCobroTPReng) y crear movimientos en Caja/Banco
            for (let i = 0; i < activeFormasPago.length; i++) {
                const tp = activeFormasPago[i];
                const rengNum = i + 1;
                let movNumC = null;
                let movNumB = null;

                if (tp.forma_pag === 'EF' || tp.forma_pag === 'TJ' || tp.forma_pag === 'CT') {
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
                        // Generar correlativo de movimiento de Caja (MOVC_NUM)
                        const resCorrCaja = await transaction.request().query(`
                            UPDATE saSerie
                            SET prox_n = prox_n + 1, fe_us_mo = GETDATE()
                            OUTPUT INSERTED.prox_n
                            WHERE co_serie = (
                                SELECT TOP 1 co_serie
                                FROM saConsecutivo
                                WHERE UPPER(LTRIM(RTRIM(co_consecutivo))) = 'MOVC_NUM'
                                  AND co_serie IS NOT NULL
                            )
                        `);
                        let corrCaja = resCorrCaja.recordset[0];
                        if (!corrCaja || !corrCaja.prox_n) {
                            throw new Error("No se pudo obtener el correlativo de movimiento de caja.");
                        }
                        movNumC = Number(corrCaja.prox_n).toString().padStart(10, '0');

                        // Crear Movimiento de Caja
                        const rMovC = new sql.Request(transaction);
                        rMovC.input('sMov_Num', sql.Char(20), padProfit(movNumC, 20));
                        rMovC.input('sdFecha', sql.SmallDateTime, tsDate);
                        rMovC.input('sDescrip', sql.VarChar(60), (`INGR. COBRO ${cobNum} - ${data.co_cli}`).substring(0, 60));
                        rMovC.input('sCod_Caja', sql.Char(6), padProfit(tp.cod_caja, 6));
                        rMovC.input('deTasa', sql.Decimal(21, 8), isUSDcaja ? rate : 1);
                        rMovC.input('sTipo_Mov', sql.Char(2), 'I');
                        rMovC.input('sForma_Pag', sql.Char(2), tp.forma_pag);
                        rMovC.input('sNum_Pago', sql.VarChar(20), tp.num_doc ? tp.num_doc.substring(0, 20) : null);
                        rMovC.input('sCo_Ban', sql.Char(6), tp.co_ban ? padProfit(tp.co_ban, 6) : null);
                        rMovC.input('sCo_Tar', sql.Char(6), tp.co_tar ? padProfit(tp.co_tar, 6) : null);
                        rMovC.input('sCo_Cta_Ingr_Egr', sql.Char(20), padProfit(defCtaIE, 20));
                        rMovC.input('deMonto', sql.Decimal(18, 2), finalMontoCaja);
                        rMovC.input('bSaldo_Ini', sql.Bit, 0);
                        rMovC.input('sOrigen', sql.Char(3), 'COB');
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
                        // Generar correlativo de movimiento de Banco (MOVB_NUM)
                        const resCorrBanco = await transaction.request().query(`
                            UPDATE saSerie
                            SET prox_n = prox_n + 1, fe_us_mo = GETDATE()
                            OUTPUT INSERTED.prox_n
                            WHERE co_serie = (
                                SELECT TOP 1 co_serie
                                FROM saConsecutivo
                                WHERE UPPER(LTRIM(RTRIM(co_consecutivo))) = 'MOVB_NUM'
                                  AND co_serie IS NOT NULL
                            )
                        `);
                        let corrBanco = resCorrBanco.recordset[0];
                        if (!corrBanco || !corrBanco.prox_n) {
                            throw new Error("No se pudo obtener el correlativo de movimiento de banco.");
                        }
                        movNumB = Number(corrBanco.prox_n).toString().padStart(10, '0');

                        // Tipo de operación para el banco
                        let tipoOp = 'TR'; // Transferencia
                        if (tp.forma_pag === 'DP') tipoOp = 'DP'; // Depósito
                        if (tp.forma_pag === 'CH') tipoOp = 'CH'; // Cheque

                        // Crear Movimiento de Banco
                        const rMovB = new sql.Request(transaction);
                        rMovB.input('sMov_Num', sql.Char(20), padProfit(movNumB, 20));
                        rMovB.input('sDescrip', sql.VarChar(160), (`INGR. COBRO ${cobNum} - ${data.co_cli}`).substring(0, 160));
                        rMovB.input('sCod_Cta', sql.Char(6), padProfit(tp.cod_cta, 6));
                        rMovB.input('sdFecha', sql.SmallDateTime, tsDate);
                        rMovB.input('deTasa', sql.Decimal(21, 8), isUSDcuenta ? rate : 1);
                        rMovB.input('sTipo_Op', sql.Char(2), tipoOp);
                        rMovB.input('sDoc_Num', sql.VarChar(20), (tp.num_doc || '').substring(0, 20));
                        rMovB.input('deMonto', sql.Decimal(18, 2), finalMontoBanco);
                        rMovB.input('sCo_Cta_Ingr_Egr', sql.Char(20), padProfit(defCtaIE, 20));
                        rMovB.input('sOrigen', sql.Char(3), 'COB');
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

                // Insertar renglón de forma de pago del cobro
                await transaction.request()
                    .input('reng_num', sql.Int, rengNum)
                    .input('cob_num', sql.Char(20), padProfit(cobNum, 20))
                    .input('co_tar', sql.Char(6), tp.co_tar ? padProfit(tp.co_tar, 6) : null)
                    .input('co_ban', sql.Char(6), tp.co_ban ? padProfit(tp.co_ban, 6) : null)
                    .input('forma_pag', sql.Char(2), tp.forma_pag === 'TE' ? 'TP' : tp.forma_pag) // Profit nativamente usa 'TP' para Transferencia
                    .input('cod_cta', sql.Char(6), tp.cod_cta ? padProfit(tp.cod_cta, 6) : null)
                    .input('cod_caja', sql.Char(6), tp.cod_caja ? padProfit(tp.cod_caja, 6) : null)
                    .input('mov_num_c', sql.Char(20), movNumC ? padProfit(movNumC, 20) : null)
                    .input('mov_num_b', sql.Char(20), movNumB ? padProfit(movNumB, 20) : null)
                    .input('num_doc', sql.Char(20), tp.num_doc ? padProfit(tp.num_doc, 20) : null)
                    .input('mont_doc', sql.Decimal(18, 2), Number(tp.mont_doc))
                    .input('fecha_che', sql.SmallDateTime, tp.fecha_che ? new Date(tp.fecha_che) : tsDate)
                    .input('co_sucu_in', sql.Char(6), padProfit(sucuCode, 6))
                    .input('co_us_in', sql.Char(6), padProfit(auditUser, 6))
                    .query(`
                        INSERT INTO saCobroTPReng (
                            reng_num, cob_num, co_tar, co_ban, forma_pag, cod_cta, cod_caja, co_vale,
                            mov_num_c, mov_num_b, num_doc, devuelto, mont_doc, fecha_che,
                            co_sucu_in, co_us_in, fe_us_in, co_sucu_mo, co_us_mo, fe_us_mo,
                            trasnfe, revisado, rowguid
                        ) VALUES (
                            @reng_num, @cob_num, @co_tar, @co_ban, @forma_pag, @cod_cta, @cod_caja, NULL,
                            @mov_num_c, @mov_num_b, @num_doc, 0, @mont_doc, @fecha_che,
                            @co_sucu_in, @co_us_in, GETDATE(), @co_sucu_in, @co_us_in, GETDATE(),
                            NULL, NULL, NEWID()
                        )
                    `);
            }

            // 5. Insertar desgloses de Retenciones de IVA (saCobroRetenIvaReng)
            if (data.retenciones_iva && data.retenciones_iva.length > 0) {
                for (let i = 0; i < data.retenciones_iva.length; i++) {
                    const ret = data.retenciones_iva[i];
                    const parentDocNum = ret.nro_doc_asoc?.trim();
                    const lineGuid = rengDocGuidMap.get(parentDocNum);

                    if (!lineGuid) {
                        throw new Error(`No se encontró el renglón del documento asoc. ${parentDocNum} para la retención de IVA.`);
                    }

                    await transaction.request()
                        .input('reng_num', sql.Int, i + 1)
                        .input('rowguid_reng_cob', sql.UniqueIdentifier, lineGuid)
                        .input('rif_contribuyente', sql.Char(10), ret.rif_contribuyente ? ret.rif_contribuyente.substring(0, 10) : ' ')
                        .input('periodo_impositivo', sql.Decimal(6), Number(ret.periodo_impositivo))
                        .input('fecha_documento', sql.SmallDateTime, ret.fecha_documento ? new Date(ret.fecha_documento) : tsDate)
                        .input('tipo_documento', sql.Char(4), 'FACT')
                        .input('rif_comprador', sql.Char(10), ret.rif_comprador ? ret.rif_comprador.substring(0, 10) : ' ')
                        .input('numero_documento', sql.Char(20), padProfit(ret.numero_documento, 20))
                        .input('numero_control_documento', sql.Char(20), padProfit(ret.numero_control_documento || '', 20))
                        .input('monto_documento', sql.Decimal(15, 2), Number(ret.monto_documento))
                        .input('base_imponible', sql.Decimal(15, 2), Number(ret.base_imponible))
                        .input('monto_ret_imp', sql.Decimal(15, 2), Number(ret.monto_ret_imp))
                        .input('numero_documento_afectado', sql.Char(20), padProfit(ret.numero_documento_afectado, 20))
                        .input('num_comprobante', sql.Char(14), ret.num_comprobante.substring(0, 14))
                        .input('monto_excento', sql.Decimal(15, 2), Number(ret.monto_excento || 0))
                        .input('alicuota', sql.Decimal(5, 2), Number(ret.alicuota))
                        .input('reten_tercero', sql.Bit, 0)
                        .input('numero_expediente', sql.Char(15), ' ')
                        .input('co_us_in', sql.Char(6), padProfit(auditUser, 6))
                        .input('co_sucu_in', sql.Char(6), padProfit(sucuCode, 6))
                        .query(`
                            INSERT INTO saCobroRetenIvaReng (
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
            }

            // 6. Insertar desgloses de Retenciones de ISLR/Municipal (saCobroRentenReng)
            if (data.retenciones_islr && data.retenciones_islr.length > 0) {
                for (let i = 0; i < data.retenciones_islr.length; i++) {
                    const ret = data.retenciones_islr[i];
                    const parentDocNum = ret.nro_doc_asoc?.trim();
                    const lineGuid = rengDocGuidMap.get(parentDocNum);

                    if (!lineGuid) {
                        throw new Error(`No se encontró el renglón del documento asoc. ${parentDocNum} para la retención de ISLR.`);
                    }

                    await transaction.request()
                        .input('reng_num', sql.Int, i + 1)
                        .input('rowguid_reng_cob', sql.UniqueIdentifier, lineGuid)
                        .input('co_islr', sql.Char(6), padProfit(ret.co_islr, 6))
                        .input('monto', sql.Decimal(18, 5), Number(ret.monto))
                        .input('monto_reten', sql.Decimal(18, 5), Number(ret.monto_reten))
                        .input('monto_obj', sql.Decimal(18, 5), Number(ret.monto_obj))
                        .input('sustraendo', sql.Decimal(18, 5), Number(ret.sustraendo || 0))
                        .input('porc_retn', sql.Decimal(18, 5), Number(ret.porc_retn))
                        .input('co_us_in', sql.Char(6), padProfit(auditUser, 6))
                        .input('co_sucu_in', sql.Char(6), padProfit(sucuCode, 6))
                        .query(`
                            INSERT INTO saCobroRentenReng (
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

            await transaction.commit();
            return { success: true, doc_num: cobNum };

        } catch (err) {
            if (transaction._aborted === false) await transaction.rollback();
            throw err;
        }
    });

    return writeResponse(res, outcome);
});

// --- EDITAR COBRO (ACTUALIZAR ABONOS, RETENCIONES E INSTRUMENTOS) ---
router.put('/:cob_num', async (req, res) => {
    const { cob_num } = req.params;
    const data = req.body;

    if (!data.co_cli || !data.renglones) {
        return res.status(400).json({ success: false, message: 'Campos obligatorios: co_cli, renglones' });
    }

    const outcome = await executeWrite(req.query.sede || null, req.sqlAuth, async (pool, srv) => {
        const [resVen, resSucu, resCtaIE] = await Promise.all([
            pool.request().query(`SELECT TOP 1 RTRIM(co_ven) AS co_ven FROM saVendedor`),
            pool.request().query(`SELECT TOP 1 RTRIM(co_sucur) AS co_sucur FROM saSucursal`),
            pool.request().query(`SELECT TOP 1 RTRIM(co_cta_ingr_egr) AS co_cta_ingr_egr FROM saCuentaIngEgr`)
        ]);

        const defVen = resVen.recordset[0]?.co_ven || '01';
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
            const cobNum = padProfit(cob_num, 20);

            // 1. Verificar existencia y estado del cobro
            const resStatus = await transaction.request()
                .input('cob_num', sql.Char(20), cobNum)
                .query(`SELECT anulado, LTRIM(RTRIM(co_cli)) AS co_cli, fecha FROM saCobro WHERE cob_num = @cob_num`);
            
            const cobRecord = resStatus.recordset[0];
            if (!cobRecord) {
                throw new Error(`El cobro N° ${cob_num} no existe.`);
            }
            if (cobRecord.anulado) {
                throw new Error(`El cobro N° ${cob_num} se encuentra anulado y no se puede editar.`);
            }
            const originalCobDate = cobRecord.fecha;

            // 2. Verificar conciliación de movimientos de banco y caja
            const resCtaMoviCheck = await transaction.request()
                .input('cob_num', sql.Char(20), cobNum)
                .query(`
                    SELECT COUNT(*) AS reconciled 
                    FROM saMovimientoBanco 
                    WHERE mov_num IN (SELECT RTRIM(mov_num_b) FROM saCobroTPReng WHERE cob_num = @cob_num) 
                      AND LTRIM(RTRIM(tipo_op)) IN ('DP', 'CH', 'TR') 
                      AND conciliado = 1
                `);
            const resCajaMoviCheck = await transaction.request()
                .input('cob_num', sql.Char(20), cobNum)
                .query(`
                    SELECT COUNT(*) AS reconciled 
                    FROM saMovimientoCaja 
                    WHERE mov_num IN (SELECT RTRIM(mov_num_c) FROM saCobroTPReng WHERE cob_num = @cob_num) 
                      AND LTRIM(RTRIM(tipo_mov)) = 'I' 
                      AND (depositado = 1 OR transferido = 1)
                `);

            if (resCtaMoviCheck.recordset[0].reconciled > 0 || resCajaMoviCheck.recordset[0].reconciled > 0) {
                throw new Error(`El cobro N° ${cob_num} posee movimientos conciliados en banco o caja y no puede ser editado.`);
            }

            // 3. Reversar saldos de las facturas/documentos originales amortizados
            const resOldReng = await transaction.request()
                .input('cob_num', sql.Char(20), cobNum)
                .query(`
                    SELECT LTRIM(RTRIM(co_tipo_doc)) AS co_tipo_doc, LTRIM(RTRIM(nro_doc)) AS nro_doc, mont_cob
                    FROM saCobroDocReng
                    WHERE cob_num = @cob_num AND LTRIM(RTRIM(co_tipo_doc)) IN ('FACT', 'NDEB', 'N/DB', 'GIRO', 'AJPA')
                `);

            for (const oldDoc of resOldReng.recordset) {
                await transaction.request()
                    .input('co_tipo_doc', sql.Char(6), padProfit(oldDoc.co_tipo_doc, 6))
                    .input('nro_doc', sql.Char(20), padProfit(oldDoc.nro_doc, 20))
                    .input('mont_cob', sql.Decimal(18, 2), oldDoc.mont_cob)
                    .query(`
                        UPDATE saDocumentoVenta
                        SET saldo = saldo + @mont_cob, fe_us_mo = GETDATE()
                        WHERE LTRIM(RTRIM(co_tipo_doc)) = LTRIM(RTRIM(@co_tipo_doc))
                          AND LTRIM(RTRIM(nro_doc)) = LTRIM(RTRIM(@nro_doc))
                    `);

                if (oldDoc.co_tipo_doc === 'FACT') {
                    await transaction.request()
                        .input('nro_doc', sql.Char(20), padProfit(oldDoc.nro_doc, 20))
                        .input('mont_cob', sql.Decimal(18, 2), oldDoc.mont_cob)
                        .query(`
                            UPDATE saFacturaVenta
                            SET saldo = saldo + @mont_cob, fe_us_mo = GETDATE()
                            WHERE LTRIM(RTRIM(doc_num)) = LTRIM(RTRIM(@nro_doc))
                        `);
                }
            }

            // 4. Eliminar movimientos de Caja y Banco asociados
            const resOldTP = await transaction.request()
                .input('cob_num', sql.Char(20), cobNum)
                .query(`SELECT RTRIM(mov_num_c) AS mov_num_c, RTRIM(mov_num_b) AS mov_num_b FROM saCobroTPReng WHERE cob_num = @cob_num`);

            for (const oldTP of resOldTP.recordset) {
                if (oldTP.mov_num_c) {
                    await transaction.request()
                        .input('mov_num_c', sql.Char(20), padProfit(oldTP.mov_num_c, 20))
                        .query(`DELETE FROM saMovimientoCaja WHERE LTRIM(RTRIM(mov_num)) = LTRIM(RTRIM(@mov_num_c))`);
                }
                if (oldTP.mov_num_b) {
                    await transaction.request()
                        .input('mov_num_b', sql.Char(20), padProfit(oldTP.mov_num_b, 20))
                        .query(`DELETE FROM saMovimientoBanco WHERE LTRIM(RTRIM(mov_num)) = LTRIM(RTRIM(@mov_num_b))`);
                }
            }

            // 5. Eliminar detalles de renglones viejos
            await transaction.request()
                .input('cob_num', sql.Char(20), cobNum)
                .query(`
                    DELETE FROM saCobroRetenIvaReng WHERE rowguid_reng_cob IN (SELECT rowguid FROM saCobroDocReng WHERE cob_num = @cob_num);
                    DELETE FROM saCobroRentenReng WHERE rowguid_reng_cob IN (SELECT rowguid FROM saCobroDocReng WHERE cob_num = @cob_num);
                    DELETE FROM saCobroDocReng WHERE cob_num = @cob_num;
                    DELETE FROM saCobroTPReng WHERE cob_num = @cob_num;
                `);

            // 6. Eliminar documentos de retención de IVA/ISLR o diferenciales asociados de saDocumentoVenta
            await transaction.request()
                .input('cob_num', sql.Char(20), cobNum)
                .query(`
                    DELETE FROM saDocumentoVenta
                    WHERE doc_orig = 'COBRO' 
                      AND LTRIM(RTRIM(nro_orig)) = LTRIM(RTRIM(@cob_num)) 
                      AND LTRIM(RTRIM(co_tipo_doc)) IN ('IVAN', 'ISLR', 'N/DB', 'N/CR')
                `);

            // 7. Actualizar Cabecera de Cobro
            let collectionMone = data.co_mone || 'USD';
            if (collectionMone.trim().toUpperCase() === 'US$') {
                collectionMone = 'USD';
            }
            const rateCobro = Number(data.tasa || 1);

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
                finalMontoHeader = collectionMone === 'USD' 
                    ? Math.round((totalFormasPagoBs / rateCobro) * 100) / 100 
                    : totalFormasPagoBs;
            } else {
                finalMontoHeader = Math.max(0, collectionMone === 'USD' 
                    ? Math.round((totalAbonoBs / rateCobro) * 100) / 100 
                    : totalAbonoBs);
            }

            await transaction.request()
                .input('cob_num', sql.Char(20), cobNum)
                .input('co_cli', sql.Char(16), padProfit(data.co_cli, 16))
                .input('co_ven', sql.Char(6), padProfit(data.co_ven || defVen, 6))
                .input('co_mone', sql.Char(6), padProfit(collectionMone, 6))
                .input('tasa', sql.Decimal(21, 8), rateCobro)
                .input('monto', sql.Decimal(18, 2), finalMontoHeader)
                .input('descrip', sql.VarChar(60), (data.descrip || 'COBRO DE CLIENTE').substring(0, 60))
                .input('co_us_mo', sql.Char(6), padProfit(auditUser, 6))
                .query(`
                    UPDATE saCobro
                    SET co_cli = @co_cli, co_ven = @co_ven, co_mone = @co_mone, tasa = @tasa,
                        monto = @monto, descrip = @descrip, co_us_mo = @co_us_mo, fe_us_mo = GETDATE()
                    WHERE cob_num = @cob_num
                `);

            // Mapas y variables para relacionar Renglones
            const rengDocGuidMap = new Map();
            const insertedRenglones = [];

            // 8. Re-Insertar Renglones de Documentos (saCobroDocReng)
            const parentTypes = ['FACT', 'NDEB', 'N/DB', 'GIRO', 'AJPA'];
            const parentLines = data.renglones.filter(r => parentTypes.includes(r.co_tipo_doc.trim().toUpperCase()));
            const childLines = data.renglones.filter(r => !parentTypes.includes(r.co_tipo_doc.trim().toUpperCase()));
            const sortedRenglones = [...parentLines, ...childLines];
            let nextRengNum = 1;

            for (let i = 0; i < sortedRenglones.length; i++) {
                const line = sortedRenglones[i];
                const rengNum = nextRengNum++;

                let parentGuid = null;
                if (!parentTypes.includes(line.co_tipo_doc.trim().toUpperCase())) {
                    const lookupKey = line.parent_doc ? line.parent_doc.trim() : line.nro_doc?.trim();
                    parentGuid = rengDocGuidMap.get(lookupKey);
                }

                let docSaldo = 0;
                let docTasa = 1;
                let docMone = 'BS';
                let docCoCli = data.co_cli;
                let docCoVen = data.co_ven || defVen;

                let docFecEmisStr = '';
                const docTypeUpper = line.co_tipo_doc.trim().toUpperCase();
                const queryTypes = ['FACT', 'NDEB', 'N/DB', 'GIRO', 'AJPA', 'N/CR'];
                if (queryTypes.includes(docTypeUpper)) {
                    const docInfo = await transaction.request()
                        .input('co_tipo_doc', sql.Char(6), padProfit(line.co_tipo_doc, 6))
                        .input('nro_doc', sql.Char(20), padProfit(line.nro_doc, 20))
                        .query(`
                            SELECT RTRIM(co_mone) AS co_mone, tasa, saldo, co_cli, co_ven,
                                   CONVERT(VARCHAR(10), fec_emis, 120) AS fec_emis_str
                            FROM saDocumentoVenta
                            WHERE LTRIM(RTRIM(co_tipo_doc)) = LTRIM(RTRIM(@co_tipo_doc))
                              AND LTRIM(RTRIM(nro_doc)) = LTRIM(RTRIM(@nro_doc))
                        `);
                    if (docInfo.recordset.length > 0) {
                        docSaldo = Number(docInfo.recordset[0].saldo || 0);
                        docTasa = Number(docInfo.recordset[0].tasa || 1);
                        docMone = docInfo.recordset[0].co_mone || 'BS';
                        docCoCli = docInfo.recordset[0].co_cli || docCoCli;
                        docCoVen = docInfo.recordset[0].co_ven || docCoVen;
                        docFecEmisStr = docInfo.recordset[0].fec_emis_str || '';
                    }
                }

                let finalMontCob = Math.abs(Number(line.mont_cob));
                let adjustedMontoRetencionIva = Number(line.monto_retencion_iva || 0);
                let adjustedMontoRetencion = Number(line.monto_retencion || 0);
                let diffBs = 0;

                let totalRebaje = finalMontCob + adjustedMontoRetencionIva + adjustedMontoRetencion;
                const todayStr = new Date().toISOString().split('T')[0];
                const isPreviousDateDoc = docFecEmisStr && docFecEmisStr < todayStr;

                if (isPreviousDateDoc && docSaldo > 0 && docTasa > 0 && rateCobro > docTasa) {
                    const saldoUsd = docSaldo / docTasa;
                    const montoBsActual = Math.round((saldoUsd * rateCobro) * 100) / 100;
                    diffBs = Math.max(0, Math.round((montoBsActual - docSaldo) * 100) / 100);

                    // La factura original se amortiza exactamente por su saldo en Bs original
                    finalMontCob = Math.max(0, docSaldo - adjustedMontoRetencionIva - adjustedMontoRetencion);
                    totalRebaje = finalMontCob + adjustedMontoRetencionIva + adjustedMontoRetencion;
                } else {
                    if (totalRebaje > docSaldo) {
                        const excess = totalRebaje - docSaldo;
                        finalMontCob = Math.max(0, finalMontCob - excess);
                        diffBs = diffBs + excess;
                        totalRebaje = finalMontCob + adjustedMontoRetencionIva + adjustedMontoRetencion;
                    }
                }

                const guidResult = await transaction.request().query('SELECT NEWID() AS guid');
                const lineGuid = guidResult.recordset[0].guid;
                rengDocGuidMap.set(line.nro_doc?.trim(), lineGuid);

                await transaction.request()
                    .input('reng_num', sql.Int, rengNum)
                    .input('cob_num', sql.Char(20), cobNum)
                    .input('co_tipo_doc', sql.Char(6), padProfit(line.co_tipo_doc, 6))
                    .input('nro_doc', sql.Char(20), padProfit(line.nro_doc, 20))
                    .input('mont_cob', sql.Decimal(18, 2), totalRebaje)
                    .input('monto_retencion_iva', sql.Decimal(18, 5), adjustedMontoRetencionIva)
                    .input('monto_retencion', sql.Decimal(18, 2), adjustedMontoRetencion)
                    .input('rowguid_reng_ori', sql.UniqueIdentifier, parentGuid)
                    .input('co_sucu_in', sql.Char(6), padProfit(sucuCode, 6))
                    .input('co_us_in', sql.Char(6), padProfit(auditUser, 6))
                    .input('rowguid', sql.UniqueIdentifier, lineGuid)
                    .query(`
                        INSERT INTO saCobroDocReng (
                            reng_num, cob_num, co_tipo_doc, nro_doc, mont_cob,
                            dpcobro_porc_desc, dpcobro_monto, monto_retencion_iva, monto_retencion,
                            reten_tercero_rowguid_ori, tipo_doc, num_doc, rowguid_reng_ori,
                            tipo_origen, gen_origen, co_sucu_in, co_us_in, fe_us_in, co_sucu_mo, co_us_mo, fe_us_mo,
                            trasnfe, revisado, rowguid
                        ) VALUES (
                            @reng_num, @cob_num, @co_tipo_doc, @nro_doc, @mont_cob,
                            0.00, 0.00, @monto_retencion_iva, @monto_retencion,
                            NULL, NULL, NULL, @rowguid_reng_ori,
                            NULL, NULL, @co_sucu_in, @co_us_in, GETDATE(), @co_sucu_in, @co_us_in, GETDATE(),
                            NULL, NULL, @rowguid
                        )
                    `);

                // Rebajar saldo en saDocumentoVenta
                totalRebaje = finalMontCob + adjustedMontoRetencionIva + adjustedMontoRetencion;
                await transaction.request()
                    .input('co_tipo_doc', sql.Char(6), padProfit(line.co_tipo_doc, 6))
                    .input('nro_doc', sql.Char(20), padProfit(line.nro_doc, 20))
                    .input('rebaje', sql.Decimal(18, 2), totalRebaje)
                    .input('user', sql.Char(6), padProfit(auditUser, 6))
                    .query(`
                        UPDATE saDocumentoVenta
                        SET saldo = saldo - @rebaje, fe_us_mo = GETDATE(), co_us_mo = @user
                        WHERE LTRIM(RTRIM(co_tipo_doc)) = LTRIM(RTRIM(@co_tipo_doc))
                          AND LTRIM(RTRIM(nro_doc)) = LTRIM(RTRIM(@nro_doc))
                    `);

                if (line.co_tipo_doc.trim().toUpperCase() === 'FACT') {
                    await transaction.request()
                        .input('nro_doc', sql.Char(20), padProfit(line.nro_doc, 20))
                        .input('rebaje', sql.Decimal(18, 2), totalRebaje)
                        .input('user', sql.Char(6), padProfit(auditUser, 6))
                        .query(`
                            UPDATE saFacturaVenta
                            SET saldo = saldo - @rebaje, fe_us_mo = GETDATE(), co_us_mo = @user
                            WHERE LTRIM(RTRIM(doc_num)) = LTRIM(RTRIM(@nro_doc))
                        `);
                }

                // Generar documentos retención de IVA
                if (adjustedMontoRetencionIva > 0) {
                    let ivanNum = null;
                    try {
                        const ivanRes = await transaction.request()
                            .input('sCo_Sucur', sql.Char(6), padProfit(sucuCode, 6))
                            .input('sCo_Consecutivo', sql.Char(16), padProfit('DOC_VEN_IVAN', 16))
                            .execute('pConsecutivoProximo');
                        ivanNum = ivanRes.recordset[0]?.ProximoConsecutivo?.trim();
                    } catch (err) {
                        const ivanResGlobal = await transaction.request()
                            .input('sCo_Sucur', sql.Char(6), '')
                            .input('sCo_Consecutivo', sql.Char(16), padProfit('DOC_VEN_IVAN', 16))
                            .execute('pConsecutivoProximo');
                        ivanNum = ivanResGlobal.recordset[0]?.ProximoConsecutivo?.trim();
                    }

                    if (!ivanNum) {
                        throw new Error('No se pudo obtener el próximo consecutivo para el documento IVAN.');
                    }

                    const retIvaMatch = data.retenciones_iva?.find(r => r.nro_doc_asoc?.trim() === line.nro_doc?.trim());
                    const numComprobante = retIvaMatch?.num_comprobante || '';

                    await transaction.request()
                        .input('co_tipo_doc', sql.Char(6), padProfit('IVAN', 6))
                        .input('nro_doc', sql.Char(20), padProfit(ivanNum, 20))
                        .input('co_cli', sql.Char(16), padProfit(data.co_cli, 16))
                        .input('co_ven', sql.Char(6), padProfit(data.co_ven || defVen, 6))
                        .input('co_mone', sql.Char(6), padProfit(docMone, 6))
                        .input('tasa', sql.Decimal(21, 8), docTasa)
                        .input('observa', sql.VarChar(120), `COBRO N° ${cob_num}`)
                        .input('doc_orig', sql.Char(6), padProfit('COBRO', 6))
                        .input('tipo_origen', sql.Int, 0)
                        .input('nro_orig', sql.VarChar(20), cob_num)
                        .input('total_bruto', sql.Decimal(18, 2), adjustedMontoRetencionIva)
                        .input('total_neto', sql.Decimal(18, 2), adjustedMontoRetencionIva)
                        .input('saldo', sql.Decimal(18, 2), 0.00)
                        .input('co_us_in', sql.Char(6), padProfit(auditUser, 6))
                        .input('co_sucu_in', sql.Char(6), padProfit(sucuCode, 6))
                        .input('num_comprobante', sql.Char(14), numComprobante.substring(0, 14))
                        .input('fecha_doc', sql.SmallDateTime, originalCobDate)
                        .query(`
                            INSERT INTO saDocumentoVenta (
                                co_tipo_doc, nro_doc, co_cli, co_ven, co_mone, tasa, observa,
                                fec_reg, fec_emis, fec_venc, anulado, aut, contrib,
                                doc_orig, tipo_origen, nro_orig, saldo, total_bruto,
                                total_neto, monto_imp, monto_imp2, monto_imp3, porc_imp, porc_imp2, porc_imp3,
                                comis1, comis2, comis3, comis4, comis5, comis6, adicional, ven_ter,
                                otros1, otros2, otros3, n_control, co_us_in, co_sucu_in, fe_us_in,
                                co_us_mo, co_sucu_mo, fe_us_mo, rowguid,
                                monto_desc_glob, monto_reca, num_comprobante, tipo_imp
                            ) VALUES (
                                @co_tipo_doc, @nro_doc, @co_cli, @co_ven, @co_mone, @tasa, @observa,
                                CONVERT(VARCHAR(10), @fecha_doc, 120), CONVERT(VARCHAR(10), @fecha_doc, 120), CONVERT(VARCHAR(10), @fecha_doc, 120), 0, 1, 0,
                                @doc_orig, @tipo_origen, @nro_orig, @saldo, @total_bruto,
                                @total_neto, 0, 0, 0, 0, 0, 0,
                                0, 0, 0, 0, 0, 0, 0, 0,
                                0, 0, 0, '', @co_us_in, @co_sucu_in, GETDATE(),
                                @co_us_in, @co_sucu_in, GETDATE(), NEWID(),
                                0, 0, @num_comprobante, 7
                            )
                        `);

                    // Insertar renglón de IVAN en saCobroDocReng
                    const ivanRengNum = nextRengNum++;
                    const ivanRengGuidResult = await transaction.request().query('SELECT NEWID() AS guid');
                    const ivanRengGuid = ivanRengGuidResult.recordset[0].guid;

                    await transaction.request()
                        .input('reng_num', sql.Int, ivanRengNum)
                        .input('cob_num', cobNum)
                        .input('co_tipo_doc', sql.Char(6), padProfit('IVAN', 6))
                        .input('nro_doc', sql.Char(20), padProfit(ivanNum, 20))
                        .input('mont_cob', sql.Decimal(18, 2), adjustedMontoRetencionIva)
                        .input('rowguid_reng_ori', sql.UniqueIdentifier, lineGuid)
                        .input('co_sucu_in', sql.Char(6), padProfit(sucuCode, 6))
                        .input('co_us_in', sql.Char(6), padProfit(auditUser, 6))
                        .input('rowguid', sql.UniqueIdentifier, ivanRengGuid)
                        .query(`
                            INSERT INTO saCobroDocReng (
                                reng_num, cob_num, co_tipo_doc, nro_doc, mont_cob,
                                dpcobro_porc_desc, dpcobro_monto, monto_retencion_iva, monto_retencion,
                                rowguid_reng_ori, co_sucu_in, co_us_in, fe_us_in, co_sucu_mo, co_us_mo, fe_us_mo, rowguid
                            ) VALUES (
                                @reng_num, @cob_num, @co_tipo_doc, @nro_doc, @mont_cob,
                                0.00, 0.00, 0.00, 0.00,
                                @rowguid_reng_ori, @co_sucu_in, @co_us_in, GETDATE(), @co_sucu_in, @co_us_in, GETDATE(), @rowguid
                            )
                        `);
                }

                // Generar documentos retención de ISLR
                if (adjustedMontoRetencion > 0) {
                    let islrNum = null;
                    try {
                        const islrRes = await transaction.request()
                            .input('sCo_Sucur', sql.Char(6), padProfit(sucuCode, 6))
                            .input('sCo_Consecutivo', sql.Char(16), padProfit('DOC_VEN_ISLR', 16))
                            .execute('pConsecutivoProximo');
                        islrNum = islrRes.recordset[0]?.ProximoConsecutivo?.trim();
                    } catch (err) {
                        const islrResGlobal = await transaction.request()
                            .input('sCo_Sucur', sql.Char(6), '')
                            .input('sCo_Consecutivo', sql.Char(16), padProfit('DOC_VEN_ISLR', 16))
                            .execute('pConsecutivoProximo');
                        islrNum = islrResGlobal.recordset[0]?.ProximoConsecutivo?.trim();
                    }

                    if (!islrNum) {
                        throw new Error('No se pudo obtener el próximo consecutivo para el documento ISLR.');
                    }

                    await transaction.request()
                        .input('co_tipo_doc', sql.Char(6), padProfit('ISLR', 6))
                        .input('nro_doc', sql.Char(20), padProfit(islrNum, 20))
                        .input('co_cli', sql.Char(16), padProfit(data.co_cli, 16))
                        .input('co_ven', sql.Char(6), padProfit(data.co_ven || defVen, 6))
                        .input('co_mone', sql.Char(6), padProfit(docMone, 6))
                        .input('tasa', sql.Decimal(21, 8), docTasa)
                        .input('observa', sql.VarChar(120), `COBRO N° ${cob_num}`)
                        .input('doc_orig', sql.Char(6), padProfit('COBRO', 6))
                        .input('tipo_origen', sql.Int, 0)
                        .input('nro_orig', sql.VarChar(20), cob_num)
                        .input('total_bruto', sql.Decimal(18, 2), adjustedMontoRetencion)
                        .input('total_neto', sql.Decimal(18, 2), adjustedMontoRetencion)
                        .input('saldo', sql.Decimal(18, 2), 0.00)
                        .input('co_us_in', sql.Char(6), padProfit(auditUser, 6))
                        .input('co_sucu_in', sql.Char(6), padProfit(sucuCode, 6))
                        .input('fecha_doc', sql.SmallDateTime, originalCobDate)
                        .query(`
                            INSERT INTO saDocumentoVenta (
                                co_tipo_doc, nro_doc, co_cli, co_ven, co_mone, tasa, observa,
                                fec_reg, fec_emis, fec_venc, anulado, aut, contrib,
                                doc_orig, tipo_origen, nro_orig, saldo, total_bruto,
                                total_neto, monto_imp, monto_imp2, monto_imp3, porc_imp, porc_imp2, porc_imp3,
                                comis1, comis2, comis3, comis4, comis5, comis6, adicional, ven_ter,
                                otros1, otros2, otros3, n_control, co_us_in, co_sucu_in, fe_us_in,
                                co_us_mo, co_sucu_mo, fe_us_mo, rowguid,
                                monto_desc_glob, monto_reca
                            ) VALUES (
                                @co_tipo_doc, @nro_doc, @co_cli, @co_ven, @co_mone, @tasa, @observa,
                                CONVERT(VARCHAR(10), @fecha_doc, 120), CONVERT(VARCHAR(10), @fecha_doc, 120), CONVERT(VARCHAR(10), @fecha_doc, 120), 0, 1, 0,
                                @doc_orig, @tipo_origen, @nro_orig, @saldo, @total_bruto,
                                @total_neto, 0, 0, 0, 0, 0, 0,
                                0, 0, 0, 0, 0, 0, 0, 0,
                                0, 0, 0, '', @co_us_in, @co_sucu_in, GETDATE(),
                                @co_us_in, @co_sucu_in, GETDATE(), NEWID(),
                                0, 0
                            )
                        `);

                    // Insertar renglón de ISLR en saCobroDocReng
                    const islrRengNum = nextRengNum++;
                    const islrRengGuidResult = await transaction.request().query('SELECT NEWID() AS guid');
                    const islrRengGuid = islrRengGuidResult.recordset[0].guid;

                    await transaction.request()
                        .input('reng_num', sql.Int, islrRengNum)
                        .input('cob_num', cobNum)
                        .input('co_tipo_doc', sql.Char(6), padProfit('ISLR', 6))
                        .input('nro_doc', sql.Char(20), padProfit(islrNum, 20))
                        .input('mont_cob', sql.Decimal(18, 2), adjustedMontoRetencion)
                        .input('rowguid_reng_ori', sql.UniqueIdentifier, lineGuid)
                        .input('co_sucu_in', sql.Char(6), padProfit(sucuCode, 6))
                        .input('co_us_in', sql.Char(6), padProfit(auditUser, 6))
                        .input('rowguid', sql.UniqueIdentifier, islrRengGuid)
                        .query(`
                            INSERT INTO saCobroDocReng (
                                reng_num, cob_num, co_tipo_doc, nro_doc, mont_cob,
                                dpcobro_porc_desc, dpcobro_monto, monto_retencion_iva, monto_retencion,
                                rowguid_reng_ori, co_sucu_in, co_us_in, fe_us_in, co_sucu_mo, co_us_mo, fe_us_mo, rowguid
                            ) VALUES (
                                @reng_num, @cob_num, @co_tipo_doc, @nro_doc, @mont_cob,
                                0.00, 0.00, 0.00, 0.00,
                                @rowguid_reng_ori, @co_sucu_in, @co_us_in, GETDATE(), @co_sucu_in, @co_us_in, GETDATE(), @rowguid
                            )
                        `);
                }

                insertedRenglones.push({
                    co_tipo_doc: line.co_tipo_doc,
                    nro_doc: line.nro_doc,
                    rowguid: lineGuid
                });

                // Generar Nota de Débito (N/DB) o Crédito (N/CR) por diferencial cambiario en edición
                if (Math.abs(diffBs) > 0.01) {
                    const isDebit = diffBs > 0;
                    const diffDocType = isDebit ? 'N/DB' : 'N/CR';
                    const consecName = isDebit ? 'DOC_VEN_N/DB' : 'DOC_VEN_N/CR';

                    const resCorrDiff = await transaction.request().query(`
                        UPDATE saSerie
                        SET prox_n = prox_n + 1, fe_us_mo = GETDATE()
                        OUTPUT INSERTED.prox_n, RTRIM(INSERTED.desde_a) as prefijo
                        WHERE co_serie = (
                            SELECT TOP 1 co_serie
                            FROM saConsecutivo
                            WHERE UPPER(LTRIM(RTRIM(co_consecutivo))) = '${consecName}'
                              AND co_serie IS NOT NULL
                        )
                    `);
                    let corrDiff = resCorrDiff.recordset[0];
                    if (!corrDiff || !corrDiff.prox_n) {
                        throw new Error(`No se pudo obtener el correlativo para la Nota de ${isDebit ? 'Débito' : 'Crédito'} de diferencial cambiario.`);
                    }
                    const proxDiff = Number(corrDiff.prox_n || 0);
                    const diffDocNum = proxDiff.toString().padStart(10, '0');

                    // Insertar N/DB o N/CR en saDocumentoVenta
                    await transaction.request()
                        .input('co_tipo_doc', sql.Char(6), padProfit(diffDocType, 6))
                        .input('nro_doc', sql.Char(20), padProfit(diffDocNum, 20))
                        .input('co_cli', sql.Char(16), padProfit(docCoCli, 16))
                        .input('co_ven', sql.Char(6), padProfit(docCoVen, 6))
                        .input('co_mone', sql.Char(6), padProfit('BS', 6))
                        .input('tasa', sql.Decimal(21, 8), 1.00)
                        .input('observa', sql.VarChar(120), (`Diferencial Cambiario COBRO N° ${cob_num} ${line.nro_doc.trim()}`).substring(0, 120))
                        .input('doc_orig', sql.Char(6), padProfit('COBRO', 6))
                        .input('tipo_origen', sql.Int, 2)
                        .input('nro_orig', sql.VarChar(20), cob_num)
                        .input('total_bruto', sql.Decimal(18, 2), Math.abs(diffBs))
                        .input('total_neto', sql.Decimal(18, 2), Math.abs(diffBs))
                        .input('saldo', sql.Decimal(18, 2), 0.00)
                        .input('co_us_in', sql.Char(6), padProfit(auditUser, 6))
                        .input('co_sucu_in', sql.Char(6), padProfit(sucuCode, 6))
                        .query(`
                            INSERT INTO saDocumentoVenta (
                                co_tipo_doc, nro_doc, co_cli, co_ven, co_mone, tasa, observa,
                                fec_reg, fec_emis, fec_venc, anulado, aut, contrib,
                                doc_orig, tipo_origen, nro_orig, saldo, total_bruto,
                                total_neto, monto_imp, monto_imp2, monto_imp3, porc_imp, porc_imp2, porc_imp3,
                                comis1, comis2, comis3, comis4, comis5, comis6, adicional, ven_ter,
                                otros1, otros2, otros3, n_control, co_us_in, co_sucu_in, fe_us_in,
                                co_us_mo, co_sucu_mo, fe_us_mo, rowguid,
                                monto_desc_glob, monto_reca
                            ) VALUES (
                                @co_tipo_doc, @nro_doc, @co_cli, @co_ven, @co_mone, @tasa, @observa,
                                CONVERT(VARCHAR(10), GETDATE(), 120), CONVERT(VARCHAR(10), GETDATE(), 120), CONVERT(VARCHAR(10), GETDATE(), 120), 0, 1, 0,
                                @doc_orig, @tipo_origen, @nro_orig, @saldo, @total_bruto,
                                @total_neto, 0, 0, 0, 0, 0, 0,
                                0, 0, 0, 0, 0, 0, 0, 0,
                                0, 0, 0, @nro_doc, @co_us_in, @co_sucu_in, GETDATE(),
                                @co_us_in, @co_sucu_in, GETDATE(), NEWID(),
                                0.00, 0.00
                            )
                        `);

                    // Insertar renglón en saCobroDocReng
                    const diffRengNum = nextRengNum++;
                    const diffRengGuidResult = await transaction.request().query('SELECT NEWID() AS guid');
                    const diffRengGuid = diffRengGuidResult.recordset[0].guid;

                    await transaction.request()
                        .input('reng_num', sql.Int, diffRengNum)
                        .input('cob_num', cobNum)
                        .input('co_tipo_doc', sql.Char(6), padProfit(diffDocType, 6))
                        .input('nro_doc', sql.Char(20), padProfit(diffDocNum, 20))
                        .input('mont_cob', sql.Decimal(18, 2), Math.abs(diffBs))
                        .input('co_sucu_in', sql.Char(6), padProfit(sucuCode, 6))
                        .input('co_us_in', sql.Char(6), padProfit(auditUser, 6))
                        .input('rowguid', sql.UniqueIdentifier, diffRengGuid)
                        .query(`
                            INSERT INTO saCobroDocReng (
                                reng_num, cob_num, co_tipo_doc, nro_doc, mont_cob,
                                dpcobro_porc_desc, dpcobro_monto, monto_retencion_iva, monto_retencion,
                                co_sucu_in, co_us_in, fe_us_in, co_sucu_mo, co_us_mo, fe_us_mo, rowguid
                            ) VALUES (
                                @reng_num, @cob_num, @co_tipo_doc, @nro_doc, @mont_cob,
                                0.00, 0.00, 0.00, 0.00,
                                @co_sucu_in, @co_us_in, GETDATE(), @co_sucu_in, @co_us_in, GETDATE(), @rowguid
                            )
                        `);
                }
            }

            // 9. Re-Insertar Formas de Pago (saCobroTPReng) y crear movimientos en Caja/Banco
            const dummyCaja = '02';
            const activeFormasPago = data.formas_pago && data.formas_pago.length > 0
                ? data.formas_pago
                : [{
                    forma_pag: 'EF',
                    cod_caja: dummyCaja,
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

                if (tp.forma_pag === 'EF' || tp.forma_pag === 'TJ' || tp.forma_pag === 'CT') {
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
                        const resCorrCaja = await transaction.request().query(`
                            UPDATE saSerie
                            SET prox_n = prox_n + 1, fe_us_mo = GETDATE()
                            OUTPUT INSERTED.prox_n
                            WHERE co_serie = (
                                SELECT TOP 1 co_serie
                                FROM saConsecutivo
                                WHERE UPPER(LTRIM(RTRIM(co_consecutivo))) = 'MOVC_NUM'
                                  AND co_serie IS NOT NULL
                            )
                        `);
                        let corrCaja = resCorrCaja.recordset[0];
                        if (!corrCaja || !corrCaja.prox_n) {
                            throw new Error("No se pudo obtener el correlativo de movimiento de caja.");
                        }
                        movNumC = Number(corrCaja.prox_n).toString().padStart(10, '0');

                        const rMovC = new sql.Request(transaction);
                        rMovC.input('sMov_Num', sql.Char(20), padProfit(movNumC, 20));
                        rMovC.input('sdFecha', sql.SmallDateTime, tsDate);
                        rMovC.input('sDescrip', sql.VarChar(60), (`INGR. COBRO ${cob_num} - ${data.co_cli}`).substring(0, 60));
                        rMovC.input('sCod_Caja', sql.Char(6), padProfit(tp.cod_caja, 6));
                        rMovC.input('deTasa', sql.Decimal(21, 8), isUSDcaja ? rate : 1);
                        rMovC.input('sTipo_Mov', sql.Char(2), 'I');
                        rMovC.input('sForma_Pag', sql.Char(2), tp.forma_pag);
                        rMovC.input('sNum_Pago', sql.VarChar(20), tp.num_doc ? tp.num_doc.substring(0, 20) : null);
                        rMovC.input('sCo_Ban', sql.Char(6), tp.co_ban ? padProfit(tp.co_ban, 6) : null);
                        rMovC.input('sCo_Tar', sql.Char(6), tp.co_tar ? padProfit(tp.co_tar, 6) : null);
                        rMovC.input('sCo_Cta_Ingr_Egr', sql.Char(20), padProfit(defCtaIE, 20));
                        rMovC.input('deMonto', sql.Decimal(18, 2), finalMontoCaja);
                        rMovC.input('bSaldo_Ini', sql.Bit, 0);
                        rMovC.input('sOrigen', sql.Char(3), 'COB');
                        rMovC.input('sDoc_Num', sql.VarChar(20), cob_num.substring(0, 20));
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
                        const resCorrBanco = await transaction.request().query(`
                            UPDATE saSerie
                            SET prox_n = prox_n + 1, fe_us_mo = GETDATE()
                            OUTPUT INSERTED.prox_n
                            WHERE co_serie = (
                                SELECT TOP 1 co_serie
                                FROM saConsecutivo
                                WHERE UPPER(LTRIM(RTRIM(co_consecutivo))) = 'MOVB_NUM'
                                  AND co_serie IS NOT NULL
                            )
                        `);
                        let corrBanco = resCorrBanco.recordset[0];
                        if (!corrBanco || !corrBanco.prox_n) {
                            throw new Error("No se pudo obtener el correlativo de movimiento de banco.");
                        }
                        movNumB = Number(corrBanco.prox_n).toString().padStart(10, '0');

                        let tipoOp = 'TR';
                        if (tp.forma_pag === 'DP') tipoOp = 'DP';
                        if (tp.forma_pag === 'CH') tipoOp = 'CH';

                        const rMovB = new sql.Request(transaction);
                        rMovB.input('sMov_Num', sql.Char(20), padProfit(movNumB, 20));
                        rMovB.input('sDescrip', sql.VarChar(160), (`INGR. COBRO ${cob_num} - ${data.co_cli}`).substring(0, 160));
                        rMovB.input('sCod_Cta', sql.Char(6), padProfit(tp.cod_cta, 6));
                        rMovB.input('sdFecha', sql.SmallDateTime, tsDate);
                        rMovB.input('deTasa', sql.Decimal(21, 8), isUSDcuenta ? rate : 1);
                        rMovB.input('sTipo_Op', sql.Char(2), tipoOp);
                        rMovB.input('sDoc_Num', sql.VarChar(20), (tp.num_doc || '').substring(0, 20));
                        rMovB.input('deMonto', sql.Decimal(18, 2), finalMontoBanco);
                        rMovB.input('sCo_Cta_Ingr_Egr', sql.Char(20), padProfit(defCtaIE, 20));
                        rMovB.input('sOrigen', sql.Char(3), 'COB');
                        rMovB.input('sCob_Pag', sql.Char(20), cobNum);
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

                await transaction.request()
                    .input('reng_num', sql.Int, rengNum)
                    .input('cob_num', cobNum)
                    .input('co_tar', sql.Char(6), tp.co_tar ? padProfit(tp.co_tar, 6) : null)
                    .input('co_ban', sql.Char(6), tp.co_ban ? padProfit(tp.co_ban, 6) : null)
                    .input('forma_pag', sql.Char(2), tp.forma_pag === 'TE' ? 'TP' : tp.forma_pag)
                    .input('cod_cta', sql.Char(6), tp.cod_cta ? padProfit(tp.cod_cta, 6) : null)
                    .input('cod_caja', sql.Char(6), tp.cod_caja ? padProfit(tp.cod_caja, 6) : null)
                    .input('mov_num_c', sql.Char(20), movNumC ? padProfit(movNumC, 20) : null)
                    .input('mov_num_b', sql.Char(20), movNumB ? padProfit(movNumB, 20) : null)
                    .input('num_doc', sql.Char(20), tp.num_doc ? padProfit(tp.num_doc, 20) : null)
                    .input('mont_doc', sql.Decimal(18, 2), Number(tp.mont_doc))
                    .input('fecha_che', sql.SmallDateTime, tp.fecha_che ? new Date(tp.fecha_che) : tsDate)
                    .input('co_sucu_in', sql.Char(6), padProfit(sucuCode, 6))
                    .input('co_us_in', sql.Char(6), padProfit(auditUser, 6))
                    .query(`
                        INSERT INTO saCobroTPReng (
                            reng_num, cob_num, co_tar, co_ban, forma_pag, cod_cta, cod_caja, co_vale,
                            mov_num_c, mov_num_b, num_doc, devuelto, mont_doc, fecha_che,
                            co_sucu_in, co_us_in, fe_us_in, co_sucu_mo, co_us_mo, fe_us_mo,
                            trasnfe, revisado, rowguid
                        ) VALUES (
                            @reng_num, @cob_num, @co_tar, @co_ban, @forma_pag, @cod_cta, @cod_caja, NULL,
                            @mov_num_c, @mov_num_b, @num_doc, 0, @mont_doc, @fecha_che,
                            @co_sucu_in, @co_us_in, GETDATE(), @co_sucu_in, @co_us_in, GETDATE(),
                            NULL, NULL, NEWID()
                        )
                    `);
            }

            // 10. Re-Insertar desgloses de Retenciones de ISLR/Municipal (saCobroRentenReng)
            if (data.retenciones_islr && data.retenciones_islr.length > 0) {
                for (let i = 0; i < data.retenciones_islr.length; i++) {
                    const ret = data.retenciones_islr[i];
                    const parentDocNum = ret.nro_doc_asoc?.trim();
                    const lineGuid = rengDocGuidMap.get(parentDocNum);

                    if (!lineGuid) {
                        throw new Error(`No se encontró el renglón del documento asoc. ${parentDocNum} para la retención de ISLR.`);
                    }

                    await transaction.request()
                        .input('reng_num', sql.Int, i + 1)
                        .input('rowguid_reng_cob', sql.UniqueIdentifier, lineGuid)
                        .input('co_islr', sql.Char(6), padProfit(ret.co_islr, 6))
                        .input('monto', sql.Decimal(18, 5), Number(ret.monto))
                        .input('monto_reten', sql.Decimal(18, 5), Number(ret.monto_reten))
                        .input('monto_obj', sql.Decimal(18, 5), Number(ret.monto_obj))
                        .input('sustraendo', sql.Decimal(18, 5), Number(ret.sustraendo || 0))
                        .input('porc_retn', sql.Decimal(18, 5), Number(ret.porc_retn))
                        .input('co_us_in', sql.Char(6), padProfit(auditUser, 6))
                        .input('co_sucu_in', sql.Char(6), padProfit(sucuCode, 6))
                        .query(`
                            INSERT INTO saCobroRentenReng (
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

            await transaction.commit();
            return { success: true, doc_num: cob_num };

        } catch (err) {
            if (transaction._aborted === false) await transaction.rollback();
            throw err;
        }
    });

    return writeResponse(res, outcome);
});

module.exports = router;
