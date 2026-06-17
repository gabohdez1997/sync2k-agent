const express = require('express');
const router = express.Router();
const { sql, getPool, getServers, getExchangeRate } = require('../db');
const { executeWrite, writeResponse, paginatedResponse, padProfit } = require('../helpers/multiSede');

/**
 * @swagger
 * tags:
 *   name: Facturas
 *   description: Gestión de Facturas de venta (Caja)
 */

// --- OBTENER LISTADO ---
router.get('/', async (req, res) => {
    try {
        const page  = parseInt(req.query.page)  || 1;
        const limit = parseInt(req.query.limit) || 12;
        const { sede, doc_num, co_cli, co_ven, co_us_in, fec_d, fec_h, search } = req.query;
        
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
                if (co_cli) {
                    request.input('co_cli_search', sql.VarChar, `%${co_cli}%`);
                    whereClauses.push("(f.co_cli LIKE @co_cli_search OR cl.cli_des LIKE @co_cli_search OR f.doc_num LIKE @co_cli_search OR cl.rif LIKE @co_cli_search)");
                }
                if (search) {
                    request.input('search_all', sql.VarChar, `%${search}%`);
                    whereClauses.push("(f.doc_num LIKE @search_all OR f.co_cli LIKE @search_all OR cl.cli_des LIKE @search_all OR cl.rif LIKE @search_all)");
                }
                if (co_ven) {
                    request.input('co_ven_filter', sql.VarChar, co_ven.trim().toUpperCase());
                    whereClauses.push("LTRIM(RTRIM(f.co_ven)) = @co_ven_filter");
                }
                if (co_us_in) {
                    request.input('co_us_in_filter', sql.VarChar, co_us_in.trim().toUpperCase());
                    whereClauses.push("LTRIM(RTRIM(f.co_us_in)) = @co_us_in_filter");
                }
                if (fec_d) {
                    request.input('fec_d', sql.SmallDateTime, fec_d);
                    whereClauses.push("f.fec_emis >= @fec_d");
                }
                if (fec_h) {
                    request.input('fec_h', sql.SmallDateTime, fec_h);
                    whereClauses.push("f.fec_emis <= @fec_h");
                }

                const whereSQL = whereClauses.join(" AND ");
                
                const result = await request.query(`
                    SELECT RTRIM(f.doc_num) AS doc_num, RTRIM(f.descrip) AS descrip,
                           RTRIM(f.co_cli)  AS co_cli,  RTRIM(cl.cli_des) AS cli_des,
                           f.fec_emis, f.fec_venc, f.fec_reg, f.fe_us_in AS fec_us_in, f.fe_us_mo AS fec_us_mo, f.anulado,
                           RTRIM(f.co_mone) AS co_mone, f.tasa, f.total_neto,
                           RTRIM(f.co_ven) AS co_ven, RTRIM(f.co_us_in) AS co_us_in
                    FROM saFacturaVenta f
                    LEFT JOIN saCliente cl ON f.co_cli = cl.co_cli
                    WHERE ${whereSQL}
                    ORDER BY f.fec_emis DESC, f.doc_num DESC
                `);

                return result.recordset.map(c => ({ ...c, sede_id: srv.id, sede_nombre: srv.name }));
            } catch (e) { 
                console.error(`[FACTURAS] Error en sede ${srv.id}:`, e.message);
                return []; 
            }
        }));

        const combined = [].concat(...allData);
        combined.sort((a, b) => new Date(b.fec_emis) - new Date(a.fec_emis));
        return paginatedResponse(res, combined, page, limit);
    } catch (error) {
        res.status(500).json({ success: false, message: 'Error al consultar Facturas.', error: error.message });
    }
});

// --- OBTENER DETALLE ---
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
                        SELECT RTRIM(f.doc_num) AS doc_num, RTRIM(f.descrip) AS descrip,
                               RTRIM(f.co_cli)  AS co_cli,  RTRIM(cl.cli_des) AS cli_des,
                               RTRIM(f.co_ven)  AS co_ven,  RTRIM(v.ven_des)  AS ven_des,
                               RTRIM(f.co_cond) AS co_cond, RTRIM(cd.cond_des) AS cond_des,
                               f.fec_emis, f.fec_venc, f.fec_reg, f.fe_us_in AS fec_us_in, f.fe_us_mo AS fec_us_mo, f.anulado,
                               RTRIM(f.co_mone) AS co_mone, f.tasa,
                               f.total_bruto, f.monto_imp, f.total_neto,
                               RTRIM(f.comentario) AS comentario,
                               RTRIM(cl.rif) AS rif, RTRIM(cl.direc1) AS direc1, 
                               RTRIM(cl.telefonos) AS telefonos, RTRIM(cl.email) AS email,
                               RTRIM(cl.co_zon) AS co_zon, RTRIM(z.zon_des) AS zon_des, 
                               cl.contribu_e, cl.porc_esp,
                               RTRIM(f.co_us_in) AS co_us_in
                        FROM saFacturaVenta f
                        LEFT JOIN saCliente      cl ON f.co_cli  = cl.co_cli
                        LEFT JOIN saVendedor     v  ON f.co_ven  = v.co_ven
                        LEFT JOIN saCondicionPago cd ON f.co_cond = cd.co_cond
                        LEFT JOIN saZona         z  ON cl.co_zon = z.co_zon
                        WHERE LTRIM(RTRIM(f.doc_num)) = LTRIM(RTRIM(@doc_num))
                    `),
                    pool.request().input('doc_num', sql.VarChar, doc_num).query(`
                        SELECT r.reng_num, RTRIM(r.co_art) AS co_art, RTRIM(a.art_des) AS art_des,
                               RTRIM(a.co_lin) AS co_lin, RTRIM(a.co_subl) AS co_subl,
                               r.total_art AS cantidad, r.pendiente, RTRIM(r.co_alma) AS co_alma,
                               r.co_precio AS co_precio, r.prec_vta AS precio,
                               RTRIM(r.tipo_imp) AS tipo_imp, r.porc_imp, r.reng_neto AS total_renglon,
                               r.prec_vta_om, RTRIM(r.co_uni) AS co_uni, RTRIM(u.des_uni) AS unidad,
                               RTRIM(r.tipo_doc) AS tipo_doc, RTRIM(r.num_doc) AS num_doc, r.rowguid_doc
                        FROM saFacturaVentaReng r
                        LEFT JOIN saArticulo a ON r.co_art = a.co_art
                        LEFT JOIN saUnidad u ON r.co_uni = u.co_uni
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
            return res.status(404).json({ success: false, message: 'Factura no encontrada.' });

        res.status(200).json({ success: true, count: found.length, data: results.filter(r => r !== null) });

    } catch (error) {
        res.status(500).json({ success: false, message: 'Error al consultar Factura.', error: error.message });
    }
});

// --- ANULAR FACTURA ---
router.post('/:doc_num/anular', async (req, res) => {
    try {
        const { doc_num } = req.params;
        const { sede } = req.query;

        const outcome = await executeWrite(sede || null, req.sqlAuth, async (pool) => {
            const resH = await pool.request().input('doc_num', sql.VarChar, doc_num).query(
                `SELECT rowguid, anulado, RTRIM(co_us_in) AS co_us_in
                 FROM saFacturaVenta
                 WHERE LTRIM(RTRIM(doc_num)) = LTRIM(RTRIM(@doc_num))`
            );
            if (!resH.recordset.length) throw new Error('Factura no existe.');

            const { anulado } = resH.recordset[0];
            const isAnulada = !!anulado;
            if (isAnulada) {
                throw new Error(`La factura ${doc_num} ya está anulada.`);
            }

            // Fetch invoice lines to update stock and origin documents
            const resL = await pool.request().input('doc_num', sql.VarChar, doc_num).query(
                `SELECT reng_num, co_art, co_alma, co_uni, total_art, rowguid, RTRIM(tipo_doc) AS tipo_doc, RTRIM(num_doc) AS num_doc, rowguid_doc 
                 FROM saFacturaVentaReng 
                 WHERE LTRIM(RTRIM(doc_num)) = LTRIM(RTRIM(@doc_num))`
            );

            const transaction = new sql.Transaction(pool);
            await transaction.begin();

            try {
                const auditUser = (req.profitUser || 'API').substring(0, 10).toUpperCase();

                // 1. Anular cabecera de la factura
                await transaction.request()
                    .input('doc_num', sql.Char(20), padProfit(doc_num, 20))
                    .input('auditUser', sql.Char(6), padProfit(auditUser, 6))
                    .query(`
                        UPDATE saFacturaVenta
                        SET anulado = 1,
                            fe_us_mo = GETDATE(),
                            co_us_mo = @auditUser
                        WHERE LTRIM(RTRIM(doc_num)) = LTRIM(RTRIM(@doc_num))
                    `);

                // 2. Anular renglones de la factura (poner pendiente a 0)
                await transaction.request()
                    .input('doc_num', sql.Char(20), padProfit(doc_num, 20))
                    .input('auditUser', sql.Char(6), padProfit(auditUser, 6))
                    .query(`
                        UPDATE saFacturaVentaReng
                        SET pendiente = 0,
                            fe_us_mo = GETDATE(),
                            co_us_mo = @auditUser
                        WHERE LTRIM(RTRIM(doc_num)) = LTRIM(RTRIM(@doc_num))
                    `);

                // 3. Devolver stock al almacén (Sumar stock tipo 'ACT')
                console.log(`🧹 [AGENT] Devolviendo stock de ${resL.recordset.length} renglones de la factura...`);
                for (const line of resL.recordset) {
                    const rStock = new sql.Request(transaction);
                    rStock.input('sCo_Alma',              sql.Char(6),  line.co_alma);
                    rStock.input('sCo_Art',               sql.Char(30), line.co_art);
                    rStock.input('sCo_Uni',               sql.Char(6),  line.co_uni);
                    rStock.input('deCantidad',            sql.Decimal(18, 5), line.total_art);
                    rStock.input('sTipoStock',            sql.Char(4),  'ACT');
                    rStock.input('bSumarStock',           sql.Bit,      1); // Sumar stock (devolver)
                    rStock.input('bPermiteStockNegativo', sql.Bit,      1);
                    await rStock.execute('pStockActualizar');

                    // 4. Si la línea se originó de un Pedido de venta ('PEDI' o 'PED'), revertimos su pendiente
                    if ((line.tipo_doc === 'PEDI' || line.tipo_doc === 'PED') && line.rowguid_doc && line.num_doc) {
                        console.log(`📈 [AGENT] Revirtiendo ${line.total_art} al pendiente del pedido ${line.num_doc}`);
                        const rRevert = new sql.Request(transaction);
                        rRevert.input('qty', sql.Decimal(18, 5), line.total_art);
                        rRevert.input('rowguid_doc', sql.UniqueIdentifier, line.rowguid_doc);
                        rRevert.input('num_doc', sql.Char(20), padProfit(line.num_doc, 20));
                        rRevert.input('auditUser', sql.Char(6), padProfit(auditUser, 6));

                        await rRevert.query(`
                            UPDATE saPedidoVentaReng
                            SET pendiente = CASE WHEN pendiente + @qty > total_art THEN total_art ELSE pendiente + @qty END,
                                fe_us_mo = GETDATE(),
                                co_us_mo = @auditUser
                            WHERE rowguid = @rowguid_doc AND doc_num = @num_doc;

                            DECLARE @total_qty DECIMAL(18,5), @pending_qty DECIMAL(18,5);
                            SELECT @total_qty = SUM(total_art), @pending_qty = SUM(pendiente)
                            FROM saPedidoVentaReng
                            WHERE doc_num = @num_doc;

                            UPDATE saPedidoVenta
                            SET status = CASE 
                                WHEN @pending_qty = 0 THEN '2'
                                WHEN @pending_qty < @total_qty THEN '1'
                                ELSE '0'
                            END,
                            fe_us_mo = GETDATE(),
                            co_us_mo = @auditUser
                            WHERE doc_num = @num_doc;
                        `);
                    }
                }

                // 5. Anular el documento de venta correspondiente en saDocumentoVenta
                await transaction.request()
                    .input('doc_num', sql.Char(20), padProfit(doc_num, 20))
                    .query(`
                        UPDATE saDocumentoVenta
                        SET anulado = 1,
                            fe_us_mo = GETDATE()
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
        res.status(500).json({ success: false, message: 'Error al anular factura.', error: error.message });
    }
});

module.exports = router;
