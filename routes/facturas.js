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
                           RTRIM(f.co_mone) AS co_mone, f.tasa, f.total_neto, f.monto_imp,
                           ISNULL(d.saldo, 0) AS saldo,
                           RTRIM(f.co_ven) AS co_ven, RTRIM(v.ven_des) AS ven_des, RTRIM(f.co_us_in) AS co_us_in, RTRIM(f.co_sucu_in) AS co_sucu_in
                    FROM saFacturaVenta f
                    LEFT JOIN saDocumentoVenta d ON LTRIM(RTRIM(f.doc_num)) = LTRIM(RTRIM(d.nro_doc)) AND LTRIM(RTRIM(d.co_tipo_doc)) = 'FACT'
                    LEFT JOIN saCliente cl ON f.co_cli = cl.co_cli
                    LEFT JOIN saVendedor v ON f.co_ven = v.co_ven
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
                               RTRIM(f.co_us_in) AS co_us_in, RTRIM(f.co_sucu_in) AS co_sucu_in
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

                    // 3.b. Restar de Stock por Despachar (DES)
                    const rStockDes = new sql.Request(transaction);
                    rStockDes.input('sCo_Alma',              sql.Char(6),  line.co_alma);
                    rStockDes.input('sCo_Art',               sql.Char(30), line.co_art);
                    rStockDes.input('sCo_Uni',               sql.Char(6),  line.co_uni);
                    rStockDes.input('deCantidad',            sql.Decimal(18, 5), line.total_art);
                    rStockDes.input('sTipoStock',            sql.Char(4),  'DES');
                    rStockDes.input('bSumarStock',           sql.Bit,      0); // Restar stock
                    rStockDes.input('bPermiteStockNegativo', sql.Bit,      1);
                    await rStockDes.execute('pStockActualizar');

                    // 3.c. Si venía de Pedido, volver a Comprometer (COM)
                    if ((line.tipo_doc === 'PCLI' || line.tipo_doc === 'PEDI' || line.tipo_doc === 'PED') && line.rowguid_doc && line.num_doc) {
                        console.log(`📈 [AGENT] Volviendo a comprometer stock (COM) de ${line.total_art} para el pedido ${line.num_doc}`);
                        const rStockCom = new sql.Request(transaction);
                        rStockCom.input('sCo_Alma',              sql.Char(6),  line.co_alma);
                        rStockCom.input('sCo_Art',               sql.Char(30), line.co_art);
                        rStockCom.input('sCo_Uni',               sql.Char(6),  line.co_uni);
                        rStockCom.input('deCantidad',            sql.Decimal(18, 5), line.total_art);
                        rStockCom.input('sTipoStock',            sql.Char(4),  'COM');
                        rStockCom.input('bSumarStock',           sql.Bit,      1); // Sumar stock
                        rStockCom.input('bPermiteStockNegativo', sql.Bit,      1);
                        await rStockCom.execute('pStockActualizar');
                    }

                    // 4. Si la línea se originó de un Pedido de venta ('PCLI', 'PEDI' o 'PED'), revertimos su pendiente
                    if ((line.tipo_doc === 'PCLI' || line.tipo_doc === 'PEDI' || line.tipo_doc === 'PED') && line.rowguid_doc && line.num_doc) {
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

// --- GUARDAR FACTURA DE VENTA ---
router.post('/', async (req, res) => {
    const data = req.body;
    console.log("📥 [AGENT] Recibiendo Factura de Venta (SAVE):", JSON.stringify({ ...data, renglones: data.renglones?.length }, null, 2));

    if (!data.co_cli || !data.renglones) {
        return res.status(400).json({ success: false, message: 'Campos obligatorios: co_cli, renglones' });
    }

    const outcome = await executeWrite(req.query.sede || null, req.sqlAuth, async (pool, srv) => {
        // 1. Cargar Catálogos y Parámetros Globales
        const [resMoneda, resUSD, resAlma, resVen, resCond, resSucu, resCli, resTax, resTasa, resCtaIE, resTran] = await Promise.all([
            pool.request().query(`SELECT TOP 1 RTRIM(g_moneda) AS g_moneda FROM par_emp`),
            pool.request().query(`SELECT TOP 1 RTRIM(co_mone)  AS co_mone   FROM saMoneda WHERE LTRIM(RTRIM(co_mone)) IN ('US$','USD','DOL','$','US') OR mone_des LIKE '%Dolar%'`),
            pool.request().query(`SELECT TOP 1 RTRIM(co_alma) AS co_alma FROM saAlmacen`),
            pool.request().query(`SELECT TOP 1 RTRIM(co_ven)  AS co_ven  FROM saVendedor`),
            pool.request().query(`SELECT TOP 1 RTRIM(co_cond) AS co_cond FROM saCondicionPago`),
            pool.request().query(`SELECT TOP 1 RTRIM(co_sucur) AS co_sucur FROM saSucursal`),
            pool.request().input('co_cli', sql.Char(16), data.co_cli).query(`SELECT RTRIM(co_mone) as co_mone, RTRIM(cond_pag) as cond_pag, RTRIM(co_ven) as co_ven, RTRIM(co_sucu_in) as co_sucu FROM saCliente WHERE co_cli = @co_cli`),
            pool.request().query(`SELECT TOP 1 RTRIM(tax_id) AS tax_id FROM saTax`),
            pool.request().query(`SELECT TOP 1 tasa_v FROM saTasa WHERE LTRIM(RTRIM(co_mone)) IN ('US$','USD','DOL','$','US') ORDER BY fecha DESC`),
            pool.request().query(`SELECT TOP 1 RTRIM(co_cta_ingr_egr) AS co_cta_ingr_egr FROM saCuentaIngEgr`),
            pool.request().query(`SELECT TOP 1 RTRIM(co_tran) AS co_tran FROM saTransporte`)
        ]);

        const cli = resCli.recordset[0] || {};
        const usdCode = resUSD.recordset[0]?.co_mone || 'US$';
        const defVen   = cli.co_ven   || resVen.recordset[0]?.co_ven     || '01';
        const defCond  = cli.cond_pag || resCond.recordset[0]?.co_cond   || '01';
        const defAlma  = resAlma.recordset[0]?.co_alma   || '01';
        const defSucu  = resSucu.recordset[0]?.co_sucur  || '01';
        const defCtaIE = resCtaIE.recordset[0]?.co_cta_ingr_egr || '01';
        const defTran  = resTran.recordset[0]?.co_tran || '01';
        const rawTax   = resTax.recordset[0]?.tax_id;
        const defTax   = rawTax ? rawTax.trim() : null;
        
        const auditUser = (req.profitUser || req.sqlAuth?.user || 'API').substring(0, 10).toUpperCase();
        const tsDate    = new Date();

        // 2. Resolver la tasa
        const currentTasa = resTasa.recordset[0]?.tasa_v || 1;
        let tasaDoc = Number(data.tasa || currentTasa);
        if (tasaDoc <= 1 && currentTasa > 1) {
            tasaDoc = currentTasa;
        }

        // 3. Calcular montos
        // La factura se guardará en USD, por lo tanto, los montos recibidos en el renglón (precio)
        // se asume que están en USD. Calculamos montos en Bs para guardar en campos locales de base.
        let totalBrutoBs = 0;
        let totalImpBs   = 0;
        
        data.renglones.forEach(item => {
            const qty = Number(item.cantidad || 0);
            const prcUSD = Number(item.precio || 0);
            const pImp = Number(item.porc_imp || 0);
            
            const prcBs = prcUSD * tasaDoc;
            const subBs = Math.round((qty * prcBs) * 100) / 100;
            const impBs = Math.round(((subBs * pImp) / 100) * 100) / 100;
            
            totalBrutoBs += subBs;
            totalImpBs   += impBs;
        });

        const igtfMontoDivisa = Number(data.igtf_monto_divisa || 0);
        const igtfBs = igtfMontoDivisa > 0 ? Math.round((igtfMontoDivisa * 0.03 * tasaDoc) * 100) / 100 : 0;

        const totalNetoBs = Math.round((totalBrutoBs + totalImpBs + igtfBs) * 100) / 100;

        // 4. Determinar Sucursal (co_sucu_in, co_sucu_mo) según requerimiento de IVA o sucursal forzada (force_sucu)
        const branchCodes = srv.profit_branch_codes || [];
        const defaultCodeObj = branchCodes.find(b => b.is_default === true) || branchCodes[0] || { code: defSucu };
        const nonDefaultCodeObj = branchCodes.find(b => b.is_default === false) || defaultCodeObj;

        let sucuCode;
        if (data.force_sucu) {
            sucuCode = data.force_sucu;
        } else {
            sucuCode = totalImpBs === 0 ? nonDefaultCodeObj.code : defaultCodeObj.code;
        }
        console.log(`🏢 [AGENT] Resolviendo sucursal. IVA = ${totalImpBs}, force_sucu = ${data.force_sucu || 'N/A'}. Sucu asignada = ${sucuCode} (Default = ${defaultCodeObj.code}, Non-Default = ${nonDefaultCodeObj.code})`);

        const transaction = new sql.Transaction(pool);
        await transaction.begin();

        try {
            // 5. Correlativo de Factura (DOC_VEN_FACT)
            const resCorr = await transaction.request().query(`
                UPDATE saSerie
                SET prox_n = prox_n + 1, fe_us_mo = GETDATE()
                OUTPUT INSERTED.prox_n, RTRIM(INSERTED.desde_a) as prefijo
                WHERE co_serie = (
                    SELECT TOP 1 co_serie
                    FROM saConsecutivo
                    WHERE UPPER(LTRIM(RTRIM(co_consecutivo))) = 'DOC_VEN_FACT'
                )
            `);
            let corrRow = resCorr.recordset[0];
            if (!corrRow || !corrRow.prox_n) {
                throw new Error("No se pudo obtener el correlativo de factura de venta.");
            }
            const proxN = Number(corrRow.prox_n || 0);
            const docNum = proxN.toString().padStart(10, '0');
            console.log(`✨ [AGENT] Nuevo número de factura generado: ${docNum}`);

            // 6. Insertar Cabecera de Factura
            const rH = new sql.Request(transaction);
            rH.input('sDoc_Num',          sql.Char(20),         padProfit(docNum, 20));
            rH.input('sDescrip',          sql.VarChar(60),      (data.descrip || 'FACTURA WEB').substring(0, 60));
            rH.input('sCo_Cli',           sql.Char(16),         padProfit(data.co_cli, 16));
            rH.input('sCo_Cta_Ingr_Egr',  sql.Char(20),         null);
            rH.input('sCo_Tran',          sql.Char(6),          padProfit(data.co_tran || defTran, 6));
            rH.input('sCo_Mone',          sql.Char(6),          padProfit(usdCode, 6));
            rH.input('sCo_Ven',           sql.Char(6),          padProfit(data.co_ven  || defVen, 6));
            rH.input('sCo_Cond',          sql.Char(6),          padProfit(data.co_cond || defCond, 6));
            rH.input('sdFec_Emis',        sql.SmallDateTime,    tsDate);
            rH.input('sdFec_Venc',        sql.SmallDateTime,    tsDate);
            rH.input('sdFec_Reg',         sql.SmallDateTime,    tsDate);
            rH.input('bAnulado',          sql.Bit,              0);
            rH.input('sStatus',           sql.Char(1),          '0'); // Sin procesar / pendiente
            rH.input('deTasa',            sql.Decimal(21, 8),   tasaDoc);
            rH.input('sN_Control',        sql.VarChar(20),      docNum);
            rH.input('sPorc_Desc_Glob',   sql.VarChar(15),      null);
            rH.input('deMonto_Desc_Glob', sql.Decimal(18, 2),   0);
            rH.input('sPorc_Reca',        sql.VarChar(15),      null);
            rH.input('deMonto_Reca',      sql.Decimal(18, 2),   0);
            rH.input('deSaldo',           sql.Decimal(18, 2),   totalNetoBs);
            rH.input('deTotal_Bruto',     sql.Decimal(18, 2),   totalBrutoBs);
            rH.input('deMonto_Imp',       sql.Decimal(18, 2),   totalImpBs);
            rH.input('deMonto_Imp2',      sql.Decimal(18, 2),   0);
            rH.input('deMonto_Imp3',      sql.Decimal(18, 2),   0);
            rH.input('deOtros1',          sql.Decimal(18, 2),   igtfBs);
            rH.input('deOtros2',          sql.Decimal(18, 2),   0);
            rH.input('deOtros3',          sql.Decimal(18, 2),   0);
            rH.input('deTotal_Neto',      sql.Decimal(18, 2),   totalNetoBs);
            rH.input('sDis_Cen',          sql.VarChar(sql.MAX), null);
            rH.input('sComentario',       sql.VarChar(sql.MAX), (data.comentario || 'Creado vía API').substring(0, 500));
            rH.input('sDir_Ent',          sql.VarChar(sql.MAX), null);
            rH.input('bContrib',          sql.Bit,              data.contrib ?? 1);
            rH.input('bImpresa',          sql.Bit,              0);
            rH.input('sSalestax',         sql.Char(8),          defTax);
            rH.input('sImpfis',           sql.VarChar(20),      null);
            rH.input('sImpfisfac',        sql.VarChar(20),      null);
            rH.input('sImp_nro_z',        sql.Char(15),         null);
            rH.input('bVen_Ter',          sql.Bit,              0);
            rH.input('sCo_Us_In',         sql.Char(6),          padProfit(auditUser, 6));
            rH.input('sCo_Sucu_In',       sql.Char(6),          padProfit(sucuCode, 6));
            rH.input('sRevisado',         sql.Char(1),          null);
            rH.input('sTrasnfe',          sql.Char(1),          null);
            rH.input('sMaquina',          sql.VarChar(60),      'SYNC2K');

            await rH.execute('pInsertarFacturaVenta');

            const guidRes = await transaction.request().input('doc_num', sql.Char(20), docNum).query('SELECT rowguid FROM saFacturaVenta WHERE doc_num = @doc_num');
            const rowguidDoc = guidRes.recordset[0]?.rowguid;

            // 7. Insertar Renglones de Factura
            for (let i = 0; i < data.renglones.length; i++) {
                const item = data.renglones[i];
                const qty = Number(item.cantidad || 0);
                const prcUSD = Number(item.precio || 0);
                const pImp = Number(item.porc_imp || 0);
                const coPrecio = String(item.co_precio || '01').trim().substring(0, 6);

                const prcBs = prcUSD * tasaDoc;
                const subBs = Math.round((qty * prcBs) * 100) / 100;
                const impBs = Math.round(((subBs * pImp) / 100) * 100) / 100;
                
                const finalUni = String(item.co_uni || 'UNI').trim();

                const rL = new sql.Request(transaction);
                rL.input('iReng_Num',          sql.Int,              i + 1);
                rL.input('sDoc_Num',           sql.Char(20),         padProfit(docNum, 20));
                rL.input('sCo_Art',            sql.Char(30),         padProfit(item.co_art, 30));
                rL.input('sDes_Art',           sql.VarChar(120),     (item.art_des || '').substring(0, 120));
                rL.input('sCo_Uni',            sql.Char(6),          padProfit(finalUni, 6));
                rL.input('sSco_Uni',           sql.Char(6),          padProfit(finalUni, 6));
                rL.input('sCo_Alma',           sql.Char(6),          padProfit(item.co_alma || defAlma, 6));
                rL.input('sCo_Precio',         sql.Char(6),          padProfit(coPrecio || '01', 6));
                rL.input('sTipo_Imp',          sql.Char(1),          item.tipo_imp || '1');
                rL.input('sTipo_Imp2',         sql.Char(1),          null);
                rL.input('sTipo_Imp3',         sql.Char(1),          null);
                rL.input('deTotal_Art',        sql.Decimal(18, 5),   qty);
                rL.input('deSTotal_Art',       sql.Decimal(18, 5),   0);
                rL.input('dePrec_Vta',         sql.Decimal(18, 5),   prcBs);
                rL.input('sPorc_Desc',         sql.VarChar(15),      null);
                rL.input('deMonto_Desc',       sql.Decimal(18, 5),   0);
                rL.input('deReng_Neto',        sql.Decimal(18, 2),   subBs);
                rL.input('dePendiente',        sql.Decimal(18, 5),   qty); // Pendiente igual a cantidad
                rL.input('dePendiente2',       sql.Decimal(18, 5),   0);
                rL.input('deMonto_Desc_Glob',  sql.Decimal(18, 5),   0);
                rL.input('deMonto_reca_Glob',  sql.Decimal(18, 5),   0);
                rL.input('deOtros1_glob',      sql.Decimal(18, 5),   0);
                rL.input('deOtros2_glob',      sql.Decimal(18, 5),   0);
                rL.input('deOtros3_glob',      sql.Decimal(18, 5),   0);
                rL.input('deMonto_imp_afec_glob',  sql.Decimal(18, 5), 0);
                rL.input('deMonto_imp2_afec_glob', sql.Decimal(18, 5), 0);
                rL.input('deMonto_imp3_afec_glob', sql.Decimal(18, 5), 0);
                
                const tipoDocVal = (item.tipo_doc && String(item.tipo_doc).trim()) ? String(item.tipo_doc).trim().toUpperCase() : null;
                const numDocVal = (item.num_doc && String(item.num_doc).trim()) ? String(item.num_doc).trim() : null;
                const rowguidDocVal = (item.rowguid_doc && String(item.rowguid_doc).trim()) ? String(item.rowguid_doc).trim() : null;

                rL.input('sTipo_Doc',          sql.Char(4),          tipoDocVal ? padProfit(tipoDocVal, 4) : null);
                rL.input('gRowguid_Doc',       sql.UniqueIdentifier, rowguidDocVal || null);
                rL.input('sNum_Doc',           sql.VarChar(20),      numDocVal ? padProfit(numDocVal, 20) : null);
                rL.input('dePorc_Imp',         sql.Decimal(18, 5),   pImp);
                rL.input('dePorc_Imp2',        sql.Decimal(18, 5),   0);
                rL.input('dePorc_Imp3',        sql.Decimal(18, 5),   0);
                rL.input('deMonto_Imp',        sql.Decimal(18, 5),   impBs);
                rL.input('deMonto_Imp2',       sql.Decimal(18, 5),   0);
                rL.input('deMonto_Imp3',       sql.Decimal(18, 5),   0);
                rL.input('deOtros',            sql.Decimal(18, 5),   0);
                rL.input('deTotal_Dev',        sql.Decimal(18, 5),   0);
                rL.input('deMonto_Dev',        sql.Decimal(18, 5),   0);
                rL.input('sComentario',        sql.VarChar(sql.MAX),  '');
                rL.input('sDis_Cen',           sql.VarChar(sql.MAX),  null);
                rL.input('sCo_Sucu_In',        sql.Char(6),           padProfit(sucuCode, 6));
                rL.input('sCo_Us_In',          sql.Char(6),           padProfit(auditUser, 6));
                rL.input('sREVISADO',          sql.Char(1),           null);
                rL.input('sTRASNFE',           sql.Char(1),           null);
                rL.input('sMaquina',           sql.VarChar(60),      'SYNC2K');
                await rL.execute('pInsertarRenglonesFacturaVenta');

                // Forzar prec_vta_om
                await transaction.request()
                    .input('om', sql.Decimal(18, 5), prcUSD)
                    .input('doc', sql.Char(20), padProfit(docNum, 20))
                    .input('reng', sql.Int, i + 1)
                    .query(`UPDATE saFacturaVentaReng SET prec_vta_om = @om WHERE doc_num = @doc AND reng_num = @reng`);

                // 8. Descontar del Pedido de Origen
                if ((tipoDocVal === 'PCLI' || tipoDocVal === 'PEDI' || tipoDocVal === 'PED') && rowguidDocVal && numDocVal) {
                    console.log(`📉 [AGENT] Descontando ${qty} del pendiente en pedido de venta ${numDocVal}, renglón guid: ${rowguidDocVal}`);
                    const rOrder = new sql.Request(transaction);
                    rOrder.input('qty', sql.Decimal(18, 5), qty);
                    rOrder.input('rowguid_doc', sql.UniqueIdentifier, rowguidDocVal);
                    rOrder.input('num_doc', sql.Char(20), padProfit(numDocVal, 20));
                    rOrder.input('auditUser', sql.Char(6), padProfit(auditUser, 6));
                    
                    await rOrder.query(`
                        UPDATE saPedidoVentaReng
                        SET pendiente = CASE WHEN pendiente >= @qty THEN pendiente - @qty ELSE 0 END,
                            fe_us_mo = GETDATE(),
                            co_us_mo = @auditUser
                        WHERE rowguid = @rowguid_doc AND doc_num = @num_doc;

                        DECLARE @total_qty DECIMAL(18,5), @pending_qty DECIMAL(18,5);
                        SELECT @total_qty = SUM(total_art), @pending_qty = SUM(pendiente)
                        FROM saPedidoVentaReng
                        WHERE doc_num = @num_doc;

                        UPDATE saPedidoVenta
                        SET status = CASE 
                            WHEN @pending_qty = 0 THEN '2' -- Procesado totalmente
                            WHEN @pending_qty < @total_qty THEN '1' -- Procesado parcial
                            ELSE '0' -- Sin procesar
                        END,
                        fe_us_mo = GETDATE(),
                        co_us_mo = @auditUser
                        WHERE doc_num = @num_doc;
                    `);
                }

                // 9. Actualizar Stock Físico (Restar de Almacén tipo 'ACT')
                const rStock = new sql.Request(transaction);
                rStock.input('sCo_Alma',              sql.Char(6),  padProfit(item.co_alma || defAlma, 6));
                rStock.input('sCo_Art',               sql.Char(30), padProfit(item.co_art, 30));
                rStock.input('sCo_Uni',               sql.Char(6),  padProfit(finalUni, 6));
                rStock.input('deCantidad',            sql.Decimal(18, 5), qty);
                rStock.input('sTipoStock',            sql.Char(4),  'ACT');
                rStock.input('bSumarStock',           sql.Bit,      0); // Restar stock
                rStock.input('bPermiteStockNegativo', sql.Bit,      1);
                await rStock.execute('pStockActualizar');

                // 9.b. Sumar a Stock por Despachar (DES)
                const rStockDes = new sql.Request(transaction);
                rStockDes.input('sCo_Alma',              sql.Char(6),  padProfit(item.co_alma || defAlma, 6));
                rStockDes.input('sCo_Art',               sql.Char(30), padProfit(item.co_art, 30));
                rStockDes.input('sCo_Uni',               sql.Char(6),  padProfit(finalUni, 6));
                rStockDes.input('deCantidad',            sql.Decimal(18, 5), qty);
                rStockDes.input('sTipoStock',            sql.Char(4),  'DES');
                rStockDes.input('bSumarStock',           sql.Bit,      1); // Sumar stock
                rStockDes.input('bPermiteStockNegativo', sql.Bit,      1);
                await rStockDes.execute('pStockActualizar');

                // 9.c. Restar de Stock Comprometido (COM) si viene de un Pedido
                if ((tipoDocVal === 'PCLI' || tipoDocVal === 'PEDI' || tipoDocVal === 'PED') && rowguidDocVal && numDocVal) {
                    console.log(`📉 [AGENT] Restando ${qty} de stock comprometido (COM) por pedido de venta ${numDocVal}`);
                    const rStockCom = new sql.Request(transaction);
                    rStockCom.input('sCo_Alma',              sql.Char(6),  padProfit(item.co_alma || defAlma, 6));
                    rStockCom.input('sCo_Art',               sql.Char(30), padProfit(item.co_art, 30));
                    rStockCom.input('sCo_Uni',               sql.Char(6),  padProfit(finalUni, 6));
                    rStockCom.input('deCantidad',            sql.Decimal(18, 5), qty);
                    rStockCom.input('sTipoStock',            sql.Char(4),  'COM');
                    rStockCom.input('bSumarStock',           sql.Bit,      0); // Restar stock
                    rStockCom.input('bPermiteStockNegativo', sql.Bit,      1);
                    await rStockCom.execute('pStockActualizar');
                }
            }

            // 10. Insertar Documento de Venta en saDocumentoVenta
            const rDoc = new sql.Request(transaction);
            rDoc.input('sCo_Tipo_Doc', sql.Char(6), 'FACT');
            rDoc.input('sNro_Doc', sql.Char(20), padProfit(docNum, 20));
            rDoc.input('sCo_Cli', sql.Char(16), padProfit(data.co_cli, 16));
            rDoc.input('sCo_Ven', sql.Char(6), padProfit(data.co_ven || defVen, 6));
            rDoc.input('sCo_Mone', sql.Char(6), padProfit(usdCode, 6));
            rDoc.input('sMov_Ban', sql.Char(20), null);
            rDoc.input('sCo_Cta_Ingr_Egr', sql.Char(20), null);
            rDoc.input('deTasa', sql.Decimal(21, 8), tasaDoc);
            rDoc.input('sObserva', sql.VarChar(sql.MAX), (data.descrip || 'FACTURA DESDE PORTAL WEB').substring(0, 60));
            rDoc.input('sdFec_Reg', sql.SmallDateTime, tsDate);
            rDoc.input('sdFec_Emis', sql.SmallDateTime, tsDate);
            rDoc.input('sdFec_Venc', sql.SmallDateTime, tsDate);
            rDoc.input('bAnulado', sql.Bit, 0);
            rDoc.input('bAut', sql.Bit, 1);
            rDoc.input('bContrib', sql.Bit, data.contrib ?? 1);
            rDoc.input('sDoc_Orig', sql.Char(6), 'FACT');
            rDoc.input('sNro_Orig', sql.VarChar(20), padProfit(docNum, 20));
            rDoc.input('sNro_Che', sql.VarChar(20), null);
            rDoc.input('deMonto_Imp', sql.Decimal(18, 2), totalImpBs);
            rDoc.input('deSaldo', sql.Decimal(18, 2), totalNetoBs);
            rDoc.input('deTotal_Bruto', sql.Decimal(18, 2), totalBrutoBs);
            rDoc.input('deMonto_Desc_Glob', sql.Decimal(18, 2), 0);
            rDoc.input('sPorc_Desc_Glob', sql.VarChar(15), null);
            rDoc.input('sPorc_Reca', sql.VarChar(15), null);
            rDoc.input('deMonto_Reca', sql.Decimal(18, 2), 0);
            rDoc.input('deTotal_Neto', sql.Decimal(18, 2), totalNetoBs);
            rDoc.input('deMonto_Imp2', sql.Decimal(18, 2), 0);
            rDoc.input('deMonto_Imp3', sql.Decimal(18, 2), 0);
            
            const tipoImp = data.renglones[0]?.tipo_imp || '1';
            rDoc.input('sTipo_Imp', sql.Char(1), tipoImp);
            rDoc.input('iTipo_Origen', sql.Int, null);
            
            const porcImp = data.renglones[0]?.porc_imp || 16;
            rDoc.input('dePorc_Imp', sql.Decimal(18, 5), porcImp);
            rDoc.input('dePorc_Imp2', sql.Decimal(18, 5), 0);
            rDoc.input('dePorc_Imp3', sql.Decimal(18, 5), 0);
            rDoc.input('sNum_Comprobante', sql.Char(14), null);
            rDoc.input('sN_Control', sql.VarChar(20), docNum);
            rDoc.input('sDis_Cen', sql.VarChar(sql.MAX), null);
            rDoc.input('deComis1', sql.Decimal(18, 2), 0);
            rDoc.input('deComis2', sql.Decimal(18, 2), 0);
            rDoc.input('deComis3', sql.Decimal(18, 2), 0);
            rDoc.input('deComis4', sql.Decimal(18, 2), 0);
            rDoc.input('deComis5', sql.Decimal(18, 2), 0);
            rDoc.input('deComis6', sql.Decimal(18, 2), 0);
            rDoc.input('deAdicional', sql.Decimal(18, 2), 0);
            rDoc.input('sSalestax', sql.Char(8), defTax);
            rDoc.input('bVen_Ter', sql.Bit, 0);
            rDoc.input('sImpfis', sql.VarChar(20), null);
            rDoc.input('sImpfisfac', sql.VarChar(15), null);
            rDoc.input('sImp_nro_z', sql.Char(15), null);
            rDoc.input('deOtros1', sql.Decimal(18, 2), igtfBs);
            rDoc.input('deOtros2', sql.Decimal(18, 2), 0);
            rDoc.input('deOtros3', sql.Decimal(18, 2), 0);
            rDoc.input('sCampo1', sql.VarChar(60), null);
            rDoc.input('sCampo2', sql.VarChar(60), null);
            rDoc.input('sCampo3', sql.VarChar(60), null);
            rDoc.input('sCampo4', sql.VarChar(60), null);
            rDoc.input('sCampo5', sql.VarChar(60), null);
            rDoc.input('sCampo6', sql.VarChar(60), null);
            rDoc.input('sCampo7', sql.VarChar(60), null);
            rDoc.input('sCampo8', sql.VarChar(60), null);
            rDoc.input('sRevisado', sql.Char(1), null);
            rDoc.input('sTrasnfe', sql.Char(1), null);
            rDoc.input('sCo_Sucu_In', sql.Char(6), padProfit(sucuCode, 6));
            rDoc.input('sCo_Us_In', sql.Char(6), padProfit(auditUser, 6));
            rDoc.input('sMaquina', sql.VarChar(60), 'SYNC2K');

            const resDoc = await rDoc.execute('pInsertarDocumentoVenta');

            // Insertar información de IGTF si corresponde
            if (igtfBs > 0) {
                const docGuid = resDoc.recordset[0]?.rowguid;
                if (docGuid) {
                    await transaction.request()
                        .input('rowguid', sql.UniqueIdentifier, docGuid)
                        .input('base_imponible', sql.Decimal(18, 2), Math.round((igtfMontoDivisa * tasaDoc) * 100) / 100)
                        .input('porc_aplic', sql.Decimal(18, 2), 3.00)
                        .query(`
                            INSERT INTO saDocumentoVentaInfoIGTF (rowguid, base_imponible, porc_aplic)
                            VALUES (@rowguid, @base_imponible, @porc_aplic)
                        `);
                }
            }

            await transaction.commit();
            return { doc_num: docNum, success: true, message: 'Factura guardada exitosamente en base de datos.' };
        } catch (err) {
            if (transaction._aborted === false) await transaction.rollback();
            throw err;
        }
    });

    return writeResponse(res, outcome);
});

module.exports = router;
