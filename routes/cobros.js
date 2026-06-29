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
        const page  = parseInt(req.query.page)  || 1;
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
                           RTRIM(c.co_ven) AS co_ven, RTRIM(c.co_us_in) AS co_us_in
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
                           c.contribu_e, c.porc_esp, c.co_ven
                    FROM saDocumentoVenta d
                    INNER JOIN saCliente c ON d.co_cli = c.co_cli
                    WHERE ${whereSQL}
                    ORDER BY d.fec_emis DESC
                `);

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
                               r.rowguid, r.rowguid_reng_ori
                        FROM saCobroDocReng r
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
                    `),
                    pool.request().input('cob_num', sql.VarChar, cob_num).query(`
                        SELECT rn.reng_num, rn.rowguid_reng_cob, RTRIM(rn.co_islr) AS co_islr,
                               rn.monto, rn.monto_reten, rn.monto_obj, rn.porc_retn
                        FROM saCobroRentenReng rn
                        INNER JOIN saCobroDocReng cdr ON rn.rowguid_reng_cob = cdr.rowguid
                        WHERE LTRIM(RTRIM(cdr.cob_num)) = LTRIM(RTRIM(@cob_num))
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

        const defVen   = resVen.recordset[0]?.co_ven || '01';
        const defSucu  = resSucu.recordset[0]?.co_sucur || '01';
        const defCtaIE = resCtaIE.recordset[0]?.co_cta_ingr_egr || '01';

        const auditUser = (req.profitUser || req.sqlAuth?.user || 'API').substring(0, 10).toUpperCase();
        const tsDate    = new Date();

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
            rH.input('sCob_Num',      sql.Char(20),       padProfit(cobNum, 20));
            rH.input('sRecibo',       sql.Char(15),       cobNum);
            rH.input('sCo_cli',       sql.Char(16),       padProfit(data.co_cli, 16));
            rH.input('sCo_ven',       sql.Char(6),        padProfit(data.co_ven || defVen, 6));
            rH.input('sCo_Mone',      sql.Char(6),        padProfit(data.co_mone || 'US$', 6));
            rH.input('deTasa',        sql.Decimal(21, 8), Number(data.tasa || 1));
            rH.input('sdFecha',       sql.SmallDateTime,  tsDate);
            rH.input('bAnulado',      sql.Bit,            0);
            rH.input('deMonto',       sql.Decimal(18, 2), Number(data.monto));
            rH.input('sDis_cen',      sql.VarChar(sql.MAX), null);
            rH.input('sDescrip',      sql.VarChar(60),    (data.descrip || 'COBRO DE CLIENTE').substring(0, 60));
            rH.input('sCo_Us_In',     sql.Char(6),        padProfit(auditUser, 6));
            rH.input('sCo_Sucu_In',   sql.Char(6),        padProfit(sucuCode, 6));
            rH.input('sRevisado',     sql.Char(1),        null);
            rH.input('sTrasnfe',      sql.Char(1),        null);

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

            for (let i = 0; i < sortedRenglones.length; i++) {
                const line = sortedRenglones[i];
                const rengNum = i + 1;
                const docGuid = sql.UniqueIdentifier;
                
                // Determinar rowguid_reng_ori para notas de crédito/retenciones asociadas
                let parentGuid = null;
                if (!parentTypes.includes(line.co_tipo_doc.trim().toUpperCase())) {
                    const lookupKey = line.parent_doc ? line.parent_doc.trim() : line.nro_doc?.trim();
                    parentGuid = rengDocGuidMap.get(lookupKey);
                }

                const rR = new sql.Request(transaction);
                const rowGuidValue = new sql.UniqueIdentifier; // Se autogenera, pero lo resolveremos en base de datos usando NEWID() y devolviendo el output o generándolo en Node
                
                const guidResult = await transaction.request().query('SELECT NEWID() AS guid');
                const lineGuid = guidResult.recordset[0].guid;
                rengDocGuidMap.set(line.nro_doc?.trim(), lineGuid);

                await transaction.request()
                    .input('reng_num',                  sql.Int,              rengNum)
                    .input('cob_num',                   sql.Char(20),         padProfit(cobNum, 20))
                    .input('co_tipo_doc',               sql.Char(6),          padProfit(line.co_tipo_doc, 6))
                    .input('nro_doc',                   sql.Char(20),         padProfit(line.nro_doc, 20))
                    .input('mont_cob',                  sql.Decimal(18, 2),   Number(line.mont_cob))
                    .input('monto_retencion_iva',       sql.Decimal(18, 5),   Number(line.monto_retencion_iva || 0))
                    .input('monto_retencion',           sql.Decimal(18, 2),   Number(line.monto_retencion || 0))
                    .input('rowguid_reng_ori',          sql.UniqueIdentifier, parentGuid)
                    .input('co_sucu_in',                sql.Char(6),          padProfit(sucuCode, 6))
                    .input('co_us_in',                  sql.Char(6),          padProfit(auditUser, 6))
                    .input('rowguid',                   sql.UniqueIdentifier, lineGuid)
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

                // 3.1 Rebajar el saldo del documento en saDocumentoVenta
                // En un cobro, los montos aplicados (abono + retenciones) reducen el saldo del documento por cobrar
                const totalRebaje = Number(line.mont_cob) + Number(line.monto_retencion_iva || 0) + Number(line.monto_retencion || 0);
                await transaction.request()
                    .input('co_tipo_doc', sql.Char(6), padProfit(line.co_tipo_doc, 6))
                    .input('nro_doc',     sql.Char(20), padProfit(line.nro_doc, 20))
                    .input('rebaje',      sql.Decimal(18, 2), totalRebaje)
                    .input('user',        sql.Char(6), padProfit(auditUser, 6))
                    .query(`
                        UPDATE saDocumentoVenta
                        SET saldo = saldo - @rebaje,
                            fe_us_mo = GETDATE(),
                            co_us_mo = @user
                        WHERE LTRIM(RTRIM(co_tipo_doc)) = LTRIM(RTRIM(@co_tipo_doc))
                          AND LTRIM(RTRIM(nro_doc)) = LTRIM(RTRIM(@nro_doc))
                    `);

                insertedRenglones.push({
                    co_tipo_doc: line.co_tipo_doc,
                    nro_doc: line.nro_doc,
                    rowguid: lineGuid
                });
            }

            // 4. Insertar Formas de Pago (saCobroTPReng) y crear movimientos en Caja/Banco
            for (let i = 0; i < data.formas_pago.length; i++) {
                const tp = data.formas_pago[i];
                const rengNum = i + 1;
                let movNumC = null;
                let movNumB = null;

                if (tp.forma_pag === 'EF' || tp.forma_pag === 'TJ' || tp.forma_pag === 'CT') {
                    // Generar correlativo de movimiento de Caja (MOVC_NUM)
                    const resCorrCaja = await transaction.request().query(`
                        UPDATE saSerie
                        SET prox_n = prox_n + 1, fe_us_mo = GETDATE()
                        OUTPUT INSERTED.prox_n
                        WHERE co_serie = (
                            SELECT TOP 1 co_serie
                            FROM saConsecutivo
                            WHERE UPPER(LTRIM(RTRIM(co_consecutivo))) = 'MOVC_NUM'
                        )
                    `);
                    let corrCaja = resCorrCaja.recordset[0];
                    if (!corrCaja || !corrCaja.prox_n) {
                        throw new Error("No se pudo obtener el correlativo de movimiento de caja.");
                    }
                    movNumC = Number(corrCaja.prox_n).toString().padStart(10, '0');

                    // Crear Movimiento de Caja
                    const rMovC = new sql.Request(transaction);
                    rMovC.input('sMov_Num',          sql.Char(20),       padProfit(movNumC, 20));
                    rMovC.input('sdFecha',           sql.SmallDateTime,  tsDate);
                    rMovC.input('sDescrip',          sql.VarChar(60),    (`INGR. COBRO ${cobNum} - ${data.co_cli}`).substring(0, 60));
                    rMovC.input('sCod_Caja',         sql.Char(6),        padProfit(tp.cod_caja, 6));
                    rMovC.input('deTasa',            sql.Decimal(21, 8), Number(data.tasa || 1));
                    rMovC.input('sTipo_Mov',         sql.Char(2),        'I');
                    rMovC.input('sForma_Pag',        sql.Char(2),        tp.forma_pag);
                    rMovC.input('sNum_Pago',         sql.VarChar(20),    tp.num_doc ? tp.num_doc.substring(0, 20) : null);
                    rMovC.input('sCo_Ban',           sql.Char(6),        tp.co_ban ? padProfit(tp.co_ban, 6) : null);
                    rMovC.input('sCo_Tar',           sql.Char(6),        tp.co_tar ? padProfit(tp.co_tar, 6) : null);
                    rMovC.input('sCo_Cta_Ingr_Egr',  sql.Char(20),       padProfit(defCtaIE, 20));
                    rMovC.input('deMonto',           sql.Decimal(18, 2), Number(tp.mont_doc));
                    rMovC.input('bSaldo_Ini',        sql.Bit,            0);
                    rMovC.input('sOrigen',           sql.Char(3),        'COB');
                    rMovC.input('sDoc_Num',          sql.VarChar(20),    cobNum.substring(0, 20));
                    rMovC.input('sDep_Num',          sql.VarChar(20),    '');
                    rMovC.input('bAnulado',          sql.Bit,            0);
                    rMovC.input('bDepositado',       sql.Bit,            0);
                    rMovC.input('bConciliado',       sql.Bit,            0);
                    rMovC.input('bTransferido',      sql.Bit,            0);
                    rMovC.input('sdFecha_Che',       sql.SmallDateTime,  tsDate);
                    rMovC.input('sCo_Us_In',         sql.Char(6),        padProfit(auditUser, 6));
                    rMovC.input('sCo_Sucu_In',       sql.Char(6),        padProfit(sucuCode, 6));
                    rMovC.input('sRevisado',         sql.Char(1),        null);
                    rMovC.input('sTrasnfe',          sql.Char(1),        null);

                    await rMovC.execute('pInsertarMovimientoCaja');

                } else if (tp.forma_pag === 'TE' || tp.forma_pag === 'DP' || tp.forma_pag === 'CH' || tp.forma_pag === 'TP') {
                    // Generar correlativo de movimiento de Banco (MOVB_NUM)
                    const resCorrBanco = await transaction.request().query(`
                        UPDATE saSerie
                        SET prox_n = prox_n + 1, fe_us_mo = GETDATE()
                        OUTPUT INSERTED.prox_n
                        WHERE co_serie = (
                            SELECT TOP 1 co_serie
                            FROM saConsecutivo
                            WHERE UPPER(LTRIM(RTRIM(co_consecutivo))) = 'MOVB_NUM'
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
                    rMovB.input('sMov_Num',          sql.Char(20),       padProfit(movNumB, 20));
                    rMovB.input('sDescrip',          sql.VarChar(160),   (`INGR. COBRO ${cobNum} - ${data.co_cli}`).substring(0, 160));
                    rMovB.input('sCod_Cta',          sql.Char(6),        padProfit(tp.cod_cta, 6));
                    rMovB.input('sdFecha',           sql.SmallDateTime,  tsDate);
                    rMovB.input('deTasa',            sql.Decimal(21, 8), Number(data.tasa || 1));
                    rMovB.input('sTipo_Op',          sql.Char(2),        tipoOp);
                    rMovB.input('sDoc_Num',          sql.VarChar(20),    (tp.num_doc || '').substring(0, 20));
                    rMovB.input('deMonto',           sql.Decimal(18, 2), Number(tp.mont_doc));
                    rMovB.input('sCo_Cta_Ingr_Egr',  sql.Char(20),       padProfit(defCtaIE, 20));
                    rMovB.input('sOrigen',           sql.Char(3),        'COB');
                    rMovB.input('sCob_Pag',          sql.Char(20),       padProfit(cobNum, 20));
                    rMovB.input('deIDB',             sql.Decimal(18, 2), 0.00);
                    rMovB.input('sDep_Num',          sql.Char(20),       null);
                    rMovB.input('bAnulado',          sql.Bit,            0);
                    rMovB.input('bSaldo_Ini',        sql.Bit,            0);
                    rMovB.input('bConciliado',       sql.Bit,            0);
                    rMovB.input('bOri_Dep',          sql.Bit,            0);
                    rMovB.input('iDep_Con',          sql.Int,            0);
                    rMovB.input('sCod_IngBen',       sql.Char(6),        null);
                    rMovB.input('sdFecha_Che',       sql.SmallDateTime,  tsDate);
                    rMovB.input('sCo_Us_In',         sql.Char(6),        padProfit(auditUser, 6));
                    rMovB.input('sCo_Sucu_In',       sql.Char(6),        padProfit(sucuCode, 6));
                    rMovB.input('sRevisado',         sql.Char(1),        null);
                    rMovB.input('sTrasnfe',          sql.Char(1),        null);

                    await rMovB.execute('pInsertarMovimientoBanco');
                }

                // Insertar renglón de forma de pago del cobro
                await transaction.request()
                    .input('reng_num',    sql.Int,              rengNum)
                    .input('cob_num',     sql.Char(20),         padProfit(cobNum, 20))
                    .input('co_tar',      sql.Char(6),          tp.co_tar ? padProfit(tp.co_tar, 6) : null)
                    .input('co_ban',      sql.Char(6),          tp.co_ban ? padProfit(tp.co_ban, 6) : null)
                    .input('forma_pag',   sql.Char(2),          tp.forma_pag === 'TE' ? 'TP' : tp.forma_pag) // Profit nativamente usa 'TP' para Transferencia
                    .input('cod_cta',     sql.Char(6),          tp.cod_cta ? padProfit(tp.cod_cta, 6) : null)
                    .input('cod_caja',    sql.Char(6),          tp.cod_caja ? padProfit(tp.cod_caja, 6) : null)
                    .input('mov_num_c',   sql.Char(20),         movNumC ? padProfit(movNumC, 20) : null)
                    .input('mov_num_b',   sql.Char(20),         movNumB ? padProfit(movNumB, 20) : null)
                    .input('num_doc',     sql.Char(20),         tp.num_doc ? padProfit(tp.num_doc, 20) : null)
                    .input('mont_doc',    sql.Decimal(18, 2),   Number(tp.mont_doc))
                    .input('fecha_che',   sql.SmallDateTime,    tp.fecha_che ? new Date(tp.fecha_che) : tsDate)
                    .input('co_sucu_in',  sql.Char(6),          padProfit(sucuCode, 6))
                    .input('co_us_in',    sql.Char(6),          padProfit(auditUser, 6))
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
                        .input('reng_num',                  sql.Int,              i + 1)
                        .input('rowguid_reng_cob',          sql.UniqueIdentifier, lineGuid)
                        .input('rif_contribuyente',         sql.Char(10),         ret.rif_contribuyente ? ret.rif_contribuyente.substring(0, 10) : ' ')
                        .input('periodo_impositivo',        sql.Decimal(6),       Number(ret.periodo_impositivo))
                        .input('fecha_documento',           sql.SmallDateTime,    ret.fecha_documento ? new Date(ret.fecha_documento) : tsDate)
                        .input('tipo_documento',            sql.Char(4),          'FACT')
                        .input('rif_comprador',             sql.Char(10),         ret.rif_comprador ? ret.rif_comprador.substring(0, 10) : ' ')
                        .input('numero_documento',          sql.Char(20),         padProfit(ret.numero_documento, 20))
                        .input('numero_control_documento',  sql.Char(20),         padProfit(ret.numero_control_documento || '', 20))
                        .input('monto_documento',           sql.Decimal(15, 2),   Number(ret.monto_documento))
                        .input('base_imponible',            sql.Decimal(15, 2),   Number(ret.base_imponible))
                        .input('monto_ret_imp',             sql.Decimal(15, 2),   Number(ret.monto_ret_imp))
                        .input('numero_documento_afectado', sql.Char(20),         padProfit(ret.numero_documento_afectado, 20))
                        .input('num_comprobante',           sql.Char(14),         ret.num_comprobante.substring(0, 14))
                        .input('monto_excento',             sql.Decimal(15, 2),   Number(ret.monto_excento || 0))
                        .input('alicuota',                  sql.Decimal(5, 2),    Number(ret.alicuota))
                        .input('reten_tercero',             sql.Bit,              0)
                        .input('numero_expediente',         sql.Char(15),         ' ')
                        .input('co_us_in',                  sql.Char(6),          padProfit(auditUser, 6))
                        .input('co_sucu_in',                sql.Char(6),          padProfit(sucuCode, 6))
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
                        .input('reng_num',          sql.Int,              i + 1)
                        .input('rowguid_reng_cob',  sql.UniqueIdentifier, lineGuid)
                        .input('co_islr',           sql.Char(6),          padProfit(ret.co_islr, 6))
                        .input('monto',             sql.Decimal(18, 5),   Number(ret.monto))
                        .input('monto_reten',       sql.Decimal(18, 5),   Number(ret.monto_reten))
                        .input('monto_obj',         sql.Decimal(18, 5),   Number(ret.monto_obj))
                        .input('sustraendo',        sql.Decimal(18, 5),   Number(ret.sustraendo || 0))
                        .input('porc_retn',         sql.Decimal(18, 5),   Number(ret.porc_retn))
                        .input('co_us_in',          sql.Char(6),          padProfit(auditUser, 6))
                        .input('co_sucu_in',        sql.Char(6),          padProfit(sucuCode, 6))
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

module.exports = router;
