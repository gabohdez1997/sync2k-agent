const express = require('express');
const router = express.Router();
const { sql, getPool, getServers } = require('../db');
const { executeWrite, writeResponse, paginatedResponse } = require('../helpers/multiSede');

/**
 * @swagger
 * tags:
 *   name: Pedidos
 *   description: Gestión de pedidos de venta (Encabezados y Renglones)
 */

// ────────────────────────────────────────────────────────────────────────────
// 1. GET /api/v1/pedidos — Listado paginado desde todas las sedes
// ────────────────────────────────────────────────────────────────────────────
/**
 * @swagger
 * /api/v1/pedidos:
 *   get:
 *     summary: Obtener listado paginado de pedidos de venta
 *     tags: [Pedidos]
 *     parameters:
 *       - in: query
 *         name: page
 *         schema: { type: integer, default: 1 }
 *       - in: query
 *         name: limit
 *         schema: { type: integer, default: 10 }
 *       - in: query
 *         name: sede
 *         schema: { type: string }
 *         description: ID de la sede para filtrar
 *     responses:
 *       200:
 *         description: Listado de pedidos
 */
router.get('/', async (req, res) => {
    try {
        const page  = parseInt(req.query.page)  || 1;
        const limit = parseInt(req.query.limit) || 10;
        const { sede } = req.query;
        const servers = getServers();
        const targets = sede ? servers.filter(s => s.id === sede) : servers;

        if (targets.length === 0)
            return res.status(404).json({ success: false, message: `Sede "${sede}" no encontrada.` });

        const allData = await Promise.all(targets.map(async (srv) => {
            try {
                const pool = await getPool(srv.id);
                const [result, resTasa] = await Promise.all([
                    pool.request().query(`
                        SELECT RTRIM(p.doc_num) AS doc_num, RTRIM(p.descrip) AS descrip,
                               RTRIM(p.co_cli)  AS co_cli,  RTRIM(c.cli_des) AS cli_des,
                               p.fec_emis, p.fec_venc, p.status, p.anulado,
                               RTRIM(p.co_cond) AS co_cond, RTRIM(p.co_mone) AS co_mone,
                               p.tasa AS tasa_doc, p.total_neto, p.saldo
                        FROM saPedidoVenta p
                        LEFT JOIN saCliente c ON p.co_cli = c.co_cli
                        ORDER BY p.fec_emis DESC, p.doc_num DESC
                    `),
                    pool.request().query(`SELECT TOP 1 tasa_v AS tasa_bcv FROM saTasa
                                          WHERE LTRIM(RTRIM(co_mone)) IN ('US$','USD') ORDER BY fecha DESC`)
                ]);
                const tasa_bcv = resTasa.recordset[0]?.tasa_bcv || null;
                return result.recordset.map(p => ({ ...p, tasa_bcv, sede_id: srv.id, sede_nombre: srv.name }));
            } catch (e) { return []; }
        }));

        const combined = [].concat(...allData);
        combined.sort((a, b) => new Date(b.fec_emis) - new Date(a.fec_emis) || b.doc_num.localeCompare(a.doc_num));
        return paginatedResponse(res, combined, page, limit);

    } catch (error) {
        res.status(500).json({ success: false, message: 'Error al consultar pedidos.', error: error.message });
    }
});

// ────────────────────────────────────────────────────────────────────────────
// 2. GET /api/v1/pedidos/:doc_num — Detalle del pedido desde todas las sedes
// ────────────────────────────────────────────────────────────────────────────
/**
 * @swagger
 * /api/v1/pedidos/{doc_num}:
 *   get:
 *     summary: Obtener detalle completo de un pedido (incluye renglones)
 *     tags: [Pedidos]
 *     parameters:
 *       - in: path
 *         name: doc_num
 *         required: true
 *         schema: { type: string }
 *       - in: query
 *         name: sede
 *         schema: { type: string }
 *     responses:
 *       200:
 *         description: Detalle del pedido
 *       404:
 *         description: Pedido no encontrado
 */
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
                const pool = await getPool(srv.id);

                const [resEnc, resReng, resTasa] = await Promise.all([
                    pool.request().input('doc_num', sql.VarChar, doc_num).query(`
                        SELECT RTRIM(p.doc_num) AS doc_num, RTRIM(p.descrip) AS descrip,
                               RTRIM(p.co_cli)  AS co_cli,  RTRIM(c.cli_des) AS cli_des,
                               RTRIM(p.co_ven)  AS co_ven,  RTRIM(v.ven_des)  AS ven_des,
                               RTRIM(p.co_cond) AS co_cond, RTRIM(cd.cond_des) AS cond_des,
                               p.fec_emis, p.fec_venc, p.status, p.anulado,
                               RTRIM(p.co_mone) AS co_mone, p.tasa AS tasa_doc,
                               p.total_bruto, p.monto_imp, p.total_neto, p.saldo,
                               RTRIM(p.comentario) AS comentario
                        FROM saPedidoVenta p
                        LEFT JOIN saCliente      c  ON p.co_cli  = c.co_cli
                        LEFT JOIN saVendedor     v  ON p.co_ven  = v.co_ven
                        LEFT JOIN saCondicionPago cd ON p.co_cond = cd.co_cond
                        WHERE LTRIM(RTRIM(p.doc_num)) = LTRIM(RTRIM(@doc_num))
                    `),
                    pool.request().input('doc_num', sql.VarChar, doc_num).query(`
                        SELECT r.reng_num, RTRIM(r.co_art) AS co_art, RTRIM(a.art_des) AS art_des,
                               r.total_art AS cantidad, RTRIM(r.co_alma) AS co_alma,
                               RTRIM(r.co_precio) AS co_precio, r.prec_vta AS precio,
                               RTRIM(r.tipo_imp) AS tipo_imp, r.reng_neto AS total_renglon
                        FROM saPedidoVentaReng r
                        LEFT JOIN saArticulo a ON r.co_art = a.co_art
                        WHERE LTRIM(RTRIM(r.doc_num)) = LTRIM(RTRIM(@doc_num))
                        ORDER BY r.reng_num
                    `),
                    pool.request().query(`SELECT TOP 1 tasa_v AS tasa_bcv FROM saTasa
                                          WHERE LTRIM(RTRIM(co_mone)) IN ('US$','USD') ORDER BY fecha DESC`)
                ]);

                if (!resEnc.recordset.length) return null;
                const tasa_bcv = resTasa.recordset[0]?.tasa_bcv || null;
                return { ...resEnc.recordset[0], tasa_bcv, renglones: resReng.recordset, sede_id: srv.id, sede_nombre: srv.name };
            } catch (e) {
                return { sede_id: srv.id, sede_nombre: srv.name, error: e.message };
            }
        }));

        const found = results.filter(r => r && !r.error);
        if (!found.length)
            return res.status(404).json({ success: false, message: 'Pedido no encontrado.' });

        res.status(200).json({ success: true, count: found.length, data: results.filter(r => r !== null) });

    } catch (error) {
        res.status(500).json({ success: false, message: 'Error al consultar pedido.', error: error.message });
    }
});

// ────────────────────────────────────────────────────────────────────────────
// 3. POST /api/v1/pedidos — Crear pedido (targeted o broadcast)
// ────────────────────────────────────────────────────────────────────────────
/**
 * @swagger
 * /api/v1/pedidos:
 *   post:
 *     summary: Crear un nuevo pedido de venta
 *     tags: [Pedidos]
 *     parameters:
 *       - in: query
 *         name: sede
 *         schema: { type: string }
 *     requestBody:
 *       required: true
 *       content:
 *         application/json:
 *           schema:
 *             type: object
 *             required: [co_cli, renglones]
 *             properties:
 *               co_cli: { type: string }
 *               descrip: { type: string }
 *               comentario: { type: string }
 *               renglones:
 *                 type: array
 *                 items:
 *                   type: object
 *                   required: [co_art, cantidad, precio]
 *                   properties:
 *                     co_art: { type: string }
 *                     cantidad: { type: number }
 *                     precio: { type: number }
 *                     co_alma: { type: string }
 *     responses:
 *       201:
 *         description: Pedido creado
 */
router.post('/', async (req, res) => {
    const data = req.body;
    if (!data.co_cli)
        return res.status(400).json({ success: false, message: 'Campo obligatorio: co_cli' });

    const outcome = await executeWrite(req.query.sede || null, async (pool) => {
        const [resTasa, resMoneda, resAlma, resVen, resCond] = await Promise.all([
            pool.request().query(`SELECT TOP 1 tasa_v FROM saTasa WHERE LTRIM(RTRIM(co_mone)) IN ('US$','USD') ORDER BY fecha DESC`),
            pool.request().query(`SELECT TOP 1 RTRIM(g_moneda) AS g_moneda FROM par_emp`),
            pool.request().query(`SELECT TOP 1 RTRIM(co_alma) AS co_alma FROM saAlmacen`),
            pool.request().query(`SELECT TOP 1 RTRIM(co_ven)  AS co_ven  FROM saVendedor`),
            pool.request().query(`SELECT TOP 1 RTRIM(co_cond) AS co_cond FROM saCondicionPago`)
        ]);

        const tasa     = resTasa.recordset[0]?.tasa_v || 1;
        const defMone  = resMoneda.recordset[0]?.g_moneda || 'BS';
        const defAlma  = resAlma.recordset[0]?.co_alma   || '01';
        const defVen   = resVen.recordset[0]?.co_ven     || '01';
        const defCond  = resCond.recordset[0]?.co_cond   || '01';

        const renglones   = data.renglones || [];
        const totalBruto  = renglones.reduce((s, r) => s + (parseFloat(r.cantidad)||0) * (parseFloat(r.precio)||0), 0);
        const docNum      = 'API-' + Date.now().toString().slice(-8);
        const tsDate      = new Date();
        const transaction = new sql.Transaction(pool);
        await transaction.begin();

        try {
            const rH = new sql.Request(transaction);
            rH.input('sDoc_Num',         sql.Char(20),         docNum);
            rH.input('sDescrip',         sql.VarChar(60),      data.descrip || 'Pedido API');
            rH.input('sCo_Cli',          sql.Char(16),         data.co_cli);
            rH.input('sCo_Tran',         sql.Char(6),          data.co_tran || '01');
            rH.input('sCo_Mone',         sql.Char(6),          data.co_mone || defMone);
            rH.input('sCo_Cta_Ingr_Egr', sql.Char(20),         null);
            rH.input('sCo_Ven',          sql.Char(6),          data.co_ven  || defVen);
            rH.input('sCo_Cond',         sql.Char(6),          data.co_cond || defCond);
            rH.input('sdFec_Emis',       sql.SmallDateTime,    tsDate);
            rH.input('sdFec_Venc',       sql.SmallDateTime,    tsDate);
            rH.input('sdFec_Reg',        sql.SmallDateTime,    tsDate);
            rH.input('bAnulado',         sql.Bit,              0);
            rH.input('sStatus',          sql.Char(1),          '0');
            rH.input('deTasa',           sql.Decimal(18,5),    tasa);
            rH.input('deSaldo',          sql.Decimal(18,5),    totalBruto);
            rH.input('deTotal_Bruto',    sql.Decimal(18,5),    totalBruto);
            rH.input('deMonto_Imp',      sql.Decimal(18,5),    0);
            rH.input('deMonto_Imp2',     sql.Decimal(18,5),    0);
            rH.input('deMonto_Imp3',     sql.Decimal(18,5),    0);
            rH.input('deOtros1',         sql.Decimal(18,5),    0);
            rH.input('deOtros2',         sql.Decimal(18,5),    0);
            rH.input('deOtros3',         sql.Decimal(18,5),    0);
            rH.input('deTotal_Neto',     sql.Decimal(18,5),    totalBruto);
            rH.input('sComentario',      sql.VarChar(sql.MAX), data.comentario || 'Sync2k API');
            rH.input('bContrib',         sql.Bit,              1);
            rH.input('bImpresa',         sql.Bit,              0);
            rH.input('bVen_Ter',         sql.Bit,              0);
            rH.input('sCo_Us_In',        sql.Char(6),          '999');
            rH.input('sMaquina',         sql.VarChar(60),      'SYNC2K');
            rH.input('sRevisado',        sql.Char(1),          '0');
            rH.input('sTrasnfe',         sql.Char(1),          '0');
            await rH.execute('pInsertarPedidoVenta');

            // Obtener GUID del encabezado
            const gRes = await new sql.Request(transaction)
                .input('doc_num', sql.VarChar, docNum)
                .query('SELECT rowguid FROM saPedidoVenta WHERE LTRIM(RTRIM(doc_num)) = @doc_num');
            const rowguidDoc = gRes.recordset[0].rowguid;

            // Insertar renglones
            for (let i = 0; i < renglones.length; i++) {
                const item = renglones[i];
                const q    = parseFloat(item.cantidad) || 0;
                const p    = parseFloat(item.precio)   || 0;
                const rL   = new sql.Request(transaction);
                rL.input('iReng_Num',    sql.Int,              i + 1);
                rL.input('sDoc_Num',     sql.Char(20),         docNum);
                rL.input('sCo_Art',      sql.Char(30),         item.co_art);
                rL.input('sCo_Uni',      sql.Char(6),          'UNI');
                rL.input('sCo_Alma',     sql.Char(6),          item.co_alma || defAlma);
                rL.input('sCo_Precio',   sql.Char(6),          '01');
                rL.input('sTipo_Imp',    sql.Char(1),          '1');
                rL.input('deTotal_Art',  sql.Decimal(18,5),    q);
                rL.input('deSTotal_Art', sql.Decimal(18,5),    0);
                rL.input('dePrec_Vta',   sql.Decimal(18,5),    p);
                rL.input('deMonto_Desc', sql.Decimal(18,5),    0);
                rL.input('deReng_Neto',  sql.Decimal(18,5),    q * p);
                rL.input('dePendiente',  sql.Decimal(18,5),    q);
                rL.input('dePendiente2', sql.Decimal(18,5),    0);
                rL.input('sTipo_Doc',    sql.Char(4),          'PVEN');
                rL.input('gRowguid_Doc', sql.UniqueIdentifier, rowguidDoc);
                rL.input('sNum_Doc',     sql.VarChar(20),      docNum);
                rL.input('sCo_Us_In',    sql.Char(6),          '999');
                rL.input('sMaquina',     sql.VarChar(60),      'SYNC2K');
                rL.input('sREVISADO',    sql.Char(1),          '0');
                rL.input('sTRASNFE',     sql.Char(1),          '0');
                await rL.execute('pInsertarRenglonesPedidoVenta');
            }

            await transaction.commit();
            return { doc_num: docNum, renglones_insertados: renglones.length };
        } catch (err) {
            await transaction.rollback();
            throw err;
        }
    });

    const httpStatus = outcome.notFound ? 404
        : outcome.results.every(r => r.success) ? 201 : 207;
    return writeResponse(res, { ...outcome, results: outcome.results }, `Sede "${req.query.sede}" no encontrada.`);
});

// ────────────────────────────────────────────────────────────────────────────
// 4. DELETE /api/v1/pedidos/:doc_num — Eliminar pedido (targeted o broadcast)
// ────────────────────────────────────────────────────────────────────────────
router.delete('/:doc_num', async (req, res) => {
    try {
        const { doc_num } = req.params;

        const outcome = await executeWrite(req.query.sede || null, async (pool) => {
            const resH = await pool.request().input('doc_num', sql.VarChar, doc_num).query(
                `SELECT validador, rowguid FROM saPedidoVenta WHERE LTRIM(RTRIM(doc_num)) = LTRIM(RTRIM(@doc_num))`
            );
            if (!resH.recordset.length) throw new Error('Pedido no existe en esta sede.');

            const { validador, rowguid } = resH.recordset[0];
            const resL = await pool.request().input('doc_num', sql.VarChar, doc_num).query(
                `SELECT reng_num, rowguid FROM saPedidoVentaReng WHERE LTRIM(RTRIM(doc_num)) = LTRIM(RTRIM(@doc_num))`
            );

            const transaction = new sql.Transaction(pool);
            await transaction.begin();

            try {
                // Eliminar renglones primero (FK)
                for (const line of resL.recordset) {
                    const rL = new sql.Request(transaction);
                    rL.input('sDoc_NumOri',  sql.Char(20),          doc_num);
                    rL.input('iReng_NumOri', sql.Int,               line.reng_num);
                    rL.input('sCo_Us_Mo',    sql.Char(6),           '999');
                    rL.input('sMaquina',     sql.VarChar(60),        'SYNC2K');
                    rL.input('gRowguid',     sql.UniqueIdentifier,  line.rowguid);
                    await rL.execute('pEliminarRenglonesPedidoVenta');
                }

                // Eliminar encabezado
                const rH = new sql.Request(transaction);
                rH.input('sDoc_NumOri', sql.Char(20),          doc_num);
                rH.input('tsValidador', sql.VarBinary,         validador);
                rH.input('sMaquina',    sql.VarChar(60),        'SYNC2K');
                rH.input('sCo_Us_Mo',   sql.Char(6),           '999');
                rH.input('gRowguid',    sql.UniqueIdentifier,  rowguid);
                await rH.execute('pEliminarPedidoVenta');

                await transaction.commit();
                return { renglones_eliminados: resL.recordset.length };
            } catch (err) {
                await transaction.rollback();
                throw err;
            }
        });

        return writeResponse(res, outcome, `Sede "${req.query.sede}" no encontrada.`);
    } catch (error) {
        res.status(500).json({ success: false, message: 'Error interno.', error: error.message });
    }
});

module.exports = router;
