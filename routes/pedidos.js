const express = require('express');
const router = express.Router();
const { sql, getPool } = require('../db');

/**
 * @swagger
 * /api/v1/pedidos:
 *   get:
 *     summary: Obtener lista de pedidos de venta
 *     description: Retorna un listado paginado de pedidos ordenados por fecha de emisión descendente.
 *     tags: [Pedidos]
 *     parameters:
 *       - in: query
 *         name: page
 *         schema:
 *           type: integer
 *           default: 1
 *         description: Número de página
 *       - in: query
 *         name: limit
 *         schema:
 *           type: integer
 *           default: 10
 *         description: Cantidad de registros por página
 *     responses:
 *       200:
 *         description: Lista de pedidos obtenida
 */
router.get('/', async (req, res) => {
    try {
        const page = parseInt(req.query.page) || 1;
        const limit = parseInt(req.query.limit) || 10;
        const offset = (page - 1) * limit;

        const pool = await getPool();

        // Query para contar total de registros
        const countResult = await pool.request().query('SELECT COUNT(*) AS total FROM saPedidoVenta');
        const totalRegistros = countResult.recordset[0].total;

        // Query principal paginada
        const request = pool.request();
        request.input('offset', sql.Int, offset);
        request.input('limit', sql.Int, limit);

        const query = `
      SELECT 
        RTRIM(p.doc_num) as doc_num,
        RTRIM(p.descrip) as descrip,
        RTRIM(p.co_cli) as co_cli,
        RTRIM(c.cli_des) as cli_des,
        p.fec_emis,
        p.fec_venc,
        p.status,
        p.anulado,
        RTRIM(p.co_cond) as co_cond,
        RTRIM(p.co_mone) as co_mone,
        p.total_neto,
        p.saldo
      FROM saPedidoVenta p
      LEFT JOIN saCliente c ON p.co_cli = c.co_cli
      ORDER BY p.fec_emis DESC, p.doc_num DESC
      OFFSET @offset ROWS 
      FETCH NEXT @limit ROWS ONLY
    `;

        const result = await request.query(query);

        res.status(200).json({
            success: true,
            data: result.recordset,
            pagination: {
                total: totalRegistros,
                page: page,
                limit: limit,
                totalPages: Math.ceil(totalRegistros / limit)
            }
        });

    } catch (error) {
        console.error('Error al obtener lista de pedidos:', error);
        res.status(500).json({ success: false, message: 'Error en BD.', error: error.message });
    }
});

/**
 * @swagger
 * /api/v1/pedidos/{doc_num}:
 *   get:
 *     summary: Obtener detalle de un pedido
 *     description: Retorna un pedido específico y su lista de renglones (artículos).
 *     tags: [Pedidos]
 *     parameters:
 *       - in: path
 *         name: doc_num
 *         required: true
 *         schema:
 *           type: string
 *         description: Número de documento del pedido
 *     responses:
 *       200:
 *         description: Pedido obtenido
 *       404:
 *         description: Pedido no encontrado
 */
router.get('/:doc_num', async (req, res) => {
    try {
        const doc_num = req.params.doc_num;
        const pool = await getPool();

        // 1. Obtener Encabezado
        const queryEncabezado = `
      SELECT 
        RTRIM(p.doc_num) as doc_num,
        RTRIM(p.descrip) as descrip,
        RTRIM(p.co_cli) as co_cli,
        RTRIM(c.cli_des) as cli_des,
        RTRIM(p.co_ven) as co_ven,
        RTRIM(v.ven_des) as ven_des,
        RTRIM(p.co_cond) as co_cond,
        RTRIM(cd.cond_des) as cond_des,
        p.fec_emis,
        p.fec_venc,
        p.status,
        p.anulado,
        RTRIM(p.co_mone) as co_mone,
        p.tasa,
        p.total_bruto,
        p.monto_imp,
        p.total_neto,
        p.saldo,
        RTRIM(p.comentario) as comentario
      FROM saPedidoVenta p
      LEFT JOIN saCliente c ON p.co_cli = c.co_cli
      LEFT JOIN saVendedor v ON p.co_ven = v.co_ven
      LEFT JOIN saCondicionPago cd ON p.co_cond = cd.co_cond
      WHERE LTRIM(RTRIM(p.doc_num)) = LTRIM(RTRIM(@doc_num))
    `;

        const requestEnc = pool.request();
        requestEnc.input('doc_num', sql.VarChar, doc_num);
        const resultEnc = await requestEnc.query(queryEncabezado);

        if (resultEnc.recordset.length === 0) {
            return res.status(404).json({ success: false, message: 'Pedido no encontrado' });
        }

        const pedido = resultEnc.recordset[0];

        // 2. Obtener Renglones (Detalle)
        const queryRenglones = `
      SELECT 
        r.reng_num,
        RTRIM(r.co_art) as co_art,
        RTRIM(a.art_des) as art_des,
        r.total_art as cantidad,
        RTRIM(r.co_alma) as co_alma,
        RTRIM(r.co_precio) as co_precio,
        r.prec_vta as precio,
        RTRIM(r.tipo_imp) as tipo_imp,
        r.reng_neto as total_renglon
      FROM saPedidoVentaReng r
      LEFT JOIN saArticulo a ON r.co_art = a.co_art
      WHERE LTRIM(RTRIM(r.doc_num)) = LTRIM(RTRIM(@doc_num))
      ORDER BY r.reng_num
    `;

        const requestReng = pool.request();
        requestReng.input('doc_num', sql.VarChar, doc_num);
        const resultReng = await requestReng.query(queryRenglones);

        pedido.renglones = resultReng.recordset;

        res.status(200).json({ success: true, data: pedido });

    } catch (error) {
        console.error('Error al obtener detalle del pedido:', error);
        res.status(500).json({ success: false, message: 'Error en BD.', error: error.message });
    }
});

/**
 * @swagger
 * /api/v1/pedidos:
 *   post:
 *     summary: Crear un nuevo pedido de venta
 *     description: Inserta transaccionalmente un pedido y sus renglones correspondientes.
 *     tags: [Pedidos]
 *     responses:
 *       201:
 *         description: Pedido creado exitosamente
 *       500:
 *         description: Error en la transacción
 */
router.post('/', async (req, res) => {
    const data = req.body;
    const pool = await getPool();
    const transaction = new sql.Transaction(pool);

    try {
        await transaction.begin();

        // 1. Obtención de Fallbacks y Configurables
        // Obtener la tasa de cambio actual USD
        const tasaReq = new sql.Request(transaction);
        const resultTasa = await tasaReq.query("SELECT TOP 1 tasa_v FROM saTasa WHERE LTRIM(RTRIM(co_mone)) IN ('US$', 'USD') ORDER BY fecha DESC");
        const tasaCambio = resultTasa.recordset.length > 0 ? resultTasa.recordset[0].tasa_v : 1;

        // Obtener moneda predeterminada
        const monedaReq = new sql.Request(transaction);
        const resultMoneda = await monedaReq.query("SELECT TOP 1 RTRIM(g_moneda) as g_moneda FROM par_emp");
        const defMoneda = resultMoneda.recordset.length > 0 ? resultMoneda.recordset[0].g_moneda : 'BS';

        // Obtener Almacén predeterminado para evitar errores de Foreign Key (FK) en Renglones
        const almaReq = new sql.Request(transaction);
        const resultAlma = await almaReq.query("SELECT TOP 1 RTRIM(co_alma) as co_alma FROM saAlmacen");
        const defAlma = resultAlma.recordset.length > 0 ? resultAlma.recordset[0].co_alma : '01';

        // Calcular valores de la cabecera en base a los renglones (si no vienen informados)
        const renglones = data.renglones || [];
        let calcTotalBruto = 0;
        let calcTotalNeto = 0;

        renglones.forEach(r => {
            const cantidad = parseFloat(r.cantidad) || 0;
            const precio = parseFloat(r.precio) || 0;
            const total_art = cantidad * precio;
            calcTotalBruto += total_art;
            calcTotalNeto += total_art; // Simplificación: sin impuestos en esta prueba
        });

        const valTotalBruto = parseFloat(data.total_bruto) || calcTotalBruto;
        const valTotalNeto = parseFloat(data.total_neto) || calcTotalNeto;

        // Generar un número de Documento o usar provisional (Ideal obtener correlativo saCorrelativo, usaremos uno generado por timestamp)
        // Para entornos reales en Profit, se debe usar el trigger o tabla de correlativos
        const generatedDocNum = 'API-' + Date.now().toString().slice(-8);
        const tsDate = new Date();

        // 2. Insertar Encabezado (pInsertarPedidoVenta)
        const reqHeader = new sql.Request(transaction);

        reqHeader.input('sDoc_Num', sql.Char(20), generatedDocNum);
        reqHeader.input('sDescrip', sql.VarChar(60), data.descrip || 'Pedido API');
        reqHeader.input('sCo_Cli', sql.Char(16), data.co_cli);
        reqHeader.input('sCo_Tran', sql.Char(6), data.co_tran || '01');
        reqHeader.input('sCo_Mone', sql.Char(6), data.co_mone || defMoneda);
        reqHeader.input('sCo_Cta_Ingr_Egr', sql.Char(20), null);
        reqHeader.input('sCo_Ven', sql.Char(6), data.co_ven || '01');
        reqHeader.input('sCo_Cond', sql.Char(6), data.co_cond || '01');
        reqHeader.input('sdFec_Emis', sql.SmallDateTime, tsDate);
        reqHeader.input('sdFec_Venc', sql.SmallDateTime, tsDate);
        reqHeader.input('sdFec_Reg', sql.SmallDateTime, tsDate);
        reqHeader.input('bAnulado', sql.Bit, 0);
        reqHeader.input('sStatus', sql.Char(1), '0'); // 0=Sin Procesar
        reqHeader.input('deTasa', sql.Decimal(18, 5), tasaCambio);
        reqHeader.input('sN_Control', sql.VarChar(20), null);
        reqHeader.input('sNro_Doc', sql.VarChar(20), null);
        reqHeader.input('sPorc_Desc_Glob', sql.VarChar(15), null);
        reqHeader.input('deMonto_Desc_Glob', sql.Decimal(18, 5), 0);
        reqHeader.input('sPorc_Reca', sql.VarChar(15), null);
        reqHeader.input('deMonto_Reca', sql.Decimal(18, 5), 0);
        reqHeader.input('deSaldo', sql.Decimal(18, 5), valTotalNeto);
        reqHeader.input('deTotal_Bruto', sql.Decimal(18, 5), valTotalBruto);
        reqHeader.input('deMonto_Imp', sql.Decimal(18, 5), 0);
        reqHeader.input('deMonto_Imp2', sql.Decimal(18, 5), 0);
        reqHeader.input('deMonto_Imp3', sql.Decimal(18, 5), 0);
        reqHeader.input('deOtros1', sql.Decimal(18, 5), 0);
        reqHeader.input('deOtros2', sql.Decimal(18, 5), 0);
        reqHeader.input('deOtros3', sql.Decimal(18, 5), 0);
        reqHeader.input('deTotal_Neto', sql.Decimal(18, 5), valTotalNeto);
        reqHeader.input('sDis_Cen', sql.VarChar(sql.MAX), null);
        reqHeader.input('sComentario', sql.VarChar(sql.MAX), data.comentario || 'Generado via Sync2k API');
        reqHeader.input('sDir_Ent', sql.VarChar(sql.MAX), null);
        reqHeader.input('bContrib', sql.Bit, 1);
        reqHeader.input('bImpresa', sql.Bit, 0);
        reqHeader.input('sSalestax', sql.Char(8), null);
        reqHeader.input('sImpfis', sql.VarChar(20), null);
        reqHeader.input('sImpfisfac', sql.VarChar(20), null);
        reqHeader.input('bVen_Ter', sql.Bit, 0);
        reqHeader.input('sCampo1', sql.VarChar(60), null);
        reqHeader.input('sCampo2', sql.VarChar(200), null);
        reqHeader.input('sCampo3', sql.VarChar(60), null);
        reqHeader.input('sCampo4', sql.VarChar(200), null);
        reqHeader.input('sCampo5', sql.VarChar(60), null);
        reqHeader.input('sCampo6', sql.VarChar(60), null);
        reqHeader.input('sCampo7', sql.VarChar(60), null);
        reqHeader.input('sCampo8', sql.VarChar(60), null);
        reqHeader.input('sCo_Us_In', sql.Char(6), '999');
        reqHeader.input('sCo_Sucu_In', sql.Char(6), null);
        reqHeader.input('sRevisado', sql.Char(1), '0');
        reqHeader.input('sTrasnfe', sql.Char(1), '0');
        reqHeader.input('sMaquina', sql.VarChar(60), 'API-APP');

        await reqHeader.execute('pInsertarPedidoVenta');

        // 3. Insertar Renglones (Iteración de pInsertarRenglonesPedidoVenta)
        // Para esto Profit Plus exige un uniqueidentifier global por renglón y el link al documento.

        // Obtenemos el GUID insertado
        const checkHeader = new sql.Request(transaction);
        checkHeader.input('doc_num', sql.VarChar, generatedDocNum);
        const gResult = await checkHeader.query('SELECT rowguid FROM saPedidoVenta WHERE LTRIM(RTRIM(doc_num)) = @doc_num');
        const rowguidDoc = gResult.recordset[0].rowguid;

        let rengNum = 1;

        for (const item of renglones) {
            const q = parseFloat(item.cantidad) || 1;
            const p = parseFloat(item.precio) || 0;
            const subt = q * p;

            const reqLine = new sql.Request(transaction);

            reqLine.input('iReng_Num', sql.Int, rengNum);
            reqLine.input('sDoc_Num', sql.Char(20), generatedDocNum);
            reqLine.input('sCo_Art', sql.Char(30), item.co_art);
            reqLine.input('sDes_Art', sql.VarChar(120), null);
            reqLine.input('sCo_Uni', sql.Char(6), 'UNI');
            reqLine.input('sSco_Uni', sql.Char(6), null);
            reqLine.input('sCo_Alma', sql.Char(6), item.co_alma || defAlma);
            reqLine.input('sCo_Precio', sql.Char(6), '01');
            reqLine.input('sTipo_Imp', sql.Char(1), '1'); // Exento o def
            reqLine.input('sTipo_Imp2', sql.Char(1), null);
            reqLine.input('sTipo_Imp3', sql.Char(1), null);
            reqLine.input('deTotal_Art', sql.Decimal(18, 5), q);
            reqLine.input('deSTotal_Art', sql.Decimal(18, 5), 0);
            reqLine.input('dePrec_Vta', sql.Decimal(18, 5), p);
            reqLine.input('sPorc_Desc', sql.VarChar(15), null);
            reqLine.input('deMonto_Desc', sql.Decimal(18, 5), 0);
            reqLine.input('deReng_Neto', sql.Decimal(18, 5), subt);
            reqLine.input('dePendiente', sql.Decimal(18, 5), q);
            reqLine.input('dePendiente2', sql.Decimal(18, 5), 0);
            reqLine.input('deMonto_Desc_Glob', sql.Decimal(18, 5), 0);
            reqLine.input('deMonto_reca_Glob', sql.Decimal(18, 5), 0);
            reqLine.input('deOtros1_glob', sql.Decimal(18, 5), 0);
            reqLine.input('deOtros2_glob', sql.Decimal(18, 5), 0);
            reqLine.input('deOtros3_glob', sql.Decimal(18, 5), 0);
            reqLine.input('deMonto_imp_afec_glob', sql.Decimal(18, 5), 0);
            reqLine.input('deMonto_imp2_afec_glob', sql.Decimal(18, 5), 0);
            reqLine.input('deMonto_imp3_afec_glob', sql.Decimal(18, 5), 0);
            reqLine.input('sTipo_Doc', sql.Char(4), 'PVEN');
            reqLine.input('gRowguid_Doc', sql.UniqueIdentifier, rowguidDoc);
            reqLine.input('sNum_Doc', sql.VarChar(20), generatedDocNum);
            reqLine.input('dePorc_Imp', sql.Decimal(18, 5), 0);
            reqLine.input('dePorc_Imp2', sql.Decimal(18, 5), 0);
            reqLine.input('dePorc_Imp3', sql.Decimal(18, 5), 0);
            reqLine.input('deMonto_Imp', sql.Decimal(18, 5), 0);
            reqLine.input('deMonto_Imp2', sql.Decimal(18, 5), 0);
            reqLine.input('deMonto_Imp3', sql.Decimal(18, 5), 0);
            reqLine.input('deOtros', sql.Decimal(18, 5), 0);
            reqLine.input('deTotal_Dev', sql.Decimal(18, 5), 0);
            reqLine.input('deMonto_Dev', sql.Decimal(18, 5), 0);
            reqLine.input('sComentario', sql.VarChar(sql.MAX), null);
            reqLine.input('sDis_Cen', sql.VarChar(sql.MAX), null);
            reqLine.input('sCo_Sucu_In', sql.Char(6), null);
            reqLine.input('sCo_Us_In', sql.Char(6), '999');
            reqLine.input('sREVISADO', sql.Char(1), '0');
            reqLine.input('sTRASNFE', sql.Char(1), '0');
            reqLine.input('sMaquina', sql.VarChar(60), 'API-APP');

            await reqLine.execute('pInsertarRenglonesPedidoVenta');
            rengNum++;
        }

        // 4. Confirmación
        await transaction.commit();
        res.status(201).json({ success: true, message: 'Pedido creado exitosamente.', doc_num: generatedDocNum });

    } catch (error) {
        if (transaction) {
            try { await transaction.rollback(); } catch (rbError) { console.error('Error Rollback:', rbError); }
        }
        console.error('Error al crear pedido:', error);
        let errMsg = error.message;
        if (error.precedingErrors && error.precedingErrors.length > 0) {
            errMsg = error.precedingErrors.map(e => e.message).join(' | ');
        } else if (error.originalError && error.originalError.errors) {
            errMsg = error.originalError.errors.map(e => e.message).join(' | ');
        }
        res.status(500).json({ success: false, message: 'Error transaccional BD.', error: errMsg || error.toString() });
    }
});

/**
 * @swagger
 * /api/v1/pedidos/{doc_num}:
 *   delete:
 *     summary: Eliminar un pedido
 *     description: Elimina transaccionalmente el encabezado y sus renglones.
 *     tags: [Pedidos]
 *     parameters:
 *       - in: path
 *         name: doc_num
 *         required: true
 *         schema:
 *           type: string
 *     responses:
 *       200:
 *         description: Pedido eliminado exitosamente
 *       404:
 *         description: Pedido no encontrado
 */
router.delete('/:doc_num', async (req, res) => {
    const doc_num = req.params.doc_num;
    const pool = await getPool();
    const transaction = new sql.Transaction(pool);

    try {
        // 1. Obtención de Encabezado y Renglones previos para Extraer Validadores
        const queryHeader = 'SELECT RTRIM(doc_num) as doc_num, validador, rowguid FROM saPedidoVenta WHERE LTRIM(RTRIM(doc_num)) = LTRIM(RTRIM(@doc_num))';
        const reqHeaderCheck = pool.request();
        reqHeaderCheck.input('doc_num', sql.VarChar, doc_num);
        const resHeader = await reqHeaderCheck.query(queryHeader);

        if (resHeader.recordset.length === 0) {
            return res.status(404).json({ success: false, message: 'Pedido no existe o ya fue borrado' });
        }

        const validadorH = resHeader.recordset[0].validador;
        const rowguidH = resHeader.recordset[0].rowguid;

        const queryLines = 'SELECT RTRIM(doc_num) as doc_num, reng_num, rowguid FROM saPedidoVentaReng WHERE LTRIM(RTRIM(doc_num)) = LTRIM(RTRIM(@doc_num))';
        const reqLinesCheck = pool.request();
        reqLinesCheck.input('doc_num', sql.VarChar, doc_num);
        const resLines = await reqLinesCheck.query(queryLines);

        // 2. Iniciar transacción de Borrado
        await transaction.begin();

        // Eliminar renglones primero (dependencia Foreign Key a Inverso)
        for (const line of resLines.recordset) {
            const lineDelReq = new sql.Request(transaction);
            lineDelReq.input('sDoc_NumOri', sql.Char(20), line.doc_num);
            lineDelReq.input('iReng_NumOri', sql.Int, line.reng_num);
            lineDelReq.input('sCo_Us_Mo', sql.Char(6), '999');
            lineDelReq.input('sMaquina', sql.VarChar(60), 'API-APP');
            lineDelReq.input('sCo_Sucu_Mo', sql.Char(6), null);
            lineDelReq.input('gRowguid', sql.UniqueIdentifier, line.rowguid);
            await lineDelReq.execute('pEliminarRenglonesPedidoVenta');
        }

        // Eliminar encabezado
        const headDelReq = new sql.Request(transaction);
        headDelReq.input('sDoc_NumOri', sql.Char(20), doc_num);
        headDelReq.input('tsValidador', sql.VarBinary, validadorH);
        headDelReq.input('sMaquina', sql.VarChar(60), 'API-APP');
        headDelReq.input('sCo_Us_Mo', sql.Char(6), '999');
        headDelReq.input('sCo_Sucu_Mo', sql.Char(6), null);
        headDelReq.input('gRowguid', sql.UniqueIdentifier, rowguidH);

        await headDelReq.execute('pEliminarPedidoVenta');

        // 3. Confirmar transacción
        await transaction.commit();
        res.status(200).json({ success: true, message: 'Pedido eliminado correctamente.' });

    } catch (error) {
        if (transaction) {
            try { await transaction.rollback(); } catch (rbErr) { }
        }
        console.error('Error al borrar pedido:', error);
        let errMsg = error.message;
        if (error.precedingErrors && error.precedingErrors.length > 0) {
            errMsg = error.precedingErrors.map(e => e.message).join(' | ');
        } else if (error.originalError && error.originalError.errors) {
            errMsg = error.originalError.errors.map(e => e.message).join(' | ');
        }
        res.status(500).json({ success: false, message: 'Error al eliminar.', error: errMsg || error.toString() });
    }
});

module.exports = router;
