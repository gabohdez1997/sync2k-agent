const express = require('express');
const router = express.Router();
const sql = require('mssql');
const db = require('../db');
const { padProfit } = require('../helpers/multiSede');
const { getProximoConsecutivo } = require('../helpers/consecutivos');

console.log('🏦 [AGENT] Iniciando Módulo de Bancos (Movimientos, Transferencias y Conciliación)');

const CREDIT_TYPES = new Set(['DP', 'NC', 'TP']);
const DEBIT_TYPES = new Set(['CH', 'ND', 'TR', 'RC']);
const ALL_VALID_TYPES = new Set([...CREDIT_TYPES, ...DEBIT_TYPES, 'ID']);

const TIPO_OP_LABELS = {
    DP: 'Depósito',
    NC: 'Nota de Crédito',
    TP: 'Transferencia Positiva (Ingreso)',
    CH: 'Cheque',
    ND: 'Nota de Débito',
    TR: 'Transferencia Negativa (Egreso)',
    RC: 'Retiro / Débito',
    ID: 'IDB / IGTF Bancario',
    CHD: 'Cheque Devuelto'
};

function round2(n) {
    return Math.round((Number(n || 0) + Number.EPSILON) * 100) / 100;
}

function isCurrencyUSD(coMone) {
    const m = String(coMone || '').trim().toUpperCase();
    return m !== '' && m !== 'BS' && m !== 'BSS' && m !== 'BS.' && m !== 'VEF' && m !== 'VES';
}

function parseProfitDate(val) {
    if (!val) {
        const now = new Date();
        return new Date(now.getFullYear(), now.getMonth(), now.getDate(), 0, 0, 0, 0);
    }
    const str = String(val).trim();
    const match = str.match(/^(\d{4})-(\d{2})-(\d{2})/);
    if (match) {
        return new Date(Number(match[1]), Number(match[2]) - 1, Number(match[3]), 0, 0, 0, 0);
    }
    const dt = new Date(str);
    if (isNaN(dt.getTime())) {
        const now = new Date();
        return new Date(now.getFullYear(), now.getMonth(), now.getDate(), 0, 0, 0, 0);
    }
    return new Date(dt.getFullYear(), dt.getMonth(), dt.getDate(), 0, 0, 0, 0);
}

function getProfitUser(req, bodyUser) {
    const raw = req.headers['x-profit-user'] || bodyUser || 'SYNC2K';
    return String(raw).trim().toUpperCase().substring(0, 6) || 'SYNC2K';
}

function getSucuCode(srv, bodySucu) {
    const raw = bodySucu || srv.profit_branch_code || '01';
    return String(raw).trim().substring(0, 6) || '01';
}

function convertToAccountCurrency(amount, inputCurrency, accountCoMone, tasa) {
    const amt = Number(amount || 0);
    const rate = Number(tasa || 1) > 0 ? Number(tasa) : 1;
    const accIsUSD = isCurrencyUSD(accountCoMone);
    const inputIsUSD = inputCurrency ? isCurrencyUSD(inputCurrency) : accIsUSD;

    if (accIsUSD === inputIsUSD) {
        return round2(amt);
    }
    if (accIsUSD && !inputIsUSD) {
        return round2(amt / rate);
    }
    return round2(amt * rate);
}

/**
 * Ejecuta una operación de escritura dentro de una transacción SQL Server real.
 */
async function withTransaction(req, res, fn, successStatus = 200) {
    const ctx = await resolvePool(req, res);
    if (!ctx) return;
    const { srv, pool } = ctx;

    let transaction = null;
    try {
        transaction = new sql.Transaction(pool);
        await transaction.begin();
        const result = await fn(transaction, srv, pool);
        await transaction.commit();
        return res.status(successStatus).json({
            success: true,
            sede: srv.name,
            ...result
        });
    } catch (err) {
        if (transaction) {
            try {
                await transaction.rollback();
            } catch (_) {
                // La transacción ya pudo haber sido revertida por SQL Server
            }
        }
        let deepError = err.message || 'Error ejecutando transacción bancaria.';
        if (err.precedingErrors && err.precedingErrors.length > 0) {
            deepError = err.precedingErrors.map(e => e.message).join(' | ');
        } else if (err.originalError && err.originalError.info && err.originalError.info.message) {
            deepError = err.originalError.info.message;
        }
        console.error('❌ [BANCOS TX ERROR]:', deepError);
        return res.status(400).json({ success: false, message: deepError });
    }
}

/**
 * Recalcula y sincroniza saSaldoBanco (TI, CI, TF, CF) para una cuenta bancaria
 * usando exactamente la fórmula de [dbo].[pValidarSaldoBanco].
 */
async function syncSaldoBanco(runner, codCta) {
    const ctaClean = String(codCta || '').trim();
    if (!ctaClean) return;

    const ctaPadded = padProfit(ctaClean, 6);
    const req = new sql.Request(runner);
    req.input('CodCta', sql.Char(6), ctaPadded);

    await req.query(`
        DECLARE @Saldos TABLE (tipo CHAR(2), saldo DECIMAL(18, 2));

        INSERT INTO @Saldos (tipo, saldo)
        SELECT 'TI', ISNULL(SUM(monto_h - monto_d - (idb * CASE WHEN monto_h > 0 THEN -1 ELSE 1 END)), 0)
        FROM saMovimientoBanco
        WHERE cod_cta = @CodCta AND saldo_ini = 1 AND anulado = 0
        UNION ALL
        SELECT 'CI', ISNULL(SUM(monto_h - monto_d - (idb * CASE WHEN monto_h > 0 THEN -1 ELSE 1 END)), 0)
        FROM saMovimientoBanco
        WHERE cod_cta = @CodCta AND saldo_ini = 1 AND conciliado = 1 AND anulado = 0
        UNION ALL
        SELECT 'TF', ISNULL(SUM(monto_h - monto_d - (idb * CASE WHEN monto_h > 0 THEN -1 ELSE 1 END)), 0)
        FROM saMovimientoBanco
        WHERE cod_cta = @CodCta AND anulado = 0
        UNION ALL
        SELECT 'CF', ISNULL(SUM(monto_h - monto_d - (idb * CASE WHEN monto_h > 0 THEN -1 ELSE 1 END)), 0)
        FROM saMovimientoBanco
        WHERE cod_cta = @CodCta AND conciliado = 1 AND anulado = 0;

        UPDATE sb
        SET sb.saldo = s.saldo, sb.revisado = NULL, sb.trasnfe = NULL
        FROM saSaldoBanco sb
        INNER JOIN @Saldos s ON sb.cod_cta = @CodCta AND sb.tipo = s.tipo;

        INSERT INTO saSaldoBanco (cod_cta, tipo, saldo, revisado, trasnfe)
        SELECT @CodCta, s.tipo, s.saldo, NULL, NULL
        FROM @Saldos s
        WHERE NOT EXISTS (
            SELECT 1 FROM saSaldoBanco sb WHERE sb.cod_cta = @CodCta AND sb.tipo = s.tipo
        );
    `);
}

async function generateUniqueMovNum(runner, sucuCode) {
    for (let attempt = 0; attempt < 5; attempt++) {
        const res = await getProximoConsecutivo({
            runner,
            co_tipo_serie: 'B002',
            co_consecutivos: ['MOVB_NUM', 'B002'],
            co_sucur: sucuCode,
            table: 'saMovimientoBanco',
            col: 'mov_num'
        });
        const candidate = String(res.docNum || '').trim();
        if (!candidate) continue;

        const chk = new sql.Request(runner);
        chk.input('Mov', sql.Char(20), padProfit(candidate, 20));
        const existsRes = await chk.query(`SELECT 1 AS ex FROM saMovimientoBanco WHERE mov_num = @Mov`);
        if (existsRes.recordset.length === 0) {
            return candidate;
        }
    }
    throw new Error('No se pudo asignar un número de movimiento único desde la serie B002.');
}

async function generateUniqueTransferNum(runner, sucuCode) {
    for (let attempt = 0; attempt < 5; attempt++) {
        const res = await getProximoConsecutivo({
            runner,
            co_tipo_serie: 'B005',
            co_consecutivos: ['TRENCU_NUM', 'B005'],
            co_sucur: sucuCode,
            table: 'saTransferenciaEntreCuentas',
            col: 'co_trans_ban'
        });
        const candidate = String(res.docNum || '').trim();
        if (!candidate) continue;

        const chk = new sql.Request(runner);
        chk.input('CoTrans', sql.Char(20), padProfit(candidate, 20));
        const existsRes = await chk.query(`SELECT 1 AS ex FROM saTransferenciaEntreCuentas WHERE co_trans_ban = @CoTrans`);
        if (existsRes.recordset.length === 0) {
            return candidate;
        }
    }
    throw new Error('No se pudo asignar un código único de transferencia desde la serie B005.');
}

async function insertMovementSP(runner, params) {
    const req = new sql.Request(runner);
    req.input('sMov_Num', sql.Char(20), padProfit(params.mov_num, 20));
    req.input('sDescrip', sql.VarChar(160), params.descrip ? String(params.descrip).trim().substring(0, 160) : null);
    req.input('sCod_Cta', sql.Char(6), padProfit(params.cod_cta, 6));
    req.input('sdFecha', sql.SmallDateTime, params.fecha);
    req.input('deTasa', sql.Decimal(21, 8), Number(params.tasa || 1));
    req.input('sTipo_Op', sql.Char(2), padProfit(params.tipo_op, 2));
    req.input('sDoc_Num', sql.VarChar(20), String(params.doc_num || params.mov_num).trim().substring(0, 20));
    req.input('deMonto', sql.Decimal(18, 2), round2(params.monto));
    req.input('sCo_Cta_Ingr_Egr', sql.Char(20), padProfit(params.co_cta_ingr_egr, 20));
    req.input('sOrigen', sql.Char(3), padProfit('BAN', 3));
    req.input('sCob_Pag', sql.Char(20), params.cob_pag ? padProfit(params.cob_pag, 20) : null);
    req.input('deIDB', sql.Decimal(18, 2), round2(params.idb || 0));
    req.input('sDep_Num', sql.Char(20), null);
    req.input('bAnulado', sql.Bit, 0);
    req.input('bSaldo_Ini', sql.Bit, 0);
    req.input('bConciliado', sql.Bit, 0);
    req.input('bOri_Dep', sql.Bit, 0);
    req.input('iDep_Con', sql.Int, 0);
    req.input('sdFec_Con', sql.SmallDateTime, null);
    req.input('sCod_IngBen', sql.Char(6), null);
    req.input('sdFecha_Che', sql.SmallDateTime, params.fecha_che || params.fecha);
    req.input('sDis_Cen', sql.VarChar(sql.MAX), null);
    req.input('iNro_Transf_Nomi', sql.Int, null);
    req.input('sCampo1', sql.VarChar(60), params.campo1 ? String(params.campo1).substring(0, 60) : null);
    req.input('sCampo2', sql.VarChar(60), null);
    req.input('sCampo3', sql.VarChar(60), null);
    req.input('sCampo4', sql.VarChar(60), null);
    req.input('sCampo5', sql.VarChar(60), null);
    req.input('sCampo6', sql.VarChar(60), null);
    req.input('sCampo7', sql.VarChar(60), null);
    req.input('sCampo8', sql.VarChar(60), null);
    req.input('sCo_Us_In', sql.Char(6), padProfit(params.co_us_in, 6));
    req.input('sCo_Sucu_In', sql.Char(6), padProfit(params.co_sucu_in, 6));
    req.input('sMaquina', sql.VarChar(60), 'SYNC2K');
    req.input('sRevisado', sql.Char(1), null);
    req.input('sTrasnfe', sql.Char(1), null);
    await req.execute('pInsertarMovimientoBanco');
}

async function insertPista(runner, { user, fecha, sucu, tabla, rowguid, tipoOp, campos }) {
    const pista = new sql.Request(runner);
    pista.input('sUsuario_Id', sql.Char(6), padProfit(user, 6));
    pista.input('dtFecha', sql.DateTime, fecha);
    pista.input('sCo_Sucu', sql.Char(6), padProfit(sucu, 6));
    pista.input('sTablaOri', sql.VarChar(30), tabla);
    pista.input('rowguidOri', sql.UniqueIdentifier, rowguid);
    pista.input('sTipo_Op', sql.Char(1), tipoOp);
    pista.input('sMaquina', sql.VarChar(60), 'SYNC2K');
    pista.input('sCampos', sql.VarChar(sql.MAX), campos || null);
    await pista.execute('pInsertarPista');
}

async function resolvePool(req, res) {
    const sedeId = req.query.sede || req.query.branch_id || req.headers['x-branch-id'] || (req.body && req.body.sede);
    const srv = db.getServers().find(s => !sedeId || s.id === sedeId || s.name === sedeId) || db.getServers()[0];
    if (!srv) {
        res.status(404).json({ success: false, message: 'No hay sucursal/servidor Profit configurado.' });
        return null;
    }
    const pool = await db.getPool(srv.id, req.sqlAuth);
    return { srv, pool };
}

// ============================================================================
// 1. CATÁLOGOS DE BANCOS Y CUENTAS INGRESO/EGRESO
// ============================================================================
router.get('/catalogos', async (req, res) => {
    try {
        const ctx = await resolvePool(req, res);
        if (!ctx) return;
        const { srv, pool } = ctx;

        const [cuentasRes, ingEgrRes, tasa] = await Promise.all([
            pool.request().query(`
                SELECT
                    RTRIM(cb.cod_cta) AS cod_cta,
                    RTRIM(cb.num_cta) AS num_cta,
                    RTRIM(cb.co_ban) AS co_ban,
                    RTRIM(b.des_ban) AS des_ban,
                    RTRIM(cb.co_mone) AS co_mone,
                    RTRIM(cb.tipo_cu) AS tipo_cu,
                    cb.inactivo,
                    cb.fecini,
                    cb.mes_ini,
                    ISNULL(tf.saldo, 0) AS saldo_total,
                    ISNULL(cf.saldo, 0) AS saldo_conciliado,
                    ISNULL(ti.saldo, 0) AS saldo_inicial
                FROM saCuentaBancaria cb
                INNER JOIN saBanco b ON cb.co_ban = b.co_ban
                LEFT JOIN saSaldoBanco tf ON cb.cod_cta = tf.cod_cta AND tf.tipo = 'TF'
                LEFT JOIN saSaldoBanco cf ON cb.cod_cta = cf.cod_cta AND cf.tipo = 'CF'
                LEFT JOIN saSaldoBanco ti ON cb.cod_cta = ti.cod_cta AND ti.tipo = 'TI'
                ORDER BY cb.inactivo ASC, b.des_ban ASC, cb.cod_cta ASC
            `),
            pool.request().query(`
                SELECT
                    RTRIM(co_cta_ingr_egr) AS co_cta_ingr_egr,
                    RTRIM(descrip) AS descrip,
                    RTRIM(ISNULL(co_islr, '')) AS co_islr
                FROM saCuentaIngEgr
                ORDER BY co_cta_ingr_egr ASC
            `),
            db.getExchangeRate(pool)
        ]);

        const todasCuentas = cuentasRes.recordset.map(c => ({
            ...c,
            moneda_tipo: isCurrencyUSD(c.co_mone) ? 'USD' : 'BS'
        }));

        res.json({
            success: true,
            sede: srv.name,
            tasa: Number(tasa || 1),
            cuentas_bancarias: todasCuentas.filter(c => !c.inactivo),
            todas_cuentas_bancarias: todasCuentas,
            cuentas_ingr_egr: ingEgrRes.recordset,
            tipos_operacion: Object.entries(TIPO_OP_LABELS).map(([codigo, label]) => ({
                codigo,
                label,
                naturaleza: codigo === 'ID' ? 'IDB' : CREDIT_TYPES.has(codigo) ? 'INGRESO' : 'EGRESO'
            })),
            defaults: {
                cta_ingr_egr_ingreso: '01',
                cta_ingr_egr_egreso: '02'
            }
        });
    } catch (err) {
        console.error('❌ [BANCOS] Error en GET /catalogos:', err.message);
        res.status(500).json({ success: false, message: 'Error consultando catálogos bancarios: ' + err.message });
    }
});

// ============================================================================
// 2. LISTADO HISTORIAL DE MOVIMIENTOS BANCARIOS
// ============================================================================
router.get('/movimientos', async (req, res) => {
    try {
        const ctx = await resolvePool(req, res);
        if (!ctx) return;
        const { pool } = ctx;

        const page = Math.max(1, parseInt(req.query.page, 10) || 1);
        const limit = Math.min(100, Math.max(1, parseInt(req.query.limit, 10) || 15));
        const offset = (page - 1) * limit;

        const search = req.query.search ? String(req.query.search).trim() : '';
        const codCta = req.query.cod_cta ? String(req.query.cod_cta).trim() : '';
        const tipoOp = req.query.tipo_op ? String(req.query.tipo_op).trim().toUpperCase() : '';
        const origen = req.query.origen ? String(req.query.origen).trim().toUpperCase() : '';
        const estado = req.query.estado ? String(req.query.estado).trim().toLowerCase() : 'all';
        const fechaD = req.query.fecha_d || req.query.fec_d || '';
        const fechaH = req.query.fecha_h || req.query.fec_h || '';
        const coUsIn = req.query.co_us_in ? String(req.query.co_us_in).trim().toUpperCase() : '';

        const sortByRaw = String(req.query.sortBy || 'fecha').toLowerCase();
        const sortDir = String(req.query.sortDir || 'desc').toUpperCase() === 'ASC' ? 'ASC' : 'DESC';

        const SORT_MAP = {
            fecha: 'mb.fecha',
            mov_num: 'mb.mov_num',
            doc_num: 'mb.doc_num',
            cod_cta: 'mb.cod_cta',
            tipo_op: 'mb.tipo_op',
            origen: 'mb.origen',
            monto: '(mb.monto_h - mb.monto_d - mb.idb)',
            usuario: 'mb.co_us_in'
        };
        const orderCol = SORT_MAP[sortByRaw] || 'mb.fecha';

        const whereParts = ['1=1'];
        const bindInputs = (request) => {
            if (search) request.input('Search', sql.VarChar(120), `%${search}%`);
            if (codCta) request.input('CodCta', sql.Char(6), padProfit(codCta, 6));
            if (tipoOp) request.input('TipoOp', sql.Char(2), padProfit(tipoOp, 2));
            if (origen) request.input('Origen', sql.Char(3), padProfit(origen, 3));
            if (fechaD) request.input('FechaD', sql.SmallDateTime, parseProfitDate(fechaD));
            if (fechaH) request.input('FechaH', sql.SmallDateTime, parseProfitDate(fechaH));
            if (coUsIn) request.input('CoUsIn', sql.VarChar(20), coUsIn);
        };

        if (search) {
            whereParts.push(`(
                RTRIM(mb.mov_num) LIKE @Search
                OR RTRIM(mb.doc_num) LIKE @Search
                OR ISNULL(mb.descrip, '') LIKE @Search
                OR RTRIM(ISNULL(mb.cob_pag, '')) LIKE @Search
                OR RTRIM(cb.num_cta) LIKE @Search
                OR RTRIM(b.des_ban) LIKE @Search
            )`);
        }
        if (codCta) whereParts.push('mb.cod_cta = @CodCta');
        if (tipoOp) whereParts.push('mb.tipo_op = @TipoOp');
        if (origen) whereParts.push('mb.origen = @Origen');
        if (fechaD) whereParts.push('mb.fecha >= @FechaD');
        if (fechaH) whereParts.push('mb.fecha < DATEADD(DAY, 1, @FechaH)');
        if (coUsIn) whereParts.push('UPPER(RTRIM(mb.co_us_in)) = @CoUsIn');

        if (estado === 'activos') {
            whereParts.push('mb.anulado = 0');
        } else if (estado === 'anulados') {
            whereParts.push('mb.anulado = 1');
        } else if (estado === 'conciliados') {
            whereParts.push('mb.anulado = 0 AND mb.conciliado = 1');
        } else if (estado === 'no_conciliados') {
            whereParts.push('mb.anulado = 0 AND mb.conciliado = 0');
        }

        const whereClause = whereParts.join(' AND ');

        const metricsReq = pool.request();
        bindInputs(metricsReq);
        const metricsQ = await metricsReq.query(`
            SELECT
                COUNT(*) AS total_registros,
                SUM(CASE WHEN mb.anulado = 1 THEN 1 ELSE 0 END) AS total_anulados,
                SUM(CASE WHEN mb.anulado = 0 AND mb.conciliado = 1 THEN 1 ELSE 0 END) AS total_conciliados,
                SUM(CASE WHEN mb.anulado = 0 AND UPPER(RTRIM(cb.co_mone)) IN ('BS','BSS','BS.','VEF','VES') THEN mb.monto_h ELSE 0 END) AS total_ingresos_bs,
                SUM(CASE WHEN mb.anulado = 0 AND UPPER(RTRIM(cb.co_mone)) NOT IN ('BS','BSS','BS.','VEF','VES') THEN mb.monto_h ELSE 0 END) AS total_ingresos_usd,
                SUM(CASE WHEN mb.anulado = 0 AND UPPER(RTRIM(cb.co_mone)) IN ('BS','BSS','BS.','VEF','VES') THEN mb.monto_d ELSE 0 END) AS total_egresos_bs,
                SUM(CASE WHEN mb.anulado = 0 AND UPPER(RTRIM(cb.co_mone)) NOT IN ('BS','BSS','BS.','VEF','VES') THEN mb.monto_d ELSE 0 END) AS total_egresos_usd,
                SUM(CASE WHEN mb.anulado = 0 AND UPPER(RTRIM(cb.co_mone)) IN ('BS','BSS','BS.','VEF','VES') THEN mb.idb ELSE 0 END) AS total_idb_bs,
                SUM(CASE WHEN mb.anulado = 0 AND UPPER(RTRIM(cb.co_mone)) NOT IN ('BS','BSS','BS.','VEF','VES') THEN mb.idb ELSE 0 END) AS total_idb_usd
            FROM saMovimientoBanco mb
            INNER JOIN saCuentaBancaria cb ON mb.cod_cta = cb.cod_cta
            INNER JOIN saBanco b ON cb.co_ban = b.co_ban
            WHERE ${whereClause}
        `);

        const totalItems = Number(metricsQ.recordset[0]?.total_registros || 0);
        const totalPages = Math.max(1, Math.ceil(totalItems / limit));

        const dataReq = pool.request();
        bindInputs(dataReq);
        dataReq.input('Offset', sql.Int, offset);
        dataReq.input('Limit', sql.Int, limit);

        const listQ = await dataReq.query(`
            SELECT
                RTRIM(mb.mov_num) AS mov_num,
                RTRIM(ISNULL(mb.descrip, '')) AS descrip,
                RTRIM(mb.cod_cta) AS cod_cta,
                RTRIM(cb.num_cta) AS num_cta,
                RTRIM(b.des_ban) AS des_ban,
                RTRIM(cb.co_mone) AS co_mone,
                RTRIM(mb.co_cta_ingr_egr) AS co_cta_ingr_egr,
                RTRIM(ISNULL(cie.descrip, '')) AS cta_ing_egr_des,
                mb.fecha,
                mb.fecha_che,
                mb.tasa,
                RTRIM(mb.tipo_op) AS tipo_op,
                RTRIM(mb.doc_num) AS doc_num,
                mb.monto_d,
                mb.monto_h,
                mb.idb,
                CASE
                    WHEN RTRIM(mb.tipo_op) = 'ID' THEN mb.idb
                    WHEN mb.monto_h > 0 THEN mb.monto_h
                    ELSE mb.monto_d
                END AS monto,
                CASE
                    WHEN RTRIM(mb.tipo_op) = 'ID' THEN 'IDB'
                    WHEN mb.monto_h > 0 OR RTRIM(mb.tipo_op) IN ('DP','NC','TP') THEN 'INGRESO'
                    ELSE 'EGRESO'
                END AS naturaleza,
                mb.saldo_ini,
                RTRIM(mb.origen) AS origen,
                RTRIM(ISNULL(mb.cob_pag, '')) AS cob_pag,
                RTRIM(ISNULL(mb.dep_num, '')) AS dep_num,
                mb.conciliado,
                mb.fec_con,
                mb.anulado,
                RTRIM(mb.co_us_in) AS co_us_in,
                mb.fe_us_in,
                RTRIM(ISNULL(mb.co_sucu_in, '')) AS co_sucu_in,
                RTRIM(ISNULL(tr.co_trans_ban, '')) AS co_trans_ban
            FROM saMovimientoBanco mb
            INNER JOIN saCuentaBancaria cb ON mb.cod_cta = cb.cod_cta
            INNER JOIN saBanco b ON cb.co_ban = b.co_ban
            LEFT JOIN saCuentaIngEgr cie ON mb.co_cta_ingr_egr = cie.co_cta_ingr_egr
            LEFT JOIN saTransferenciaEntreCuentas tr
                ON (mb.mov_num = tr.mov_ban_origen OR mb.mov_num = tr.mov_ban_destino OR mb.mov_num = tr.mov_ban_comision)
            WHERE ${whereClause}
            ORDER BY ${orderCol} ${sortDir}, mb.fe_us_in DESC, mb.mov_num DESC
            OFFSET @Offset ROWS FETCH NEXT @Limit ROWS ONLY
        `);

        const data = listQ.recordset.map(row => ({
            ...row,
            tipo_op_label: TIPO_OP_LABELS[row.tipo_op] || row.tipo_op,
            es_transferencia: Boolean(row.co_trans_ban),
            editable: row.origen === 'BAN' && !row.anulado && !row.conciliado,
            anulable: row.origen === 'BAN' && !row.anulado && !row.conciliado,
            eliminable: row.origen === 'BAN' && !row.conciliado
        }));

        const mRow = metricsQ.recordset[0] || {};
        res.json({
            success: true,
            data,
            metrics: {
                total_registros: Number(mRow.total_registros || 0),
                total_anulados: Number(mRow.total_anulados || 0),
                total_conciliados: Number(mRow.total_conciliados || 0),
                total_ingresos_bs: Number(mRow.total_ingresos_bs || 0),
                total_ingresos_usd: Number(mRow.total_ingresos_usd || 0),
                total_egresos_bs: Number(mRow.total_egresos_bs || 0),
                total_egresos_usd: Number(mRow.total_egresos_usd || 0),
                total_idb_bs: Number(mRow.total_idb_bs || 0),
                total_idb_usd: Number(mRow.total_idb_usd || 0)
            },
            pagination: {
                page,
                limit,
                total_items: totalItems,
                total_pages: totalPages
            }
        });
    } catch (err) {
        console.error('❌ [BANCOS] Error en GET /movimientos:', err.message);
        res.status(500).json({ success: false, message: 'Error listando movimientos bancarios: ' + err.message });
    }
});

// ============================================================================
// 3. DETALLE DE UN MOVIMIENTO BANCARIO
// ============================================================================
router.get('/movimientos/:mov_num', async (req, res) => {
    try {
        const ctx = await resolvePool(req, res);
        if (!ctx) return;
        const { pool } = ctx;

        const movNum = decodeURIComponent(req.params.mov_num || '').trim();
        if (!movNum) return res.status(400).json({ success: false, message: 'Número de movimiento requerido.' });

        const reqMov = pool.request();
        reqMov.input('Mov', sql.Char(20), padProfit(movNum, 20));
        const movRes = await reqMov.query(`
            SELECT
                RTRIM(mb.mov_num) AS mov_num,
                RTRIM(ISNULL(mb.descrip, '')) AS descrip,
                RTRIM(mb.cod_cta) AS cod_cta,
                RTRIM(cb.num_cta) AS num_cta,
                RTRIM(cb.co_ban) AS co_ban,
                RTRIM(b.des_ban) AS des_ban,
                RTRIM(cb.co_mone) AS co_mone,
                RTRIM(mb.co_cta_ingr_egr) AS co_cta_ingr_egr,
                RTRIM(ISNULL(cie.descrip, '')) AS cta_ing_egr_des,
                mb.fecha,
                mb.fecha_che,
                mb.tasa,
                RTRIM(mb.tipo_op) AS tipo_op,
                RTRIM(mb.doc_num) AS doc_num,
                mb.monto_d,
                mb.monto_h,
                mb.idb,
                mb.saldo_ini,
                RTRIM(mb.origen) AS origen,
                RTRIM(ISNULL(mb.cob_pag, '')) AS cob_pag,
                RTRIM(ISNULL(mb.dep_num, '')) AS dep_num,
                mb.conciliado,
                mb.fec_con,
                mb.anulado,
                RTRIM(ISNULL(mb.campo1, '')) AS campo1,
                RTRIM(mb.co_us_in) AS co_us_in,
                mb.fe_us_in,
                RTRIM(ISNULL(mb.co_us_mo, '')) AS co_us_mo,
                mb.fe_us_mo,
                RTRIM(ISNULL(mb.co_sucu_in, '')) AS co_sucu_in
            FROM saMovimientoBanco mb
            INNER JOIN saCuentaBancaria cb ON mb.cod_cta = cb.cod_cta
            INNER JOIN saBanco b ON cb.co_ban = b.co_ban
            LEFT JOIN saCuentaIngEgr cie ON mb.co_cta_ingr_egr = cie.co_cta_ingr_egr
            WHERE mb.mov_num = @Mov
        `);

        if (movRes.recordset.length === 0) {
            return res.status(404).json({ success: false, message: `Movimiento ${movNum} no encontrado.` });
        }

        const mov = movRes.recordset[0];
        const monto = mov.tipo_op === 'ID' ? Number(mov.idb) : (Number(mov.monto_h) > 0 ? Number(mov.monto_h) : Number(mov.monto_d));
        const naturaleza = mov.tipo_op === 'ID' ? 'IDB' : (Number(mov.monto_h) > 0 || CREDIT_TYPES.has(mov.tipo_op) ? 'INGRESO' : 'EGRESO');

        const trReq = pool.request();
        trReq.input('Mov', sql.Char(20), padProfit(movNum, 20));
        const trRes = await trReq.query(`
            SELECT TOP 1
                RTRIM(co_trans_ban) AS co_trans_ban,
                RTRIM(des_trans_ban) AS des_trans_ban,
                fecha,
                procesado,
                monto,
                comision,
                RTRIM(cta_origen) AS cta_origen,
                RTRIM(cta_ingr_egr_origen) AS cta_ingr_egr_origen,
                fecha_origen,
                RTRIM(mov_ban_origen) AS mov_ban_origen,
                RTRIM(ISNULL(referencia_origen, '')) AS referencia_origen,
                RTRIM(ISNULL(cta_comision, '')) AS cta_comision,
                RTRIM(ISNULL(cta_ingr_egr_comision, '')) AS cta_ingr_egr_comision,
                RTRIM(ISNULL(mov_ban_comision, '')) AS mov_ban_comision,
                RTRIM(ISNULL(referencia_comis, '')) AS referencia_comis,
                RTRIM(cta_destino) AS cta_destino,
                RTRIM(cta_ingr_egr_destino) AS cta_ingr_egr_destino,
                fecha_destino,
                RTRIM(mov_ban_destino) AS mov_ban_destino,
                RTRIM(ISNULL(referencia_destino, '')) AS referencia_destino,
                tasa,
                tasa_origen,
                RTRIM(co_us_in) AS co_us_in,
                fe_us_in
            FROM saTransferenciaEntreCuentas
            WHERE mov_ban_origen = @Mov OR mov_ban_destino = @Mov OR mov_ban_comision = @Mov
        `);

        let transferencia = null;
        if (trRes.recordset.length > 0) {
            const t = trRes.recordset[0];
            const legsReq = pool.request();
            legsReq.input('Orig', sql.Char(20), padProfit(t.mov_ban_origen, 20));
            legsReq.input('Dest', sql.Char(20), padProfit(t.mov_ban_destino, 20));
            legsReq.input('Com', sql.Char(20), t.mov_ban_comision ? padProfit(t.mov_ban_comision, 20) : null);
            const legsQ = await legsReq.query(`
                SELECT
                    RTRIM(mb.mov_num) AS mov_num,
                    RTRIM(mb.cod_cta) AS cod_cta,
                    RTRIM(cb.num_cta) AS num_cta,
                    RTRIM(b.des_ban) AS des_ban,
                    RTRIM(cb.co_mone) AS co_mone,
                    RTRIM(mb.tipo_op) AS tipo_op,
                    RTRIM(mb.doc_num) AS doc_num,
                    mb.monto_d,
                    mb.monto_h,
                    mb.idb,
                    mb.tasa,
                    mb.conciliado,
                    mb.anulado
                FROM saMovimientoBanco mb
                INNER JOIN saCuentaBancaria cb ON mb.cod_cta = cb.cod_cta
                INNER JOIN saBanco b ON cb.co_ban = b.co_ban
                WHERE mb.mov_num IN (@Orig, @Dest) OR (@Com IS NOT NULL AND mb.mov_num = @Com)
            `);
            transferencia = {
                ...t,
                movimientos_vinculados: legsQ.recordset
            };
        }

        res.json({
            success: true,
            data: {
                ...mov,
                monto,
                naturaleza,
                tipo_op_label: TIPO_OP_LABELS[mov.tipo_op] || mov.tipo_op,
                es_transferencia: Boolean(transferencia),
                transferencia
            }
        });
    } catch (err) {
        console.error('❌ [BANCOS] Error en GET /movimientos/:mov_num:', err.message);
        res.status(500).json({ success: false, message: 'Error consultando detalle del movimiento: ' + err.message });
    }
});

// ============================================================================
// 4. REGISTRAR MOVIMIENTO BANCARIO INDIVIDUAL (origen = 'BAN')
// ============================================================================
router.post('/movimientos', async (req, res) => {
    const payload = req.body?.movimiento || req.body || {};

    const tipoOp = String(payload.tipo_op || '').trim().toUpperCase();
    const codCta = String(payload.cod_cta || '').trim();
    const montoRaw = Number(payload.monto || 0);
    const idbRaw = Number(payload.idb || 0);
    const tasaInput = Number(payload.tasa || 1) > 0 ? Number(payload.tasa) : 1;
    const monedaInput = payload.moneda_input ? String(payload.moneda_input).trim().toUpperCase() : '';
    const manualMovNum = payload.mov_num ? String(payload.mov_num).trim().substring(0, 20) : '';
    const docNumInput = payload.doc_num ? String(payload.doc_num).trim().substring(0, 20) : '';
    const descrip = payload.descrip ? String(payload.descrip).trim().substring(0, 160) : '';
    const fecMov = parseProfitDate(payload.fecha);
    const fecChe = payload.fecha_che ? parseProfitDate(payload.fecha_che) : fecMov;
    const defaultIngEgr = CREDIT_TYPES.has(tipoOp) ? '01' : '02';
    const coCtaIngrEgr = String(payload.co_cta_ingr_egr || defaultIngEgr).trim().substring(0, 20);
    const profitUser = getProfitUser(req, payload.co_us_in);

    if (!codCta) return res.status(400).json({ success: false, message: 'Debe seleccionar una cuenta bancaria.' });
    if (!ALL_VALID_TYPES.has(tipoOp)) return res.status(400).json({ success: false, message: `Tipo de operación inválido (${tipoOp}).` });
    if (montoRaw <= 0) return res.status(400).json({ success: false, message: 'El monto debe ser mayor a cero.' });

    await withTransaction(req, res, async (transaction, srv) => {
        const sucuCode = getSucuCode(srv, payload.co_sucu_in);

        const ctaReq = new sql.Request(transaction);
        ctaReq.input('CodCta', sql.Char(6), padProfit(codCta, 6));
        const ctaRes = await ctaReq.query(`SELECT RTRIM(cod_cta) AS cod_cta, RTRIM(co_mone) AS co_mone, inactivo FROM saCuentaBancaria WHERE cod_cta = @CodCta`);
        if (ctaRes.recordset.length === 0) {
            throw new Error(`La cuenta bancaria ${codCta} no existe en Profit Plus.`);
        }
        const cta = ctaRes.recordset[0];
        if (cta.inactivo) {
            throw new Error(`La cuenta bancaria ${codCta} se encuentra inactiva.`);
        }

        let movNum = manualMovNum;
        if (movNum) {
            const dupReq = new sql.Request(transaction);
            dupReq.input('Mov', sql.Char(20), padProfit(movNum, 20));
            const dupRes = await dupReq.query(`SELECT 1 AS ex FROM saMovimientoBanco WHERE mov_num = @Mov`);
            if (dupRes.recordset.length > 0) {
                throw new Error(`El N° de movimiento "${movNum}" ya existe en Profit Plus.`);
            }
        } else {
            movNum = await generateUniqueMovNum(transaction, sucuCode);
        }

        const montoNativo = convertToAccountCurrency(montoRaw, monedaInput, cta.co_mone, tasaInput);
        const idbNativo = tipoOp === 'ID' ? 0 : convertToAccountCurrency(idbRaw, monedaInput, cta.co_mone, tasaInput);
        const tasaGuardar = isCurrencyUSD(cta.co_mone) ? tasaInput : (monedaInput && isCurrencyUSD(monedaInput) ? tasaInput : 1);
        const finalDocNum = docNumInput || movNum;

        await insertMovementSP(transaction, {
            mov_num: movNum,
            descrip: descrip || `${TIPO_OP_LABELS[tipoOp] || tipoOp} ${finalDocNum}`,
            cod_cta: codCta,
            fecha: fecMov,
            fecha_che: fecChe,
            tasa: tasaGuardar,
            tipo_op: tipoOp,
            doc_num: finalDocNum,
            monto: montoNativo,
            co_cta_ingr_egr: coCtaIngrEgr,
            idb: idbNativo,
            co_us_in: profitUser,
            co_sucu_in: sucuCode
        });

        await syncSaldoBanco(transaction, codCta);

        return {
            mov_num: movNum,
            doc_num: finalDocNum,
            cod_cta: codCta,
            co_mone: cta.co_mone,
            monto: montoNativo,
            idb: tipoOp === 'ID' ? montoNativo : idbNativo,
            tipo_op: tipoOp,
            message: `Movimiento bancario ${movNum} registrado con éxito.`
        };
    }, 201);
});

// ============================================================================
// 5. ACTUALIZAR MOVIMIENTO BANCARIO INDIVIDUAL (origen = 'BAN')
// ============================================================================
router.put('/movimientos/:mov_num', async (req, res) => {
    const movNumOri = decodeURIComponent(req.params.mov_num || '').trim();
    const payload = req.body?.movimiento || req.body || {};

    if (!movNumOri) return res.status(400).json({ success: false, message: 'Número de movimiento requerido.' });

    const tipoOp = String(payload.tipo_op || '').trim().toUpperCase();
    const newCodCta = String(payload.cod_cta || '').trim();
    const montoRaw = Number(payload.monto || 0);
    const idbRaw = Number(payload.idb || 0);
    const tasaInput = Number(payload.tasa || 1) > 0 ? Number(payload.tasa) : 1;
    const monedaInput = payload.moneda_input ? String(payload.moneda_input).trim().toUpperCase() : '';
    const docNumInput = payload.doc_num ? String(payload.doc_num).trim().substring(0, 20) : movNumOri;
    const descrip = payload.descrip ? String(payload.descrip).trim().substring(0, 160) : '';
    const fecMov = parseProfitDate(payload.fecha);
    const fecChe = payload.fecha_che ? parseProfitDate(payload.fecha_che) : fecMov;
    const defaultIngEgr = CREDIT_TYPES.has(tipoOp) ? '01' : '02';
    const coCtaIngrEgr = String(payload.co_cta_ingr_egr || defaultIngEgr).trim().substring(0, 20);
    const profitUser = getProfitUser(req, payload.co_us_mo);

    if (!newCodCta) return res.status(400).json({ success: false, message: 'Debe seleccionar una cuenta bancaria.' });
    if (!ALL_VALID_TYPES.has(tipoOp)) return res.status(400).json({ success: false, message: `Tipo de operación inválido (${tipoOp}).` });
    if (montoRaw <= 0) return res.status(400).json({ success: false, message: 'El monto debe ser mayor a cero.' });

    await withTransaction(req, res, async (transaction, srv) => {
        const sucuCode = getSucuCode(srv, payload.co_sucu_mo);

        const curReq = new sql.Request(transaction);
        curReq.input('Mov', sql.Char(20), padProfit(movNumOri, 20));
        const curRes = await curReq.query(`
            SELECT
                RTRIM(mov_num) AS mov_num,
                RTRIM(cod_cta) AS cod_cta,
                RTRIM(origen) AS origen,
                anulado,
                conciliado
            FROM saMovimientoBanco
            WHERE mov_num = @Mov
        `);
        if (curRes.recordset.length === 0) throw new Error(`Movimiento ${movNumOri} no encontrado.`);
        const cur = curRes.recordset[0];
        if (cur.origen !== 'BAN') {
            throw new Error(`Solo se pueden editar movimientos directos de Banco (origen BAN). Este movimiento proviene de ${cur.origen}.`);
        }
        if (cur.anulado) throw new Error('No se puede editar un movimiento anulado.');
        if (cur.conciliado) throw new Error('No se puede editar un movimiento que ya está conciliado. Debe desconciliarlo primero.');

        const trChk = new sql.Request(transaction);
        trChk.input('Mov', sql.Char(20), padProfit(movNumOri, 20));
        const trRes = await trChk.query(`SELECT TOP 1 RTRIM(co_trans_ban) AS co_trans_ban FROM saTransferenciaEntreCuentas WHERE mov_ban_origen = @Mov OR mov_ban_destino = @Mov OR mov_ban_comision = @Mov`);
        if (trRes.recordset.length > 0) {
            throw new Error(`Este movimiento pertenece a la Transferencia entre Cuentas #${trRes.recordset[0].co_trans_ban}. Edítela desde el modo Transferencia.`);
        }

        const ctaReq = new sql.Request(transaction);
        ctaReq.input('CodCta', sql.Char(6), padProfit(newCodCta, 6));
        const ctaRes = await ctaReq.query(`SELECT RTRIM(cod_cta) AS cod_cta, RTRIM(co_mone) AS co_mone, inactivo FROM saCuentaBancaria WHERE cod_cta = @CodCta`);
        if (ctaRes.recordset.length === 0) throw new Error(`Cuenta bancaria ${newCodCta} no encontrada.`);
        const cta = ctaRes.recordset[0];

        const montoNativo = convertToAccountCurrency(montoRaw, monedaInput, cta.co_mone, tasaInput);
        const idbNativo = tipoOp === 'ID' ? montoNativo : convertToAccountCurrency(idbRaw, monedaInput, cta.co_mone, tasaInput);
        const montoD = DEBIT_TYPES.has(tipoOp) ? montoNativo : 0;
        const montoH = CREDIT_TYPES.has(tipoOp) ? montoNativo : 0;
        const tasaGuardar = isCurrencyUSD(cta.co_mone) ? tasaInput : (monedaInput && isCurrencyUSD(monedaInput) ? tasaInput : 1);

        const updReq = new sql.Request(transaction);
        updReq.input('MovOri', sql.Char(20), padProfit(movNumOri, 20));
        updReq.input('Descrip', sql.VarChar(160), descrip || `${TIPO_OP_LABELS[tipoOp] || tipoOp} ${docNumInput}`);
        updReq.input('CodCta', sql.Char(6), padProfit(newCodCta, 6));
        updReq.input('Fecha', sql.SmallDateTime, fecMov);
        updReq.input('FechaChe', sql.SmallDateTime, fecChe);
        updReq.input('Tasa', sql.Decimal(21, 8), tasaGuardar);
        updReq.input('TipoOp', sql.Char(2), padProfit(tipoOp, 2));
        updReq.input('DocNum', sql.VarChar(20), docNumInput);
        updReq.input('MontoD', sql.Decimal(18, 2), round2(montoD));
        updReq.input('MontoH', sql.Decimal(18, 2), round2(montoH));
        updReq.input('IDB', sql.Decimal(18, 2), round2(idbNativo));
        updReq.input('CtaIngEgr', sql.Char(20), padProfit(coCtaIngrEgr, 20));
        updReq.input('CoUsMo', sql.Char(6), padProfit(profitUser, 6));
        updReq.input('CoSucuMo', sql.Char(6), padProfit(sucuCode, 6));

        const updRes = await updReq.query(`
            UPDATE saMovimientoBanco
            SET
                descrip = @Descrip,
                cod_cta = @CodCta,
                fecha = @Fecha,
                fecha_che = @FechaChe,
                tasa = @Tasa,
                tipo_op = @TipoOp,
                doc_num = @DocNum,
                monto_d = @MontoD,
                monto_h = @MontoH,
                idb = @IDB,
                co_cta_ingr_egr = @CtaIngEgr,
                co_us_mo = @CoUsMo,
                co_sucu_mo = @CoSucuMo,
                fe_us_mo = GETDATE()
            OUTPUT inserted.rowguid, inserted.fe_us_mo
            WHERE mov_num = @MovOri
        `);

        if (updRes.recordset.length > 0) {
            await insertPista(transaction, {
                user: profitUser,
                fecha: updRes.recordset[0].fe_us_mo,
                sucu: sucuCode,
                tabla: 'saMovimientoBanco',
                rowguid: updRes.recordset[0].rowguid,
                tipoOp: 'M',
                campos: movNumOri
            });
        }

        if (cur.cod_cta !== newCodCta) {
            await syncSaldoBanco(transaction, cur.cod_cta);
        }
        await syncSaldoBanco(transaction, newCodCta);

        return {
            mov_num: movNumOri,
            cod_cta: newCodCta,
            monto: montoNativo,
            tipo_op: tipoOp,
            message: `Movimiento ${movNumOri} actualizado con éxito.`
        };
    });
});

// ============================================================================
// 6. ANULAR MOVIMIENTO BANCARIO (Y TRANSFERENCIA SI APLICA)
// ============================================================================
async function handleAnularMovimiento(req, res) {
    const movNum = decodeURIComponent(req.params.mov_num || '').trim();
    if (!movNum) return res.status(400).json({ success: false, message: 'Número de movimiento requerido.' });
    const profitUser = getProfitUser(req, req.body?.co_us_mo);

    await withTransaction(req, res, async (transaction, srv) => {
        const sucuCode = getSucuCode(srv, req.body?.co_sucu_mo);

        const curReq = new sql.Request(transaction);
        curReq.input('Mov', sql.Char(20), padProfit(movNum, 20));
        const curRes = await curReq.query(`
            SELECT RTRIM(mov_num) AS mov_num, RTRIM(cod_cta) AS cod_cta, RTRIM(origen) AS origen, anulado, conciliado
            FROM saMovimientoBanco WHERE mov_num = @Mov
        `);
        if (curRes.recordset.length === 0) throw new Error(`Movimiento ${movNum} no encontrado.`);
        const cur = curRes.recordset[0];
        if (cur.origen !== 'BAN') throw new Error(`Solo pueden anularse movimientos de origen BAN desde este módulo (origen: ${cur.origen}).`);
        if (cur.anulado) throw new Error(`El movimiento ${movNum} ya se encuentra anulado.`);
        if (cur.conciliado) throw new Error(`No puede anular un movimiento conciliado (${movNum}).`);

        const trReq = new sql.Request(transaction);
        trReq.input('Mov', sql.Char(20), padProfit(movNum, 20));
        const trRes = await trReq.query(`
            SELECT TOP 1
                RTRIM(co_trans_ban) AS co_trans_ban,
                RTRIM(mov_ban_origen) AS mov_ban_origen,
                RTRIM(mov_ban_destino) AS mov_ban_destino,
                RTRIM(ISNULL(mov_ban_comision, '')) AS mov_ban_comision,
                rowguid
            FROM saTransferenciaEntreCuentas
            WHERE mov_ban_origen = @Mov OR mov_ban_destino = @Mov OR mov_ban_comision = @Mov
        `);

        const accountsToSync = new Set([cur.cod_cta]);
        const movsToVoid = [cur.mov_num];
        let transferCode = null;

        if (trRes.recordset.length > 0) {
            const tr = trRes.recordset[0];
            transferCode = tr.co_trans_ban;

            const chkReq = new sql.Request(transaction);
            chkReq.input('M1', sql.Char(20), padProfit(tr.mov_ban_origen, 20));
            chkReq.input('M2', sql.Char(20), padProfit(tr.mov_ban_destino, 20));
            chkReq.input('M3', sql.Char(20), tr.mov_ban_comision ? padProfit(tr.mov_ban_comision, 20) : null);
            const legs = await chkReq.query(`
                SELECT RTRIM(mov_num) AS mov_num, RTRIM(cod_cta) AS cod_cta, conciliado, anulado
                FROM saMovimientoBanco
                WHERE mov_num IN (@M1, @M2) OR (@M3 IS NOT NULL AND mov_num = @M3)
            `);
            for (const l of legs.recordset) {
                if (l.conciliado) {
                    throw new Error(`El movimiento vinculado ${l.mov_num} de la transferencia ${transferCode} ya está conciliado.`);
                }
                accountsToSync.add(l.cod_cta);
                if (!l.anulado && !movsToVoid.includes(l.mov_num)) movsToVoid.push(l.mov_num);
            }

            const updTr = new sql.Request(transaction);
            updTr.input('CoTrans', sql.Char(20), padProfit(transferCode, 20));
            updTr.input('User', sql.Char(6), padProfit(profitUser, 6));
            updTr.input('Sucu', sql.Char(6), padProfit(sucuCode, 6));
            await updTr.query(`
                UPDATE saTransferenciaEntreCuentas
                SET procesado = 0, co_us_mo = @User, co_sucu_mo = @Sucu, fe_us_mo = GETDATE()
                WHERE co_trans_ban = @CoTrans
            `);
        }

        for (const m of movsToVoid) {
            const vReq = new sql.Request(transaction);
            vReq.input('Mov', sql.Char(20), padProfit(m, 20));
            vReq.input('User', sql.Char(6), padProfit(profitUser, 6));
            vReq.input('Sucu', sql.Char(6), padProfit(sucuCode, 6));
            const vRes = await vReq.query(`
                UPDATE saMovimientoBanco
                SET anulado = 1, co_us_mo = @User, co_sucu_mo = @Sucu, fe_us_mo = GETDATE()
                OUTPUT inserted.rowguid, inserted.fe_us_mo
                WHERE mov_num = @Mov AND anulado = 0
            `);
            if (vRes.recordset.length > 0) {
                await insertPista(transaction, {
                    user: profitUser,
                    fecha: vRes.recordset[0].fe_us_mo,
                    sucu: sucuCode,
                    tabla: 'saMovimientoBanco',
                    rowguid: vRes.recordset[0].rowguid,
                    tipoOp: 'A',
                    campos: `ANULACION ${m}`
                });
            }
        }

        for (const cta of accountsToSync) {
            await syncSaldoBanco(transaction, cta);
        }

        return {
            mov_num: movNum,
            transfer_code: transferCode,
            voided_movs: movsToVoid,
            message: transferCode
                ? `Transferencia ${transferCode} y sus movimientos (${movsToVoid.join(', ')}) anulados con éxito.`
                : `Movimiento ${movNum} anulado con éxito.`
        };
    });
}

router.post('/movimientos/:mov_num/anular', handleAnularMovimiento);
router.patch('/movimientos/:mov_num/anular', handleAnularMovimiento);

// ============================================================================
// 7. ELIMINAR MOVIMIENTO BANCARIO FÍSICAMENTE (Y TRANSFERENCIA SI APLICA)
// ============================================================================
async function handleEliminarMovimiento(req, res) {
    const movNum = decodeURIComponent(req.params.mov_num || '').trim();
    if (!movNum) return res.status(400).json({ success: false, message: 'Número de movimiento requerido.' });
    const profitUser = getProfitUser(req, req.body?.co_us_mo);

    await withTransaction(req, res, async (transaction, srv) => {
        const sucuCode = getSucuCode(srv, req.body?.co_sucu_mo);

        const curReq = new sql.Request(transaction);
        curReq.input('Mov', sql.Char(20), padProfit(movNum, 20));
        const curRes = await curReq.query(`
            SELECT RTRIM(mov_num) AS mov_num, RTRIM(cod_cta) AS cod_cta, RTRIM(origen) AS origen, conciliado, validador, rowguid
            FROM saMovimientoBanco WHERE mov_num = @Mov
        `);
        if (curRes.recordset.length === 0) throw new Error(`Movimiento ${movNum} no encontrado.`);
        const cur = curRes.recordset[0];
        if (cur.origen !== 'BAN') throw new Error(`Solo pueden eliminarse movimientos de origen BAN desde este módulo (origen: ${cur.origen}).`);
        if (cur.conciliado) throw new Error(`No puede eliminar un movimiento conciliado (${movNum}).`);

        const trReq = new sql.Request(transaction);
        trReq.input('Mov', sql.Char(20), padProfit(movNum, 20));
        const trRes = await trReq.query(`
            SELECT TOP 1
                RTRIM(co_trans_ban) AS co_trans_ban,
                RTRIM(mov_ban_origen) AS mov_ban_origen,
                RTRIM(mov_ban_destino) AS mov_ban_destino,
                RTRIM(ISNULL(mov_ban_comision, '')) AS mov_ban_comision,
                validador,
                rowguid
            FROM saTransferenciaEntreCuentas
            WHERE mov_ban_origen = @Mov OR mov_ban_destino = @Mov OR mov_ban_comision = @Mov
        `);

        const accountsToSync = new Set([cur.cod_cta]);
        const movsToDelete = [];
        let transferCode = null;

        if (trRes.recordset.length > 0) {
            const tr = trRes.recordset[0];
            transferCode = tr.co_trans_ban;

            const legsReq = new sql.Request(transaction);
            legsReq.input('M1', sql.Char(20), padProfit(tr.mov_ban_origen, 20));
            legsReq.input('M2', sql.Char(20), padProfit(tr.mov_ban_destino, 20));
            legsReq.input('M3', sql.Char(20), tr.mov_ban_comision ? padProfit(tr.mov_ban_comision, 20) : null);
            const legsRes = await legsReq.query(`
                SELECT RTRIM(mov_num) AS mov_num, RTRIM(cod_cta) AS cod_cta, conciliado, validador, rowguid
                FROM saMovimientoBanco
                WHERE mov_num IN (@M1, @M2) OR (@M3 IS NOT NULL AND mov_num = @M3)
            `);
            for (const l of legsRes.recordset) {
                if (l.conciliado) {
                    throw new Error(`El movimiento ${l.mov_num} de la transferencia ${transferCode} está conciliado y no puede eliminarse.`);
                }
                accountsToSync.add(l.cod_cta);
                movsToDelete.push(l);
            }

            const delTr = new sql.Request(transaction);
            delTr.input('sco_trans_banOri', sql.Char(20), padProfit(transferCode, 20));
            delTr.input('tsValidador', sql.VarBinary(8), tr.validador);
            delTr.input('sMaquina', sql.VarChar(60), 'SYNC2K');
            delTr.input('sCo_Us_Mo', sql.Char(6), padProfit(profitUser, 6));
            delTr.input('sCo_Sucu_Mo', sql.Char(6), padProfit(sucuCode, 6));
            delTr.input('gRowguid', sql.UniqueIdentifier, tr.rowguid);
            await delTr.execute('pEliminarTransferenciaEntreCuentas');
        } else {
            movsToDelete.push(cur);
        }

        for (const m of movsToDelete) {
            const delMov = new sql.Request(transaction);
            delMov.input('sMov_NumOri', sql.Char(20), padProfit(m.mov_num, 20));
            delMov.input('tsValidador', sql.VarBinary(8), m.validador);
            delMov.input('sMaquina', sql.VarChar(60), 'SYNC2K');
            delMov.input('sCo_Us_Mo', sql.Char(6), padProfit(profitUser, 6));
            delMov.input('sCo_Sucu_Mo', sql.Char(6), padProfit(sucuCode, 6));
            delMov.input('gRowguid', sql.UniqueIdentifier, m.rowguid);
            await delMov.execute('pEliminarMovimientoBanco');
        }

        for (const cta of accountsToSync) {
            await syncSaldoBanco(transaction, cta);
        }

        return {
            mov_num: movNum,
            transfer_code: transferCode,
            deleted_movs: movsToDelete.map(x => x.mov_num),
            message: transferCode
                ? `Transferencia ${transferCode} y sus movimientos (${movsToDelete.map(x => x.mov_num).join(', ')}) eliminados con éxito.`
                : `Movimiento ${movNum} eliminado con éxito.`
        };
    });
}

router.delete('/movimientos/:mov_num', handleEliminarMovimiento);
router.post('/movimientos/:mov_num/eliminar', handleEliminarMovimiento);

// ============================================================================
// 8. TRANSFERENCIAS ENTRE CUENTAS (CONSULTA, CREAR Y EDITAR)
// ============================================================================
router.get('/transferencias/:co_trans_ban', async (req, res) => {
    try {
        const ctx = await resolvePool(req, res);
        if (!ctx) return;
        const { pool } = ctx;

        const coTransBan = decodeURIComponent(req.params.co_trans_ban || '').trim();
        if (!coTransBan) return res.status(400).json({ success: false, message: 'Código de transferencia requerido.' });

        const trReq = pool.request();
        trReq.input('CoTrans', sql.Char(20), padProfit(coTransBan, 20));
        const trRes = await trReq.query(`
            SELECT TOP 1
                RTRIM(co_trans_ban) AS co_trans_ban,
                RTRIM(des_trans_ban) AS des_trans_ban,
                fecha,
                procesado,
                monto,
                comision,
                RTRIM(cta_origen) AS cta_origen,
                RTRIM(cta_ingr_egr_origen) AS cta_ingr_egr_origen,
                fecha_origen,
                RTRIM(mov_ban_origen) AS mov_ban_origen,
                RTRIM(ISNULL(referencia_origen, '')) AS referencia_origen,
                RTRIM(ISNULL(cta_comision, '')) AS cta_comision,
                RTRIM(ISNULL(cta_ingr_egr_comision, '')) AS cta_ingr_egr_comision,
                RTRIM(ISNULL(mov_ban_comision, '')) AS mov_ban_comision,
                RTRIM(ISNULL(referencia_comis, '')) AS referencia_comis,
                RTRIM(cta_destino) AS cta_destino,
                RTRIM(cta_ingr_egr_destino) AS cta_ingr_egr_destino,
                fecha_destino,
                RTRIM(mov_ban_destino) AS mov_ban_destino,
                RTRIM(ISNULL(referencia_destino, '')) AS referencia_destino,
                tasa,
                tasa_origen,
                RTRIM(co_us_in) AS co_us_in,
                fe_us_in
            FROM saTransferenciaEntreCuentas
            WHERE co_trans_ban = @CoTrans
        `);

        if (trRes.recordset.length === 0) {
            return res.status(404).json({ success: false, message: `Transferencia ${coTransBan} no encontrada.` });
        }

        const tr = trRes.recordset[0];
        const legsReq = pool.request();
        legsReq.input('Orig', sql.Char(20), padProfit(tr.mov_ban_origen, 20));
        legsReq.input('Dest', sql.Char(20), padProfit(tr.mov_ban_destino, 20));
        legsReq.input('Com', sql.Char(20), tr.mov_ban_comision ? padProfit(tr.mov_ban_comision, 20) : null);
        const legsQ = await legsReq.query(`
            SELECT
                RTRIM(mb.mov_num) AS mov_num,
                RTRIM(mb.cod_cta) AS cod_cta,
                RTRIM(cb.num_cta) AS num_cta,
                RTRIM(b.des_ban) AS des_ban,
                RTRIM(cb.co_mone) AS co_mone,
                RTRIM(mb.tipo_op) AS tipo_op,
                RTRIM(mb.doc_num) AS doc_num,
                mb.monto_d,
                mb.monto_h,
                mb.idb,
                mb.tasa,
                mb.conciliado,
                mb.anulado
            FROM saMovimientoBanco mb
            INNER JOIN saCuentaBancaria cb ON mb.cod_cta = cb.cod_cta
            INNER JOIN saBanco b ON cb.co_ban = b.co_ban
            WHERE mb.mov_num IN (@Orig, @Dest) OR (@Com IS NOT NULL AND mb.mov_num = @Com)
        `);

        res.json({
            success: true,
            data: {
                ...tr,
                movimientos_vinculados: legsQ.recordset
            }
        });
    } catch (err) {
        console.error('❌ [BANCOS] Error en GET /transferencias/:co_trans_ban:', err.message);
        res.status(500).json({ success: false, message: err.message });
    }
});

router.post('/transferencias', async (req, res) => {
    const payload = req.body?.transferencia || req.body || {};

    const ctaOrigen = String(payload.cta_origen || '').trim();
    const ctaDestino = String(payload.cta_destino || '').trim();
    const ctaComision = String(payload.cta_comision || ctaOrigen).trim();
    const montoOrigenInput = Number(payload.monto || 0);
    const comisionInput = Math.max(0, Number(payload.comision || 0));
    const tasaCambio = Number(payload.tasa || 1) > 0 ? Number(payload.tasa) : 1;
    const montoDestinoExplicit = payload.monto_destino !== undefined && payload.monto_destino !== null && Number(payload.monto_destino) > 0
        ? Number(payload.monto_destino)
        : null;

    const fecha = parseProfitDate(payload.fecha);
    const fechaOrigen = payload.fecha_origen ? parseProfitDate(payload.fecha_origen) : fecha;
    const fechaDestino = payload.fecha_destino ? parseProfitDate(payload.fecha_destino) : fecha;
    const manualCoTransBan = payload.co_trans_ban ? String(payload.co_trans_ban).trim().substring(0, 20) : '';
    const descripRaw = payload.des_trans_ban ? String(payload.des_trans_ban).trim() : '';

    const ctaIngEgrOrigen = String(payload.cta_ingr_egr_origen || '02').trim().substring(0, 20);
    const ctaIngEgrDestino = String(payload.cta_ingr_egr_destino || '01').trim().substring(0, 20);
    const ctaIngEgrComision = String(payload.cta_ingr_egr_comision || '02').trim().substring(0, 20);
    const profitUser = getProfitUser(req, payload.co_us_in);

    if (!ctaOrigen || !ctaDestino) {
        return res.status(400).json({ success: false, message: 'Debe seleccionar la cuenta origen y la cuenta destino.' });
    }
    if (ctaOrigen === ctaDestino) {
        return res.status(400).json({ success: false, message: 'La cuenta origen y destino no pueden ser la misma.' });
    }
    if (montoOrigenInput <= 0) {
        return res.status(400).json({ success: false, message: 'El monto a transferir debe ser mayor a cero.' });
    }

    await withTransaction(req, res, async (transaction, srv) => {
        const sucuCode = getSucuCode(srv, payload.co_sucu_in);

        const ctasReq = new sql.Request(transaction);
        ctasReq.input('Orig', sql.Char(6), padProfit(ctaOrigen, 6));
        ctasReq.input('Dest', sql.Char(6), padProfit(ctaDestino, 6));
        ctasReq.input('Com', sql.Char(6), padProfit(ctaComision, 6));
        const ctasRes = await ctasReq.query(`
            SELECT RTRIM(cod_cta) AS cod_cta, RTRIM(co_ban) AS co_ban, RTRIM(co_mone) AS co_mone, inactivo
            FROM saCuentaBancaria
            WHERE cod_cta IN (@Orig, @Dest, @Com)
        `);

        const ctaMap = new Map(ctasRes.recordset.map(c => [c.cod_cta, c]));
        const oInfo = ctaMap.get(ctaOrigen);
        const dInfo = ctaMap.get(ctaDestino);
        const cInfo = ctaMap.get(ctaComision);
        if (!oInfo || !dInfo) throw new Error('Cuenta bancaria origen o destino no encontrada.');
        if (oInfo.inactivo || dInfo.inactivo) throw new Error('Una de las cuentas seleccionadas está inactiva.');

        let coTransBan = manualCoTransBan;
        if (coTransBan) {
            const dup = new sql.Request(transaction);
            dup.input('CoTrans', sql.Char(20), padProfit(coTransBan, 20));
            const dupRes = await dup.query(`SELECT 1 AS ex FROM saTransferenciaEntreCuentas WHERE co_trans_ban = @CoTrans`);
            if (dupRes.recordset.length > 0) {
                throw new Error(`La transferencia #${coTransBan} ya existe en Profit Plus.`);
            }
        } else {
            coTransBan = await generateUniqueTransferNum(transaction, sucuCode);
        }

        const origUSD = isCurrencyUSD(oInfo.co_mone);
        const destUSD = isCurrencyUSD(dInfo.co_mone);

        let montoDestinoFinal = montoOrigenInput;
        let tasaOrigen = origUSD ? tasaCambio : 1;
        let tasaDestino = destUSD ? tasaCambio : 1;

        if (origUSD !== destUSD) {
            if (montoDestinoExplicit !== null) {
                montoDestinoFinal = round2(montoDestinoExplicit);
            } else {
                montoDestinoFinal = origUSD
                    ? round2(montoOrigenInput * tasaCambio)
                    : round2(montoOrigenInput / tasaCambio);
            }
            tasaOrigen = origUSD ? tasaCambio : 1;
            tasaDestino = tasaCambio;
        } else if (montoDestinoExplicit !== null) {
            montoDestinoFinal = round2(montoDestinoExplicit);
        }

        const refOrigen = String(payload.referencia_origen || coTransBan).trim().substring(0, 60);
        const refDestino = String(payload.referencia_destino || coTransBan).trim().substring(0, 60);
        const refComis = comisionInput > 0
            ? String(payload.referencia_comis || refOrigen || coTransBan).trim().substring(0, 60)
            : null;

        const movDesc = descripRaw || `Transferencia entre cuentas N° ${coTransBan}`;
        const transDesc = (descripRaw || 'TRANSFERENCIA ENTRE CUENTAS').substring(0, 60);

        const movBanOrigen = await generateUniqueMovNum(transaction, sucuCode);
        await insertMovementSP(transaction, {
            mov_num: movBanOrigen,
            descrip: movDesc,
            cod_cta: ctaOrigen,
            fecha: fechaOrigen,
            fecha_che: fechaOrigen,
            tasa: tasaOrigen,
            tipo_op: 'TR',
            doc_num: refOrigen,
            monto: round2(montoOrigenInput),
            co_cta_ingr_egr: ctaIngEgrOrigen,
            cob_pag: coTransBan,
            idb: 0,
            co_us_in: profitUser,
            co_sucu_in: sucuCode
        });

        const movBanDestino = await generateUniqueMovNum(transaction, sucuCode);
        await insertMovementSP(transaction, {
            mov_num: movBanDestino,
            descrip: movDesc,
            cod_cta: ctaDestino,
            fecha: fechaDestino,
            fecha_che: fechaDestino,
            tasa: tasaDestino,
            tipo_op: 'TP',
            doc_num: refDestino,
            monto: round2(montoDestinoFinal),
            co_cta_ingr_egr: ctaIngEgrDestino,
            cob_pag: coTransBan,
            idb: 0,
            co_us_in: profitUser,
            co_sucu_in: sucuCode
        });

        let movBanComision = null;
        if (comisionInput > 0) {
            if (!cInfo) throw new Error('La cuenta de comisión no existe.');
            movBanComision = await generateUniqueMovNum(transaction, sucuCode);
            await insertMovementSP(transaction, {
                mov_num: movBanComision,
                descrip: `Comisión Transf. N° ${coTransBan} - ${movDesc}`.substring(0, 160),
                cod_cta: ctaComision,
                fecha: fechaOrigen,
                fecha_che: fechaOrigen,
                tasa: isCurrencyUSD(cInfo.co_mone) ? tasaCambio : 1,
                tipo_op: 'ND',
                doc_num: refComis,
                monto: round2(comisionInput),
                co_cta_ingr_egr: ctaIngEgrComision,
                cob_pag: coTransBan,
                idb: 0,
                co_us_in: profitUser,
                co_sucu_in: sucuCode
            });
        }

        const trReq = new sql.Request(transaction);
        trReq.input('sco_trans_ban', sql.Char(20), padProfit(coTransBan, 20));
        trReq.input('sdes_trans_ban', sql.Char(60), padProfit(transDesc, 60));
        trReq.input('dfecha', sql.DateTime, fecha);
        trReq.input('bprocesado', sql.Bit, 1);
        trReq.input('demonto', sql.Decimal(18, 2), round2(montoOrigenInput));
        trReq.input('decomision', sql.Decimal(18, 2), round2(comisionInput));
        trReq.input('scta_origen', sql.Char(6), padProfit(ctaOrigen, 6));
        trReq.input('scta_ingr_egr_origen', sql.Char(20), padProfit(ctaIngEgrOrigen, 20));
        trReq.input('dfecha_origen', sql.DateTime, fechaOrigen);
        trReq.input('smov_ban_origen', sql.Char(20), padProfit(movBanOrigen, 20));
        trReq.input('sreferencia_origen', sql.Char(60), padProfit(refOrigen, 60));
        trReq.input('scta_comision', sql.Char(6), comisionInput > 0 ? padProfit(ctaComision, 6) : null);
        trReq.input('scta_ingr_egr_comision', sql.Char(20), comisionInput > 0 ? padProfit(ctaIngEgrComision, 20) : null);
        trReq.input('smov_ban_comision', sql.Char(20), movBanComision ? padProfit(movBanComision, 20) : null);
        trReq.input('scta_destino', sql.Char(6), padProfit(ctaDestino, 6));
        trReq.input('scta_ingr_egr_destino', sql.Char(20), padProfit(ctaIngEgrDestino, 20));
        trReq.input('dfecha_destino', sql.DateTime, fechaDestino);
        trReq.input('smov_ban_destino', sql.Char(20), padProfit(movBanDestino, 20));
        trReq.input('sreferencia_destino', sql.Char(60), padProfit(refDestino, 60));
        trReq.input('sCampo1', sql.VarChar(60), null);
        trReq.input('sCampo2', sql.VarChar(60), null);
        trReq.input('sCampo3', sql.VarChar(60), null);
        trReq.input('sCampo4', sql.VarChar(60), null);
        trReq.input('sCampo5', sql.VarChar(60), null);
        trReq.input('sCampo6', sql.VarChar(60), null);
        trReq.input('sCampo7', sql.VarChar(60), null);
        trReq.input('sCampo8', sql.VarChar(60), null);
        trReq.input('sCo_Us_In', sql.Char(6), padProfit(profitUser, 6));
        trReq.input('sCo_Sucu_In', sql.Char(6), padProfit(sucuCode, 6));
        trReq.input('sMaquina', sql.VarChar(60), 'SYNC2K');
        trReq.input('sRevisado', sql.Char(1), null);
        trReq.input('sTrasnfe', sql.Char(1), null);
        trReq.input('sBan_Origen', sql.Char(6), padProfit(oInfo.co_ban, 6));
        trReq.input('sBan_Destino', sql.Char(6), padProfit(dInfo.co_ban, 6));
        trReq.input('deTasa', sql.Decimal(21, 8), tasaDestino);
        trReq.input('sreferencia_comis', sql.Char(60), refComis ? padProfit(refComis, 60) : null);
        trReq.input('deTasa_Origen', sql.Decimal(21, 8), tasaOrigen);
        await trReq.execute('pInsertarTransferenciaEntreCuentas');

        await syncSaldoBanco(transaction, ctaOrigen);
        await syncSaldoBanco(transaction, ctaDestino);
        if (comisionInput > 0 && ctaComision !== ctaOrigen && ctaComision !== ctaDestino) {
            await syncSaldoBanco(transaction, ctaComision);
        }

        return {
            co_trans_ban: coTransBan,
            mov_ban_origen: movBanOrigen,
            mov_ban_destino: movBanDestino,
            mov_ban_comision: movBanComision,
            monto_origen: round2(montoOrigenInput),
            monto_destino: round2(montoDestinoFinal),
            comision: round2(comisionInput),
            message: `Transferencia entre cuentas #${coTransBan} registrada con éxito.`
        };
    }, 201);
});

router.put('/transferencias/:co_trans_ban', async (req, res) => {
    const coTransBan = decodeURIComponent(req.params.co_trans_ban || '').trim();
    const payload = req.body?.transferencia || req.body || {};

    if (!coTransBan) return res.status(400).json({ success: false, message: 'Código de transferencia requerido.' });

    const ctaOrigen = String(payload.cta_origen || '').trim();
    const ctaDestino = String(payload.cta_destino || '').trim();
    const ctaComision = String(payload.cta_comision || ctaOrigen).trim();
    const montoOrigenInput = Number(payload.monto || 0);
    const comisionInput = Math.max(0, Number(payload.comision || 0));
    const tasaCambio = Number(payload.tasa || 1) > 0 ? Number(payload.tasa) : 1;
    const montoDestinoExplicit = payload.monto_destino !== undefined && payload.monto_destino !== null && Number(payload.monto_destino) > 0
        ? Number(payload.monto_destino)
        : null;

    const fecha = parseProfitDate(payload.fecha);
    const fechaOrigen = payload.fecha_origen ? parseProfitDate(payload.fecha_origen) : fecha;
    const fechaDestino = payload.fecha_destino ? parseProfitDate(payload.fecha_destino) : fecha;
    const descripRaw = payload.des_trans_ban ? String(payload.des_trans_ban).trim() : '';

    const ctaIngEgrOrigen = String(payload.cta_ingr_egr_origen || '02').trim().substring(0, 20);
    const ctaIngEgrDestino = String(payload.cta_ingr_egr_destino || '01').trim().substring(0, 20);
    const ctaIngEgrComision = String(payload.cta_ingr_egr_comision || '02').trim().substring(0, 20);
    const profitUser = getProfitUser(req, payload.co_us_mo);

    if (!ctaOrigen || !ctaDestino) {
        return res.status(400).json({ success: false, message: 'Debe seleccionar la cuenta origen y la cuenta destino.' });
    }
    if (ctaOrigen === ctaDestino) {
        return res.status(400).json({ success: false, message: 'La cuenta origen y destino no pueden ser la misma.' });
    }
    if (montoOrigenInput <= 0) {
        return res.status(400).json({ success: false, message: 'El monto a transferir debe ser mayor a cero.' });
    }

    await withTransaction(req, res, async (transaction, srv) => {
        const sucuCode = getSucuCode(srv, payload.co_sucu_mo);

        const trCurReq = new sql.Request(transaction);
        trCurReq.input('CoTrans', sql.Char(20), padProfit(coTransBan, 20));
        const trCurRes = await trCurReq.query(`
            SELECT
                RTRIM(co_trans_ban) AS co_trans_ban,
                procesado,
                RTRIM(cta_origen) AS cta_origen,
                RTRIM(mov_ban_origen) AS mov_ban_origen,
                RTRIM(cta_destino) AS cta_destino,
                RTRIM(mov_ban_destino) AS mov_ban_destino,
                RTRIM(ISNULL(cta_comision, '')) AS cta_comision,
                RTRIM(ISNULL(mov_ban_comision, '')) AS mov_ban_comision,
                RTRIM(ISNULL(co_sucu_in, '01')) AS co_sucu_in,
                validador,
                rowguid
            FROM saTransferenciaEntreCuentas
            WHERE co_trans_ban = @CoTrans
        `);
        if (trCurRes.recordset.length === 0) {
            throw new Error(`Transferencia ${coTransBan} no encontrada.`);
        }
        const curTr = trCurRes.recordset[0];
        if (!curTr.procesado) {
            throw new Error(`La transferencia ${coTransBan} está anulada / inactiva y no puede editarse.`);
        }

        // Validar movimientos vinculados (no anulados ni conciliados)
        const legsReq = new sql.Request(transaction);
        legsReq.input('M1', sql.Char(20), padProfit(curTr.mov_ban_origen, 20));
        legsReq.input('M2', sql.Char(20), padProfit(curTr.mov_ban_destino, 20));
        legsReq.input('M3', sql.Char(20), curTr.mov_ban_comision ? padProfit(curTr.mov_ban_comision, 20) : null);
        const legsRes = await legsReq.query(`
            SELECT RTRIM(mov_num) AS mov_num, RTRIM(cod_cta) AS cod_cta, conciliado, anulado, validador, rowguid
            FROM saMovimientoBanco
            WHERE mov_num IN (@M1, @M2) OR (@M3 IS NOT NULL AND mov_num = @M3)
        `);
        const legMap = new Map(legsRes.recordset.map(l => [l.mov_num, l]));
        const accountsToSync = new Set([curTr.cta_origen, curTr.cta_destino, ctaOrigen, ctaDestino]);
        if (curTr.cta_comision) accountsToSync.add(curTr.cta_comision);

        for (const l of legsRes.recordset) {
            if (l.conciliado) {
                throw new Error(`El movimiento vinculado ${l.mov_num} está conciliado y bloquea la edición de la transferencia.`);
            }
            if (l.anulado) {
                throw new Error(`El movimiento vinculado ${l.mov_num} está anulado y bloquea la edición.`);
            }
            accountsToSync.add(l.cod_cta);
        }

        const ctasReq = new sql.Request(transaction);
        ctasReq.input('Orig', sql.Char(6), padProfit(ctaOrigen, 6));
        ctasReq.input('Dest', sql.Char(6), padProfit(ctaDestino, 6));
        ctasReq.input('Com', sql.Char(6), padProfit(ctaComision, 6));
        const ctasRes = await ctasReq.query(`
            SELECT RTRIM(cod_cta) AS cod_cta, RTRIM(co_ban) AS co_ban, RTRIM(co_mone) AS co_mone, inactivo
            FROM saCuentaBancaria
            WHERE cod_cta IN (@Orig, @Dest, @Com)
        `);
        const ctaMap = new Map(ctasRes.recordset.map(c => [c.cod_cta, c]));
        const oInfo = ctaMap.get(ctaOrigen);
        const dInfo = ctaMap.get(ctaDestino);
        const cInfo = ctaMap.get(ctaComision);
        if (!oInfo || !dInfo) throw new Error('Cuenta bancaria origen o destino no encontrada.');

        const origUSD = isCurrencyUSD(oInfo.co_mone);
        const destUSD = isCurrencyUSD(dInfo.co_mone);

        let montoDestinoFinal = montoOrigenInput;
        let tasaOrigen = origUSD ? tasaCambio : 1;
        let tasaDestino = destUSD ? tasaCambio : 1;

        if (origUSD !== destUSD) {
            if (montoDestinoExplicit !== null) {
                montoDestinoFinal = round2(montoDestinoExplicit);
            } else {
                montoDestinoFinal = origUSD
                    ? round2(montoOrigenInput * tasaCambio)
                    : round2(montoOrigenInput / tasaCambio);
            }
            tasaOrigen = origUSD ? tasaCambio : 1;
            tasaDestino = tasaCambio;
        } else if (montoDestinoExplicit !== null) {
            montoDestinoFinal = round2(montoDestinoExplicit);
        }

        const refOrigen = String(payload.referencia_origen || coTransBan).trim().substring(0, 60);
        const refDestino = String(payload.referencia_destino || coTransBan).trim().substring(0, 60);
        const refComis = comisionInput > 0
            ? String(payload.referencia_comis || refOrigen || coTransBan).trim().substring(0, 60)
            : null;
        const movDesc = descripRaw || `Transferencia entre cuentas N° ${coTransBan}`;
        const transDesc = (descripRaw || 'TRANSFERENCIA ENTRE CUENTAS').substring(0, 60);

        // Actualizar movimiento origen (TR)
        const updOrig = new sql.Request(transaction);
        updOrig.input('Mov', sql.Char(20), padProfit(curTr.mov_ban_origen, 20));
        updOrig.input('CodCta', sql.Char(6), padProfit(ctaOrigen, 6));
        updOrig.input('CtaIE', sql.Char(20), padProfit(ctaIngEgrOrigen, 20));
        updOrig.input('Fecha', sql.SmallDateTime, fechaOrigen);
        updOrig.input('Tasa', sql.Decimal(21, 8), tasaOrigen);
        updOrig.input('DocNum', sql.VarChar(20), refOrigen.substring(0, 20));
        updOrig.input('Monto', sql.Decimal(18, 2), round2(montoOrigenInput));
        updOrig.input('Descrip', sql.VarChar(160), movDesc.substring(0, 160));
        updOrig.input('User', sql.Char(6), padProfit(profitUser, 6));
        updOrig.input('Sucu', sql.Char(6), padProfit(sucuCode, 6));
        await updOrig.query(`
            UPDATE saMovimientoBanco
            SET cod_cta = @CodCta, co_cta_ingr_egr = @CtaIE, fecha = @Fecha, fecha_che = @Fecha,
                tasa = @Tasa, doc_num = @DocNum, monto_d = @Monto, monto_h = 0, descrip = @Descrip,
                co_us_mo = @User, co_sucu_mo = @Sucu, fe_us_mo = GETDATE()
            WHERE mov_num = @Mov
        `);

        // Actualizar movimiento destino (TP)
        const updDest = new sql.Request(transaction);
        updDest.input('Mov', sql.Char(20), padProfit(curTr.mov_ban_destino, 20));
        updDest.input('CodCta', sql.Char(6), padProfit(ctaDestino, 6));
        updDest.input('CtaIE', sql.Char(20), padProfit(ctaIngEgrDestino, 20));
        updDest.input('Fecha', sql.SmallDateTime, fechaDestino);
        updDest.input('Tasa', sql.Decimal(21, 8), tasaDestino);
        updDest.input('DocNum', sql.VarChar(20), refDestino.substring(0, 20));
        updDest.input('Monto', sql.Decimal(18, 2), round2(montoDestinoFinal));
        updDest.input('Descrip', sql.VarChar(160), movDesc.substring(0, 160));
        updDest.input('User', sql.Char(6), padProfit(profitUser, 6));
        updDest.input('Sucu', sql.Char(6), padProfit(sucuCode, 6));
        await updDest.query(`
            UPDATE saMovimientoBanco
            SET cod_cta = @CodCta, co_cta_ingr_egr = @CtaIE, fecha = @Fecha, fecha_che = @Fecha,
                tasa = @Tasa, doc_num = @DocNum, monto_d = 0, monto_h = @Monto, descrip = @Descrip,
                co_us_mo = @User, co_sucu_mo = @Sucu, fe_us_mo = GETDATE()
            WHERE mov_num = @Mov
        `);

        // Manejar comisión (actualizar, crear o eliminar)
        let movBanComision = curTr.mov_ban_comision || null;
        if (comisionInput > 0) {
            if (!cInfo) throw new Error('La cuenta de comisión no existe.');
            accountsToSync.add(ctaComision);
            const tasaCom = isCurrencyUSD(cInfo.co_mone) ? tasaCambio : 1;
            const descCom = `Comisión Transf. N° ${coTransBan} - ${movDesc}`.substring(0, 160);

            if (movBanComision && legMap.has(movBanComision)) {
                const updCom = new sql.Request(transaction);
                updCom.input('Mov', sql.Char(20), padProfit(movBanComision, 20));
                updCom.input('CodCta', sql.Char(6), padProfit(ctaComision, 6));
                updCom.input('CtaIE', sql.Char(20), padProfit(ctaIngEgrComision, 20));
                updCom.input('Fecha', sql.SmallDateTime, fechaOrigen);
                updCom.input('Tasa', sql.Decimal(21, 8), tasaCom);
                updCom.input('DocNum', sql.VarChar(20), refComis.substring(0, 20));
                updCom.input('Monto', sql.Decimal(18, 2), round2(comisionInput));
                updCom.input('Descrip', sql.VarChar(160), descCom);
                updCom.input('User', sql.Char(6), padProfit(profitUser, 6));
                updCom.input('Sucu', sql.Char(6), padProfit(sucuCode, 6));
                await updCom.query(`
                    UPDATE saMovimientoBanco
                    SET cod_cta = @CodCta, co_cta_ingr_egr = @CtaIE, fecha = @Fecha, fecha_che = @Fecha,
                        tasa = @Tasa, doc_num = @DocNum, monto_d = @Monto, monto_h = 0, descrip = @Descrip,
                        co_us_mo = @User, co_sucu_mo = @Sucu, fe_us_mo = GETDATE()
                    WHERE mov_num = @Mov
                `);
            } else {
                movBanComision = await generateUniqueMovNum(transaction, sucuCode);
                await insertMovementSP(transaction, {
                    mov_num: movBanComision,
                    descrip: descCom,
                    cod_cta: ctaComision,
                    fecha: fechaOrigen,
                    fecha_che: fechaOrigen,
                    tasa: tasaCom,
                    tipo_op: 'ND',
                    doc_num: refComis,
                    monto: round2(comisionInput),
                    co_cta_ingr_egr: ctaIngEgrComision,
                    cob_pag: coTransBan,
                    idb: 0,
                    co_us_in: profitUser,
                    co_sucu_in: sucuCode
                });
            }
        } else if (movBanComision && legMap.has(movBanComision)) {
            const oldComLeg = legMap.get(movBanComision);
            // Liberar referencia antes de eliminar movimiento de comisión
            movBanComision = null;
            const clrReq = new sql.Request(transaction);
            clrReq.input('CoTrans', sql.Char(20), padProfit(coTransBan, 20));
            await clrReq.query(`
                UPDATE saTransferenciaEntreCuentas
                SET cta_comision = NULL, cta_ingr_egr_comision = NULL, mov_ban_comision = NULL, referencia_comis = NULL, comision = 0
                WHERE co_trans_ban = @CoTrans
            `);
            // Refrescar validador tras UPDATE previo si fuera necesario, y eliminar
            const delMov = new sql.Request(transaction);
            delMov.input('sMov_NumOri', sql.Char(20), padProfit(oldComLeg.mov_num, 20));
            delMov.input('tsValidador', sql.VarBinary(8), oldComLeg.validador);
            delMov.input('sMaquina', sql.VarChar(60), 'SYNC2K');
            delMov.input('sCo_Us_Mo', sql.Char(6), padProfit(profitUser, 6));
            delMov.input('sCo_Sucu_Mo', sql.Char(6), padProfit(sucuCode, 6));
            delMov.input('gRowguid', sql.UniqueIdentifier, oldComLeg.rowguid);
            await delMov.execute('pEliminarMovimientoBanco');

            // Releer validador de transferencia después del cleanup
            const refTrReq = new sql.Request(transaction);
            refTrReq.input('CoTrans', sql.Char(20), padProfit(coTransBan, 20));
            const refTrRes = await refTrReq.query(`SELECT validador FROM saTransferenciaEntreCuentas WHERE co_trans_ban = @CoTrans`);
            curTr.validador = refTrRes.recordset[0]?.validador || curTr.validador;
        }

        // Actualizar maestro saTransferenciaEntreCuentas con SP oficial
        const updTr = new sql.Request(transaction);
        updTr.input('sco_trans_ban', sql.Char(20), padProfit(coTransBan, 20));
        updTr.input('sco_trans_banOri', sql.Char(20), padProfit(coTransBan, 20));
        updTr.input('sdes_trans_ban', sql.Char(60), padProfit(transDesc, 60));
        updTr.input('dfecha', sql.DateTime, fecha);
        updTr.input('bprocesado', sql.Bit, 1);
        updTr.input('demonto', sql.Decimal(18, 2), round2(montoOrigenInput));
        updTr.input('decomision', sql.Decimal(18, 2), round2(comisionInput));
        updTr.input('scta_origen', sql.Char(6), padProfit(ctaOrigen, 6));
        updTr.input('scta_ingr_egr_origen', sql.Char(20), padProfit(ctaIngEgrOrigen, 20));
        updTr.input('dfecha_origen', sql.DateTime, fechaOrigen);
        updTr.input('smov_ban_origen', sql.Char(20), padProfit(curTr.mov_ban_origen, 20));
        updTr.input('sreferencia_origen', sql.Char(60), padProfit(refOrigen, 60));
        updTr.input('scta_comision', sql.Char(6), comisionInput > 0 ? padProfit(ctaComision, 6) : null);
        updTr.input('scta_ingr_egr_comision', sql.Char(20), comisionInput > 0 ? padProfit(ctaIngEgrComision, 20) : null);
        updTr.input('smov_ban_comision', sql.Char(20), movBanComision ? padProfit(movBanComision, 20) : null);
        updTr.input('scta_destino', sql.Char(6), padProfit(ctaDestino, 6));
        updTr.input('scta_ingr_egr_destino', sql.Char(20), padProfit(ctaIngEgrDestino, 20));
        updTr.input('dfecha_destino', sql.DateTime, fechaDestino);
        updTr.input('smov_ban_destino', sql.Char(20), padProfit(curTr.mov_ban_destino, 20));
        updTr.input('sreferencia_destino', sql.Char(60), padProfit(refDestino, 60));
        updTr.input('sCampo1', sql.VarChar(60), null);
        updTr.input('sCampo2', sql.VarChar(60), null);
        updTr.input('sCampo3', sql.VarChar(60), null);
        updTr.input('sCampo4', sql.VarChar(60), null);
        updTr.input('sCampo5', sql.VarChar(60), null);
        updTr.input('sCampo6', sql.VarChar(60), null);
        updTr.input('sCampo7', sql.VarChar(60), null);
        updTr.input('sCampo8', sql.VarChar(60), null);
        updTr.input('sCo_Sucu_In', sql.Char(6), padProfit(curTr.co_sucu_in, 6));
        updTr.input('sMaquina', sql.VarChar(60), 'SYNC2K');
        updTr.input('sRevisado', sql.Char(1), null);
        updTr.input('sCampos', sql.VarChar(sql.MAX), coTransBan);
        updTr.input('sTrasnfe', sql.Char(1), null);
        updTr.input('sCo_Us_Mo', sql.Char(6), padProfit(profitUser, 6));
        updTr.input('sCo_Sucu_Mo', sql.Char(6), padProfit(sucuCode, 6));
        updTr.input('tsValidador', sql.VarBinary(8), curTr.validador);
        updTr.input('growguid', sql.UniqueIdentifier, curTr.rowguid);
        updTr.input('sBan_Origen', sql.Char(6), padProfit(oInfo.co_ban, 6));
        updTr.input('sBan_Destino', sql.Char(6), padProfit(dInfo.co_ban, 6));
        updTr.input('deTasa', sql.Decimal(21, 8), tasaDestino);
        updTr.input('sreferencia_comis', sql.Char(60), refComis ? padProfit(refComis, 60) : null);
        updTr.input('deTasa_Origen', sql.Decimal(21, 8), tasaOrigen);
        await updTr.execute('pActualizarTransferenciaEntreCuentas');

        for (const cta of accountsToSync) {
            await syncSaldoBanco(transaction, cta);
        }

        return {
            co_trans_ban: coTransBan,
            mov_ban_origen: curTr.mov_ban_origen,
            mov_ban_destino: curTr.mov_ban_destino,
            mov_ban_comision: movBanComision,
            monto_origen: round2(montoOrigenInput),
            monto_destino: round2(montoDestinoFinal),
            comision: round2(comisionInput),
            message: `Transferencia entre cuentas #${coTransBan} actualizada con éxito.`
        };
    });
});

// ============================================================================
// 9. CONCILIACIÓN BANCARIA (CONSULTAR Y GUARDAR)
// ============================================================================
router.get('/conciliacion', async (req, res) => {
    try {
        const ctx = await resolvePool(req, res);
        if (!ctx) return;
        const { pool } = ctx;

        const codCta = String(req.query.cod_cta || '').trim();
        const now = new Date();
        const anio = parseInt(req.query.anio, 10) || now.getFullYear();
        const mes = parseInt(req.query.mes, 10) || (now.getMonth() + 1);

        if (!codCta) {
            return res.status(400).json({ success: false, message: 'Debe seleccionar una cuenta bancaria.' });
        }
        if (mes < 1 || mes > 12) {
            return res.status(400).json({ success: false, message: 'El mes debe estar entre 1 y 12.' });
        }

        let prevMes = mes - 1;
        let prevAnio = anio;
        if (prevMes === 0) {
            prevMes = 12;
            prevAnio -= 1;
        }

        const accReq = pool.request();
        accReq.input('CodCta', sql.Char(6), padProfit(codCta, 6));
        accReq.input('Anio', sql.Int, anio);
        accReq.input('Mes', sql.Int, mes);
        accReq.input('PrevAnio', sql.Int, prevAnio);
        accReq.input('PrevMes', sql.Int, prevMes);

        const headerRes = await accReq.query(`
            SELECT
                RTRIM(cb.cod_cta) AS cod_cta,
                RTRIM(cb.num_cta) AS num_cta,
                RTRIM(b.des_ban) AS des_ban,
                RTRIM(cb.co_mone) AS co_mone,
                RTRIM(cb.tipo_cu) AS tipo_cu,
                ISNULL(tf.saldo, 0) AS saldo_total_libros,
                ISNULL(cf.saldo, 0) AS saldo_conciliado_global,
                ISNULL(ti.saldo, 0) AS saldo_inicial_cuenta,
                ISNULL(dbo.saSaldoConciliacion(cb.cod_cta, cb.cod_cta, @PrevAnio, @PrevMes), 0) AS saldo_inicial_conciliado,
                ISNULL(dbo.saSaldoConciliacion(cb.cod_cta, cb.cod_cta, @Anio, @Mes), 0) AS saldo_final_conciliado
            FROM saCuentaBancaria cb
            INNER JOIN saBanco b ON cb.co_ban = b.co_ban
            LEFT JOIN saSaldoBanco tf ON cb.cod_cta = tf.cod_cta AND tf.tipo = 'TF'
            LEFT JOIN saSaldoBanco cf ON cb.cod_cta = cf.cod_cta AND cf.tipo = 'CF'
            LEFT JOIN saSaldoBanco ti ON cb.cod_cta = ti.cod_cta AND ti.tipo = 'TI'
            WHERE cb.cod_cta = @CodCta
        `);

        if (headerRes.recordset.length === 0) {
            return res.status(404).json({ success: false, message: `Cuenta bancaria ${codCta} no encontrada.` });
        }

        const cuenta = headerRes.recordset[0];

        const movsReq = pool.request();
        movsReq.input('CodCta', sql.Char(6), padProfit(codCta, 6));
        movsReq.input('Anio', sql.Int, anio);
        movsReq.input('Mes', sql.Int, mes);

        const movsRes = await movsReq.query(`
            SELECT DISTINCT
                RTRIM(mb.mov_num) AS mov_num,
                RTRIM(ISNULL(mb.descrip, '')) AS descrip,
                RTRIM(mb.cod_cta) AS cod_cta,
                RTRIM(mb.co_cta_ingr_egr) AS co_cta_ingr_egr,
                RTRIM(ISNULL(cie.descrip, '')) AS cta_ing_egr_des,
                mb.fecha,
                mb.fecha_che,
                mb.tasa,
                RTRIM(mb.tipo_op) AS tipo_op,
                RTRIM(mb.doc_num) AS doc_num,
                mb.monto_d,
                mb.monto_h,
                mb.idb,
                CASE
                    WHEN RTRIM(mb.tipo_op) IN ('CH', 'ND', 'RC', 'TR') THEN mb.monto_d
                    WHEN RTRIM(mb.tipo_op) = 'ID' THEN mb.idb
                    ELSE mb.monto_h
                END AS monto,
                CASE
                    WHEN RTRIM(mb.tipo_op) = 'ID' THEN 'IDB'
                    WHEN mb.monto_h > 0 OR RTRIM(mb.tipo_op) IN ('DP', 'NC', 'TP') THEN 'INGRESO'
                    ELSE 'EGRESO'
                END AS naturaleza,
                mb.saldo_ini,
                RTRIM(mb.origen) AS origen,
                RTRIM(ISNULL(mb.cob_pag, '')) AS cob_pag,
                RTRIM(ISNULL(mb.dep_num, '')) AS dep_num,
                ISNULL(mb.conciliado, 0) AS conciliado,
                mb.fec_con,
                mb.anulado,
                ISNULL(cb.con_auto, 0) AS con_auto,
                MONTH(mb.fecha) AS mes_mov,
                YEAR(mb.fecha) AS anio_mov,
                RTRIM(mb.co_us_in) AS co_us_in
            FROM saMovimientoBanco mb
            LEFT JOIN saCuentaIngEgr cie ON mb.co_cta_ingr_egr = cie.co_cta_ingr_egr
            LEFT JOIN saConcBanco cb ON mb.mov_num = cb.mov_num
            WHERE
                mb.anulado = 0
                AND mb.cod_cta = @CodCta
                AND (
                    (mb.conciliado = 1 AND YEAR(mb.fec_con) = @Anio AND MONTH(mb.fec_con) = @Mes)
                    OR (mb.conciliado = 0 AND ((YEAR(mb.fecha) = @Anio AND MONTH(mb.fecha) <= @Mes) OR YEAR(mb.fecha) < @Anio))
                )
            ORDER BY mb.fecha DESC, mov_num DESC
        `);

        let ingresosConciliados = 0;
        let egresosConciliados = 0;
        let idbConciliados = 0;
        let ingresosPendientes = 0;
        let egresosPendientes = 0;
        let idbPendientes = 0;

        const movimientos = movsRes.recordset.map(m => {
            const d = Number(m.monto_d || 0);
            const h = Number(m.monto_h || 0);
            const idbVal = Number(m.idb || 0);
            const idbEffect = h > 0 ? idbVal : -idbVal;
            const netoConciliacion = round2(h - d + idbEffect);

            if (m.conciliado) {
                if (!m.saldo_ini) {
                    ingresosConciliados += h;
                    egresosConciliados += d;
                    idbConciliados += idbVal;
                }
            } else {
                if (!m.saldo_ini) {
                    ingresosPendientes += h;
                    egresosPendientes += d;
                    idbPendientes += idbVal;
                }
            }

            return {
                ...m,
                tipo_op_label: TIPO_OP_LABELS[m.tipo_op] || m.tipo_op,
                efecto_neto: netoConciliacion
            };
        });

        res.json({
            success: true,
            cuenta,
            periodo: { anio, mes, prev_anio: prevAnio, prev_mes: prevMes },
            movimientos,
            resumen: {
                saldo_inicial_conciliado: Number(cuenta.saldo_inicial_conciliado || 0),
                saldo_final_conciliado: Number(cuenta.saldo_final_conciliado || 0),
                saldo_total_libros: Number(cuenta.saldo_total_libros || 0),
                saldo_conciliado_global: Number(cuenta.saldo_conciliado_global || 0),
                ingresos_conciliados: round2(ingresosConciliados),
                egresos_conciliados: round2(egresosConciliados),
                idb_conciliados: round2(idbConciliados),
                ingresos_pendientes: round2(ingresosPendientes),
                egresos_pendientes: round2(egresosPendientes),
                idb_pendientes: round2(idbPendientes),
                total_movimientos: movimientos.length,
                cantidad_conciliados: movimientos.filter(m => m.conciliado).length,
                cantidad_pendientes: movimientos.filter(m => !m.conciliado).length
            }
        });
    } catch (err) {
        console.error('❌ [BANCOS] Error en GET /conciliacion:', err.message);
        res.status(500).json({ success: false, message: 'Error cargando conciliación bancaria: ' + err.message });
    }
});

router.post('/conciliacion/guardar', async (req, res) => {
    const payload = req.body || {};

    const codCta = String(payload.cod_cta || '').trim();
    const anio = parseInt(payload.anio, 10);
    const mes = parseInt(payload.mes, 10);
    const cambios = Array.isArray(payload.movimientos) ? payload.movimientos : [];
    const profitUser = getProfitUser(req, payload.co_us_mo);

    if (!codCta) return res.status(400).json({ success: false, message: 'Cuenta bancaria requerida.' });
    if (!anio || !mes || mes < 1 || mes > 12) return res.status(400).json({ success: false, message: 'Periodo mes/año inválido.' });
    if (cambios.length === 0) return res.status(400).json({ success: false, message: 'No hay cambios de conciliación para guardar.' });

    const fecConDate = new Date(anio, mes - 1, 1, 0, 0, 0, 0);

    await withTransaction(req, res, async (transaction, srv) => {
        const sucuCode = getSucuCode(srv, payload.co_sucu_mo);

        let updatedCount = 0;
        for (const item of cambios) {
            const movNum = String(item.mov_num || '').trim();
            const isConc = Boolean(item.conciliado);
            if (!movNum) continue;

            const upd = new sql.Request(transaction);
            upd.input('Mov', sql.Char(20), padProfit(movNum, 20));
            upd.input('CodCta', sql.Char(6), padProfit(codCta, 6));
            upd.input('Conc', sql.Bit, isConc ? 1 : 0);
            upd.input('FecCon', sql.SmallDateTime, isConc ? fecConDate : null);
            upd.input('User', sql.Char(6), padProfit(profitUser, 6));
            upd.input('Sucu', sql.Char(6), padProfit(sucuCode, 6));

            const updRes = await upd.query(`
                UPDATE saMovimientoBanco
                SET
                    conciliado = @Conc,
                    fec_con = @FecCon,
                    co_us_mo = @User,
                    co_sucu_mo = @Sucu,
                    fe_us_mo = GETDATE()
                OUTPUT inserted.rowguid, inserted.fe_us_mo
                WHERE mov_num = @Mov AND cod_cta = @CodCta AND anulado = 0
            `);

            if (updRes.recordset.length > 0) {
                updatedCount++;
                await insertPista(transaction, {
                    user: profitUser,
                    fecha: updRes.recordset[0].fe_us_mo,
                    sucu: sucuCode,
                    tabla: 'saMovimientoBanco',
                    rowguid: updRes.recordset[0].rowguid,
                    tipoOp: 'M',
                    campos: `CONCILIACION ${anio}-${String(mes).padStart(2, '0')} => ${isConc ? 'SI' : 'NO'}`
                });
            }
        }

        await syncSaldoBanco(transaction, codCta);

        const salReq = new sql.Request(transaction);
        salReq.input('CodCta', sql.Char(6), padProfit(codCta, 6));
        const salRes = await salReq.query(`
            SELECT tipo, saldo FROM saSaldoBanco WHERE cod_cta = @CodCta AND tipo IN ('TF', 'CF')
        `);
        const saldoTF = salRes.recordset.find(r => r.tipo === 'TF')?.saldo || 0;
        const saldoCF = salRes.recordset.find(r => r.tipo === 'CF')?.saldo || 0;

        return {
            cod_cta: codCta,
            anio,
            mes,
            updated_count: updatedCount,
            saldo_total_libros: Number(saldoTF),
            saldo_conciliado_global: Number(saldoCF),
            message: `Conciliación bancaria guardada (${updatedCount} movimientos actualizados).`
        };
    });
});

module.exports = router;
