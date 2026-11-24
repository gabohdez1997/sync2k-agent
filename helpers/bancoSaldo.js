const { sql } = require('../db');
const { padProfit } = require('./multiSede');
const { getProximoConsecutivo } = require('./consecutivos');

function round2(n) {
    return Math.round((Number(n || 0) + Number.EPSILON) * 100) / 100;
}

function isCurrencyUSD(coMone) {
    const m = String(coMone || '').trim().toUpperCase();
    return m !== '' && m !== 'BS' && m !== 'BSS' && m !== 'BS.' && m !== 'VEF' && m !== 'VES';
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

async function insertMovimientoBancoSP(runner, params) {
    const req = new sql.Request(runner);
    const origenTrim = String(params.origen || 'BAN').trim().toUpperCase().substring(0, 3);
    req.input('sMov_Num', sql.Char(20), padProfit(params.mov_num, 20));
    req.input('sDescrip', sql.VarChar(160), params.descrip ? String(params.descrip).trim().substring(0, 160) : null);
    req.input('sCod_Cta', sql.Char(6), padProfit(params.cod_cta, 6));
    req.input('sdFecha', sql.SmallDateTime, params.fecha);
    req.input('deTasa', sql.Decimal(21, 8), Number(params.tasa || 1));
    req.input('sTipo_Op', sql.Char(2), padProfit(params.tipo_op, 2));
    req.input('sDoc_Num', sql.VarChar(20), String(params.doc_num || params.mov_num).trim().substring(0, 20));
    req.input('deMonto', sql.Decimal(18, 2), round2(params.monto));
    req.input('sCo_Cta_Ingr_Egr', sql.Char(20), padProfit(params.co_cta_ingr_egr, 20));
    req.input('sOrigen', sql.Char(3), padProfit(origenTrim, 3));
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
    req.input('sMaquina', sql.VarChar(60), params.maquina || 'SYNC2K');
    req.input('sRevisado', sql.Char(1), null);
    req.input('sTrasnfe', sql.Char(1), null);
    await req.execute('pInsertarMovimientoBanco');
}

module.exports = {
    round2,
    isCurrencyUSD,
    syncSaldoBanco,
    generateUniqueMovNum,
    insertMovimientoBancoSP
};

