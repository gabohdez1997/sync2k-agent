const express = require('express');
const router = express.Router();
const { sql, getPool, getServers } = require('../db');

function padProfit(str, length) {
    if (!str) return ' '.repeat(length);
    return str.toString().trim().padEnd(length, ' ');
}

/**
 * POST /api/v1/ajustes
 * Registra un Ajuste de Inventario en Profit Plus (Salida '02' o Entrada '01')
 */
router.post('/', async (req, res) => {
    const data = req.body;
    console.log('[AJUSTES HIT] Creando ajuste de inventario:', JSON.stringify({
        branch_id: data.branch_id,
        tipo: data.tipo,
        motivo: data.motivo,
        renglones_count: data.renglones?.length
    }));

    if (!data.branch_id) {
        return res.status(400).json({ success: false, message: 'El parámetro branch_id es requerido.' });
    }

    if (!data.renglones || !Array.isArray(data.renglones) || data.renglones.length === 0) {
        return res.status(400).json({ success: false, message: 'Debe incluir al menos un renglón de artículo para el ajuste.' });
    }

    try {
        const pool = await getPool(data.branch_id, req.sqlAuth);
        const transaction = new sql.Transaction(pool);
        await transaction.begin();

        try {
            // 1. Determinar el tipo de ajuste (02 = Salida, 01 = Entrada)
            const isSalida = String(data.tipo || '').toUpperCase() === 'SAL' || String(data.co_tipo || '').trim() === '02';
            const coTipo = isSalida ? '02' : '01';

            // 2. Obtener el próximo consecutivo para AJUS_NUM
            let ajueNum = null;
            try {
                const consecRes = await transaction.request()
                    .input('sCo_Consecutivo', sql.Char(16), padProfit('AJUS_NUM', 16))
                    .execute('pConsecutivoProximo');
                if (consecRes.recordset && consecRes.recordset[0]?.ProximoConsecutivo) {
                    ajueNum = consecRes.recordset[0].ProximoConsecutivo.trim();
                }
            } catch (e) {
                console.warn('[AJUSTES] Falló pConsecutivoProximo para AJUS_NUM, intentando vía saSerie:', e.message);
            }

            if (!ajueNum) {
                const resCorr = await transaction.request().query(`
                    UPDATE saSerie
                    SET prox_n = prox_n + 1, fe_us_mo = GETDATE()
                    OUTPUT INSERTED.prox_n, RTRIM(INSERTED.desde_a) as prefijo
                    WHERE co_serie = (
                        SELECT TOP 1 co_serie
                        FROM saConsecutivo
                        WHERE UPPER(LTRIM(RTRIM(co_consecutivo))) IN ('AJUS_NUM', 'AJUS', 'AJUSTE')
                           OR UPPER(LTRIM(RTRIM(co_consecutivo))) LIKE '%AJU%'
                    )
                `);
                const corrRow = resCorr.recordset[0] || null;
                if (corrRow && corrRow.prox_n) {
                    const proxN = Number(corrRow.prox_n || 0);
                    ajueNum = proxN.toString().padStart(8, '0');
                } else {
                    const resDirect = await transaction.request().query(`
                        UPDATE saSerie
                        SET prox_n = prox_n + 1, fe_us_mo = GETDATE()
                        OUTPUT INSERTED.prox_n
                        WHERE LTRIM(RTRIM(co_serie)) = '001'
                    `);
                    if (resDirect.recordset && resDirect.recordset.length > 0) {
                        ajueNum = Number(resDirect.recordset[0].prox_n).toString().padStart(8, '0');
                    } else {
                        throw new Error('No se pudo generar el consecutivo para el ajuste de inventario en Profit Plus.');
                    }
                }
            }

            const sucuCode = data.co_sucu_in || '01';
            const auditUser = (data.co_us_in || 'PROFIT').substring(0, 6);
            const today = new Date();
            const motivoText = (data.motivo || (isSalida ? 'Traslado Salida entre Sedes' : 'Traslado Entrada entre Sedes')).substring(0, 80);

            // 3. Insertar saAjuste (Encabezado)
            const ajueGuidRes = await transaction.request().query('SELECT NEWID() AS guid');
            const ajueGuid = ajueGuidRes.recordset[0].guid;

            await transaction.request()
                .input('ajue_num', sql.Char(20), padProfit(ajueNum, 20))
                .input('fecha', sql.SmallDateTime, today)
                .input('motivo', sql.VarChar(80), motivoText)
                .input('co_mone', sql.Char(6), padProfit('BS', 6))
                .input('tasa', sql.Decimal(18, 2), 1.00)
                .input('seriales_s', sql.Int, 0)
                .input('seriales_e', sql.Int, 0)
                .input('anulado', sql.Bit, 0)
                .input('co_us_in', sql.Char(6), padProfit(auditUser, 6))
                .input('co_sucu_in', sql.Char(6), padProfit(sucuCode, 6))
                .input('co_us_mo', sql.Char(6), padProfit(auditUser, 6))
                .input('co_sucu_mo', sql.Char(6), padProfit(sucuCode, 6))
                .input('rowguid', sql.UniqueIdentifier, ajueGuid)
                .query(`
                    INSERT INTO saAjuste (
                        ajue_num, fecha, motivo, co_mone, tasa, seriales_s, seriales_e, anulado,
                        co_us_in, co_sucu_in, fe_us_in, co_us_mo, co_sucu_mo, fe_us_mo, rowguid
                    ) VALUES (
                        @ajue_num, @fecha, @motivo, @co_mone, @tasa, @seriales_s, @seriales_e, @anulado,
                        @co_us_in, @co_sucu_in, GETDATE(), @co_us_mo, @co_sucu_mo, GETDATE(), @rowguid
                    )
                `);

            // 4. Insertar saAjusteReng (Detalles) y actualizar stock en saStockAlmacen
            let rengNum = 1;
            for (const reng of data.renglones) {
                const coArt = padProfit(reng.co_art, 30);
                const coAlma = padProfit(reng.co_alma || '01', 6);
                const qty = Math.abs(Number(reng.total_art || 0));
                const costUnit = Number(reng.cost_unit || 0);

                if (qty <= 0) continue;

                // Obtener unidad principal del artículo si no viene especificada
                let coUni = reng.co_uni ? padProfit(reng.co_uni, 6) : padProfit('UNI', 6);
                const artUniRes = await transaction.request()
                    .input('co_art_check', sql.Char(30), coArt)
                    .query('SELECT TOP 1 co_uni FROM saArticulo WHERE co_art = @co_art_check');
                if (artUniRes.recordset.length > 0 && artUniRes.recordset[0].co_uni) {
                    coUni = padProfit(artUniRes.recordset[0].co_uni, 6);
                }

                const rengGuidRes = await transaction.request().query('SELECT NEWID() AS guid');
                const rengGuid = rengGuidRes.recordset[0].guid;

                await transaction.request()
                    .input('ajue_num', sql.Char(20), padProfit(ajueNum, 20))
                    .input('reng_num', sql.Int, rengNum)
                    .input('co_tipo', sql.Char(6), padProfit(coTipo, 6))
                    .input('co_art', sql.Char(30), coArt)
                    .input('co_alma', sql.Char(6), coAlma)
                    .input('co_uni', sql.Char(6), coUni)
                    .input('total_art', sql.Decimal(18, 5), qty)
                    .input('stotal_art', sql.Decimal(18, 5), qty)
                    .input('cost_unit', sql.Decimal(18, 5), costUnit)
                    .input('lote_asignado', sql.Bit, 0)
                    .input('costo_adi1', sql.Decimal(18, 5), 0)
                    .input('costo_adi2', sql.Decimal(18, 5), 0)
                    .input('costo_adi3', sql.Decimal(18, 5), 0)
                    .input('co_us_in', sql.Char(6), padProfit(auditUser, 6))
                    .input('co_sucu_in', sql.Char(6), padProfit(sucuCode, 6))
                    .input('co_us_mo', sql.Char(6), padProfit(auditUser, 6))
                    .input('co_sucu_mo', sql.Char(6), padProfit(sucuCode, 6))
                    .input('rowguid', sql.UniqueIdentifier, rengGuid)
                    .query(`
                        INSERT INTO saAjusteReng (
                            ajue_num, reng_num, co_tipo, co_art, co_alma, co_uni,
                            total_art, stotal_art, cost_unit, lote_asignado,
                            costo_adi1, costo_adi2, costo_adi3,
                            co_us_in, co_sucu_in, fe_us_in, co_us_mo, co_sucu_mo, fe_us_mo, rowguid
                        ) VALUES (
                            @ajue_num, @reng_num, @co_tipo, @co_art, @co_alma, @co_uni,
                            @total_art, @stotal_art, @cost_unit, @lote_asignado,
                            @costo_adi1, @costo_adi2, @costo_adi3,
                            @co_us_in, @co_sucu_in, GETDATE(), @co_us_mo, @co_sucu_mo, GETDATE(), @rowguid
                        )
                    `);

                // 5. Actualizar stock en saStockAlmacen (ACT)
                // Si es Salida (02), se resta del stock; si es Entrada (01), se suma al stock.
                const stockFactor = isSalida ? -qty : qty;
                const updateStockRes = await transaction.request()
                    .input('co_art_stk', sql.Char(30), coArt)
                    .input('co_alma_stk', sql.Char(6), coAlma)
                    .input('qty_stk', sql.Decimal(18, 5), stockFactor)
                    .query(`
                        UPDATE saStockAlmacen
                        SET stock = stock + @qty_stk
                        WHERE co_art = @co_art_stk AND co_alma = @co_alma_stk AND LTRIM(RTRIM(tipo)) = 'ACT';

                        IF @@ROWCOUNT = 0
                        BEGIN
                            INSERT INTO saStockAlmacen (co_art, co_alma, tipo, stock, rowguid)
                            VALUES (@co_art_stk, @co_alma_stk, 'ACT', CASE WHEN @qty_stk < 0 THEN 0 ELSE @qty_stk END, NEWID());
                        END
                    `);

                rengNum++;
            }

            await transaction.commit();
            console.log(`✅ [AJUSTES SUCCESS] Ajuste de inventario ${ajueNum} (${isSalida ? 'Salida' : 'Entrada'}) creado exitosamente.`);
            return res.status(200).json({
                success: true,
                message: `Ajuste de inventario ${ajueNum} registrado con éxito.`,
                ajue_num: ajueNum,
                tipo: isSalida ? 'SAL' : 'ENT'
            });

        } catch (err) {
            await transaction.rollback();
            console.error('❌ [AJUSTES ERROR TRANSACTION]:', err.message);
            throw err;
        }
    } catch (error) {
        console.error('❌ [AJUSTES ERROR CRÍTICO]:', error.message);
        return res.status(500).json({
            success: false,
            message: 'Error al registrar el ajuste de inventario en Profit Plus.',
            error: error.message
        });
    }
});

module.exports = router;
