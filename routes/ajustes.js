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

        // 0. Verificar si todos los artículos existen en la tabla saArticulo de esta sede
        for (const reng of data.renglones) {
            const coArtTrim = String(reng.co_art || '').trim();
            if (!coArtTrim) continue;

            const artCheckRes = await pool.request()
                .input('co_art_val', sql.Char(30), padProfit(coArtTrim, 30))
                .query('SELECT TOP 1 co_art, RTRIM(art_des) as art_des FROM saArticulo WHERE LTRIM(RTRIM(co_art)) = LTRIM(RTRIM(@co_art_val))');

            if (!artCheckRes.recordset || artCheckRes.recordset.length === 0) {
                const artName = reng.art_des ? ` (${String(reng.art_des).trim()})` : '';
                return res.status(400).json({
                    success: false,
                    message: `El artículo "${coArtTrim}"${artName} no existe en el catálogo de inventario de esta sede.`
                });
            }
        }

        const isSalida = String(data.tipo || '').toUpperCase() === 'SAL' || String(data.co_tipo || '').trim() === '02';
        const coTipo = isSalida ? '02' : '01';

        // 1. Obtener el próximo consecutivo para AJUS_NUM (fuera de la transacción para no abortar si falla un Sp)
        let ajueNum = null;
        for (const consecName of ['AJUS_NUM', 'AJUS', 'AJU', 'AJUSTE', 'AJUSTES', 'AJU_ENT', 'AJU_SAL']) {
            try {
                const consecRes = await pool.request()
                    .input('sCo_Consecutivo', sql.Char(16), padProfit(consecName, 16))
                    .execute('pConsecutivoProximo');
                if (consecRes.recordset && consecRes.recordset[0]?.ProximoConsecutivo) {
                    ajueNum = consecRes.recordset[0].ProximoConsecutivo.trim();
                    if (ajueNum) break;
                }
            } catch (e) {
                // Continuar con el siguiente candidato fuera de transacción
            }
        }

        if (!ajueNum) {
            try {
                const resCorr = await pool.request().query(`
                    UPDATE saSerie
                    SET prox_n = prox_n + 1, fe_us_mo = GETDATE()
                    OUTPUT INSERTED.prox_n, RTRIM(INSERTED.desde_a) as prefijo
                    WHERE co_serie = (
                        SELECT TOP 1 co_serie
                        FROM saConsecutivo
                        WHERE UPPER(LTRIM(RTRIM(co_consecutivo))) IN ('AJUS_NUM', 'AJUS', 'AJUSTE', 'AJUSTES', 'AJU_ENT', 'AJU_SAL')
                           OR UPPER(LTRIM(RTRIM(co_consecutivo))) LIKE '%AJU%'
                    )
                `);
                const corrRow = resCorr.recordset[0] || null;
                if (corrRow && corrRow.prox_n) {
                    const proxN = Number(corrRow.prox_n || 0);
                    ajueNum = proxN.toString().padStart(8, '0');
                } else {
                    const resDirect = await pool.request().query(`
                        UPDATE saSerie
                        SET prox_n = prox_n + 1, fe_us_mo = GETDATE()
                        OUTPUT INSERTED.prox_n
                        WHERE LTRIM(RTRIM(co_serie)) = '001'
                    `);
                    if (resDirect.recordset && resDirect.recordset.length > 0) {
                        ajueNum = Number(resDirect.recordset[0].prox_n).toString().padStart(8, '0');
                    }
                }
            } catch (eSerie) {
                console.warn('[AJUSTES] Falló saSerie fallback:', eSerie.message);
            }
        }

        if (!ajueNum) {
            try {
                const resMax = await pool.request().query(`
                    SELECT ISNULL(MAX(CASE WHEN ISNUMERIC(LTRIM(RTRIM(ajue_num))) = 1 THEN CAST(LTRIM(RTRIM(ajue_num)) AS BIGINT) ELSE 0 END), 0) + 1 as max_num
                    FROM saAjuste
                `);
                if (resMax.recordset && resMax.recordset[0]?.max_num) {
                    const proxN = String(resMax.recordset[0].max_num);
                    ajueNum = proxN.padStart(8, '0');
                }
            } catch (eMax) {
                console.error('[AJUSTES] Falló fallback MAX(ajue_num):', eMax.message);
            }
        }

        if (!ajueNum) {
            return res.status(400).json({
                success: false,
                message: 'No se pudo generar el consecutivo para el ajuste de inventario en Profit Plus.'
            });
        }

        // 2. Obtener Moneda USD y Tasa del Día (fuera de la transacción)
        let coMoneUSD = 'USD';
        let tasaDia = 1.00;
        try {
            const resMon = await pool.request().query(`
                SELECT TOP 1 RTRIM(co_mone) AS co_mone 
                FROM saMoneda 
                WHERE LTRIM(RTRIM(co_mone)) IN ('USD', 'US$', 'DOL', '$', 'US') 
                   OR mone_des LIKE '%Dolar%'
            `);
            if (resMon.recordset && resMon.recordset[0]?.co_mone) {
                coMoneUSD = resMon.recordset[0].co_mone.trim();
            }

            const resTasa = await pool.request()
                .input('co_mone_check', sql.Char(6), padProfit(coMoneUSD, 6))
                .query(`
                    SELECT TOP 1 tasa_v 
                    FROM saTasa 
                    WHERE LTRIM(RTRIM(co_mone)) IN ('USD', 'US$', 'DOL', '$', 'US') 
                       OR LTRIM(RTRIM(co_mone)) = LTRIM(RTRIM(@co_mone_check))
                    ORDER BY fecha DESC
                `);
            if (resTasa.recordset && resTasa.recordset[0]?.tasa_v && Number(resTasa.recordset[0].tasa_v) > 0) {
                tasaDia = Number(resTasa.recordset[0].tasa_v);
            } else {
                const resMonTasa = await pool.request().query(`
                    SELECT TOP 1 tasa_v 
                    FROM saMoneda 
                    WHERE LTRIM(RTRIM(co_mone)) IN ('USD', 'US$', 'DOL', '$', 'US') 
                       OR mone_des LIKE '%Dolar%'
                `);
                if (resMonTasa.recordset && resMonTasa.recordset[0]?.tasa_v && Number(resMonTasa.recordset[0].tasa_v) > 0) {
                    tasaDia = Number(resMonTasa.recordset[0].tasa_v);
                }
            }
        } catch (eTasa) {
            console.warn('[AJUSTES] Error al consultar moneda/tasa USD:', eTasa.message);
        }

        if (data.tasa && Number(data.tasa) > 0) {
            tasaDia = Number(data.tasa);
        }
        if (data.co_mone && String(data.co_mone).trim()) {
            coMoneUSD = String(data.co_mone).trim();
        }

        const sucuCode = String(data.co_sucu_in || data.co_sucu_mo || data.sucu_code || '01').trim();
        const auditUser = String(data.co_us_in || data.co_us_mo || data.profit_user || 'PROFIT').trim().substring(0, 6);
        const today = new Date();
        const motivoText = (data.motivo || (isSalida ? 'Traslado Salida entre Sedes' : 'Traslado Entrada entre Sedes')).substring(0, 80);

        // Pre-consultar unidades de medida si faltan (fuera de transacción)
        for (const reng of data.renglones) {
            if (!reng.co_uni) {
                try {
                    const artUniRes = await pool.request()
                        .input('co_art_check', sql.Char(30), padProfit(reng.co_art, 30))
                        .query('SELECT TOP 1 co_uni FROM saArtUnidad WHERE co_art = @co_art_check ORDER BY uni_principal DESC');
                    if (artUniRes.recordset && artUniRes.recordset.length > 0 && artUniRes.recordset[0].co_uni) {
                        reng.co_uni = artUniRes.recordset[0].co_uni.trim();
                    }
                } catch (e) {
                    // Fallback
                }
            }
            if (!reng.co_uni) reng.co_uni = 'UND';
        }

        // 3. INICIAR LA TRANSACCIÓN SOLO PARA LAS INSERCIONES DE ESCRITURA
        const transaction = new sql.Transaction(pool);
        await transaction.begin();

        try {
            const ajueGuidRes = await transaction.request().query('SELECT NEWID() AS guid');
            const ajueGuid = ajueGuidRes.recordset[0].guid;

            await transaction.request()
                .input('ajue_num', sql.Char(20), padProfit(ajueNum, 20))
                .input('fecha', sql.SmallDateTime, today)
                .input('motivo', sql.VarChar(80), motivoText)
                .input('co_mone', sql.Char(6), padProfit(coMoneUSD, 6))
                .input('tasa', sql.Decimal(18, 5), tasaDia)
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

            let rengNum = 1;
            for (const reng of data.renglones) {
                const coArt = padProfit(reng.co_art, 30);
                const coAlma = padProfit(reng.co_alma || '01', 6);
                const qty = Math.abs(Number(reng.total_art || 0));
                const costUnit = Number(reng.cost_unit || 0);

                if (qty <= 0) continue;

                const coUni = padProfit(reng.co_uni || 'UND', 6);

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

                const stockFactor = isSalida ? -qty : qty;
                await transaction.request()
                    .input('co_art_stk', sql.Char(30), coArt)
                    .input('co_alma_stk', sql.Char(6), coAlma)
                    .input('qty_stk', sql.Decimal(18, 5), stockFactor)
                    .query(`
                        UPDATE saStockAlmacen
                        SET stock = stock + @qty_stk
                        WHERE co_art = @co_art_stk AND co_alma = @co_alma_stk AND LTRIM(RTRIM(tipo)) = 'ACT';

                        IF @@ROWCOUNT = 0
                        BEGIN
                            INSERT INTO saStockAlmacen (co_art, co_alma, tipo, stock)
                            VALUES (@co_art_stk, @co_alma_stk, 'ACT', CASE WHEN @qty_stk < 0 THEN 0 ELSE @qty_stk END);
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

/**
 * POST /api/v1/ajustes/:ajue_num/anular
 * Anula/revierte un Ajuste de Inventario en Profit Plus y revierte el stock.
 */
router.post('/:ajue_num/anular', async (req, res) => {
    const { ajue_num } = req.params;
    const branchId = req.body.branch_id;

    if (!ajue_num) {
        return res.status(400).json({ success: false, message: 'El número de ajuste es requerido.' });
    }

    try {
        const pool = await getPool(branchId, req.sqlAuth);

        // 1. Consultar encabezado del ajuste fuera de la transacción
        const ajueRes = await pool.request()
            .input('ajue_num', sql.Char(20), padProfit(ajue_num, 20))
            .query('SELECT TOP 1 ajue_num, anulado, fe_us_in FROM saAjuste WHERE LTRIM(RTRIM(ajue_num)) = LTRIM(RTRIM(@ajue_num))');

        if (!ajueRes.recordset || ajueRes.recordset.length === 0) {
            return res.status(404).json({ success: false, message: `El ajuste ${ajue_num} no fue encontrado en Profit Plus.` });
        }

        const ajueHeader = ajueRes.recordset[0];
        if (ajueHeader.anulado) {
            return res.status(400).json({ success: false, message: `El ajuste ${ajue_num} ya se encuentra anulado.` });
        }

        // 2. Obtener renglones del ajuste fuera de transacción
        const rengRes = await pool.request()
            .input('ajue_num', sql.Char(20), padProfit(ajue_num, 20))
            .query('SELECT co_tipo, co_art, co_alma, total_art FROM saAjusteReng WHERE LTRIM(RTRIM(ajue_num)) = LTRIM(RTRIM(@ajue_num))');

        const renglones = rengRes.recordset || [];

        const transaction = new sql.Transaction(pool);
        await transaction.begin();

        try {
            for (const reng of renglones) {
                const coTipo = String(reng.co_tipo || '').trim();
                const isSalida = coTipo === '02';
                const qty = Math.abs(Number(reng.total_art || 0));
                const revertFactor = isSalida ? qty : -qty;

                await transaction.request()
                    .input('co_art_stk', sql.Char(30), padProfit(reng.co_art, 30))
                    .input('co_alma_stk', sql.Char(6), padProfit(reng.co_alma, 6))
                    .input('qty_stk', sql.Decimal(18, 5), revertFactor)
                    .query(`
                        UPDATE saStockAlmacen
                        SET stock = stock + @qty_stk
                        WHERE co_art = @co_art_stk AND co_alma = @co_alma_stk AND LTRIM(RTRIM(tipo)) = 'ACT';
                    `);
            }

            const auditUser = String(req.body.co_us_in || req.body.profit_user || 'PROFIT').trim().substring(0, 6);
            await transaction.request()
                .input('ajue_num', sql.Char(20), padProfit(ajue_num, 20))
                .input('co_us_mo', sql.Char(6), padProfit(auditUser, 6))
                .query(`
                    UPDATE saAjuste
                    SET anulado = 1, fe_us_mo = GETDATE(), co_us_mo = @co_us_mo
                    WHERE LTRIM(RTRIM(ajue_num)) = LTRIM(RTRIM(@ajue_num))
                `);

            await transaction.commit();
            console.log(`✅ [AJUSTES VOID SUCCESS] Ajuste ${ajue_num} anulado correctamente y stock revertido.`);
            return res.status(200).json({
                success: true,
                message: `Ajuste ${ajue_num} anulado con éxito.`
            });

        } catch (err) {
            await transaction.rollback();
            console.error('❌ [AJUSTES VOID ERROR]:', err.message);
            throw err;
        }

    } catch (error) {
        console.error('❌ [AJUSTES VOID CRITICAL]:', error.message);
        return res.status(500).json({
            success: false,
            message: `Error al anular el ajuste de inventario ${ajue_num}.`,
            error: error.message
        });
    }
});

/**
 * PUT /api/v1/ajustes/:ajue_num
 * Edita los renglones y datos de un Ajuste de Inventario de Salida (02) antes de confirmación.
 */
router.put('/:ajue_num', async (req, res) => {
    const { ajue_num } = req.params;
    const data = req.body;

    if (!ajue_num) {
        return res.status(400).json({ success: false, message: 'El número de ajuste es requerido.' });
    }

    if (!data.renglones || !Array.isArray(data.renglones) || data.renglones.length === 0) {
        return res.status(400).json({ success: false, message: 'Debe incluir al menos un renglón de artículo.' });
    }

    try {
        const pool = await getPool(data.branch_id, req.sqlAuth);

        // 0. Verificar que el ajuste exista y no esté anulado (fuera de la transacción)
        const ajueCheck = await pool.request()
            .input('ajue_num', sql.Char(20), padProfit(ajue_num, 20))
            .query('SELECT TOP 1 ajue_num, anulado FROM saAjuste WHERE LTRIM(RTRIM(ajue_num)) = LTRIM(RTRIM(@ajue_num))');

        if (!ajueCheck.recordset || ajueCheck.recordset.length === 0) {
            return res.status(404).json({ success: false, message: `El ajuste ${ajue_num} no fue encontrado.` });
        }

        if (ajueCheck.recordset[0].anulado) {
            return res.status(400).json({ success: false, message: `El ajuste ${ajue_num} está anulado y no se puede editar.` });
        }

        // 1. Validar existencia de los nuevos artículos en saArticulo (fuera de la transacción)
        for (const reng of data.renglones) {
            const coArtTrim = String(reng.co_art || '').trim();
            if (!coArtTrim) continue;

            const artCheckRes = await pool.request()
                .input('co_art_val', sql.Char(30), padProfit(coArtTrim, 30))
                .query('SELECT TOP 1 co_art, RTRIM(art_des) as art_des FROM saArticulo WHERE LTRIM(RTRIM(co_art)) = LTRIM(RTRIM(@co_art_val))');

            if (!artCheckRes.recordset || artCheckRes.recordset.length === 0) {
                const artName = reng.art_des ? ` (${String(reng.art_des).trim()})` : '';
                return res.status(400).json({
                    success: false,
                    message: `El artículo "${coArtTrim}"${artName} no existe en el catálogo de inventario de esta sede.`
                });
            }
        }

        // 2. Obtener renglones anteriores (fuera de la transacción)
        const oldRengRes = await pool.request()
            .input('ajue_num', sql.Char(20), padProfit(ajue_num, 20))
            .query('SELECT co_tipo, co_art, co_alma, total_art FROM saAjusteReng WHERE LTRIM(RTRIM(ajue_num)) = LTRIM(RTRIM(@ajue_num))');

        const oldRenglones = oldRengRes.recordset || [];

        // Pre-consultar unidades de medida si faltan (fuera de la transacción)
        for (const reng of data.renglones) {
            if (!reng.co_uni) {
                try {
                    const artUniRes = await pool.request()
                        .input('co_art_check', sql.Char(30), padProfit(reng.co_art, 30))
                        .query('SELECT TOP 1 co_uni FROM saArtUnidad WHERE co_art = @co_art_check ORDER BY uni_principal DESC');
                    if (artUniRes.recordset && artUniRes.recordset.length > 0 && artUniRes.recordset[0].co_uni) {
                        reng.co_uni = artUniRes.recordset[0].co_uni.trim();
                    }
                } catch (e) {
                    // Fallback
                }
            }
            if (!reng.co_uni) reng.co_uni = 'UND';
        }

        const transaction = new sql.Transaction(pool);
        await transaction.begin();

        try {
            // Revertir el stock de los renglones viejos
            for (const oldReng of oldRenglones) {
                const isSalida = String(oldReng.co_tipo || '').trim() === '02';
                const qty = Math.abs(Number(oldReng.total_art || 0));
                const revertFactor = isSalida ? qty : -qty;

                await transaction.request()
                    .input('co_art_stk', sql.Char(30), padProfit(oldReng.co_art, 30))
                    .input('co_alma_stk', sql.Char(6), padProfit(oldReng.co_alma, 6))
                    .input('qty_stk', sql.Decimal(18, 5), revertFactor)
                    .query(`
                        UPDATE saStockAlmacen
                        SET stock = stock + @qty_stk
                        WHERE co_art = @co_art_stk AND co_alma = @co_alma_stk AND LTRIM(RTRIM(tipo)) = 'ACT';
                    `);
            }

            // Eliminar renglones anteriores
            await transaction.request()
                .input('ajue_num', sql.Char(20), padProfit(ajue_num, 20))
                .query('DELETE FROM saAjusteReng WHERE LTRIM(RTRIM(ajue_num)) = LTRIM(RTRIM(@ajue_num))');

            const sucuCode = String(data.co_sucu_mo || data.co_sucu_in || '01').trim();
            const auditUser = String(data.co_us_mo || data.co_us_in || 'PROFIT').trim().substring(0, 6);
            const isSalida = String(data.tipo || '').toUpperCase() === 'SAL' || String(data.co_tipo || '').trim() === '02';
            const coTipo = isSalida ? '02' : '01';

            let rengNum = 1;
            for (const reng of data.renglones) {
                const coArt = padProfit(reng.co_art, 30);
                const coAlma = padProfit(reng.co_alma || '01', 6);
                const qty = Math.abs(Number(reng.total_art || 0));
                const costUnit = Number(reng.cost_unit || 0);

                if (qty <= 0) continue;

                const coUni = padProfit(reng.co_uni || 'UND', 6);

                const rengGuidRes = await transaction.request().query('SELECT NEWID() AS guid');
                const rengGuid = rengGuidRes.recordset[0].guid;

                await transaction.request()
                    .input('ajue_num', sql.Char(20), padProfit(ajue_num, 20))
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

                const newStockFactor = isSalida ? -qty : qty;
                await transaction.request()
                    .input('co_art_stk', sql.Char(30), coArt)
                    .input('co_alma_stk', sql.Char(6), coAlma)
                    .input('qty_stk', sql.Decimal(18, 5), newStockFactor)
                    .query(`
                        UPDATE saStockAlmacen
                        SET stock = stock + @qty_stk
                        WHERE co_art = @co_art_stk AND co_alma = @co_alma_stk AND LTRIM(RTRIM(tipo)) = 'ACT';

                        IF @@ROWCOUNT = 0
                        BEGIN
                            INSERT INTO saStockAlmacen (co_art, co_alma, tipo, stock)
                            VALUES (@co_art_stk, @co_alma_stk, 'ACT', CASE WHEN @qty_stk < 0 THEN 0 ELSE @qty_stk END);
                        END
                    `);

                rengNum++;
            }

            const motivoText = (data.motivo || 'Ajuste de Traslado Modificado').substring(0, 80);
            await transaction.request()
                .input('ajue_num', sql.Char(20), padProfit(ajue_num, 20))
                .input('motivo', sql.VarChar(80), motivoText)
                .input('co_us_mo', sql.Char(6), padProfit(auditUser, 6))
                .input('co_sucu_mo', sql.Char(6), padProfit(sucuCode, 6))
                .query(`
                    UPDATE saAjuste
                    SET motivo = @motivo, fe_us_mo = GETDATE(), co_us_mo = @co_us_mo, co_sucu_mo = @co_sucu_mo
                    WHERE LTRIM(RTRIM(ajue_num)) = LTRIM(RTRIM(@ajue_num))
                `);

            await transaction.commit();
            console.log(`✅ [AJUSTES EDIT SUCCESS] Ajuste ${ajue_num} modificado exitosamente.`);
            return res.status(200).json({
                success: true,
                message: `Ajuste ${ajue_num} modificado con éxito.`
            });

        } catch (err) {
            await transaction.rollback();
            console.error('❌ [AJUSTES EDIT ERROR TRANSACTION]:', err.message);
            throw err;
        }
    } catch (error) {
        console.error('❌ [AJUSTES EDIT CRITICAL]:', error.message);
        return res.status(500).json({
            success: false,
            message: `Error al modificar el ajuste de inventario ${ajue_num}.`,
            error: error.message
        });
    }
});

module.exports = router;
