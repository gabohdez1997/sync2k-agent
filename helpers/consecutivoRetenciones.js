/**
 * consecutivoRetenciones.js
 * 
 * Helper para asignación y sincronización de correlativos únicos de retenciones (IVA e ISLR)
 * entre todas las sedes de Profit Plus (Boca de Río, Paraparal, etc.).
 * 
 * Garantiza que cuando una sede genere una retención (ej. 001):
 * 1. Se tome el mayor correlativo global existente entre todas las sedes y PostgreSQL central.
 * 2. Se asigne dicho número al documento actual.
 * 3. Se incremente el correlativo (ej. a 002) atómicamente en la sede actual.
 * 4. Se replique el nuevo correlativo (002) en saSerie de todas las demás sedes activas.
 * 5. Se registre el correlativo en PostgreSQL (global_consecutivos) como fuente central de verdad.
 */

const { getPool, getServers, getAllActiveServers, pgPool } = require('../db');
const { DOC_TYPE_CONFIG } = require('./consecutivos');

/**
 * Inicializa la tabla global_consecutivos en PostgreSQL central si no existe.
 */
let pgTableInitialized = false;
async function ensurePgTable() {
    if (pgTableInitialized) return;
    try {
        await pgPool.query(`
            CREATE TABLE IF NOT EXISTS global_consecutivos (
                tipo VARCHAR(50) PRIMARY KEY,
                prox_n BIGINT NOT NULL,
                updated_at TIMESTAMP DEFAULT NOW()
            )
        `);
        pgTableInitialized = true;
    } catch (e) {
        console.warn('⚠️ [consecutivoRetenciones] No se pudo verificar tabla global_consecutivos en PG:', e.message);
    }
}

/**
 * Obtiene el próximo correlativo único sincronizado entre todas las sedes para retenciones.
 * Consulta en el momento exacto el mayor comprobante físico emitido en TODAS las sedes de Profit Plus
 * para evitar desfases si se registran documentos directamente desde el cliente Desktop de Profit.
 * 
 * @param {Object} params
 * @param {Object} params.runner - Transacción o pool de la sede actual
 * @param {string} params.co_tipo_serie - 'IVAN_COMPRA' o 'ISLR_COMPRA' (o 'C016' / 'C015')
 * @param {string} [params.co_sucur] - Código de sucursal Profit (ej. '01')
 * @param {string} params.currentSrvId - ID de la sede actual (ej. 'G3', 'G4')
 * @param {Object} [params.sqlAuth] - Credenciales de autenticación SQL
 * @returns {Promise<{ docNum: string, proxN: number, prefijo: string, sufijo: string, longitud: number }>}
 */
async function getProximoConsecutivoRetencion(params) {
    const { runner, co_sucur = '01', currentSrvId, sqlAuth } = params;
    if (!runner) {
        throw new Error('getProximoConsecutivoRetencion: Se requiere un runner (pool o transacción mssql).');
    }

    const typeKey = (params.co_tipo_serie || '').toUpperCase();
    const conf = DOC_TYPE_CONFIG[typeKey] || {
        co_tipo_serie: typeKey === 'IVAN_COMPRA' ? 'C016' : typeKey === 'ISLR_COMPRA' ? 'C015' : typeKey,
        table: 'saDocumentoCompra',
        col: 'nro_doc'
    };

    const profitSerieCode = conf.co_tipo_serie;
    const docTypeProfit = typeKey === 'IVAN_COMPRA' || profitSerieCode === 'C016' ? 'IVAN' : 'ISLR';
    
    // Obtener TODAS las sedes activas desde PostgreSQL sin limitarse por LOCAL_BRANCH_NAME
    const allServers = await getAllActiveServers();

    await ensurePgTable();

    // ── 1. Consultar metadatos locales (prefijo, sufijo, longitud, prox_n local) ──
    const metaRes = await runner.request().query(`
        SELECT TOP 1 
            s.prox_n,
            RTRIM(ISNULL(st.prefijo, '')) AS prefijo,
            RTRIM(ISNULL(st.sufijo, '')) AS sufijo,
            ISNULL(st.longitud, 10) AS longitud,
            RTRIM(s.co_serie) AS co_serie
        FROM saSerie s
        INNER JOIN saSerieTipo st ON s.co_tipo_serie = st.co_tipo_serie
        WHERE UPPER(RTRIM(s.co_tipo_serie)) = '${profitSerieCode.toUpperCase()}'
    `);

    const localMeta = metaRes.recordset[0] || {
        prox_n: 1,
        prefijo: '',
        sufijo: '',
        longitud: 10,
        co_serie: profitSerieCode
    };

    const prefijo = (localMeta.prefijo || '').trim();
    const sufijo = (localMeta.sufijo || '').trim();
    const longitud = Number(localMeta.longitud) || 10;

    const maxIssuedNumbers = [];
    const nextPointers = [];

    // ── 1. Consultar comprobantes emitidos en sede local ──
    if (docTypeProfit === 'IVAN') {
        try {
            const localMaxCompRes = await runner.request().query(`
                SELECT ISNULL(MAX(
                    CASE 
                        WHEN TRY_CAST(RIGHT(LTRIM(RTRIM(num_comprobante)), 8) AS BIGINT) IS NOT NULL 
                        THEN CAST(RIGHT(LTRIM(RTRIM(num_comprobante)), 8) AS BIGINT) 
                        ELSE 0 
                    END
                ), 0) AS max_c
                FROM saPagoRetenIvaReng
            `);
            const maxCLocal = Number(localMaxCompRes.recordset[0]?.max_c || 0);
            if (maxCLocal > 0) maxIssuedNumbers.push(maxCLocal);

            const docCompRes = await runner.request().query(`
                SELECT ISNULL(MAX(
                    CASE 
                        WHEN TRY_CAST(RIGHT(LTRIM(RTRIM(num_comprobante)), 8) AS BIGINT) IS NOT NULL 
                        THEN CAST(RIGHT(LTRIM(RTRIM(num_comprobante)), 8) AS BIGINT) 
                        ELSE 0 
                    END
                ), 0) AS max_c
                FROM saDocumentoCompra
                WHERE UPPER(RTRIM(co_tipo_doc)) = 'IVAN'
            `);
            const maxCDoc = Number(docCompRes.recordset[0]?.max_c || 0);
            if (maxCDoc > 0) maxIssuedNumbers.push(maxCDoc);
        } catch (eCompLocal) {
            console.warn(`[consecutivoRetenciones] Advertencia al consultar max_comprobante local:`, eCompLocal.message);
        }
    } else if (docTypeProfit === 'ISLR') {
        try {
            const localMaxDocRes = await runner.request().query(`
                SELECT ISNULL(MAX(
                    CASE 
                        WHEN TRY_CAST(RIGHT(LTRIM(RTRIM(nro_doc)), 10) AS BIGINT) IS NOT NULL 
                        THEN CAST(RIGHT(LTRIM(RTRIM(nro_doc)), 10) AS BIGINT) 
                        ELSE 0 
                    END
                ), 0) AS max_n
                FROM saDocumentoCompra
                WHERE UPPER(RTRIM(co_tipo_doc)) = 'ISLR'
            `);
            const maxDocLocal = Number(localMaxDocRes.recordset[0]?.max_n || 0);
            if (maxDocLocal > 0) maxIssuedNumbers.push(maxDocLocal);
        } catch (eDocLocal) {
            console.warn(`[consecutivoRetenciones] Advertencia al consultar max_doc ISLR local:`, eDocLocal.message);
        }
    }

    // Puntero local en saSerie (es el próximo número configurado)
    if (Number.isFinite(Number(localMeta.prox_n)) && Number(localMeta.prox_n) > 0) {
        nextPointers.push(Number(localMeta.prox_n));
    }

    // ── 2. Consultar en PostgreSQL central (global_consecutivos) ──
    try {
        const pgRes = await pgPool.query(`SELECT prox_n FROM global_consecutivos WHERE tipo = $1`, [profitSerieCode]);
        if (pgRes.rows.length > 0) {
            const pgN = Number(pgRes.rows[0].prox_n);
            if (pgN > 0) nextPointers.push(pgN);
        }
    } catch (ePg) {
        console.warn(`[consecutivoRetenciones] Advertencia al consultar PG global_consecutivos:`, ePg.message);
    }

    // ── 3. Consultar en las DEMÁS sedes activas (comprobantes físicos reales y saSerie) ──
    const otherServers = allServers.filter(s => s.id && s.id !== currentSrvId);
    await Promise.all(otherServers.map(async (otherSrv) => {
        try {
            const otherPool = await getPool(otherSrv.id, sqlAuth);
            
            // Puntero en saSerie de la otra sede
            const otherSerieRes = await otherPool.request().query(`
                SELECT TOP 1 prox_n 
                FROM saSerie 
                WHERE UPPER(RTRIM(co_tipo_serie)) = '${profitSerieCode.toUpperCase()}'
            `);
            if (otherSerieRes.recordset.length > 0) {
                const otherN = Number(otherSerieRes.recordset[0].prox_n);
                if (otherN > 0) nextPointers.push(otherN);
            }

            // Consultar comprobantes reales emitidos en la otra sede
            if (docTypeProfit === 'IVAN') {
                const otherMaxCompRes = await otherPool.request().query(`
                    SELECT ISNULL(MAX(
                        CASE 
                            WHEN TRY_CAST(RIGHT(LTRIM(RTRIM(num_comprobante)), 8) AS BIGINT) IS NOT NULL 
                            THEN CAST(RIGHT(LTRIM(RTRIM(num_comprobante)), 8) AS BIGINT) 
                            ELSE 0 
                        END
                    ), 0) AS max_c
                    FROM saPagoRetenIvaReng
                `);
                const otherMaxC = Number(otherMaxCompRes.recordset[0]?.max_c || 0);
                if (otherMaxC > 0) maxIssuedNumbers.push(otherMaxC);

                const otherDocRes = await otherPool.request().query(`
                    SELECT ISNULL(MAX(
                        CASE 
                            WHEN TRY_CAST(RIGHT(LTRIM(RTRIM(num_comprobante)), 8) AS BIGINT) IS NOT NULL 
                            THEN CAST(RIGHT(LTRIM(RTRIM(num_comprobante)), 8) AS BIGINT) 
                            ELSE 0 
                        END
                    ), 0) AS max_c
                    FROM saDocumentoCompra
                    WHERE UPPER(RTRIM(co_tipo_doc)) = 'IVAN'
                `);
                const otherDocC = Number(otherDocRes.recordset[0]?.max_c || 0);
                if (otherDocC > 0) maxIssuedNumbers.push(otherDocC);
            } else if (docTypeProfit === 'ISLR') {
                const otherMaxDocRes = await otherPool.request().query(`
                    SELECT ISNULL(MAX(
                        CASE 
                            WHEN TRY_CAST(RIGHT(LTRIM(RTRIM(nro_doc)), 10) AS BIGINT) IS NOT NULL 
                            THEN CAST(RIGHT(LTRIM(RTRIM(nro_doc)), 10) AS BIGINT) 
                            ELSE 0 
                        END
                    ), 0) AS max_n
                    FROM saDocumentoCompra
                    WHERE UPPER(RTRIM(co_tipo_doc)) = 'ISLR'
                `);
                const maxDocOther = Number(otherMaxDocRes.recordset[0]?.max_n || 0);
                if (maxDocOther > 0) maxIssuedNumbers.push(maxDocOther);
            }
        } catch (eOther) {
            console.warn(`[consecutivoRetenciones] No se pudo leer sede remota ${otherSrv.id}: ${eOther.message}`);
        }
    }));

    // ── 4. Calcular número fiscal a asignar ──
    // Regla SENIAT estricta:
    // Los comprobantes emitidos en saPagoRetenIvaReng / saDocumentoCompra son la verdad absoluta.
    // El próximo correlativo asignable es (mayor emitido en cualquier sede) + 1.
    const maxIssued = maxIssuedNumbers.length > 0 ? Math.max(...maxIssuedNumbers) : 0;
    
    // Si hay comprobantes emitidos, el asignado es maxIssued + 1.
    // Si no hubiera comprobantes emitidos nunca en ninguna sede, usamos el prox_n configurado en saSerie.
    let assignedN = 1;
    if (maxIssued > 0) {
        assignedN = maxIssued + 1;
    } else if (nextPointers.length > 0) {
        assignedN = Math.max(...nextPointers);
    }

    // El próximo correlativo que quedará almacenado para el siguiente documento
    const nextN = assignedN + 1;

    console.log(`🎯 [RETENCIONES GLOBAL] Tipo: ${profitSerieCode} (${docTypeProfit}) | Mayor emitido global: ${maxIssued} | Asignado fiscal: ${assignedN} | Siguiente guardado: ${nextN}`);

    // ── 5. Actualizar saSerie en la sede actual al siguiente número ──
    await runner.request().query(`
        UPDATE saSerie
        SET prox_n = ${nextN}, fe_us_mo = GETDATE()
        WHERE UPPER(RTRIM(co_tipo_serie)) = '${profitSerieCode.toUpperCase()}'
    `);

    // ── 6. Actualizar registro central en PostgreSQL ──
    try {
        await pgPool.query(`
            INSERT INTO global_consecutivos (tipo, prox_n, updated_at)
            VALUES ($1, $2, NOW())
            ON CONFLICT (tipo)
            DO UPDATE SET prox_n = GREATEST(global_consecutivos.prox_n, EXCLUDED.prox_n), updated_at = NOW()
        `, [profitSerieCode, nextN]);
    } catch (ePgUp) {
        console.warn(`[consecutivoRetenciones] No se pudo guardar en PG global_consecutivos:`, ePgUp.message);
    }

    // ── 7. Replicar nextN a TODAS las demás sedes en saSerie ──
    Promise.allSettled(otherServers.map(async (otherSrv) => {
        try {
            const remotePool = await getPool(otherSrv.id, sqlAuth);
            await remotePool.request().query(`
                UPDATE saSerie
                SET prox_n = ${nextN}, fe_us_mo = GETDATE()
                WHERE UPPER(RTRIM(co_tipo_serie)) = '${profitSerieCode.toUpperCase()}'
                  AND prox_n < ${nextN}
            `);
            console.log(`📡 [RETENCIONES] Sede remota ${otherSrv.id} sincronizada: prox_n=${nextN} para ${profitSerieCode}`);
        } catch (errRemote) {
            console.warn(`⚠️ [RETENCIONES] No se pudo replicar a sede ${otherSrv.id} (${errRemote.message}). Se sincronizará en su próxima lectura.`);
        }
    })).catch(() => {});

    // ── 6. Formatear y retornar número de documento ──
    const numStr = assignedN.toString().padStart(longitud, '0');
    let docNum = `${prefijo}${numStr}${sufijo}`;

    // Si es IVAN, verificar que docNum no colisione físicamente en saDocumentoCompra
    if (docTypeProfit === 'IVAN') {
        try {
            const existsCheck = await runner.request().query(`
                SELECT TOP 1 nro_doc 
                FROM saDocumentoCompra 
                WHERE UPPER(RTRIM(co_tipo_doc)) = 'IVAN' 
                  AND UPPER(RTRIM(nro_doc)) = '${docNum.trim().toUpperCase()}'
            `);
            if (existsCheck.recordset.length > 0) {
                const maxPhysRes = await runner.request().query(`
                    SELECT ISNULL(MAX(
                        CASE 
                            WHEN TRY_CAST(RIGHT(LTRIM(RTRIM(nro_doc)), 10) AS BIGINT) IS NOT NULL 
                            THEN CAST(RIGHT(LTRIM(RTRIM(nro_doc)), 10) AS BIGINT) 
                            ELSE 0 
                        END
                    ), 0) AS max_phys
                    FROM saDocumentoCompra
                    WHERE UPPER(RTRIM(co_tipo_doc)) = 'IVAN'
                `);
                const nextPhysN = Number(maxPhysRes.recordset[0]?.max_phys || 0) + 1;
                docNum = `${prefijo}${nextPhysN.toString().padStart(longitud, '0')}${sufijo}`;
                console.log(`⚠️ [RETENCIONES IVAN] nro_doc físico ajustado a "${docNum}" para evitar duplicidad de Primary Key en saDocumentoCompra (Comprobante fiscal se mantiene en ${assignedN})`);
            }
        } catch (eCheck) {
            console.warn(`[consecutivoRetenciones] Advertencia al verificar duplicidad física de nro_doc:`, eCheck.message);
        }
    }

    return {
        docNum,
        proxN: assignedN,
        prefijo,
        sufijo,
        longitud
    };
}

module.exports = {
    getProximoConsecutivoRetencion
};
