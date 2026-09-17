/**
 * consecutivoRetenciones.js
 * 
 * Helper para asignación y sincronización de correlativos únicos de retenciones (IVA e ISLR)
 * entre todas las sedes de Profit Plus (Boca de Río, Paraparal, etc.).
 * 
 * Garantiza que cuando una sede genere una retención (ej. 001):
 * 1. Se consulte la fuente de verdad central en Supabase Cloud (global_consecutivos).
 * 2. Se consulte el mayor comprobante físico emitido en Profit Plus (local y demás sedes accesibles)
 *    para blindar contra desfases si se emiten retenciones directamente desde el Desktop de Profit.
 * 3. Se asigne el correlativo estricto sin saltos ni duplicados.
 * 4. Se incremente y sincronice el nuevo correlativo (ej. 002) atómicamente:
 *    - En saSerie de la sede actual (en la misma transacción del pago).
 *    - En Supabase Cloud (global_consecutivos) de forma inmediata.
 *    - En saSerie de las demás sedes remotas y PostgreSQL local.
 */

const { getPool, getServers, getAllActiveServers, pgPool } = require('../db');
const { DOC_TYPE_CONFIG } = require('./consecutivos');

const SUPABASE_URL = process.env.SUPABASE_URL || process.env.PUBLIC_SUPABASE_URL || 'https://rwblykcpnduniexbivra.supabase.co';
const SUPABASE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY || 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InJ3Ymx5a2NwbmR1bmlleGJpdnJhIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc4MTUxNzI0NCwiZXhwIjoyMDk3MDkzMjQ0fQ.Q04ibUleUEFCPcOsQ73qJI4W8nwDupfwACDeIczFAnw';

/**
 * Consulta el próximo correlativo registrado centralmente en Supabase Cloud.
 */
async function getSupabaseConsecutivo(tipo) {
    try {
        const res = await fetch(`${SUPABASE_URL}/rest/v1/global_consecutivos?tipo=eq.${encodeURIComponent(tipo)}&select=prox_n`, {
            headers: {
                apikey: SUPABASE_KEY,
                Authorization: `Bearer ${SUPABASE_KEY}`
            }
        });
        if (res.ok) {
            const rows = await res.json();
            if (rows && rows.length > 0) {
                const n = Number(rows[0].prox_n);
                if (Number.isFinite(n) && n > 0) return n;
            }
        }
    } catch (err) {
        console.warn(`⚠️ [consecutivoRetenciones] Error al consultar Supabase Cloud:`, err.message);
    }
    return null;
}

/**
 * Actualiza el próximo correlativo en Supabase Cloud (fuente de verdad multi-sede).
 */
async function updateSupabaseConsecutivo(tipo, prox_n) {
    try {
        const res = await fetch(`${SUPABASE_URL}/rest/v1/global_consecutivos`, {
            method: 'POST',
            headers: {
                apikey: SUPABASE_KEY,
                Authorization: `Bearer ${SUPABASE_KEY}`,
                'Content-Type': 'application/json',
                'Prefer': 'resolution=merge-duplicates'
            },
            body: JSON.stringify({
                tipo: tipo,
                prox_n: prox_n,
                updated_at: new Date().toISOString()
            })
        });
        if (res.ok) {
            console.log(`☁️ [RETENCIONES SUPABASE] Sincronizado en la nube: ${tipo} -> prox_n=${prox_n}`);
        } else {
            console.warn(`⚠️ [RETENCIONES SUPABASE] Falló actualización en Supabase (${res.status}):`, await res.text());
        }
    } catch (err) {
        console.warn(`⚠️ [consecutivoRetenciones] Error al guardar en Supabase Cloud:`, err.message);
    }
}

/**
 * Inicializa la tabla global_consecutivos en PostgreSQL local si existe y está activo.
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
        // PG local es opcional
    }
}

/**
 * Obtiene el próximo correlativo único sincronizado entre todas las sedes para retenciones.
 * Consulta en el momento exacto:
 * 1. Supabase Cloud (fuente centralizada compartida entre todas las sedes).
 * 2. Comprobantes físicos reales emitidos en Profit Plus (local y remotas accesibles).
 * 3. saSerie local.
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
    
    // Obtener TODAS las sedes activas (consulta Supabase Cloud primero)
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

    // ═══════════════════════════════════════════════════════════════════════════
    // REGLA PRINCIPAL: Obtener el MAX de comprobantes físicos emitidos en TODAS
    // las sedes (local + remotas) y asignar MAX + 1. Los documentos físicos
    // SIEMPRE tienen prioridad sobre cualquier puntero en Supabase o saSerie.
    // ═══════════════════════════════════════════════════════════════════════════
    const maxIssuedNumbers = [];
    const nextPointers = [];
    const sedesConsultadas = [];
    const sedesFallidas = [];

    // ── 2. Consultar comprobantes físicos emitidos en la sede LOCAL ──
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
            sedesConsultadas.push(currentSrvId || 'LOCAL');
        } catch (eCompLocal) {
            console.warn(`[consecutivoRetenciones] ⚠️ Error al consultar max_comprobante en sede LOCAL:`, eCompLocal.message);
            sedesFallidas.push(currentSrvId || 'LOCAL');
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
            sedesConsultadas.push(currentSrvId || 'LOCAL');
        } catch (eDocLocal) {
            console.warn(`[consecutivoRetenciones] ⚠️ Error al consultar max_doc ISLR en sede LOCAL:`, eDocLocal.message);
            sedesFallidas.push(currentSrvId || 'LOCAL');
        }
    }

    // ── 3. Consultar comprobantes físicos en TODAS las demás sedes (OBLIGATORIO) ──
    const otherServers = allServers.filter(s => s.id && s.id !== currentSrvId);
    await Promise.all(otherServers.map(async (otherSrv) => {
        try {
            const otherPool = await getPool(otherSrv.id, sqlAuth);

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
            sedesConsultadas.push(otherSrv.id);
        } catch (eOther) {
            console.error(`⚠️ [consecutivoRetenciones] NO SE PUDO CONSULTAR sede remota ${otherSrv.id} (${otherSrv.name}): ${eOther.message}`);
            sedesFallidas.push(otherSrv.id);
        }
    }));

    // ── 4. Consultar punteros en Supabase Cloud y PostgreSQL local como respaldo ──
    const supabaseProxN = await getSupabaseConsecutivo(profitSerieCode);
    if (supabaseProxN && supabaseProxN > 0) {
        nextPointers.push(supabaseProxN);
    }

    try {
        const pgRes = await pgPool.query(`SELECT prox_n FROM global_consecutivos WHERE tipo = $1`, [profitSerieCode]);
        if (pgRes.rows.length > 0) {
            const pgN = Number(pgRes.rows[0].prox_n);
            if (pgN > 0) nextPointers.push(pgN);
        }
    } catch (ePg) {
        // PG local opcional
    }

    // ── 5. Calcular número fiscal asignable ──
    // REGLA: El comprobante asignado = MAX(todos los comprobantes físicos de TODAS las sedes) + 1
    // Si hubo sedes fallidas, también considerar Supabase/PG como respaldo
    const maxIssued = maxIssuedNumbers.length > 0 ? Math.max(...maxIssuedNumbers) : 0;
    const supabaseN = Number(supabaseProxN) || 0;
    const maxPointer = nextPointers.length > 0 ? Math.max(...nextPointers) : 0;

    let assignedN = 1;

    // Caso principal: hay comprobantes físicos emitidos → asignar MAX + 1
    if (maxIssued > 0) {
        assignedN = maxIssued + 1;
        // Si Supabase o PG apuntan a un número aún mayor, respetar ese puntero
        // (esto cubre el caso donde se eliminó un comprobante y Supabase ya avanzó)
        if (maxPointer > assignedN) {
            assignedN = maxPointer;
        }
    } else if (maxPointer > 0) {
        // No hay comprobantes físicos pero Supabase/PG tienen un puntero
        assignedN = maxPointer;
    } else {
        // Último recurso: usar saSerie local
        assignedN = Number(localMeta.prox_n) || 1;
    }

    if (assignedN < 1) assignedN = 1;

    // El siguiente correlativo que debe quedar guardado para el próximo documento
    const nextN = assignedN + 1;

    console.log(`🎯 [RETENCIONES GLOBAL] Tipo: ${profitSerieCode} (${docTypeProfit}) | Sedes consultadas: [${sedesConsultadas.join(', ')}] | Sedes fallidas: [${sedesFallidas.join(', ') || 'ninguna'}] | Mayor físico emitido: ${maxIssued} | Supabase prox_n: ${supabaseN} | PG prox_n: ${maxPointer} | ► ASIGNADO: ${assignedN} | Siguiente: ${nextN}`);

    // ── 6. Actualizar saSerie en la sede actual al siguiente número ──
    await runner.request().query(`
        UPDATE saSerie
        SET prox_n = ${nextN}, fe_us_mo = GETDATE()
        WHERE UPPER(RTRIM(co_tipo_serie)) = '${profitSerieCode.toUpperCase()}'
    `);

    // ── 7. Actualizar Supabase Cloud de inmediato ──
    await updateSupabaseConsecutivo(profitSerieCode, nextN);

    // ── 8. Actualizar PostgreSQL local si existe ──
    try {
        await pgPool.query(`
            INSERT INTO global_consecutivos (tipo, prox_n, updated_at)
            VALUES ($1, $2, NOW())
            ON CONFLICT (tipo)
            DO UPDATE SET prox_n = GREATEST(global_consecutivos.prox_n, EXCLUDED.prox_n), updated_at = NOW()
        `, [profitSerieCode, nextN]);
    } catch (ePgUp) {
        // PG local opcional
    }

    // ── 9. Propagar nextN a TODAS las demás sedes (síncrono para evitar race conditions) ──
    try {
        await Promise.allSettled(otherServers.map(async (otherSrv) => {
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
                console.warn(`⚠️ [RETENCIONES] No se pudo sincronizar sede remota ${otherSrv.id}: ${errRemote.message}`);
            }
        }));
    } catch (ePropagation) {
        console.warn(`⚠️ [RETENCIONES] Error en propagación multi-sede:`, ePropagation.message);
    }

    // ── 10. Formatear y retornar número de documento ──
    const numStr = assignedN.toString().padStart(longitud, '0');
    let docNum = `${prefijo}${numStr}${sufijo}`;

    // Si es IVAN, verificar que docNum no colisione físicamente en saDocumentoCompra de la sede local
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
                            WHEN TRY_CAST(RIGHT(LTRIM(RTRIM(nro_doc)), ${longitud}) AS BIGINT) IS NOT NULL 
                            THEN CAST(RIGHT(LTRIM(RTRIM(nro_doc)), ${longitud}) AS BIGINT) 
                            ELSE 0 
                        END
                    ), 0) AS max_phys
                    FROM saDocumentoCompra
                    WHERE UPPER(RTRIM(co_tipo_doc)) = 'IVAN'
                `);
                const maxPhysFromDocs = Number(maxPhysRes.recordset[0]?.max_phys || 0);
                const maxFromSerie = Number(localMeta.prox_n || 0);
                const nextPhysN = Math.max(maxPhysFromDocs, maxFromSerie) + 1;
                docNum = `${prefijo}${nextPhysN.toString().padStart(longitud, '0')}${sufijo}`;
                console.log(`⚠️ [RETENCIONES IVAN] nro_doc físico local ajustado a "${docNum}" para evitar duplicidad de PK en saDocumentoCompra (Comprobante fiscal SENIAT se mantiene en ${assignedN})`);
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

/**
 * Al eliminar un pago que contenía un comprobante de retención (IVA o ISLR),
 * verifica si dicho comprobante correspondía al ÚLTIMO emitido en el sistema.
 * Si es el último emitido (es decir, ningún comprobante físico con número mayor existe):
 * Reestablece el próximo correlativo a (mayor_físico_restante + 1) en:
 * 1. Supabase Cloud (global_consecutivos)
 * 2. PostgreSQL local (global_consecutivos)
 * 3. saSerie en la sede actual
 * 4. saSerie en las demás sedes remotas alcanzables
 *
 * @param {Object} params
 * @param {Object} params.runner - Pool o transacción mssql de la sede actual
 * @param {string} params.co_tipo_serie - 'IVAN_COMPRA' o 'ISLR_COMPRA' (o 'C016' / 'C015')
 * @param {string|number} params.deletedNum - Número o comprobante que se eliminó
 * @param {string} [params.currentSrvId] - ID de la sede actual
 * @param {Object} [params.sqlAuth] - Credenciales para sedes remotas
 * @returns {Promise<{ reverted: boolean, newNextN?: number, currentMax?: number }>}
 */
async function revertirConsecutivoRetencionIfLast(params) {
    const { runner, currentSrvId, sqlAuth } = params;
    if (!runner) return { reverted: false };

    let delCorrelativo = null;
    const rawVal = params.deletedNum;
    if (typeof rawVal === 'number') {
        delCorrelativo = rawVal;
    } else if (typeof rawVal === 'string') {
        const digits = rawVal.trim().replace(/\D/g, '');
        if (digits.length >= 8) {
            delCorrelativo = parseInt(digits.slice(-8), 10);
        } else if (digits.length > 0) {
            delCorrelativo = parseInt(digits, 10);
        }
    }

    if (!delCorrelativo || isNaN(delCorrelativo) || delCorrelativo <= 0) {
        console.log(`[revertirConsecutivoRetencion] No se pudo determinar el correlativo numérico de:`, rawVal);
        return { reverted: false };
    }

    const typeKey = (params.co_tipo_serie || '').toUpperCase();
    const conf = DOC_TYPE_CONFIG[typeKey] || {
        co_tipo_serie: typeKey === 'IVAN_COMPRA' ? 'C016' : typeKey === 'ISLR_COMPRA' ? 'C015' : typeKey,
        table: 'saDocumentoCompra',
        col: 'nro_doc'
    };
    const profitSerieCode = conf.co_tipo_serie;
    const docTypeProfit = typeKey === 'IVAN_COMPRA' || profitSerieCode === 'C016' ? 'IVAN' : 'ISLR';

    const maxIssuedNumbers = [];

    // 1. Consultar comprobantes físicos restantes en la sede local
    if (docTypeProfit === 'IVAN') {
        try {
            const r1 = await runner.request().query(`
                SELECT ISNULL(MAX(
                    CASE 
                        WHEN TRY_CAST(RIGHT(LTRIM(RTRIM(num_comprobante)), 8) AS BIGINT) IS NOT NULL 
                        THEN CAST(RIGHT(LTRIM(RTRIM(num_comprobante)), 8) AS BIGINT) 
                        ELSE 0 
                    END
                ), 0) AS max_c
                FROM saPagoRetenIvaReng
            `);
            const m1 = Number(r1.recordset[0]?.max_c || 0);
            if (m1 > 0) maxIssuedNumbers.push(m1);

            const r2 = await runner.request().query(`
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
            const m2 = Number(r2.recordset[0]?.max_c || 0);
            if (m2 > 0) maxIssuedNumbers.push(m2);
        } catch (e) {
            console.warn('[revertirConsecutivoRetencion] Error consultando max_c local:', e.message);
        }
    } else if (docTypeProfit === 'ISLR') {
        try {
            const rIslr = await runner.request().query(`
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
            const mIslr = Number(rIslr.recordset[0]?.max_n || 0);
            if (mIslr > 0) maxIssuedNumbers.push(mIslr);
        } catch (e) {
            console.warn('[revertirConsecutivoRetencion] Error consultando max_n ISLR local:', e.message);
        }
    }

    // 2. Consultar en las demás sedes remotas si están conectadas
    const allServers = await getAllActiveServers();
    const otherServers = allServers.filter(s => s.id && s.id !== currentSrvId);
    await Promise.all(otherServers.map(async (otherSrv) => {
        try {
            const otherPool = await getPool(otherSrv.id, sqlAuth);
            if (docTypeProfit === 'IVAN') {
                const o1 = await otherPool.request().query(`
                    SELECT ISNULL(MAX(
                        CASE 
                            WHEN TRY_CAST(RIGHT(LTRIM(RTRIM(num_comprobante)), 8) AS BIGINT) IS NOT NULL 
                            THEN CAST(RIGHT(LTRIM(RTRIM(num_comprobante)), 8) AS BIGINT) 
                            ELSE 0 
                        END
                    ), 0) AS max_c
                    FROM saPagoRetenIvaReng
                `);
                const om1 = Number(o1.recordset[0]?.max_c || 0);
                if (om1 > 0) maxIssuedNumbers.push(om1);

                const o2 = await otherPool.request().query(`
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
                const om2 = Number(o2.recordset[0]?.max_c || 0);
                if (om2 > 0) maxIssuedNumbers.push(om2);
            } else if (docTypeProfit === 'ISLR') {
                const oIslr = await otherPool.request().query(`
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
                const omIslr = Number(oIslr.recordset[0]?.max_n || 0);
                if (omIslr > 0) maxIssuedNumbers.push(omIslr);
            }
        } catch (e) {}
    }));

    const currentMaxPhysical = maxIssuedNumbers.length > 0 ? Math.max(...maxIssuedNumbers) : 0;

    // 3. Evaluar si el comprobante eliminado era efectivamente el último emitido
    if (delCorrelativo > currentMaxPhysical) {
        const newNextN = currentMaxPhysical + 1;
        console.log(`🔄 [RETENCIONES REVERT] Comprobante ${delCorrelativo} era el último emitido para ${profitSerieCode}. Revertiendo próximo correlativo a ${newNextN} (máximo físico actual: ${currentMaxPhysical})`);

        // A) Actualizar Supabase Cloud
        await updateSupabaseConsecutivo(profitSerieCode, newNextN);

        // B) Actualizar PostgreSQL local si está disponible
        try {
            await pgPool.query(`
                INSERT INTO global_consecutivos (tipo, prox_n, updated_at)
                VALUES ($1, $2, NOW())
                ON CONFLICT (tipo)
                DO UPDATE SET prox_n = EXCLUDED.prox_n, updated_at = NOW()
            `, [profitSerieCode, newNextN]);
        } catch (ePg) {}

        // C) Actualizar saSerie en la sede actual
        await runner.request().query(`
            UPDATE saSerie
            SET prox_n = ${newNextN}, fe_us_mo = GETDATE()
            WHERE UPPER(RTRIM(co_tipo_serie)) = '${profitSerieCode.toUpperCase()}'
        `);

        // D) Replicar a sedes remotas en segundo plano
        Promise.allSettled(otherServers.map(async (otherSrv) => {
            try {
                const remotePool = await getPool(otherSrv.id, sqlAuth);
                await remotePool.request().query(`
                    UPDATE saSerie
                    SET prox_n = ${newNextN}, fe_us_mo = GETDATE()
                    WHERE UPPER(RTRIM(co_tipo_serie)) = '${profitSerieCode.toUpperCase()}'
                `);
            } catch (e) {}
        })).catch(() => {});

        return { reverted: true, newNextN, currentMax: currentMaxPhysical, delCorrelativo };
    } else {
        console.log(`ℹ️ [RETENCIONES REVERT] Comprobante ${delCorrelativo} NO era el último (máximo físico actual: ${currentMaxPhysical}). No se retrocede el consecutivo para preservar correlatividad.`);
        return { reverted: false, currentMax: currentMaxPhysical, delCorrelativo };
    }
}

module.exports = {
    getProximoConsecutivoRetencion,
    revertirConsecutivoRetencionIfLast,
    updateSupabaseConsecutivo
};
