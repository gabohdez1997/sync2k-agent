const https = require('https');
const path = require('path');
const { execFile } = require('child_process');
const { sql, getPool, getServers, pgPool } = require('../db');

const SUPABASE_URL = process.env.SUPABASE_URL || process.env.PUBLIC_SUPABASE_URL || 'https://rwblykcpnduniexbivra.supabase.co';
const SUPABASE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY || 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InJ3Ymx5a2NwbmR1bmlleGJpdnJhIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc4MTUxNzI0NCwiZXhwIjoyMDk3MDkzMjQ0fQ.Q04ibUleUEFCPcOsQ73qJI4W8nwDupfwACDeIczFAnw';

const BCV_URL = 'https://www.bcv.org.ve/';
const CARACAS_TIMEZONE = 'America/Caracas';
const SCHEDULE_HOUR_CARACAS = 6;
const SCHEDULER_POLL_INTERVAL_MS = 30 * 1000;
const WINDOWS_TASK_NAME = 'Sync2k_BCV_Tasa_6AM';

let schedulerTimer = null;
let schedulerStartedAt = null;
let lastRunDateCaracas = null;
let lastSyncStatus = null;
let activeSyncPromise = null;

/**
 * Obtiene la fecha y hora actual en zona horaria America/Caracas (UTC-4).
 */
function getCaracasDateTimeParts(date = new Date()) {
    const formatter = new Intl.DateTimeFormat('en-CA', {
        timeZone: CARACAS_TIMEZONE,
        year: 'numeric',
        month: '2-digit',
        day: '2-digit',
        hour: '2-digit',
        minute: '2-digit',
        second: '2-digit',
        hour12: false
    });

    const parts = {};
    for (const part of formatter.formatToParts(date)) {
        if (part.type !== 'literal') {
            parts[part.type] = part.value;
        }
    }

    const hour = parts.hour === '24' ? '00' : parts.hour;
    const dateStr = `${parts.year}-${parts.month}-${parts.day}`;
    const timeStr = `${hour}:${parts.minute}:${parts.second}`;

    return {
        dateStr,
        hour,
        minute: parts.minute,
        second: parts.second,
        formatted: `${dateStr} ${timeStr} (America/Caracas)`
    };
}

/**
 * Convierte texto numérico con formato venezolano/europeo ("914,02180000" o "1.234,56") a número.
 */
function parseBcvRateText(rawText) {
    let val = String(rawText || '').trim();
    if (!val) return NaN;

    if (val.includes('.') && val.includes(',')) {
        val = val.replace(/\./g, '').replace(',', '.');
    } else {
        val = val.replace(',', '.');
    }

    return parseFloat(val);
}

/**
 * Realiza una petición HTTPS GET (tolerando certificados incompletos del BCV).
 */
function fetchUrlHtml(urlString, redirectsLeft = 3) {
    return new Promise((resolve, reject) => {
        const parsed = new URL(urlString);
        const options = {
            hostname: parsed.hostname,
            port: parsed.port || 443,
            path: `${parsed.pathname}${parsed.search}`,
            method: 'GET',
            headers: {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Language': 'es-ES,es;q=0.9'
            },
            rejectUnauthorized: false,
            timeout: 15000
        };

        const req = https.request(options, (res) => {
            if ([301, 302, 303, 307, 308].includes(res.statusCode) && res.headers.location && redirectsLeft > 0) {
                res.resume();
                const nextUrl = new URL(res.headers.location, urlString).toString();
                resolve(fetchUrlHtml(nextUrl, redirectsLeft - 1));
                return;
            }

            if (res.statusCode && res.statusCode >= 400) {
                res.resume();
                reject(new Error(`HTTP ${res.statusCode} al consultar ${urlString}`));
                return;
            }

            let data = '';
            res.setEncoding('utf8');
            res.on('data', chunk => { data += chunk; });
            res.on('end', () => resolve(data));
        });

        req.on('error', reject);
        req.on('timeout', () => {
            req.destroy();
            reject(new Error('Timeout al conectar con bcv.org.ve'));
        });

        req.end();
    });
}

/**
 * Extrae la tasa USD y la fecha valor del HTML de bcv.org.ve.
 */
function extractUsdFromBcvHtml(html) {
    let rawTasa = null;
    let metodo = null;

    // Buscar: <div id="dolar" ...> ... <strong> 914,02180000 </strong>
    const dolarMatch = html.match(/id="dolar"[\s\S]*?<strong[^>]*>\s*([\d.,]+)\s*<\/strong>/);
    if (dolarMatch && dolarMatch[1]) {
        rawTasa = parseBcvRateText(dolarMatch[1]);
        metodo = 'id=dolar';
    }

    // Fallback: buscar "USD" seguido de un strong con cifra
    if (!rawTasa || isNaN(rawTasa)) {
        const usdMatch = html.match(/USD[\s\S]*?<strong[^>]*>\s*([\d.,]+)/i);
        if (usdMatch && usdMatch[1]) {
            rawTasa = parseBcvRateText(usdMatch[1]);
            metodo = 'fallback-USD';
        }
    }

    if (!rawTasa || isNaN(rawTasa) || rawTasa <= 0) {
        return null;
    }

    const fechaMatch =
        html.match(/property="dc:date"\s+content="([^"]+)"/i) ||
        html.match(/class="date-display-single"[^>]*>([^<]+)</i);
    const fechaBcv = fechaMatch && fechaMatch[1]
        ? fechaMatch[1].replace(/\s+/g, ' ').trim()
        : null;

    return {
        tasa: Math.round(rawTasa * 100) / 100,
        rawTasa,
        moneda: 'USD',
        fuente: BCV_URL,
        metodo,
        fechaBcv,
        fecha: new Date().toISOString()
    };
}

/**
 * Consulta la tasa oficial del dólar USD desde https://www.bcv.org.ve/ con hasta 3 intentos.
 */
async function fetchBcvUsdRate(maxAttempts = 3) {
    let lastError = null;

    for (let attempt = 1; attempt <= maxAttempts; attempt++) {
        try {
            const html = await fetchUrlHtml(BCV_URL);
            const parsed = extractUsdFromBcvHtml(html);
            if (!parsed) {
                const parseErr = new Error('No se pudo extraer la tasa del dólar de bcv.org.ve.');
                parseErr.code = 'BCV_PARSE_ERROR';
                throw parseErr;
            }
            return parsed;
        } catch (err) {
            lastError = err;
            if (attempt < maxAttempts) {
                console.warn(`[BCV SYNC] Intento ${attempt}/${maxAttempts} fallido (${err.message}). Reintentando en 1.5s...`);
                await new Promise(r => setTimeout(r, 1500));
            }
        }
    }

    throw lastError;
}

/**
 * Compara la última tasa USD registrada en saTasa contra bcvRate e inserta un nuevo registro solo si es diferente.
 */
async function syncBcvRateForPool(pool, options = {}) {
    const rawRate = Number(options.bcvRate);
    if (!rawRate || isNaN(rawRate) || rawRate <= 0) {
        throw new Error('Tasa BCV inválida.');
    }

    const targetRate = Math.round(rawRate * 100) / 100;
    const rawUser = options.user && String(options.user).trim()
        ? String(options.user).trim().substring(0, 6).toUpperCase()
        : 'PROFIT';
    const dryRun = Boolean(options.dryRun);

    const moneRes = await pool.request().query(
        "SELECT co_mone FROM saMoneda WHERE LTRIM(RTRIM(co_mone)) IN ('USD', 'US$')"
    );
    const codes = moneRes.recordset.map(r => r.co_mone.trim());

    if (codes.length === 0) {
        throw new Error('No se encontró la moneda USD o US$ en la tabla saMoneda.');
    }

    const details = [];

    for (const code of codes) {
        const lastRes = await pool.request()
            .input('mone', sql.Char(6), code)
            .query(`
                SELECT TOP 1
                    RTRIM(co_mone) AS co_mone,
                    tasa_v,
                    tasa_c,
                    CONVERT(VARCHAR(19), fecha, 120) AS fecha_str,
                    fecha
                FROM saTasa
                WHERE LTRIM(RTRIM(co_mone)) = LTRIM(RTRIM(@mone))
                ORDER BY fecha DESC
            `);

        const lastRow = lastRes.recordset[0] || null;
        const lastRate = lastRow && lastRow.tasa_v != null
            ? Math.round(Number(lastRow.tasa_v) * 100) / 100
            : null;
        const lastFecha = lastRow ? (lastRow.fecha_str || lastRow.fecha) : null;

        if (lastRate !== null && Math.abs(lastRate - targetRate) < 0.005) {
            details.push({
                code,
                updated: false,
                action: 'unchanged',
                previousRate: lastRate,
                newRate: targetRate,
                lastFecha
            });
            continue;
        }

        if (dryRun) {
            details.push({
                code,
                updated: false,
                action: 'would_insert',
                previousRate: lastRate,
                newRate: targetRate,
                lastFecha,
                dryRun: true
            });
            continue;
        }

        const insertRes = await pool.request()
            .input('tasa', sql.Decimal(21, 8), targetRate)
            .input('mone', sql.Char(6), code)
            .input('user', sql.VarChar(6), rawUser)
            .query(`
                DECLARE @nowSmall SMALLDATETIME = CAST(GETDATE() AS SMALLDATETIME);
                DECLARE @maxFecha SMALLDATETIME = (
                    SELECT MAX(fecha)
                    FROM saTasa
                    WHERE LTRIM(RTRIM(co_mone)) = LTRIM(RTRIM(@mone))
                );
                DECLARE @insertFecha SMALLDATETIME = CASE
                    WHEN @maxFecha IS NOT NULL AND @nowSmall <= @maxFecha THEN DATEADD(MINUTE, 1, @maxFecha)
                    ELSE @nowSmall
                END;
                DECLARE @finalUser CHAR(6) = ISNULL(NULLIF(RTRIM(@user), ''), 'PROFIT');

                INSERT INTO saTasa (co_mone, tasa_v, tasa_c, fecha, co_us_in, fe_us_in, co_us_mo, fe_us_mo)
                VALUES (@mone, @tasa, @tasa, @insertFecha, @finalUser, GETDATE(), @finalUser, GETDATE());

                SELECT CONVERT(VARCHAR(19), @insertFecha, 120) AS inserted_fecha;
            `);

        const insertedFecha = insertRes.recordset[0]?.inserted_fecha || null;

        details.push({
            code,
            updated: true,
            action: 'inserted',
            previousRate: lastRate,
            newRate: targetRate,
            lastFecha,
            insertedFecha
        });
    }

    const primary = details[0] || {};
    const anyUpdated = details.some(d => d.updated);
    const anyWouldInsert = details.some(d => d.action === 'would_insert');

    return {
        code: primary.code || 'USD',
        updated: anyUpdated,
        action: anyUpdated ? 'inserted' : (anyWouldInsert ? 'would_insert' : 'unchanged'),
        previousRate: primary.previousRate ?? null,
        newRate: targetRate,
        lastFecha: primary.lastFecha || null,
        insertedFecha: primary.insertedFecha || null,
        details
    };
}

/**
 * Valida si un string tiene formato UUID estándar.
 */
function isValidUuid(id) {
    return typeof id === 'string' && /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(id.trim());
}

/**
 * Registra un evento de cambio de tasa en la auditoría de Supabase Cloud (tabla audit_log)
 * y en PostgreSQL Local como respaldo / réplica offline.
 */
async function logAuditRecordToSupabase(entry = {}) {
    const supabaseUrl = process.env.SUPABASE_URL || process.env.PUBLIC_SUPABASE_URL || SUPABASE_URL;
    const supabaseKey = process.env.SUPABASE_SERVICE_ROLE_KEY || SUPABASE_KEY;

    const branchId = isValidUuid(entry.branch_id) ? entry.branch_id.trim() : null;
    const payload = {
        action: entry.action || 'UPDATE',
        module: entry.module || 'cash_exchange',
        record_id: entry.record_id || 'BCV_RATE',
        user_email: entry.user_email || 'sistema@sync2k.local',
        branch_id: branchId,
        old_data: entry.old_data || null,
        new_data: entry.new_data || null,
        metadata: entry.metadata || null,
        source: entry.source || 'agent'
    };

    let cloudSaved = false;

    // 1. Guardado en Supabase Cloud vía REST API
    try {
        const res = await fetch(`${supabaseUrl}/rest/v1/audit_log`, {
            method: 'POST',
            headers: {
                'apikey': supabaseKey,
                'Authorization': `Bearer ${supabaseKey}`,
                'Content-Type': 'application/json',
                'Prefer': 'return=minimal'
            },
            body: JSON.stringify(payload)
        });

        if (res.ok) {
            cloudSaved = true;
            console.log(`☁️ [AUDIT BCV] Registro de auditoría guardado en Supabase Cloud (${entry.branchName || branchId || 'Global'}).`);
        } else {
            const errBody = await res.text().catch(() => '');
            console.warn(`⚠️ [AUDIT BCV] Supabase Cloud retornó HTTP ${res.status}: ${errBody}`);
        }
    } catch (cloudErr) {
        console.warn(`⚠️ [AUDIT BCV] Error al conectar con Supabase Cloud: ${cloudErr.message}`);
    }

    // 2. Guardado en PostgreSQL Local (sync2k local)
    try {
        if (pgPool) {
            await pgPool.query(
                `INSERT INTO audit_log (action, module, record_id, user_email, branch_id, old_data, new_data, metadata, source)
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
                [
                    payload.action,
                    payload.module,
                    payload.record_id,
                    payload.user_email,
                    payload.branch_id,
                    payload.old_data ? JSON.stringify(payload.old_data) : null,
                    payload.new_data ? JSON.stringify(payload.new_data) : null,
                    payload.metadata ? JSON.stringify(payload.metadata) : null,
                    payload.source
                ]
            );
            console.log(`💾 [AUDIT BCV] Registro de auditoría guardado en PostgreSQL Local (${entry.branchName || branchId || 'Global'}).`);
        }
    } catch (pgErr) {
        if (!cloudSaved) {
            console.warn(`⚠️ [AUDIT BCV] Error al guardar auditoría en PG Local: ${pgErr.message}`);
        }
    }
}

/**
 * Ejecuta la sincronización BCV -> saTasa en todas las sedes configuradas.
 */
async function executeBcvAutoSync(options = {}) {
    const executedAtCaracas = getCaracasDateTimeParts().formatted;
    const dryRun = Boolean(options.dryRun);
    const trigger = options.trigger || 'manual';
    const user = options.user || 'PROFIT';

    console.log(`[BCV SYNC] (${executedAtCaracas}) Iniciando sincronización [${trigger}]${dryRun ? ' (DRY-RUN)' : ''}...`);

    let bcvInfo;
    try {
        bcvInfo = await fetchBcvUsdRate();
        console.log(`[BCV SYNC] Tasa oficial BCV: ${bcvInfo.tasa} Bs. (raw: ${bcvInfo.rawTasa}, fecha BCV: ${bcvInfo.fechaBcv || 'N/A'})`);
    } catch (err) {
        const errorSummary = {
            success: false,
            trigger,
            dryRun,
            executedAtCaracas,
            error: err.message,
            bcv: null,
            results: []
        };
        if (!dryRun) lastSyncStatus = errorSummary;
        console.error(`[BCV SYNC] ❌ Error consultando BCV: ${err.message}`);
        return errorSummary;
    }

    const servers = getServers();
    const results = [];

    for (const srv of servers) {
        try {
            const pool = await getPool(srv.id, options.sqlAuth || null);
            const syncResult = await syncBcvRateForPool(pool, {
                bcvRate: bcvInfo.tasa,
                user,
                dryRun,
                sedeName: srv.name
            });

            if (syncResult.action === 'inserted') {
                console.log(`[BCV SYNC] ✅ Sede "${srv.name}": Nueva tasa registrada (${syncResult.previousRate ?? 'N/A'} -> ${syncResult.newRate}) @ ${syncResult.insertedFecha}`);

                if (!dryRun) {
                    await logAuditRecordToSupabase({
                        action: 'UPDATE',
                        module: 'cash_exchange',
                        record_id: 'BCV_RATE',
                        user_email: user === 'PROFIT' ? 'sistema@sync2k.local' : user,
                        branch_id: srv.id,
                        branchName: srv.name,
                        old_data: {
                            tasa: syncResult.previousRate,
                            fecha_anterior: syncResult.lastFecha
                        },
                        new_data: {
                            tasa: syncResult.newRate,
                            fuente: bcvInfo.fuente || BCV_URL,
                            fecha_bcv: bcvInfo.fechaBcv || null,
                            raw_tasa: bcvInfo.rawTasa || null,
                            metodo: bcvInfo.metodo || null,
                            fecha_insercion: syncResult.insertedFecha,
                            moneda: 'USD',
                            trigger
                        },
                        metadata: {
                            message: `Tasa BCV sincronizada automáticamente a ${syncResult.newRate} Bs/$ para sede ${srv.name}`,
                            sede_nombre: srv.name,
                            trigger,
                            executed_at_caracas: executedAtCaracas
                        },
                        source: 'agent'
                    });
                }
            } else if (syncResult.action === 'would_insert') {
                console.log(`[BCV SYNC] 🔍 Sede "${srv.name}" [DRY-RUN]: Insertaría nueva tasa (${syncResult.previousRate ?? 'N/A'} -> ${syncResult.newRate})`);
            } else {
                console.log(`[BCV SYNC] ℹ️ Sede "${srv.name}": Sin cambios (última tasa en BD: ${syncResult.previousRate} == BCV: ${syncResult.newRate})`);
            }

            results.push({
                sede_id: srv.id,
                sede_nombre: srv.name,
                success: true,
                ...syncResult
            });
        } catch (err) {
            console.error(`[BCV SYNC] ❌ Error en sede "${srv.name}": ${err.message}`);
            results.push({
                sede_id: srv.id,
                sede_nombre: srv.name,
                success: false,
                error: err.message
            });
        }
    }

    const summary = {
        success: servers.length > 0 && results.every(r => r.success),
        trigger,
        dryRun,
        executedAtCaracas,
        bcv: bcvInfo,
        results
    };

    if (!dryRun) {
        lastSyncStatus = summary;
    }

    return summary;
}

async function runBcvAutoSync(options = {}) {
    if (activeSyncPromise) {
        return activeSyncPromise;
    }

    activeSyncPromise = executeBcvAutoSync(options).finally(() => {
        activeSyncPromise = null;
    });

    return activeSyncPromise;
}

/**
 * Verifica/crea automáticamente la tarea programada de Windows en cualquier VM donde se sincronice el agente.
 */
function execFileAsync(cmd, args) {
    return new Promise((resolve, reject) => {
        execFile(cmd, args, { windowsHide: true }, (err, stdout, stderr) => {
            if (err) {
                err.stdout = stdout;
                err.stderr = stderr;
                return reject(err);
            }
            resolve({ stdout, stderr });
        });
    });
}

async function ensureWindowsScheduledTask() {
    if (process.platform !== 'win32') {
        return { skipped: true, reason: 'non-windows' };
    }

    const nodePath = process.execPath;
    const scriptPath = path.resolve(__dirname, '..', 'scripts', 'sync_bcv_rate.js');
    const taskCommand = `"${nodePath}" "${scriptPath}"`;

    try {
        const { stdout } = await execFileAsync('schtasks.exe', ['/Query', '/TN', WINDOWS_TASK_NAME, '/XML']);
        const normalizedXml = String(stdout || '').toLowerCase();
        const hasNode = normalizedXml.includes(nodePath.toLowerCase());
        const hasScript = normalizedXml.includes(scriptPath.toLowerCase());
        const hasSixAm = normalizedXml.includes('t06:00:00');

        if (hasNode && hasScript && hasSixAm) {
            return { ensured: true, action: 'exists', taskName: WINDOWS_TASK_NAME, command: taskCommand };
        }
    } catch {
        // La tarea aún no existe en esta VM; se creará a continuación
    }

    try {
        await execFileAsync('schtasks.exe', [
            '/Create',
            '/TN', WINDOWS_TASK_NAME,
            '/TR', taskCommand,
            '/SC', 'DAILY',
            '/ST', '06:00',
            '/F'
        ]);
        console.log(`🗓️ [BCV SCHEDULER] Tarea programada de Windows "${WINDOWS_TASK_NAME}" configurada diariamente a las 06:00 AM (${scriptPath}).`);
        return { ensured: true, action: 'created_or_updated', taskName: WINDOWS_TASK_NAME, command: taskCommand };
    } catch (err) {
        const detail = (err.stderr || err.message || '').trim();
        console.warn(`⚠️ [BCV SCHEDULER] No se pudo registrar automáticamente la tarea de Windows "${WINDOWS_TASK_NAME}": ${detail}. Puedes ejecutar scripts\\INSTALAR_TAREA_BCV.bat como administrador.`);
        return { ensured: false, action: 'error', error: detail };
    }
}

/**
 * Inicia el programador diario que se ejecuta a las 06:00 AM hora de Caracas (UTC-4).
 */
function startBcvDailyScheduler() {
    if (schedulerTimer) {
        return schedulerTimer;
    }

    const nowCaracas = getCaracasDateTimeParts();
    schedulerStartedAt = nowCaracas.formatted;
    console.log(`⏰ [BCV SCHEDULER] Programador diario activo: 06:00 AM America/Caracas (hora actual: ${nowCaracas.formatted})`);

    if (process.platform === 'win32') {
        ensureWindowsScheduledTask().catch(() => {});
    }

    const checkSchedule = async () => {
        try {
            const current = getCaracasDateTimeParts();
            const hourNum = Number(current.hour);
            const minuteNum = Number(current.minute);

            if (hourNum === SCHEDULE_HOUR_CARACAS && minuteNum <= 2 && lastRunDateCaracas !== current.dateStr) {
                lastRunDateCaracas = current.dateStr;
                await runBcvAutoSync({ trigger: 'cron-6am-caracas', user: 'PROFIT' });
            }
        } catch (err) {
            console.error('[BCV SCHEDULER] Error en ciclo del programador:', err.message);
        }
    };

    checkSchedule();
    schedulerTimer = setInterval(checkSchedule, SCHEDULER_POLL_INTERVAL_MS);
    return schedulerTimer;
}

function getBcvSchedulerStatus() {
    return {
        enabled: true,
        activeInProcess: Boolean(schedulerTimer),
        schedule: '06:00 America/Caracas (UTC-4)',
        currentCaracasTime: getCaracasDateTimeParts().formatted,
        schedulerStartedAt,
        lastRunDateCaracas,
        lastSyncStatus
    };
}

module.exports = {
    fetchBcvUsdRate,
    syncBcvRateForPool,
    runBcvAutoSync,
    startBcvDailyScheduler,
    getBcvSchedulerStatus,
    getCaracasDateTimeParts,
    ensureWindowsScheduledTask,
    logAuditRecordToSupabase
};

