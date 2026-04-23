/**
 * Helpers para operaciones multi-sede.
 * Simplifica lecturas y escrituras broadcast/targeted en múltiples instancias.
 */
const { getPool, getServers } = require('../db');

/**
 * Ejecuta una función de lectura en TODAS las sedes y agrega los resultados.
 * @param {Function} fn - async (pool, srv) => array de registros
 * @returns {Array} Array plano de todos los resultados combinados
 */
async function aggregateRead(sqlAuth, fn) {
    const servers = getServers();
    const results = await Promise.all(
        servers.map(async (srv) => {
            try {
                const pool = await getPool(srv.id, sqlAuth);
                const rows = await fn(pool, srv);
                return Array.isArray(rows) ? rows : [];
            } catch (e) {
                console.warn(`[multiSede] Error en sede ${srv.id}: ${e.message}`);
                return [];
            }
        })
    );
    return [].concat(...results);
}

/**
 * Ejecuta una función de lectura en TODAS las sedes y devuelve una colección única.
 * @param {Function} fn - async (pool, srv) => array de registros
 * @param {string} uniqueKey - campo clave para deduplicar
 * @param {string} [sortKey] - campo para ordenar alfabéticamente
 * @returns {Array} Array único y ordenado
 */
async function aggregateUnique(sqlAuth, fn, uniqueKey, sortKey = null) {
    const combined = await aggregateRead(sqlAuth, fn);
    const unique = Array.from(new Map(combined.map(item => [item[uniqueKey], item])).values());
    if (sortKey) unique.sort((a, b) => String(a[sortKey]).localeCompare(String(b[sortKey])));
    return unique;
}

/**
 * Ejecuta una operación de escritura en sedes específicas o en broadcast.
 * @param {string|null} sedeId - ID de sede específica, o null para broadcast
 * @param {Function} fn - async (pool, srv) => objeto de resultado
 * @returns {{ targets: Array, results: Array }}
 */
async function executeWrite(sedeId, sqlAuth, fn) {
    const servers = getServers();
    let targets = sedeId ? servers.filter(s => s.id === sedeId) : servers;

    // Fallback: si el ID exacto no coincide (p.ej. UUID de Firestore vs ID local),
    // intentar coincidencia por nombre (case-insensitive) o usar todos si solo hay uno.
    if (sedeId && targets.length === 0) {
        const byName = servers.filter(s =>
            (s.name || s.nombre || '').trim().toLowerCase() === sedeId.trim().toLowerCase()
        );
        if (byName.length > 0) {
            console.warn(`[multiSede] ID "${sedeId}" no coincide — usando coincidencia por nombre.`);
            targets = byName;
        } else if (servers.length === 1) {
            console.warn(`[multiSede] ID "${sedeId}" no coincide y solo hay 1 servidor — usando broadcast.`);
            targets = servers;
        } else {
            return { targets: [], results: [], notFound: true };
        }
    }

    if (targets.length === 0) {
        return { targets: [], results: [], notFound: true };
    }

    const results = await Promise.all(
        targets.map(async (srv) => {
            try {
                const pool = await getPool(srv.id, sqlAuth);
                const result = await fn(pool, srv);
                return { sede_id: srv.id, sede_nombre: srv.name, success: true, ...result };
            } catch (err) {
                return { sede_id: srv.id, sede_nombre: srv.name, success: false, error: err.message };
            }
        })
    );

    return { targets, results, notFound: false };
}

/**
 * Genera una respuesta de escritura estándar.
 * @param {object} res - Express response object
 * @param {{ results: Array, notFound: boolean }} outcome
 * @param {string} [notFoundMsg]
 */
function writeResponse(res, { results, notFound }, notFoundMsg = 'Sede no encontrada.') {
    if (notFound) return res.status(404).json({ success: false, message: notFoundMsg });
    
    const allOk = results.every(r => r.success);
    const anyOk = results.some(r => r.success);
    const status = allOk ? 200 : anyOk ? 207 : 500;
    
    let message = allOk ? 'Operación completada con éxito.' : 
                  anyOk ? 'Operación completada con fallas en algunas sedes.' :
                  'La operación falló en todas las sedes configuradas.';

    // Si falló todo, intentamos extraer el primer error para dar contexto
    if (!anyOk && results.length > 0) {
        message = `Error: ${results[0].error || 'Falla desconocida en el agente.'}`;
    }

    return res.status(status).json({ success: anyOk, message, results });
}

/**
 * Genera una respuesta de listado paginado estándar.
 * @param {object} res - Express response object
 * @param {Array} combined - Datos combinados (ya ordenados)
 * @param {number} page
 * @param {number} limit
 */
function paginatedResponse(res, combined, page, limit) {
    const total = combined.length;
    const data = combined.slice((page - 1) * limit, page * limit);
    return res.status(200).json({
        success: true,
        page,
        limit,
        total_items: total,
        total_pages: Math.ceil(total / limit),
        count: data.length,
        data
    });
}

/**
 * Rellena una cadena con espacios al final para coincidir con la longitud de un campo CHAR en Profit Plus.
 * @param {string|number|null} val - Valor a procesar
 * @param {number} length - Longitud total deseada
 * @returns {string} Cadena acolchada
 */
function padProfit(val, length) {
    if (val === null || val === undefined) return null;
    const str = String(val).trim();
    if (str.length >= length) return str.substring(0, length);
    return str.padEnd(length, ' ');
}

module.exports = { 
    aggregateRead, 
    aggregateUnique, 
    executeWrite, 
    writeResponse, 
    paginatedResponse,
    padProfit
};
