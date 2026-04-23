/**
 * padChar.js — Utilidad para rellenar valores CHAR con espacios en blanco
 * 
 * SQL Server CHAR(n) almacena el valor + espacios hasta completar n caracteres.
 * Profit Plus compara con igualdad estricta, por lo que '01' ≠ '01                  '.
 * Esta función asegura que todos los valores enviados a los Stored Procedures
 * tengan el padding correcto.
 * 
 * Basado en: SQL/estructura_profit.sql
 */

// Mapa de campos CHAR con su longitud definida en el esquema de Profit Plus
// Formato: { nombre_parametro_sp: longitud_char }
const CHAR_FIELD_LENGTHS = {
    // ── saCliente ──
    co_cli:             16,
    co_seg:              6,
    co_zon:              6,
    co_ven:              6,
    co_mone:             6,
    cond_pag:            6,
    co_cond:             6,
    co_cta_ingr_egr:    20,
    tip_cli:             6,
    co_tran:             6,
    tipo_per:            1,
    co_pais:             6,
    co_us_in:            6,
    co_us_mo:            6,
    co_sucu_in:          6,
    co_sucu_mo:          6,
    revisado:            1,
    trasnfe:             1,
    estado:              1,
    serialP:            30,
    salestax:            8,
    n_cr:                6,
    n_db:                6,
    tcomp:               6,
    co_tab:              6,
    matriz:             16,
    
    // ── saCotizacionCliente ──
    doc_num:            20,
    status:              1,
    n_control:          20,

    // ── saCotizacionClienteReng ──
    co_art:             30,
    co_uni:              6,
    co_alma:             6,
    co_precio:           6,
    tipo_imp:            1,
    tipo_doc:            4,
};

/**
 * Rellena un valor string con espacios en blanco hasta la longitud
 * definida en el esquema de Profit Plus para campos CHAR.
 * 
 * @param {string} fieldName - Nombre del campo (debe existir en CHAR_FIELD_LENGTHS)
 * @param {string|null} value - Valor a rellenar
 * @returns {string|null} Valor rellenado con espacios, o null si el input es null/undefined
 * 
 * @example
 *   padChar('co_cta_ingr_egr', '01') → '01                  ' (20 chars)
 *   padChar('co_ven', 'V123')        → 'V123  ' (6 chars)
 *   padChar('unknown_field', 'test') → 'test' (sin cambio)
 */
function padChar(fieldName, value) {
    if (value === null || value === undefined) return null;
    
    const str = String(value);
    const len = CHAR_FIELD_LENGTHS[fieldName.toLowerCase()];
    
    if (!len) return str; // Campo no encontrado en el mapa, devolver sin cambio
    
    return str.padEnd(len, ' ');
}

/**
 * Aplica padChar a múltiples campos de un objeto.
 * Útil para sanitizar payloads completos antes de enviarlos a un SP.
 * 
 * @param {Object} data - Objeto con los campos a procesar
 * @param {string[]} fields - Lista de nombres de campo a procesar
 * @returns {Object} Nuevo objeto con los campos rellenados
 * 
 * @example
 *   padFields({ co_cli: 'V123', co_ven: '01' }, ['co_cli', 'co_ven'])
 *   → { co_cli: 'V123            ', co_ven: '01    ' }
 */
function padFields(data, fields) {
    const result = { ...data };
    for (const field of fields) {
        if (result[field] !== undefined && result[field] !== null) {
            result[field] = padChar(field, result[field]);
        }
    }
    return result;
}

module.exports = { padChar, padFields, CHAR_FIELD_LENGTHS };
