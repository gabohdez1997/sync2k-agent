/**
 * consecutivos.js — Helper centralizado para asignación y cálculo de correlativos de documentos en Profit Plus.
 * 
 * Consulta dinámicamente saSerieTipo para obtener:
 * - prefijo (ej. 'G3')
 * - sufijo (si aplica)
 * - longitud (por defecto 10 dígitos)
 * 
 * Incrementa atómicamente saSerie.prox_n y formatea el número de documento resultante:
 * `${prefijo}${String(prox_n).padStart(longitud, '0')}${sufijo}`
 */

/**
 * Mapeo predeterminado de tipos de documentos a sus tipos de serie y consecutivos en Profit Plus
 */
const DOC_TYPE_CONFIG = {
    COTIZACION: {
        co_tipo_serie: 'V004',
        co_consecutivos: ['COTI_NUM', 'CCLI_NUM', 'COTIZ_NUM'],
        table: 'saCotizacionCliente',
        col: 'doc_num'
    },
    PEDIDO: {
        co_tipo_serie: 'V003',
        co_consecutivos: ['PCLI_NUM', 'PED_NUM', 'PEDIDO_NUM'],
        table: 'saPedidoVenta',
        col: 'doc_num'
    },
    FACTURA: {
        co_tipo_serie: 'V001',
        co_consecutivos: ['DOC_VEN_FACT', 'FACT_NUM', 'FACT_VTA'],
        table: 'saFacturaVenta',
        col: 'doc_num'
    },
    COBRO: {
        co_tipo_serie: 'V024',
        co_consecutivos: ['COBRO', 'COBR_NUM', 'COB_NUM'],
        table: 'saCobro',
        col: 'cob_num'
    },
    AJUSTE: {
        co_tipo_serie: 'I001',
        co_consecutivos: ['AJUS_NUM', 'AJUS', 'AJU_NUM', 'AJU_ENT', 'AJU_SAL'],
        table: 'saAjuste',
        col: 'ajue_num'
    },
    TRASLADO: {
        co_tipo_serie: 'I002',
        co_consecutivos: ['TRAS_NUM', 'TRAS', 'TRASLADO_NUM'],
        table: 'saTraslado',
        col: 'tras_num'
    },
    NOTA_RECEPCION: {
        co_tipo_serie: 'C005',
        co_consecutivos: ['NREC_NUM', 'NREC', 'REC_NUM'],
        table: 'saNotaRecepcionCompra',
        col: 'doc_num'
    },
    ORDEN_COMPRA: {
        co_tipo_serie: 'C002',
        co_consecutivos: ['OCOM_NUM', 'OCOM', 'ORD_NUM'],
        table: 'saOrdenCompra',
        col: 'doc_num'
    }
};

/**
 * Obtiene e incrementa el próximo número de documento con su prefijo y formato.
 * 
 * @param {Object} params
 * @param {Object} params.runner - Pool o Transacción mssql activa
 * @param {string} params.co_tipo_serie - Código en saSerieTipo (ej: 'V004', 'V003', etc.) o clave de DOC_TYPE_CONFIG
 * @param {string[]} [params.co_consecutivos] - Códigos candidatos en saConsecutivo
 * @param {string} [params.co_sucur] - Código de sucursal opcional (ej: '01')
 * @param {string} [params.table] - Nombre de la tabla para fallback
 * @param {string} [params.col] - Columna del doc_num en la tabla para fallback
 * @returns {Promise<{ docNum: string, proxN: number, prefijo: string, sufijo: string, longitud: number }>}
 */
async function getProximoConsecutivo(params) {
    const { runner, co_sucur = '01' } = params;
    if (!runner) {
        throw new Error('getProximoConsecutivo: Se requiere un runner (pool o transaction de mssql).');
    }

    // Resolver configuración si se pasó un alias conocido (ej. 'COTIZACION')
    let co_tipo_serie = params.co_tipo_serie || '';
    let co_consecutivos = params.co_consecutivos || [];
    let table = params.table || '';
    let col = params.col || 'doc_num';

    if (DOC_TYPE_CONFIG[co_tipo_serie.toUpperCase()]) {
        const conf = DOC_TYPE_CONFIG[co_tipo_serie.toUpperCase()];
        co_tipo_serie = conf.co_tipo_serie;
        if (!co_consecutivos.length) co_consecutivos = conf.co_consecutivos;
        if (!table) table = conf.table;
        if (params.col === undefined) col = conf.col;
    }

    if (!co_consecutivos.length) {
        co_consecutivos = [co_tipo_serie];
    }

    let corrRow = null;

    // ── 1. Intento principal: saConsecutivo -> saSerie -> saSerieTipo ──
    try {
        const queryConsec = `
            UPDATE s
            SET s.prox_n = s.prox_n + 1, s.fe_us_mo = GETDATE()
            OUTPUT 
                INSERTED.prox_n,
                RTRIM(ISNULL(st.prefijo, '')) AS prefijo,
                RTRIM(ISNULL(st.sufijo, '')) AS sufijo,
                ISNULL(st.longitud, 10) AS longitud
            FROM saSerie s
            INNER JOIN saSerieTipo st ON s.co_tipo_serie = st.co_tipo_serie
            WHERE s.co_serie = (
                SELECT TOP 1 c.co_serie
                FROM saConsecutivo c
                WHERE c.co_serie IS NOT NULL 
                  AND (
                      UPPER(RTRIM(c.co_consecutivo)) IN (${co_consecutivos.map(c => `'${c.toUpperCase()}'`).join(',')})
                      OR UPPER(RTRIM(c.co_serie)) LIKE '${co_tipo_serie}%'
                  )
                ORDER BY CASE 
                    WHEN RTRIM(c.co_sucur) = '${co_sucur}' THEN 1 
                    WHEN c.co_sucur IS NULL THEN 2 
                    ELSE 3 
                END
            )
        `;
        const res = await runner.request().query(queryConsec);
        if (res.recordset && res.recordset.length > 0) {
            corrRow = res.recordset[0];
        }
    } catch (err1) {
        console.warn(`[CONSECUTIVOS] Falló intento 1 (saConsecutivo) para ${co_tipo_serie}:`, err1.message);
    }

    // ── 2. Fallback 1: saSerie directa por co_tipo_serie ──
    if (!corrRow) {
        try {
            const querySerieDirect = `
                UPDATE s
                SET s.prox_n = s.prox_n + 1, s.fe_us_mo = GETDATE()
                OUTPUT 
                    INSERTED.prox_n,
                    RTRIM(ISNULL(st.prefijo, '')) AS prefijo,
                    RTRIM(ISNULL(st.sufijo, '')) AS sufijo,
                    ISNULL(st.longitud, 10) AS longitud
                FROM saSerie s
                INNER JOIN saSerieTipo st ON s.co_tipo_serie = st.co_tipo_serie
                WHERE UPPER(RTRIM(s.co_tipo_serie)) = '${co_tipo_serie.toUpperCase()}'
            `;
            const res = await runner.request().query(querySerieDirect);
            if (res.recordset && res.recordset.length > 0) {
                corrRow = res.recordset[0];
            }
        } catch (err2) {
            console.warn(`[CONSECUTIVOS] Falló intento 2 (saSerie directo) para ${co_tipo_serie}:`, err2.message);
        }
    }

    // ── 3. Fallback 2: Consultar saSerieTipo y calcular MAX(col) de la tabla ──
    if (!corrRow) {
        try {
            const metaRes = await runner.request().query(`
                SELECT TOP 1
                    RTRIM(ISNULL(prefijo, '')) AS prefijo,
                    RTRIM(ISNULL(sufijo, '')) AS sufijo,
                    ISNULL(longitud, 10) AS longitud
                FROM saSerieTipo
                WHERE UPPER(RTRIM(co_tipo_serie)) = '${co_tipo_serie.toUpperCase()}'
            `);
            const meta = metaRes.recordset[0] || { prefijo: '', sufijo: '', longitud: 10 };

            let maxN = 1;
            if (table && col) {
                const maxRes = await runner.request().query(`
                    SELECT ISNULL(MAX(
                        CASE 
                            WHEN TRY_CAST(RIGHT(LTRIM(RTRIM(${col})), 10) AS BIGINT) IS NOT NULL 
                            THEN CAST(RIGHT(LTRIM(RTRIM(${col})), 10) AS BIGINT) 
                            ELSE 0 
                        END
                    ), 0) + 1 AS prox_n
                    FROM ${table}
                `);
                maxN = Number(maxRes.recordset[0]?.prox_n || 1);
            }

            corrRow = {
                prox_n: maxN,
                prefijo: meta.prefijo || '',
                sufijo: meta.sufijo || '',
                longitud: meta.longitud || 10
            };
        } catch (err3) {
            console.error(`[CONSECUTIVOS] Falló intento 3 (MAX fallback) para ${co_tipo_serie}:`, err3.message);
        }
    }

    if (!corrRow || corrRow.prox_n === undefined || corrRow.prox_n === null) {
        throw new Error(`No se pudo obtener ni generar el consecutivo para el tipo de documento ${co_tipo_serie}.`);
    }

    // ── 4. Validación de sincronización con la tabla física ──
    // Si saSerie.prox_n estaba desfasado/atrasado respecto a documentos existentes en la tabla,
    // sincronizarlo automáticamente hacia adelante para evitar saltos o colisiones.
    if (table && col) {
        try {
            const maxRes = await runner.request().query(`
                SELECT ISNULL(MAX(
                    CASE 
                        WHEN TRY_CAST(RIGHT(LTRIM(RTRIM(${col})), 10) AS BIGINT) IS NOT NULL 
                        THEN CAST(RIGHT(LTRIM(RTRIM(${col})), 10) AS BIGINT) 
                        ELSE 0 
                    END
                ), 0) AS max_n
                FROM ${table}
            `);
            const maxN = Number(maxRes.recordset[0]?.max_n || 0);
            if (maxN >= Number(corrRow.prox_n)) {
                const nextN = maxN + 1;
                console.log(`⚠️ [CONSECUTIVOS] saSerie.prox_n (${corrRow.prox_n}) estaba atrasado respecto a ${table} (${maxN}). Sincronizando a ${nextN}...`);
                corrRow.prox_n = nextN;
                await runner.request().query(`
                    UPDATE saSerie
                    SET prox_n = ${nextN}, fe_us_mo = GETDATE()
                    WHERE UPPER(RTRIM(co_tipo_serie)) = '${co_tipo_serie.toUpperCase()}'
                `);
            }
        } catch (eSync) {
            console.warn(`[CONSECUTIVOS] No se pudo verificar MAX en tabla ${table}:`, eSync.message);
        }
    }

    const prefijo = (corrRow.prefijo || '').trim();
    const sufijo = (corrRow.sufijo || '').trim();
    const longitud = Number(corrRow.longitud) || 10;
    const proxN = Number(corrRow.prox_n);
    const numStr = proxN.toString().padStart(longitud, '0');
    const docNum = `${prefijo}${numStr}${sufijo}`;

    return {
        docNum,
        proxN,
        prefijo,
        sufijo,
        longitud
    };
}

module.exports = {
    getProximoConsecutivo,
    DOC_TYPE_CONFIG
};
