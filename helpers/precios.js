const { sql } = require('../db');

/**
 * Normaliza códigos de precio ('01' -> '1', '02' -> '2', etc.)
 * para empatar formato Profit ('1'..'9') con códigos de dos dígitos ('01'..'09').
 */
function normalizePriceCode(code) {
    const s = String(code || '').trim().toUpperCase();
    if (!s) return '1';
    if (/^\d+$/.test(s)) return String(parseInt(s, 10));
    return s;
}

/**
 * Determina si un renglón tiene precio especial explícitamente autorizado.
 */
function isAuthorizedCustomPrice(item) {
    return String(item?.comentario || '').trim().toUpperCase().startsWith('PRECIO_AUT:');
}

/**
 * Consulta en SQL Server (saArtPrecio) el último precio vigente por artículo y tipo de precio.
 * Devuelve Map<CO_ART_UPPER, { byCode: Map<normCode, priceObj>, first: priceObj }>.
 */
async function fetchLatestArticlePrices(runner, artCodes, tasa) {
    const uniqueArts = [...new Set(
        (artCodes || []).map((c) => String(c || '').trim()).filter(Boolean)
    )];
    const priceMap = new Map();
    if (!uniqueArts.length || !runner) return priceMap;

    const safeTasa = Number(tasa) > 0 ? Number(tasa) : 1;
    const chunkSize = 400;

    for (let offset = 0; offset < uniqueArts.length; offset += chunkSize) {
        const chunk = uniqueArts.slice(offset, offset + chunkSize);
        const req = new sql.Request(runner);
        const placeholders = chunk.map((code, idx) => {
            const pName = `a${idx}`;
            req.input(pName, sql.VarChar(60), code);
            return `@${pName}`;
        }).join(', ');

        const res = await req.query(`
            WITH UP AS (
                SELECT LTRIM(RTRIM(p.co_art)) AS co_art,
                       LTRIM(RTRIM(p.co_precio)) AS co_precio,
                       p.monto,
                       LTRIM(RTRIM(ISNULL(p.co_mone, ''))) AS co_mone,
                       ROW_NUMBER() OVER(
                           PARTITION BY LTRIM(RTRIM(p.co_art)), LTRIM(RTRIM(p.co_precio))
                           ORDER BY p.desde DESC
                       ) AS rn
                FROM saArtPrecio p
                WHERE LTRIM(RTRIM(p.co_art)) IN (${placeholders})
                  AND p.Inactivo = 0
                  AND GETDATE() >= p.desde
                  AND (p.hasta IS NULL OR GETDATE() <= p.hasta)
                  AND p.monto > 0
            )
            SELECT co_art, co_precio, monto, co_mone
            FROM UP
            WHERE rn = 1
        `);

        for (const row of (res.recordset || [])) {
            const artKey = String(row.co_art || '').trim().toUpperCase();
            if (!artKey) continue;

            const normCode = normalizePriceCode(row.co_precio);
            const coMone = String(row.co_mone || '').trim().toUpperCase();
            const monto = Number(row.monto || 0);
            const isUsd = coMone.includes('US') || coMone === '$';
            const precio_usd = isUsd
                ? monto
                : Number((monto / safeTasa).toFixed(5));
            const precio_bs = isUsd
                ? Number((monto * safeTasa).toFixed(5))
                : monto;

            const priceObj = {
                co_precio: String(row.co_precio || '').trim(),
                norm_code: normCode,
                monto,
                co_mone: coMone,
                precio_usd,
                precio_bs
            };

            let entry = priceMap.get(artKey);
            if (!entry) {
                entry = { byCode: new Map(), first: null };
                priceMap.set(artKey, entry);
            }
            entry.byCode.set(normCode, priceObj);
        }
    }

    for (const entry of priceMap.values()) {
        if (entry.byCode.has('1')) {
            entry.first = entry.byCode.get('1');
        } else {
            const sortedKeys = [...entry.byCode.keys()].sort((a, b) => {
                const na = Number(a);
                const nb = Number(b);
                if (!Number.isNaN(na) && !Number.isNaN(nb)) return na - nb;
                return a.localeCompare(b);
            });
            entry.first = sortedKeys.length ? entry.byCode.get(sortedKeys[0]) : null;
        }
    }

    return priceMap;
}

/**
 * Obtiene el precio de catálogo para un artículo y código de precio dados.
 */
function resolveCatalogPrice(priceMap, coArt, coPrecio) {
    const artKey = String(coArt || '').trim().toUpperCase();
    const entry = priceMap?.get(artKey);
    if (!entry) return null;
    return entry.byCode.get(normalizePriceCode(coPrecio || '01')) || entry.first || null;
}

/**
 * Resuelve el precio final a guardar para un renglón:
 * - Si NO tiene autorización explícita (PRECIO_AUT:), usa siempre el último precio vigente del catálogo según su tipo (co_precio).
 * - Si tiene autorización explícita o no existe fila vigente en catálogo, respeta el precio enviado.
 */
function resolveLinePricing(item, priceMap, { isUSD, tasaDoc }) {
    const safeTasa = Number(tasaDoc) > 0 ? Number(tasaDoc) : 1;
    const coPrecio = String(
        item.co_precio || item.id_precio || item.co_tipo_precio || '01'
    ).trim().substring(0, 6);
    const authorized = isAuthorizedCustomPrice(item);

    if (!authorized) {
        const latest = resolveCatalogPrice(priceMap, item.co_art, coPrecio);
        if (latest && latest.precio_usd > 0) {
            return {
                coPrecio,
                prcUSD: latest.precio_usd,
                prcBs: latest.precio_bs,
                authorized: false,
                fromCatalog: true
            };
        }
    }

    const prcIn = Number(item.precio || 0);
    const prcBs = isUSD ? (prcIn * safeTasa) : prcIn;
    const prcUSD = isUSD
        ? prcIn
        : (Number(item.prec_vta_om) > 0 ? Number(item.prec_vta_om) : (prcIn / safeTasa));

    return {
        coPrecio,
        prcUSD,
        prcBs,
        authorized,
        fromCatalog: false
    };
}

module.exports = {
    normalizePriceCode,
    isAuthorizedCustomPrice,
    fetchLatestArticlePrices,
    resolveCatalogPrice,
    resolveLinePricing
};
