/**
 * Utilidad Nivel 1 para búsqueda fragmentada, insensible a tildes (CI_AI)
 * y normalizada para nomenclatura de ferretería en Profit Plus 2k12.
 */

const STOPWORDS = new Set([
    'de', 'del', 'd', 'd.',
    'para', 'p', 'p.',
    'con', 'c', 'c.',
    'en', 'el', 'la', 'los', 'las',
    'un', 'una', 'por', 'al', 'a',
    'y', 'e', 'o', 'x', '-'
]);

const KNOWN_UNITS = new Set([
    'mm', 'cm', 'm', 'mt', 'mts',
    'kg', 'gr', 'g',
    'ml', 'lt', 'l', 'gal',
    'w', 'v', 'amp', 'lb',
    'pzas', 'pza'
]);

const FRACTION_TO_DECIMAL = new Map([
    ['1/2', '5'],
    ['1/4', '25'],
    ['3/4', '75']
]);

const DECIMAL_TO_FRACTION = new Map([
    ['5', '1/2'],
    ['50', '1/2'],
    ['25', '1/4'],
    ['75', '3/4']
]);

const MAX_VARIANTS_PER_TOKEN = 6;

/**
 * Normaliza texto para comparación: recorta, remueve diacríticos, unifica
 * comillas tipográficas y símbolo ×, y pasa a minúsculas.
 */
function normalizeForSearch(str) {
    if (str == null) return '';
    return String(str)
        .trim()
        .normalize('NFD')
        .replace(/[\u0300-\u036f]/g, '')
        .replace(/[“”″]/g, '"')
        .replace(/[‘’′]/g, "'")
        .replace(/×/g, 'x')
        .toLowerCase()
        .replace(/\s+/g, ' ')
        .trim();
}

/**
 * Escapa comodines de LIKE en SQL Server ([ ] % _).
 */
function escapeLike(str) {
    return String(str)
        .replace(/\[/g, '[[]')
        .replace(/%/g, '[%]')
        .replace(/_/g, '[_]');
}

function addVariant(list, value, limit = MAX_VARIANTS_PER_TOKEN) {
    if (!value || list.length >= limit) return;
    if (!list.includes(value)) list.push(value);
}

/**
 * Genera variantes útiles de ferretería y singular/plural (máx 6 por token).
 */
function expandTokenVariants(token) {
    const variants = [];
    const clean = normalizeForSearch(token);
    if (!clean) return variants;

    addVariant(variants, clean);

    const endsWithInch = clean.endsWith('"');
    const baseNoQuote = endsWithInch ? clean.slice(0, -1) : clean;

    // 1. Pulgadas ("):
    if (endsWithInch && baseNoQuote) {
        if (baseNoQuote.includes('/') || baseNoQuote.includes('-')) {
            // Ej: 1/2" -> 1/2 ; 4-1/2" -> 4-1/2, 4 1/2
            addVariant(variants, baseNoQuote);
        } else {
            // Ej: 2" -> 2", 2 pulg (sin degradar a '2' suelto para no cruzar con 20MM o co_art)
            addVariant(variants, `${baseNoQuote} pulg`);
        }
    }

    // 2. Fracción mixta: ej. 4-1/2, 1-1/2 (con o sin ")
    const mixedMatch = baseNoQuote.match(/^(\d+)-(\d+\/\d+)$/);
    if (mixedMatch) {
        const [, whole, frac] = mixedMatch;
        addVariant(variants, `${whole} ${frac}`);
        const decSuffix = FRACTION_TO_DECIMAL.get(frac);
        if (decSuffix) {
            addVariant(variants, `${whole}.${decSuffix}`);
            addVariant(variants, `${whole},${decSuffix}`);
        }
    }

    // 3. Decimal con unidad opcional: ej. 0.9mm, 4.8, 1.5kg, 4.5, 2.00mm
    const decMatch = clean.match(/^(\d+)[.,](\d+)([a-z"]*)$/i);
    if (decMatch) {
        const [, intPart, fracPart, unitRaw] = decMatch;
        const unit = unitRaw.toLowerCase();

        // Equivalentes . <-> ,
        addVariant(variants, `${intPart},${fracPart}${unit}`);
        addVariant(variants, `${intPart}.${fracPart}${unit}`);

        // Equivalentes de fracción cuando no hay unidad (o es ") para .5 / .25 / .75
        if (!unit || unit === '"') {
            const fracEquiv = DECIMAL_TO_FRACTION.get(fracPart);
            if (fracEquiv) {
                if (intPart === '0') {
                    addVariant(variants, fracEquiv);
                    addVariant(variants, `${fracEquiv}"`);
                } else {
                    addVariant(variants, `${intPart}-${fracEquiv}`);
                    addVariant(variants, `${intPart} ${fracEquiv}`);
                }
            }
        }

        // Si parte decimal tiene 1 dígito (ej. 0.9mm -> 0,90mm / 0.90mm)
        if (fracPart.length === 1) {
            addVariant(variants, `${intPart},${fracPart}0${unit}`);
            addVariant(variants, `${intPart}.${fracPart}0${unit}`);
        }

        // Si tiene ceros a la derecha (ej. 2.00mm -> 2mm / 2,0mm ; 0.90mm -> 0,9mm)
        const strippedFrac = fracPart.replace(/0+$/, '');
        if (strippedFrac !== fracPart) {
            if (strippedFrac === '') {
                addVariant(variants, `${intPart}${unit}`);
                addVariant(variants, `${intPart},0${unit}`);
            } else {
                addVariant(variants, `${intPart},${strippedFrac}${unit}`);
                addVariant(variants, `${intPart}.${strippedFrac}${unit}`);
            }
        }

        // Forma separada si la unidad es conocida (ej. 1,5 kg / 1.5 kg)
        if (unit && KNOWN_UNITS.has(unit)) {
            addVariant(variants, `${intPart},${fracPart} ${unit}`);
            addVariant(variants, `${intPart}.${fracPart} ${unit}`);
        }
    }

    // 4. Entero + unidad conocida: ej. 2mm, 3mm, 4mm, 15kg, 1gal
    const intUnitMatch = clean.match(/^(\d+)([a-z]+)$/i);
    if (intUnitMatch) {
        const [, intPart, unitRaw] = intUnitMatch;
        const unit = unitRaw.toLowerCase();
        if (KNOWN_UNITS.has(unit)) {
            addVariant(variants, `${intPart},00${unit}`);
            addVariant(variants, `${intPart}.00${unit}`);
            addVariant(variants, `${intPart} ${unit}`);
        }
    }

    // 5. Singular / plural básico en español (solo tokens alfabéticos)
    if (/^[a-zñ]+$/.test(clean)) {
        if (clean.endsWith('ces') && clean.length >= 5) {
            addVariant(variants, `${clean.slice(0, -3)}z`);
        } else if (/[rlndj]es$/.test(clean) && clean.length >= 6) {
            addVariant(variants, clean.slice(0, -2));
            addVariant(variants, clean.slice(0, -1));
        } else if (clean.endsWith('s') && clean.length >= 5 && !clean.endsWith('ss') && !clean.endsWith('is')) {
            addVariant(variants, clean.slice(0, -1));
        }
    }

    return variants.slice(0, MAX_VARIANTS_PER_TOKEN);
}

/**
 * Fragmenta el texto de búsqueda en tokens normalizados y sus variantes.
 */
function tokenizeSearch(rawSearch, maxTokens = 8) {
    let text = normalizeForSearch(rawSearch);
    if (!text) return [];

    // Convertir "2 pulg", "2 pulgadas", "1/2 pulg." -> 2", 1/2"
    text = text.replace(/(\d+(?:[-/]\d+|[.,]\d+)?)\s*(?:pulgadas?|pulg\.?)\b/gi, '$1"');

    // Separar dimensiones compactas solo entre dígito/" y dígito (ej. 40x40x4 -> 40 40 4, 2"x2" -> 2" 2")
    text = text.replace(/([\d"])\s*[xX]\s*(?=\d)/g, '$1 ');

    const rawTokens = text
        .split(/\s+/)
        .map(t => t.replace(/^[,;:()\-]+|[,;:()\-]+$/g, ''))
        .filter(Boolean);

    if (!rawTokens.length) return [];

    // Quitar conectores solo si queda al menos un token útil
    const nonStop = rawTokens.filter(t => !STOPWORDS.has(t));
    const effective = nonStop.length > 0 ? nonStop : rawTokens;

    const seen = new Set();
    const deduped = [];
    for (const tok of effective) {
        if (!seen.has(tok)) {
            seen.add(tok);
            deduped.push(tok);
            if (deduped.length >= maxTokens) break;
        }
    }

    return deduped.map(token => ({
        token,
        variants: expandTokenVariants(token)
    }));
}

/**
 * Construye una cláusula WHERE parametrizada para SQL Server (CI_AI) donde todos
 * los tokens deben existir en alguno de los campos del artículo (en cualquier orden).
 */
function buildArticleSearchFilter(rawSearch, options = {}) {
    const columns = options.columns || ['a.co_art', 'a.art_des', 'a.modelo', 'a.ref'];
    const paramPrefix = options.paramPrefix || 'qtok';
    const maxTokens = options.maxTokens || 8;

    const tokens = tokenizeSearch(rawSearch, maxTokens);
    if (!tokens.length) {
        return { clause: '', params: [], tokens: [] };
    }

    const haystack = `(${columns.map(c => `RTRIM(ISNULL(${c}, ''))`).join(" + ' ' + ")}) COLLATE Latin1_General_CI_AI`;
    const params = [];
    const tokenClauses = [];

    tokens.forEach((tokObj, i) => {
        const variantClauses = [];
        tokObj.variants.forEach((variant, j) => {
            const paramName = `${paramPrefix}_${i}_${j}`;
            params.push({ name: paramName, value: `%${escapeLike(variant)}%` });
            variantClauses.push(`${haystack} LIKE @${paramName}`);
        });
        tokenClauses.push(`(${variantClauses.join(' OR ')})`);
    });

    return {
        clause: `(${tokenClauses.join(' AND ')})`,
        params,
        tokens
    };
}

/**
 * Registra los parámetros generados por buildArticleSearchFilter en un sql.Request.
 */
function bindSearchParams(request, params, sql) {
    if (!request || !Array.isArray(params) || !params.length) return;
    for (const p of params) {
        request.input(p.name, sql.VarChar, p.value);
    }
}

/**
 * Ordena resultados en memoria priorizando coincidencias exactas de código/modelo/ref,
 * frase exacta en descripción, coincidencia al inicio y cantidad de tokens en descripción.
 */
function sortBySearchRelevance(items, rawSearch) {
    if (!Array.isArray(items) || items.length <= 1) return items;

    const normQuery = normalizeForSearch(rawSearch);
    const tokens = tokenizeSearch(rawSearch);
    if (!normQuery && !tokens.length) return items;

    const scoreItem = (item) => {
        if (!item || item.query_fallido) return -1;
        const coArt = normalizeForSearch(item.co_art);
        const desc = normalizeForSearch(item.descripcion ?? item.art_des);
        const mod = normalizeForSearch(item.modelo);
        const ref = normalizeForSearch(item.referencia ?? item.ref);

        let score = 0;

        if (normQuery) {
            if (coArt === normQuery) score += 1000;
            else if (coArt.startsWith(normQuery)) score += 800;

            if (mod === normQuery || ref === normQuery) score += 700;
            if (desc.includes(normQuery)) score += 500;
        }

        if (tokens.length > 0) {
            if (tokens[0].variants.some(v => desc.startsWith(v))) {
                score += 300;
            }
            let directDescMatches = 0;
            for (const tok of tokens) {
                if (tok.variants.some(v => desc.includes(v))) {
                    directDescMatches += 1;
                }
            }
            score += directDescMatches * 40;
        }

        return score;
    };

    const scored = new Map();
    for (const item of items) {
        scored.set(item, scoreItem(item));
    }

    return items.sort((a, b) => {
        const diff = (scored.get(b) || 0) - (scored.get(a) || 0);
        if (diff !== 0) return diff;

        const descA = String(a?.descripcion ?? a?.art_des ?? '');
        const descB = String(b?.descripcion ?? b?.art_des ?? '');
        if (descA.length !== descB.length) return descA.length - descB.length;

        return descA.localeCompare(descB);
    });
}

module.exports = {
    normalizeForSearch,
    escapeLike,
    expandTokenVariants,
    tokenizeSearch,
    buildArticleSearchFilter,
    bindSearchParams,
    sortBySearchRelevance
};
