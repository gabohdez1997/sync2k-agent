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

function splitDescWords(desc) {
    if (!desc) return [];
    return desc
        .replace(/\.+(?!\d)|(?<!\d)\.+/g, ' ')
        .split(/\s+/)
        .map(w => w.replace(/^[,;:()]+|[,;:()]+$/g, ''))
        .filter(Boolean);
}

/**
 * Calidad de coincidencia de una variante dentro de una palabra:
 * 0 = palabra exacta (o medida exacta con "), 1 = prefijo/unidad, 2 = subcadena/dígito contiguo, 3 = subcadena interna, Infinity = sin match.
 */
function matchVariantInWord(word, variant) {
    if (!word || !variant) return Infinity;

    if (variant.endsWith('"')) {
        if (word === variant) return 0;
        if (word.startsWith(variant)) return 1;
        if (word.includes(variant)) return 2;
        return Infinity;
    }

    const wordNoQuote = word.replace(/"+$/, '');
    if (word === variant || wordNoQuote === variant) return 0;

    if (/\d$/.test(variant)) {
        if (word.startsWith(variant)) {
            const nextChar = word[variant.length];
            if (nextChar && !/\d/.test(nextChar)) return 1;
            return 2;
        }
        if (word.includes(variant)) return 3;
        return Infinity;
    }

    if (word.startsWith(variant)) return 1;
    if (word.includes(variant)) return 2;
    return Infinity;
}

/**
 * Busca la mejor coincidencia de un token (cualquiera de sus variantes) en descWords desde minPos.
 */
function findTokenMatch(desc, descWords, tokObj, minPos = 0, preferPosOverQuality = false) {
    let best = null;

    const consider = (pos, quality) => {
        if (pos < minPos || !Number.isFinite(quality)) return;
        if (!best) {
            best = { pos, quality };
            return;
        }
        if (preferPosOverQuality) {
            if (pos < best.pos || (pos === best.pos && quality < best.quality)) {
                best = { pos, quality };
            }
        } else {
            if (quality < best.quality || (quality === best.quality && pos < best.pos)) {
                best = { pos, quality };
            }
        }
    };

    for (let pos = minPos; pos < descWords.length; pos++) {
        const word = descWords[pos];
        for (const variant of tokObj.variants) {
            if (variant.includes(' ')) continue;
            const q = matchVariantInWord(word, variant);
            if (q < Infinity) consider(pos, q);
        }
    }

    // Variantes multi-palabra (ej. "4 1/2", "2 pulg")
    for (const variant of tokObj.variants) {
        if (!variant.includes(' ')) continue;
        let searchFrom = 0;
        while (searchFrom < desc.length) {
            const idx = desc.indexOf(variant, searchFrom);
            if (idx === -1) break;
            const beforeOk = idx === 0 || /[\s.;,:(/-]/.test(desc[idx - 1]);
            const afterIdx = idx + variant.length;
            const afterOk = afterIdx === desc.length || /[\s.;,:)"/-]/.test(desc[afterIdx]);
            if (beforeOk && afterOk) {
                const pos = splitDescWords(desc.slice(0, idx)).length;
                consider(pos, 0);
            }
            searchFrom = idx + 1;
        }
    }

    return best;
}

/**
 * Ordena resultados priorizando código/modelo/ref exactos y, sobre descripción,
 * coincidencia en la misma posición, orden de búsqueda y cercanía de tokens.
 */
function sortBySearchRelevance(items, rawSearch) {
    if (!Array.isArray(items) || items.length <= 1) return items;

    const normQuery = normalizeForSearch(rawSearch);
    const tokens = tokenizeSearch(rawSearch);
    if (!normQuery && !tokens.length) return items;

    const buildRankTuple = (item) => {
        if (!item || item.query_fallido) {
            return [9, 999, 999, 999, 999, 999, 999999, 999999, 999999];
        }

        const coArt = normalizeForSearch(item.co_art);
        const desc = normalizeForSearch(item.descripcion ?? item.art_des);
        const mod = normalizeForSearch(item.modelo);
        const ref = normalizeForSearch(item.referencia ?? item.ref);

        let idTier = 3;
        if (normQuery) {
            if (coArt === normQuery) idTier = 0;
            else if (mod === normQuery || ref === normQuery) idTier = 1;
            else if (/^[\d-]+$/.test(normQuery) && coArt.startsWith(normQuery)) idTier = 2;
        }

        const descWords = splitDescWords(desc);
        let missingInDesc = 0;
        let firstTokPos = 999;
        let firstTokQuality = 999;
        let outOfOrderCount = 0;
        let totalQuality = 0;
        let totalPosDistance = 0;
        const matchedPositions = [];

        if (tokens.length > 0) {
            const firstMatch = findTokenMatch(desc, descWords, tokens[0], 0, true);
            let prevPos = -1;
            if (firstMatch) {
                firstTokPos = firstMatch.pos;
                firstTokQuality = firstMatch.quality;
                prevPos = firstMatch.pos;
                totalQuality += firstMatch.quality;
                totalPosDistance += Math.abs(firstMatch.pos - 0);
                matchedPositions.push(firstMatch.pos);
            } else {
                missingInDesc += 1;
            }

            for (let i = 1; i < tokens.length; i++) {
                const startAt = prevPos >= 0 ? prevPos + 1 : 0;
                const inOrderMatch = findTokenMatch(desc, descWords, tokens[i], startAt, false);
                if (inOrderMatch) {
                    prevPos = inOrderMatch.pos;
                    totalQuality += inOrderMatch.quality;
                    totalPosDistance += Math.abs(inOrderMatch.pos - i);
                    matchedPositions.push(inOrderMatch.pos);
                } else {
                    const anyOrderMatch = startAt > 0
                        ? findTokenMatch(desc, descWords, tokens[i], 0, false)
                        : null;
                    if (anyOrderMatch) {
                        outOfOrderCount += 1;
                        totalQuality += anyOrderMatch.quality;
                        totalPosDistance += Math.abs(anyOrderMatch.pos - i);
                        matchedPositions.push(anyOrderMatch.pos);
                    } else {
                        missingInDesc += 1;
                    }
                }
            }
        }

        const span = matchedPositions.length >= 2
            ? Math.max(...matchedPositions) - Math.min(...matchedPositions)
            : 0;

        return [
            idTier,
            missingInDesc,
            firstTokPos,
            firstTokQuality,
            outOfOrderCount,
            totalQuality,
            totalPosDistance,
            span,
            desc.length
        ];
    };

    const tuples = new Map();
    for (const item of items) {
        tuples.set(item, buildRankTuple(item));
    }

    return items.sort((a, b) => {
        const ta = tuples.get(a);
        const tb = tuples.get(b);
        for (let i = 0; i < ta.length; i++) {
            if (ta[i] !== tb[i]) return ta[i] - tb[i];
        }

        const descA = String(a?.descripcion ?? a?.art_des ?? '');
        const descB = String(b?.descripcion ?? b?.art_des ?? '');
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
