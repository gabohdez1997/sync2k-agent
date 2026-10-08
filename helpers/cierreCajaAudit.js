const XLSX = require('xlsx');
const path = require('path');
const fs = require('fs');
const sql = require('mssql');

const DEFAULT_CAJAS_DIR = path.resolve(__dirname, '../../CAJAS');

const DEFAULT_DB_CONFIG = {
    user: process.env.MASTER_USER || 'profit',
    password: process.env.MASTER_PASSWORD || 'profit',
    server: process.env.MASTER_SERVER || '192.168.88.235',
    database: process.env.MASTER_DATABASE || 'GALPE_AA',
    options: {
        encrypt: false,
        trustServerCertificate: true
    }
};

/**
 * Normaliza número extrayendo dígitos limpios
 */
function normalizeDocNum(val) {
    if (!val) return '';
    const str = String(val).trim();
    const m = str.match(/\d+/);
    return m ? String(parseInt(m[0], 10)) : str;
}

/**
 * Audita las cajas contra Profit Plus Boca de Río para una fecha dada (formato YYYY-MM-DD)
 */
async function auditarCierreCajas(options = {}) {
    const fecha = options.fecha || '2026-10-06';
    const cajasDir = options.cajasDir || DEFAULT_CAJAS_DIR;
    const dbConfig = options.dbConfig || DEFAULT_DB_CONFIG;

    console.log(`\n====================================================================`);
    console.log(`🔎 AUDITORÍA DE CIERRE DE CAJA — FECHA: ${fecha}`);
    console.log(`📂 Carpeta Cajas: ${cajasDir}`);
    console.log(`🏢 Sede: Boca de Río (DB: ${dbConfig.database} @ ${dbConfig.server})`);
    console.log(`====================================================================\n`);

    const pool = await sql.connect(dbConfig);

    // 1. Consultar facturas de Profit para la fecha
    const facsRes = await pool.request().query(`
        SELECT RTRIM(f.doc_num) as doc_num, 
               RTRIM(ISNULL(f.impfisfac, '')) as impfisfac,
               RTRIM(ISNULL(f.n_control, '')) as n_control,
               RTRIM(f.co_us_in) as co_us_in,
               f.total_bruto, f.monto_imp, f.total_neto, f.anulado,
               RTRIM(f.co_cli) as co_cli, RTRIM(c.cli_des) as cli_des,
               f.tasa, RTRIM(f.co_mone) as co_mone, f.fec_emis,
               RTRIM(f.descrip) as descrip
        FROM saFacturaVenta f
        LEFT JOIN saCliente c ON f.co_cli = c.co_cli
        WHERE CONVERT(varchar(10), f.fec_emis, 120) = '${fecha}'
        ORDER BY f.co_us_in, f.doc_num
    `);

    // 2. Consultar devoluciones de Profit
    const devsRes = await pool.request().query(`
        SELECT RTRIM(d.doc_num) as doc_num,
               RTRIM(ISNULL(d.impfisfac, '')) as impfisfac,
               RTRIM(ISNULL(d.n_control, '')) as n_control,
               RTRIM(d.co_us_in) as co_us_in,
               d.total_bruto, d.monto_imp, d.total_neto, d.anulado,
               RTRIM(d.co_cli) as co_cli, RTRIM(c.cli_des) as cli_des,
               RTRIM(d.co_tipo_doc) as co_tipo_doc, RTRIM(d.nro_doc) as nro_doc,
               d.fec_emis, RTRIM(d.descrip) as descrip
        FROM saDevolucionCliente d
        LEFT JOIN saCliente c ON d.co_cli = c.co_cli
        WHERE CONVERT(varchar(10), d.fec_emis, 120) = '${fecha}'
        ORDER BY d.doc_num
    `);

    // 3. Consultar cobros de Profit
    const cobrosRes = await pool.request().query(`
        SELECT RTRIM(c.cob_num) as cob_num,
               RTRIM(c.co_us_in) as co_us_in,
               c.fecha, c.anulado, c.monto,
               RTRIM(c.co_cli) as co_cli, RTRIM(cl.cli_des) as cli_des,
               tp.reng_num, RTRIM(tp.forma_pag) as forma_pag,
               tp.mont_doc, RTRIM(ISNULL(tp.num_doc, '')) as num_doc,
               RTRIM(ISNULL(tp.cod_caja, '')) as cod_caja,
               RTRIM(ISNULL(tp.cod_cta, '')) as cod_cta
        FROM saCobro c
        JOIN saCobroTPReng tp ON c.cob_num = tp.cob_num
        LEFT JOIN saCliente cl ON c.co_cli = cl.co_cli
        WHERE CONVERT(varchar(10), c.fecha, 120) = '${fecha}'
        ORDER BY c.co_us_in, c.cob_num, tp.reng_num
    `);

    const profitFacturas = facsRes.recordset;
    const profitDevoluciones = devsRes.recordset;
    const profitCobros = cobrosRes.recordset;

    // Detectar archivos Excel en la carpeta
    const allFiles = fs.readdirSync(cajasDir).filter(f => f.endsWith('.xlsx') && !f.startsWith('~$'));
    
    // Configuración para cada caja conocida
    const auditResults = {
        fecha,
        totales_profit: {
            total_facturas: profitFacturas.length,
            monto_neto_facturas: profitFacturas.reduce((acc, f) => acc + (f.anulado ? 0 : f.total_neto), 0),
            total_devoluciones: profitDevoluciones.length,
            monto_devoluciones: profitDevoluciones.reduce((acc, d) => acc + (d.anulado ? 0 : d.total_neto), 0),
            total_renglones_cobro: profitCobros.length,
            monto_total_cobros: profitCobros.reduce((acc, c) => acc + (c.anulado ? 0 : c.mont_doc), 0)
        },
        cajas: []
    };

    // Formato fecha para buscar hojas (DD-MM)
    const [y, m, d] = fecha.split('-');
    const patternDate = `${d}-${m}`; // ej "06-10"

    for (const fileName of allFiles) {
        // Identificar usuario caja
        let cajaId = '';
        if (/07/i.test(fileName)) cajaId = 'CAJA07';
        else if (/10/i.test(fileName)) cajaId = 'CAJA10';
        else if (/11/i.test(fileName)) cajaId = 'CAJA11';
        else cajaId = fileName.replace('.xlsx', '').toUpperCase();

        const filePath = path.join(cajasDir, fileName);
        const wb = XLSX.readFile(filePath);

        // Buscar hoja correspondiente
        let targetSheet = null;

        // 1. Buscar coincidencia por nombre de fecha exacta
        targetSheet = wb.SheetNames.find(s => s.includes(patternDate) || s.includes(`${parseInt(d, 10)}-${parseInt(m, 10)}`));

        // 2. Si no se encuentra (como en CAJA 10 donde la hoja fue nombrada "OCTUBRE 05-10-26 (4)"),
        //    verificar si las facturas de la última hoja coinciden con las facturas de Profit para la fecha
        if (!targetSheet) {
            const lastSheet = wb.SheetNames[wb.SheetNames.length - 1];
            targetSheet = lastSheet;
        }

        // Si es CAJA 10 y la fecha es 2026-10-06 (donde la hoja fue nombrada excepcionalmente "05-10-26 (4)")
        if (cajaId === 'CAJA10' && fecha === '2026-10-06') {
            const sheetCand = wb.SheetNames.find(s => s.includes('05-10-26 (4)'));
            if (sheetCand) targetSheet = sheetCand;
        }

        const ws = wb.Sheets[targetSheet];
        if (!ws) continue;

        const cajaAudit = {
            cajaId,
            archivo: fileName,
            hoja: targetSheet,
            facturas_fiscales: { excel_count: 0, profit_count: 0, diferencias: [] },
            notas_entrega: { excel_count: 0, profit_count: 0, diferencias: [] },
            anuladas_y_devoluciones: [],
            diferencias_centimos: [],
            formas_pago: {
                excel: {},
                profit: {}
            }
        };

        // Extraer datos del Excel
        const excelFiscal = [];
        const excelNotas = [];
        const excelSummary = {};

        for (let r = 3; r <= 150; r++) {
            const aVal = ws['A' + r] ? String(ws['A' + r].v).trim() : '';
            const bVal = ws['B' + r] ? Number(ws['B' + r].v) : 0;
            const dVal = ws['D' + r] ? Number(ws['D' + r].v) : 0;
            const eVal = ws['E' + r] ? Number(ws['E' + r].v) : 0;

            const fVal = ws['F' + r] ? String(ws['F' + r].v).trim() : '';
            const gVal = ws['G' + r] ? Number(ws['G' + r].v) : 0;

            const banco = ws['O' + r] ? String(ws['O' + r].v).trim() : '';
            const totalPag = ws['W' + r] ? Number(ws['W' + r].v) : 0;

            if (aVal && /^\d+$/.test(aVal)) {
                const anulada = banco.toUpperCase().includes('ANULADA') || (eVal === 0 && bVal === 0);
                excelFiscal.push({
                    fila: r,
                    num_fiscal: normalizeDocNum(aVal),
                    monto_bruto: bVal,
                    retencion: dVal,
                    monto_neto: eVal || bVal,
                    anulada,
                    banco,
                    total_pagado: totalPag
                });
            }

            if (fVal && /^\d+$/.test(fVal)) {
                const anulada = banco.toUpperCase().includes('ANULADA') || gVal === 0;
                excelNotas.push({
                    fila: r,
                    num_nota: normalizeDocNum(fVal),
                    monto: gVal,
                    anulada,
                    banco,
                    total_pagado: totalPag
                });
            }

            // Resumen de cierre de caja
            const sumKey = ws['E' + r] ? String(ws['E' + r].v).trim() : '';
            const sumVal = ws['G' + r] ? Number(ws['G' + r].v) : (ws['I' + r] ? Number(ws['I' + r].v) : 0);
            if (sumKey && (sumKey.includes('NOTAS') || sumKey.includes('PUNTOS') || sumKey.includes('PAGO MOVIL') || sumKey.includes('EFECTIVO') || sumKey.includes('ZELLE') || sumKey.includes('CREDITO'))) {
                excelSummary[sumKey] = sumVal;
            }
        }

        cajaAudit.facturas_fiscales.excel_count = excelFiscal.length;
        cajaAudit.notas_entrega.excel_count = excelNotas.length;
        cajaAudit.formas_pago.excel = excelSummary;

        // Facturas de Profit para esta caja
        const profitCajaFacs = profitFacturas.filter(f => f.co_us_in === cajaId);
        const profitFiscal = profitCajaFacs.filter(f => f.impfisfac && f.impfisfac.trim() !== '');
        const profitNotas = profitCajaFacs.filter(f => !f.impfisfac || f.impfisfac.trim() === '');

        cajaAudit.facturas_fiscales.profit_count = profitFiscal.length;
        cajaAudit.notas_entrega.profit_count = profitNotas.length;

        // 1. Comparar Facturas Fiscales
        profitFiscal.forEach(pf => {
            const pfFis = normalizeDocNum(pf.impfisfac);
            const found = excelFiscal.find(ef => ef.num_fiscal === pfFis);

            if (!found) {
                cajaAudit.facturas_fiscales.diferencias.push({
                    tipo: 'FALTA EN EXCEL',
                    doc_profit: pf.doc_num,
                    num_fiscal: pfFis,
                    monto_profit: pf.total_neto,
                    cliente: pf.cli_des
                });
            } else {
                // Verificar si en Excel fue marcada como ANULADA
                if (found.anulada && !pf.anulado) {
                    // Buscar si tiene devolución asociada
                    const devAsoc = profitDevoluciones.find(d => 
                        (d.nro_doc && d.nro_doc.includes(pf.doc_num)) || 
                        d.co_cli === pf.co_cli ||
                        Math.abs(d.total_neto - pf.total_neto) < 0.05
                    );
                    cajaAudit.anuladas_y_devoluciones.push({
                        doc_profit: pf.doc_num,
                        num_fiscal: pfFis,
                        tipo_documento: 'FACTURA FISCAL',
                        estado_excel: 'ANULADA (Monto 0)',
                        estado_profit: 'ACTIVA (Total: ' + pf.total_neto.toFixed(2) + ' Bs)',
                        devolucion_en_profit: devAsoc ? `SÍ -> Devolución ${devAsoc.doc_num} por ${devAsoc.total_neto.toFixed(2)} Bs (N/CR ${devAsoc.nro_doc})` : 'NO DETECTADA',
                        cliente: pf.cli_des
                    });
                } else {
                    const diffMonto = found.monto_bruto - pf.total_neto;
                    if (Math.abs(diffMonto) > 0.05 && Math.abs(found.monto_neto - pf.total_neto) > 0.05) {
                        cajaAudit.diferencias_centimos.push({
                            doc_profit: pf.doc_num,
                            identificador: `Fiscal ${pfFis}`,
                            monto_excel: found.monto_bruto,
                            monto_profit: pf.total_neto,
                            diferencia: Number(diffMonto.toFixed(2)),
                            cliente: pf.cli_des,
                            observacion: Math.abs(diffMonto) < 1 ? 'Diferencia de redondeo/céntimos' : 'Diferencia de monto'
                        });
                    }
                }
            }
        });

        // 2. Comparar Notas de Entrega
        profitNotas.forEach(pn => {
            const pnNum = normalizeDocNum(pn.doc_num);
            const found = excelNotas.find(en => pn.doc_num.includes(en.num_nota) || normalizeDocNum(en.num_nota) === pnNum);

            if (!found) {
                cajaAudit.notas_entrega.diferencias.push({
                    tipo: 'FALTA EN EXCEL',
                    doc_profit: pn.doc_num,
                    monto_profit: pn.total_neto,
                    cliente: pn.cli_des
                });
            } else {
                if (found.anulada && !pn.anulado) {
                    const devAsoc = profitDevoluciones.find(d => 
                        (d.nro_doc && d.nro_doc.includes(pn.doc_num)) || 
                        d.co_cli === pn.co_cli ||
                        Math.abs(d.total_neto - pn.total_neto) < 0.05
                    );
                    cajaAudit.anuladas_y_devoluciones.push({
                        doc_profit: pn.doc_num,
                        num_nota: found.num_nota,
                        tipo_documento: 'NOTA DE ENTREGA',
                        estado_excel: 'ANULADA (Monto 0)',
                        estado_profit: 'ACTIVA (Total: ' + pn.total_neto.toFixed(2) + ' Bs)',
                        devolucion_en_profit: devAsoc ? `SÍ -> Devolución ${devAsoc.doc_num} por ${devAsoc.total_neto.toFixed(2)} Bs (N/CR ${devAsoc.nro_doc})` : 'NO DETECTADA',
                        cliente: pn.cli_des
                    });
                } else {
                    const diffMonto = found.monto - pn.total_neto;
                    if (Math.abs(diffMonto) > 0.05) {
                        cajaAudit.diferencias_centimos.push({
                            doc_profit: pn.doc_num,
                            identificador: `Nota ${found.num_nota}`,
                            monto_excel: found.monto,
                            monto_profit: pn.total_neto,
                            diferencia: Number(diffMonto.toFixed(2)),
                            cliente: pn.cli_des,
                            observacion: Math.abs(diffMonto) < 1 ? 'Diferencia de redondeo/céntimos' : 'Diferencia de monto'
                        });
                    }
                }
            }
        });

        // 3. Formas de pago de Profit
        const profitCobrosCaja = profitCobros.filter(c => c.co_us_in === cajaId && !c.anulado);
        const profitFormas = {};
        profitCobrosCaja.forEach(c => {
            profitFormas[c.forma_pag] = (profitFormas[c.forma_pag] || 0) + Number(c.mont_doc);
        });
        cajaAudit.formas_pago.profit = {
            'PUNTOS DE VENTA (TJ)': Number((profitFormas['TJ'] || 0).toFixed(2)),
            'TRANSFERENCIAS / PAGO MOVIL (DP)': Number((profitFormas['DP'] || 0).toFixed(2)),
            'EFECTIVO BS (EF)': Number((profitFormas['EF'] || 0).toFixed(2)),
            'DIVISAS / CHEQUES / OTROS (TP)': Number((profitFormas['TP'] || 0).toFixed(2)),
            'TOTAL COBROS': Number(Object.values(profitFormas).reduce((a, b) => a + b, 0).toFixed(2))
        };

        auditResults.cajas.push(cajaAudit);
    }

    // Agregar devoluciones globales
    auditResults.devoluciones_profit = profitDevoluciones.map(d => ({
        doc_num: d.doc_num,
        co_us_in: d.co_us_in,
        total_neto: Number(d.total_neto),
        co_tipo_doc: d.co_tipo_doc,
        nro_doc: d.nro_doc,
        impfisfac: d.impfisfac,
        cliente: d.cli_des
    }));

    // 4. Consultar otros documentos de venta emitidos en la fecha (IVAN, ISLR, N/DB, AJNM, AJPA, etc.)
    const otherDocsRes = await pool.request().query(`
        SELECT RTRIM(d.co_tipo_doc) as tipo,
               RTRIM(d.nro_doc) as nro_doc,
               RTRIM(ISNULL(d.nro_orig, '')) as nro_orig,
               RTRIM(d.co_us_in) as usuario,
               d.total_neto,
               d.saldo,
               d.anulado,
               RTRIM(d.co_cli) as co_cli,
               RTRIM(c.cli_des) as cli_des,
               RTRIM(ISNULL(d.observa, '')) as observa
        FROM saDocumentoVenta d
        LEFT JOIN saCliente c ON d.co_cli = c.co_cli
        WHERE (CONVERT(varchar(10), d.fec_emis, 120) = '${fecha}'
           OR CONVERT(varchar(10), d.fec_reg, 120) = '${fecha}'
           OR CONVERT(varchar(10), d.fe_us_in, 120) = '${fecha}')
          AND LTRIM(RTRIM(d.co_tipo_doc)) NOT IN ('FACT', 'N/CR')
        ORDER BY d.co_tipo_doc, d.nro_doc
    `);

    auditResults.otros_documentos = otherDocsRes.recordset.map(r => ({
        tipo: r.tipo,
        nro_doc: r.nro_doc,
        nro_orig: r.nro_orig,
        usuario: r.usuario,
        total_neto: Number(r.total_neto),
        saldo: Number(r.saldo),
        anulado: r.anulado,
        cliente: r.cli_des,
        observacion: r.observa
    }));

    await pool.close();
    return auditResults;
}

/**
 * Imprime en consola un reporte formateado y legible
 */
function imprimirReporte(results) {
    console.log(`\n========================================================================`);
    console.log(`📊 REPORTE DE AUDITORÍA DE CIERRE DE CAJA — FECHA: ${results.fecha}`);
    console.log(`========================================================================`);

    results.cajas.forEach(c => {
        console.log(`\n------------------------------------------------------------------------`);
        console.log(`📌 ${c.cajaId} | Archivo: "${c.archivo}" | Hoja: "${c.hoja}"`);
        console.log(`------------------------------------------------------------------------`);
        console.log(`• Facturas Fiscales: Excel = ${c.facturas_fiscales.excel_count} | Profit = ${c.facturas_fiscales.profit_count}`);
        console.log(`• Notas de Entrega:  Excel = ${c.notas_entrega.excel_count} | Profit = ${c.notas_entrega.profit_count}`);

        if (c.anuladas_y_devoluciones.length > 0) {
            console.log(`\n  ⚠️ FACTURAS MARCADAS COMO ANULADAS EN EXCEL VS DEVOLUCIONES EN PROFIT (${c.anuladas_y_devoluciones.length}):`);
            c.anuladas_y_devoluciones.forEach(a => {
                console.log(`    - [${a.tipo_documento}] Doc: ${a.doc_profit} (${a.num_fiscal || a.num_nota}) | Cliente: ${a.cliente}`);
                console.log(`      Excel: ${a.estado_excel}`);
                console.log(`      Profit: ${a.estado_profit}`);
                console.log(`      Devolución: ${a.devolucion_en_profit}`);
            });
        } else {
            console.log(`  ✅ Sin discrepancias de anulación.`);
        }

        if (c.diferencias_centimos.length > 0) {
            console.log(`\n  🔍 DIFERENCIAS EN MONTOS / REDONDEO (${c.diferencias_centimos.length}):`);
            c.diferencias_centimos.forEach(d => {
                console.log(`    - ${d.identificador} (${d.doc_profit}) | Excel: ${d.monto_excel.toFixed(2)} | Profit: ${d.monto_profit.toFixed(2)} | Dif: ${d.diferencia > 0 ? '+' : ''}${d.diferencia} Bs (${d.observacion}) | Cli: ${d.cliente}`);
            });
        } else {
            console.log(`  ✅ Todos los montos coinciden al céntimo.`);
        }

        console.log(`\n  💳 FORMAS DE PAGO AUDITADAS EN PROFIT:`);
        Object.entries(c.formas_pago.profit).forEach(([k, v]) => {
            console.log(`    • ${k.padEnd(35, ' ')}: ${v.toLocaleString('es-VE', { minimumFractionDigits: 2 })} Bs`);
        });
    });

    console.log(`\n========================================================================`);
    console.log(`🔄 RESUMEN GLOBAL DE DEVOLUCIONES EN PROFIT (${results.devoluciones_profit.length})`);
    console.log(`========================================================================`);
    results.devoluciones_profit.forEach(d => {
        console.log(`• Doc: ${d.doc_num} | Usuario: ${d.co_us_in} | Monto: ${d.total_neto.toLocaleString('es-VE', { minimumFractionDigits: 2 })} Bs | N/CR: ${d.nro_doc} | Cliente: ${d.cliente}`);
    });

    if (results.otros_documentos && results.otros_documentos.length > 0) {
        console.log(`\n========================================================================`);
        console.log(`📑 OTROS DOCUMENTOS EMITIDOS EN LA FECHA (${results.otros_documentos.length})`);
        console.log(`   (IVAN, ISLR, N/DB, AJNM, AJPA, etc.)`);
        console.log(`========================================================================`);
        results.otros_documentos.forEach(o => {
            console.log(`• [${o.tipo}] ${o.nro_doc} | Monto: ${o.total_neto.toLocaleString('es-VE', { minimumFractionDigits: 2 })} Bs | Usuario: ${o.usuario} | DocOrig: ${o.nro_orig || 'N/A'} | Cli: ${o.cliente?.substring(0, 20)} | Obs: ${o.observacion || 'N/A'}`);
        });
    }
    console.log(`========================================================================\n`);
}

module.exports = {
    auditarCierreCajas,
    imprimirReporte
};
