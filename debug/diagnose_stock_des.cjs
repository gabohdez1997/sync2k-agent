/**
 * DIAGNÓSTICO DE STOCK POR DESPACHAR (DES) - Profit Plus 8.8
 * 
 * Verifica:
 * 1. Inconsistencias de stock DES vs facturas sin despachar
 * 2. Artículos con stock DES negativo
 * 3. Facturas con status "Sin Despachar" y su stock DES esperado
 * 4. Definición del SP pDespacharFacturaVenta si existe
 * 5. Flujo completo del despacho en el agente
 */
const sql = require('mssql');
const pg = require('pg');
require('dotenv').config();

const pgUrl = process.env.LOCAL_PG_URL || 'postgresql://postgres:Galpe2021*@localhost:5432/sync2k';

async function main() {
    console.log('='.repeat(80));
    console.log('  DIAGNÓSTICO DE STOCK POR DESPACHAR (DES) - Profit Plus 8.8');
    console.log('='.repeat(80));

    const pgPool = new pg.Pool({ connectionString: pgUrl });
    const localBranchName = process.env.LOCAL_BRANCH_NAME;

    let query = 'SELECT id, name, sql_config FROM branches WHERE active = true';
    const params = [];
    if (localBranchName) {
        query += ' AND (name = $1 OR id::text = $1)';
        params.push(localBranchName);
    }

    const { rows } = await pgPool.query(query, params);
    if (rows.length === 0) {
        console.error('❌ No se encontraron sedes activas.');
        await pgPool.end();
        return;
    }

    const branch = rows[0];
    const config = branch.sql_config || {};
    console.log(`\n📡 Conectando a: ${branch.name} (${config.host}/${config.database})\n`);

    const pool = await sql.connect({
        user: config.user || 'sa',
        password: config.password,
        server: config.host,
        database: config.database,
        options: { encrypt: false, trustServerCertificate: true, enableArithAbort: true, requestTimeout: 60000 }
    });

    // =====================================================================
    // TEST 1: Status de facturas (entender el flujo de despacho)
    // =====================================================================
    console.log('\n' + '='.repeat(80));
    console.log('  TEST 1: Distribución de status en saFacturaVenta');
    console.log('='.repeat(80));

    try {
        const statusDist = await pool.request().query(`
            SELECT 
                RTRIM(ISNULL(status, '(NULL)')) AS status,
                CASE RTRIM(ISNULL(status, ''))
                    WHEN '0' THEN 'Sin Despachar'
                    WHEN '1' THEN 'Parcialmente Despachada'
                    WHEN '2' THEN 'Totalmente Despachada'
                    ELSE 'Desconocido'
                END AS descripcion,
                anulado,
                COUNT(*) AS total
            FROM saFacturaVenta
            GROUP BY status, anulado
            ORDER BY status, anulado
        `);
        console.log('\n📋 Status de facturas:');
        statusDist.recordset.forEach(r => {
            console.log(`   Status=${r.status} (${r.descripcion.padEnd(30)}) | Anulado=${r.anulado} | Total=${r.total}`);
        });
    } catch (err) {
        console.error('Error:', err.message);
    }

    // =====================================================================
    // TEST 2: Facturas NO anuladas con pendiente de despacho
    // =====================================================================
    console.log('\n' + '='.repeat(80));
    console.log('  TEST 2: Facturas no anuladas pendientes de despacho');
    console.log('='.repeat(80));

    try {
        const factPendientes = await pool.request().query(`
            SELECT 
                RTRIM(f.doc_num) AS doc_num,
                RTRIM(f.co_cli) AS co_cli,
                RTRIM(cl.cli_des) AS cli_des,
                f.fec_emis,
                RTRIM(ISNULL(f.status, '0')) AS status,
                f.total_neto,
                (SELECT COUNT(*) FROM saFacturaVentaReng r WHERE r.doc_num = f.doc_num AND r.pendiente > 0) AS rengs_pendientes,
                (SELECT SUM(r.pendiente) FROM saFacturaVentaReng r WHERE r.doc_num = f.doc_num) AS total_pendiente,
                (SELECT SUM(r.total_art) FROM saFacturaVentaReng r WHERE r.doc_num = f.doc_num) AS total_qty
            FROM saFacturaVenta f
            LEFT JOIN saCliente cl ON f.co_cli = cl.co_cli
            WHERE f.anulado = 0
              AND (RTRIM(ISNULL(f.status, '0')) IN ('0', '1'))
            ORDER BY f.fec_emis DESC
        `);

        if (factPendientes.recordset.length > 0) {
            console.log(`\n📋 ${factPendientes.recordset.length} facturas pendientes de despacho:\n`);
            factPendientes.recordset.slice(0, 30).forEach(r => {
                const statusLabel = { '0': 'Sin Despachar', '1': 'Parcial' }[r.status] || r.status;
                console.log(`   Factura=${r.doc_num} | ${statusLabel.padEnd(15)} | Fecha=${r.fec_emis?.toISOString?.()?.substring(0,10)} | Rengs_Pend=${r.rengs_pendientes} | Pend=${r.total_pendiente}/${r.total_qty} | ${(r.cli_des || '').substring(0, 30)}`);
            });
            if (factPendientes.recordset.length > 30) {
                console.log(`   ... y ${factPendientes.recordset.length - 30} facturas más`);
            }
        } else {
            console.log('\n✅ No hay facturas pendientes de despacho.');
        }
    } catch (err) {
        console.error('Error:', err.message);
    }

    // =====================================================================
    // TEST 3: Inconsistencias de Stock DES vs Facturas pendientes
    // =====================================================================
    console.log('\n' + '='.repeat(80));
    console.log('  TEST 3: Inconsistencias de Stock DES vs Facturas Sin Despachar');
    console.log('='.repeat(80));

    try {
        const inconsistencias = await pool.request().query(`
            ;WITH FacturasPendientes AS (
                SELECT 
                    RTRIM(r.co_art) AS co_art,
                    RTRIM(r.co_alma) AS co_alma,
                    SUM(r.pendiente) AS total_pendiente_despacho
                FROM saFacturaVentaReng r
                INNER JOIN saFacturaVenta f ON r.doc_num = f.doc_num
                WHERE f.anulado = 0
                  AND r.pendiente > 0
                GROUP BY r.co_art, r.co_alma
            ),
            StockDES AS (
                SELECT 
                    RTRIM(co_art) AS co_art,
                    RTRIM(co_alma) AS co_alma,
                    stock AS stock_des
                FROM saStockAlmacen
                WHERE RTRIM(tipo) = 'DES'
            )
            SELECT 
                fp.co_art,
                fp.co_alma,
                fp.total_pendiente_despacho AS des_esperado,
                ISNULL(sd.stock_des, 0) AS des_actual,
                fp.total_pendiente_despacho - ISNULL(sd.stock_des, 0) AS diferencia,
                RTRIM(a.art_des) AS art_des,
                RTRIM(a.tipo) AS tipo_art
            FROM FacturasPendientes fp
            LEFT JOIN StockDES sd ON fp.co_art = sd.co_art AND fp.co_alma = sd.co_alma
            LEFT JOIN saArticulo a ON fp.co_art = a.co_art
            WHERE ABS(fp.total_pendiente_despacho - ISNULL(sd.stock_des, 0)) > 0.001
            ORDER BY ABS(fp.total_pendiente_despacho - ISNULL(sd.stock_des, 0)) DESC
        `);

        if (inconsistencias.recordset.length > 0) {
            console.log(`\n⚠️  Se encontraron ${inconsistencias.recordset.length} artículos con stock DES inconsistente:\n`);
            console.log('   ARTÍCULO'.padEnd(35) + 'ALMA'.padEnd(6) + 'DES_ESPERADO'.padEnd(14) + 'DES_ACTUAL'.padEnd(14) + 'DIFERENCIA'.padEnd(14) + 'TIPO'.padEnd(6) + 'DESCRIPCIÓN');
            console.log('   ' + '-'.repeat(110));
            inconsistencias.recordset.slice(0, 50).forEach(r => {
                const sign = r.diferencia > 0 ? '+' : '';
                console.log(`   ${r.co_art.padEnd(33)} ${r.co_alma.padEnd(4)}  ${String(Number(r.des_esperado).toFixed(2)).padEnd(12)} ${String(Number(r.des_actual).toFixed(2)).padEnd(12)} ${(sign + Number(r.diferencia).toFixed(2)).padEnd(12)} ${(r.tipo_art || '').padEnd(4)}  ${(r.art_des || '').substring(0, 40)}`);
            });
            if (inconsistencias.recordset.length > 50) {
                console.log(`   ... y ${inconsistencias.recordset.length - 50} más`);
            }
        } else {
            console.log('\n✅ No se encontraron inconsistencias de stock DES.');
        }
    } catch (err) {
        console.error('Error:', err.message);
    }

    // =====================================================================
    // TEST 4: Artículos con stock DES negativo
    // =====================================================================
    console.log('\n' + '='.repeat(80));
    console.log('  TEST 4: Artículos con stock DES negativo');
    console.log('='.repeat(80));

    try {
        const negStock = await pool.request().query(`
            SELECT RTRIM(s.co_art) AS co_art, RTRIM(s.co_alma) AS co_alma, s.stock,
                   RTRIM(a.art_des) AS art_des
            FROM saStockAlmacen s
            LEFT JOIN saArticulo a ON s.co_art = a.co_art
            WHERE RTRIM(s.tipo) = 'DES' AND s.stock < 0
            ORDER BY s.stock ASC
        `);

        if (negStock.recordset.length > 0) {
            console.log(`\n⚠️  Se encontraron ${negStock.recordset.length} artículos con stock DES NEGATIVO:\n`);
            negStock.recordset.forEach(r => {
                console.log(`   ${r.co_art.padEnd(33)} Almacén=${r.co_alma} | DES=${r.stock} | ${(r.art_des || '').substring(0, 45)}`);
            });
        } else {
            console.log('\n✅ No hay artículos con stock DES negativo.');
        }
    } catch (err) {
        console.error('Error:', err.message);
    }

    // =====================================================================
    // TEST 5: Artículos con stock DES > 0 pero SIN facturas pendientes
    // =====================================================================
    console.log('\n' + '='.repeat(80));
    console.log('  TEST 5: Stock DES huérfano (stock DES > 0 sin facturas pendientes)');
    console.log('='.repeat(80));

    try {
        const huerfano = await pool.request().query(`
            SELECT 
                RTRIM(s.co_art) AS co_art,
                RTRIM(s.co_alma) AS co_alma,
                s.stock AS stock_des,
                RTRIM(a.art_des) AS art_des,
                RTRIM(a.tipo) AS tipo_art
            FROM saStockAlmacen s
            LEFT JOIN saArticulo a ON s.co_art = a.co_art
            WHERE RTRIM(s.tipo) = 'DES' AND s.stock > 0
              AND NOT EXISTS (
                  SELECT 1 
                  FROM saFacturaVentaReng r
                  INNER JOIN saFacturaVenta f ON r.doc_num = f.doc_num
                  WHERE r.co_art = s.co_art 
                    AND r.co_alma = s.co_alma
                    AND f.anulado = 0
                    AND r.pendiente > 0
              )
            ORDER BY s.stock DESC
        `);

        if (huerfano.recordset.length > 0) {
            console.log(`\n⚠️  Se encontraron ${huerfano.recordset.length} artículos con stock DES huérfano (sin factura pendiente):\n`);
            huerfano.recordset.slice(0, 30).forEach(r => {
                console.log(`   ${r.co_art.padEnd(33)} Almacén=${r.co_alma} | DES=${r.stock_des} | Tipo=${r.tipo_art} | ${(r.art_des || '').substring(0, 40)}`);
            });
            if (huerfano.recordset.length > 30) {
                console.log(`   ... y ${huerfano.recordset.length - 30} más`);
            }
        } else {
            console.log('\n✅ No hay stock DES huérfano.');
        }
    } catch (err) {
        console.error('Error:', err.message);
    }

    // =====================================================================
    // TEST 6: SPs relacionados al despacho
    // =====================================================================
    console.log('\n' + '='.repeat(80));
    console.log('  TEST 6: Stored Procedures de despacho');
    console.log('='.repeat(80));

    try {
        const sps = await pool.request().query(`
            SELECT name FROM sys.objects
            WHERE type = 'P' AND (
                name LIKE '%Despacho%' OR name LIKE '%Despachar%' 
                OR name LIKE '%Guia%' OR name LIKE '%guia%'
                OR name LIKE '%NotaEntrega%' OR name LIKE '%NotaDeEntrega%'
            )
            ORDER BY name
        `);
        console.log('\n📋 SPs de despacho encontrados:');
        if (sps.recordset.length > 0) {
            sps.recordset.forEach(r => console.log(`   • ${r.name}`));
        } else {
            console.log('   (Ninguno encontrado)');
        }
    } catch (err) {
        console.error('Error:', err.message);
    }

    // =====================================================================
    // TEST 7: Tablas de despacho/guía
    // =====================================================================
    console.log('\n' + '='.repeat(80));
    console.log('  TEST 7: Tablas de despacho en la BD');
    console.log('='.repeat(80));

    try {
        const tables = await pool.request().query(`
            SELECT name FROM sys.tables
            WHERE name LIKE '%Despacho%' OR name LIKE '%Guia%' OR name LIKE '%NotaEntrega%'
               OR name LIKE '%GuiaDesp%' OR name LIKE '%NotaDeEntrega%'
            ORDER BY name
        `);
        console.log('\n📋 Tablas de despacho:');
        if (tables.recordset.length > 0) {
            tables.recordset.forEach(r => console.log(`   • ${r.name}`));
        } else {
            console.log('   (Ninguna encontrada)');
        }
    } catch (err) {
        console.error('Error:', err.message);
    }

    // =====================================================================
    // TEST 8: Cómo se maneja el despacho nativo (verificar saGuiaDespacho)
    // =====================================================================
    console.log('\n' + '='.repeat(80));
    console.log('  TEST 8: Registros de Guías de Despacho');
    console.log('='.repeat(80));

    try {
        // Intentar consultar la tabla de guías
        const guias = await pool.request().query(`
            SELECT TOP 10 
                RTRIM(doc_num) AS doc_num,
                fec_emis,
                RTRIM(co_cli) AS co_cli,
                RTRIM(ISNULL(status, '')) AS status,
                anulado
            FROM saGuiaDespacho
            ORDER BY fec_emis DESC
        `);
        console.log(`\n📋 Últimas ${guias.recordset.length} guías de despacho:`);
        guias.recordset.forEach(r => {
            console.log(`   Guía=${r.doc_num} | Fecha=${r.fec_emis?.toISOString?.()?.substring(0,10)} | Cliente=${r.co_cli} | Status=${r.status} | Anulado=${r.anulado}`);
        });

        // Contar totales
        const guiasTotal = await pool.request().query(`SELECT COUNT(*) AS total FROM saGuiaDespacho WHERE anulado = 0`);
        console.log(`\n   Total guías no anuladas: ${guiasTotal.recordset[0].total}`);
    } catch (err) {
        console.log(`   ℹ️ saGuiaDespacho: ${err.message}`);
    }

    // =====================================================================
    // TEST 9: Verificar el artículo 0235004015 específicamente
    // =====================================================================
    console.log('\n' + '='.repeat(80));
    console.log('  TEST 9: Stock DES del artículo 0235004015');
    console.log('='.repeat(80));

    try {
        const stockAll = await pool.request().query(`
            SELECT RTRIM(co_art) AS co_art, RTRIM(co_alma) AS co_alma, RTRIM(tipo) AS tipo, stock
            FROM saStockAlmacen
            WHERE RTRIM(co_art) = '0235004015'
            ORDER BY co_alma, tipo
        `);
        console.log('\n📦 Stock completo del artículo 0235004015:');
        stockAll.recordset.forEach(r => {
            const label = { ACT: 'Actual', COM: 'Comprometido', DES: 'Por Despachar', LLE: 'Por Llegar' }[r.tipo] || r.tipo;
            console.log(`   Almacén=${r.co_alma} | ${label.padEnd(20)} | stock=${r.stock}`);
        });

        // Facturas sin despachar con este artículo
        const facPend = await pool.request().query(`
            SELECT RTRIM(r.doc_num) AS doc_num, r.reng_num, r.total_art, r.pendiente,
                   RTRIM(r.co_alma) AS co_alma, f.fec_emis, RTRIM(f.status) AS fac_status
            FROM saFacturaVentaReng r
            INNER JOIN saFacturaVenta f ON r.doc_num = f.doc_num
            WHERE RTRIM(r.co_art) = '0235004015'
              AND f.anulado = 0
              AND r.pendiente > 0
            ORDER BY f.fec_emis DESC
        `);
        console.log(`\n📋 Facturas sin despachar para 0235004015:`);
        if (facPend.recordset.length > 0) {
            let totalPend = 0;
            facPend.recordset.forEach(r => {
                totalPend += Number(r.pendiente);
                console.log(`   Factura=${r.doc_num} | Reng=${r.reng_num} | Qty=${r.total_art} | Pend=${r.pendiente} | Alma=${r.co_alma} | Status=${r.fac_status} | Fecha=${r.fec_emis?.toISOString?.()?.substring(0,10)}`);
            });
            console.log(`\n   Total pendiente de despacho: ${totalPend}`);
        } else {
            console.log('   (No hay facturas sin despachar para este artículo)');
        }
    } catch (err) {
        console.error('Error:', err.message);
    }

    // =====================================================================
    // TEST 10: Resumen general de stock DES
    // =====================================================================
    console.log('\n' + '='.repeat(80));
    console.log('  TEST 10: Resumen general de stock DES');
    console.log('='.repeat(80));

    try {
        const resumen = await pool.request().query(`
            SELECT 
                COUNT(*) AS total_registros_des,
                SUM(CASE WHEN stock > 0 THEN 1 ELSE 0 END) AS con_stock_positivo,
                SUM(CASE WHEN stock < 0 THEN 1 ELSE 0 END) AS con_stock_negativo,
                SUM(CASE WHEN stock = 0 THEN 1 ELSE 0 END) AS con_stock_cero,
                SUM(stock) AS stock_des_total
            FROM saStockAlmacen
            WHERE RTRIM(tipo) = 'DES'
        `);
        const r = resumen.recordset[0];
        console.log(`\n📊 Resumen de registros tipo DES en saStockAlmacen:`);
        console.log(`   Total registros:    ${r.total_registros_des}`);
        console.log(`   Con stock > 0:      ${r.con_stock_positivo}`);
        console.log(`   Con stock = 0:      ${r.con_stock_cero}`);
        console.log(`   Con stock < 0:      ${r.con_stock_negativo}`);
        console.log(`   Stock DES total:    ${r.stock_des_total}`);
    } catch (err) {
        console.error('Error:', err.message);
    }

    // Cleanup
    await pool.close();
    await pgPool.end();
    console.log('\n' + '='.repeat(80));
    console.log('  DIAGNÓSTICO DES COMPLETADO');
    console.log('='.repeat(80));
}

main().catch(err => {
    console.error('❌ Error fatal:', err);
    process.exit(1);
});
