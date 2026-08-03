/**
 * DIAGNÓSTICO DE STOCK COMPROMETIDO - Profit Plus 8.8
 * 
 * Este script verifica:
 * 1. Si pEliminarRenglonesPedidoVenta resta stock COM internamente
 * 2. Qué valores de tipo_doc se usan en los renglones de pedidos y facturas
 * 3. Inconsistencias de stock COM vs pedidos abiertos
 * 4. Estado específico del artículo 0235004015
 */
const sql = require('mssql');
const pg = require('pg');
require('dotenv').config();

const pgUrl = process.env.LOCAL_PG_URL || 'postgresql://postgres:Galpe2021*@localhost:5432/sync2k';

async function main() {
    console.log('='.repeat(80));
    console.log('  DIAGNÓSTICO DE STOCK COMPROMETIDO - Profit Plus 8.8');
    console.log('='.repeat(80));

    // 1. Obtener configuración de conexión SQL desde PostgreSQL
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
        console.error('❌ No se encontraron sedes activas en PostgreSQL.');
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
        options: {
            encrypt: false,
            trustServerCertificate: true,
            enableArithAbort: true,
            requestTimeout: 30000
        }
    });

    // =====================================================================
    // TEST 1: Verificar contenido del SP pEliminarRenglonesPedidoVenta
    // =====================================================================
    console.log('\n' + '='.repeat(80));
    console.log('  TEST 1: Definición de pEliminarRenglonesPedidoVenta');
    console.log('='.repeat(80));

    try {
        const spText = await pool.request().query(`
            SELECT m.definition
            FROM sys.sql_modules m
            JOIN sys.objects o ON m.object_id = o.object_id
            WHERE o.name = 'pEliminarRenglonesPedidoVenta'
        `);

        if (spText.recordset.length > 0) {
            const spDef = spText.recordset[0].definition;
            console.log('\n--- Código completo del SP ---');
            console.log(spDef);
            console.log('--- Fin del SP ---\n');

            // Buscar si llama a pStockActualizar
            const callsStock = spDef.toLowerCase().includes('pstockactualizar');
            const callsCOM = spDef.toLowerCase().includes("'com'");
            console.log(`🔍 ¿Llama a pStockActualizar? ${callsStock ? '✅ SÍ' : '❌ NO'}`);
            console.log(`🔍 ¿Referencia a tipo COM? ${callsCOM ? '✅ SÍ' : '❌ NO'}`);

            if (callsStock) {
                console.log('\n⚠️  CONFIRMADO: El SP nativo YA resta stock COM.');
                console.log('   → La llamada manual en pedidos.js L263-272 causa DOBLE RESTA.');
            } else {
                console.log('\n✅ El SP nativo NO resta stock COM.');
                console.log('   → La llamada manual en pedidos.js L263-272 es NECESARIA.');
            }
        } else {
            console.log('❌ SP pEliminarRenglonesPedidoVenta NO encontrado en la BD.');
        }
    } catch (err) {
        console.error('Error consultando SP:', err.message);
    }

    // =====================================================================
    // TEST 1b: Verificar SP pEliminarPedidoVenta (cabecera)
    // =====================================================================
    console.log('\n' + '='.repeat(80));
    console.log('  TEST 1b: Definición de pEliminarPedidoVenta (cabecera)');
    console.log('='.repeat(80));

    try {
        const spText = await pool.request().query(`
            SELECT m.definition
            FROM sys.sql_modules m
            JOIN sys.objects o ON m.object_id = o.object_id
            WHERE o.name = 'pEliminarPedidoVenta'
        `);

        if (spText.recordset.length > 0) {
            const spDef = spText.recordset[0].definition;
            console.log('\n--- Código completo del SP ---');
            console.log(spDef);
            console.log('--- Fin del SP ---\n');

            const callsStock = spDef.toLowerCase().includes('pstockactualizar');
            console.log(`🔍 ¿Llama a pStockActualizar? ${callsStock ? '✅ SÍ' : '❌ NO'}`);
        } else {
            console.log('❌ SP pEliminarPedidoVenta NO encontrado.');
        }
    } catch (err) {
        console.error('Error consultando SP:', err.message);
    }

    // =====================================================================
    // TEST 2: Verificar qué tipo_doc se usa en los renglones
    // =====================================================================
    console.log('\n' + '='.repeat(80));
    console.log('  TEST 2: Valores de tipo_doc en uso');
    console.log('='.repeat(80));

    try {
        // En renglones de pedido
        const pedTipos = await pool.request().query(`
            SELECT DISTINCT RTRIM(ISNULL(tipo_doc, '(NULL)')) AS tipo_doc, COUNT(*) AS total
            FROM saPedidoVentaReng
            GROUP BY tipo_doc
            ORDER BY total DESC
        `);
        console.log('\n📋 tipo_doc en saPedidoVentaReng:');
        pedTipos.recordset.forEach(r => {
            console.log(`   ${r.tipo_doc.padEnd(10)} → ${r.total} renglones`);
        });

        // En renglones de factura
        const facTipos = await pool.request().query(`
            SELECT DISTINCT RTRIM(ISNULL(tipo_doc, '(NULL)')) AS tipo_doc, COUNT(*) AS total
            FROM saFacturaVentaReng
            GROUP BY tipo_doc
            ORDER BY total DESC
        `);
        console.log('\n📋 tipo_doc en saFacturaVentaReng:');
        facTipos.recordset.forEach(r => {
            console.log(`   ${r.tipo_doc.padEnd(10)} → ${r.total} renglones`);
        });

        // En renglones de factura con referencia a pedido
        const facPedTipos = await pool.request().query(`
            SELECT DISTINCT RTRIM(ISNULL(tipo_doc, '(NULL)')) AS tipo_doc, 
                   RTRIM(ISNULL(num_doc, '(NULL)')) AS ejemplo_num_doc,
                   COUNT(*) AS total
            FROM saFacturaVentaReng
            WHERE tipo_doc IS NOT NULL AND LTRIM(RTRIM(tipo_doc)) != ''
            GROUP BY tipo_doc, num_doc
            ORDER BY total DESC
        `);
        console.log('\n📋 Facturas con tipo_doc referenciando documentos:');
        if (facPedTipos.recordset.length > 0) {
            facPedTipos.recordset.slice(0, 20).forEach(r => {
                console.log(`   tipo_doc=${r.tipo_doc.padEnd(6)} num_doc=${r.ejemplo_num_doc} (${r.total} rengs)`);
            });
        } else {
            console.log('   (No hay facturas con tipo_doc referenciado)');
        }

    } catch (err) {
        console.error('Error consultando tipo_doc:', err.message);
    }

    // =====================================================================
    // TEST 3: Inconsistencias de Stock COM
    // =====================================================================
    console.log('\n' + '='.repeat(80));
    console.log('  TEST 3: Inconsistencias de Stock COM vs Pedidos Abiertos');
    console.log('='.repeat(80));

    try {
        const inconsistencias = await pool.request().query(`
            ;WITH PedidosPendientes AS (
                SELECT 
                    RTRIM(r.co_art) AS co_art,
                    RTRIM(r.co_alma) AS co_alma,
                    SUM(r.pendiente) AS total_pendiente
                FROM saPedidoVentaReng r
                INNER JOIN saPedidoVenta p ON r.doc_num = p.doc_num
                WHERE p.anulado = 0
                  AND RTRIM(ISNULL(p.status, '0')) IN ('0', '1')
                  AND r.pendiente > 0
                GROUP BY r.co_art, r.co_alma
            ),
            StockCOM AS (
                SELECT 
                    RTRIM(co_art) AS co_art,
                    RTRIM(co_alma) AS co_alma,
                    stock AS stock_com
                FROM saStockAlmacen
                WHERE RTRIM(tipo) = 'COM'
            )
            SELECT 
                pp.co_art,
                pp.co_alma,
                pp.total_pendiente AS com_esperado,
                ISNULL(sc.stock_com, 0) AS com_actual,
                pp.total_pendiente - ISNULL(sc.stock_com, 0) AS diferencia,
                RTRIM(a.art_des) AS art_des
            FROM PedidosPendientes pp
            LEFT JOIN StockCOM sc ON pp.co_art = sc.co_art AND pp.co_alma = sc.co_alma
            LEFT JOIN saArticulo a ON pp.co_art = a.co_art
            WHERE pp.total_pendiente != ISNULL(sc.stock_com, 0)
            ORDER BY ABS(pp.total_pendiente - ISNULL(sc.stock_com, 0)) DESC
        `);

        if (inconsistencias.recordset.length > 0) {
            console.log(`\n⚠️  Se encontraron ${inconsistencias.recordset.length} artículos con stock COM inconsistente:\n`);
            console.log('   ARTÍCULO'.padEnd(35) + 'ALMACÉN'.padEnd(10) + 'COM_ESPERADO'.padEnd(15) + 'COM_ACTUAL'.padEnd(15) + 'DIFERENCIA'.padEnd(15) + 'DESCRIPCIÓN');
            console.log('   ' + '-'.repeat(105));
            inconsistencias.recordset.forEach(r => {
                const sign = r.diferencia > 0 ? '+' : '';
                console.log(`   ${r.co_art.padEnd(33)} ${r.co_alma.padEnd(8)}   ${String(r.com_esperado).padEnd(13)} ${String(r.com_actual).padEnd(13)} ${(sign + r.diferencia).padEnd(13)} ${(r.art_des || '').substring(0, 40)}`);
            });
        } else {
            console.log('\n✅ No se encontraron inconsistencias de stock COM.');
        }
    } catch (err) {
        console.error('Error consultando inconsistencias:', err.message);
    }

    // =====================================================================
    // TEST 4: Estado del artículo 0235004015 específicamente
    // =====================================================================
    console.log('\n' + '='.repeat(80));
    console.log('  TEST 4: Estado del artículo 0235004015');
    console.log('='.repeat(80));

    try {
        // Stock actual
        const stockAll = await pool.request().query(`
            SELECT RTRIM(co_art) AS co_art, RTRIM(co_alma) AS co_alma, RTRIM(tipo) AS tipo, stock
            FROM saStockAlmacen
            WHERE RTRIM(co_art) = '0235004015'
            ORDER BY co_alma, tipo
        `);
        console.log('\n📦 Registros en saStockAlmacen para 0235004015:');
        if (stockAll.recordset.length > 0) {
            stockAll.recordset.forEach(r => {
                const label = { ACT: 'Actual', COM: 'Comprometido', DES: 'Despachar', LLE: 'Por Llegar' }[r.tipo] || r.tipo;
                console.log(`   Almacén=${r.co_alma} | ${label.padEnd(20)} | stock=${r.stock}`);
            });
        } else {
            console.log('   ❌ No hay registros de stock para este artículo');
        }

        // Pedidos abiertos con este artículo
        const pedidos = await pool.request().query(`
            SELECT RTRIM(r.doc_num) AS doc_num, r.reng_num, r.total_art, r.pendiente,
                   RTRIM(r.co_alma) AS co_alma, RTRIM(p.status) AS status, p.anulado,
                   p.fec_emis
            FROM saPedidoVentaReng r
            INNER JOIN saPedidoVenta p ON r.doc_num = p.doc_num
            WHERE RTRIM(r.co_art) = '0235004015'
            ORDER BY p.fec_emis DESC
        `);
        console.log('\n📋 Pedidos con artículo 0235004015:');
        if (pedidos.recordset.length > 0) {
            pedidos.recordset.forEach(r => {
                const statusLabel = { '0': 'Sin Procesar', '1': 'Parcial', '2': 'Procesado' }[r.status] || r.status;
                console.log(`   Pedido=${r.doc_num} | Reng=${r.reng_num} | Qty=${r.total_art} | Pend=${r.pendiente} | Alma=${r.co_alma} | Status=${statusLabel} | Anulado=${r.anulado} | Fecha=${r.fec_emis?.toISOString?.()?.substring(0,10)}`);
            });
        } else {
            console.log('   (No hay pedidos con este artículo)');
        }

        // Facturas con este artículo
        const facturas = await pool.request().query(`
            SELECT TOP 10 RTRIM(r.doc_num) AS doc_num, r.reng_num, r.total_art,
                   RTRIM(r.co_alma) AS co_alma, RTRIM(r.tipo_doc) AS tipo_doc, 
                   RTRIM(r.num_doc) AS num_doc, f.anulado, f.fec_emis
            FROM saFacturaVentaReng r
            INNER JOIN saFacturaVenta f ON r.doc_num = f.doc_num
            WHERE RTRIM(r.co_art) = '0235004015'
            ORDER BY f.fec_emis DESC
        `);
        console.log('\n📋 Últimas facturas con artículo 0235004015:');
        if (facturas.recordset.length > 0) {
            facturas.recordset.forEach(r => {
                console.log(`   Factura=${r.doc_num} | Reng=${r.reng_num} | Qty=${r.total_art} | Alma=${r.co_alma} | tipo_doc=${r.tipo_doc || '(null)'} | num_doc=${r.num_doc || '(null)'} | Anulado=${r.anulado} | Fecha=${r.fec_emis?.toISOString?.()?.substring(0,10)}`);
            });
        } else {
            console.log('   (No hay facturas con este artículo)');
        }

        // Artículo tipo
        const artInfo = await pool.request().query(`
            SELECT RTRIM(co_art) AS co_art, RTRIM(art_des) AS art_des, RTRIM(tipo) AS tipo, 
                   RTRIM(co_lin) AS co_lin, RTRIM(co_subl) AS co_subl
            FROM saArticulo
            WHERE RTRIM(co_art) = '0235004015'
        `);
        if (artInfo.recordset.length > 0) {
            const a = artInfo.recordset[0];
            console.log(`\n📝 Info del artículo: ${a.art_des} | Tipo=${a.tipo} | Línea=${a.co_lin} | SubLínea=${a.co_subl}`);
        }

    } catch (err) {
        console.error('Error consultando artículo:', err.message);
    }

    // =====================================================================
    // TEST 5: Verificar triggers en tablas de Pedido
    // =====================================================================
    console.log('\n' + '='.repeat(80));
    console.log('  TEST 5: Triggers en tablas de Pedido y Factura');
    console.log('='.repeat(80));

    try {
        const triggers = await pool.request().query(`
            SELECT 
                t.name AS trigger_name,
                OBJECT_NAME(t.parent_id) AS table_name,
                t.is_disabled,
                te.type_desc AS event_type
            FROM sys.triggers t
            JOIN sys.trigger_events te ON t.object_id = te.object_id
            WHERE OBJECT_NAME(t.parent_id) IN (
                'saPedidoVenta', 'saPedidoVentaReng',
                'saFacturaVenta', 'saFacturaVentaReng',
                'saStockAlmacen'
            )
            ORDER BY table_name, trigger_name
        `);

        if (triggers.recordset.length > 0) {
            console.log('\n📋 Triggers encontrados:');
            triggers.recordset.forEach(r => {
                const status = r.is_disabled ? '⛔ DESHABILITADO' : '✅ ACTIVO';
                console.log(`   ${r.table_name.padEnd(25)} | ${r.trigger_name.padEnd(35)} | ${r.event_type.padEnd(10)} | ${status}`);
            });
        } else {
            console.log('\n✅ No hay triggers en las tablas de Pedido/Factura/Stock.');
        }
    } catch (err) {
        console.error('Error consultando triggers:', err.message);
    }

    // =====================================================================
    // TEST 6: Artículos con stock COM negativo o huérfano
    // =====================================================================
    console.log('\n' + '='.repeat(80));
    console.log('  TEST 6: Artículos con stock COM negativo');
    console.log('='.repeat(80));

    try {
        const negStock = await pool.request().query(`
            SELECT RTRIM(s.co_art) AS co_art, RTRIM(s.co_alma) AS co_alma, s.stock,
                   RTRIM(a.art_des) AS art_des
            FROM saStockAlmacen s
            LEFT JOIN saArticulo a ON s.co_art = a.co_art
            WHERE RTRIM(s.tipo) = 'COM' AND s.stock < 0
            ORDER BY s.stock ASC
        `);

        if (negStock.recordset.length > 0) {
            console.log(`\n⚠️  Se encontraron ${negStock.recordset.length} artículos con stock COM NEGATIVO:\n`);
            negStock.recordset.forEach(r => {
                console.log(`   ${r.co_art.padEnd(33)} Almacén=${r.co_alma} | COM=${r.stock} | ${(r.art_des || '').substring(0, 45)}`);
            });
        } else {
            console.log('\n✅ No hay artículos con stock COM negativo.');
        }
    } catch (err) {
        console.error('Error consultando stock negativo:', err.message);
    }

    // Cleanup
    await pool.close();
    await pgPool.end();
    console.log('\n' + '='.repeat(80));
    console.log('  DIAGNÓSTICO COMPLETADO');
    console.log('='.repeat(80));
}

main().catch(err => {
    console.error('❌ Error fatal:', err);
    process.exit(1);
});
