const path = require('path');
require('dotenv').config({ path: path.join(__dirname, '../.env') });
const { getPool, getServers, initServers } = require('../db');

async function run() {
    try {
        await initServers();
        const servers = getServers();
        const pool = await getPool(servers[0].id);
        
        const rengRes = await pool.request().query(`
            SELECT 
                r.reng_num, r.co_art, RTRIM(a.art_des) as art_des, r.co_precio, r.total_art,
                r.prec_vta, r.prec_vta_om, r.tipo_imp, r.porc_imp, r.monto_imp, r.reng_neto
            FROM saCotizacionClienteReng r
            LEFT JOIN saArticulo a ON r.co_art = a.co_art
            WHERE r.doc_num LIKE '%11016%'
            ORDER BY r.reng_num
        `);
        
        console.log("\n=== DETALLE ITEM POR ITEM DE COTIZACION 11016 ===");
        let sum_quote = 0;
        let sum_p1 = 0;
        let sum_p2 = 0;
        let sum_matched_price = 0;

        for (const r of rengRes.recordset) {
            const co_art = r.co_art.trim();
            const co_precio_quote = r.co_precio.trim();
            const pRes = await pool.request().input('co_art', co_art).query(`
                SELECT co_precio, monto
                FROM saArtPrecio
                WHERE co_art = @co_art
            `);
            
            const prices = {};
            pRes.recordset.forEach(p => {
                prices[p.co_precio.trim()] = p.monto;
            });
            
            const q_sub = r.prec_vta_om * r.total_art;
            sum_quote += q_sub;
            
            const p1 = (prices['1'] || prices['01'] || r.prec_vta_om) * r.total_art;
            sum_p1 += p1;

            const p2 = (prices['2'] || prices['02'] || r.prec_vta_om) * r.total_art;
            sum_p2 += p2;

            console.log(`\n[Reng ${r.reng_num}] ${co_art}: ${r.art_des}`);
            console.log(`  Cant: ${r.total_art} | Cotizado co_precio='${co_precio_quote}' a ${r.prec_vta_om} USD = ${q_sub.toFixed(2)} USD`);
            console.log(`  Precios en Maestro:`, prices);
            
            // Check if price in master for that co_precio matches quote
            const masterPriceForCoPrecio = prices[co_precio_quote];
            console.log(`  Precio maestro para '${co_precio_quote}': ${masterPriceForCoPrecio} USD (Total: ${(masterPriceForCoPrecio * r.total_art).toFixed(2)})`);
            if (masterPriceForCoPrecio !== r.prec_vta_om) {
                console.log(`  🚨 DIFERENCIA: Cotizado a ${r.prec_vta_om} pero maestro tiene ${masterPriceForCoPrecio} (Dif total: ${((masterPriceForCoPrecio - r.prec_vta_om) * r.total_art).toFixed(2)} USD)`);
            }
            if (prices['1'] && prices['1'] !== r.prec_vta_om) {
                console.log(`  ⚠️ Si usara Precio 1 (${prices['1']}): Total ${(prices['1'] * r.total_art).toFixed(2)} (Dif con cotización: ${((prices['1'] - r.prec_vta_om) * r.total_art).toFixed(2)} USD)`);
            }
        }
        
        console.log(`\nTotal Cotizado: ${sum_quote.toFixed(2)} USD`);
        console.log(`Total si todo fuera Precio 1: ${sum_p1.toFixed(2)} USD`);
        console.log(`Total si todo fuera Precio 2: ${sum_p2.toFixed(2)} USD`);
        
        process.exit(0);
    } catch (err) {
        console.error("ERROR:", err);
        process.exit(1);
    }
}

run();
