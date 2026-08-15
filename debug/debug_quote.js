const path = require('path');
require('dotenv').config({ path: path.join(__dirname, '../.env') });
const { getPool, getServers, initServers } = require('../db');

async function run() {
    try {
        await initServers();
        const servers = getServers();
        const pool = await getPool(servers[0].id);
        
        const headerRes = await pool.request().query(`
            SELECT 
                doc_num, fec_emis, co_cli, co_mone, tasa, total_bruto, monto_imp, total_neto,
                porc_desc_glob, monto_desc_glob, co_ven, anulado, status
            FROM saCotizacionCliente
            WHERE doc_num LIKE '%11016%'
        `);
        
        console.log("=== HEADER COTIZACION 11016 ===");
        console.log(JSON.stringify(headerRes.recordset, null, 2));
        
        const rengRes = await pool.request().query(`
            SELECT 
                r.reng_num, r.co_art, RTRIM(a.art_des) as art_des, r.co_precio, r.total_art,
                r.prec_vta, r.prec_vta_om, r.tipo_imp, r.porc_imp, r.monto_imp, r.reng_neto
            FROM saCotizacionClienteReng r
            LEFT JOIN saArticulo a ON r.co_art = a.co_art
            WHERE r.doc_num LIKE '%11016%'
            ORDER BY r.reng_num
        `);
        
        console.log("\n=== RENGLONES COTIZACION 11016 ===");
        let sum_om = 0;
        let sum_bs = 0;
        for (const r of rengRes.recordset) {
            const om_total = (r.prec_vta_om || 0) * r.total_art;
            sum_om += om_total;
            sum_bs += r.reng_neto;
            console.log(`Reng ${r.reng_num}: ${r.co_art.trim()} | ${r.art_des} | Cant: ${r.total_art} | Prec OM: ${r.prec_vta_om} (Tot OM: ${om_total.toFixed(2)}) | Prec BS: ${r.prec_vta} | TipoImp: ${r.tipo_imp} | PorcImp: ${r.porc_imp}% | MontoImp: ${r.monto_imp} | RengNeto BS: ${r.reng_neto}`);
        }
        console.log(`\nSuma directa OM (sin imp): ${sum_om.toFixed(2)} USD`);
        console.log(`Suma reng_neto BS: ${sum_bs.toFixed(2)} BS`);
        const tasa = headerRes.recordset[0].tasa;
        console.log(`Tasa: ${tasa}`);
        console.log(`Suma BS / Tasa: ${(sum_bs / tasa).toFixed(2)} USD`);

        // Check if there is an imported order (saPedidoVenta) from this quote or client
        const pedRes = await pool.request().query(`
            SELECT TOP 5 doc_num, fec_emis, co_cli, co_mone, tasa, total_bruto, monto_imp, total_neto, anulado, status
            FROM saPedidoVenta
            WHERE doc_num LIKE '%11016%' OR co_cli LIKE '%11771866%'
            ORDER BY fec_emis DESC
        `);
        console.log("\n=== PEDIDOS ASOCIADOS ===");
        console.log(JSON.stringify(pedRes.recordset, null, 2));

        if (pedRes.recordset.length > 0) {
            const pedDocNum = pedRes.recordset[0].doc_num;
            const pedRengRes = await pool.request().input('doc_num', pedDocNum).query(`
                SELECT 
                    r.reng_num, r.co_art, RTRIM(a.art_des) as art_des, r.co_precio, r.total_art,
                    r.prec_vta, r.prec_vta_om, r.tipo_imp, r.porc_imp, r.monto_imp, r.reng_neto
                FROM saPedidoVentaReng r
                LEFT JOIN saArticulo a ON r.co_art = a.co_art
                WHERE r.doc_num = @doc_num
                ORDER BY r.reng_num
            `);
            console.log(`\n=== RENGLONES DEL PEDIDO ${pedDocNum} ===`);
            let sum_ped_om = 0;
            for (const r of pedRengRes.recordset) {
                const om_total = (r.prec_vta_om || 0) * r.total_art;
                sum_ped_om += om_total;
                console.log(`Reng ${r.reng_num}: ${r.co_art.trim()} | ${r.art_des} | Cant: ${r.total_art} | Prec OM: ${r.prec_vta_om} (Tot OM: ${om_total.toFixed(2)}) | Prec BS: ${r.prec_vta} | TipoImp: ${r.tipo_imp} | PorcImp: ${r.porc_imp}% | MontoImp: ${r.monto_imp} | RengNeto BS: ${r.reng_neto}`);
            }
            console.log(`\nSuma Pedido OM: ${sum_ped_om.toFixed(2)} USD`);
        }
        
        process.exit(0);
    } catch (err) {
        console.error("ERROR:", err);
        process.exit(1);
    }
}

run();
