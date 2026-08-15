const path = require('path');
require('dotenv').config({ path: path.join(__dirname, '../.env') });
const { getPool, getServers, initServers } = require('../db');

async function run() {
    try {
        await initServers();
        const servers = getServers();
        const pool = await getPool(servers[0].id);
        
        const nrCount = await pool.request().query(`
            SELECT COUNT(*) as count FROM saNotaRecepcionCompra WHERE anulado = 0
        `);
        console.log("Notas de Recepcion activas:", nrCount.recordset[0].count);

        const fcCount = await pool.request().query(`
            SELECT COUNT(*) as count FROM saFacturaCompra WHERE anulado = 0
        `);
        console.log("Facturas de Compra activas:", fcCount.recordset[0].count);

        // Check recent receipts for an active article (e.g. 0306001001 or 0103001021)
        const recentNR = await pool.request().query(`
            SELECT TOP 5 nr.doc_num, nr.fec_emis, r.co_art, r.total_art
            FROM saNotaRecepcionCompra nr
            JOIN saNotaRecepcionCompraReng r ON nr.doc_num = r.doc_num
            WHERE nr.anulado = 0
            ORDER BY nr.fec_emis DESC
        `);
        console.log("Ultimas recepciones:", recentNR.recordset);

        process.exit(0);
    } catch (err) {
        console.error("ERROR:", err);
        process.exit(1);
    }
}

run();
