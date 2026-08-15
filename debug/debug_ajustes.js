const path = require('path');
require('dotenv').config({ path: path.join(__dirname, '../.env') });
const { getPool, getServers, initServers } = require('../db');

async function run() {
    try {
        await initServers();
        const servers = getServers();
        const pool = await getPool(servers[0].id);
        
        const tipos = await pool.request().query(`SELECT * FROM saTipoAjuste`);
        console.log("saTipoAjuste:", tipos.recordset);

        const sampleAjustes = await pool.request().query(`
            SELECT TOP 5 r.ajue_num, a.fecha, a.anulado, r.co_art, r.total_art, r.co_tipo, ta.des_tipo, ta.tipo_trans
            FROM saAjusteReng r
            JOIN saAjuste a ON r.ajue_num = a.ajue_num
            LEFT JOIN saTipoAjuste ta ON r.co_tipo = ta.co_tipo
            WHERE a.anulado = 0
            ORDER BY a.fecha DESC
        `);
        console.log("Ultimos ajustes:", sampleAjustes.recordset);

        process.exit(0);
    } catch (err) {
        console.error("ERROR:", err);
        process.exit(1);
    }
}

run();
