const { getPool, getServers, initServers } = require('./db');
const sql = require('mssql');
require('dotenv').config();

async function check() {
    try {
        await initServers();
        const servers = getServers();
        if (!servers.length) {
            console.log("No servers found");
            return;
        }
        
        console.log("Checking server:", servers[0].name);
        const pool = await getPool(servers[0].id);
        
        const res = await pool.request().query("SELECT RTRIM(co_consecutivo) as co_consecutivo, RTRIM(co_serie) as co_serie FROM saConsecutivo WHERE co_consecutivo LIKE '%CLI%' OR co_consecutivo LIKE '%COT%'");
        console.log("Consecutivos encontrados en saConsecutivo:");
        console.table(res.recordset);

        if (res.recordset.length > 0) {
            const series = res.recordset.filter(r => r.co_serie).map(r => `'${r.co_serie.trim()}'`).join(",");
            if (series) {
                const resSerie = await pool.request().query(`SELECT RTRIM(co_serie) as co_serie, prox_n, RTRIM(desde_a) as desde_a FROM saSerie WHERE co_serie IN (${series})`);
                console.log("\nDetalle de Series en saSerie:");
                console.table(resSerie.recordset);
            }
        } else {
            console.log("\nNo se encontraron consecutivos que coincidan con CLI o COT. Listando todos los tipos disponibles:");
            const resTypes = await pool.request().query("SELECT RTRIM(co_consecutivo) as code, des_consecutivo FROM saConsecutivoTipo");
            console.table(resTypes.recordset);
        }

    } catch (err) {
        console.error("Error:", err);
    } finally {
        process.exit();
    }
}

check();
