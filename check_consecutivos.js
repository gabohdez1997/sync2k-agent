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
        
        const pg = require('pg');
        const pgUrl = process.env.LOCAL_PG_URL || 'postgresql://postgres:Galpe2021*@localhost:5432/sync2k';
        const client = new pg.Client({ connectionString: pgUrl });
        await client.connect();
        const res = await client.query("SELECT profit_branch_codes FROM branches");
        console.log("profit_branch_codes in PG:", JSON.stringify(res.rows[0].profit_branch_codes, null, 2));
        await client.end();
        process.exit(0);

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
