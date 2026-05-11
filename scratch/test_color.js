const { getPool, initServers, closeAllPools, sql } = require('../db');

async function run() {
    try {
        const servers = await initServers();
        if (servers.length === 0) return;
        const pool = await getPool(servers[0].id);
        const resCol = await pool.request().query('SELECT TOP 1 RTRIM(co_color) AS id FROM saColor');
        console.log("Color:", resCol.recordset);
    } catch (e) {
        console.error(e);
    } finally {
        await closeAllPools();
    }
}
run();
