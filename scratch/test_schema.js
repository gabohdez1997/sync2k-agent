const { getPool, initServers, closeAllPools } = require('../db');

async function run() {
    try {
        const servers = await initServers();
        if (servers.length === 0) return;
        const pool = await getPool(servers[0].id);
        
        const res = await pool.request().query("SELECT TOP 0 * FROM saArtPrecio");
        console.log("Columns:", Object.keys(res.recordset.columns));
        
    } catch (e) {
        console.error(e);
    } finally {
        await closeAllPools();
    }
}
run();
