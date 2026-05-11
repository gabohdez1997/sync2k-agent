const { getPool, initServers, closeAllPools } = require('../db');

async function run() {
    try {
        const servers = await initServers();
        if (servers.length === 0) return;
        const pool = await getPool(servers[0].id);
        
        const res = await pool.request().query("SELECT * FROM saArtPrecio WHERE co_art = '0101001013'");
        console.log(res.recordset);
        
    } catch (e) {
        console.error(e);
    } finally {
        await closeAllPools();
    }
}
run();
