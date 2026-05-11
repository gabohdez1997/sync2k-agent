const { getPool, initServers, closeAllPools } = require('../db');

async function run() {
    try {
        const servers = await initServers();
        if (servers.length === 0) return;
        const pool = await getPool(servers[0].id);
        
        const res = await pool.request().query("SELECT TOP 0 * FROM saArticulo");
        const cols = Object.keys(res.recordset.columns);
        console.log("Articulo Margins:", cols.filter(c => c.toLowerCase().includes('margen')));
        
        const res2 = await pool.request().query("SELECT * FROM sys.tables WHERE name LIKE '%Margen%'");
        console.log("Margin tables:", res2.recordset.map(t => t.name));
        
    } catch (e) {
        console.error(e);
    } finally {
        await closeAllPools();
    }
}
run();
