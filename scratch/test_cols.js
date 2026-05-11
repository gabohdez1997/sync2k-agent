const { getPool, initServers, closeAllPools } = require('../db');

async function run() {
    try {
        const servers = await initServers();
        if (servers.length === 0) return;
        const pool = await getPool(servers[0].id);
        
        // Get column info with nullable
        const res = await pool.request().query(`
            SELECT COLUMN_NAME, IS_NULLABLE, DATA_TYPE, COLUMN_DEFAULT
            FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_NAME = 'saArtPrecio'
            ORDER BY ORDINAL_POSITION
        `);
        console.log("saArtPrecio columns:");
        res.recordset.forEach(c => {
            console.log(`  ${c.COLUMN_NAME.padEnd(20)} ${c.IS_NULLABLE.padEnd(5)} ${c.DATA_TYPE.padEnd(15)} default: ${c.COLUMN_DEFAULT || 'none'}`);
        });
        
    } catch (e) {
        console.error(e);
    } finally {
        await closeAllPools();
    }
}
run();
