const { getPool } = require('../db');
const { initMultiSede, getServers } = require('../helpers/multiSede');

async function checkCols() {
    try {
        await initMultiSede();
        const servers = getServers();
        if(servers.length === 0) throw new Error('No servers found');
        const pool = await getPool(servers[0].id);
        const res = await pool.request().query(`
            SELECT c.name, t.name as type, c.is_nullable 
            FROM sys.columns c 
            JOIN sys.types t ON c.user_type_id = t.user_type_id 
            WHERE c.object_id = OBJECT_ID('saCatArticulo')
        `);
        console.table(res.recordset);
    } catch(e) {
        console.error(e);
    }
    process.exit(0);
}
checkCols();
