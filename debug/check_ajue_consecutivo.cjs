const { getPool, getServers } = require('./db');

async function test() {
    const servers = getServers();
    if (servers.length === 0) {
        console.log("No servers found");
        return;
    }
    const pool = await getPool(servers[0].id);
    const res = await pool.request().query(`
        SELECT TOP 10 co_consecutivo, proximo, num_digitos 
        FROM saConsecutivo 
        WHERE co_consecutivo LIKE '%AJU%' OR co_consecutivo LIKE '%INV%' OR co_consecutivo LIKE '%ART%'
    `);
    console.log("Consecutivos de Ajuste:", res.recordset);
    process.exit(0);
}

test().catch(e => { console.error(e); process.exit(1); });
