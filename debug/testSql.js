const { getPool, getServers } = require('./db.js');

async function test() {
    const servers = getServers();
    for (const srv of servers) {
        try {
            const pool = await getPool(srv.id);
            const res = await pool.request().query("SELECT name, type_desc FROM sys.objects WHERE name LIKE '%Cotiz%'");
            console.log('---', srv.name, '---');
            console.log(res.recordset);
        } catch (e) {
            console.error(e.message);
        }
    }
    process.exit(0);
}

test();
