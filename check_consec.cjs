require('dotenv').config();
const { sql, getPool, getServers, initServers } = require('./db');

async function test() {
    await initServers();
    const servers = getServers();
    const pool = await getPool(servers[0].id);

    const tipoAjuste = await pool.request().query(`SELECT * FROM saTipoAjuste`);
    console.log("Tipos de Ajuste completos:", tipoAjuste.recordset);

    process.exit(0);
}

test().catch(e => { console.error(e); process.exit(1); });
