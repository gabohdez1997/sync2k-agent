
require('dotenv').config();
const { getPool, initServers, getServers } = require('./db');
const sql = require('mssql');

async function checkStock() {
    await initServers();
    const servers = getServers();
    const srv = servers[0]; // Usar la primera sede
    console.log(`Checking stock in ${srv.name}...`);

    const pool = await getPool(srv.id);
    const co_art = '010101'; // Cambiar por uno real si se conoce, o buscar uno
    
    const res = await pool.request()
        .input('co_art', sql.Char(30), co_art.padEnd(30, ' '))
        .query(`
            SELECT co_art, co_alma, stock_act, stock_com, stock_des 
            FROM saStockArt 
            WHERE co_art = @co_art
        `);
    
    console.log('Stock data:', res.recordset);
}

checkStock().then(() => process.exit(0)).catch(err => {
    console.error(err);
    process.exit(1);
});
