const { getPool, initServers, closeAllPools, sql } = require('../db');

async function run() {
    try {
        const servers = await initServers();
        if (servers.length === 0) return;
        const pool = await getPool(servers[0].id);
        
        console.log("Inserting precio...");
        const pCheck = await pool.request()
            .input('co_art', sql.Char(30), '0101001013')
            .input('co_precio', sql.Char(6), '01')
            .input('margen', sql.Decimal(18,5), 70)
            .input('user', sql.Char(6), '999')
            .query(`
                INSERT INTO saArtPrecio (co_art, co_precio, co_mone, desde, hasta, inactivo, margen_min, margen_max, monto, co_us_in, fe_us_in, co_us_mo, fe_us_mo)
                VALUES (@co_art, @co_precio, 'US$', GETDATE(), '2050-12-31', 0, @margen, @margen, 0, @user, GETDATE(), @user, GETDATE());
            `);
        console.log("Success:", pCheck);
    } catch (e) {
        console.error("FAILED!");
        console.error(e);
    } finally {
        await closeAllPools();
    }
}
run();
