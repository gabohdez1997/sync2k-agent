const { getPool, initServers, closeAllPools, sql } = require('../db');

async function run() {
    try {
        const servers = await initServers();
        if (servers.length === 0) return;
        const pool = await getPool(servers[0].id);
        const co_art = '0101001012'; // Check BOTH!
        
        const resArt = await pool.request().input('co_art', sql.VarChar, co_art).query(
            `SELECT RTRIM(a.co_art) AS co_art, RTRIM(a.art_des) AS descripcion
             FROM saArticulo a
             WHERE LTRIM(RTRIM(a.co_art)) = LTRIM(RTRIM(@co_art))`
        );
        console.log("0101001012:", resArt.recordset);
        
        const resArt2 = await pool.request().input('co_art', sql.VarChar, '0101001013').query(
            `SELECT RTRIM(a.co_art) AS co_art, RTRIM(a.art_des) AS descripcion
             FROM saArticulo a
             WHERE LTRIM(RTRIM(a.co_art)) = LTRIM(RTRIM(@co_art))`
        );
        console.log("0101001013:", resArt2.recordset);
        
    } catch (e) {
        console.error(e);
    } finally {
        await closeAllPools();
    }
}
run();
