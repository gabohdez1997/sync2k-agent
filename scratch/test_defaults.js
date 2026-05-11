const { getPool, initServers, closeAllPools } = require('../db');

async function run() {
    try {
        const servers = await initServers();
        if (servers.length === 0) return;
        const pool = await getPool(servers[0].id);
        
        const [resLin, resSubl, resCat, resCol, resUbic] = await Promise.all([
            pool.request().query('SELECT TOP 1 RTRIM(co_lin) AS id FROM saLineaArticulo'),
            pool.request().query('SELECT TOP 1 RTRIM(co_subl) AS id FROM saSubLinea'),
            pool.request().query('SELECT TOP 1 RTRIM(co_cat) AS id FROM saCatArticulo'),
            pool.request().query('SELECT TOP 1 RTRIM(co_color) AS id FROM saColor'),
            pool.request().query('SELECT TOP 1 RTRIM(co_ubicacion) AS id FROM saUbicacion')
        ]);
        
        console.log("Lin:", resLin.recordset);
        console.log("Subl:", resSubl.recordset);
        console.log("Cat:", resCat.recordset);
        console.log("Col:", resCol.recordset);
        console.log("Ubic:", resUbic.recordset);
        
    } catch (e) {
        console.error(e);
    } finally {
        await closeAllPools();
    }
}
run();
