const { getPool, initServers, closeAllPools, sql } = require('../db');

(async () => {
    await initServers();
    const s = require('../db').getServers();
    const p = await getPool(s[0].id);

    const r1 = await p.request().query("SELECT TOP 5 RTRIM(cod_proc) as cod_proc, RTRIM(des_proc) as des_proc FROM saProcedencia");
    console.log("Procedencias:", JSON.stringify(r1.recordset));

    const r2 = await p.request().query("SELECT TOP 5 RTRIM(co_uni) as co_uni, RTRIM(des_uni) as des_uni FROM saUnidad");
    console.log("Unidades:", JSON.stringify(r2.recordset));

    const r3 = await p.request().input('c', sql.VarChar, '0101001012').query(
        "SELECT RTRIM(a.cod_proc) as cod_proc FROM saArticulo a WHERE LTRIM(RTRIM(a.co_art)) = LTRIM(RTRIM(@c))"
    );
    console.log("Articulo cod_proc:", JSON.stringify(r3.recordset));

    await closeAllPools();
})();
