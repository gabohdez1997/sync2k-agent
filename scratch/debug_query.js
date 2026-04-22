
const { getPool, initServers } = require('../db');
const sql = require('mssql');

async function testQuery() {
    await initServers();
    // Use the Boca de Rio branch ID from Supabase
    const branchId = "7ce94a3f-eb9b-451c-a654-3d4090fe822a";
    const pool = await getPool(branchId);
    
    const page = 1;
    const limit = 12;
    const isGlobalNeeded = false;
    const whereSQL = "a.anulado = 0";
    const stockCondition = ""; // forcing empty for test
    const orderByClause = "ORDER BY a.art_des ASC";
    const topClause = `TOP (${page * limit})`;
    const joinPrecioClause = "";

    const fromClause = `FROM saArticulo a 
                       LEFT JOIN (
                           SELECT co_art, co_ubicacion, co_ubicacion2, co_ubicacion3,
                                  ROW_NUMBER() OVER(PARTITION BY co_art ORDER BY co_ubicacion) as rn
                           FROM saArtUbicacion
                       ) au ON a.co_art = au.co_art AND au.rn = 1`;

    const querySQL = `SELECT ${topClause} RTRIM(a.co_art) AS co_art, RTRIM(a.art_des) AS descripcion,
                 RTRIM(a.tipo) AS tipo, RTRIM(a.modelo) AS modelo, RTRIM(a.ref) AS referencia,
                 RTRIM(aun.co_uni) AS co_uni,
                 RTRIM(a.tipo_imp) AS tipo_imp,
                 CAST(CASE WHEN a.art_des LIKE '%TIPO B%' THEN 1 ELSE 0 END AS bit) AS oferta
          ${fromClause}
          LEFT JOIN (
              SELECT co_art, co_uni, 
                     ROW_NUMBER() OVER(PARTITION BY co_art ORDER BY uni_principal DESC) as rn
              FROM saArtUnidad
          ) aun ON a.co_art = aun.co_art AND aun.rn = 1
          WHERE ${whereSQL} ${stockCondition}
          ${orderByClause}`;

    console.log("Running query...");
    try {
        const res = await pool.request().query(querySQL);
        console.log("Results count:", res.recordset.length);
        if (res.recordset.length > 0) {
            console.log("First item:", res.recordset[0]);
        }
    } catch (e) {
        console.error("Query failed:", e.message);
    }
    process.exit();
}

testQuery();
