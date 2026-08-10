const { Pool } = require('pg');
const sql = require('mssql');

async function test() {
    try {
        const pgPool = new Pool({ connectionString: 'postgresql://postgres:Galpe2021*@localhost:5432/sync2k' });
        const { rows } = await pgPool.query(`SELECT id, name, sql_config FROM branches WHERE active = true LIMIT 1`);
        
        const config = rows[0].sql_config;

        const dbConfig = {
            user: config.user || 'sa',
            password: config.password,
            server: config.host,
            database: config.database,
            options: { encrypt: false, trustServerCertificate: true }
        };

        const pool = await sql.connect(dbConfig);
        
        const querySQL = `SELECT TOP (1) RTRIM(a.co_art) AS co_art, RTRIM(a.art_des) AS descripcion,
                             RTRIM(a.tipo) AS tipo, RTRIM(a.modelo) AS modelo, RTRIM(a.ref) AS referencia,
                             RTRIM(a.co_lin) AS co_lin, RTRIM(a.co_subl) AS co_subl, RTRIM(l.lin_des) AS linea, RTRIM(c.cat_des) AS categoria,
                             RTRIM(au.co_ubicacion) AS co_ubicacion,
                             RTRIM(ISNULL(NULLIF(RTRIM(aun.co_uni), ''), a.co_uni)) AS co_uni, RTRIM(ISNULL(un.des_uni, a.co_uni)) AS unidad,
                             RTRIM(a.tipo_imp) AS tipo_imp, RTRIM(a.campo7) AS campo7,
                             CAST(CASE WHEN a.art_des LIKE '%TIPO B%' OR c.cat_des LIKE '%TIPO B%' OR a.art_des LIKE '%SEGUNDA%' THEN 1 ELSE 0 END AS bit) AS oferta
                             
                      FROM saArticulo a 
                                   LEFT JOIN (
                                       SELECT co_art, co_ubicacion, co_ubicacion2, co_ubicacion3,
                                              ROW_NUMBER() OVER(PARTITION BY co_art ORDER BY co_ubicacion) as rn
                                       FROM saArtUbicacion
                                       
                                   ) au ON a.co_art = au.co_art AND au.rn = 1
                      LEFT JOIN saLineaArticulo l ON a.co_lin = l.co_lin
                      LEFT JOIN saCatArticulo c ON a.co_cat = c.co_cat
                      LEFT JOIN (
                          SELECT co_art, co_uni, 
                                 ROW_NUMBER() OVER(PARTITION BY co_art ORDER BY uni_principal DESC) as rn
                          FROM saArtUnidad
                      ) aun ON a.co_art = aun.co_art AND aun.rn = 1
                      LEFT JOIN saUnidad un ON aun.co_uni = un.co_uni
                      
                      WHERE  (
                    LTRIM(RTRIM(a.co_lin)) = '09' OR 
                    RTRIM(a.tipo) IN ('S', '2') OR 
                    EXISTS (
                        SELECT 1 FROM saStockAlmacen st 
                        WHERE st.co_art = a.co_art 
                        
                        GROUP BY st.co_art
                        HAVING (SUM(ISNULL(CASE WHEN RTRIM(tipo)='ACT' THEN stock ELSE 0 END, 0)) - 
                                SUM(ISNULL(CASE WHEN RTRIM(tipo)='COM' THEN stock ELSE 0 END, 0))) > 0
                    )
                )
                      ORDER BY a.co_art ASC`;

        const result = await pool.request().query(querySQL);
        console.log("Success! Found rows:", result.recordset.length);
        process.exit(0);
    } catch (e) {
        console.error("SQL ERROR:", e.message);
        process.exit(1);
    }
}
test();
