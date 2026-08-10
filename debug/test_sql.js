const { Pool } = require('pg');
const sql = require('mssql');

async function test() {
    try {
        const pgPool = new Pool({ connectionString: 'postgresql://postgres:Galpe2021*@localhost:5432/sync2k' });
        const { rows } = await pgPool.query(`SELECT id, name, sql_config FROM branches WHERE active = true LIMIT 1`);
        if (rows.length === 0) throw new Error("No branches found");
        
        const config = rows[0].sql_config;
        console.log("Branch:", rows[0].name);

        const dbConfig = {
            user: config.user || 'sa',
            password: config.password,
            server: config.host,
            database: config.database,
            options: { encrypt: false, trustServerCertificate: true }
        };

        const pool = await sql.connect(dbConfig);
        console.log("Connected to SQL Server");

        const result = await pool.request().query(`
            SELECT TOP 1 RTRIM(a.co_art) AS co_art, RTRIM(a.campo7) AS campo7
            FROM saArticulo a
        `);
        console.log(result.recordset);
        process.exit(0);
    } catch (e) {
        console.error(e);
        process.exit(1);
    }
}
test();
