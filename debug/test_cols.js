const { Pool } = require('pg');
const sql = require('mssql');

async function test() {
    try {
        const pgPool = new Pool({ connectionString: 'postgresql://postgres:Galpe2021*@localhost:5432/sync2k' });
        const { rows } = await pgPool.query(`SELECT sql_config FROM branches WHERE active = true LIMIT 1`);
        const dbConfig = { ...rows[0].sql_config, options: { encrypt: false, trustServerCertificate: true }, server: rows[0].sql_config.host };

        const pool = await sql.connect(dbConfig);
        const result = await pool.request().query("SELECT TOP 1 * FROM saArticulo");
        console.log(Object.keys(result.recordset[0]).join(", "));
        process.exit(0);
    } catch (e) {
        console.error("SQL ERROR:", e.message);
        process.exit(1);
    }
}
test();
