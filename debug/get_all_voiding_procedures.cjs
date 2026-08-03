const pg = require('pg');
const sql = require('mssql');

const pgUrl = 'postgresql://postgres:Galpe2021*@localhost:5432/sync2k';
const pgPool = new pg.Pool({ connectionString: pgUrl });

async function run() {
  try {
    const { rows: branches } = await pgPool.query("SELECT id, name, sql_config FROM branches WHERE active = true");
    const branch = branches[0];
    if (!branch) return;

    const config = branch.sql_config;
    const dbConfig = {
      user: config.user || 'sa',
      password: config.password,
      server: config.host,
      database: config.database,
      options: {
        useUTC: false,
        encrypt: false,
        trustServerCertificate: true,
        enableArithAbort: true
      }
    };

    const pool = await sql.connect(dbConfig);

    console.log('--- DEFINITION of pv_ActualizarDocVentaAnular ---');
    const r1 = await pool.request().query("SELECT OBJECT_DEFINITION(OBJECT_ID('pv_ActualizarDocVentaAnular')) AS def");
    console.log(r1.recordset[0]?.def);

    await sql.close();
  } catch (err) {
    console.error('Error:', err);
  } finally {
    await pgPool.end();
  }
}

run();
