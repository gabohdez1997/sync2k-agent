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

    console.log('--- saTipoDocumento for IVAN and IVAP ---');
    const r1 = await pool.request().query(`
      SELECT co_tipo_doc, des_tipo_doc, pg_iva, pg_islr 
      FROM saTipoDocumento
      WHERE co_tipo_doc IN ('IVAN', 'IVAP')
    `);
    console.log(r1.recordset);

    console.log('\n--- all document types related to IVA ---');
    const r2 = await pool.request().query(`
      SELECT co_tipo_doc, des_tipo_doc, pg_iva, pg_islr 
      FROM saTipoDocumento
      WHERE des_tipo_doc LIKE '%IVA%' OR co_tipo_doc LIKE '%IVA%'
    `);
    console.log(r2.recordset);

    await sql.close();
  } catch (err) {
    console.error('Error:', err);
  } finally {
    await pgPool.end();
  }
}

run();
