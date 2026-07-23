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
    const nro_doc = '0000021910';

    console.log('--- Document 0000021910 ---');
    const r1 = await pool.request().input('nro_doc', sql.VarChar, nro_doc).query(`
      SELECT co_tipo_doc, nro_doc, co_mone, tasa, total_neto, saldo
      FROM saDocumentoVenta
      WHERE LTRIM(RTRIM(nro_doc)) = LTRIM(RTRIM(@nro_doc))
    `);
    console.log(r1.recordset);

    console.log('--- Document 0000021910 in saFacturaVenta ---');
    const r2 = await pool.request().input('nro_doc', sql.VarChar, nro_doc).query(`
      SELECT doc_num, co_mone, tasa, total_neto, saldo
      FROM saFacturaVenta
      WHERE LTRIM(RTRIM(doc_num)) = LTRIM(RTRIM(@nro_doc))
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
