const pg = require('pg');
const sql = require('mssql');

const pgUrl = 'postgresql://postgres:Galpe2021*@localhost:5432/sync2k';
const pgPool = new pg.Pool({ connectionString: pgUrl });

async function run() {
  try {
    const { rows: branches } = await pgPool.query("SELECT id, name, sql_config FROM branches WHERE active = true");
    const branch = branches[0];
    if (!branch) return;

    console.log(`Connecting to SQL Server for: ${branch.name}`);
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

    console.log('\n--- Searching for tables that might store Client Accounting info ---');
    const r1 = await pool.request().query(`
      SELECT TABLE_NAME 
      FROM INFORMATION_SCHEMA.TABLES 
      WHERE TABLE_NAME LIKE '%cli%' AND (TABLE_NAME LIKE '%cta%' OR TABLE_NAME LIKE '%con%' OR TABLE_NAME LIKE '%map%' OR TABLE_NAME LIKE '%integracion%' OR TABLE_NAME LIKE '%rel%')
    `);
    console.log('Tables matching pattern:', r1.recordset);

    console.log('\n--- Searching for any table containing both co_cli and co_cta ---');
    const r2 = await pool.request().query(`
      SELECT DISTINCT t.TABLE_NAME 
      FROM INFORMATION_SCHEMA.COLUMNS c1
      INNER JOIN INFORMATION_SCHEMA.COLUMNS c2 ON c1.TABLE_NAME = c2.TABLE_NAME
      INNER JOIN INFORMATION_SCHEMA.TABLES t ON c1.TABLE_NAME = t.TABLE_NAME
      WHERE c1.COLUMN_NAME = 'co_cli' AND c2.COLUMN_NAME LIKE '%cta%'
    `);
    console.log('Tables with co_cli and cta/cuenta:', r2.recordset);

    console.log('\n--- Searching for tables containing co_cli and tab_num ---');
    const r3 = await pool.request().query(`
      SELECT DISTINCT t.TABLE_NAME 
      FROM INFORMATION_SCHEMA.COLUMNS c1
      INNER JOIN INFORMATION_SCHEMA.COLUMNS c2 ON c1.TABLE_NAME = c2.TABLE_NAME
      INNER JOIN INFORMATION_SCHEMA.TABLES t ON c1.TABLE_NAME = t.TABLE_NAME
      WHERE c1.COLUMN_NAME = 'co_cli' AND c2.COLUMN_NAME LIKE '%tab%'
    `);
    console.log('Tables with co_cli and tab_num:', r3.recordset);

    console.log('\n--- Querying list of tables with "cont" or "con" or "cta" or "margen" ---');
    const r4 = await pool.request().query(`
      SELECT TABLE_NAME 
      FROM INFORMATION_SCHEMA.TABLES 
      WHERE TABLE_NAME LIKE 'saCl%' OR TABLE_NAME LIKE 'saRel%' OR TABLE_NAME LIKE 'saCta%'
    `);
    console.log('Related tables starting with saCl, saRel, saCta:', r4.recordset);

    await sql.close();
  } catch (err) {
    console.error('Error:', err);
  } finally {
    await pgPool.end();
  }
}

run();
