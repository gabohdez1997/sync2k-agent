const sql = require('mssql');
const pg = require('pg');
require('dotenv').config();

const pgUrl = process.env.LOCAL_PG_URL || 'postgresql://postgres:Galpe2021*@localhost:5432/sync2k';
const pgPool = new pg.Pool({ connectionString: pgUrl });

async function getPool(config) {
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
    const pool = new sql.ConnectionPool(dbConfig);
    await pool.connect();
    return pool;
}

async function main() {
    const { rows } = await pgPool.query("SELECT id, name, sql_config FROM branches WHERE name = 'Boca de Rio'");
    const r = rows[0];
    const pool = await getPool(r.sql_config);
    
    console.log(`=== saArtPrecio Primary Key info ===`);
    const pkQuery = await pool.request().query(`
        SELECT 
            tc.CONSTRAINT_NAME,
            kcu.COLUMN_NAME,
            kcu.ORDINAL_POSITION
        FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
        JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
            ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
        WHERE tc.TABLE_NAME = 'saArtPrecio' AND tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
        ORDER BY kcu.ORDINAL_POSITION
    `);
    console.table(pkQuery.recordset);

    console.log(`\n=== saArtPrecio columns ===`);
    const colsQuery = await pool.request().query(`
        SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, IS_NULLABLE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = 'saArtPrecio'
    `);
    console.table(colsQuery.recordset);

    await pool.close();
    await pgPool.end();
}

main();
