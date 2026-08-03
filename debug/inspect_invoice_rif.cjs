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
    
    const docNum = '0000022107';
    console.log(`=== saDocumentoVenta for ${docNum} ===`);
    const resD = await pool.request()
        .input('docNum', docNum)
        .query("SELECT co_tipo_doc, nro_doc, co_cli, total_neto, saldo, anulado FROM saDocumentoVenta WHERE nro_doc = @docNum");
    console.table(resD.recordset);

    if (resD.recordset.length > 0) {
        const coCli = resD.recordset[0].co_cli;
        console.log(`\n=== saCliente for ${coCli} ===`);
        const resC = await pool.request()
            .input('coCli', coCli)
            .query("SELECT co_cli, cli_des, rif, contribu_e FROM saCliente WHERE co_cli = @coCli");
        console.table(resC.recordset);
    }

    await pool.close();
    await pgPool.end();
}

main();
