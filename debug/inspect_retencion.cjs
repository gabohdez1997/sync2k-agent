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
    
    const cobNum = '0000021465';
    console.log(`=== saCobro for ${cobNum} ===`);
    const resCob = await pool.request()
        .input('cobNum', cobNum)
        .query("SELECT cob_num, co_cli, monto, fe_us_in FROM saCobro WHERE cob_num = @cobNum");
    console.table(resCob.recordset);

    console.log(`\n=== saCobroRetenIvaReng for ${cobNum} ===`);
    const resRetReng = await pool.request()
        .input('cobNum', cobNum)
        .query(`
            SELECT r.reng_num, r.numero_documento, r.num_comprobante, r.monto_ret_imp 
            FROM saCobroRetenIvaReng r
            INNER JOIN saCobroDocReng cdr ON r.rowguid_reng_cob = cdr.rowguid
            WHERE cdr.cob_num = @cobNum
        `);
    console.table(resRetReng.recordset);

    console.log(`\n=== saDocumentoVenta (IVAN/ISLR) asoc. to ${cobNum} ===`);
    const resDoc = await pool.request()
        .input('cobNum', cobNum)
        .query("SELECT co_tipo_doc, nro_doc, co_cli, total_neto, num_comprobante FROM saDocumentoVenta WHERE doc_orig = 'COBRO' AND nro_orig = @cobNum");
    console.table(resDoc.recordset);

    await pool.close();
    await pgPool.end();
}

main();
