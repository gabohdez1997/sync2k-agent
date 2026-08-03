require('dotenv').config();
const sql = require('mssql');
const dbConfig = require('./db');

async function test() {
    try {
        await sql.connect(dbConfig.config);
        const result = await sql.query("SELECT RTRIM(co_art) as co_art, reng_neto FROM saFacturaVentaReng WHERE LTRIM(RTRIM(doc_num)) = '0000024575'");
        console.log(result.recordset);
    } catch(e) {
        console.error(e);
    } finally {
        sql.close();
    }
}

test();
