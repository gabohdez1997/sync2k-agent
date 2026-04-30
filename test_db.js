const sql = require('mssql');
const checkLocals = require('../check_locals.js');
async function run() {
    const pool = await sql.connect(checkLocals.dbConfig || {
        user: process.env.DB_USER || 'profit',
        password: process.env.DB_PASSWORD || 'profit',
        server: '192.168.88.235',
        database: 'GALPE_AA',
        options: { trustServerCertificate: true }
    });
    const res = await pool.request().query('SELECT TOP 5 doc_num, co_art, RTRIM(co_uni) as co_uni, RTRIM(sco_uni) as sco_uni, RTRIM(sComentario) as com, RTRIM(tipo_doc) as td, num_doc, sREVISADO, sTRASNFE FROM saCotizacionClienteReng ORDER BY doc_num DESC');
    console.table(res.recordset);
    process.exit(0);
}
run();
