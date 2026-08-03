const { getPool } = require('./db.js');

async function test() {
    try {
        const pool = await getPool('BARQ', null);
        const q = `
            SELECT TOP 100 
                RTRIM(d.co_tipo_doc) AS co_tipo_doc, 
                RTRIM(d.nro_doc) AS nro_doc,
                RTRIM(f.co_us_in) AS co_us_in
            FROM saDocumentoVenta d
            INNER JOIN saCliente c ON d.co_cli = c.co_cli
            LEFT JOIN saFacturaVenta f ON RTRIM(d.co_tipo_doc) = 'FACT' AND LTRIM(RTRIM(d.nro_doc)) = LTRIM(RTRIM(f.doc_num))
            WHERE d.saldo > 0 AND d.anulado = 0 AND RTRIM(d.co_tipo_doc) IN ('FACT', 'NDEB', 'N/DB', 'GIRO', 'AJPA', 'N/CR')
            ORDER BY d.fec_emis DESC
        `;
        const res = await pool.request().query(q);
        console.log(res.recordset.slice(0, 5));
        process.exit(0);
    } catch (e) {
        console.error(e);
        process.exit(1);
    }
}
test();
