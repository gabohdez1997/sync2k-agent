const { getPool } = require('./db.js');
const sql = require('mssql');

async function test() {
    try {
        const pool = await getPool('BARQ', null);
        const q = `
                SELECT 
                    RTRIM(co_us_in) AS usuario, 
                    RTRIM(forma_pag) AS forma_pag, 
                    CASE 
                        WHEN RTRIM(forma_pag) IN ('DP', 'TP') THEN RTRIM(ISNULL(cod_cta, ''))
                        WHEN RTRIM(forma_pag) = 'EF' THEN RTRIM(ISNULL(cod_caja, ''))
                        WHEN RTRIM(forma_pag) = 'TJ' THEN RTRIM(ISNULL(co_tar, ''))
                        ELSE ''
                    END AS detalle,
                    SUM(mont_doc) AS total_bs
                FROM saCobroTPReng
                WHERE fe_us_in >= '2026-07-25 00:00:00' AND fe_us_in <= '2026-07-25 23:59:59'
                GROUP BY 
                    RTRIM(co_us_in), 
                    RTRIM(forma_pag), 
                    CASE 
                        WHEN RTRIM(forma_pag) IN ('DP', 'TP') THEN RTRIM(ISNULL(cod_cta, ''))
                        WHEN RTRIM(forma_pag) = 'EF' THEN RTRIM(ISNULL(cod_caja, ''))
                        WHEN RTRIM(forma_pag) = 'TJ' THEN RTRIM(ISNULL(co_tar, ''))
                        ELSE ''
                    END
                ORDER BY 
                    RTRIM(co_us_in), 
                    RTRIM(forma_pag), 
                    detalle
        `;
        const res = await pool.request().query(q);
        console.log("TEST WITH CASE:");
        console.log(JSON.stringify(res.recordset, null, 2));
        
        const q2 = `
                SELECT TOP 10
                    RTRIM(co_us_in) AS usuario, 
                    RTRIM(forma_pag) AS forma_pag, 
                    RTRIM(ISNULL(cod_cta, '')) as cod_cta,
                    RTRIM(ISNULL(cod_caja, '')) as cod_caja,
                    RTRIM(ISNULL(co_tar, '')) as co_tar,
                    mont_doc
                FROM saCobroTPReng
                WHERE fe_us_in >= '2026-07-25 00:00:00' AND fe_us_in <= '2026-07-25 23:59:59'
        `;
        const res2 = await pool.request().query(q2);
        console.log("RAW COLUMNS:");
        console.log(JSON.stringify(res2.recordset, null, 2));

        process.exit(0);
    } catch (e) {
        console.error(e);
    }
}
test();
