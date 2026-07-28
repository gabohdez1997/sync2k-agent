const { getPool } = require('./db.js');

async function checkDates() {
    try {
        const pool = await getPool('BARQ', null);
        const q = `
            SELECT 
                r.cob_num,
                c.fecha AS fecha_documento,
                r.fecha_che,
                r.fe_us_in AS r_fe_us_in,
                c.fe_us_in AS c_fe_us_in
            FROM saCobroTPReng r
            LEFT JOIN saCobro c ON r.cob_num = c.cob_num
            WHERE r.cob_num = '0000024033'
        `;
        const res = await pool.request().query(q);
        console.log(JSON.stringify(res.recordset, null, 2));
        process.exit(0);
    } catch (e) {
        console.error(e);
        process.exit(1);
    }
}
checkDates();
