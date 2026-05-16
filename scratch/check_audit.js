
const { getPool, sql } = require('../db');
async function test() {
    try {
        const pool = await getPool('7ce94a3f-eb9b-451c-a654-3d4090fe822a');
        const res = await pool.request().query("SELECT name FROM sys.tables WHERE name LIKE '%Log%'");
        console.log("Tables like Log:");
        console.log(JSON.stringify(res.recordset, null, 2));
    } catch(e) {
        console.error(e);
    }
}
test();
