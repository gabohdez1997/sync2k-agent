const { getPool } = require('./db.js');

async function check() {
    try {
        const pool = await getPool('01');
        const r = await pool.request().query("SELECT campo7 FROM saArticulo WHERE co_art='0233002775'");
        console.log("DB RESULT:");
        console.log(r.recordset);
        process.exit(0);
    } catch(e) {
        console.error(e);
        process.exit(1);
    }
}
check();
