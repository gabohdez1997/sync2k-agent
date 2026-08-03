const sql = require('mssql');

const masterConfig = {
    user: 'profit',
    password: '123',
    server: '127.0.0.1',
    database: 'MasterProfitPro',
    options: { encrypt: false, trustServerCertificate: true, enableArithAbort: true }
};

async function check() {
    try {
        const dbs = await sql.connect({ ...masterConfig, database: 'master' });
        const comp = await dbs.request().query("SELECT name FROM sys.databases WHERE name LIKE '%profit%' OR name LIKE '%sync%' OR name LIKE '%2k%'");
        console.log("DBs:", comp.recordset.map(r => r.name));
        process.exit(0);
    } catch (e) {
        console.error(e);
        process.exit(1);
    }
}
check();
