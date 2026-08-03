const sql = require('mssql');

const config = {
    user: 'sa',
    password: 'Galpe2021*',
    server: '192.168.88.235',
    database: 'GALPE_AA',
    options: {
        encrypt: false,
        trustServerCertificate: true
    }
};

async function check() {
    try {
        const pool = await sql.connect(config);
        const res = await pool.request().query('SELECT * FROM saTax');
        console.log('--- TAX TABLE ---');
        console.table(res.recordset);
        await pool.close();
    } catch (err) {
        console.error(err);
    }
}

check();
