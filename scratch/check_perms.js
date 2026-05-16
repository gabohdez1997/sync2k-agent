
const { Client } = require('pg');
async function check() {
    const client = new Client({ connectionString: 'postgresql://postgres:Galpe2021*@localhost:5432/sync2k' });
    await client.connect();
    const res = await client.query("SELECT email FROM profiles LIMIT 10");
    console.log(JSON.stringify(res.rows, null, 2));
    await client.end();
}
check();
