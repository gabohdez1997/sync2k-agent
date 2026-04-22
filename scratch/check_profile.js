const pg = require('pg');
const pool = new pg.Pool({ connectionString: 'postgresql://postgres:Galpe2021*@localhost:5432/sync2k' });
async function check() {
  try {
    const res = await pool.query('SELECT id, email FROM profiles WHERE id = $1', ['24438d57-4a0e-4883-8bd2-a91dedc02898']);
    console.log(JSON.stringify(res.rows));
  } catch (err) {
    console.error(err);
  } finally {
    await pool.end();
  }
}
check();
