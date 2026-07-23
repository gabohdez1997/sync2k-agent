const pg = require('pg');

const pgUrl = 'postgresql://postgres:Galpe2021*@localhost:5432/sync2k';
const pgPool = new pg.Pool({ connectionString: pgUrl });

async function run() {
  try {
    console.log('--- Current Profiles ---');
    const { rows: profiles } = await pgPool.query(`
      SELECT email, profit_user, is_superadmin
      FROM profiles
    `);
    console.log(profiles);
  } catch (err) {
    console.error('Error:', err);
  } finally {
    await pgPool.end();
  }
}

run();
