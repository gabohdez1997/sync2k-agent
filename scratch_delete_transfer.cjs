const pg = require('pg');
require('dotenv').config();

const pgUrl = process.env.LOCAL_PG_URL || 'postgresql://postgres:Galpe2021*@localhost:5432/sync2k';
const pgPool = new pg.Pool({ connectionString: pgUrl });

async function main() {
    try {
        const { rows } = await pgPool.query("SELECT id, transfer_number FROM stock_transfers WHERE transfer_number = 'TR-20260723-5166'");
        if (rows.length === 0) {
            console.log('Traslado TR-20260723-5166 no encontrado en PG');
            process.exit(0);
        }

        const tId = rows[0].id;
        console.log('Eliminando traslado incompleto:', rows[0]);

        await pgPool.query("DELETE FROM stock_transfer_items WHERE transfer_id = $1", [tId]);
        await pgPool.query("DELETE FROM stock_transfers WHERE id = $1", [tId]);

        console.log('✅ Traslado TR-20260723-5166 eliminado exitosamente de la base de datos.');
    } catch (err) {
        console.error('Error:', err.message);
    }
    process.exit(0);
}

main();
