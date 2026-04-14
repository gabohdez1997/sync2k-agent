import { getPool } from '../db.js';

async function checkSP() {
    try {
        const servers = [
             { id: 'G3', name: 'G3' } // Adjust based on known servers
        ];
        
        const pool = await getPool('G3'); // Assuming a default server
        const res = await pool.request()
            .input('spName', 'pInsertarCotizacionCliente')
            .query(`
                SELECT OBJECT_DEFINITION(OBJECT_ID(@spName)) as definition
            `);
        
        console.log("SP Definition:", res.recordset[0]?.definition);
        
    } catch (e) {
        console.error("Error checking SP:", e);
    }
    process.exit(0);
}

checkSP();
