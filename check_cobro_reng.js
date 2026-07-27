// Script to check saCobroTPReng schema
const { getPool, sql, pgPool } = require('./db');

(async () => {
    try {
        const { rows } = await pgPool.query("SELECT id, name FROM branches WHERE active = true LIMIT 1");
        if (!rows.length) { console.log('No hay sedes activas'); process.exit(1); }
        const branchId = rows[0].id;
        console.log(`🏢 Usando sede: ${rows[0].name} (${branchId})`);
        
        const pool = await getPool(branchId);
        
        const colsRes = await pool.request()
            .query(`
                SELECT COLUMN_NAME, DATA_TYPE 
                FROM INFORMATION_SCHEMA.COLUMNS 
                WHERE TABLE_NAME = 'saCobroTPReng' 
            `);
        console.log('\n📋 Columnas de saCobroTPReng:');
        colsRes.recordset.forEach(c => {
            console.log(`   ${c.COLUMN_NAME} (${c.DATA_TYPE})`);
        });

        const sample = await pool.request()
            .query(`
                SELECT TOP 5 * FROM saCobroTPReng ORDER BY fe_us_in DESC
            `);
        
        console.log('\nMuestra:');
        console.log(JSON.stringify(sample.recordset, null, 2));

        await pgPool.end();
        process.exit(0);
    } catch (e) {
        console.error('Error:', e.message);
        process.exit(1);
    }
})();
