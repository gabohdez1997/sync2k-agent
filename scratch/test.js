const sql = require('mssql');
const { getPool } = require('../db');

async function test() {
    try {
        const pool = await getPool('local');
        pool.on('info', msg => console.log('INFO EVENT:', msg));
        
        console.log('Inserting saCatArticulo...');
        const result = await pool.request().query(`
            INSERT INTO saCatArticulo (
                co_cat, cat_des, co_imun, co_reten, feccom, numcom, dis_cen, movil,
                campo1, campo2, campo3, campo4, campo5, campo6, campo7, campo8,
                co_us_in, co_sucu_in, fe_us_in, co_us_mo, co_sucu_mo, fe_us_mo,
                revisado, trasnfe, rowguid
            ) VALUES (
                '101004', 'PRUEBA2', NULL, NULL, NULL, NULL, NULL, '0',
                NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
                'COMP03', '01    ', GETDATE(), 'COMP03', '01    ', GETDATE(),
                NULL, NULL, NEWID()
            )
        `);
        console.log('Result:', result);
    } catch (error) {
        console.error('ERROR CATCHED:', error.message);
    }
    process.exit(0);
}

test();
