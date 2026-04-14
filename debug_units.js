const { getPool, getServers } = require('./db');
require('dotenv').config();

async function run() {
    try {
        const servers = getServers();
        if (!servers.length) {
            console.log("No servers configured.");
            return;
        }
        const srv = servers[0];
        console.log(`Checking server: ${srv.name} (${srv.id})`);
        
        const pool = await getPool(srv.id);
        const res = await pool.request()
            .input('co', '0314004001')
            .query(`
                SELECT 
                    au.co_art, 
                    au.co_uni, 
                    au.uni_principal, 
                    u.des_uni 
                FROM saArtUnidad au 
                JOIN saUnidad u ON au.co_uni = u.co_uni 
                WHERE LTRIM(RTRIM(au.co_art)) = @co
            `);
            
        console.log("RESULT_START");
        console.log(JSON.stringify(res.recordset, null, 2));
        console.log("RESULT_END");
        
        process.exit(0);
    } catch (err) {
        console.error("ERROR:", err.message);
        process.exit(1);
    }
}

run();
