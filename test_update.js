const { getPool } = require('./db.js');

async function testUpdate() {
    try {
        const pool = await getPool('01'); // Ensure '01' is the correct branch ID if using a UUID in branches table... Wait, getPool expects branch ID.
        // Let's get the active branch ID first
        const config = require('./config/config.json');
        console.log("Config sedes:", Object.keys(config.sedes));
        const sedeId = Object.keys(config.sedes)[0];
        
        const p = await getPool(sedeId);
        console.log("Connected to", sedeId);

        const r = await p.request().query("UPDATE saArticulo SET campo7 = 'https://sksdxzrmbldzldpukgln.supabase.co/storage/v1/object/public/articulos/0233002775-123456789.webp' WHERE co_art='0233002775'");
        console.log("Rows affected:", r.rowsAffected);
        
        const r2 = await p.request().query("SELECT campo7 FROM saArticulo WHERE co_art='0233002775'");
        console.log("New value:", r2.recordset[0]);
        process.exit(0);
    } catch(e) {
        console.error("ERROR:", e.message);
        process.exit(1);
    }
}
testUpdate();
