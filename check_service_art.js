const { getPool, sql, pgPool } = require('./db');

(async () => {
    try {
        const { rows } = await pgPool.query("SELECT id, name FROM branches WHERE active = true ORDER BY name");
        console.log(`🏢 Sedes activas: ${rows.map(r => r.name).join(', ')}`);

        for (const branch of rows) {
            console.log(`\n--- Actualizando en: ${branch.name} ---`);
            try {
                const pool = await getPool(branch.id);

                // Verificar estado actual
                const before = await pool.request()
                    .query(`SELECT LTRIM(RTRIM(co_art)) AS co_art, RTRIM(art_des) AS art_des, tipo 
                            FROM saArticulo WHERE LTRIM(RTRIM(co_art)) = '0901001004'`);

                if (before.recordset.length === 0) {
                    console.log(`   ⚠️ Artículo 0901001004 no existe en esta sede`);
                    continue;
                }

                console.log(`   Antes: tipo = '${before.recordset[0].tipo}'`);

                // Actualizar tipo a 'S' (Servicio)
                await pool.request()
                    .query(`UPDATE saArticulo SET tipo = 'S' WHERE LTRIM(RTRIM(co_art)) = '0901001004'`);

                // Verificar cambio
                const after = await pool.request()
                    .query(`SELECT tipo FROM saArticulo WHERE LTRIM(RTRIM(co_art)) = '0901001004'`);

                console.log(`   Después: tipo = '${after.recordset[0].tipo}' ✅`);
            } catch (e) {
                console.error(`   ❌ Error en ${branch.name}: ${e.message}`);
            }
        }

        await pgPool.end();
        process.exit(0);
    } catch (e) {
        console.error('Error:', e.message);
        process.exit(1);
    }
})();
