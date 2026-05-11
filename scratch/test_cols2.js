const { getPool, initServers, closeAllPools } = require('../db');
(async () => {
    await initServers();
    const s = require('../db').getServers();
    const p = await getPool(s[0].id);
    const r = await p.request().query("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='saArtUnidad' ORDER BY ORDINAL_POSITION");
    console.log(r.recordset.map(c => c.COLUMN_NAME).join(', '));
    await closeAllPools();
})();
