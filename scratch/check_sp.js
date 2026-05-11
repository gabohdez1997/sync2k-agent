const { getPool, initServers, closeAllPools } = require('./db');
(async () => {
    await initServers();
    const s = require('./db').getServers();
    const p = await getPool(s[0].id);
    const r = await p.request().query("SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[pEliminarArticulo]') AND type in (N'P', N'PC')");
    console.log('pEliminarArticulo exists:', r.recordset.length > 0);
    await closeAllPools();
})();
