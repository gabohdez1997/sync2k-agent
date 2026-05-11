const { getPool, initServers, closeAllPools, sql } = require('../db');

async function run() {
    try {
        const servers = await initServers();
        if (servers.length === 0) return;
        const srv = servers[0]; // local server
        const pool = await getPool(srv.id);
        
        const f = new Date();
        const r = new sql.Request(pool);
        
        r.input('sCo_Art', sql.Char(30), "0101001012");
        r.input('sdFecha_Reg', sql.SmallDateTime, f);
        r.input('sArt_Des', sql.VarChar(120), "PRUEBA");
        r.input('sTipo', sql.Char(1), "V");
        r.input('bAnulado', sql.Bit, 0);
        r.input('sdFecha_Inac', sql.SmallDateTime, f);
        r.input('sCo_Lin', sql.Char(6), "01");
        r.input('sCo_Subl', sql.Char(6), "01");
        r.input('sCo_Cat', sql.Char(6), "01");
        r.input('sCo_Color', sql.Char(6), "01"); // using '01' now
        r.input('sCo_Ubicacion', sql.Char(6), "CONT1A");
        r.input('sItem', sql.VarChar(10), null);
        r.input('sModelo', sql.VarChar(20), '');
        r.input('sRef', sql.VarChar(20), '');
        r.input('bGenerico', sql.Bit, 0);
        r.input('bManeja_Serial', sql.Bit, 0);
        r.input('bManeja_Lote', sql.Bit, 0);
        r.input('bManeja_Lote_Venc', sql.Bit, 0);
        r.input('deMargen_Min', sql.Decimal(18, 5), 0);
        r.input('deMargen_Max', sql.Decimal(18, 5), 0);
        r.input('sTipo_Imp', sql.Char(1), '1');
        r.input('sTipo_Imp2', sql.Char(1), '7');
        r.input('sTipo_Imp3', sql.Char(1), '7');
        r.input('sCo_Reten', sql.Char(6), null);
        r.input('sCod_Proc', sql.Char(6), null);
        r.input('sGarantia', sql.VarChar(30), '0');
        r.input('deVolumen', sql.Decimal(18, 5), 0);
        r.input('dePeso', sql.Decimal(18, 5), 0);
        r.input('deStock_Min', sql.Decimal(18, 5), 0);
        r.input('deStock_Max', sql.Decimal(18, 5), 0);
        r.input('deStock_Pedido', sql.Decimal(18, 5), 0);
        r.input('iRelac_Unidad', sql.Int, 1);
        r.input('dePunt_Ven', sql.Decimal(18, 5), 0);
        r.input('dePunt_Cli', sql.Decimal(18, 5), 0);
        r.input('deLic_Mon_Ilc', sql.Decimal(18, 5), 0);
        r.input('deLic_Capacidad', sql.Decimal(18, 5), 0);
        r.input('deLic_Grado_Al', sql.Decimal(18, 5), 0);
        r.input('sLic_Tipo', sql.Char(1), null);
        r.input('bPrec_Om', sql.Bit, 0);
        r.input('sComentario', sql.VarChar(sql.MAX), null);
        r.input('sTipo_Cos', sql.Char(4), '1');
        r.input('dePorc_Margen_Minimo', sql.Decimal(18, 5), 0);
        r.input('dePorc_Margen_Maximo', sql.Decimal(18, 5), 0);
        r.input('deMont_Comi', sql.Decimal(18, 5), 0);
        r.input('dePorc_Arancel', sql.Decimal(18, 5), 0);
        r.input('sI_Art_Des', sql.VarChar(120), null);
        r.input('sDis_Cen', sql.VarChar(sql.MAX), null);
        r.input('sReten_Iva_Tercero', sql.Char(16), null);
        r.input('sCampo1', sql.VarChar(60), '');
        r.input('sCampo2', sql.VarChar(60), '');
        r.input('sCampo3', sql.VarChar(60), '');
        r.input('sCampo4', sql.VarChar(60), '');
        r.input('sCampo5', sql.VarChar(60), '');
        r.input('sCampo6', sql.VarChar(60), '');
        r.input('sCampo7', sql.VarChar(60), '');
        r.input('sCampo8', sql.VarChar(60), '');
        r.input('sCo_Us_In', sql.Char(6), '999');
        r.input('sCo_Sucu_In', sql.Char(6), null);
        r.input('sMaquina', sql.VarChar(60), 'SYNC2K');
        r.input('sRevisado', sql.Char(1), '0');
        r.input('sTrasnfe', sql.Char(1), '0');

        console.log("Executing pInsertarArticulo...");
        await r.execute('pInsertarArticulo');
        console.log("Success!");
    } catch (e) {
        console.error("FAILED!");
        if (e.precedingErrors) {
            console.error("PRECEDING ERRORS:");
            for (let pe of e.precedingErrors) {
                console.error("-", pe.message);
            }
        } else {
            console.error(e.message);
        }
    } finally {
        await closeAllPools();
    }
}
run();
