const path = require('path');

// Asegurar que .env se cargue correctamente aunque el Programador de Tareas de Windows inicie en System32
const agentRoot = path.resolve(__dirname, '..');
process.chdir(agentRoot);
require('dotenv').config({ path: path.join(agentRoot, '.env') });

const { initServers, closeAllPools } = require('../db');
const { runBcvAutoSync, getCaracasDateTimeParts } = require('../helpers/bcvSync');

async function main() {
    const dryRun = process.argv.includes('--dry-run');
    const trigger = dryRun ? 'cli-dry-run' : 'windows-task-6am';
    let exitCode = 0;

    console.log(`[CLI BCV SYNC] Hora Caracas: ${getCaracasDateTimeParts().formatted} | Modo: ${dryRun ? 'DRY-RUN' : 'EJECUCIÓN REAL'}`);

    try {
        await initServers();
        const summary = await runBcvAutoSync({
            trigger,
            user: 'PROFIT',
            dryRun
        });

        console.log('\n=== Resumen Sincronización BCV ===');
        console.log(JSON.stringify(summary, null, 2));

        if (!summary.success) {
            exitCode = 1;
        }
    } catch (err) {
        console.error('❌ [CLI BCV SYNC] Error fatal:', err);
        exitCode = 1;
    } finally {
        await closeAllPools().catch(() => {});
        process.exit(exitCode);
    }
}

main();

