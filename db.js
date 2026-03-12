const sql = require('mssql');
const fs = require('fs');
const path = require('path');

// Cargar configuración de servidores
const serversPath = path.join(__dirname, 'config', 'servers.json');
let servers = [];
try {
    servers = JSON.parse(fs.readFileSync(serversPath, 'utf8'));
} catch (err) {
    console.error('❌ Error cargando config/servers.json:', err);
}

const pools = new Map();

/**
 * Obtiene o crea un pool de conexiones para un servidor específico
 */
async function getPool(serverId) {
    const config = serverId ? servers.find(s => s.id === serverId) : servers[0];
    
    if (!config) {
        throw new Error(`Servidor con ID "${serverId}" no configurado.`);
    }

    const id = config.id;

    if (!pools.has(id)) {
        const dbConfig = {
            user: config.user,
            password: config.password,
            server: config.server,
            database: config.database,
            options: {
                encrypt: config.options ? config.options.encrypt : false,
                trustServerCertificate: true,
                enableArithAbort: true
            },
            pool: {
                max: 10,
                min: 0,
                idleTimeoutMillis: 30000
            }
        };

        console.log(`Conectando a SQL Server: ${config.name} (${id})...`);
        const pool = new sql.ConnectionPool(dbConfig);
        const connectedPool = await pool.connect();
        console.log(`✅ Conexión establecida: ${config.name}`);
        pools.set(id, connectedPool);
    }

    return pools.get(id);
}

/**
 * Retorna todos los servidores configurados
 */
function getServers() {
    return servers;
}

/**
 * Cierra todos los pools activos
 */
async function closeAllPools() {
    for (const [id, pool] of pools.entries()) {
        await pool.close();
        pools.delete(id);
    }
}

// Inicializar pools de todos los servidores al arrancar
if (servers.length > 0) {
    Promise.all(servers.map(s => 
        getPool(s.id).catch(err => {
            console.warn(`⚠️ Error al conectar al servidor "${s.name}" (${s.id}):`, err.message);
        })
    ));
}

module.exports = {
    sql,
    getPool,
    getServers,
    closeAllPools
};
