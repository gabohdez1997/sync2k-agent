const sql = require('mssql');
const pg = require('pg');
require('dotenv').config();

// ── Conexión a PostgreSQL Local (sync2k) ───────────────────────────────────
// Esta base de datos es alimentada en background por "profit-web/sync-daemon"
const pgUrl = process.env.LOCAL_PG_URL || 'postgresql://postgres:Galpe2021*@localhost:5432/sync2k';
console.log(`🔗 Iniciando Pool PG en Agente apuntando a: ${pgUrl}`);
const pgPool = new pg.Pool({ connectionString: pgUrl });

// ── Variables Legado de Master ─────────────────────────────────────────────
let masterConfig = {
    user:     process.env.MASTER_USER     || 'profit',
    password: process.env.MASTER_PASSWORD || 'profit',
    server:   process.env.MASTER_SERVER   || '127.0.0.1',
    database: process.env.MASTER_DATABASE || 'MasterProfitPro',
    options: {
        encrypt: false,
        trustServerCertificate: true,
        enableArithAbort: true
    },
    pool: { max: 5, min: 0, idleTimeoutMillis: 30000 }
};

const pools = new Map();

/**
 * Obtiene o crea un pool de conexiones para un servidor específico (empresas de Profit)
 * a través de las credenciales publicadas en la DB sincronizada de PostgreSQL
 */
async function getPool(serverId, sqlAuth = null) {
    if (!serverId) throw new Error("Se requiere serverId (Branch ID) para abrir la conexión SQL.");
    
    const poolId = serverId;

    if (!pools.has(poolId)) {
        console.log(`🔍 [Agente DB] Buscando sql_config para nodo: ${serverId}...`);
        
        // Consultar la base de datos local centralizada en busca de las credenciales MS-SQL de este nodo
        const { rows } = await pgPool.query(`SELECT name, sql_config FROM branches WHERE id = $1 AND active = true`, [serverId]);
        
        if (rows.length === 0) {
            throw new Error(`Nodo con ID "${serverId}" no existe o se encuentra inactivo.`);
        }

        const branch = rows[0];
        const config = branch.sql_config || {};

        if (!config.host || !config.database) {
            throw new Error(`Nodo "${branch.name}" carece de los parámetros obligatorios sql_config.host y/o sql_config.database.`);
        }

        const dbConfig = {
            user: config.user || 'sa',
            password: config.password,
            server: config.host,
            database: config.database,
            options: {
                // Forzamos sin encriptación estricta y cert de confianza true para SQL antiguos (2008 - 2012)
                encrypt: false, 
                trustServerCertificate: true,
                enableArithAbort: true
            },
            pool: { 
                max: 10, 
                min: 0, 
                idleTimeoutMillis: 30000 
            }
        };

        console.log(`🚀 Preparando pool SQL a -> Host: ${config.host} | DB: ${config.database}`);
        const pool = new sql.ConnectionPool(dbConfig);
        const connectedPool = await pool.connect();
        console.log(`✅ [SQL] Conexión abierta con éxito a nodo: ${branch.name}`);
        pools.set(poolId, connectedPool);
    }
    return pools.get(poolId);
}

/**
 * Obtiene o crea el pool de conexión para MasterProfitPro (usado en contadas excepciones)
 */
let masterPool = null;
async function getMasterPool() {
    if (masterPool) return masterPool;

    console.log(`Conectando a MasterProfitPro (${masterConfig.server})...`);
    const pool = new sql.ConnectionPool(masterConfig);
    masterPool = await pool.connect();
    console.log(`✅ Conexión MasterProfitPro establecida.`);
    return masterPool;
}

/**
 * Cierra todos los pools activos
 */
async function closeAllPools() {
    for (const [id, pool] of pools.entries()) {
        await pool.close();
        pools.delete(id);
    }
    if (masterPool) { await masterPool.close(); masterPool = null; }
    await pgPool.end();
}

/**
 * Métodos Descontinuados 
 * Ahora la base de datos es controlada asincronamente por profit-web/sync-daemon en Postgres.
 */
async function setServers(newServers) { console.warn('⚠️ setServers() deshabilitado: La configuración se lee dinámicamente de PG.'); }
function getServers() { return []; }
async function addOrUpdateServer(server, persist = true) { console.warn('⚠️ addOrUpdateServer() deshabilitado.'); }
async function removeServer(serverId) { console.warn('⚠️ removeServer() deshabilitado.'); }
async function setMasterConfig(config) { console.warn('⚠️ setMasterConfig() deshabilitado.'); }


module.exports = { 
  sql, 
  getPool, 
  getMasterPool, 
  getServers, 
  setServers, 
  setMasterConfig, 
  addOrUpdateServer, 
  removeServer, 
  closeAllPools 
};
