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
let cachedServers = [];

/**
 * Inicializa y puebla el caché de sedes desde PostgreSQL Local.
 * Se DEBE llamar al iniciar el servidor para cargar la configuración.
 */
async function initServers() {
    console.log('📡 [Agente DB] Sincronizando configuración de sedes desde PostgreSQL Local...');
    try {
        const { rows } = await pgPool.query('SELECT id, name, sql_config FROM branches WHERE active = true');
        cachedServers = rows.map(r => ({
            id: r.id,
            name: r.name,
            server: r.sql_config?.host || r.sql_config?.server,
            database: r.sql_config?.database,
            sql_config: r.sql_config
        }));
        console.log(`✅ [Agente DB] Sincronización exitosa. ${cachedServers.length} sedes registradas.`);
        return cachedServers;
    } catch (err) {
        console.error('❌ [Agente DB] Error fatal al sincronizar sedes desde PG:', err.message);
        // Fallback a servidores manuales si fallase lo dinámico
        return [];
    }
}

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
                encrypt: false, 
                trustServerCertificate: true,
                enableArithAbort: true,
                connectionTimeout: 10000, // 10s timeout
                requestTimeout: 30000     // 30s timeout
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
/**
 * Retorna el listado de sedes configuradas (basado en el caché de PG)
 */
function getServers() { 
    return cachedServers; 
}
async function addOrUpdateServer(server, persist = true) { console.warn('⚠️ addOrUpdateServer() deshabilitado.'); }
async function removeServer(serverId) { console.warn('⚠️ removeServer() deshabilitado.'); }
async function setMasterConfig(config) { console.warn('⚠️ setMasterConfig() deshabilitado.'); }


/**
 * Busca de forma inteligente la tasa de cambio más apropiada en Profit
 */
async function getExchangeRate(pool) {
  try {
    const res = await pool.request().query(`
      SELECT TOP 1 tasa_v AS tasa 
      FROM saTasa 
      WHERE LTRIM(RTRIM(co_mone)) IN ('US$','USD','DOL','$','US')
      ORDER BY fecha DESC
    `);
    
    if (res.recordset.length > 0) return Number(res.recordset[0].tasa);

    // Fallback: buscar la tasa más reciente que NO sea la moneda base bolivares
    const fallback = await pool.request().query(`
      SELECT TOP 1 tasa_v AS tasa 
      FROM saTasa 
      WHERE LTRIM(RTRIM(co_mone)) NOT IN ('BS','VES','VEB','VEF')
      ORDER BY fecha DESC
    `);
    
    return Number(fallback.recordset[0]?.tasa || 1);
  } catch (err) {
    console.error('❌ [Agente DB] Error obteniendo tasa:', err.message);
    return 1;
  }
}

module.exports = { 
  sql, 
  getPool, 
  initServers,
  getMasterPool, 
  getServers, 
  setServers, 
  setMasterConfig, 
  addOrUpdateServer, 
  getExchangeRate,
  removeServer, 
  closeAllPools 
};
