const sql = require('mssql');
const fs = require('fs');
const path = require('path');

require('dotenv').config();

// Configuration will be received via API and kept in memory
let servers = [];
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
 */
async function getPool(serverId) {
    const config = serverId ? servers.find(s => s.id === serverId) : servers[0];
    if (!config) throw new Error(`Servidor con ID "${serverId}" no configurado.`);

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
            pool: { max: 10, min: 0, idleTimeoutMillis: 30000 }
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
 * Obtiene o crea el pool de conexión para MasterProfitPro (usuarios y permisos globales)
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
 * Establece la configuración de los servidores dinámicamente (Agrega o Actualiza)
 */
async function setServers(newServers) {
    console.log(`🔄 Procesando ${newServers.length} sedes entrantes...`);
    for (const srv of newServers) {
        await addOrUpdateServer(srv);
    }
    console.log(`✅ Ahora hay ${servers.length} sedes configuradas en total.`);
}

/**
 * Establece la configuración del MasterProfitPro dinámicamente
 */
async function setMasterConfig(config) {
    console.log('🔄 Actualizando configuración MasterProfitPro...');
    if (masterPool) {
        await masterPool.close();
        masterPool = null;
    }
    masterConfig = {
        ...masterConfig,
        ...config,
        options: { ...masterConfig.options, ...(config.options || {}) },
        pool: { ...masterConfig.pool, ...(config.pool || {}) }
    };
    console.log(`✅ Configuración MasterProfitPro actualizada.`);
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
    if (masterPool) { await masterPool.close(); masterPool = null; }
}

/**
 * Agrega o actualiza una sede específica dinámicamente
 */
async function addOrUpdateServer(server) {
    const sName = server.name || 'Sede';
    console.log(`🔄 Actualizando sede: ${sName} (${server.id})...`);
    
    // Si ya existe el pool, cerrarlo
    if (pools.has(server.id)) {
        const pool = pools.get(server.id);
        await pool.close();
        pools.delete(server.id);
    }

    // Actualizar o añadir al array de servers
    const index = servers.findIndex(s => s.id === server.id);
    if (index !== -1) {
        // Realizar MERGE para no perder campos existentes si se envía un PATCH parcial
        servers[index] = { ...servers[index], ...server };
    } else {
        servers.push(server);
    }
    console.log(`✅ Sede ${server.id} actualizada/añadida.`);
}

/**
 * Elimina una sede específica dinámicamente
 */
async function removeServer(serverId) {
    console.log(`🔄 Eliminando sede: ${serverId}...`);
    
    // Cerrar pool si existe
    if (pools.has(serverId)) {
        const pool = pools.get(serverId);
        await pool.close();
        pools.delete(serverId);
    }

    // Eliminar del array
    servers = servers.filter(s => s.id !== serverId);
    console.log(`✅ Sede ${serverId} eliminada.`);
}

// Al arrancar no iniciamos nada automáticamente si no hay config
// (servers se inicializará por API)

module.exports = { sql, getPool, getMasterPool, getServers, setServers, setMasterConfig, addOrUpdateServer, removeServer, closeAllPools };
