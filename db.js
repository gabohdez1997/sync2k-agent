const sql = require('mssql');

const dbConfig = {
    user: process.env.DB_USER,
    password: process.env.DB_PASSWORD,
    server: process.env.DB_SERVER,
    database: process.env.DB_NAME,
    options: {
        encrypt: process.env.DB_ENCRYPT === 'true',
        trustServerCertificate: true,
        enableArithAbort: true
    },
    pool: {
        max: 10,
        min: 0,
        idleTimeoutMillis: 30000
    }
};

let poolPromise;

function getPool() {
    if (!poolPromise) {
        console.log('Conectando a SQL Server...');
        poolPromise = new sql.ConnectionPool(dbConfig)
            .connect()
            .then(pool => {
                console.log('✅ Conexión a SQL Server establecida exitosamente.');
                return pool;
            })
            .catch(err => {
                console.error('❌ Error crítico al intentar conectar con SQL Server:', err);
                poolPromise = null;
                throw err;
            });
    }
    return poolPromise;
}

function closePool() {
    if (poolPromise) {
        return poolPromise.then(pool => pool.close());
    }
    return Promise.resolve();
}

// Inicializamos la conexión al levantar el server
getPool().catch(() => {
    console.warn('⚠️  El servidor inició, pero la base de datos no está disponible aún.');
});

module.exports = {
    sql,
    getPool,
    closePool
};
