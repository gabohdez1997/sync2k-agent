require('dotenv').config();
const express = require('express');
const cors = require('cors');

// Importar módulo de base de datos
const { getServers, closeAllPools } = require('./db');
const swaggerUi = require('swagger-ui-express');
const swaggerSpecs = require('./swaggerOptions');

// Las rutas se cargarán bajo demanda (lazy loading) en el bloque de endpoints


const app = express();

// Middlewares globales
app.use(cors());
app.use(express.json());

// Configuraciones obtenidas del entorno
const PORT = process.env.PORT || 3000;
const API_KEY = process.env.API_KEY || 'mi-clave-secreta';

// ==========================================
// MIDDLEWARE DE AUTENTICACIÓN
// ==========================================
function authenticateAPIKey(req, res, next) {
    const apiKey = req.headers['x-api-key'];

    if (!apiKey || apiKey !== API_KEY) {
        return res.status(401).json({
            success: false,
            message: 'Acceso Denegado. API Key faltante o inválida.'
        });
    }

    next();
}

// El endpoint de login NO requiere API Key (es público para obtener el JWT)
app.use('/api/v1/auth', (req, res, next) => require('./routes/auth')(req, res, next));

// Aplicar middleware de seguridad a todas las demás rutas bajo /api
app.use('/api', authenticateAPIKey);

// ==========================================
// DOCUMENTACIÓN DE LA API (SWAGGER)
// ==========================================
app.use('/api-docs', swaggerUi.serve, swaggerUi.setup(swaggerSpecs));
app.get('/swagger.json', (req, res) => {
    res.setHeader('Content-Type', 'application/json');
    res.send(swaggerSpecs);
});

// ==========================================
// RUTAS DE LA API (Lazy Loading)
// ==========================================
// Solo se hará el 'require' y se cargará en memoria la primera vez que se consulte el endpoint
app.use('/api/v1/articulos', (req, res, next) => require('./routes/articulos')(req, res, next));
app.use('/api/v1/catalogos', (req, res, next) => require('./routes/catalogos')(req, res, next));
app.use('/api/v1/clientes',  (req, res, next) => require('./routes/clientes') (req, res, next));
app.use('/api/v1/pedidos',   (req, res, next) => require('./routes/pedidos')  (req, res, next));
app.use('/api/v1/usuarios',  (req, res, next) => require('./routes/usuarios') (req, res, next));

// Manejo de rutas no encontradas (404)
app.use((req, res) => {
    res.status(404).json({ success: false, message: 'Ruta no encontrada.' });
});

// ==========================================
// MANEJO DE CIERRE GRÁCIL (GRACEFUL SHUTDOWN)
// ==========================================
function gracefulShutdown() {
    console.log('\nRecibida señal de apagado (SIGINT/SIGTERM). Cerrando servicios...');
    closeAllPools().then(() => {
        console.log('✅ Conexiones a SQL Server cerradas correctamente.');
        process.exit(0);
    }).catch(err => {
        console.error('❌ Error al cerrar la conexión:', err);
        process.exit(1);
    });
}

process.on('SIGINT', gracefulShutdown);
process.on('SIGTERM', gracefulShutdown);

// Iniciar express
app.listen(PORT, () => {
    console.log(`\n🚀 Agente "Sync2k" ejecutándose en el puerto ${PORT}`);
    
    const configuredServers = getServers();
    if (configuredServers.length > 0) {
        console.log(`📡 Sedes configuradas (${configuredServers.length}):`);
        configuredServers.forEach(s => {
            console.log(`   - [${s.id}] ${s.name} (${s.server})`);
        });
    } else {
        console.log(`⚠️ No hay sedes configuradas en config/servers.json`);
    }

    console.log(`\n🔑 Para consultar, usar header "x-api-key: ${API_KEY}"\n`);
});
