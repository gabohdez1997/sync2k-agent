require('dotenv').config();
const STARTUP_AT = Date.now(); // Guard against spurious SIGINT from MINGW64 (Git Bash)
const express = require('express');
const cors = require('cors');

// Importar módulo de base de datos
const { getServers, initServers, closeAllPools } = require('./db');
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

function parseSQLAuth(req, res, next) {
    const authHeader = req.headers['x-sql-auth'];
    if (authHeader) {
        try {
            const decoded = Buffer.from(authHeader, 'base64').toString('utf8');
            const parts = decoded.split(':');
            const user = parts[0];
            const pass = parts.slice(1).join(':') || '';
            if (user) {
                req.sqlAuth = { user, pass };
            }
        } catch (e) {
            console.warn('Invalid X-SQL-Auth header ignored.');
        }
    }

    // Header separado para el usuario de auditoría de Profit (co_ven, co_us_in, co_us_mo)
    const profitUser = req.headers['x-profit-user'];
    console.log('[AGENT AUTH] x-profit-user header:', profitUser || 'NO LLEGÓ');
    console.log('[AGENT AUTH] x-sql-auth header:', req.headers['x-sql-auth'] ? 'PRESENTE' : 'AUSENTE');
    if (profitUser && typeof profitUser === 'string' && profitUser.trim()) {
        req.profitUser = profitUser.trim().substring(0, 10).toUpperCase();
        console.log('[AGENT AUTH] req.profitUser seteado a:', req.profitUser);
    }

    next();
}

// El endpoint de login NO requiere API Key (es público para obtener el JWT)
app.use('/api/v1/auth', (req, res, next) => require('./routes/auth')(req, res, next));

// Aplicar middleware de seguridad a todas las demás rutas bajo /api
app.use('/api', authenticateAPIKey, parseSQLAuth);

// ==========================================
// DOCUMENTACIÓN DE LA API (SWAGGER)
// ==========================================
app.use('/api-docs', swaggerUi.serve, swaggerUi.setup(swaggerSpecs));
app.get('/swagger.json', (req, res) => {
    res.setHeader('Content-Type', 'application/json');
    res.send(swaggerSpecs);
});

// ==========================================
// RUTAS DE LA API (Montaje Directo para correcto recorte de rutas)
// ==========================================
app.use('/api/v1/articulos',   require('./routes/articulos'));
app.use('/api/v1/catalogos',   require('./routes/catalogos'));
app.use('/api/v1/clientes',    require('./routes/clientes'));
app.use('/api/v1/pedidos',     require('./routes/pedidos'));
app.use('/api/v1/usuarios',    require('./routes/usuarios'));
app.use('/api/v1/config',      require('./routes/config'));
app.use('/api/v1/cotizaciones', require('./routes/cotizaciones'));
app.use('/api/v1/ubicaciones', require('./routes/ubicaciones'));
// Manejo de rutas no encontradas (404)
app.use((req, res) => {
    res.status(404).json({ success: false, message: 'Ruta no encontrada.' });
});

// ==========================================
// MANEJO DE CIERRE GRÁCIL (GRACEFUL SHUTDOWN)
// ==========================================
function gracefulShutdown(signal) {
    const uptime = Date.now() - STARTUP_AT;
    // MINGW64 (Git Bash) en Windows dispara un SIGINT espúreo al arrancar npm.
    // Lo ignoramos si llega antes de 1500ms de vida del proceso.
    if (uptime < 1500) {
        console.log(`\n⚠️  Señal ${signal} ignorada (proceso recién iniciado: ${uptime}ms) — Git Bash quirk`);
        return;
    }
    console.log('\nRecibida señal de apagado (SIGINT/SIGTERM). Cerrando servicios...');
    closeAllPools().then(() => {
        console.log('✅ Conexiones a SQL Server cerradas correctamente.');
        process.exit(0);
    }).catch(err => {
        console.error('❌ Error al cerrar la conexión:', err);
        process.exit(1);
    });
}

process.on('SIGINT',  () => gracefulShutdown('SIGINT'));
process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));

// Iniciar express (con sincronización previa de sedes)
async function boot() {
    await initServers();

    app.listen(PORT, () => {
        console.log(`\n🚀 Agente "Sync2k" ejecutándose en el puerto ${PORT}`);
        
        const configuredServers = getServers();
        if (configuredServers.length > 0) {
            console.log(`📡 Sedes configuradas (${configuredServers.length}):`);
            configuredServers.forEach(s => {
                console.log(`   - [${s.id}] ${s.name} (${s.server || 'Local'})`);
            });
        } else {
            console.log(`⚠️ No se encontraron sedes activas en la base de datos local.`);
        }

        console.log(`\n🔑 Para consultar, usar header "x-api-key: ${API_KEY}"\n`);
    });
}

boot().then(() => {
    // Refresco periódico del cache de sedes cada 5 minutos
    setInterval(async () => {
        console.log('🔄 Refrescando cache de sedes (auto-sync)...');
        await initServers();
    }, 5 * 60 * 1000);
}).catch(err => {
    console.error('❌ Error fatal al iniciar el Agente:', err);
    process.exit(1);
});
