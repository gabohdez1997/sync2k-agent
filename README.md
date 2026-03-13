# Sync2k Agent

**Sync2k Agent** es una API RESTful desarrollada en Node.js con Express que funciona como middleware entre aplicaciones externas y una base de datos de Microsoft SQL Server (típicamente Profit Plus 2k12 o similar). Permite consultar y gestionar de forma segura e independiente entidades como Artículos, Catálogos, Clientes y Pedidos.

## Características Principales
- 🚀 **Framework**: Construido sobre Node.js y Express.js.
- 🗄️ **Base de Datos**: Integración con Microsoft SQL Server a través del paquete `mssql`.
- 🔐 **Seguridad**: Autenticación centralizada y obligatoria en todos los endpoints mediante el header `x-api-key`.
- ⚡ **Optimización**: Utiliza *Lazy loading* para cargar las rutas únicamente cuando son requeridas por primera vez y gestiona un Pool de conexiones a la base de datos para máxima eficiencia y cierre grácil (*graceful shutdown*).
- 🚦 **Módulos y Rutas**:
  - `/api/v1/articulos`: Consulta y gestión del inventario de productos.
  - `/api/v1/catalogos`: Consulta de entidades maestras (vendedores, condiciones de pago, zonas, etc.).
  - `/api/v1/clientes`: Manejo y consulta de la cartera de clientes.
  - `/api/v1/pedidos`: Creación, actualización, anulación y consulta de notas de pedidos (Sales Orders).

## Requisitos Previos
- [Node.js](https://nodejs.org/) (versión 14.x o superior).
- Acceso de red a una instancia de SQL Server de Profit Plus.

## Instalación y Configuración

1. **Clona el repositorio** o descarga los archivos.
   ```bash
   git clone <URL_DEL_REPOSITORIO>
   cd sync2k-agent
   ```

2. **Instala las dependencias necesarias** ejecutando:
   ```bash
   npm install
   ```
   *(Esto instalará `cors`, `dotenv`, `express` y `mssql` desde el `package.json`)*.

3. **Configura tus variables de entorno**:
   Copia el archivo `.env.example` y renómbralo a `.env`:
   ```bash
   cp .env.example .env
   ```
   Abre `.env` en tu editor y ajusta el puerto y tu clave de API:
   ```env
   PORT=3000
   API_KEY=tu_clave_secreta_aqui
   ```

4. **Configura las sedes (Bases de Datos)**:
   A diferencia de versiones anteriores, las conexiones se gestionan en `config/servers.json`. Este archivo permite configurar múltiples instancias de SQL Server:
   ```json
   [
     {
       "id": "SEDE1",
       "name": "Nombre de la Sede",
       "server": "192.168.1.10",
       "database": "DB_PROFIT",
       "user": "sa",
       "password": "password",
       "options": { "encrypt": false, "trustServerCertificate": true }
     }
   ]
   ```

## Ejecución

Para levantar el servidor en el entorno actual, ejecuta:

```bash
node server.js
```

Si la conexión es exitosa, verás el mensaje de que el Agente "Sync2k" se está ejecutando en el puerto definido.

## Uso de la API

Dado que todas las rutas bajo `/api` requieren autenticación, cada petición que realices deberá contener el header de seguridad de la siguiente forma:

- **Header:** `x-api-key`
- **Valor:** El mismo que colocaste en la variable `API_KEY` de tu archivo `.env`.

**Ejemplo de Petición con cURL (Obtener Artículos):**
```bash
curl -X GET http://localhost:3000/api/v1/articulos \
     -H "x-api-key: tu_clave_secreta_super_segura_aqui" \
     -H "Content-Type: application/json"
```

## Estructura del Proyecto

```text
sync2k-agent/
│
├── .env                  # (No incluido en git) Configuración local (Puerto y API Key).
├── .env.example          # Plantilla para variables de entorno.
├── db.js                 # Gestión de pools de conexión multi-sede.
├── package.json          # Información general del proyecto y sus dependencias.
├── server.js             # Punto de entrada principal.
├── helpers/              # Utilidades para operaciones multi-sede.
├── config/               # Carpeta de configuraciones.
│   └── servers.json      # Configuración detallada de cada sede/instancia SQL.
└── routes/               # Endpoints por módulo
    ├── articulos.js
    ├── catalogos.js
    ├── clientes.js
    └── pedidos.js
```

## Licencia
ISC
