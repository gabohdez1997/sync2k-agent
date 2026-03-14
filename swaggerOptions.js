const swaggerJsdoc = require('swagger-jsdoc');

const options = {
  definition: {
    openapi: '3.0.0',
    info: {
      title: 'Sync2k Agent API',
      version: '1.0.0',
      description: 'API para la gestión de múltiples sedes de Profit Plus mediante el agente Sync2k.',
      contact: {
        name: 'Soporte Sync2k',
      },
    },
    servers: [
      {
        url: 'http://localhost:3000',
        description: 'Servidor de desarrollo',
      },
    ],
    components: {
      securitySchemes: {
        ApiKeyAuth: {
          type: 'apiKey',
          in: 'header',
          name: 'x-api-key',
          description: 'Introduce tu API Key en el header x-api-key',
        },
      },
    },
    security: [
      {
        ApiKeyAuth: [],
      },
    ],
  },
  apis: ['./routes/*.js'], // Ruta a los archivos que contienen las anotaciones
};

const specs = swaggerJsdoc(options);

module.exports = specs;
