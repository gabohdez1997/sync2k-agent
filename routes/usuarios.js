const express = require('express');
const router  = express.Router();
const { sql, getMasterPool } = require('../db');

// ── Middleware de autenticación JWT ──────────────────────────────────────────
const jwt = require('jsonwebtoken');
const JWT_SECRET = process.env.JWT_SECRET || 'sync2k_secret';

function requireAuth(req, res, next) {
    const auth = req.headers.authorization;
    if (!auth || !auth.startsWith('Bearer ')) {
        return res.status(401).json({ success: false, message: 'Token requerido. (Authorization: Bearer <token>)' });
    }
    try {
        req.user = jwt.verify(auth.split(' ')[1], JWT_SECRET);
        next();
    } catch {
        return res.status(401).json({ success: false, message: 'Token inválido o expirado.' });
    }
}

/**
 * @swagger
 * tags:
 *   name: Usuarios
 *   description: Gestión de usuarios Profit Plus (MasterProfitPro)
 */

// ────────────────────────────────────────────────────────────────────────────
// GET /api/v1/usuarios — Listado de usuarios
// ────────────────────────────────────────────────────────────────────────────
/**
 * @swagger
 * /api/v1/usuarios:
 *   get:
 *     summary: Listado de todos los usuarios registrados en Profit Plus
 *     tags: [Usuarios]
 *     security:
 *       - bearerAuth: []
 *     parameters:
 *       - in: query
 *         name: estado
 *         schema:
 *           type: string
 *           enum: ["3", "0"]
 *         description: "Filtrar por estado: 3=Activo, 0=Inactivo"
 *     responses:
 *       200:
 *         description: Listado de usuarios
 */
router.get('/', requireAuth, async (req, res) => {
    try {
        const pool = await getMasterPool();
        const { estado } = req.query;
        let whereExtra = '';
        const r = pool.request();
        if (estado !== undefined) {
            r.input('sEstado', sql.Char(1), estado);
            whereExtra = 'AND u.Estado = @sEstado';
        }

        const result = await r.query(`
            SELECT RTRIM(u.Cod_Usuario)  AS cod_usuario,
                   RTRIM(u.Desc_Usuario) AS nombre,
                   u.Estado,
                   u.Prioridad,
                   RTRIM(u.co_mapa)      AS co_mapa_cont,
                   RTRIM(u.co_mapa_nomi) AS co_mapa_nomi,
                   RTRIM(u.co_mapa_admi) AS co_mapa_admi,
                   RTRIM(m.des_mapa)     AS des_mapa_admi,
                   u.Acceso_Todas_Empresa,
                   u.Acceso_Todas_Empresa_Admi,
                   u.Fec_Ult             AS ultimo_ingreso,
                   u.fe_us_in           AS fecha_creacion
            FROM MpUsuario u
            LEFT JOIN MpMapa m ON m.co_mapa = u.co_mapa_admi
            WHERE 1=1 ${whereExtra}
            ORDER BY u.Desc_Usuario
        `);

        res.status(200).json({ success: true, count: result.recordset.length, data: result.recordset });
    } catch (error) {
        res.status(500).json({ success: false, message: 'Error al listar usuarios.', error: error.message });
    }
});

// ────────────────────────────────────────────────────────────────────────────
// GET /api/v1/usuarios/:id — Detalle de usuario + permisos
// ────────────────────────────────────────────────────────────────────────────
/**
 * @swagger
 * /api/v1/usuarios/{id}:
 *   get:
 *     summary: Detalle completo de un usuario incluyendo sus mapas y empresas asignadas
 *     tags: [Usuarios]
 *     security:
 *       - bearerAuth: []
 *     parameters:
 *       - in: path
 *         name: id
 *         required: true
 *         schema:
 *           type: string
 *         description: Código del usuario (Cod_Usuario)
 *     responses:
 *       200:
 *         description: Detalle del usuario con permisos
 *       404:
 *         description: Usuario no encontrado
 */
router.get('/:id', requireAuth, async (req, res) => {
    try {
        const pool = await getMasterPool();
        const { id } = req.params;
        const producto = req.query.producto || 'ADMI';

        // 1. Datos básicos del usuario
        const userRes = await pool.request().input('sCod', sql.Char(6), id).query(`
            SELECT RTRIM(u.Cod_Usuario)  AS cod_usuario,
                   RTRIM(u.Desc_Usuario) AS nombre,
                   u.Estado,
                   u.Prioridad,
                   RTRIM(u.co_mapa)      AS co_mapa_cont,
                   RTRIM(u.co_mapa_nomi) AS co_mapa_nomi,
                   RTRIM(u.co_mapa_admi) AS co_mapa_admi,
                   u.Acceso_Todas_Empresa,
                   u.Acceso_Todas_Empresa_Nomi,
                   u.Acceso_Todas_Empresa_Admi,
                   u.Fec_Ult             AS ultimo_ingreso,
                   u.fe_us_in           AS fecha_creacion
            FROM MpUsuario u
            WHERE u.Cod_Usuario = RTRIM(@sCod)
        `);

        if (!userRes.recordset.length) {
            return res.status(404).json({ success: false, message: 'Usuario no encontrado.' });
        }

        const user = userRes.recordset[0];

        // Determinar co_mapa según el producto solicitado
        const coMapaActivo =
            producto === 'NOMI' ? user.co_mapa_nomi :
            producto === 'CONT' ? user.co_mapa_cont :
            user.co_mapa_admi;

        // 2. Accesos por empresa + detalle del mapa de cada acceso (en paralelo)
        const [accesosRes, mapaRes, modulosRes] = await Promise.all([
            // empresas/mapas asignados al usuario para este producto
            pool.request()
                .input('sCod_Usuario', sql.Char(6), id)
                .input('sProducto', sql.Char(6), producto)
                .execute('pConsultarUsuarioAccesos'),

            // descripción e info del mapa principal del usuario
            coMapaActivo
                ? pool.request()
                    .input('sCoMapa',    sql.Char(6), coMapaActivo)
                    .input('sProducto',  sql.Char(6), producto)
                    .query(`
                        SELECT RTRIM(co_mapa)  AS co_mapa,
                               RTRIM(des_mapa) AS des_mapa,
                               producto
                        FROM MpMapa
                        WHERE co_mapa = @sCoMapa AND producto = @sProducto
                    `)
                : Promise.resolve({ recordset: [] }),

            // reportes asignados al mapa (vía MpReporteSegMapa + MpReporte)
            coMapaActivo
                ? pool.request()
                    .input('sCoMapa',   sql.Char(6), coMapaActivo)
                    .input('sProducto', sql.Char(6), producto)
                    .query(`
                        SELECT DISTINCT
                               RTRIM(r.co_reporte)      AS co_reporte,
                               RTRIM(r.des_reporte)     AS des_reporte,
                               RTRIM(r.co_tiporeporte)  AS tipo,
                               r.favorito
                        FROM MpReporteSegMapa rsm
                        INNER JOIN MpReporte r ON r.co_reporte = rsm.co_reporte
                                              AND r.producto   = rsm.producto
                        WHERE rsm.co_mapa   = @sCoMapa
                          AND rsm.producto  = @sProducto
                        ORDER BY r.des_reporte
                    `)
                : Promise.resolve({ recordset: [] })
        ]);

        // Módulos disponibles para este producto (catálogo completo — el mapa aplica bitmask internamente)
        const modulosCatRes = await pool.request()
            .input('sProducto', sql.Char(6), producto)
            .query(`
                SELECT RTRIM(co_modulo) AS co_modulo,
                       RTRIM(des_modulo) AS des_modulo,
                       orden
                FROM MpModulo
                WHERE producto = @sProducto
                ORDER BY orden
            `);

        const accesos = accesosRes.recordset.map(r => ({
            cod_empresa:  r.cod_empresa?.trim(),
            desc_empresa: r.desc_empresa?.trim(),
            co_mapa:      r.co_mapa?.trim()
        }));

        const mapaInfo = mapaRes.recordset[0] || null;
        const reportes = modulosRes.recordset;
        const modulos  = modulosCatRes.recordset;

        res.status(200).json({
            success: true,
            data: {
                ...user,
                producto,
                mapa: mapaInfo
                    ? {
                        co_mapa:  mapaInfo.co_mapa,
                        des_mapa: mapaInfo.des_mapa,
                        // Nota: los permisos internos de pantallas/acciones se almacenan
                        // como bitmask varbinary en MpMapa y no son decodificables desde SQL.
                        // Los módulos y reportes corresponden a lo asignado a este mapa.
                        modulos_disponibles: modulos,
                        reportes_asignados:  reportes
                      }
                    : null,
                accesos
            }
        });
    } catch (error) {
        res.status(500).json({ success: false, message: 'Error al consultar usuario.', error: error.message });
    }
});

// ────────────────────────────────────────────────────────────────────────────
// POST /api/v1/usuarios — Crear usuario (pInsertarUsuario)
// ────────────────────────────────────────────────────────────────────────────
/**
 * @swagger
 * /api/v1/usuarios:
 *   post:
 *     summary: Crear un nuevo usuario en Profit Plus
 *     tags: [Usuarios]
 *     security:
 *       - bearerAuth: []
 *     requestBody:
 *       required: true
 *       content:
 *         application/json:
 *           schema:
 *             type: object
 *             required: [cod_usuario, nombre, password, prioridad]
 *             properties:
 *               cod_usuario: { type: string, description: "Código de 6 chars" }
 *               nombre:      { type: string }
 *               password:    { type: string }
 *               prioridad:   { type: integer, description: "Nivel de prioridad 0-999" }
 *               co_mapa_admi: { type: string, description: "Código del mapa (permisos ADMI)" }
 *               co_mapa:     { type: string, description: "Código del mapa (permisos CONT)" }
 *     responses:
 *       200:
 *         description: Usuario creado exitosamente
 *       400:
 *         description: Datos inválidos
 */
router.post('/', requireAuth, async (req, res) => {
    try {
        const pool = await getMasterPool();
        const d = req.body;
        if (!d.cod_usuario || !d.nombre || !d.password || d.prioridad === undefined) {
            return res.status(400).json({ success: false, message: 'Requeridos: cod_usuario, nombre, password, prioridad.' });
        }

        const r = pool.request();
        r.input('sCod_Usuario',     sql.Char(6),       d.cod_usuario);
        r.input('sDesc_Usuario',    sql.VarChar(60),   d.nombre);
        r.input('sPassword',        sql.VarChar(60),   d.password);
        r.input('dePrioridad',      sql.Decimal(3, 0), d.prioridad || 0);
        r.input('sCo_Mapa',         sql.Char(6),       d.co_mapa     || null);
        r.input('sCo_Mapa_Nomi',    sql.Char(6),       d.co_mapa_nomi || null);
        r.input('sCo_Mapa_Admi',    sql.Char(6),       d.co_mapa_admi || null);
        r.input('sIdIdioma',        sql.Char(1),       d.id_idioma   || 'E');
        r.input('bAcceso_Todas',    sql.Bit,            d.acceso_todas ?? 0);
        r.input('bAcceso_Todas_Admi', sql.Bit,          d.acceso_todas_admi ?? 0);
        r.input('sCo_Us_In',        sql.Char(6),       req.user.cod_usuario || '999');
        r.input('sCo_Sucu_In',      sql.Char(6),       null);
        r.input('sMaquina',         sql.VarChar(60),   'SYNC2K');

        await r.execute('pInsertarUsuario');

        res.status(200).json({ success: true, message: `Usuario ${d.cod_usuario} creado exitosamente.` });
    } catch (error) {
        res.status(500).json({ success: false, message: 'Error al crear usuario.', error: error.message });
    }
});

// ────────────────────────────────────────────────────────────────────────────
// PUT /api/v1/usuarios/:id — Actualizar usuario (pActualizarUsuario)
// ────────────────────────────────────────────────────────────────────────────
/**
 * @swagger
 * /api/v1/usuarios/{id}:
 *   put:
 *     summary: Actualizar datos de un usuario
 *     tags: [Usuarios]
 *     security:
 *       - bearerAuth: []
 *     parameters:
 *       - in: path
 *         name: id
 *         required: true
 *         schema:
 *           type: string
 *     requestBody:
 *       content:
 *         application/json:
 *           schema:
 *             type: object
 *             properties:
 *               nombre:       { type: string }
 *               co_mapa_admi: { type: string }
 *               co_mapa:      { type: string }
 *               prioridad:    { type: integer }
 *     responses:
 *       200:
 *         description: Usuario actualizado
 *       404:
 *         description: Usuario no encontrado
 */
router.put('/:id', requireAuth, async (req, res) => {
    try {
        const pool = await getMasterPool();
        const { id } = req.params;
        const d = req.body;

        // Obtener validador actual
        const check = await pool.request()
            .input('sCod', sql.Char(6), id)
            .query(`SELECT validador, RTRIM(Desc_Usuario) AS nombre, Prioridad,
                           RTRIM(co_mapa) AS co_mapa, RTRIM(co_mapa_nomi) AS co_mapa_nomi, 
                           RTRIM(co_mapa_admi) AS co_mapa_admi, RTRIM(Id_Idioma) AS id_idioma,
                           Acceso_Todas_Empresa, Acceso_Todas_Empresa_Nomi, Acceso_Todas_Empresa_Admi
                    FROM MpUsuario WHERE Cod_Usuario = RTRIM(@sCod)`);

        if (!check.recordset.length) {
            return res.status(404).json({ success: false, message: 'Usuario no encontrado.' });
        }
        const row = check.recordset[0];

        const r = pool.request();
        r.input('sCod_Usuario',       sql.Char(6),       id);
        r.input('sCod_UsuarioOri',    sql.Char(6),       id);
        r.input('sDesc_Usuario',      sql.VarChar(60),   d.nombre       || row.nombre);
        r.input('dePrioridad',        sql.Decimal(3, 0), d.prioridad    ?? row.Prioridad);
        r.input('sCo_Mapa',           sql.Char(6),       d.co_mapa      || row.co_mapa);
        r.input('sCo_Mapa_Nomi',      sql.Char(6),       d.co_mapa_nomi || row.co_mapa_nomi);
        r.input('sCo_Mapa_Admi',      sql.Char(6),       d.co_mapa_admi || row.co_mapa_admi);
        r.input('sIdIdioma',          sql.Char(1),       d.id_idioma    || row.id_idioma || 'E');
        r.input('bAcceso_Todas',      sql.Bit,            d.acceso_todas      ?? row.Acceso_Todas_Empresa);
        r.input('bAcceso_Todas_Nomi', sql.Bit,            d.acceso_todas_nomi ?? row.Acceso_Todas_Empresa_Nomi);
        r.input('bAcceso_Todas_Admi', sql.Bit,            d.acceso_todas_admi ?? row.Acceso_Todas_Empresa_Admi);
        r.input('sCo_Us_Mo',          sql.Char(6),       req.user.cod_usuario || '999');
        r.input('sCo_Sucu_Mo',        sql.Char(6),       null);
        r.input('sMaquina',           sql.VarChar(60),   'SYNC2K');
        r.input('tsValidador',        sql.VarBinary,     row.validador);

        await r.execute('pActualizarUsuario');

        res.status(200).json({ success: true, message: `Usuario ${id} actualizado.` });
    } catch (error) {
        res.status(500).json({ success: false, message: 'Error al actualizar usuario.', error: error.message });
    }
});

// ────────────────────────────────────────────────────────────────────────────
// PUT /api/v1/usuarios/:id/password — Cambiar contraseña
// ────────────────────────────────────────────────────────────────────────────
/**
 * @swagger
 * /api/v1/usuarios/{id}/password:
 *   put:
 *     summary: Cambiar contraseña de un usuario
 *     tags: [Usuarios]
 *     security:
 *       - bearerAuth: []
 *     parameters:
 *       - in: path
 *         name: id
 *         required: true
 *         schema:
 *           type: string
 *     requestBody:
 *       required: true
 *       content:
 *         application/json:
 *           schema:
 *             type: object
 *             required: [password]
 *             properties:
 *               password: { type: string }
 *     responses:
 *       200:
 *         description: Contraseña actualizada
 */
router.put('/:id/password', requireAuth, async (req, res) => {
    try {
        const pool = await getMasterPool();
        const { id } = req.params;
        const { password } = req.body;
        if (!password) return res.status(400).json({ success: false, message: 'Campo requerido: password.' });

        const check = await pool.request()
            .input('sCod', sql.Char(6), id)
            .query(`SELECT validador FROM MpUsuario WHERE Cod_Usuario = RTRIM(@sCod)`);
        if (!check.recordset.length) {
            return res.status(404).json({ success: false, message: 'Usuario no encontrado.' });
        }

        const r = pool.request();
        r.input('sPkUsuario',   sql.Char(6),     id);
        r.input('sPassword',    sql.VarChar(60),  password);
        r.input('sReiniciar',   sql.Bit,          0);
        r.input('sMaquina',     sql.VarChar(60),  'SYNC2K');
        r.input('sCo_Us_Mo',    sql.Char(6),      req.user.cod_usuario || '999');
        r.input('sCo_Sucu_Mo',  sql.Char(6),      null);
        r.input('tsValidador',  sql.VarBinary,    check.recordset[0].validador);

        await r.execute('pCambiarContrasenhaUsuario');

        res.status(200).json({ success: true, message: `Contraseña del usuario ${id} actualizada.` });
    } catch (error) {
        res.status(500).json({ success: false, message: 'Error al cambiar contraseña.', error: error.message });
    }
});

// ────────────────────────────────────────────────────────────────────────────
// DELETE /api/v1/usuarios/:id — Eliminar usuario
// ────────────────────────────────────────────────────────────────────────────
/**
 * @swagger
 * /api/v1/usuarios/{id}:
 *   delete:
 *     summary: Eliminar un usuario de Profit Plus
 *     tags: [Usuarios]
 *     security:
 *       - bearerAuth: []
 *     parameters:
 *       - in: path
 *         name: id
 *         required: true
 *         schema:
 *           type: string
 *     responses:
 *       200:
 *         description: Usuario eliminado
 *       404:
 *         description: Usuario no encontrado
 */
router.delete('/:id', requireAuth, async (req, res) => {
    try {
        const pool = await getMasterPool();
        const { id } = req.params;

        const check = await pool.request()
            .input('sCod', sql.Char(6), id)
            .query(`SELECT validador FROM MpUsuario WHERE Cod_Usuario = RTRIM(@sCod)`);
        if (!check.recordset.length) {
            return res.status(404).json({ success: false, message: 'Usuario no encontrado.' });
        }

        const r = pool.request();
        r.input('sCod_UsuarioOri', sql.Char(6),    id);
        r.input('tsValidadorOri',  sql.VarBinary,  check.recordset[0].validador);
        r.input('sMaquina',        sql.VarChar(60), 'SYNC2K');
        r.input('sCo_Us_Mo',       sql.Char(6),    req.user.cod_usuario || '999');
        r.input('sCo_Sucu_Mo',     sql.Char(6),    null);

        await r.execute('pEliminarUsuario');

        res.status(200).json({ success: true, message: `Usuario ${id} eliminado.` });
    } catch (error) {
        res.status(500).json({ success: false, message: 'Error al eliminar usuario.', error: error.message });
    }
});

module.exports = router;
