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

// ────────────────────────────────────────────────────────────────────────────
// 7. GET /api/v1/usuarios/export-master — Exportar todos los usuarios, mapas y perfiles
// ────────────────────────────────────────────────────────────────────────────
router.get('/sync/export-master', async (req, res) => {
    try {
        const sede = req.query.sede || req.query.sede_id || req.headers['x-branch-id'];
        const pool = await getMasterPool(sede);

        const [mapasRes, usersRes, perfilesRes, repSegRes] = await Promise.all([
            pool.request().query(`
                SELECT RTRIM(co_mapa) as co_mapa,
                       RTRIM(des_mapa) as des_mapa,
                       RTRIM(producto) as producto,
                       pantallas,
                       reportes,
                       modulos,
                       RTRIM(co_idioma) as co_idioma,
                       liquidar
                FROM MpMapa
            `),
            pool.request().query(`
                SELECT RTRIM(Cod_Usuario) as Cod_Usuario,
                       RTRIM(Desc_Usuario) as Desc_Usuario,
                       Password,
                       Prioridad,
                       RTRIM(Usuario_Nodo) as Usuario_Nodo,
                       RTRIM(Id_Grupo) as Id_Grupo,
                       Camb_Sucu,
                       Pide_Sucu,
                       RTRIM(Sucursal) as Sucursal,
                       RTRIM(Id_Idioma) as Id_Idioma,
                       Estado,
                       RTRIM(Cod_Empresa) as Cod_Empresa,
                       RTRIM(Cod_Empresa_Nomi) as Cod_Empresa_Nomi,
                       RTRIM(Cod_Empresa_Admi) as Cod_Empresa_Admi,
                       Sel_Emp,
                       Sel_Emp_Nomi,
                       Sel_Emp_Admi,
                       RTRIM(co_mapa) as co_mapa,
                       RTRIM(co_mapa_nomi) as co_mapa_nomi,
                       RTRIM(co_mapa_admi) as co_mapa_admi,
                       Acceso_Todas_Empresa,
                       Acceso_Todas_Empresa_Nomi,
                       Acceso_Todas_Empresa_Admi
                FROM MpUsuario
            `),
            pool.request().query(`
                SELECT RTRIM(cod_usuario) as cod_usuario,
                       RTRIM(cod_empresa) as cod_empresa,
                       RTRIM(co_mapa) as co_mapa,
                       estado
                FROM MpUsuario_Perfil
            `),
            pool.request().query(`
                SELECT RTRIM(co_mapa) as co_mapa,
                       RTRIM(co_reporte) as co_reporte,
                       RTRIM(producto) as producto
                FROM MpReporteSegMapa
            `).catch(() => ({ recordset: [] }))
        ]);

        const mapas = mapasRes.recordset.map(m => ({
            ...m,
            pantallas: m.pantallas ? ('0x' + m.pantallas.toString('hex')) : null,
            reportes:  m.reportes  ? ('0x' + m.reportes.toString('hex'))  : null,
            modulos:   m.modulos   ? ('0x' + m.modulos.toString('hex'))   : null
        }));

        const usuarios = usersRes.recordset.map(u => ({
            ...u,
            Password: u.Password ? ('0x' + u.Password.toString('hex')) : null
        }));

        const perfiles = perfilesRes.recordset;
        const reportes_mapa = repSegRes.recordset;

        res.status(200).json({
            success: true,
            sede: sede || 'default',
            counts: {
                mapas: mapas.length,
                usuarios: usuarios.length,
                perfiles: perfiles.length
            },
            data: {
                mapas,
                usuarios,
                perfiles,
                reportes_mapa
            }
        });
    } catch (error) {
        console.error('[EXPORT MASTER ERROR]:', error);
        res.status(500).json({ success: false, message: 'Error exportando datos de MasterProfitPro', error: error.message });
    }
});

// ────────────────────────────────────────────────────────────────────────────
// 8. POST /api/v1/usuarios/sync/import-master — Importar / UPSERT lote en MasterProfitPro
// ────────────────────────────────────────────────────────────────────────────
router.post('/sync/import-master', async (req, res) => {
    try {
        const { sede, data } = req.body;
        if (!data) return res.status(400).json({ success: false, message: 'Cuerpo requerido: data.' });

        const pool = await getMasterPool(sede);
        const { mapas = [], usuarios = [], perfiles = [], reportes_mapa = [] } = data;

        let migratedMapas = 0;
        let migratedUsers = 0;
        let migratedPerfiles = 0;

        // 1. Sincronizar MpMapa
        for (const m of mapas) {
            if (!m.co_mapa || !m.producto) continue;
            try {
                const reqM = pool.request();
                reqM.input('co_mapa', sql.Char(6), m.co_mapa.padEnd(6, ' '));
                reqM.input('des_mapa', sql.VarChar(60), m.des_mapa || '');
                reqM.input('producto', sql.Char(6), (m.producto || 'ADMI').padEnd(6, ' '));
                
                const bufPantallas = m.pantallas ? Buffer.from(String(m.pantallas).replace(/^0x/, ''), 'hex') : null;
                const bufReportes  = m.reportes  ? Buffer.from(String(m.reportes).replace(/^0x/, ''), 'hex') : null;
                const bufModulos   = m.modulos   ? Buffer.from(String(m.modulos).replace(/^0x/, ''), 'hex') : null;

                reqM.input('pantallas', sql.VarBinary, bufPantallas);
                reqM.input('reportes',  sql.VarBinary, bufReportes);
                reqM.input('modulos',   sql.VarBinary, bufModulos);
                reqM.input('co_idioma', sql.VarChar(6), m.co_idioma || 'ES-VE');
                reqM.input('liquidar',  sql.Bit, m.liquidar ? 1 : 0);

                await reqM.query(`
                    IF EXISTS (SELECT 1 FROM MpMapa WHERE RTRIM(co_mapa) = RTRIM(@co_mapa) AND RTRIM(producto) = RTRIM(@producto))
                    BEGIN
                        UPDATE MpMapa SET
                            des_mapa = @des_mapa,
                            pantallas = COALESCE(@pantallas, pantallas),
                            reportes = COALESCE(@reportes, reportes),
                            modulos = COALESCE(@modulos, modulos),
                            co_idioma = @co_idioma,
                            liquidar = @liquidar
                        WHERE RTRIM(co_mapa) = RTRIM(@co_mapa) AND RTRIM(producto) = RTRIM(@producto)
                    END
                    ELSE
                    BEGIN
                        INSERT INTO MpMapa (co_mapa, des_mapa, producto, pantallas, reportes, modulos, co_idioma, liquidar)
                        VALUES (@co_mapa, @des_mapa, @producto, @pantallas, @reportes, @modulos, @co_idioma, @liquidar)
                    END
                `);
                migratedMapas++;
            } catch (errM) {
                console.warn(`[IMPORT MASTER] Error en mapa ${m.co_mapa}:`, errM.message);
            }
        }

        // 2. Sincronizar MpUsuario
        for (const u of usuarios) {
            if (!u.Cod_Usuario) continue;
            try {
                const reqU = pool.request();
                reqU.input('Cod_Usuario', sql.Char(6), u.Cod_Usuario.padEnd(6, ' '));
                const bufPass = u.Password ? Buffer.from(String(u.Password).replace(/^0x/, ''), 'hex') : null;
                reqU.input('Password', sql.VarBinary, bufPass);
                reqU.input('Prioridad', sql.Decimal(18, 0), u.Prioridad || 0);
                const sEstado = (u.Estado !== undefined && u.Estado !== null && String(u.Estado).trim() !== '') ? String(u.Estado).trim() : 'A';
                reqU.input('Estado', sql.Char(1), sEstado);

                reqU.input('co_mapa', sql.Char(6), u.co_mapa ? u.co_mapa.padEnd(6, ' ') : null);
                reqU.input('co_mapa_nomi', sql.Char(6), u.co_mapa_nomi ? u.co_mapa_nomi.padEnd(6, ' ') : null);
                reqU.input('co_mapa_admi', sql.Char(6), u.co_mapa_admi ? u.co_mapa_admi.padEnd(6, ' ') : null);

                reqU.input('Acceso_Todas_Empresa', sql.Bit, u.Acceso_Todas_Empresa ? 1 : 0);
                reqU.input('Acceso_Todas_Empresa_Nomi', sql.Bit, u.Acceso_Todas_Empresa_Nomi ? 1 : 0);
                reqU.input('Acceso_Todas_Empresa_Admi', sql.Bit, u.Acceso_Todas_Empresa_Admi ? 1 : 0);

                reqU.input('Cod_Empresa', sql.Char(20), u.Cod_Empresa ? u.Cod_Empresa.padEnd(20, ' ') : null);
                reqU.input('Cod_Empresa_Nomi', sql.Char(20), u.Cod_Empresa_Nomi ? u.Cod_Empresa_Nomi.padEnd(20, ' ') : null);
                reqU.input('Cod_Empresa_Admi', sql.Char(20), u.Cod_Empresa_Admi ? u.Cod_Empresa_Admi.padEnd(20, ' ') : null);

                reqU.input('Sucursal', sql.Char(6), u.Sucursal ? u.Sucursal.padEnd(6, ' ') : null);
                reqU.input('Camb_Sucu', sql.Bit, u.Camb_Sucu ? 1 : 0);
                reqU.input('Pide_Sucu', sql.Bit, u.Pide_Sucu ? 1 : 0);

                await reqU.query(`
                    IF EXISTS (SELECT 1 FROM MpUsuario WHERE RTRIM(Cod_Usuario) = RTRIM(@Cod_Usuario))
                    BEGIN
                        UPDATE MpUsuario SET
                            Desc_Usuario = @Desc_Usuario,
                            Password = COALESCE(@Password, Password),
                            Prioridad = @Prioridad,
                            Estado = @Estado,
                            co_mapa = @co_mapa,
                            co_mapa_nomi = @co_mapa_nomi,
                            co_mapa_admi = @co_mapa_admi,
                            Acceso_Todas_Empresa = @Acceso_Todas_Empresa,
                            Acceso_Todas_Empresa_Nomi = @Acceso_Todas_Empresa_Nomi,
                            Acceso_Todas_Empresa_Admi = @Acceso_Todas_Empresa_Admi,
                            Cod_Empresa = @Cod_Empresa,
                            Cod_Empresa_Nomi = @Cod_Empresa_Nomi,
                            Cod_Empresa_Admi = @Cod_Empresa_Admi,
                            Sucursal = @Sucursal,
                            Camb_Sucu = @Camb_Sucu,
                            Pide_Sucu = @Pide_Sucu
                        WHERE RTRIM(Cod_Usuario) = RTRIM(@Cod_Usuario)
                    END
                    ELSE
                    BEGIN
                        INSERT INTO MpUsuario (
                            Cod_Usuario, Desc_Usuario, Password, Prioridad, Estado,
                            co_mapa, co_mapa_nomi, co_mapa_admi,
                            Acceso_Todas_Empresa, Acceso_Todas_Empresa_Nomi, Acceso_Todas_Empresa_Admi,
                            Cod_Empresa, Cod_Empresa_Nomi, Cod_Empresa_Admi,
                            Sucursal, Camb_Sucu, Pide_Sucu
                        ) VALUES (
                            @Cod_Usuario, @Desc_Usuario, @Password, @Prioridad, @Estado,
                            @co_mapa, @co_mapa_nomi, @co_mapa_admi,
                            @Acceso_Todas_Empresa, @Acceso_Todas_Empresa_Nomi, @Acceso_Todas_Empresa_Admi,
                            @Cod_Empresa, @Cod_Empresa_Nomi, @Cod_Empresa_Admi,
                            @Sucursal, @Camb_Sucu, @Pide_Sucu
                        )
                    END
                `);
                migratedUsers++;
            } catch (errU) {
                console.warn(`[IMPORT MASTER] Error en usuario ${u.Cod_Usuario}:`, errU.message);
            }
        }

        // 3. Sincronizar MpUsuario_Perfil
        const empresasDest = await pool.request().query('SELECT RTRIM(cod_empresa) as cod_empresa FROM MpEmpresa');
        const validEmpresas = new Set(empresasDest.recordset.map(e => e.cod_empresa.toUpperCase()));

        for (const p of perfiles) {
            if (!p.cod_usuario || !p.cod_empresa) continue;
            const empClean = p.cod_empresa.trim().toUpperCase();
            if (!validEmpresas.has(empClean)) continue;

            try {
                const reqP = pool.request();
                reqP.input('cod_usuario', sql.Char(6), p.cod_usuario.padEnd(6, ' '));
                reqP.input('cod_empresa', sql.Char(20), p.cod_empresa.padEnd(20, ' '));
                reqP.input('co_mapa', sql.Char(6), (p.co_mapa || '300').padEnd(6, ' '));
                reqP.input('estado', sql.Char(1), p.estado || 'A');

                await reqP.query(`
                    IF EXISTS (SELECT 1 FROM MpUsuario_Perfil WHERE RTRIM(cod_usuario) = RTRIM(@cod_usuario) AND RTRIM(cod_empresa) = RTRIM(@cod_empresa))
                    BEGIN
                        UPDATE MpUsuario_Perfil SET
                            co_mapa = @co_mapa,
                            estado = @estado
                        WHERE RTRIM(cod_usuario) = RTRIM(@cod_usuario) AND RTRIM(cod_empresa) = RTRIM(@cod_empresa)
                    END
                    ELSE
                    BEGIN
                        INSERT INTO MpUsuario_Perfil (cod_usuario, cod_empresa, co_mapa, estado)
                        VALUES (@cod_usuario, @cod_empresa, @co_mapa, @estado)
                    END
                `);
                migratedPerfiles++;
            } catch (errP) {
                console.warn(`[IMPORT MASTER] Error en perfil ${p.cod_usuario} - ${p.cod_empresa}:`, errP.message);
            }
        }

        res.status(200).json({
            success: true,
            message: `Sincronización en ${sede || 'Master'} completada.`,
            migrated_mapas: migratedMapas,
            migrated_usuarios: migratedUsers,
            migrated_perfiles: migratedPerfiles
        });
    } catch (error) {
        console.error('[IMPORT MASTER FATAL ERROR]:', error);
        res.status(500).json({ success: false, message: 'Error importando datos a MasterProfitPro', error: error.message });
    }
});

// ────────────────────────────────────────────────────────────────────────────
// 9. POST /api/v1/usuarios/sync — Sincronización multisede global de MasterProfitPro
// ────────────────────────────────────────────────────────────────────────────
router.post('/sync', async (req, res) => {
    try {
        const { getServers } = require('../db');
        const servers = getServers();
        if (servers.length < 2) {
            return res.status(400).json({
                success: false,
                message: 'Se requieren al menos 2 sucursales activas para sincronizar usuarios y mapas.'
            });
        }

        // 1. Exportar datos de todos los servidores
        const allExports = [];
        for (const srv of servers) {
            try {
                const pool = await getMasterPool(srv.id);
                const [mapasRes, usersRes, perfilesRes] = await Promise.all([
                    pool.request().query(`SELECT RTRIM(co_mapa) as co_mapa, RTRIM(des_mapa) as des_mapa, RTRIM(producto) as producto, pantallas, reportes, modulos, RTRIM(co_idioma) as co_idioma, liquidar FROM MpMapa`),
                    pool.request().query(`SELECT RTRIM(Cod_Usuario) as Cod_Usuario, RTRIM(Desc_Usuario) as Desc_Usuario, Password, Prioridad, RTRIM(Usuario_Nodo) as Usuario_Nodo, RTRIM(Id_Grupo) as Id_Grupo, Camb_Sucu, Pide_Sucu, RTRIM(Sucursal) as Sucursal, RTRIM(Id_Idioma) as Id_Idioma, Estado, RTRIM(Cod_Empresa) as Cod_Empresa, RTRIM(Cod_Empresa_Nomi) as Cod_Empresa_Nomi, RTRIM(Cod_Empresa_Admi) as Cod_Empresa_Admi, Sel_Emp, Sel_Emp_Nomi, Sel_Emp_Admi, RTRIM(co_mapa) as co_mapa, RTRIM(co_mapa_nomi) as co_mapa_nomi, RTRIM(co_mapa_admi) as co_mapa_admi, Acceso_Todas_Empresa, Acceso_Todas_Empresa_Nomi, Acceso_Todas_Empresa_Admi FROM MpUsuario`),
                    pool.request().query(`SELECT RTRIM(cod_usuario) as cod_usuario, RTRIM(cod_empresa) as cod_empresa, RTRIM(co_mapa) as co_mapa, estado FROM MpUsuario_Perfil`)
                ]);
                allExports.push({
                    server: srv,
                    mapas: mapasRes.recordset,
                    usuarios: usersRes.recordset,
                    perfiles: perfilesRes.recordset
                });
            } catch (errSrv) {
                console.warn(`[SYNC MASTER] Error exportando de ${srv.name}:`, errSrv.message);
            }
        }

        if (allExports.length < 2) {
            return res.status(500).json({
                success: false,
                message: 'No se pudo conectar a MasterProfitPro en suficientes sedes activas.'
            });
        }

        // 2. Unificar catálogo maestro
        const masterMapas = new Map();
        const masterUsers = new Map();
        const masterPerfiles = new Map();

        for (const exp of allExports) {
            for (const m of exp.mapas) {
                const key = `${m.co_mapa.trim().toUpperCase()}__${(m.producto || 'ADMI').trim().toUpperCase()}`;
                if (!masterMapas.has(key)) masterMapas.set(key, m);
            }
            for (const u of exp.usuarios) {
                const key = u.Cod_Usuario.trim().toUpperCase();
                if (!masterUsers.has(key)) masterUsers.set(key, u);
            }
            for (const p of exp.perfiles) {
                const key = `${p.cod_usuario.trim().toUpperCase()}__${p.cod_empresa.trim().toUpperCase()}`;
                if (!masterPerfiles.has(key)) masterPerfiles.set(key, p);
            }
        }

        const unifiedMapas = Array.from(masterMapas.values());
        const unifiedUsers = Array.from(masterUsers.values());
        const unifiedPerfiles = Array.from(masterPerfiles.values());

        // 3. Replicar a todas las sedes
        const summary = [];
        let totalUsersSynced = 0;
        let totalMapasSynced = 0;

        for (const srv of servers) {
            try {
                const pool = await getMasterPool(srv.id);
                let migUsers = 0;
                let migMapas = 0;
                let migPerfiles = 0;

                // Sincronizar mapas
                for (const m of unifiedMapas) {
                    try {
                        const reqM = pool.request();
                        reqM.input('co_mapa', sql.Char(6), m.co_mapa.padEnd(6, ' '));
                        reqM.input('des_mapa', sql.VarChar(60), m.des_mapa || '');
                        reqM.input('producto', sql.Char(6), (m.producto || 'ADMI').padEnd(6, ' '));
                        reqM.input('pantallas', sql.VarBinary, m.pantallas);
                        reqM.input('reportes',  sql.VarBinary, m.reportes);
                        reqM.input('modulos',   sql.VarBinary, m.modulos);
                        reqM.input('co_idioma', sql.VarChar(6), m.co_idioma || 'ES-VE');
                        reqM.input('liquidar',  sql.Bit, m.liquidar ? 1 : 0);

                        await reqM.query(`
                            IF EXISTS (SELECT 1 FROM MpMapa WHERE RTRIM(co_mapa) = RTRIM(@co_mapa) AND RTRIM(producto) = RTRIM(@producto))
                            BEGIN
                                UPDATE MpMapa SET
                                    des_mapa = @des_mapa,
                                    pantallas = COALESCE(@pantallas, pantallas),
                                    reportes = COALESCE(@reportes, reportes),
                                    modulos = COALESCE(@modulos, modulos),
                                    co_idioma = @co_idioma,
                                    liquidar = @liquidar
                                WHERE RTRIM(co_mapa) = RTRIM(@co_mapa) AND RTRIM(producto) = RTRIM(@producto)
                            END
                            ELSE
                            BEGIN
                                INSERT INTO MpMapa (co_mapa, des_mapa, producto, pantallas, reportes, modulos, co_idioma, liquidar)
                                VALUES (@co_mapa, @des_mapa, @producto, @pantallas, @reportes, @modulos, @co_idioma, @liquidar)
                            END
                        `);
                        migMapas++;
                    } catch (eM) {}
                }

                // Sincronizar usuarios
                for (const u of unifiedUsers) {
                    try {
                        const reqU = pool.request();
                        reqU.input('Cod_Usuario', sql.Char(6), u.Cod_Usuario.padEnd(6, ' '));
                        reqU.input('Desc_Usuario', sql.VarChar(60), u.Desc_Usuario || '');
                        reqU.input('Password', sql.VarBinary, u.Password);
                        reqU.input('Prioridad', sql.Decimal(18, 0), u.Prioridad || 0);
                        const sEstado = (u.Estado !== undefined && u.Estado !== null && String(u.Estado).trim() !== '') ? String(u.Estado).trim() : 'A';
                        reqU.input('Estado', sql.Char(1), sEstado);

                        reqU.input('co_mapa', sql.Char(6), u.co_mapa ? u.co_mapa.padEnd(6, ' ') : null);
                        reqU.input('co_mapa_nomi', sql.Char(6), u.co_mapa_nomi ? u.co_mapa_nomi.padEnd(6, ' ') : null);
                        reqU.input('co_mapa_admi', sql.Char(6), u.co_mapa_admi ? u.co_mapa_admi.padEnd(6, ' ') : null);

                        reqU.input('Acceso_Todas_Empresa', sql.Bit, u.Acceso_Todas_Empresa ? 1 : 0);
                        reqU.input('Acceso_Todas_Empresa_Nomi', sql.Bit, u.Acceso_Todas_Empresa_Nomi ? 1 : 0);
                        reqU.input('Acceso_Todas_Empresa_Admi', sql.Bit, u.Acceso_Todas_Empresa_Admi ? 1 : 0);

                        reqU.input('Cod_Empresa', sql.Char(20), u.Cod_Empresa ? u.Cod_Empresa.padEnd(20, ' ') : null);
                        reqU.input('Cod_Empresa_Nomi', sql.Char(20), u.Cod_Empresa_Nomi ? u.Cod_Empresa_Nomi.padEnd(20, ' ') : null);
                        reqU.input('Cod_Empresa_Admi', sql.Char(20), u.Cod_Empresa_Admi ? u.Cod_Empresa_Admi.padEnd(20, ' ') : null);

                        reqU.input('Sucursal', sql.Char(6), u.Sucursal ? u.Sucursal.padEnd(6, ' ') : null);
                        reqU.input('Camb_Sucu', sql.Bit, u.Camb_Sucu ? 1 : 0);
                        reqU.input('Pide_Sucu', sql.Bit, u.Pide_Sucu ? 1 : 0);

                        await reqU.query(`
                            IF EXISTS (SELECT 1 FROM MpUsuario WHERE RTRIM(Cod_Usuario) = RTRIM(@Cod_Usuario))
                            BEGIN
                                UPDATE MpUsuario SET
                                    Desc_Usuario = @Desc_Usuario,
                                    Password = COALESCE(@Password, Password),
                                    Prioridad = @Prioridad,
                                    Estado = @Estado,
                                    co_mapa = @co_mapa,
                                    co_mapa_nomi = @co_mapa_nomi,
                                    co_mapa_admi = @co_mapa_admi,
                                    Acceso_Todas_Empresa = @Acceso_Todas_Empresa,
                                    Acceso_Todas_Empresa_Nomi = @Acceso_Todas_Empresa_Nomi,
                                    Acceso_Todas_Empresa_Admi = @Acceso_Todas_Empresa_Admi,
                                    Cod_Empresa = @Cod_Empresa,
                                    Cod_Empresa_Nomi = @Cod_Empresa_Nomi,
                                    Cod_Empresa_Admi = @Cod_Empresa_Admi,
                                    Sucursal = @Sucursal,
                                    Camb_Sucu = @Camb_Sucu,
                                    Pide_Sucu = @Pide_Sucu
                                WHERE RTRIM(Cod_Usuario) = RTRIM(@Cod_Usuario)
                            END
                            ELSE
                            BEGIN
                                INSERT INTO MpUsuario (
                                    Cod_Usuario, Desc_Usuario, Password, Prioridad, Estado,
                                    co_mapa, co_mapa_nomi, co_mapa_admi,
                                    Acceso_Todas_Empresa, Acceso_Todas_Empresa_Nomi, Acceso_Todas_Empresa_Admi,
                                    Cod_Empresa, Cod_Empresa_Nomi, Cod_Empresa_Admi,
                                    Sucursal, Camb_Sucu, Pide_Sucu
                                ) VALUES (
                                    @Cod_Usuario, @Desc_Usuario, @Password, @Prioridad, @Estado,
                                    @co_mapa, @co_mapa_nomi, @co_mapa_admi,
                                    @Acceso_Todas_Empresa, @Acceso_Todas_Empresa_Nomi, @Acceso_Todas_Empresa_Admi,
                                    @Cod_Empresa, @Cod_Empresa_Nomi, @Cod_Empresa_Admi,
                                    @Sucursal, @Camb_Sucu, @Pide_Sucu
                                )
                            END
                        `);
                        migUsers++;
                    } catch (eU) {}
                }

                // Sincronizar perfiles
                const empresasDest = await pool.request().query('SELECT RTRIM(cod_empresa) as cod_empresa FROM MpEmpresa');
                const validEmpresas = new Set(empresasDest.recordset.map(e => e.cod_empresa.toUpperCase()));

                for (const p of unifiedPerfiles) {
                    if (!p.cod_usuario || !p.cod_empresa) continue;
                    const empClean = p.cod_empresa.trim().toUpperCase();
                    if (!validEmpresas.has(empClean)) continue;

                    try {
                        const reqP = pool.request();
                        reqP.input('cod_usuario', sql.Char(6), p.cod_usuario.padEnd(6, ' '));
                        reqP.input('cod_empresa', sql.Char(20), p.cod_empresa.padEnd(20, ' '));
                        reqP.input('co_mapa', sql.Char(6), (p.co_mapa || '300').padEnd(6, ' '));
                        reqP.input('estado', sql.Char(1), p.estado || 'A');

                        await reqP.query(`
                            IF EXISTS (SELECT 1 FROM MpUsuario_Perfil WHERE RTRIM(cod_usuario) = RTRIM(@cod_usuario) AND RTRIM(cod_empresa) = RTRIM(@cod_empresa))
                            BEGIN
                                UPDATE MpUsuario_Perfil SET
                                    co_mapa = @co_mapa,
                                    estado = @estado
                                WHERE RTRIM(cod_usuario) = RTRIM(@cod_usuario) AND RTRIM(cod_empresa) = RTRIM(@cod_empresa)
                            END
                            ELSE
                            BEGIN
                                INSERT INTO MpUsuario_Perfil (cod_usuario, cod_empresa, co_mapa, estado)
                                VALUES (@cod_usuario, @cod_empresa, @co_mapa, @estado)
                            END
                        `);
                        migPerfiles++;
                    } catch (eP) {}
                }

                totalUsersSynced += migUsers;
                totalMapasSynced += migMapas;

                summary.push({
                    sede_id: srv.id,
                    sede_nombre: srv.name,
                    migrated: migUsers,
                    migrated_mapas: migMapas,
                    migrated_perfiles: migPerfiles
                });
            } catch (errSync) {
                summary.push({
                    sede_id: srv.id,
                    sede_nombre: srv.name,
                    migrated: 0,
                    errors: [errSync.message]
                });
            }
        }

        res.status(200).json({
            success: true,
            entity: 'profit_users',
            message: `Sincronización de MasterProfitPro completada con éxito. Se procesaron ${unifiedUsers.length} usuarios y ${unifiedMapas.length} mapas en ${servers.length} sedes.`,
            total_synced: totalUsersSynced,
            summary
        });
    } catch (error) {
        console.error('[SYNC MASTER FATAL ERROR]:', error);
        res.status(500).json({
            success: false,
            message: 'Error general al sincronizar usuarios y mapas de MasterProfitPro.',
            error: error.message
        });
    }
});

module.exports = router;
