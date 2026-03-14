const express  = require('express');
const router   = express.Router();
const jwt      = require('jsonwebtoken');
const { sql, getMasterPool } = require('../db');

const JWT_SECRET     = process.env.JWT_SECRET     || 'sync2k_secret';
const JWT_EXPIRES_IN = process.env.JWT_EXPIRES_IN || '8h';

// ────────────────────────────────────────────────────────────────────────
// POST /api/v1/auth/login
// Body: { usuario, password, producto? }
// producto: 'CONT' | 'NOMI' | 'ADMI'  (default: 'ADMI')
// ────────────────────────────────────────────────────────────────────────
/**
 * @swagger
 * tags:
 *   name: Auth
 *   description: Autenticación de usuarios Profit Plus (MasterProfitPro)
 */

/**
 * @swagger
 * /api/v1/auth/login:
 *   post:
 *     summary: Login de usuario Profit Plus. Retorna JWT con permisos y mapas.
 *     tags: [Auth]
 *     requestBody:
 *       required: true
 *       content:
 *         application/json:
 *           schema:
 *             type: object
 *             required: [usuario, password]
 *             properties:
 *               usuario:
 *                 type: string
 *                 description: Código del usuario en Profit (6 chars)
 *               password:
 *                 type: string
 *               producto:
 *                 type: string
 *                 description: "Módulo: CONT | NOMI | ADMI (default: ADMI)"
 *     responses:
 *       200:
 *         description: Login exitoso, retorna token JWT con permisos del usuario
 *       401:
 *         description: Credenciales inválidas o usuario inactivo
 *       500:
 *         description: Error del servidor
 */
router.post('/login', async (req, res) => {
    try {
        const { usuario, password, producto = 'ADMI' } = req.body;
        if (!usuario || !password) {
            return res.status(400).json({ success: false, message: 'Campos requeridos: usuario, password.' });
        }

        const pool = await getMasterPool();

        // 1. Verificar credenciales usando el mismo hash SHA1 que Profit
        const authRes = await pool.request()
            .input('sUsuario',  sql.Char(6),  usuario.trim())
            .input('sPassword', sql.Char(15), password)
            .query(`
                SELECT u.Cod_Usuario,
                       RTRIM(u.Desc_Usuario)           AS nombre,
                       u.Estado,
                       u.Prioridad,
                       COUNT(*) OVER()                 AS match_count
                FROM MpUsuario u
                WHERE u.Cod_Usuario = RTRIM(@sUsuario)
                  AND u.Password = HashBytes('SHA1', RTRIM(@sPassword))
            `);

        if (!authRes.recordset.length) {
            return res.status(401).json({ success: false, message: 'Usuario o contraseña incorrectos.' });
        }

        const user = authRes.recordset[0];

        // Estado '3' = activo en Profit Plus
        if (user.Estado !== '3') {
            return res.status(401).json({ success: false, message: 'Usuario inactivo o bloqueado en Profit Plus.' });
        }

        // 2. Obtener empresas y mapas asignados para el producto solicitado
        const accesoRes = await pool.request()
            .input('sCod_Usuario', sql.Char(6), usuario.trim())
            .input('sProducto',   sql.Char(6), producto.trim())
            .execute('pConsultarUsuarioAccesos');

        const accesos = accesoRes.recordset.map(r => ({
            cod_empresa:  r.cod_empresa ? r.cod_empresa.trim()  : null,
            desc_empresa: r.desc_empresa ? r.desc_empresa.trim() : null,
            co_mapa:      r.co_mapa ? r.co_mapa.trim() : null
        }));

        // 3. Obtener descripción del mapa principal (si lo tuviese)
        const mapaField = producto === 'NOMI' ? 'co_mapa_nomi' : (producto === 'ADMI' ? 'co_mapa_admi' : 'co_mapa');
        const mapaRes = await pool.request()
            .input('sUsuario', sql.Char(6), usuario.trim())
            .query(`
                SELECT RTRIM(u.${mapaField}) AS co_mapa,
                       RTRIM(m.des_mapa)    AS des_mapa
                FROM MpUsuario u
                LEFT JOIN MpMapa m ON m.co_mapa = u.${mapaField}
                WHERE u.Cod_Usuario = RTRIM(@sUsuario)
            `);

        const mapaInfo = mapaRes.recordset[0] || {};

        // 4. Generar JWT
        const payload = {
            cod_usuario: user.Cod_Usuario.trim(),
            nombre:      user.nombre,
            prioridad:   user.Prioridad,
            producto,
            mapa:        mapaInfo.co_mapa  || null,
            des_mapa:    mapaInfo.des_mapa || null,
            accesos
        };

        const token = jwt.sign(payload, JWT_SECRET, { expiresIn: JWT_EXPIRES_IN });

        // 5. Actualizar fecha último ingreso (no bloqueante)
        pool.request()
            .input('sUsuario', sql.Char(6), usuario.trim())
            .query(`UPDATE MpUsuario SET Fec_Ult = GETDATE() WHERE Cod_Usuario = RTRIM(@sUsuario)`)
            .catch(() => {});

        return res.status(200).json({
            success: true,
            token,
            expires_in: JWT_EXPIRES_IN,
            usuario: payload
        });

    } catch (error) {
        console.error('[POST /auth/login]', error);
        res.status(500).json({ success: false, message: 'Error al autenticar.', error: error.message });
    }
});

// ────────────────────────────────────────────────────────────────────────
// POST /api/v1/auth/verify  — Verifica y decodifica un token existente
// ────────────────────────────────────────────────────────────────────────
/**
 * @swagger
 * /api/v1/auth/verify:
 *   post:
 *     summary: Verifica un token JWT y retorna su payload decodificado
 *     tags: [Auth]
 *     requestBody:
 *       required: true
 *       content:
 *         application/json:
 *           schema:
 *             type: object
 *             required: [token]
 *             properties:
 *               token:
 *                 type: string
 *     responses:
 *       200:
 *         description: Token válido
 *       401:
 *         description: Token inválido o expirado
 */
router.post('/verify', (req, res) => {
    const { token } = req.body;
    if (!token) return res.status(400).json({ success: false, message: 'Token requerido.' });
    try {
        const decoded = jwt.verify(token, JWT_SECRET);
        res.status(200).json({ success: true, valid: true, usuario: decoded });
    } catch (err) {
        res.status(401).json({ success: false, valid: false, message: 'Token inválido o expirado.' });
    }
});

module.exports = router;
