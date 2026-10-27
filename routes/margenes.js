const express = require('express');
const router = express.Router();
const { sql, getPool, getServers } = require('../db');
const { padProfit, resolveServer } = require('../helpers/multiSede');

/**
 * GET /api/v1/margenes/resumen
 * Retorna los tipos de precio en saTipoPrecio junto con el margen predominante actual
 * en saArtMargen y el número de artículos afectados.
 */
router.get('/resumen', async (req, res) => {
    try {
        const branchParam = req.query.sede_id || req.headers['x-branch-id'];
        const srv = resolveServer(branchParam);
        if (!srv) {
            return res.status(404).json({ success: false, message: 'Servidor o sucursal no encontrada.' });
        }

        const pool = await getPool(srv.id, req.sqlAuth);
        const query = `
            SELECT 
                RTRIM(tp.co_precio) AS co_precio,
                RTRIM(tp.des_precio) AS des_precio,
                ISNULL(m.margen_frecuente, 0) AS margen_actual,
                ISNULL(m.total_articulos, 0) AS total_articulos,
                ISNULL(p.total_con_precio, 0) AS total_con_precio
            FROM saTipoPrecio tp
            OUTER APPLY (
                SELECT TOP 1 
                    m1.monto_max AS margen_frecuente,
                    COUNT(*) AS total_articulos
                FROM saArtMargen m1
                WHERE LTRIM(RTRIM(m1.co_precio)) = LTRIM(RTRIM(tp.co_precio))
                  AND m1.monto_max > 0
                GROUP BY m1.monto_max
                ORDER BY COUNT(*) DESC
            ) m
            OUTER APPLY (
                SELECT COUNT(*) AS total_con_precio
                FROM saArtPrecio p1
                WHERE LTRIM(RTRIM(p1.co_precio)) = LTRIM(RTRIM(tp.co_precio))
                  AND p1.monto > 0
            ) p
            ORDER BY tp.co_precio;
        `;

        const result = await pool.request().query(query);
        const data = (result.recordset || []).map(r => ({
            co_precio: r.co_precio,
            des_precio: r.des_precio,
            margen_actual: Number(r.margen_actual) || 0,
            total_articulos: Number(r.total_articulos) || 0,
            total_con_precio: Number(r.total_con_precio) || 0
        }));

        return res.status(200).json({
            success: true,
            sede_id: srv.id,
            sede_nombre: srv.name || srv.nombre,
            data
        });
    } catch (error) {
        console.error('[MARGENES] Error al obtener resumen de márgenes:', error);
        return res.status(500).json({
            success: false,
            message: 'Error al consultar resumen de márgenes en Profit Plus.',
            error: error.message
        });
    }
});

/**
 * POST /api/v1/margenes/actualizar-masivo
 * Aplica la actualización masiva de márgenes (saArtMargen) y/o recálculo de precios (saArtPrecio)
 * con transacción segura.
 */
router.post('/actualizar-masivo', async (req, res) => {
    const {
        sede_id,
        updates = [],
        update_margenes = true,
        update_precios = true,
        filter_old_margin = false
    } = req.body;

    if (!Array.isArray(updates) || updates.length === 0) {
        return res.status(400).json({
            success: false,
            message: 'Debe proporcionar al menos un tipo de precio para actualizar (updates).'
        });
    }

    if (!update_margenes && !update_precios) {
        return res.status(400).json({
            success: false,
            message: 'Debe marcar al menos una de las opciones: actualizar márgenes o recalcular precios.'
        });
    }

    const branchParam = sede_id || req.headers['x-branch-id'];
    const srv = resolveServer(branchParam);
    if (!srv) {
        return res.status(404).json({ success: false, message: 'Servidor o sucursal no encontrada.' });
    }

    const auditUser = req.profitUser || 'PROFIT';
    const startTime = Date.now();

    try {
        const pool = await getPool(srv.id, req.sqlAuth);
        const transaction = new sql.Transaction(pool);
        await transaction.begin();

        const results = [];
        let totalMargenesAfectados = 0;
        let totalPreciosAfectados = 0;

        try {
            for (const item of updates) {
                const coPrecioRaw = String(item.co_precio || '').trim();
                const nuevoMargen = Number(item.nuevo_margen);
                if (!coPrecioRaw || isNaN(nuevoMargen) || nuevoMargen < 0) {
                    continue;
                }

                const coPrecio = padProfit(coPrecioRaw, 6);
                let preciosModificados = 0;
                let margenesModificados = 0;

                // 1. RECALCULAR PRECIOS PRIMERO (mientras saArtMargen aún tiene el margen previo para despejar costo base)
                // Fórmula: Nuevo_Precio = (Precio_Actual / (1 + Margen_Actual / 100)) * (1 + Nuevo_Margen / 100)
                if (update_precios) {
                    const reqP = new sql.Request(transaction);
                    reqP.input('co_precio', sql.Char(6), coPrecio);
                    reqP.input('nuevo_margen', sql.Decimal(18, 5), nuevoMargen);
                    reqP.input('user', sql.Char(6), padProfit(auditUser, 6));

                    let filterClause = '';
                    if (filter_old_margin && item.margen_actual != null) {
                        reqP.input('margen_actual_filtro', sql.Decimal(18, 5), Number(item.margen_actual));
                        filterClause = ' AND (m.monto_max = @margen_actual_filtro OR m.monto_min = @margen_actual_filtro)';
                    }

                    const resP = await reqP.query(`
                        UPDATE p
                        SET p.monto    = ROUND((p.monto / (1.0 + (NULLIF(m.monto_max, 0) / 100.0))) * (1.0 + (@nuevo_margen / 100.0)), 2),
                            p.fe_us_mo = GETDATE(),
                            p.co_us_mo = @user
                        FROM saArtPrecio p
                        INNER JOIN saArtMargen m 
                            ON p.co_art = m.co_art 
                           AND LTRIM(RTRIM(p.co_precio)) = LTRIM(RTRIM(m.co_precio))
                        WHERE (LTRIM(RTRIM(p.co_precio)) = LTRIM(RTRIM(@co_precio)) OR p.co_precio = @co_precio)
                          AND p.monto > 0
                          AND m.monto_max > 0
                          ${filterClause};
                    `);
                    preciosModificados = resP.rowsAffected[0] || 0;
                }

                // 2. ACTUALIZAR MÁRGENES DE UTILIDAD EN saArtMargen
                if (update_margenes) {
                    const reqM = new sql.Request(transaction);
                    reqM.input('co_precio', sql.Char(6), coPrecio);
                    reqM.input('nuevo_margen', sql.Decimal(18, 5), nuevoMargen);
                    reqM.input('user', sql.Char(6), padProfit(auditUser, 6));

                    let filterClause = '';
                    if (filter_old_margin && item.margen_actual != null) {
                        reqM.input('margen_actual_filtro', sql.Decimal(18, 5), Number(item.margen_actual));
                        filterClause = ' AND (monto_max = @margen_actual_filtro OR monto_min = @margen_actual_filtro)';
                    }

                    const resM = await reqM.query(`
                        UPDATE saArtMargen
                        SET monto_min = @nuevo_margen,
                            monto_max = @nuevo_margen,
                            fe_us_mo  = GETDATE(),
                            co_us_mo  = @user
                        WHERE (LTRIM(RTRIM(co_precio)) = LTRIM(RTRIM(@co_precio)) OR co_precio = @co_precio)
                          AND monto_max > 0
                          ${filterClause};
                    `);
                    margenesModificados = resM.rowsAffected[0] || 0;
                }

                totalPreciosAfectados += preciosModificados;
                totalMargenesAfectados += margenesModificados;

                results.push({
                    co_precio: coPrecioRaw,
                    des_precio: item.des_precio || `Precio ${coPrecioRaw}`,
                    nuevo_margen: nuevoMargen,
                    margen_anterior: item.margen_actual ?? null,
                    articulos_precios_actualizados: preciosModificados,
                    articulos_margenes_actualizados: margenesModificados
                });
            }

            await transaction.commit();
            const elapsedMs = Date.now() - startTime;

            console.log(`✅ [MARGENES] Actualización masiva completada en sede ${srv.id}: ${totalMargenesAfectados} márgenes, ${totalPreciosAfectados} precios (${elapsedMs}ms).`);

            return res.status(200).json({
                success: true,
                message: 'Actualización masiva completada exitosamente.',
                data: {
                    sede_id: srv.id,
                    sede_nombre: srv.name || srv.nombre,
                    total_margenes_actualizados: totalMargenesAfectados,
                    total_precios_actualizados: totalPreciosAfectados,
                    results,
                    elapsed_ms: elapsedMs
                }
            });

        } catch (innerErr) {
            await transaction.rollback();
            throw innerErr;
        }

    } catch (error) {
        console.error('[MARGENES] Error en transacción de actualización masiva:', error);
        return res.status(500).json({
            success: false,
            message: 'Error al aplicar actualización masiva en Profit Plus.',
            error: error.message
        });
    }
});

module.exports = router;
