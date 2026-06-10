const express = require('express');
const router = express.Router();
const net = require('net');
const { sql, getPool, getServers } = require('../db');

// ESC/POS Commands
const ESC = '\x1b';
const GS = '\x1d';
const CMD_INIT = ESC + '@';
const CMD_CENTER = ESC + 'a\x01';
const CMD_LEFT = ESC + 'a\x00';
const CMD_RIGHT = ESC + 'a\x02';
const CMD_BOLD_ON = ESC + 'E\x01';
const CMD_BOLD_OFF = ESC + 'E\x00';
const CMD_CUT = GS + 'V\x42\x00'; // Paper Cut Command (full or partial)
const CMD_DOUBLE_SIZE = GS + '!\x11'; // Double height and double width
const CMD_NORMAL_SIZE = GS + '!\x00'; // Normal size

function centerText(text, width = 40) {
    if (text.length >= width) return text.substring(0, width);
    const leftPad = Math.floor((width - text.length) / 2);
    return ' '.repeat(leftPad) + text;
}

function rowText(left, right, width = 40) {
    const space = width - left.length - right.length;
    if (space <= 0) {
        return left.substring(0, width - right.length - 1) + ' ' + right;
    }
    return left + ' '.repeat(space) + right;
}

function centerCol(text, width) {
    text = (text || '').trim();
    if (text.length >= width) return text.substring(0, width);
    const leftPad = Math.floor((width - text.length) / 2);
    const rightPad = width - text.length - leftPad;
    return ' '.repeat(leftPad) + text + ' '.repeat(rightPad);
}

// POST /api/v1/impresion/probar — Probar conexión con la impresora
router.post('/probar', async (req, res) => {
    const { ip, port } = req.body;
    const printerPort = parseInt(port || '9100');

    if (!ip) {
        return res.status(400).json({ success: false, message: 'La IP de la impresora es requerida.' });
    }

    console.log(`[IMPRESION] Probando conexión a ${ip}:${printerPort}...`);

    const socket = new net.Socket();
    socket.setTimeout(4000); // 4 seconds timeout

    socket.connect(printerPort, ip, () => {
        console.log(`[IMPRESION] Conexión establecida con ${ip}:${printerPort}. Enviando inicialización...`);

        // Enviar inicialización y un texto corto de prueba
        const testPayload = CMD_INIT + CMD_CENTER + CMD_BOLD_ON +
            "PRUEBA DE CONEXION\n" +
            "SYNC2K / PROFIT PLUS\n" +
            CMD_BOLD_OFF +
            `IP: ${ip}:${printerPort}\n` +
            new Date().toLocaleString() + "\n\n\n\n" +
            CMD_CUT;

        socket.write(testPayload, 'latin1', () => {
            socket.destroy();
            res.status(200).json({ success: true, message: 'Impresora responde correctamente.' });
        });
    });

    socket.on('error', (err) => {
        console.error(`[IMPRESION] Error conectando a ${ip}:${printerPort}:`, err.message);
        socket.destroy();
        res.status(200).json({ success: false, message: `No se pudo conectar a ${ip}:${printerPort}: ${err.message}` });
    });

    socket.on('timeout', () => {
        console.error(`[IMPRESION] Tiempo de espera agotado para ${ip}:${printerPort}`);
        socket.destroy();
        res.status(200).json({ success: false, message: `Tiempo de espera agotado al conectar a ${ip}:${printerPort}` });
    });
});

// POST /api/v1/impresion/imprimir — Enviar ticket de facturación
router.post('/imprimir', async (req, res) => {
    const { ip, port, invoice, sede } = req.body;
    const printerPort = parseInt(port || '9100');

    if (!ip || !invoice) {
        return res.status(400).json({ success: false, message: 'Faltan parámetros (ip, invoice).' });
    }

    console.log(`[IMPRESION] Imprimiendo ticket de pre-despacho del pedido ${invoice.doc_num} en ${ip}:${printerPort}...`);

    try {
        const width = 42; // Ancho estándar para tickets de 80mm

        // 1. Obtener ubicaciones de la base de datos de la sede
        let pool = null;
        try {
            const servers = getServers();
            let srv = null;
            if (sede) {
                srv = servers.find(s => s.id === sede) ||
                    servers.find(s => s.name.trim().toLowerCase() === sede.trim().toLowerCase());
            }
            if (!srv && servers.length > 0) {
                srv = servers[0];
            }
            if (srv) {
                pool = await getPool(srv.id, req.sqlAuth);
            }
        } catch (dbErr) {
            console.warn(`[IMPRESION] Error al conectar con la base de datos para buscar ubicaciones:`, dbErr.message);
        }

        if (pool && invoice.renglones) {
            for (const item of invoice.renglones) {
                try {
                    const dbRes = await pool.request()
                        .input('co_art', sql.Char(30), item.co_art)
                        .query(`
                            SELECT DISTINCT RTRIM(au.co_ubicacion) AS co_ubicacion,
                                            RTRIM(au.co_ubicacion2) AS co_ubicacion2,
                                            RTRIM(au.co_ubicacion3) AS co_ubicacion3
                            FROM saArtUbicacion au
                            WHERE LTRIM(RTRIM(au.co_art)) = LTRIM(RTRIM(@co_art))
                        `);

                    if (dbRes.recordset && dbRes.recordset.length > 0) {
                        const locations = new Set();
                        dbRes.recordset.forEach(row => {
                            if (row.co_ubicacion && row.co_ubicacion.trim()) locations.add(row.co_ubicacion.trim());
                            if (row.co_ubicacion2 && row.co_ubicacion2.trim()) locations.add(row.co_ubicacion2.trim());
                            if (row.co_ubicacion3 && row.co_ubicacion3.trim()) locations.add(row.co_ubicacion3.trim());
                        });
                        item.locations = Array.from(locations).join(', ') || '---';
                    } else {
                        item.locations = '---';
                    }
                } catch (err) {
                    console.warn(`[IMPRESION] Error al buscar ubicaciones para artículo ${item.co_art}:`, err.message);
                    item.locations = '---';
                }
            }
        }

        let t = "";

        // 1. Inicialización y Encabezado
        t += CMD_INIT;
        t += CMD_CENTER;
        t += CMD_BOLD_ON;
        t += CMD_DOUBLE_SIZE;
        t += `${(invoice.branch_name || 'INVERSIONES GALPE').toUpperCase()}\n`;
        t += CMD_NORMAL_SIZE;
        t += CMD_BOLD_OFF;
        t += `RIF: ${invoice.branch_rif || 'J-00000000-0'}\n`;
        t += "-".repeat(width) + "\n";

        // TÍTULO TICKET DE PRE-DESPACHO
        t += CMD_CENTER;
        t += CMD_BOLD_ON;
        t += "TICKET DE PRE-DESPACHO\n";
        t += CMD_BOLD_OFF;
        t += CMD_LEFT;
        t += "-".repeat(width) + "\n";

        // 2. Información del Cliente
        const d = new Date();
        const formattedDate = `${String(d.getDate()).padStart(2, '0')}/${String(d.getMonth() + 1).padStart(2, '0')}/${d.getFullYear()}`;
        const hours = d.getHours();
        const ampm = hours >= 12 ? 'PM' : 'AM';
        const displayHours = hours % 12 || 12;
        const formattedTime = `${String(displayHours).padStart(2, '0')}:${String(d.getMinutes()).padStart(2, '0')}:${String(d.getSeconds()).padStart(2, '0')} ${ampm}`;
        t += `Cliente:   ${invoice.cli_des.toUpperCase()}\n`;
        t += `R.I.F.:    ${invoice.rif || '---'}\n`;
        t += `Fecha:     ${formattedDate}  ${formattedTime}\n`;
        t += `Origen:    ${invoice.invoice_num ? 'FACTURA Nro. ' + invoice.invoice_num : 'PEDIDO Nro. ' + invoice.doc_num}\n`;
        if (invoice.vendedor) {
            t += `Vendedor:  ${invoice.vendedor.toUpperCase()}\n`;
        }
        t += "-".repeat(width) + "\n";

        // 3. Cabecera de Artículos (Descripcion = 22 chars, Cantidad = 10 chars, Ubicacion = 10 chars)
        t += CMD_BOLD_ON;
        const hDesc = centerCol("Descripcion", 22);
        const hQty = centerCol("Cantidad", 10);
        const hLoc = centerCol("Ubicacion", 10);
        t += hDesc + hQty + hLoc + "\n";
        t += CMD_BOLD_OFF;
        t += "-".repeat(width) + "\n";

        // 4. Renglones (Código + Ubicación en línea 1, Descripción + Cantidad en siguientes líneas)
        const items = invoice.renglones || [];
        items.forEach((item, idx) => {
            if (idx > 0) {
                t += "\n"; // Salto de línea entre renglones para separarlos
            }

            const descLines = [];
            let remainingDesc = (item.art_des || item.co_art || '').trim();
            while (remainingDesc.length > 0) {
                descLines.push(remainingDesc.substring(0, 22));
                remainingDesc = remainingDesc.substring(22);
            }
            if (descLines.length === 0) descLines.push('');

            const locs = (item.locations || '---').split(',').map(l => l.trim()).filter(Boolean);
            if (locs.length === 0) locs.push('---');

            // Línea 1: Código (centrado en 22 chars) + Cantidad vacía (10 chars) + Primera ubicación (centrada en 10 chars)
            const codePart = centerCol(item.co_art, 22);
            const qPart1 = " ".repeat(10);
            const lPart1 = centerCol(locs[0] || '---', 10);
            t += codePart + qPart1 + lPart1 + "\n";

            // Línea 2 y siguientes: Descripción + Cantidad (en la primera de descripción) + Resto de ubicaciones
            const totalLines = Math.max(descLines.length, locs.length - 1);
            for (let i = 0; i < totalLines; i++) {
                const dPart = centerCol(descLines[i] || '', 22);

                // Cantidad en la primera línea de la descripción (i === 0)
                const qVal = i === 0 ? Number(item.cantidad || 0).toFixed(2) : '';
                const qPart = centerCol(qVal, 10);

                // Ubicaciones adicionales
                const lPart = centerCol(locs[i + 1] || '', 10);

                t += dPart + qPart + lPart + "\n";
            }
        });
        t += "-".repeat(width) + "\n";

        // 5. Pie de Ticket: Código de pre-despacho grande en doble tamaño
        t += "\n";
        t += CMD_CENTER;
        t += CMD_BOLD_ON;
        t += CMD_DOUBLE_SIZE;
        t += `${invoice.invoice_num || invoice.doc_num}\n`;
        t += CMD_NORMAL_SIZE;
        t += CMD_BOLD_OFF;
        t += "\n\n\n";
        t += CMD_CUT; // Cortar papel

        // 6. Enviar por Socket TCP a la impresora de red
        const socket = new net.Socket();
        socket.setTimeout(5000);

        socket.connect(printerPort, ip, () => {
            console.log(`[IMPRESION] Conectado a la impresora en ${ip}:${printerPort}. Enviando ticket de pre-despacho...`);
            socket.write(t, 'latin1', () => {
                socket.destroy();
                res.status(200).json({ success: true, message: 'Ticket de pre-despacho enviado a la impresora exitosamente.' });
            });
        });

        socket.on('error', (err) => {
            console.error(`[IMPRESION] Error enviando ticket a ${ip}:${printerPort}:`, err.message);
            socket.destroy();
            res.status(200).json({ success: false, message: `Error al conectar con la impresora: ${err.message}` });
        });

        socket.on('timeout', () => {
            console.error(`[IMPRESION] Timeout enviando ticket a ${ip}:${printerPort}`);
            socket.destroy();
            res.status(200).json({ success: false, message: 'Tiempo de espera agotado al conectar a la impresora.' });
        });

    } catch (err) {
        console.error('[IMPRESION EXCEPTION]:', err);
        res.status(500).json({ success: false, message: 'Error interno en el módulo de impresión.', error: err.message });
    }
});

module.exports = router;
