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

// POST /api/v1/impresion/probar — Probar conexión con la impresora (Térmica, Matricial o Fiscal)
router.post('/probar', async (req, res) => {
    const { ip, port, printer_type, serial_port } = req.body;
    const printerPort = parseInt(port || (printer_type === 'fiscal' ? '8088' : '9100'));

    if (!ip) {
        return res.status(400).json({ success: false, message: 'La IP de la impresora o equipo es requerida.' });
    }

    // Limpiar host si el usuario ingresó barras UNC (ej. \\caja02 o \\192.168.1.50\LX350)
    let cleanHost = ip.trim();
    let shareName = '';
    if (cleanHost.includes('\\') || cleanHost.includes('/')) {
        const parts = cleanHost.replace(/^[\\\/]+/, '').split(/[\\\/]/);
        cleanHost = parts[0];
        if (parts.length > 1) shareName = parts[1];
    }

    // 1. Caso Impresora Fiscal (Micro-servicio HTTP en PC de caja)
    if (printer_type === 'fiscal') {
        console.log(`[IMPRESION FISCAL] Probando micro-servicio fiscal en http://${cleanHost}:${printerPort}/status (Puerto Serial: ${serial_port || 'COM4'})...`);
        try {
            const controller = new AbortController();
            const timeoutId = setTimeout(() => controller.abort(), 3500);
            const httpRes = await fetch(`http://${cleanHost}:${printerPort}/status`, {
                method: 'GET',
                signal: controller.signal
            });
            clearTimeout(timeoutId);
            if (httpRes.ok) {
                const data = await httpRes.json();
                return res.status(200).json({
                    success: true,
                    message: `Micro-servicio fiscal en línea en ${cleanHost}:${printerPort}. Impresora: ${data.model || 'Tally Dascom'} (${data.port || serial_port || 'COM4'}).`,
                    data
                });
            } else {
                return res.status(200).json({
                    success: false,
                    message: `El micro-servicio fiscal en ${cleanHost}:${printerPort} respondió con código ${httpRes.status}.`
                });
            }
        } catch (err) {
            return res.status(200).json({
                success: false,
                message: `No se pudo conectar con el micro-servicio fiscal en http://${cleanHost}:${printerPort}. Verifique que esté iniciado en la PC de caja.`
            });
        }
    }

    // 2. Caso Impresora Matricial (Windows Compartida o Red Directa)
    if (printer_type === 'matrix_network') {
        console.log(`[IMPRESION MATRICIAL] Probando conexión a ${cleanHost}:${printerPort}...`);
        const socket = new net.Socket();
        socket.setTimeout(4000);

        socket.connect(printerPort, cleanHost, () => {
            console.log(`[IMPRESION MATRICIAL] Conexión establecida con ${cleanHost}:${printerPort}.`);
            socket.destroy();
            const typeLabel = printerPort === 445
                ? `PC '${cleanHost}' accesible en red (Recurso Windows SMB Compartido en puerto 445)`
                : `Impresora matricial en ${cleanHost}:${printerPort} responde correctamente`;
            res.status(200).json({ success: true, message: typeLabel });
        });

        socket.on('error', (err) => {
            console.error(`[IMPRESION MATRICIAL] Error conectando a ${cleanHost}:${printerPort}:`, err.message);
            socket.destroy();
            res.status(200).json({
                success: false,
                message: `No se pudo conectar a ${cleanHost}:${printerPort}. (${err.message}). Si usaste el nombre del equipo, prueba ingresando su Dirección IP directa (ej. 192.168.1.X).`
            });
        });

        socket.on('timeout', () => {
            console.error(`[IMPRESION MATRICIAL] Tiempo de espera agotado para ${cleanHost}:${printerPort}`);
            socket.destroy();
            res.status(200).json({ success: false, message: `Tiempo de espera agotado al conectar a ${cleanHost}:${printerPort}` });
        });
        return;
    }

    // 3. Caso Impresora Térmica (ESC/POS)
    console.log(`[IMPRESION TERMICA] Probando conexión a ${cleanHost}:${printerPort}...`);

    const socket = new net.Socket();
    socket.setTimeout(4000); // 4 seconds timeout

    socket.connect(printerPort, cleanHost, () => {
        console.log(`[IMPRESION TERMICA] Conexión establecida con ${cleanHost}:${printerPort}. Enviando inicialización...`);

        // Enviar inicialización y un texto corto de prueba
        const testPayload = CMD_INIT + CMD_CENTER + CMD_BOLD_ON +
            "PRUEBA DE CONEXION\n" +
            "SYNC2K / PROFIT PLUS\n" +
            CMD_BOLD_OFF +
            `IP: ${cleanHost}:${printerPort}\n` +
            new Date().toLocaleString() + "\n\n\n\n" +
            CMD_CUT;

        socket.write(testPayload, 'latin1', () => {
            socket.destroy();
            res.status(200).json({ success: true, message: 'Impresora responde correctamente.' });
        });
    });

    socket.on('error', (err) => {
        console.error(`[IMPRESION TERMICA] Error conectando a ${cleanHost}:${printerPort}:`, err.message);
        socket.destroy();
        res.status(200).json({ success: false, message: `No se pudo conectar a ${cleanHost}:${printerPort}: ${err.message}` });
    });

    socket.on('timeout', () => {
        console.error(`[IMPRESION TERMICA] Tiempo de espera agotado para ${cleanHost}:${printerPort}`);
        socket.destroy();
        res.status(200).json({ success: false, message: `Tiempo de espera agotado al conectar a ${cleanHost}:${printerPort}` });
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

// Helper para limpiar acentos, tildes y caracteres especiales para impresoras ESC/P matriciales y termicas
function cleanAscii(str) {
    if (!str) return '';
    return String(str)
        .normalize('NFD')
        .replace(/[\u0300-\u036f]/g, '') // Quita tildes / diacriticos
        .replace(/[ñ]/g, 'n')
        .replace(/[Ñ]/g, 'N')
        .replace(/[º°]/g, ' ')
        .replace(/[^\x20-\x7E\n\r\t\x1b\x1d\x0c\x0f\x12]/g, '');
}

// Helper para construir el flujo de bytes ESC/P de Media Pagina (5.5" / 33 lineas) segun formato original Galpe
function buildEscpNotaEntrega(doc) {
    const ESC = '\x1b';
    const INIT = ESC + '@';
    const PAGE_LEN_33 = ESC + 'C\x21'; // 33 lineas (5.5 pulgadas a 6 LPI)
    const CONDENSED_ON = '\x0f';
    const CONDENSED_OFF = '\x12';
    const BOLD_ON = ESC + 'E\x01';
    const BOLD_OFF = ESC + 'E\x00';
    const DRAFT = ESC + 'x\x00'; // Draft high speed
    const FORM_FEED = '\x0c';

    const W = 80; // 80 columnas estandar a 10 CPI

    let out = '';
    out += INIT + DRAFT + PAGE_LEN_33 + CONDENSED_OFF;

    // Linea 1: Empresa + RIF + Nro de Pedido
    const company = cleanAscii(doc.branch_name || 'Inversiones Galpe 2021 C.A.').toUpperCase();
    const rif = 'R.I.F.: ' + cleanAscii(doc.branch_rif || 'J-40175035-4').toUpperCase();
    const pedidoNum = doc.pedido_num || doc.origin_doc || doc.num_doc || '---';
    const pedidoStr = 'PEDIDO: ' + cleanAscii(pedidoNum).toUpperCase();
    
    // Distribuir en 3 columnas: [Empresa] [RIF] [PEDIDO]
    const leftPart = company.padEnd(30);
    const midPart = rif.padEnd(25);
    const rightPart = pedidoStr.padStart(25);
    out += BOLD_ON + leftPart + midPart + rightPart + BOLD_OFF + '\n';

    // Linea 2: Direccion Fiscal de la Empresa (Modo condensado para que quepa en 1 sola linea)
    const fiscalDir = cleanAscii(doc.branch_address || 'CTRA NACIONAL LOS GUAYOS GUACARA CRUCE CON CLL LISBOA Y CALLE PAMPERO LOCAL GALPON NRO 13-01 SECTOR LOS GUAYOS LOS GUAYOS CARABOBO').toUpperCase();
    out += CONDENSED_ON + fiscalDir.substring(0, 136) + CONDENSED_OFF + '\n';
    out += '-'.repeat(W) + '\n';

    // Bloque 3: Datos de Cliente (Izq) y Datos de Documento (Der)
    const isUSD = doc.is_usd !== false;
    const monedaStr = isUSD ? 'DOLAR' : 'BOLIVARES';
    const condStr = cleanAscii(doc.cond_des || doc.co_cond || 'CONTADO').toUpperCase();
    const vendStr = cleanAscii(doc.vendedor || '---').toUpperCase();
    const fechaStr = cleanAscii(doc.fecha_emision || doc.fecha || dayjs().format('DD/MM/YYYY')).toUpperCase();
    const docNumStr = cleanAscii(doc.doc_num || '00000000').toUpperCase();

    // Renglones combinados Izq (48 chars) / Der (32 chars)
    // L1: Cliente                                | NOTA DE ENTREGA
    const l1_left = ('Cliente:    ' + cleanAscii(doc.cli_des || 'CLIENTE DE CONTADO')).substring(0, 48).padEnd(48);
    const l1_right = BOLD_ON + 'NOTA DE ENTREGA'.padStart(32) + BOLD_OFF;
    out += l1_left + l1_right + '\n';

    // L2: R.I.F.                                 | 0000015925
    const l2_left = ('R.I.F.:     ' + cleanAscii(doc.rif || '---')).substring(0, 48).padEnd(48);
    const l2_right = BOLD_ON + docNumStr.padStart(32) + BOLD_OFF;
    out += l2_left + l2_right + '\n';

    // L3: Telefonos                              | CREDITO 15 DIAS / CONTADO
    const l3_left = ('Telefonos:  ' + cleanAscii(doc.telefonos || '---')).substring(0, 48).padEnd(48);
    const l3_right = condStr.padStart(32);
    out += l3_left + l3_right + '\n';

    // L4: Direccion                              | Fecha Emision: 30/05/2026
    const l4_left = ('Direccion:  ' + cleanAscii(doc.direc1 || '---')).substring(0, 48).padEnd(48);
    const l4_right = ('Fecha Emision: ' + fechaStr).padStart(32);
    out += l4_left + l4_right + '\n';

    // L5: Dir. Ent.                              | Vendedor: DANILUS GUTIERREZ
    const l5_left = ('Dir. Ent.:  ' + cleanAscii(doc.dir_entrega || doc.direc1 || '---')).substring(0, 48).padEnd(48);
    const l5_right = ('Vendedor: ' + vendStr.substring(0, 22)).padStart(32);
    out += l5_left + l5_right + '\n';

    // L6: Transporte                             | Moneda: DOLAR
    const l6_left = ('Transporte: ' + cleanAscii(doc.transporte || 'INTERNO')).substring(0, 48).padEnd(48);
    const l6_right = ('Moneda:   ' + monedaStr).padStart(32);
    out += l6_left + l6_right + '\n';

    out += '='.repeat(W) + '\n';

    // 4. Cabecera de Articulos: [Codigo (13)] [Descripcion (37)] [Cantidad (8)] [Precio (10)] [Neto (12)] = 80
    out += BOLD_ON;
    out += 'Codigo'.padEnd(13) + 
           'Descripcion'.padEnd(37) + 
           'Cantidad'.padStart(8) + ' ' + 
           'Precio'.padStart(9) + ' ' + 
           'Neto'.padStart(11) + '\n';
    out += BOLD_OFF;
    out += '-'.repeat(W) + '\n';

    // 5. Detalle de Renglones
    const items = doc.renglones || [];
    let totalCant = 0;
    items.forEach(it => {
        const cod = cleanAscii(String(it.co_art || '')).substring(0, 12).padEnd(13);
        const desc = cleanAscii(String(it.art_des || it.des_art || '')).substring(0, 36).padEnd(37);
        const cant = Number(it.cantidad || it.total_art || 0);
        totalCant += cant;
        const cantStr = cant.toFixed(2).replace('.', ',').padStart(8);
        const precVal = Number(it.precio || it.prec_vta || it.cost_unit || 0);
        const totVal = Number(it.total || it.reng_neto || (cant * precVal));
        const prec = precVal.toLocaleString('de-DE', { minimumFractionDigits: 2, maximumFractionDigits: 2 }).padStart(9);
        const tot = totVal.toLocaleString('de-DE', { minimumFractionDigits: 2, maximumFractionDigits: 2 }).padStart(11);

        out += cod + desc + cantStr + ' ' + prec + ' ' + tot + '\n';
    });

    out += '-'.repeat(W) + '\n';

    // 6. Pie de Documento y Totales
    const totalVal = Number(doc.total_neto || doc.total || doc.total_bruto || 0);
    const totalNeto = totalVal.toLocaleString('de-DE', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
    const tasa = Number(doc.tasa || 1);

    // L1 Pie: Condicion / Origen a la izq | Neto a la der
    const pieNota = condStr.includes('CREDITO') ? 'NOTA A CREDITO' : 'NOTA A CONTADO';
    const netoRight = BOLD_ON + 'Neto: ' + totalNeto.padStart(16) + BOLD_OFF;
    out += rowText(pieNota, netoRight, W) + '\n';

    // L2 Pie: Pagina 1 de 1 a la izq | SIN DERECHO A CREDITO FISCAL a la der
    const refBcv = (isUSD && tasa > 1) ? ` (REF. BCV: BS. ${(totalVal * tasa).toLocaleString('de-DE', { minimumFractionDigits: 2, maximumFractionDigits: 2 })})` : '';
    const legalText = 'SIN DERECHO A CREDITO FISCAL' + refBcv;
    out += rowText('Pagina 1 de 1', legalText, W) + '\n';

    // 7. Salto de media pagina fisica (5.5")
    out += FORM_FEED;

    return out;
}

// POST /api/v1/impresion/imprimir-nota-entrega — Enviar formato ESC/P a la impresora matricial
router.post('/imprimir-nota-entrega', async (req, res) => {
    const { printer, doc } = req.body;

    if (!doc) {
        return res.status(400).json({ success: false, message: 'Datos del documento requeridos.' });
    }

    console.log(`[IMPRESION ESC/P] Generando Nota de Entrega para N° ${doc.doc_num || 'PREVIEW'}...`);

    try {
        const escpData = buildEscpNotaEntrega(doc);

        const targetIp = (printer && printer.ip_address) ? printer.ip_address.trim() : '192.168.90.207';
        const targetPort = (printer && printer.port) ? parseInt(printer.port) : 445;
        const targetShare = (printer && printer.share_name) ? printer.share_name.trim() : 'EPSON LX-350 ESCP-1';

        // Si es Windows SMB Share (puerto 445 o contiene share_name)
        if (targetPort === 445 || targetShare) {
            const fs = require('fs');
            const path = require('path');
            const { exec } = require('child_process');

            const cleanHost = targetIp.replace(/^[\\\/]+/, '').split(/[\\\/]/)[0];
            const uncPath = `\\\\${cleanHost}\\${targetShare}`;
            const tempFile = path.join(__dirname, `temp_ne_${Date.now()}.prn`);

            fs.writeFileSync(tempFile, escpData, 'latin1');

            const cmd = `cmd.exe /c copy /b "${tempFile}" "${uncPath}"`;
            console.log(`[IMPRESION SMB] Ejecutando: ${cmd}`);

            exec(cmd, (error, stdout, stderr) => {
                try { fs.unlinkSync(tempFile); } catch (e) { }
                if (error) {
                    console.warn(`[IMPRESION SMB] Error en copy /b: ${error.message}. Intentando socket TCP...`);
                    // Fallback a socket TCP
                    sendViaSocket(cleanHost, 9100, escpData, res);
                } else {
                    console.log(`[IMPRESION SMB] Copiado exitoso a ${uncPath}`);
                    return res.status(200).json({
                        success: true,
                        message: `Nota de Entrega enviada a impresora matricial (${uncPath}).`,
                        stdout
                    });
                }
            });
            return;
        }

        // Si es TCP RAW directo (puerto 9100)
        sendViaSocket(targetIp, targetPort, escpData, res);

    } catch (err) {
        console.error('[IMPRESION NOTA ENTREGA ERROR]:', err);
        res.status(500).json({ success: false, message: 'Error procesando impresión de nota de entrega: ' + err.message });
    }
});

function sendViaSocket(ip, port, data, res) {
    const socket = new net.Socket();
    socket.setTimeout(5000);

    socket.connect(port, ip, () => {
        socket.write(data, 'latin1', () => {
            socket.destroy();
            res.status(200).json({ success: true, message: `Nota de Entrega enviada por red a ${ip}:${port}.` });
        });
    });

    socket.on('error', (err) => {
        socket.destroy();
        res.status(200).json({ success: false, message: `No se pudo conectar a la impresora ${ip}:${port} (${err.message}).` });
    });

    socket.on('timeout', () => {
        socket.destroy();
        res.status(200).json({ success: false, message: `Tiempo de espera agotado al conectar a ${ip}:${port}.` });
    });
}

module.exports = router;

