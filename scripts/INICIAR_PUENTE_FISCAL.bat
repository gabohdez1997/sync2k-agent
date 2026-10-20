<# :
@echo off
title SYNC2K - SERVICIO PUENTE FISCAL (32-BITS)
color 0B
setlocal
cd /d "%~dp0"

echo =====================================================================
echo       INICIANDO MICRO-SERVICIO FISCAL SYNC2K (32-BITS)
echo =====================================================================
echo.

set "PS32=%windir%\SysWOW64\WindowsPowerShell\v1.0\powershell.exe"
if not exist "%PS32%" set "PS32=powershell.exe"

"%PS32%" -NoExit -NoProfile -ExecutionPolicy Bypass -Command "& ([ScriptBlock]::Create((Get-Content -LiteralPath '%~f0' -Raw)))"

echo.
echo El servicio se ha detenido. Presione cualquier tecla para salir...
pause >nul
goto :eof
#>

# =========================================================================================
# SYNC2K - MICRO-SERVICIO PUENTE FISCAL (THE FACTORY HKA / TALLY DASCOM / BIXOLON)
# =========================================================================================
$PuertoHttp = 8088
$targetComDefault = "COM4"
$RutaDlls = "C:\PROFIT\ADMINISTRATIVO"

[Console]::OutputEncoding = [System.Text.Encoding]::UTF8

Write-Host ""
Write-Host "=====================================================================" -ForegroundColor Cyan
Write-Host "       SYNC2K - MICRO-SERVICIO PUENTE FISCAL (THE FACTORY HKA)       " -ForegroundColor Yellow
Write-Host "=====================================================================" -ForegroundColor Cyan
Write-Host " Modo: 32-Bits (x86) | Puerto HTTP: $PuertoHttp | Puerto Serial: $targetComDefault" -ForegroundColor Gray
Write-Host "=====================================================================" -ForegroundColor Cyan
Write-Host ""

# 1. Búsqueda y carga de TfhkaNet.dll
$candidatePaths = @(
    "C:\PROFIT\ADMINISTRATIVO\TfhkaNet.dll",
    "C:\PROFIT\ADMINISTRATIVO\TfhkaNet.DLL",
    "C:\Windows\SysWOW64\TfhkaNet.dll",
    (Join-Path (Get-Location) "TfhkaNet.dll"),
    (Join-Path (Get-Location) "TfhkaNet.DLL")
)

$dllPath = $null
foreach ($p in $candidatePaths) {
    if ($p -and (Test-Path $p)) { $dllPath = $p; break }
}

if (-not $dllPath) {
    Write-Host "ERROR: No se encontro TfhkaNet.dll en C:\PROFIT\ADMINISTRATIVO ni en la carpeta actual." -ForegroundColor Red
    Write-Host "Asegurate de colocar este archivo .bat dentro de C:\PROFIT\ADMINISTRATIVO." -ForegroundColor Yellow
    Write-Host "Presiona cualquier tecla para salir..." -ForegroundColor Gray
    return
}

try {
    Set-Location (Split-Path $dllPath)
    [System.Reflection.Assembly]::LoadFrom($dllPath) | Out-Null
    Write-Host "Ensamblado TfhkaNet.dll cargado exitosamente." -ForegroundColor Green
} catch {
    Write-Host "ERROR cargando TfhkaNet.dll: $($_.Exception.Message)" -ForegroundColor Red
    return
}

# 2. Iniciar Socket TCP en puerto 8088
try {
    $listener = New-Object System.Net.Sockets.TcpListener([System.Net.IPAddress]::Any, $PuertoHttp)
    $listener.Start()
    Write-Host ""
    Write-Host "============================================================" -ForegroundColor Cyan
    Write-Host " [SYNC2K FISCAL] Micro-servicio iniciado en puerto $PuertoHttp!" -ForegroundColor Green
    Write-Host " Escuchando peticiones de Sync2k Web / Profit Agente..." -ForegroundColor Yellow
    Write-Host " (Deja esta ventana abierta para mantener el servicio activo)" -ForegroundColor DarkGray
    Write-Host "============================================================" -ForegroundColor Cyan
    Write-Host ""
} catch {
    Write-Host "Error al iniciar el puerto $PuertoHttp - $($_.Exception.Message)" -ForegroundColor Red
    Write-Host "Es posible que el puerto ya este en uso o que otra ventana lo tenga abierto." -ForegroundColor Yellow
    return
}

# 3. Bucle de atención de peticiones
while ($true) {
    try {
        $client = $listener.AcceptTcpClient()
        $stream = $client.GetStream()
        
        $buffer = New-Object byte[] 16384
        $readCount = $stream.Read($buffer, 0, $buffer.Length)
        if ($readCount -le 0) { $client.Close(); continue }

        $requestText = [System.Text.Encoding]::UTF8.GetString($buffer, 0, $readCount)
        $firstLine = $requestText.Split("`n")[0].Trim()
        $parts = $firstLine.Split(" ")
        $method = $parts[0]
        $rawUrl = if ($parts.Length -gt 1) { $parts[1] } else { "/" }

        $timestamp = (Get-Date).ToString("HH:mm:ss")
        Write-Host "[$timestamp] Peticion: $method $rawUrl" -ForegroundColor Cyan

        # Extraer puerto COM si viene en URL
        $targetCom = $targetComDefault
        if ($rawUrl.Contains("port=")) {
            $match = [regex]::Match($rawUrl, "port=([^&]+)")
            if ($match.Success) { $targetCom = $match.Groups[1].Value }
        }

        $pathOnly = $rawUrl.Split("?")[0].ToLower()
        $respObj = @{ success = $false; message = "Ruta no encontrada" }

        # --- RESPUESTAS POR RUTA O PRUEBA ---
        if ($pathOnly -in @("/", "/status", "/health") -or $firstLine.Contains("PRUEBA")) {
            $printer = New-Object TfhkaNet.IF.VE.Tfhka
            $opened = $printer.OpenFpctrl($targetCom)
            if ($opened) {
                try {
                    $s1 = $printer.GetS1PrinterData()
                    $printer.CloseFpctrl()
                    $respObj = @{
                        success = $true
                        online = $true
                        port = $targetCom
                        model = "Tally Dascom 1140 (TFHKA)"
                        serial = $s1.NroRegistro
                        rif = $s1.Rif
                        ultimoDocFiscal = $s1.NroUltimoDocFiscal
                        ultimoReporteZ = $s1.NroUltimoReporteZ
                        message = "Impresora fiscal en linea en $targetCom"
                    }
                } catch {
                    $printer.CloseFpctrl()
                    $respObj = @{ success = $true; online = $true; port = $targetCom; model = "Tally Dascom 1140 (TFHKA)" }
                }
            } else {
                $respObj = @{ success = $false; online = $false; port = $targetCom; message = "Puerto $targetCom no responde." }
            }
        }
        elseif ($pathOnly -eq "/reporte-x") {
            $printer = New-Object TfhkaNet.IF.VE.Tfhka
            if ($printer.OpenFpctrl($targetCom)) {
                $r = $printer.SendCmd("I0X")
                $printer.CloseFpctrl()
                $respObj = @{ success = [bool]$r; message = "Reporte X emitido." }
            } else {
                $respObj = @{ success = $false; message = "No se pudo abrir $targetCom." }
            }
        }
        elseif ($pathOnly -eq "/reporte-z") {
            $printer = New-Object TfhkaNet.IF.VE.Tfhka
            if ($printer.OpenFpctrl($targetCom)) {
                $r = $printer.SendCmd("I0Z")
                $printer.CloseFpctrl()
                $respObj = @{ success = [bool]$r; message = "Reporte Z emitido." }
            } else {
                $respObj = @{ success = $false; message = "No se pudo abrir $targetCom." }
            }
        }
        elseif ($pathOnly -in @("/factura", "/imprimir", "/imprimir-fiscal")) {
            $printer = New-Object TfhkaNet.IF.VE.Tfhka
            if ($printer.OpenFpctrl($targetCom)) {
                try {
                    $bodyText = ""
                    $doubleCrlf = $requestText.IndexOf("`r`n`r`n")
                    if ($doubleCrlf -ge 0) {
                        $bodyText = $requestText.Substring($doubleCrlf + 4)
                    } else {
                        $doubleLf = $requestText.IndexOf("`n`n")
                        if ($doubleLf -ge 0) { $bodyText = $requestText.Substring($doubleLf + 2) }
                    }

                    $bodyJson = if ($bodyText) { $bodyText | ConvertFrom-Json } else { $null }
                    $inv = if ($bodyJson.invoice) { $bodyJson.invoice } else { $bodyJson }

                    # 1. Datos del Cliente
                    $cliDes = if ($inv.cli_des) { $inv.cli_des } else { "CLIENTE GENERAL" }
                    $rif = if ($inv.rif) { $inv.rif } else { "---" }
                    $direc = if ($inv.direc1) { $inv.direc1 } else { "VALENCIA" }
                    $telef = if ($inv.telefonos) { $inv.telefonos } else { "" }

                    $cliClean = [System.Text.RegularExpressions.Regex]::Replace($cliDes, "[^a-zA-Z0-9 .\/#\-]", "")
                    $rifClean = [System.Text.RegularExpressions.Regex]::Replace($rif, "[^a-zA-Z0-9\-]", "")
                    $dirClean = [System.Text.RegularExpressions.Regex]::Replace($direc, "[^a-zA-Z0-9 .\/#\-]", "")
                    $telClean = [System.Text.RegularExpressions.Regex]::Replace($telef, "[^a-zA-Z0-9 .\/#\-]", "")

                    $printer.SendCmd("i01Razon Social: " + $cliClean.Substring(0, [Math]::Min(35, $cliClean.Length))) | Out-Null
                    $printer.SendCmd("i02RIF/CI: " + $rifClean.Substring(0, [Math]::Min(20, $rifClean.Length))) | Out-Null
                    if ($dirClean) {
                        $printer.SendCmd("i03Direccion: " + $dirClean.Substring(0, [Math]::Min(35, $dirClean.Length))) | Out-Null
                    }
                    if ($telClean) {
                        $printer.SendCmd("i04Telefono: " + $telClean.Substring(0, [Math]::Min(20, $telClean.Length))) | Out-Null
                    }

                    # 2. Facturar Renglones
                    $lines = if ($inv.renglones) { $inv.renglones } else { @() }
                    $tasaCambio = if ($inv.tasa) { [double]$inv.tasa } else { 1.0 }
                    if ($tasaCambio -le 0) { $tasaCambio = 1.0 }

                    foreach ($item in $lines) {
                        $porcImp = [double]($item.porc_imp)
                        $tipoImp = if ($porcImp -gt 0 -and $item.tipo_imp -ne '0') { '"' } else { '!' }

                        $precioUSD = [double]($item.precio)
                        $precioBs = [Math]::Round($precioUSD * $tasaCambio, 2)
                        $cant = [double]($item.cantidad)

                        # Precio: 8 enteros + 2 decimales = 10 dígitos
                        $precioCentavos = [long][Math]::Round($precioBs * 100)
                        $precioStr = $precioCentavos.ToString().PadLeft(10, '0')

                        # Cantidad: 5 enteros + 3 decimales = 8 dígitos
                        $cantMili = [long][Math]::Round($cant * 1000)
                        $cantStr = $cantMili.ToString().PadLeft(8, '0')

                        # Descripción: hasta 35 chars
                        $artDes = [System.Text.RegularExpressions.Regex]::Replace($item.art_des, "[^a-zA-Z0-9 .\/#\-]", "")
                        $descStr = $artDes.Substring(0, [Math]::Min(35, $artDes.Length))

                        $cmdItem = "$tipoImp$precioStr$cantStr$descStr"
                        $printer.SendCmd($cmdItem) | Out-Null
                    }

                    # 3. Subtotal
                    $printer.SendCmd("3") | Out-Null

                    # 4. Totalizar y Cierre (101 = Pago Directo Efectivo / Cierre de Factura)
                    $resCierre = $printer.SendCmd("101")

                    # 5. Obtener datos del documento fiscal emitido
                    $s1 = $printer.GetS1PrinterData()
                    $printer.CloseFpctrl()

                    $respObj = @{
                        success = [bool]$resCierre
                        message = "Factura fiscal emitida exitosamente en impresora fiscal."
                        nroFacturaFiscal = $s1.NroUltimoDocFiscal
                        serial = $s1.NroRegistro
                    }
                } catch {
                    $printer.CloseFpctrl()
                    $respObj = @{ success = $false; message = "Error durante la emisión fiscal: $($_.Exception.Message)" }
                }
            } else {
                $respObj = @{ success = $false; message = "No se pudo abrir el puerto serial $targetCom de la impresora fiscal." }
            }
        }

        # Construir y enviar respuesta HTTP JSON
        $jsonStr = $respObj | ConvertTo-Json -Depth 5
        $jsonBytes = [System.Text.Encoding]::UTF8.GetBytes($jsonStr)

        $headers = @(
            "HTTP/1.1 200 OK",
            "Content-Type: application/json; charset=utf-8",
            "Access-Control-Allow-Origin: *",
            "Access-Control-Allow-Methods: GET, POST, OPTIONS",
            "Access-Control-Allow-Headers: *",
            "Content-Length: " + $jsonBytes.Length,
            "Connection: close",
            "",
            ""
        )
        $headerText = $headers -join "`r`n"
        $headerBytes = [System.Text.Encoding]::UTF8.GetBytes($headerText)

        $stream.Write($headerBytes, 0, $headerBytes.Length)
        $stream.Write($jsonBytes, 0, $jsonBytes.Length)
        $stream.Flush()
        $client.Close()

        $statusMsg = if ($respObj.success) { "Respuesta enviada: EXITOSA (True)" } else { "Respuesta enviada: (False)" }
        $color = if ($respObj.success) { "Green" } else { "Red" }
        Write-Host "   $statusMsg" -ForegroundColor $color
        Write-Host ""
    } catch {
        if ($client) { $client.Close() }
    }
}
