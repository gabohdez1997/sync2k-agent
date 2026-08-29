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
