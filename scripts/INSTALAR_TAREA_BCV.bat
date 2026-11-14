@echo off
setlocal EnableExtensions

for %%I in ("%~dp0..") do set "AGENT_DIR=%%~fI"
set "SCRIPT_PATH=%AGENT_DIR%\scripts\sync_bcv_rate.js"

if not exist "%SCRIPT_PATH%" (
    echo [ERROR] No se encontro el script: "%SCRIPT_PATH%"
    exit /b 1
)

set "NODE_EXE="
for /f "delims=" %%N in ('where node 2^>nul') do (
    if not defined NODE_EXE set "NODE_EXE=%%~fN"
)
if not defined NODE_EXE if exist "%ProgramFiles%\nodejs\node.exe" set "NODE_EXE=%ProgramFiles%\nodejs\node.exe"
if not defined NODE_EXE if exist "%ProgramFiles(x86)%\nodejs\node.exe" set "NODE_EXE=%ProgramFiles(x86)%\nodejs\node.exe"

if not defined NODE_EXE (
    echo [ERROR] No se encontro node.exe en el sistema.
    exit /b 1
)

echo Creando tarea programada "Sync2k_BCV_Tasa_6AM"...
echo   Node:   "%NODE_EXE%"
echo   Script: "%SCRIPT_PATH%"
echo   Hora:   06:00 AM diaria

schtasks /Create /TN "Sync2k_BCV_Tasa_6AM" /TR "\"%NODE_EXE%\" \"%SCRIPT_PATH%\"" /SC DAILY /ST 06:00 /F
if errorlevel 1 (
    echo.
    echo [ERROR] No se pudo crear la tarea programada. Intenta ejecutar este script como Administrador.
    exit /b 1
)

echo.
echo [OK] Tarea "Sync2k_BCV_Tasa_6AM" configurada correctamente.
exit /b 0
