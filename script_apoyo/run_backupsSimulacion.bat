@echo off
setlocal

REM Carpeta del script 
set SCRIPT_DIR=%~dp0

REM Parámetros ajustables
set STEP_DELAY=15
set API_URL=http://localhost:8000
set STANZA=bases2-db
set DBNAME=bases2_proyectos
set NODE=pg-bases2

REM Ejecutar PowerShell con la política en bypass para evitarnos problemas 
powershell -NoProfile -ExecutionPolicy Bypass -File "%SCRIPT_DIR%backups_Simulacion.ps1" -StepDelaySeconds %STEP_DELAY% -ApiUrl %API_URL% -Stanza %STANZA% -DbName %DBNAME% -Node %NODE%

endlocal
pause
