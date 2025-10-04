@echo off
setlocal enabledelayedexpansion
cd /d %~dp0

where node >nul 2>nul || (
  echo Node.js ist nicht installiert. https://nodejs.org/
  pause & exit /b 1
)

set "LOGDIR=logs"
if not exist "%LOGDIR%" mkdir "%LOGDIR%"
for /f "tokens=1-3 delims=:.," %%a in ("%time%") do set t=%%a%%b%%c
set "LOG=%LOGDIR%\server-%date:~-4%%date:~3,2%%date:~0,2%_%t%.log"
set "LOG=%LOG: =0%"

echo Syntaxcheck...
node --check server.js || (echo Syntaxfehler. Siehe oben. & pause & exit /b 1)

echo Starte WannCannaBis...
set "NODE_OPTIONS=--trace-uncaught --trace-warnings"
node server.js >> "%LOG%" 2>&1
echo Ende. Log: %LOG%
pause
