@echo off
REM ETL Pipeline - Flask Web Server Starter
REM This batch file starts the Flask web server

echo.
echo ╔════════════════════════════════════════════════════════════╗
echo ║      ETL Pipeline - Flask Web Server Starting...          ║
echo ╚════════════════════════════════════════════════════════════╝
echo.

REM Change to script directory
cd /d "%~dp0"

echo 📂 Current directory: %CD%
echo.

REM Try to find Python in common locations
set PYTHON_PATH=
if exist "C:\Python312\python.exe" set PYTHON_PATH=C:\Python312\python.exe
if exist "C:\Python311\python.exe" set PYTHON_PATH=C:\Python311\python.exe
if exist "C:\Python310\python.exe" set PYTHON_PATH=C:\Python310\python.exe
if exist "C:\Program Files\Python312\python.exe" set PYTHON_PATH=C:\Program Files\Python312\python.exe
if exist "C:\Program Files\Python311\python.exe" set PYTHON_PATH=C:\Program Files\Python311\python.exe
if exist "C:\Program Files (x86)\Python312\python.exe" set PYTHON_PATH=C:\Program Files (x86)\Python312\python.exe

REM If not found in common locations, try to use py launcher
if "%PYTHON_PATH%"=="" (
    echo 🔍 Searching for Python installation...
    for /f "tokens=*" %%A in ('where.exe python 2^>nul') do set PYTHON_PATH=%%A
)

REM If still not found, try Microsoft Store version explicitly
if "%PYTHON_PATH%"=="" (
    set PYTHON_PATH=python
)

echo ✅ Using Python: %PYTHON_PATH%
echo.

REM Check if requirements are installed
echo 📦 Checking dependencies...
%PYTHON_PATH% -m pip list 2>nul | find "flask" >nul
if errorlevel 1 (
    echo ⚠️  Flask not installed. Installing now...
    echo.
    %PYTHON_PATH% -m pip install -r requirement.txt
    echo.
)

echo.
echo 🚀 Starting Flask server...
echo.
echo 🌐 Web Interface: http://localhost:5000
echo 📡 API Ready for requests
echo.
echo ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
echo Press Ctrl+C to stop the server
echo ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
echo.

REM Start Flask server
%PYTHON_PATH% app.py

pause
