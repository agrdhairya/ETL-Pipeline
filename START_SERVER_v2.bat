@echo off
REM ETL Pipeline - Flask Web Server Starter
REM Robust version that handles Python PATH issues

setlocal enabledelayedexpansion

echo.
echo ╔════════════════════════════════════════════════════════════╗
echo ║      ETL Pipeline - Flask Web Server Starting...          ║
echo ╚════════════════════════════════════════════════════════════╝
echo.

REM Change to script directory
cd /d "%~dp0"

echo 📂 Current directory: %CD%
echo.

REM ============================================================
REM Find Python installation (bypass Microsoft Store alias)
REM ============================================================

set PYTHON_EXE=
set PYTHON_FOUND=0

echo 🔍 Looking for Python installation...

REM Priority order: Check user AppData first, then Program Files
for %%P in (
    "%LOCALAPPDATA%\Programs\Python\Python312\python.exe"
    "%LOCALAPPDATA%\Programs\Python\Python311\python.exe"
    "%LOCALAPPDATA%\Programs\Python\Python310\python.exe"
    "%LOCALAPPDATA%\Programs\Python\Python39\python.exe"
    "C:\Program Files\Python312\python.exe"
    "C:\Program Files\Python311\python.exe"
    "C:\Program Files\Python310\python.exe"
    "C:\Program Files (x86)\Python312\python.exe"
    "C:\Program Files (x86)\Python311\python.exe"
    "C:\Program Files (x86)\Python310\python.exe"
    "C:\Python312\python.exe"
    "C:\Python311\python.exe"
    "C:\Python310\python.exe"
) do (
    if exist %%P (
        set "PYTHON_EXE=%%P"
        set PYTHON_FOUND=1
        echo ✅ Found Python at: !PYTHON_EXE!
        goto :python_found
    )
)

:python_found
if %PYTHON_FOUND%==0 (
    echo.
    echo ❌ ERROR: Python installation not found!
    echo.
    echo How to fix:
    echo   1. Download Python from: https://www.python.org/downloads/
    echo   2. Run the installer
    echo   3. ✓ Check "Add Python to PATH"
    echo   4. ✓ Check "Install for all users" (recommended)
    echo   5. Complete installation
    echo   6. Try again
    echo.
    echo Troubleshooting:
    echo   - If already installed, reinstall and check "Add to PATH"
    echo   - Try: Settings → Apps → App aliases → disable python.exe
    echo.
    pause
    exit /b 1
)

echo.

REM ============================================================
REM Check dependencies
REM ============================================================

echo 📦 Checking dependencies...
"!PYTHON_EXE!" -m pip list 2>nul | find "flask" >nul

if errorlevel 1 (
    echo ⚠️  Flask not installed. Installing now...
    echo.
    "!PYTHON_EXE!" -m pip install --upgrade pip
    echo.
    "!PYTHON_EXE!" -m pip install -r requirement.txt
    if errorlevel 1 (
        echo.
        echo ❌ Error installing dependencies!
        echo.
        pause
        exit /b 1
    )
    echo.
) else (
    echo ✅ All dependencies are installed
    echo.
)

REM ============================================================
REM Start Flask server
REM ============================================================

echo 🚀 Starting Flask server...
echo.
echo 🌐 Web Interface: http://localhost:5000
echo 📡 API Ready for requests
echo.
echo ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
echo ℹ️  Press Ctrl+C to stop the server
echo ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
echo.

"!PYTHON_EXE!" app.py

if errorlevel 1 (
    echo.
    echo ❌ Error starting Flask server!
    echo.
    pause
    exit /b 1
)

pause
