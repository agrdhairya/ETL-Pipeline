@echo off
REM Find and run Python - Bypasses Microsoft Store alias issues

setlocal enabledelayedexpansion

REM Search for Python in common installation paths
echo Searching for Python installation...
echo.

set PYTHON_FOUND=0
set PYTHON_EXE=

REM Check common installation locations
for %%P in (
    "C:\Python312\python.exe"
    "C:\Python311\python.exe"
    "C:\Python310\python.exe"
    "C:\Python39\python.exe"
    "C:\Program Files\Python312\python.exe"
    "C:\Program Files\Python311\python.exe"
    "C:\Program Files\Python310\python.exe"
    "C:\Program Files (x86)\Python312\python.exe"
    "C:\Program Files (x86)\Python311\python.exe"
    "%LOCALAPPDATA%\Programs\Python\Python312\python.exe"
    "%LOCALAPPDATA%\Programs\Python\Python311\python.exe"
    "%LOCALAPPDATA%\Programs\Python\Python310\python.exe"
) do (
    if exist %%P (
        set "PYTHON_EXE=%%P"
        set PYTHON_FOUND=1
        echo Found Python at: !PYTHON_EXE!
        echo.
        goto :found
    )
)

:found
if %PYTHON_FOUND%==0 (
    echo.
    echo ❌ ERROR: Python not found!
    echo.
    echo Please install Python from: https://www.python.org/downloads/
    echo.
    echo IMPORTANT: During installation, check:
    echo  ✓ "Add Python to PATH"
    echo.
    pause
    exit /b 1
)

REM Now run Python with the found executable
echo.
echo Running: !PYTHON_EXE! %*
echo.
"!PYTHON_EXE!" %*
