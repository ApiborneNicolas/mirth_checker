@echo off
setlocal enabledelayedexpansion

:: Get the directory of this script and go one level up to the root
cd /d "%~dp0.."

echo ===================================================
echo  Creating Python Virtual Environment (venv)
echo ===================================================
echo Target directory: %CD%\venv
echo.

:: Detect Python command
set "PY_CMD="
for %%P in (python py python3) do (
    %%P --version >nul 2>&1
    if !errorlevel! equ 0 (
        set "PY_CMD=%%P"
        goto :found_python
    )
)

:found_python
if "%PY_CMD%"=="" (
    echo [ERROR] Python not found. Please install Python and add it to your PATH.
    pause
    exit /b 1
)

echo [INFO] Using Python: %PY_CMD%
%PY_CMD% -m venv venv

if %errorlevel% neq 0 (
    echo.
    echo [ERROR] Failed to create the virtual environment.
    pause
    exit /b %errorlevel%
)

echo.
echo [OK] Virtual environment successfully created at: %CD%\venv
echo [OK] You can now install dependencies or run your scripts.
echo.
pause
