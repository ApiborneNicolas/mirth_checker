@echo off
cd /d "%~dp0.."

echo ===================================================
echo  Updating Dependencies (requirements.txt)
echo ===================================================
echo.

:: Call load script silently to set up environment
call "%~dp0venv_load.bat" --no-shell
if %errorlevel% neq 0 (
    exit /b %errorlevel%
)

:: Check if requirements.txt exists
if not exist "requirements.txt" (
    echo [WARNING] requirements.txt not found in the root directory.
    echo Creating an empty requirements.txt...
    echo. > requirements.txt
)

echo [INFO] Upgrading pip...
python -m pip install --upgrade pip

echo.
echo [INFO] Installing requirements...
python -m pip install -r requirements.txt

if %errorlevel% neq 0 (
    echo.
    echo [ERROR] Dependencies update failed.
    pause
    exit /b %errorlevel%
)

echo.
echo [OK] Dependencies updated successfully.
echo.
pause
