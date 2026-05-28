@echo off

:: Go to the root directory
cd /d "%~dp0.."

if not exist "venv\Scripts\activate.bat" (
    echo [INFO] Virtual environment not found at "%CD%\venv".
    echo [INFO] Launching venv_create.bat to initialize it...
    echo.
    call "%~dp0venv_create.bat"
    
    :: Re-check if the virtual environment was created successfully
    if not exist "venv\Scripts\activate.bat" (
        echo [ERROR] Failed to initialize virtual environment.
        pause
        exit /b 1
    )
)

:: Activate the environment in the current command process
call venv\Scripts\activate.bat
echo ===================================================
echo  Virtual Environment [venv] Loaded
echo ===================================================
	
:: Check if we should start an interactive shell
if "%~1" neq "--no-shell" (
    echo Type "deactivate" to exit the virtual environment.
    echo.
    cmd /k
)
