@echo off
setlocal enabledelayedexpansion

cd /d "%~dp0.."

:: Call load script silently to set up environment
call "%~dp0venv_load.bat" --no-shell
if %errorlevel% neq 0 (
    exit /b %errorlevel%
)

:: Check if a python script is specified in command arguments
if "%~1" neq "" (
    echo [INFO] Running: python %*
    echo.
    python %*
    echo.
    echo Code de retour [errorlevel] : !errorlevel!
    pause
    exit /b !errorlevel!
)

:menu
cls
echo ===================================================
echo  Select a Python Script to Launch
echo ===================================================
echo.

set count=0
for %%f in (*.py) do (
    set "filename=%%f"
    if "!filename:~0,1!" neq "." (
        set /a count+=1
        set "file_!count!=%%f"
        echo  [!count!] %%f
    )
)
echo  [q] Quitter
echo.

set "choice="
set /p choice="Saisissez le numero du script a lancer (1-%count%, q pour quitter) : "

if not defined choice (
    goto menu
)

if /i "%choice%"=="q" (
    goto end
)

if not defined file_%choice% (
    echo.
    echo [ERROR] Choix invalide.
    pause
    goto menu
)

:: Get selected script
set "selected_script=!file_%choice%!"

:: Launch with -h to display syntax
echo.
echo ===================================================
echo  Syntaxe d'utilisation ^(python !selected_script! -h^)
echo ===================================================
python "!selected_script!" -h
echo ===================================================
echo.

:: Prompt for parameters
set "params="
set /p params="Saisissez les parametres du script (laisser vide pour aucun) : "

:: Run python
echo.
echo [INFO] Execution de : python !selected_script! !params!
echo ===================================================
echo.

python "!selected_script!" !params!
set "EXIT_CODE=!errorlevel!"

echo.
echo Code de retour [errorlevel] : !EXIT_CODE!
pause
goto menu

:end