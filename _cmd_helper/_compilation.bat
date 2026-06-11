@echo off
setlocal ENABLEDELAYEDEXPANSION

REM Go to the root directory
cd /d "%~dp0.."


REM ---- Détection de la version de Python installée ----
set "PYTHON_EXE="
where python3 >nul 2>nul
if %ERRORLEVEL% equ 0 (
    set "PYTHON_EXE=python3"
) else (
    REM Test de la commande python si python3 n'existe pas
    where python >nul 2>nul
    if %ERRORLEVEL% equ 0 (
        set "PYTHON_EXE=python"
    )
)

if "%PYTHON_EXE%"=="" (
    echo [ERREUR] Python n'est pas installe ou n'est pas dans le PATH.
    pause
    goto fin
)

echo [INFO] Python détecté et utilisé : %PYTHON_EXE%


REM 1. Récupère la ligne "Location:"
REM Récupère le chemin site-packages dans la variable LOCATION
for /f "usebackq delims=" %%L in (`powershell -NoLogo -NoProfile -Command "%PYTHON_EXE% -m pip show pyinstaller | Select-String '^LOCATION' | %% { $_.ToString().Split(':',2)[1].Trim() }"`) do (
    set "PKGROOT=%%L"
)

REM 2. Remonte d'un niveau pour être dans le dossier parent de site-packages
REM PKGROOT = ...\Python3xx\site-packages
REM On veut ...\Python3xx
for /f "usebackq delims=" %%P in (`powershell -NoLogo -NoProfile -Command "Split-Path -Parent '%PKGROOT%'"`) do (
    set "PKGROOT=%%P"
)

set "PYINSTALLER=%PKGROOT%\Scripts\pyinstaller.exe"

REM Affiche le résultat
echo PyInstaller devrait etre ici : "%PYINSTALLER%"
REM Vérifie l'existence
if exist "%PYINSTALLER%" (
    echo [OK] pyinstaller.exe trouve.
    %PYINSTALLER% --version
) else (
    echo [ERREUR] pyinstaller.exe introuvable. Installe-le avec :
    echo     %PYTHON_EXE% -m pip install pyinstaller
)


REM ---- Installer les dépendances (optionnel si déjà installées) ----
%PYTHON_EXE% -m pip install --upgrade pip
%PYTHON_EXE% -m pip install -r requirements.txt

REM ---- Verification de la configuration SMTP ----
if not exist ".smtp_config.py" (
    echo [INFO] Creation de .smtp_config.py a partir de .smtp_config.py.template...
    copy ".smtp_config.py.template" ".smtp_config.py"
)

REM ---- Compilation ----
echo.
echo [INFO] Compilation des scripts...
%PYINSTALLER%  --onefile system_state.py
%PYINSTALLER%  --onefile mirth_api.py
%PYINSTALLER%  --onefile --add-data ".smtp_config.py;." quickmail.py
%PYINSTALLER%  --onefile --add-data ".smtp_config.py;." mirth_logs_parser.py
%PYINSTALLER%  --onefile --add-data ".smtp_config.py;." checker_service.py
echo.
echo [OK] Compilation terminee !

REM ---- Copie des fichiers ----
REM copy .\dist\*.exe .\*.exe

pause

:fin
