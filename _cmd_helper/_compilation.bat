@echo off
setlocal ENABLEDELAYEDEXPANSION

REM Go to the root directory
cd /d "%~dp0.."


REM ---- Choix de l'interpreteur Python ----
REM Priorite au venv du projet (cree par venv_create.bat), sinon le python du PATH.
REM On evite "python3" : sur Windows c'est souvent l'alias Microsoft Store
REM (WindowsApps\python3.exe), un interpreteur different de celui du PATH.
set "PYTHON_EXE="
if exist "venv\Scripts\python.exe" (
    set "PYTHON_EXE=venv\Scripts\python.exe"
) else (
    where python >nul 2>nul && set "PYTHON_EXE=python"
)

if "%PYTHON_EXE%"=="" (
    echo [ERREUR] Python n'est pas installe ou n'est pas dans le PATH.
    pause
    goto fin
)

echo [INFO] Python utilise : %PYTHON_EXE%
"%PYTHON_EXE%" --version


REM ---- Installer les dependances ----
"%PYTHON_EXE%" -m pip install --upgrade pip
"%PYTHON_EXE%" -m pip install -r requirements.txt
REM pyinstaller n'est pas dans requirements.txt : on l'installe explicitement.
"%PYTHON_EXE%" -m pip install pyinstaller

REM ---- Verification que PyInstaller est disponible ----
REM On l'appelle via "python -m PyInstaller" : pas besoin de localiser le .exe.
"%PYTHON_EXE%" -m PyInstaller --version >nul 2>nul
if not %ERRORLEVEL% equ 0 (
    echo [ERREUR] PyInstaller introuvable malgre l'installation. Abandon.
    pause
    goto fin
)
echo [OK] PyInstaller disponible.

REM ---- Verification de la configuration SMTP ----
if not exist ".smtp_config.py" (
    echo [INFO] Creation de .smtp_config.py a partir de .smtp_config.py.template...
    copy ".smtp_config.py.template" ".smtp_config.py"
)
REM ---- Verification de la configuration Mirth ----
if not exist ".mirth_config.py" (
    echo [INFO] Creation de .mirth_config.py a partir de .mirth_config.py.template...
    copy ".mirth_config.py.template" ".mirth_config.py"
)
REM ---- Compilation ----
echo.
echo [INFO] Compilation des scripts...
"%PYTHON_EXE%" -m PyInstaller --onefile system_state.py
"%PYTHON_EXE%" -m PyInstaller --onefile --add-data ".mirth_config.py;." mirth_api.py
"%PYTHON_EXE%" -m PyInstaller --onefile --add-data ".mirth_config.py;." mirth_simulator.py
"%PYTHON_EXE%" -m PyInstaller --onefile --add-data ".smtp_config.py;." quickmail.py
"%PYTHON_EXE%" -m PyInstaller --onefile --add-data ".smtp_config.py;." mirth_logs_parser.py
REM --collect-submodules rich : embarque TOUS les sous-modules de rich, dont
REM ceux importes paresseusement (rich._win32_console pour la console Windows
REM "legacy" d'un .exe lance par double-clic). Sans ca, l'analyse statique de
REM PyInstaller les rate : le thread de rendu de rich.live plante (ModuleNotFound)
REM et le tableau de bord reste un ecran noir. --add-data "web;web" embarque les
REM pages statiques (servies depuis sys._MEIPASS en mode gele).
REM --collect-submodules cryptography : embarque le backend TLS utilise pour
REM generer le certificat auto-signe au 1er lancement (HTTPS). Le certificat et la
REM base sont crees a COTE de l'exe (dossier persistant), pas dans _MEIPASS. La
REM config de securite (.mirth_config.py : HTTPS_MODE, ALLOWED_IPS, AUTH_ENABLED...)
REM est lue A COTE de l'exe (a deposer par l'exploitant), pas embarquee.
"%PYTHON_EXE%" -m PyInstaller --onefile --collect-submodules rich --collect-submodules cryptography --add-data ".smtp_config.py;." --add-data ".mirth_config.py;." --add-data "web;web" checker_service.py
REM superviseur : meta-superviseur des instances checker_service. Embarque ses
REM propres pages (web_superviseur) + la config SMTP (envoi par e-mail des mots de
REM passe generes pour ses propres comptes). --collect-submodules rich (tableau de
REM bord) et cryptography (HTTPS). La base superviseur.db et le certificat sont
REM crees a cote de l'exe au lancement ; la config de securite (.mirth_config.py)
REM est lue a cote de l'exe.
"%PYTHON_EXE%" -m PyInstaller --onefile --collect-submodules rich --collect-submodules cryptography --add-data ".smtp_config.py;." --add-data "web_superviseur;web_superviseur" superviseur.py
echo.
echo [OK] Compilation terminee !

REM ---- Copie des fichiers ----
REM copy .\dist\*.exe .\*.exe
copy .\dist\mirth_simulator.exe .\dist\mcservice.exe

pause

:fin
