@echo off
REM ==========================================================================
REM Génère un certificat TLS auto-signé pour l'interface web (HTTPS).
REM
REM En temps normal, le service génère lui-même le certificat au 1er démarrage
REM (via la bibliothèque Python "cryptography"). Ce script sert à le produire
REM À L'AVANCE, par exemple pour le déposer à côté d'un .exe compilé.
REM
REM Stratégie : OpenSSL s'il est présent, SINON repli automatique sur Python +
REM "cryptography" (venv du projet en priorité). Produit checker_cert.pem /
REM checker_key.pem à la racine du projet.
REM ==========================================================================
setlocal
cd /d "%~dp0.."

set "CERT=checker_cert.pem"
set "KEY=checker_key.pem"

REM ---- 1) OpenSSL si disponible ----
where openssl >nul 2>nul
if %ERRORLEVEL% equ 0 (
    echo [INFO] Generation via OpenSSL pour %COMPUTERNAME%...
    openssl req -x509 -newkey rsa:2048 -nodes ^
        -keyout "%KEY%" -out "%CERT%" -days 3650 ^
        -subj "/CN=%COMPUTERNAME%/O=Mirth_checker" ^
        -addext "subjectAltName=DNS:%COMPUTERNAME%,DNS:localhost,IP:127.0.0.1"
    if %ERRORLEVEL% equ 0 ( goto ok ) else ( echo [ERREUR] Echec OpenSSL. & goto fin )
)

REM ---- 2) Repli : Python + cryptography ----
echo [INFO] OpenSSL introuvable — repli sur Python + "cryptography".
set "PYTHON_EXE="
if exist "venv\Scripts\python.exe" (
    set "PYTHON_EXE=venv\Scripts\python.exe"
) else (
    where python >nul 2>nul && set "PYTHON_EXE=python"
)
if "%PYTHON_EXE%"=="" (
    echo [ERREUR] Ni OpenSSL ni Python trouves. Installez l'un des deux
    echo          ^(ou laissez le service generer le certificat au demarrage^).
    goto fin
)

"%PYTHON_EXE%" -c "import sys; sys.path.insert(0, '.'); from lib import tls; tls.ensure_self_signed_cert('%CERT%', '%KEY%'); print('[OK] Certificat genere via cryptography.')"
if %ERRORLEVEL% equ 0 ( goto ok ) else (
    echo [ERREUR] Echec de la generation via cryptography.
    echo          Installez-la : "%PYTHON_EXE%" -m pip install cryptography
    goto fin
)

:ok
echo.
echo [OK] Certificat pret : %CERT% / %KEY%
echo      Referencez-les via HTTPS_CERT / HTTPS_KEY dans .mirth_config.py,
echo      ou placez-les a cote de l'exe (ils seront reutilises tels quels).

:fin
pause
