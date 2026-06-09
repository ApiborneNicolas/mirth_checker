#!/usr/bin/env python3
import argparse
import smtplib
import ssl
from email.utils import formatdate, make_msgid
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import sys

import os
import importlib.util

# =====================================================================
# CONFIGURATION DYNAMIQUE DU SERVEUR SMTP
# =====================================================================
_cfg_server = "smtp.free.fr"
_cfg_port = 587
_cfg_ssl = False
_cfg_user = ""
_cfg_pass = ""
_cfg_sender = ""

# Trouver le dossier où se trouve le script ou le fichier compilé par PyInstaller
if getattr(sys, 'frozen', False):
    # PyInstaller décompresse tous les fichiers inclus dans sys._MEIPASS à l'exécution
    base_dir = sys._MEIPASS
else:
    base_dir = os.path.dirname(os.path.abspath(__file__))

config_file_path = os.path.join(base_dir, ".smtp_config.py")

if os.path.exists(config_file_path):
    try:
        spec = importlib.util.spec_from_file_location("smtp_config_dot", config_file_path)
        smtp_config_dot = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(smtp_config_dot)
        
        _cfg_server = getattr(smtp_config_dot, "SMTP_SERVER", _cfg_server)
        _cfg_port = int(getattr(smtp_config_dot, "SMTP_PORT", _cfg_port))
        
        _cfg_ssl_val = getattr(smtp_config_dot, "USE_SSL", _cfg_ssl)
        if isinstance(_cfg_ssl_val, bool):
            _cfg_ssl = _cfg_ssl_val
        else:
            _cfg_ssl = str(_cfg_ssl_val).lower() in ("true", "1", "yes")
            
        _cfg_user = getattr(smtp_config_dot, "SMTP_USER", _cfg_user)
        _cfg_pass = getattr(smtp_config_dot, "SMTP_PASSWORD", _cfg_pass)
        _cfg_sender = getattr(smtp_config_dot, "SENDER_ADDRESS", _cfg_sender)
    except Exception as e:
        print(f"[Avertissement] Impossible de charger la configuration depuis {config_file_path} : {e}", file=sys.stderr)

# Priorité aux variables d'environnement, sinon valeurs de smtp_config
SMTP_SERVER = os.environ.get("SMTP_SERVER", _cfg_server)

try:
    SMTP_PORT = int(os.environ.get("SMTP_PORT", _cfg_port))
except ValueError:
    SMTP_PORT = 587

if "USE_SSL" in os.environ:
    USE_SSL = os.environ["USE_SSL"].lower() in ("true", "1", "yes")
else:
    USE_SSL = _cfg_ssl

SMTP_USER = os.environ.get("SMTP_USER", _cfg_user)
SMTP_PASSWORD = os.environ.get("SMTP_PASSWORD", _cfg_pass)
SENDER_ADDRESS = os.environ.get("SENDER_ADDRESS", _cfg_sender)



def sendmail(sujet: str, message: str, dest: str, html: str = None, attachment_name: str = None, attachment_content: str = None) -> bool:
    """Envoie un email en utilisant la configuration SMTP globale."""
    if not SMTP_USER or not SMTP_PASSWORD:
        print("[Erreur SMTP] Identifiants SMTP (SMTP_USER / SMTP_PASSWORD) manquants dans la configuration.", file=sys.stderr)
        return False
    # Construction du message - mixed car contient potentiellement une pièce jointe
    msg = MIMEMultipart("mixed")
    msg["From"] = SENDER_ADDRESS
    msg["To"] = dest
    msg["Subject"] = sujet

    # --- AJOUT DES EN-TÊTES DE SÉCURITÉ REQUIS ---
    msg["Date"] = formatdate(
        localtime=True
    )  # Ajoute la date et l'heure actuelle
    msg["Message-ID"] = make_msgid()  # Génère un identifiant unique pour le mail
    # ---------------------------------------------

    # Corps de l'e-mail (texte alternatif plain/html si html fourni)
    if html:
        body_part = MIMEMultipart("alternative")
        body_part.attach(MIMEText(message, "plain", "utf-8"))
        body_part.attach(MIMEText(html, "html", "utf-8"))
        msg.attach(body_part)
    else:
        msg.attach(MIMEText(message, "plain", "utf-8"))

    # Pièce jointe
    if attachment_name and attachment_content:
        attachment_part = MIMEText(attachment_content, "plain", "utf-8")
        attachment_part.add_header(
            "Content-Disposition",
            "attachment",
            filename=attachment_name
        )
        msg.attach(attachment_part)

    try:
        if USE_SSL:
            context = ssl.create_default_context()
            with smtplib.SMTP_SSL(
                SMTP_SERVER, SMTP_PORT, context=context
            ) as server:
                server.login(SMTP_USER, SMTP_PASSWORD)
                server.sendmail(SENDER_ADDRESS, dest, msg.as_string())
        else:
            with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
                if SMTP_PORT == 587:
                    context = ssl.create_default_context()
                    server.starttls(context=context)
                server.login(SMTP_USER, SMTP_PASSWORD)
                server.sendmail(SENDER_ADDRESS, dest, msg.as_string())

        return True

    except Exception as e:
        print(f"[Erreur SMTP] {e}", file=sys.stderr)
        return False


# =====================================================================
# MODE CLI (S'exécute uniquement si le script est lancé directement)
# =====================================================================
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Envoi rapide d'un email via la ligne de commande."
    )

    # Définition des arguments requis
    parser.add_argument(
        "-s", "--sujet", required=True, help="Sujet de l'email"
    )
    parser.add_argument(
        "-m", "--message", required=True, help="Contenu du message"
    )
    parser.add_argument(
        "-d", "--dest", required=True, help="Adresse email du destinataire"
    )

    args = parser.parse_args()

    # Tentative d'envoi
    success = sendmail(sujet=args.sujet, message=args.message, dest=args.dest)

    # Affichage simple demandé
    if success:
        print("Send mail -> OK")
        sys.exit(0)
    else:
        print("Send mail -> KO")
        sys.exit(1)