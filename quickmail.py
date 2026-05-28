#!/usr/bin/env python3
import argparse
import smtplib
import ssl
from email.utils import formatdate, make_msgid
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import sys

# =====================================================================
# CONFIGURATION DU SERVEUR SMTP (À modifier avec tes identifiants)
# =====================================================================
SMTP_SERVER = "smtp.free.fr"
SMTP_PORT = 587  # 465 pour SSL (recommandé), ou 587 pour TLS
USE_SSL = False   # Mettre à False si tu utilises le port 587 (TLS)
SMTP_USER = "ron06"
SMTP_PASSWORD = "***PASSWORD***"
SENDER_ADDRESS = "ron06@free.fr"


def sendmail(sujet: str, message: str, dest: str) -> bool:
    """Envoie un email en utilisant la configuration SMTP globale."""
    # Construction du message
    msg = MIMEMultipart()
    msg["From"] = SENDER_ADDRESS
    msg["To"] = dest
    msg["Subject"] = sujet

    # --- AJOUT DES EN-TÊTES DE SÉCURITÉ REQUIS ---
    msg["Date"] = formatdate(
        localtime=True
    )  # Ajoute la date et l'heure actuelle
    msg["Message-ID"] = make_msgid()  # Génère un identifiant unique pour le mail
    # ---------------------------------------------

    msg.attach(MIMEText(message, "plain", "utf-8"))

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