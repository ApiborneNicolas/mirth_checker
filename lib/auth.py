# -*- coding: utf-8 -*-
"""
Cœur de l'authentification du service (sans I/O réseau).

Regroupe :
- le hachage des mots de passe (PBKDF2-HMAC-SHA256, sel aléatoire) et leur
  vérification en temps constant ;
- la génération de mots de passe aléatoires complexes (jamais stockés en clair,
  transmis une seule fois par e-mail) ;
- les jetons de session : génération, empreinte (seul le HASH est stocké en base),
  création / résolution avec fenêtre glissante 24 h / révocation ;
- la vérification des clés API (clients machine, ex. superviseur).

Tout tient en bibliothèque standard (`hashlib`, `hmac`, `secrets`). L'accès à la
base passe par `lib.database` (tables `users` / `web_sessions` / `api_keys`).
"""

import hmac
import string
import hashlib
import secrets
import datetime

from . import database

# Durée de la session glissante (heures). Réglable au démarrage par le service
# via set_session_ttl_hours() (argument/config SESSION_TTL_H).
SESSION_TTL_HOURS = 24

# Nombre d'itérations PBKDF2 (coût du hachage du mot de passe).
_PBKDF2_ITERATIONS = 200_000
# En-dessous de ce délai (s) depuis la dernière glisse, on NE réécrit pas
# l'échéance en base (évite une écriture SQLite à chaque requête authentifiée).
_SLIDE_THROTTLE_SECONDS = 300

_TS_FMT = "%Y-%m-%d %H:%M:%S"


def set_session_ttl_hours(hours):
    """Fixe la durée de la fenêtre de session glissante (heures)."""
    global SESSION_TTL_HOURS
    try:
        SESSION_TTL_HOURS = max(1, int(hours))
    except (TypeError, ValueError):
        pass


# --------------------------------------------------------------------------
# MOTS DE PASSE
# --------------------------------------------------------------------------
def hash_password(password):
    """Hache un mot de passe : `pbkdf2_sha256$iterations$sel_hex$hash_hex`."""
    salt = secrets.token_bytes(16)
    dk = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, _PBKDF2_ITERATIONS)
    return f"pbkdf2_sha256${_PBKDF2_ITERATIONS}${salt.hex()}${dk.hex()}"


def verify_password(password, stored):
    """Vérifie un mot de passe contre la valeur stockée (comparaison temps constant)."""
    if not stored:
        return False
    try:
        algo, iters, salt_hex, hash_hex = stored.split("$", 3)
    except ValueError:
        return False
    if algo != "pbkdf2_sha256":
        return False
    try:
        iterations = int(iters)
        salt = bytes.fromhex(salt_hex)
        expected = bytes.fromhex(hash_hex)
    except ValueError:
        return False
    dk = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, iterations)
    return hmac.compare_digest(dk, expected)


def generate_password(length=16):
    """Génère un mot de passe aléatoire complexe (>=1 de chaque classe).

    Utilisé à la création/renouvellement d'un compte : la valeur en clair n'est
    jamais stockée ni affichée à l'admin — elle part une seule fois par e-mail.
    """
    length = max(12, int(length))
    lowers = string.ascii_lowercase
    uppers = string.ascii_uppercase
    digits = string.digits
    symbols = "!@#$%*-_=+?"
    alphabet = lowers + uppers + digits + symbols
    while True:
        pwd = "".join(secrets.choice(alphabet) for _ in range(length))
        if (any(c in lowers for c in pwd) and any(c in uppers for c in pwd)
                and any(c in digits for c in pwd) and any(c in symbols for c in pwd)):
            return pwd


# --------------------------------------------------------------------------
# JETONS DE SESSION
# --------------------------------------------------------------------------
def new_token():
    """Nouveau jeton de session opaque (valeur transmise UNE fois au client)."""
    return secrets.token_urlsafe(32)


def token_fingerprint(raw_token):
    """Empreinte SHA-256 (hex) d'un jeton — SEULE valeur stockée en base."""
    return hashlib.sha256(raw_token.encode("utf-8")).hexdigest()


def _now():
    return datetime.datetime.now()


def create_session(username, ip=None, db_path=None):
    """Crée une session pour `username` et renvoie (raw_token, expires_at_str).

    Le jeton brut n'est renvoyé qu'ici ; la base ne stocke que son empreinte.
    """
    raw = new_token()
    expires = (_now() + datetime.timedelta(hours=SESSION_TTL_HOURS)).strftime(_TS_FMT)
    kw = {"db_path": db_path} if db_path else {}
    database.insert_session(token_fingerprint(raw), username, expires, ip=ip, **kw)
    return raw, expires


def resolve_session(raw_token, ip=None, db_path=None):
    """Valide un jeton et fait glisser la fenêtre 24 h. None si invalide/expiré.

    Renvoie `{username, role, email, expires_at}` si la session est valide.
    Écriture d'échéance throttlée (`_SLIDE_THROTTLE_SECONDS`).
    """
    if not raw_token:
        return None
    kw = {"db_path": db_path} if db_path else {}
    th = token_fingerprint(raw_token)
    sess = database.get_session(th, **kw)
    if not sess:
        return None
    # Compte supprimé ou désactivé => session morte.
    if sess.get("username") is None or not sess.get("enabled"):
        database.delete_session(th, **kw)
        return None
    now = _now()
    try:
        old_expires = datetime.datetime.strptime(sess["expires_at"], _TS_FMT)
    except (ValueError, TypeError):
        database.delete_session(th, **kw)
        return None
    if old_expires < now:
        database.delete_session(th, **kw)
        return None
    # Glisse l'échéance à now+TTL, mais seulement si assez de temps s'est écoulé
    # depuis la dernière glisse (évite une écriture par requête).
    new_expires = now + datetime.timedelta(hours=SESSION_TTL_HOURS)
    eff_expires = sess["expires_at"]
    if (new_expires - old_expires).total_seconds() >= _SLIDE_THROTTLE_SECONDS:
        eff_expires = new_expires.strftime(_TS_FMT)
        database.touch_session(th, eff_expires, ip=ip, **kw)
        database.touch_user_seen(sess["username"], **kw)
    return {
        "username": sess["username"],
        "role": sess.get("role") or "technicien",
        "email": sess.get("email"),
        "expires_at": eff_expires,
    }


def revoke_session(raw_token, db_path=None):
    """Révoque la session associée à un jeton brut (déconnexion)."""
    if not raw_token:
        return
    kw = {"db_path": db_path} if db_path else {}
    database.delete_session(token_fingerprint(raw_token), **kw)


# --------------------------------------------------------------------------
# CLÉS API (clients machine, ex. superviseur)
# --------------------------------------------------------------------------
def new_api_key():
    """Nouvelle clé API opaque (renvoyée une seule fois à la création)."""
    return "mck_" + secrets.token_urlsafe(32)


def verify_api_key(raw_key, db_path=None):
    """Valide une clé API. Renvoie `{username, label}` si active, None sinon."""
    if not raw_key:
        return None
    kw = {"db_path": db_path} if db_path else {}
    kh = token_fingerprint(raw_key)
    row = database.get_api_key(kh, **kw)
    if not row or not row.get("enabled"):
        return None
    database.touch_api_key(kh, **kw)
    return {"username": row.get("username"), "label": row.get("label")}
