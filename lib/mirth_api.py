# -*- coding: utf-8 -*-
"""
Client minimal pour l'API REST de Mirth Connect (NextGen Connect).

Sans dépendance externe (uniquement la librairie standard : `urllib`,
`http.cookiejar`, `ssl`, `json`). Le serveur Mirth exposant en général un
certificat auto-signé sur son port HTTPS (8443 par défaut), la vérification TLS
est désactivée par défaut.

Résolution de la configuration (par ordre de priorité décroissante) :
    1. variables d'environnement (MIRTH_BASE_URL, MIRTH_USER, MIRTH_PASSWORD,
       MIRTH_VERIFY_SSL, MIRTH_PROCESS) ;
    2. fichier `.mirth_config.py` à la racine du projet (git-ignoré) ;
    3. valeurs par défaut ci-dessous.

Point d'entrée principal : `get_overview()` interroge le serveur et renvoie un
dictionnaire JSON-friendly. Toute erreur (serveur injoignable, identifiants
invalides, format inattendu) est capturée et renvoyée dans `{"reachable": False,
"error": ...}` — l'appelant n'a jamais à gérer d'exception réseau.
"""

import os
import sys
import ssl
import json
import importlib.util
import urllib.parse
import urllib.request
import http.cookiejar

# --- Valeurs par défaut ----------------------------------------------------
_DEFAULTS = {
    "MIRTH_BASE_URL": "https://localhost:8443/api",
    "MIRTH_USER": "admin",
    "MIRTH_PASSWORD": "admin",
    "MIRTH_VERIFY_SSL": False,
    "MIRTH_PROCESS": "mcservice.exe",   # nom du processus à surveiller (system_state)
}

# Dossier de base (compatible PyInstaller, comme quickmail.py).
if getattr(sys, "frozen", False):
    _BASE_DIR = sys._MEIPASS
else:
    _BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _as_bool(value, default=False):
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    return str(value).strip().lower() in ("true", "1", "yes", "on")


def get_config():
    """Construit la configuration effective (env > .mirth_config.py > défauts)."""
    cfg = dict(_DEFAULTS)

    # 2. Fichier .mirth_config.py (git-ignoré).
    config_path = os.path.join(_BASE_DIR, ".mirth_config.py")
    if os.path.exists(config_path):
        try:
            spec = importlib.util.spec_from_file_location("mirth_config_dot", config_path)
            mod = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(mod)
            for key in cfg:
                if hasattr(mod, key):
                    cfg[key] = getattr(mod, key)
        except Exception as e:
            print(f"[mirth_api] Avertissement : configuration {config_path} ignorée ({e}).",
                  file=sys.stderr)

    # 1. Variables d'environnement (priorité maximale).
    for key in cfg:
        if key in os.environ:
            cfg[key] = os.environ[key]

    cfg["MIRTH_VERIFY_SSL"] = _as_bool(cfg.get("MIRTH_VERIFY_SSL"), default=False)
    cfg["MIRTH_BASE_URL"] = str(cfg["MIRTH_BASE_URL"]).rstrip("/")
    return cfg


def get_process_name():
    """Nom du processus Mirth à surveiller côté système (ex. 'mcservice.exe')."""
    return get_config()["MIRTH_PROCESS"]


class MirthClient:
    """Session authentifiée auprès d'un serveur Mirth Connect."""

    def __init__(self, base_url=None, user=None, password=None, verify_ssl=None,
                 timeout=8):
        cfg = get_config()
        self.base_url = (base_url or cfg["MIRTH_BASE_URL"]).rstrip("/")
        self.user = user if user is not None else cfg["MIRTH_USER"]
        self.password = password if password is not None else cfg["MIRTH_PASSWORD"]
        self.verify_ssl = cfg["MIRTH_VERIFY_SSL"] if verify_ssl is None else verify_ssl
        self.timeout = timeout

        # Contexte TLS : non vérifié par défaut (certificat auto-signé de Mirth).
        if self.verify_ssl:
            ctx = ssl.create_default_context()
        else:
            ctx = ssl.create_default_context()
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE

        self._cookies = http.cookiejar.CookieJar()
        self._opener = urllib.request.build_opener(
            urllib.request.HTTPSHandler(context=ctx),
            urllib.request.HTTPCookieProcessor(self._cookies),
        )

    # -- bas niveau ---------------------------------------------------------
    def _request(self, path, data=None, accept="application/json"):
        """Requête HTTP. `data` (dict) => POST form-encoded, sinon GET.

        Retourne (status, body_text). Lève en cas d'erreur réseau/HTTP.
        """
        url = self.base_url + path
        headers = {
            "Accept": accept,
            # Requis par la protection CSRF de Mirth sur les requêtes d'API.
            "X-Requested-With": "XMLHttpRequest",
        }
        if data is not None:
            body = urllib.parse.urlencode(data).encode("utf-8")
            headers["Content-Type"] = "application/x-www-form-urlencoded"
            req = urllib.request.Request(url, data=body, headers=headers, method="POST")
        else:
            req = urllib.request.Request(url, headers=headers, method="GET")

        with self._opener.open(req, timeout=self.timeout) as resp:
            charset = resp.headers.get_content_charset() or "utf-8"
            return resp.status, resp.read().decode(charset, errors="replace")

    def _get_json(self, path):
        status, text = self._request(path, accept="application/json")
        text = text.strip()
        if not text:
            return None
        try:
            return json.loads(text)
        except ValueError:
            return None

    # -- haut niveau --------------------------------------------------------
    def login(self):
        """Ouvre une session (cookie JSESSIONID). Lève si l'authentification échoue."""
        self._request("/users/_login",
                      data={"username": self.user, "password": self.password})

    def logout(self):
        try:
            self._request("/users/_logout", data={})
        except Exception:
            pass

    def get_version(self):
        """Version du serveur Mirth (chaîne), ou None."""
        try:
            _status, text = self._request("/server/version", accept="text/plain")
            return text.strip() or None
        except Exception:
            return None

    def get_system_stats(self):
        """Statistiques système du serveur Mirth (CPU/mémoire JVM), ou {}."""
        try:
            data = self._get_json("/system/stats")
            return data if isinstance(data, dict) else {}
        except Exception:
            return {}

    def get_system_info(self):
        """Informations JVM / OS du serveur Mirth, ou {}."""
        try:
            data = self._get_json("/system/info")
            return data if isinstance(data, dict) else {}
        except Exception:
            return {}

    def get_channels(self):
        """Liste normalisée des canaux déployés avec leurs statistiques.

        Renvoie une liste de dicts {name, channel_id, state, received, filtered,
        queued, sent, error}. Liste vide si aucun canal ou en cas d'erreur.
        """
        try:
            data = self._get_json("/channels/statuses")
        except Exception:
            return []
        return _parse_dashboard_statuses(data)


# --------------------------------------------------------------------------
# PARSING DÉFENSIF DES STATUTS DE CANAUX (format JSON variable selon version)
# --------------------------------------------------------------------------
def _as_list(value):
    """Mirth sérialise une collection à 1 élément comme un objet, sinon une liste."""
    if value is None:
        return []
    return value if isinstance(value, list) else [value]


def _parse_statistics(stats):
    """Extrait {RECEIVED, FILTERED, SENT, ERROR, ...} d'un bloc `statistics`.

    Le format Mirth est un map sérialisé : {"entry": [{"string": "RECEIVED",
    "long": "12"}, ...]}. Tolère aussi un simple dict {clé: valeur}.
    """
    out = {}
    if not isinstance(stats, dict):
        return out
    entries = stats.get("entry")
    if entries is not None:
        for e in _as_list(entries):
            if not isinstance(e, dict):
                continue
            key = e.get("string") or e.get("key")
            val = e.get("long", e.get("int", e.get("value")))
            if key is None:
                continue
            try:
                out[str(key).upper()] = int(val)
            except (TypeError, ValueError):
                out[str(key).upper()] = val
    else:
        for k, v in stats.items():
            try:
                out[str(k).upper()] = int(v)
            except (TypeError, ValueError):
                out[str(k).upper()] = v
    return out


def _parse_dashboard_statuses(data):
    """Normalise la réponse de /channels/statuses en liste de canaux."""
    if not isinstance(data, dict):
        return []
    # Forme habituelle : {"list": {"dashboardStatus": [...]}} ou directement une liste.
    container = data.get("list", data)
    if isinstance(container, dict):
        statuses = _as_list(container.get("dashboardStatus"))
    else:
        statuses = _as_list(container)

    channels = []
    for s in statuses:
        if not isinstance(s, dict):
            continue
        stats = _parse_statistics(s.get("statistics"))
        channels.append({
            "name": s.get("name"),
            "channel_id": s.get("channelId"),
            "state": s.get("state"),
            "received": stats.get("RECEIVED"),
            "filtered": stats.get("FILTERED"),
            "queued": s.get("queued", stats.get("QUEUED")),
            "sent": stats.get("SENT"),
            "error": stats.get("ERROR"),
        })
    return channels


# --------------------------------------------------------------------------
# POINT D'ENTRÉE : VUE D'ENSEMBLE
# --------------------------------------------------------------------------
def get_overview(timeout=8):
    """Interroge le serveur Mirth et renvoie une vue d'ensemble JSON-friendly.

    Ne lève jamais : en cas d'échec, renvoie {"reachable": False, "error": ...}.
    """
    cfg = get_config()
    base_url = cfg["MIRTH_BASE_URL"]
    result = {
        "reachable": False,
        "error": None,
        "base_url": base_url,
        "version": None,
        "system_stats": {},
        "system_info": {},
        "channels": [],
        "channel_count": 0,
        "channels_started": 0,
    }

    try:
        client = MirthClient(timeout=timeout)
        client.login()
    except Exception as e:
        result["error"] = f"Connexion/authentification impossible : {e}"
        return result

    try:
        result["version"] = client.get_version()
        result["system_stats"] = client.get_system_stats()
        result["system_info"] = client.get_system_info()
        channels = client.get_channels()
        result["channels"] = channels
        result["channel_count"] = len(channels)
        result["channels_started"] = sum(
            1 for c in channels if (c.get("state") or "").upper() == "STARTED"
        )
        result["reachable"] = True
    except Exception as e:
        result["error"] = f"Erreur lors de la lecture des données : {e}"
    finally:
        client.logout()

    return result
