# -*- coding: utf-8 -*-
"""
Politique de sécurité du serveur web : filtre IP, TLS et authentification.

`SecurityPolicy` est un objet unique passé à `webserver.serve()` ; son hook
`check(handler, path)` est appelé tout en haut de `_dispatch` (le point de passage
de TOUTE requête) et compose, dans l'ordre :

    0. localhost de confiance   (loopback jamais bloqué)
    1. filtre IP (liste blanche CIDR)   -> 403 si l'IP n'est pas autorisée
    3. authentification par session/clé -> 401 si jeton absent/expiré

La couche 2 (HTTPS/TLS) est portée par `tls_context`, appliqué au socket dans
`webserver.serve`. `load_security_config()` lit les réglages depuis
`.mirth_config.py` (même chargeur que `mirth_api.get_config`) avec la précédence
arguments CLI > variables d'environnement > fichier > défauts.

Réutilisé tel quel par `checker_service.py` ET `superviseur.py`.
"""

import os
import sys
import ipaddress
import importlib.util
from http.cookies import SimpleCookie

from . import auth, database
from .webserver import Response

SESSION_COOKIE = "mc_session"

# Réglages de sécurité et leurs valeurs par défaut (sécurisé par défaut).
_DEFAULTS = {
    "HTTPS_MODE": "auto",      # "auto" | "on" | "off"
    "HTTPS_CERT": "",
    "HTTPS_KEY": "",
    "ALLOWED_IPS": [],          # [] => tout autorisé (rétro-compatible)
    "AUTH_ENABLED": True,
    "SESSION_TTL_H": 24,
}


def _base_dir():
    """Dossier où chercher `.mirth_config.py` : à côté de l'exe (gelé) ou racine projet."""
    if getattr(sys, "frozen", False):
        return os.path.dirname(os.path.abspath(sys.executable))
    return os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _as_bool(value, default=False):
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    return str(value).strip().lower() in ("true", "1", "yes", "on")


def _as_ip_list(value):
    """Normalise ALLOWED_IPS : liste OU chaîne (séparée par , ; espaces / retours)."""
    if not value:
        return []
    if isinstance(value, (list, tuple, set)):
        items = list(value)
    else:
        items = str(value).replace(";", ",").replace("\n", ",").split(",")
    out = []
    for it in items:
        it = str(it).strip()
        if it:
            out.append(it)
    return out


def load_security_config(args=None):
    """Construit la config sécurité effective (args CLI > env > fichier > défauts)."""
    cfg = dict(_DEFAULTS)

    # Fichier .mirth_config.py (git-ignoré).
    config_path = os.path.join(_base_dir(), ".mirth_config.py")
    if os.path.isfile(config_path):
        try:
            spec = importlib.util.spec_from_file_location("mirth_config_sec", config_path)
            mod = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(mod)
            for key in cfg:
                if hasattr(mod, key):
                    cfg[key] = getattr(mod, key)
        except Exception as e:
            print(f"[security] Avertissement : configuration {config_path} ignorée ({e}).",
                  file=sys.stderr)

    # Variables d'environnement.
    for key in cfg:
        if key in os.environ:
            cfg[key] = os.environ[key]

    # Arguments CLI (prioritaires — l'opérateur les a saisis pour ce lancement).
    if args is not None:
        if getattr(args, "https", None) is not None:
            cfg["HTTPS_MODE"] = args.https
        if getattr(args, "cert", None):
            cfg["HTTPS_CERT"] = args.cert
        if getattr(args, "key", None):
            cfg["HTTPS_KEY"] = args.key
        if getattr(args, "allow_ips", None) is not None:
            cfg["ALLOWED_IPS"] = args.allow_ips
        if getattr(args, "auth", None) is not None:
            cfg["AUTH_ENABLED"] = (args.auth == "on")

    cfg["HTTPS_MODE"] = str(cfg["HTTPS_MODE"]).strip().lower() or "auto"
    cfg["HTTPS_CERT"] = str(cfg["HTTPS_CERT"] or "")
    cfg["HTTPS_KEY"] = str(cfg["HTTPS_KEY"] or "")
    cfg["ALLOWED_IPS"] = _as_ip_list(cfg["ALLOWED_IPS"])
    cfg["AUTH_ENABLED"] = _as_bool(cfg["AUTH_ENABLED"], default=True)
    try:
        cfg["SESSION_TTL_H"] = max(1, int(cfg["SESSION_TTL_H"]))
    except (TypeError, ValueError):
        cfg["SESSION_TTL_H"] = 24
    return cfg


def primary_ip(bind_host=None):
    """IP routable de la machine, pour bâtir des liens accessibles hors DNS local.

    Utilisé pour l'URL de base des e-mails (les liens doivent pointer sur une IP,
    faute d'entrée DNS locale pour le nom d'hôte). Si `bind_host` est déjà une
    adresse concrète (ni wildcard ni loopback), on la renvoie telle quelle.
    Sinon on déduit l'IP de l'interface qui porte la route par défaut (sans
    émettre de paquet). Replis : nom d'hôte, puis "localhost".
    """
    import socket
    h = (bind_host or "").strip()
    if h and h.lower() not in ("0.0.0.0", "::", "localhost", "127.0.0.1", "::1"):
        return h
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            s.connect(("8.8.8.8", 80))     # sélectionne la route, n'envoie rien
            ip = s.getsockname()[0]
        finally:
            s.close()
        if ip and not ip.startswith("127."):
            return ip
    except OSError:
        pass
    try:
        return socket.gethostname() or "localhost"
    except OSError:
        return "localhost"


def https_enabled_for_host(mode, host):
    """HTTPS attendu ? `on` => oui, `off` => non, `auto` => oui hors loopback."""
    mode = (mode or "auto").lower()
    if mode == "on":
        return True
    if mode == "off":
        return False
    h = (host or "").strip().lower()
    return h not in ("127.0.0.1", "::1", "localhost", "")


def parse_networks(ip_list):
    """Transforme une liste d'IP/CIDR en réseaux `ipaddress` (entrées invalides ignorées)."""
    nets = []
    for item in _as_ip_list(ip_list):
        try:
            nets.append(ipaddress.ip_network(item, strict=False))
        except ValueError:
            print(f"[security] Entrée ALLOWED_IPS ignorée (invalide) : {item!r}",
                  file=sys.stderr)
    return nets


def _to_ip(addr):
    """Adresse client -> objet ipaddress, en dépliant l'IPv4-mappée (::ffff:a.b.c.d)."""
    try:
        ip = ipaddress.ip_address(addr.split("%")[0])
    except ValueError:
        return None
    mapped = getattr(ip, "ipv4_mapped", None)
    return mapped or ip


def _read_cookie(cookie_header, name):
    if not cookie_header:
        return None
    try:
        jar = SimpleCookie()
        jar.load(cookie_header)
        morsel = jar.get(name)
        return morsel.value if morsel else None
    except Exception:
        return None


def _json_response(status, payload):
    import json
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    return Response(body, status=status,
                    content_type="application/json; charset=utf-8")


class SecurityPolicy:
    """Filtre IP + TLS + authentification, appliqués par le hook `check`."""

    def __init__(self, networks=None, tls_context=None, auth_enabled=False,
                 db_path=None, public_paths=None):
        self.networks = networks or []
        self.tls_context = tls_context
        self.auth_enabled = auth_enabled
        self.db_path = db_path
        # Routes API accessibles SANS session (le reste de /api/* est protégé).
        self.public_paths = set(public_paths or {"/api/auth/login"})

    # -- Couches 0/1 : filtre IP -------------------------------------------
    def _client_ip(self, handler):
        try:
            return handler.client_address[0]
        except Exception:
            return ""

    def _is_loopback(self, ip):
        return ip is not None and ip.is_loopback

    def _ip_allowed(self, ip):
        if ip is None:
            return False
        if self._is_loopback(ip):
            return True                 # couche 0 : localhost toujours toléré
        if not self.networks:
            return True                 # liste vide => tout autorisé
        return any(ip in net for net in self.networks)

    # -- Couche 3 : authentification ---------------------------------------
    def _authenticate(self, handler, ip_str):
        # 1. Cookie de session (navigateur).
        tok = _read_cookie(handler.headers.get("Cookie"), SESSION_COOKIE)
        if tok:
            u = auth.resolve_session(tok, ip=ip_str, db_path=self.db_path)
            if u:
                return u
        # 2. En-tête Authorization: Bearer <jeton|clé API>.
        authz = handler.headers.get("Authorization", "") or ""
        if authz.startswith("Bearer "):
            bearer = authz[7:].strip()
            u = auth.resolve_session(bearer, ip=ip_str, db_path=self.db_path)
            if u:
                return u
            k = auth.verify_api_key(bearer, db_path=self.db_path)
            if k:
                role = "technicien"
                if k.get("username"):
                    urow = database.get_user(k["username"], db_path=self.db_path)
                    if urow and urow.get("enabled"):
                        role = urow.get("role") or "technicien"
                return {"username": k.get("username") or ("apikey:" + (k.get("label") or "")),
                        "role": role, "via": "apikey"}
        return None

    def check(self, handler, path):
        """Applique filtre IP puis auth. Renvoie une Response (refus) ou None (OK)."""
        ip = _to_ip(self._client_ip(handler))
        ip_str = str(ip) if ip is not None else ""

        # Couche 1 : filtre IP.
        if not self._ip_allowed(ip):
            return _json_response(403, {"error": "forbidden",
                                        "detail": "Adresse IP non autorisée."})

        # Couche 3 : authentification.
        if not self.auth_enabled:
            return None
        if not path.startswith("/api/"):
            return None                          # pages/ressources statiques publiques
        if path in self.public_paths:
            return None                          # ex. /api/auth/login

        user = self._authenticate(handler, ip_str)
        if user:
            handler._auth_user = user
            return None

        # Bootstrap : aucun compte encore créé + accès loopback => on laisse passer
        # pour permettre la création du 1er admin depuis la machine locale.
        if self._is_loopback(ip) and database.count_users(db_path=self.db_path) == 0:
            handler._auth_user = {"username": "(bootstrap)", "role": "admin",
                                  "bootstrap": True}
            return None

        return _json_response(401, {"error": "unauthorized",
                                    "detail": "Authentification requise."})
