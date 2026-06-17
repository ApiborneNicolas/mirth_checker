# -*- coding: utf-8 -*-
"""
Mini-serveur HTTP avec routage, basé sur la librairie standard `http.server`.

Fonctionnalités :
- enregistrement de routes API (méthode + chemin -> handler) renvoyant du JSON ;
- service de fichiers statiques depuis un répertoire (les pages .html) ;
- serveur multi-thread (`ThreadingHTTPServer`) pour ne pas bloquer pendant qu'une
  requête longue (parsing de log, sonde système) est en cours.

Un handler reçoit un objet `Request` et retourne soit :
- un dict / list  -> sérialisé en JSON (HTTP 200) ;
- un tuple (status, payload) -> payload sérialisé en JSON avec le statut donné ;
- un objet `Response` pour un contrôle complet (type de contenu, octets bruts).
"""

import os
import re
import json
import socket
import mimetypes
from urllib.parse import urlparse, parse_qs
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


class _ExclusiveHTTPServer(ThreadingHTTPServer):
    """Serveur refusant de partager son port avec une autre instance.

    `HTTPServer` active `allow_reuse_address` (SO_REUSEADDR), ce qui, SOUS WINDOWS,
    autorise deux processus à se lier au MÊME port simultanément — chacun lançant
    alors ses propres collecteurs qui écrivent dans la même base, d'où des relevés
    en doublon et trop fréquents. On désactive ce partage (et on pose
    SO_EXCLUSIVEADDRUSE sous Windows) pour qu'un second lancement échoue franchement
    au lieu de polluer silencieusement l'historique.
    """
    allow_reuse_address = False

    def server_bind(self):
        if os.name == "nt" and hasattr(socket, "SO_EXCLUSIVEADDRUSE"):
            self.socket.setsockopt(socket.SOL_SOCKET,
                                   socket.SO_EXCLUSIVEADDRUSE, 1)
        super().server_bind()


class Request:
    """Représente une requête HTTP entrante exposée aux handlers."""

    def __init__(self, method, path, query, headers, body, params):
        self.method = method
        self.path = path
        self.query = query        # dict[str, str] (première valeur de chaque clé)
        self.query_multi = None   # dict[str, list[str]] (rempli ci-dessous)
        self.headers = headers
        self.body = body          # bytes
        self.params = params      # paramètres extraits du motif de route

    def get(self, key, default=None):
        return self.query.get(key, default)

    def json(self):
        """Décode le corps de la requête en JSON, ou {} si vide/invalide."""
        if not self.body:
            return {}
        try:
            return json.loads(self.body.decode("utf-8"))
        except Exception:
            return {}


class Response:
    """Réponse à contrôle complet (octets bruts + type de contenu)."""

    def __init__(self, body=b"", status=200, content_type="text/plain; charset=utf-8", headers=None):
        if isinstance(body, str):
            body = body.encode("utf-8")
        self.body = body
        self.status = status
        self.content_type = content_type
        self.headers = headers or {}


class Router:
    def __init__(self, static_dir=None, index_route="/", json_transform=None):
        """
        Args:
            static_dir (str): répertoire des fichiers statiques (pages .html).
            index_route (str): chemin servant le fichier index.html du static_dir.
            json_transform (callable): fonction optionnelle appliquée à toute
                charge utile avant sérialisation JSON (ex. normalisation des
                nombres décimaux). Reçoit la charge utile, renvoie la version
                transformée.
        """
        self.routes = []  # (method, compiled_regex, handler)
        self.static_dir = static_dir
        self.index_route = index_route
        self.json_transform = json_transform

    def add(self, method, pattern, handler):
        """
        Enregistre une route. Le motif peut contenir des segments nommés `{nom}`.
        Exemple : add("GET", "/api/item/{id}", handler) -> params["id"].
        """
        regex = "^" + re.sub(r"\{(\w+)\}", r"(?P<\1>[^/]+)", pattern) + "$"
        self.routes.append((method.upper(), re.compile(regex), handler))
        return self

    def get(self, pattern, handler):
        return self.add("GET", pattern, handler)

    def post(self, pattern, handler):
        return self.add("POST", pattern, handler)

    def match(self, method, path):
        for m, regex, handler in self.routes:
            if m != method.upper():
                continue
            match = regex.match(path)
            if match:
                return handler, match.groupdict()
        return None, None


def _build_handler_class(router):
    class _Handler(BaseHTTPRequestHandler):
        server_version = "CheckerService/1.0"

        # Silence le log par défaut (une ligne par requête) au profit d'un format compact
        def log_message(self, fmt, *args):
            return

        def _send(self, response):
            try:
                self.send_response(response.status)
                self.send_header("Content-Type", response.content_type)
                self.send_header("Content-Length", str(len(response.body)))
                for k, v in response.headers.items():
                    self.send_header(k, v)
                self.end_headers()
                if self.command != "HEAD":
                    self.wfile.write(response.body)
            except (ConnectionAbortedError, ConnectionResetError, BrokenPipeError):
                # Le client a fermé la connexion avant la fin de l'écriture (fréquent
                # avec les pages à rafraîchissement auto qui annulent une requête en
                # cours). Sans intérêt : on ignore au lieu de polluer la console.
                self.close_connection = True

        def _json(self, payload, status=200):
            if router.json_transform:
                payload = router.json_transform(payload)
            body = json.dumps(payload, ensure_ascii=False, default=str).encode("utf-8")
            self._send(Response(body, status=status,
                                content_type="application/json; charset=utf-8"))

        def _serve_static(self, path):
            if not router.static_dir:
                return False
            # Le chemin index sert index.html
            rel = "index.html" if path == router.index_route else path.lstrip("/")
            # Empêche la traversée de répertoire
            full = os.path.normpath(os.path.join(router.static_dir, rel))
            if not full.startswith(os.path.abspath(router.static_dir)):
                return False
            if os.path.isfile(full):
                ctype, _ = mimetypes.guess_type(full)
                ctype = ctype or "application/octet-stream"
                if ctype.startswith("text/") or ctype in ("application/javascript",
                                                           "application/json"):
                    ctype += "; charset=utf-8"
                # Toujours revalider : les pages/scripts du tableau de bord évoluent
                # et sont servis depuis le disque. `no-cache` force le navigateur à
                # revérifier avant de réutiliser sa copie — sans validateur, il
                # re-télécharge donc la version à jour (pas de page périmée après une
                # modification). Coût négligeable en réseau local.
                with open(full, "rb") as f:
                    self._send(Response(f.read(), content_type=ctype,
                                        headers={"Cache-Control": "no-cache"}))
                return True
            return False

        def _dispatch(self, method):
            parsed = urlparse(self.path)
            path = parsed.path
            query_multi = parse_qs(parsed.query)
            query = {k: v[0] for k, v in query_multi.items()}

            handler, params = router.match(method, path)

            # GET : tente le fichier statique si aucune route API ne correspond
            if handler is None and method in ("GET", "HEAD"):
                if self._serve_static(path):
                    return
                self._json({"error": "Not found", "path": path}, status=404)
                return

            if handler is None:
                self._json({"error": "Method not allowed or not found",
                            "path": path}, status=405)
                return

            length = int(self.headers.get("Content-Length", 0) or 0)
            body = self.rfile.read(length) if length else b""

            req = Request(method, path, query, self.headers, body, params)
            req.query_multi = query_multi

            try:
                result = handler(req)
            except Exception as e:  # une route qui plante renvoie une 500 propre
                import traceback
                traceback.print_exc()
                self._json({"error": str(e)}, status=500)
                return

            if isinstance(result, Response):
                self._send(result)
            elif isinstance(result, tuple) and len(result) == 2:
                status, payload = result
                self._json(payload, status=status)
            else:
                self._json(result)

        def do_GET(self):
            self._dispatch("GET")

        def do_HEAD(self):
            self._dispatch("GET")

        def do_POST(self):
            self._dispatch("POST")

    return _Handler


def serve(router, host="0.0.0.0", port=8800):
    """
    Démarre le serveur (bloquant). Retourne l'instance ThreadingHTTPServer si
    l'appelant souhaite la fermer ; en pratique cette fonction boucle jusqu'à
    KeyboardInterrupt.
    """
    handler_class = _build_handler_class(router)
    httpd = _ExclusiveHTTPServer((host, port), handler_class)
    httpd.daemon_threads = True
    return httpd
