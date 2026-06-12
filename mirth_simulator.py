#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Mirth Simulator
===============
Simulateur autonome du logiciel **Mirth Connect (NextGen Connect)**.

But : exposer une API REST suffisamment fidèle à celle du vrai serveur pour que
`mirth_api.py` (et donc `checker_service.py`) puissent dialoguer avec ce
simulateur exactement comme avec une installation réelle, sans aucune
modification de leur code.

Ce script est **totalement indépendant** du reste du projet : il n'importe
aucun module local et n'utilise que la bibliothèque standard de Python. Il lit
toutefois le fichier `.mirth_config.py` (même format/priorité que `mirth_api.py`)
pour connaître l'URL/le port à ouvrir et les identifiants à accepter.

Fonctionnement
--------------
  * Au démarrage, on crée `--channels` canaux nommés « Simul_Client_X ». Chaque
    canal possède deux sous-canaux (connecteurs) : un *IN* (Source) et un *OUT*
    (Destination), par lesquels transitent les données.
  * Des données aléatoires d'initialisation sont injectées (`--ok` messages OK et
    `--errors` messages en erreur répartis au hasard).
  * Un serveur HTTPS (certificat auto-signé embarqué, comme Mirth) est ouvert sur
    le host/port issus de `.mirth_config.py` (défaut https://localhost:8443/api).
  * L'écran d'accueil liste l'état des canaux et de leurs sous-canaux.

Commandes clavier (dans la fenêtre du simulateur)
-------------------------------------------------
  m1i   -> message OK sur le canal 1, sous-canal IN
  e2o   -> message ERREUR sur le canal 2, sous-canal OUT (texte + identifiant)
  m / e -> demande interactivement le canal puis le sous-canal (i/o)
  D     -> dump (sauvegarde) de la mémoire dans mirth_simulator.json
  L     -> load (chargement) de la mémoire depuis mirth_simulator.json
  r     -> rafraîchit l'affichage
  h     -> aide
  q     -> quitte

Une fois lancé, `python mirth_api.py` doit pouvoir récupérer canaux,
statistiques et messages en erreur via le protocole déjà en place.
"""

import os
import re
import ssl
import sys
import json
import time
import uuid
import random
import atexit
import socket
import argparse
import datetime
import tempfile
import threading
import importlib.util
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

# Lecture clavier directe depuis la console Windows (CONIN$), indépendante d'une
# éventuelle redirection de stdin (ex. `echo. | python ...` dans launch.bat).
try:
    import msvcrt  # Windows uniquement
except ImportError:
    msvcrt = None


# ==============================================================================
# CERTIFICAT TLS AUTO-SIGNÉ EMBARQUÉ (CN=localhost)
# ------------------------------------------------------------------------------
# Mirth Connect expose son API derrière un certificat auto-signé ; le client
# (`mirth_api.py`) désactive d'ailleurs la vérification TLS par défaut. Pour
# rester sans dépendance externe et sans génération à la volée, on embarque un
# couple cert/clé auto-signé valable ~10 ans. Il n'a aucune valeur de sécurité :
# c'est un simulateur local.
# ==============================================================================
_EMBEDDED_CERT_PEM = """-----BEGIN CERTIFICATE-----
MIIC1DCCAbygAwIBAgIUUiHepbBPN+SPVQ+1uuF1CcWL+VAwDQYJKoZIhvcNAQEL
BQAwFDESMBAGA1UEAwwJbG9jYWxob3N0MB4XDTI2MDYxMTA3MjQxOFoXDTM2MDYw
OTA3MjQxOFowFDESMBAGA1UEAwwJbG9jYWxob3N0MIIBIjANBgkqhkiG9w0BAQEF
AAOCAQ8AMIIBCgKCAQEAjYdjMKK9FKrZJN6lKVQDQo1fpaVnm38ZIVAu5v312XvS
mZ5/2juwNN0O6XGz08DOerdON4TiF4vd8AR6kDp9wcrWGr3/s2VVBOZQrS4emn1+
61gLN6RiJgSgb/Rc58nw/YfMBVhoSC37mlFFTqiURJUCNodrtN5mQqxXIBotCnk8
jlPEZsYoGkcHfWc9ssHM3YFMVExQVDzj1B2J6wihibOiK5dxnNzuGlVGulAhJ5Dl
qI1xqAPuJQ7qA/+sBtaPdi46Xg4POiqZhOfk4B2AOjRfYatqLhI/45xXelgQnGHW
fSUVpCjq4Qx8gSwqBOh+R0mGdsMrXi1hs+rGRpUfXwIDAQABox4wHDAaBgNVHREE
EzARgglsb2NhbGhvc3SHBH8AAAEwDQYJKoZIhvcNAQELBQADggEBABO7tbSWwT+4
21GBwr3uFm7Zb+Jiv5Hf+CiVNdWhX6Tbnf/3w0cE6tip6oVB5KLmgcMfdozMEmL6
EWIh49NHbd9pVbhr9+Cfxx2O4KyXYcGXs6yrmIrZjkN6lmWoyBsxgAZVVDV/Lg4T
31jqc7vQSWqRVEQPUHo9lacF8ixMMEPAdUPbgyWgK9QEhD1/SBz0Z3eRvQTjYfI2
62EvS2CE+fhVpgiSiIMr/RofOHe8CqOiBqCnNyLumq09UT9UhLlGjY1vnTgNo3Gx
+5gn1gsIP9dTyj5b7DB1nGokmXSj5bp0aJprRRzXJ/JeJ+YHB/i/eRmuw5P02JSS
C4VBdxIHBig=
-----END CERTIFICATE-----
"""

_EMBEDDED_KEY_PEM = """-----BEGIN RSA PRIVATE KEY-----
MIIEogIBAAKCAQEAjYdjMKK9FKrZJN6lKVQDQo1fpaVnm38ZIVAu5v312XvSmZ5/
2juwNN0O6XGz08DOerdON4TiF4vd8AR6kDp9wcrWGr3/s2VVBOZQrS4emn1+61gL
N6RiJgSgb/Rc58nw/YfMBVhoSC37mlFFTqiURJUCNodrtN5mQqxXIBotCnk8jlPE
ZsYoGkcHfWc9ssHM3YFMVExQVDzj1B2J6wihibOiK5dxnNzuGlVGulAhJ5DlqI1x
qAPuJQ7qA/+sBtaPdi46Xg4POiqZhOfk4B2AOjRfYatqLhI/45xXelgQnGHWfSUV
pCjq4Qx8gSwqBOh+R0mGdsMrXi1hs+rGRpUfXwIDAQABAoIBAAeHqAHa7rduSEOm
FYsnh/g0VbNA+SG0oe17+tAvETOt2wHHBTi4YJgiGMctKwBTAPQA07MFfr1Po5Jq
Z+ug3mGp/Tk1WajoSnKKdHQPq5t+83qUV8hxQQg9H8dvwg+nrArj0jmIlMfJATAa
hIGlBiUd+R0AmHzqMO5O2rXVCVzGC+6sjfxvFU7GxF6hyirXuWiheKiOQWM9QvHD
2NFpEuuvQ7/il4AtoLqo2wGAFQlo8Wp/kb9kpsVednl1AEtk6WcH1VGU5h8P78S/
ymQmF7PtgTNKxfgFtcxx2ab101xHq1GN7I3XK+e0pdHq3QgFsWktJPKwHBFTMQzG
pO6oT9ECgYEAwLl8WJo7xpT4x6cqrer9d5q/vBUOSgkfvy6sTUJZnJIKFf47Zdjy
Cr2Tvrwz1K5XZaJbPG8Mg1REEqX/A+1wpvz9QP9xs8xDqdtWEnmwcKdU/UfVp7Ut
w55A6ickHyze5Nja30Jne3oHbA+hohkGqSeoXUZP8I3rqKer6KHMIJECgYEAu/7m
Ok2bLRj7i1fxsF5hv+A5bF4b7PO1sce7a7niRz7wR4gXucXqNa5B1MNIfoien4Z4
9z9TGwg04dK0NXzN+FYOIrXzg9+yhwOsx+S1Ph6yOJjs4TaOuHzWuiwF8m5lUwcn
wjnJbpqUnv564luMXYu156aNe5YNl569MUUzOO8CgYAPbIjcGnPgP7ntWJ6czqq8
cMEZj2HWYQaOaXDWuhGr6zAtdGxSiVtNqsBxSmSnh9BszOKaYpTQyeSszWYsbUtP
wf2OvyLdbeKYbHpl/iE10t6FasNZqbFg74BofPtyF0g7bnON3KWlhy2i41lfPLuA
vDDITkFFkkYi+FBUzOYmUQKBgF89DgOBZ1icbGq2PeG8nsam4FBvCLSs7mJHLkKv
49t2HiIO5v4dLr7NLdqMqAA6VCm65TNUqFRsfuXcaaEjPfFOH1EkXl5ziCzwBqsp
yUvUHzOe/XpGulzqGZotTUH4/WnnmRPDVLGsrBg0Ear0+BI4Agp+DPUMGoyyRWRd
i0qPAoGAVswzEc+wJyJqCHQMV+EVhvpe+6X+YRGKweszg0Nx2cIy+zYM4muhNuWB
AudYwGwuhJoE+xwhCv8w9R5+Lu6+VwUOY8vFSWHqFOm6HdkhEKL1dig3ANE44FXb
42cSYWdIleJxf6BzxC6FzjWyBPH4xIcdl3O/Rn5cf0P/zxbrZFk=
-----END RSA PRIVATE KEY-----
"""

# Nom pleinement qualifié de l'énumération Status, utilisé par Mirth comme clé de
# ses Map<Status, Long> lors de la sérialisation JSON (cf. mirth_api._parse_statistics).
_STATUS_CLASS = "com.mirth.connect.donkey.model.message.Status"

# Version de serveur annoncée par /server/version.
_SERVER_VERSION = "4.5.2"

# Ordre canonique des compteurs de statistiques exposés par canal/connecteur.
_STAT_KEYS = ("RECEIVED", "FILTERED", "QUEUED", "SENT", "ERROR")

DUMP_FILE = "mirth_simulator.json"


# ==============================================================================
# CONFIGURATION (indépendante : reproduit la résolution de mirth_api.get_config)
# ==============================================================================
_CONFIG_DEFAULTS = {
    "MIRTH_BASE_URL": "https://localhost:8443/api",
    "MIRTH_USER": "admin",
    "MIRTH_PASSWORD": "admin",
}

if getattr(sys, "frozen", False):
    _BASE_DIR = sys._MEIPASS
else:
    _BASE_DIR = os.path.dirname(os.path.abspath(__file__))


def load_config():
    """Construit la configuration effective : env > .mirth_config.py > défauts."""
    cfg = dict(_CONFIG_DEFAULTS)

    config_path = os.path.join(_BASE_DIR, ".mirth_config.py")
    if os.path.exists(config_path):
        try:
            spec = importlib.util.spec_from_file_location("mirth_config_sim", config_path)
            mod = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(mod)
            for key in cfg:
                if hasattr(mod, key):
                    cfg[key] = getattr(mod, key)
        except Exception as e:
            print(f"[simulator] Avertissement : configuration {config_path} ignorée ({e}).",
                  file=sys.stderr)

    for key in cfg:
        if key in os.environ:
            cfg[key] = os.environ[key]

    cfg["MIRTH_BASE_URL"] = str(cfg["MIRTH_BASE_URL"]).rstrip("/")
    return cfg


def parse_base_url(base_url):
    """Décompose l'URL de base en (scheme, host, port, prefix de chemin)."""
    parsed = urllib.parse.urlparse(base_url)
    scheme = parsed.scheme or "https"
    host = parsed.hostname or "localhost"
    port = parsed.port or (8443 if scheme == "https" else 80)
    prefix = (parsed.path or "").rstrip("/")  # ex. "/api"
    return scheme, host, port, prefix


# ==============================================================================
# MODÈLE DE DONNÉES (état du simulateur, protégé par un verrou)
# ==============================================================================
STATE_LOCK = threading.RLock()
STATE = {"next_message_id": 1000, "channels": []}

# Libellés/identifiants des connecteurs (sous-canaux).
IN_NAME = "Source"          # sous-canal d'entrée (metaDataId 0)
OUT_NAME = "Destination 1"  # sous-canal de sortie (metaDataId 1)

# Fragments d'erreurs « SUXX » aléatoires, façon Mirth.
_ERROR_TEMPLATES = [
    "ERROR-SU01 Connection refused: l'extrémité HL7 distante n'a pas répondu",
    "ERROR-SU07 Transformer error: NullPointerException dans le script JavaScript",
    "ERROR-SU13 Timeout: aucune ACK reçue dans le délai imparti",
    "ERROR-SU21 Database write failed: deadlock détecté",
    "ERROR-SU34 Message validation failed: segment MSH manquant",
    "ERROR-SU42 SMTP dispatcher error: authentification refusée",
    "ERROR-SU55 File write error: accès refusé au répertoire de sortie",
    "ERROR-SU63 Queue overflow: la file du connecteur est saturée",
]


def _new_connector(name, meta_data_id, kind):
    return {
        "name": name,
        "metaDataId": meta_data_id,
        "kind": kind,  # "in" ou "out"
        "stats": {k: 0 for k in _STAT_KEYS},
    }


def _new_channel(index):
    return {
        "channel_id": str(uuid.uuid4()),
        "name": f"Simul_Client_{index}",
        "state": "STARTED",
        "connectors": [
            _new_connector(IN_NAME, 0, "in"),
            _new_connector(OUT_NAME, 1, "out"),
        ],
        "messages": [],  # messages en erreur conservés pour récupération
    }


def init_channels(count, ok_count, err_count, seed=None):
    """(Re)crée `count` canaux et injecte des données aléatoires d'init."""
    if seed is not None:
        random.seed(seed)
    with STATE_LOCK:
        STATE["next_message_id"] = 1000
        STATE["channels"] = [_new_channel(i) for i in range(1, count + 1)]
        if not STATE["channels"]:
            return
        for _ in range(ok_count):
            ch = random.choice(STATE["channels"])
            kind = random.choice(("in", "out"))
            _apply_ok(ch, kind)
        for _ in range(err_count):
            ch = random.choice(STATE["channels"])
            kind = random.choice(("in", "out"))
            _apply_error(ch, kind)


def _connector_by_kind(channel, kind):
    for c in channel["connectors"]:
        if c["kind"] == kind:
            return c
    return channel["connectors"][0]


def _apply_ok(channel, kind):
    """Comptabilise un message traité avec succès sur un sous-canal."""
    conn = _connector_by_kind(channel, kind)
    conn["stats"]["RECEIVED"] += 1
    conn["stats"]["SENT"] += 1


def _apply_error(channel, kind):
    """Comptabilise un message en erreur et le mémorise (récupérable via l'API)."""
    conn = _connector_by_kind(channel, kind)
    conn["stats"]["RECEIVED"] += 1
    conn["stats"]["ERROR"] += 1

    with STATE_LOCK:
        msg_id = STATE["next_message_id"]
        STATE["next_message_id"] += 1

    identifier = "SIM-%08X" % random.getrandbits(32)
    template = random.choice(_ERROR_TEMPLATES)
    now_ms = int(time.time() * 1000)
    message = {
        "messageId": msg_id,
        "received_ms": now_ms,
        "metaDataId": conn["metaDataId"],
        "connectorName": conn["name"],
        "status": "ERROR",
        "sendAttempts": random.randint(0, 3),
        "errorCode": random.choice([0, 1, 2, 4, 8, 16]),
        "identifier": identifier,
        "error": f"[{identifier}] {template}",
        "content": _fake_hl7(msg_id, identifier),
    }
    channel["messages"].append(message)
    return message


def _fake_hl7(msg_id, identifier):
    """Génère un message HL7 v2 factice servant de contenu brut."""
    ts = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    ctrl = "%s%06d" % (identifier, msg_id % 1000000)
    patient_id = random.randint(100000, 999999)
    return (
        f"MSH|^~\\&|SIMULATOR|SIM_FACILITY|RECEIVER|DEST|{ts}||ADT^A01|{ctrl}|P|2.5\r"
        f"EVN|A01|{ts}\r"
        f"PID|1||{patient_id}^^^SIM^MR||DOE^JOHN||19800101|M|||1 RUE DU TEST^^PARIS^^75000^FR\r"
        f"PV1|1|I|SIM^101^A||||0123^SMITH^JANE|||MED\r"
    )


def channel_stats(channel):
    """Agrège les statistiques des connecteurs au niveau du canal."""
    agg = {k: 0 for k in _STAT_KEYS}
    for c in channel["connectors"]:
        for k in _STAT_KEYS:
            agg[k] += c["stats"].get(k, 0)
    return agg


# ==============================================================================
# DUMP / LOAD DE LA MÉMOIRE
# ==============================================================================
def dump_memory(path=DUMP_FILE):
    with STATE_LOCK:
        data = json.loads(json.dumps(STATE))  # copie profonde sûre
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    return os.path.abspath(path)


def load_memory(path=DUMP_FILE):
    if not os.path.exists(path):
        return False
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, dict) or "channels" not in data:
        raise ValueError("fichier de mémoire invalide")
    with STATE_LOCK:
        STATE["next_message_id"] = int(data.get("next_message_id", 1000))
        STATE["channels"] = data["channels"]
    return True


# ==============================================================================
# SÉRIALISATION AU FORMAT MIRTH (reproduit les réponses JSON du vrai serveur)
# ==============================================================================
def _stats_entries(stats):
    """Sérialise un dict de stats au format Map<Status,Long> de Mirth.

    Forme produite (la plus proche du vrai serveur) :
        {"entry": [{"com.mirth...Status": "RECEIVED", "long": 12}, ...]}
    """
    return [{_STATUS_CLASS: k, "long": int(stats.get(k, 0))} for k in _STAT_KEYS]


def _connector_status_json(channel, connector):
    return {
        "channelId": channel["channel_id"],
        "name": connector["name"],
        "metaDataId": connector["metaDataId"],
        "state": channel["state"],
        "statistics": {"entry": _stats_entries(connector["stats"])},
    }


def _channel_status_json(channel):
    stats = channel_stats(channel)
    return {
        "channelId": channel["channel_id"],
        "name": channel["name"],
        "state": channel["state"],
        "queued": stats.get("QUEUED", 0),
        "statistics": {"entry": _stats_entries(stats)},
        "childStatuses": {
            "dashboardStatus": [
                _connector_status_json(channel, c) for c in channel["connectors"]
            ]
        },
    }


def json_channel_statuses():
    with STATE_LOCK:
        return {"list": {"dashboardStatus": [
            _channel_status_json(c) for c in STATE["channels"]]}}


def json_channel_statistics():
    """Endpoint d'appoint /channels/statistics (champs simples)."""
    with STATE_LOCK:
        items = []
        for ch in STATE["channels"]:
            s = channel_stats(ch)
            items.append({
                "channelId": ch["channel_id"],
                "received": s["RECEIVED"],
                "filtered": s["FILTERED"],
                "queued": s["QUEUED"],
                "sent": s["SENT"],
                "error": s["ERROR"],
            })
    return {"list": {"channelStatistics": items}}


def _message_json(msg):
    """Sérialise un message en erreur au format /channels/{id}/messages de Mirth."""
    received = {"time": msg["received_ms"], "timezone": "Europe/Paris"}
    connector_message = {
        "connectorName": msg["connectorName"],
        "metaDataId": msg["metaDataId"],
        "status": msg["status"],
        "receivedDate": received,
        "sendAttempts": msg["sendAttempts"],
        "errorCode": msg["errorCode"],
        "processingError": msg["error"],
        "raw": {"content": msg["content"]},
    }
    return {
        "messageId": msg["messageId"],
        "receivedDate": received,
        "connectorMessages": {
            "entry": [{"int": msg["metaDataId"], "connectorMessage": connector_message}]
        },
    }


def json_channel_messages(channel_id, statuses, limit):
    """Messages (en erreur) d'un canal, filtrés par statut, limités à `limit`."""
    wanted = {s.upper() for s in statuses} if statuses else None
    with STATE_LOCK:
        channel = next((c for c in STATE["channels"]
                        if c["channel_id"] == channel_id), None)
        if channel is None:
            return {"list": {"message": []}}
        selected = [m for m in channel["messages"]
                    if wanted is None or m["status"].upper() in wanted]
        selected = list(reversed(selected))[:limit]  # plus récents en tête
        return {"list": {"message": [_message_json(m) for m in selected]}}


def json_system_info():
    return {
        "jvmVersion": f"{sys.version_info.major}.{sys.version_info.minor} (simulateur)",
        "osName": "Mirth Simulator",
        "osVersion": _SERVER_VERSION,
        "osArchitecture": "x86_64",
    }


def json_system_stats():
    return {
        "cpuUsagePct": round(random.uniform(1.0, 25.0), 2),
        "allocatedMemoryBytes": 512 * 1024 * 1024,
        "freeMemoryBytes": random.randint(128, 480) * 1024 * 1024,
        "maxMemoryBytes": 1024 * 1024 * 1024,
    }


# ==============================================================================
# SERVEUR HTTP(S) — émule l'API REST de Mirth Connect
# ==============================================================================
SESSIONS = set()           # JSESSIONID valides émis par /users/_login
SESSIONS_LOCK = threading.Lock()
SERVER_CONFIG = {}         # rempli au démarrage (user, password, prefix, verbose)


class MirthSimHandler(BaseHTTPRequestHandler):
    server_version = f"MirthSimulator/{_SERVER_VERSION}"
    protocol_version = "HTTP/1.1"

    # --- utilitaires de réponse -------------------------------------------
    def _send(self, status, body=b"", content_type="application/json",
              extra_headers=None):
        if isinstance(body, str):
            body = body.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        for k, v in (extra_headers or {}):
            self.send_header(k, v)
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(body)

    def _send_json(self, obj, status=200):
        self._send(status, json.dumps(obj, ensure_ascii=False), "application/json")

    def _path_parts(self):
        parsed = urllib.parse.urlparse(self.path)
        prefix = SERVER_CONFIG["prefix"]
        path = parsed.path
        if prefix and path.startswith(prefix):
            path = path[len(prefix):]
        if not path.startswith("/"):
            path = "/" + path
        return path, urllib.parse.parse_qs(parsed.query, keep_blank_values=True)

    def _cookie_session(self):
        raw = self.headers.get("Cookie", "")
        for part in raw.split(";"):
            if "=" in part:
                name, _, value = part.strip().partition("=")
                if name == "JSESSIONID":
                    return value
        return None

    def _authenticated(self):
        sid = self._cookie_session()
        if not sid:
            return False
        with SESSIONS_LOCK:
            return sid in SESSIONS

    def _read_body_form(self):
        length = int(self.headers.get("Content-Length") or 0)
        raw = self.rfile.read(length) if length else b""
        return urllib.parse.parse_qs(raw.decode("utf-8", errors="replace"))

    # --- routage -----------------------------------------------------------
    def do_GET(self):
        try:
            self._route_get()
        except BrokenPipeError:
            pass
        except Exception as e:  # ne jamais laisser le handler crasher le thread
            self._safe_error(500, str(e))

    do_HEAD = do_GET

    def do_POST(self):
        try:
            self._route_post()
        except BrokenPipeError:
            pass
        except Exception as e:
            self._safe_error(500, str(e))

    def _safe_error(self, status, msg):
        try:
            self._send_json({"error": msg}, status=status)
        except Exception:
            pass

    def _route_post(self):
        path, _ = self._path_parts()

        if path == "/users/_login":
            form = self._read_body_form()
            user = (form.get("username") or [""])[0]
            password = (form.get("password") or [""])[0]
            if (user == SERVER_CONFIG["user"]
                    and password == SERVER_CONFIG["password"]):
                sid = uuid.uuid4().hex.upper()
                with SESSIONS_LOCK:
                    SESSIONS.add(sid)
                self._send(200, "<com.mirth.connect.model.LoginStatus>"
                                "<status>SUCCESS</status></com.mirth.connect.model.LoginStatus>",
                           content_type="application/xml",
                           extra_headers=[("Set-Cookie",
                                           f"JSESSIONID={sid}; Path=/; HttpOnly")])
            else:
                self._send_json({"error": "Identifiants invalides"}, status=401)
            return

        if path == "/users/_logout":
            sid = self._cookie_session()
            if sid:
                with SESSIONS_LOCK:
                    SESSIONS.discard(sid)
            self._send(204)
            return

        self._send_json({"error": "Not found"}, status=404)

    def _route_get(self):
        path, query = self._path_parts()

        # /server/version est public dans Mirth (texte brut).
        if path == "/server/version":
            self._send(200, _SERVER_VERSION, content_type="text/plain")
            return

        # Toutes les autres ressources nécessitent une session valide.
        if not self._authenticated():
            self._send_json({"error": "Authentification requise"}, status=401)
            return

        if path == "/system/info":
            self._send_json(json_system_info())
            return
        if path == "/system/stats":
            self._send_json(json_system_stats())
            return
        if path == "/channels/statuses":
            self._send_json(json_channel_statuses())
            return
        if path == "/channels/statistics":
            self._send_json(json_channel_statistics())
            return

        m = re.match(r"^/channels/([^/]+)/messages$", path)
        if m:
            channel_id = urllib.parse.unquote(m.group(1))
            statuses = query.get("status")  # liste de statuts répétés
            try:
                limit = int((query.get("limit") or ["50"])[0])
            except ValueError:
                limit = 50
            self._send_json(json_channel_messages(channel_id, statuses, limit))
            return

        self._send_json({"error": "Not found"}, status=404)

    # Journalisation : silencieuse sauf en mode verbeux.
    def log_message(self, fmt, *args):
        if SERVER_CONFIG.get("verbose"):
            sys.stderr.write("[http] %s - %s\n" % (self.address_string(), fmt % args))


def _write_temp_cert():
    """Écrit le cert/clé embarqués dans des fichiers temporaires (requis par ssl)."""
    tmp_dir = tempfile.mkdtemp(prefix="mirth_sim_")
    cert_path = os.path.join(tmp_dir, "cert.pem")
    key_path = os.path.join(tmp_dir, "key.pem")
    with open(cert_path, "w") as f:
        f.write(_EMBEDDED_CERT_PEM)
    with open(key_path, "w") as f:
        f.write(_EMBEDDED_KEY_PEM)

    def _cleanup():
        for p in (cert_path, key_path):
            try:
                os.remove(p)
            except OSError:
                pass
        try:
            os.rmdir(tmp_dir)
        except OSError:
            pass

    atexit.register(_cleanup)
    return cert_path, key_path


def start_server(bind_host, port, use_tls):
    """Démarre le serveur HTTP(S) dans un thread démon. Retourne l'instance."""
    httpd = ThreadingHTTPServer((bind_host, port), MirthSimHandler)
    if use_tls:
        cert_path, key_path = _write_temp_cert()
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        ctx.load_cert_chain(certfile=cert_path, keyfile=key_path)
        httpd.socket = ctx.wrap_socket(httpd.socket, server_side=True)

    thread = threading.Thread(target=httpd.serve_forever, name="mirth-sim-http",
                              daemon=True)
    thread.start()
    return httpd


# ==============================================================================
# AFFICHAGE CONSOLE
# ==============================================================================
def _safe_print(text=""):
    try:
        print(text)
    except UnicodeEncodeError:
        enc = sys.stdout.encoding or "utf-8"
        print(text.encode(enc, errors="replace").decode(enc))


def _clear_screen():
    os.system("cls" if os.name == "nt" else "clear")


def _read_line_console(prompt=""):
    """Lit une ligne directement depuis la console Windows (clavier réel).

    Contourne toute redirection de stdin : `launch.bat` lance le script via
    `echo. | python ...`, ce qui ferme stdin ; `input()` y verrait un EOF
    immédiat et le programme s'arrêterait. `msvcrt.getwch()` lit, lui, le
    clavier de la fenêtre console. Gère l'écho, le retour arrière, Entrée,
    Ctrl+C (KeyboardInterrupt) et Ctrl+Z (EOFError).
    """
    sys.stdout.write(prompt)
    sys.stdout.flush()
    buf = []
    while True:
        ch = msvcrt.getwch()
        if ch in ("\r", "\n"):
            sys.stdout.write("\n")
            sys.stdout.flush()
            return "".join(buf)
        if ch == "\x03":            # Ctrl+C
            raise KeyboardInterrupt
        if ch == "\x1a":            # Ctrl+Z
            raise EOFError
        if ch in ("\x00", "\xe0"):  # touche spéciale (flèches, F1...) : 2 octets
            msvcrt.getwch()
            continue
        if ch == "\b":              # retour arrière
            if buf:
                buf.pop()
                sys.stdout.write("\b \b")
                sys.stdout.flush()
            continue
        buf.append(ch)
        sys.stdout.write(ch)        # écho du caractère saisi
        sys.stdout.flush()


def read_line(prompt=""):
    """Lit une ligne au clavier (console Windows si possible, sinon stdin)."""
    if msvcrt is not None:
        try:
            return _read_line_console(prompt)
        except (KeyboardInterrupt, EOFError):
            raise
        except Exception:
            pass  # pas de console accessible : repli sur stdin
    return input(prompt)


def render_screen(base_url, banner=True):
    """Affiche l'état des canaux et de leurs sous-canaux sous forme de liste."""
    if banner:
        _clear_screen()
    _safe_print("=" * 78)
    _safe_print(" MIRTH SIMULATOR  —  API REST émulée sur  %s" % base_url)
    _safe_print("=" * 78)

    with STATE_LOCK:
        channels = STATE["channels"]
        grand = {k: 0 for k in _STAT_KEYS}
        if not channels:
            _safe_print(" (aucun canal)")
        for idx, ch in enumerate(channels, start=1):
            cs = channel_stats(ch)
            for k in _STAT_KEYS:
                grand[k] += cs[k]
            n_err_msgs = len(ch["messages"])
            _safe_print(" [%d] %-18s  %-8s  reçus=%-4d filtrés=%-4d envoyés=%-4d "
                        "ERREURS=%-4d  (msgs err: %d)"
                        % (idx, ch["name"], ch["state"], cs["RECEIVED"],
                           cs["FILTERED"], cs["SENT"], cs["ERROR"], n_err_msgs))
            for conn in ch["connectors"]:
                s = conn["stats"]
                tag = "IN " if conn["kind"] == "in" else "OUT"
                _safe_print("       %s %-14s  reçus=%-4d envoyés=%-4d erreurs=%-4d"
                            % (tag, "(%s)" % conn["name"], s["RECEIVED"],
                               s["SENT"], s["ERROR"]))

    _safe_print("-" * 78)
    _safe_print(" TOTAL  reçus=%d  filtrés=%d  envoyés=%d  ERREURS=%d"
                % (grand["RECEIVED"], grand["FILTERED"], grand["SENT"], grand["ERROR"]))
    _safe_print("-" * 78)
    _safe_print(" Commandes : m1i / e2o (canal+i/o) | m, e (interactif) | "
                "D dump | L load | r refresh | h aide | q quit")
    _safe_print("=" * 78)


def print_help():
    _safe_print("""
Commandes disponibles
---------------------
  m<canal><i|o>   Message OK sur un sous-canal      (ex. m1i  = canal 1, IN)
  e<canal><i|o>   Message ERREUR sur un sous-canal  (ex. e2o  = canal 2, OUT)
  m  /  e         Idem mais demande le canal puis le sous-canal (i/o)
  D               Sauvegarde la mémoire dans %s
  L               Recharge la mémoire depuis %s (s'il existe)
  r               Rafraîchit l'affichage
  h               Affiche cette aide
  q               Quitte le simulateur

  i = IN  (sous-canal Source / entrée)
  o = OUT (sous-canal Destination / sortie)
""" % (DUMP_FILE, DUMP_FILE))


# ==============================================================================
# BOUCLE INTERACTIVE (clavier)
# ==============================================================================
_CMD_RE = re.compile(r"^([me])\s*(\d+)?\s*([ioIO])?$")


def _resolve_channel(num):
    """Renvoie le canal d'index 1-based, ou None."""
    with STATE_LOCK:
        if 1 <= num <= len(STATE["channels"]):
            return STATE["channels"][num - 1]
    return None


def _prompt_channel_and_kind():
    """Demande interactivement le canal puis le sous-canal."""
    with STATE_LOCK:
        n = len(STATE["channels"])
    if n == 0:
        _safe_print("  Aucun canal.")
        return None, None
    try:
        raw_ch = read_line("  Canal (1-%d) : " % n).strip()
        num = int(raw_ch)
    except (ValueError, EOFError):
        _safe_print("  Canal invalide.")
        return None, None
    channel = _resolve_channel(num)
    if channel is None:
        _safe_print("  Canal hors plage.")
        return None, None
    raw_kind = read_line("  Sous-canal (i=IN / o=OUT) : ").strip().lower()
    if raw_kind not in ("i", "o"):
        _safe_print("  Sous-canal invalide.")
        return None, None
    return channel, ("in" if raw_kind == "i" else "out")


def handle_command(line, base_url):
    """Traite une ligne de commande. Retourne False pour quitter."""
    cmd = line.strip()
    if not cmd:
        return True

    low = cmd.lower()

    if low == "q":
        return False
    if low == "h":
        print_help()
        return True
    if low == "r":
        render_screen(base_url)
        return True
    if cmd == "D" or low == "d" and len(cmd) == 1:
        try:
            path = dump_memory()
            _safe_print("  Mémoire sauvegardée dans %s" % path)
        except Exception as e:
            _safe_print("  Échec du dump : %s" % e)
        return True
    if cmd == "L" or low == "l" and len(cmd) == 1:
        try:
            if load_memory():
                _safe_print("  Mémoire rechargée depuis %s" % DUMP_FILE)
                render_screen(base_url)
            else:
                _safe_print("  Aucun fichier %s à charger." % DUMP_FILE)
        except Exception as e:
            _safe_print("  Échec du load : %s" % e)
        return True

    m = _CMD_RE.match(cmd)
    if m:
        action = m.group(1).lower()      # 'm' ou 'e'
        num = m.group(2)
        kind_char = m.group(3)

        if num and kind_char:
            channel = _resolve_channel(int(num))
            if channel is None:
                _safe_print("  Canal hors plage.")
                return True
            kind = "in" if kind_char.lower() == "i" else "out"
        else:
            channel, kind = _prompt_channel_and_kind()
            if channel is None:
                return True

        if action == "m":
            _apply_ok(channel, kind)
            _safe_print("  + Message OK sur %s / %s"
                        % (channel["name"], "IN" if kind == "in" else "OUT"))
        else:
            msg = _apply_error(channel, kind)
            _safe_print("  ! Erreur sur %s / %s  ->  id=%s  %s"
                        % (channel["name"], "IN" if kind == "in" else "OUT",
                           msg["messageId"], msg["identifier"]))
        render_screen(base_url)
        return True

    _safe_print("  Commande inconnue : %r  (h pour l'aide)" % cmd)
    return True


def interactive_loop(base_url):
    render_screen(base_url)
    while True:
        try:
            line = read_line("mirth-sim> ")
        except (EOFError, KeyboardInterrupt):
            _safe_print("")
            break
        try:
            if not handle_command(line, base_url):
                break
        except Exception as e:
            _safe_print("  Erreur de commande : %s" % e)


# ==============================================================================
# POINT D'ENTRÉE
# ==============================================================================
def main():
    cfg = load_config()
    scheme, url_host, url_port, prefix = parse_base_url(cfg["MIRTH_BASE_URL"])

    parser = argparse.ArgumentParser(
        description="Simulateur autonome de l'API REST de Mirth Connect.")
    parser.add_argument("-c", "--channels", type=int, default=3,
                        help="Nombre de canaux Simul_Client_X à créer (def: 3)")
    parser.add_argument("--ok", type=int, default=15,
                        help="Nombre de messages OK d'initialisation (def: 15)")
    parser.add_argument("--errors", type=int, default=4,
                        help="Nombre de messages en erreur d'initialisation (def: 4)")
    parser.add_argument("--host", type=str, default=None,
                        help="Adresse d'écoute (def: host de l'URL de config)")
    parser.add_argument("--port", type=int, default=None,
                        help="Port d'écoute (def: port de l'URL de config)")
    parser.add_argument("--seed", type=int, default=None,
                        help="Graine aléatoire (reproductibilité de l'init)")
    parser.add_argument("--load", action="store_true",
                        help="Charge %s au démarrage s'il existe" % DUMP_FILE)
    parser.add_argument("-v", "--verbose", action="store_true",
                        help="Journalise les requêtes HTTP reçues")
    args = parser.parse_args()

    port = args.port or url_port
    use_tls = (scheme == "https")
    # Hôte d'écoute : 'localhost' -> 127.0.0.1 (le client vise localhost).
    bind_host = args.host or url_host or "127.0.0.1"

    SERVER_CONFIG.update({
        "user": cfg["MIRTH_USER"],
        "password": cfg["MIRTH_PASSWORD"],
        "prefix": prefix,
        "verbose": args.verbose,
    })

    # Données initiales.
    init_channels(args.channels, args.ok, args.errors, seed=args.seed)
    if args.load:
        try:
            if load_memory():
                _safe_print("Mémoire rechargée depuis %s" % DUMP_FILE)
        except Exception as e:
            _safe_print("Échec du chargement de %s : %s" % (DUMP_FILE, e))

    # Démarrage du serveur.
    try:
        httpd = start_server(bind_host, port, use_tls)
    except OSError as e:
        _safe_print("Impossible d'ouvrir le port %s:%d : %s" % (bind_host, port, e))
        sys.exit(1)

    base_url = cfg["MIRTH_BASE_URL"]
    _safe_print("Serveur %s démarré sur %s://%s:%d%s"
                % ("HTTPS" if use_tls else "HTTP", scheme, bind_host, port, prefix))
    _safe_print("Identifiants acceptés : %s / %s"
                % (SERVER_CONFIG["user"], SERVER_CONFIG["password"]))
    _safe_print("Le client mirth_api.py peut interroger : %s" % base_url)
    time.sleep(0.3)

    try:
        interactive_loop(base_url)
    finally:
        _safe_print("Arrêt du serveur...")
        try:
            httpd.shutdown()
        except Exception:
            pass


if __name__ == "__main__":
    main()
