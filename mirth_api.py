#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Mirth API
=========
Client et librairie pour l'API REST de Mirth Connect (NextGen Connect).

Conçu sur le même modèle que `system_state.py` et `mirth_logs_parser.py` :

  * PARTIE 1 — la librairie : des fonctions `get_*` réutilisables (importables)
    qui ne lèvent jamais d'exception réseau (toute erreur revient dans le champ
    `error` du dictionnaire renvoyé, avec `reachable=False`) ;
  * PARTIE 2 — la section `main` : un petit outil CLI qui interroge le serveur et
    affiche un rapport sous forme de tableaux `tabulate`.

Aucune dépendance externe pour la partie librairie (uniquement la librairie
standard : `urllib`, `http.cookiejar`, `ssl`, `json`). Le serveur Mirth exposant
en général un certificat auto-signé sur son port HTTPS (8443 par défaut), la
vérification TLS est désactivée par défaut.

Résolution de la configuration (ordre de priorité décroissante) :
    1. variables d'environnement (MIRTH_BASE_URL, MIRTH_USER, MIRTH_PASSWORD,
       MIRTH_VERIFY_SSL, MIRTH_PROCESS) ;
    2. fichier `.mirth_config.py` à la racine du projet (git-ignoré) ;
    3. valeurs par défaut ci-dessous.

Points d'entrée haut niveau (ne lèvent jamais) :
    get_overview()           -> vue d'ensemble complète (serveur + canaux + totaux)
    get_channels_overview()  -> liste des canaux et de leurs statistiques
    get_global_statistics()  -> statistiques agrégées (tous canaux confondus)
    get_server_info()        -> version, infos JVM/OS, statistiques système
    get_errors()             -> canaux en erreur (statistique ERROR > 0 ou état d'erreur)
"""

import os
import sys
import ssl
import json
import datetime
import importlib.util
import urllib.parse
import urllib.request
import http.cookiejar

# ==============================================================================
# PARTIE 1 : LA LIBRAIRIE
# ==============================================================================

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
    _BASE_DIR = os.path.dirname(os.path.abspath(__file__))


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


# --------------------------------------------------------------------------
# SESSION HTTP AUTHENTIFIÉE
# --------------------------------------------------------------------------
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
        ctx = ssl.create_default_context()
        if not self.verify_ssl:
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

    def get_channel_statuses_raw(self):
        """Réponse brute de /channels/statuses (statuts tableau de bord), ou None."""
        try:
            return self._get_json("/channels/statuses")
        except Exception:
            return None

    def get_channel_statistics_raw(self):
        """Réponse brute de /channels/statistics (statistiques dédiées), ou None.

        Endpoint complémentaire (selon version Mirth) renvoyant directement des
        objets ChannelStatistics aux champs simples (received/sent/error/...).
        Sert de source d'appoint pour fiabiliser les colonnes si le parsing des
        statuts tableau de bord laisse des valeurs manquantes.
        """
        try:
            return self._get_json("/channels/statistics")
        except Exception:
            return None

    def get_channels(self):
        """Liste normalisée des canaux déployés avec leurs statistiques.

        Renvoie une liste de dicts {name, channel_id, state, received, filtered,
        queued, sent, error}. Liste vide si aucun canal ou en cas d'erreur. Les
        statistiques manquantes dans les statuts tableau de bord sont complétées,
        si possible, par l'endpoint dédié /channels/statistics.
        """
        channels = _parse_dashboard_statuses(self.get_channel_statuses_raw())

        # Complément éventuel via l'endpoint dédié (clé = channel_id).
        if channels and any(_needs_stats(c) for c in channels):
            extra = _parse_channel_statistics(self.get_channel_statistics_raw())
            if extra:
                for c in channels:
                    src = extra.get(c.get("channel_id"))
                    if not src:
                        continue
                    for k in ("received", "filtered", "queued", "sent", "error"):
                        if c.get(k) is None and src.get(k) is not None:
                            c[k] = src[k]
        return channels

    def get_messages_raw(self, channel_id, status="ERROR", limit=50, offset=0,
                         include_content=True):
        """Réponse brute de /channels/{id}/messages (recherche de messages), ou None.

        `status` peut être une chaîne (ex. "ERROR") ou une liste de statuts ; chaque
        statut devient un paramètre `status` répété, comme l'attend l'API Mirth. Le
        contenu intégral des messages est inclus par défaut (`includeContent=true`).
        """
        params = [("offset", offset), ("limit", limit),
                  ("includeContent", "true" if include_content else "false")]
        for st in _as_list(status):
            if st:
                params.append(("status", str(st).upper()))
        query = urllib.parse.urlencode(params)
        path = "/channels/%s/messages?%s" % (
            urllib.parse.quote(str(channel_id), safe=""), query)
        try:
            return self._get_json(path)
        except Exception:
            return None


# --------------------------------------------------------------------------
# PARSING DÉFENSIF DES STATUTS DE CANAUX (format JSON variable selon version)
# --------------------------------------------------------------------------
# Statuts de statistiques connus de Mirth (clés normalisées en MAJUSCULES).
_STATUS_KEYS = {"RECEIVED", "FILTERED", "TRANSFORMED", "SENT", "ERROR", "QUEUED", "QUEUE"}


def _as_list(value):
    """Mirth sérialise une collection à 1 élément comme un objet, sinon une liste."""
    if value is None:
        return []
    return value if isinstance(value, list) else [value]


def _coerce_int(value):
    """Convertit en entier si possible, sinon renvoie la valeur telle quelle."""
    try:
        return int(value)
    except (TypeError, ValueError):
        return value


def _parse_statistics(stats):
    """Extrait {RECEIVED, FILTERED, SENT, ERROR, ...} d'un bloc `statistics`.

    Tolère les multiples formes de sérialisation rencontrées selon la version de
    Mirth et le pilote JSON utilisé (XStream/Jettison) :

      * dict simple ............ {"RECEIVED": 12, "SENT": 12, ...}
      * map « entry » + classe . {"entry": [{"com.mirth...Status": "RECEIVED",
                                             "long": 12}, ...]}
      * map « entry » + array .. {"entry": [["RECEIVED", 12], ...]}
      * map « entry » + string . {"entry": [{"string": "RECEIVED", "long": 12}]}
      * entrée unique non listée  (objet au lieu d'un tableau)

    La clé d'une entrée est repérée comme la première valeur textuelle qui n'est
    pas le compteur (champ long/int/value) — ce qui couvre le cas où le nom du
    champ de clé est le nom pleinement qualifié de l'énumération Status.
    """
    out = {}
    if stats is None:
        return out

    if isinstance(stats, list):
        entries = stats
    elif isinstance(stats, dict):
        if "entry" in stats:
            entries = _as_list(stats.get("entry"))
        else:
            # dict simple {clé: valeur}
            for k, v in stats.items():
                out[str(k).upper()] = _coerce_int(v)
            return out
    else:
        return out

    for e in entries:
        key = None
        val = None
        if isinstance(e, (list, tuple)):
            if len(e) >= 2:
                key, val = e[0], e[1]
        elif isinstance(e, dict):
            # Le compteur est sous long / int / value.
            val = e.get("long", e.get("int", e.get("value")))
            # La clé : champ explicite, sinon première valeur textuelle restante.
            key = e.get("string") or e.get("key")
            if key is None:
                for fk, fv in e.items():
                    if fk in ("long", "int", "value"):
                        continue
                    if isinstance(fv, str):
                        key = fv
                        break
        if key is None:
            continue
        out[str(key).upper()] = _coerce_int(val)
    return out


def _stats_to_channel(stats):
    """Mappe un dict de statistiques normalisées vers les colonnes du canal."""
    return {
        "received": stats.get("RECEIVED"),
        "filtered": stats.get("FILTERED"),
        "queued": stats.get("QUEUED", stats.get("QUEUE")),
        "sent": stats.get("SENT"),
        "error": stats.get("ERROR"),
    }


def _aggregate_child_statistics(status):
    """Agrège les statistiques des connecteurs enfants d'un statut de canal.

    Filet de secours lorsque le statut de canal lui-même ne porte pas de bloc
    `statistics` exploitable : on additionne celles de ses connecteurs (source +
    destinations) exposées dans `childStatuses`.
    """
    child = status.get("childStatuses")
    if isinstance(child, dict):
        children = _as_list(child.get("dashboardStatus"))
    else:
        children = _as_list(child)

    agg = {}
    for c in children:
        if not isinstance(c, dict):
            continue
        for k, v in _parse_statistics(c.get("statistics")).items():
            if isinstance(v, int):
                agg[k] = agg.get(k, 0) + v
    return agg


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
        if not stats:
            # Pas de statistiques au niveau canal : agrégation des connecteurs.
            stats = _aggregate_child_statistics(s)
        cols = _stats_to_channel(stats)
        # `queued` peut être un champ propre au statut (hors bloc statistics).
        if cols["queued"] is None and s.get("queued") is not None:
            cols["queued"] = _coerce_int(s.get("queued"))
        channels.append({
            "name": s.get("name"),
            "channel_id": s.get("channelId"),
            "state": s.get("state"),
            **cols,
        })
    return channels


def _parse_channel_statistics(data):
    """Normalise /channels/statistics en map {channel_id: {received, ...}}.

    Les objets ChannelStatistics exposent des champs simples (received, sent,
    error, filtered, queued) — source d'appoint très fiable quand elle existe.
    """
    if not isinstance(data, dict):
        return {}
    container = data.get("list", data)
    if isinstance(container, dict):
        items = _as_list(container.get("channelStatistics")
                         or container.get("statistics"))
    else:
        items = _as_list(container)

    out = {}
    for it in items:
        if not isinstance(it, dict):
            continue
        cid = it.get("channelId") or it.get("channelid")
        if not cid:
            continue
        out[cid] = {
            "received": _coerce_int(it.get("received")),
            "filtered": _coerce_int(it.get("filtered")),
            "queued": _coerce_int(it.get("queued")),
            "sent": _coerce_int(it.get("sent")),
            "error": _coerce_int(it.get("error")),
        }
    return out


def _needs_stats(channel):
    """True si au moins une colonne de statistiques du canal est manquante."""
    return any(channel.get(k) is None
               for k in ("received", "filtered", "queued", "sent", "error"))


def compute_totals(channels):
    """Somme les statistiques de tous les canaux (les valeurs nulles comptent 0)."""
    totals = {"received": 0, "filtered": 0, "queued": 0, "sent": 0, "error": 0}
    for c in channels:
        for k in totals:
            v = c.get(k)
            if isinstance(v, int):
                totals[k] += v
    return totals


# --------------------------------------------------------------------------
# PARSING DÉFENSIF DES MESSAGES EN ERREUR (/channels/{id}/messages)
# --------------------------------------------------------------------------
# Champs d'erreur portés par un message de connecteur, avec leur libellé lisible.
_ERROR_FIELDS = (
    ("processingError", "Traitement"),
    ("postProcessorError", "Post-traitement"),
    ("responseError", "Réponse"),
)


def _fmt_mirth_date(value):
    """Convertit un horodatage Mirth en 'YYYY-MM-DD HH:MM:SS'.

    Mirth sérialise les dates en `{"time": <epoch_ms>, "timezone": ...}` ; on
    tolère aussi une valeur déjà textuelle (renvoyée telle quelle) ou nulle.
    """
    if value is None:
        return None
    if isinstance(value, dict):
        t = value.get("time")
        if t is None:
            return None
        try:
            return datetime.datetime.fromtimestamp(
                int(t) / 1000.0).strftime("%Y-%m-%d %H:%M:%S")
        except (TypeError, ValueError, OSError):
            return str(t)
    return str(value)


def _content_text(connector, *keys):
    """Extrait le contenu textuel d'un message de connecteur.

    Les blocs `raw`/`encoded`/`sent` sont des objets MessageContent dont le texte
    est sous la clé `content` ; on renvoie le premier non vide rencontré.
    """
    for k in keys:
        c = connector.get(k)
        if isinstance(c, dict):
            txt = c.get("content")
            if txt:
                return txt
        elif isinstance(c, str) and c:
            return c
    return None


def _iter_connector_messages(message):
    """Itère les messages de connecteur (source + destinations) d'un message.

    `connectorMessages` est une Map<Integer, ConnectorMessage> sérialisée de façon
    variable selon la version (entry+objet, entry+tableau, ou dict simple) ; on
    renvoie une liste de dicts ConnectorMessage.
    """
    cm = message.get("connectorMessages")
    if isinstance(cm, dict):
        entries = _as_list(cm.get("entry")) if "entry" in cm else list(cm.values())
    elif isinstance(cm, list):
        entries = cm
    else:
        return []

    out = []
    for e in entries:
        conn = None
        if isinstance(e, dict):
            conn = e.get("connectorMessage")
            if conn is None:
                # entry = {clé: ConnectorMessage} : on prend la 1re valeur exploitable.
                for v in e.values():
                    if isinstance(v, dict) and ("status" in v or "connectorName" in v):
                        conn = v
                        break
                if conn is None and ("status" in e or "connectorName" in e):
                    conn = e   # l'entrée est elle-même le ConnectorMessage
        elif isinstance(e, (list, tuple)) and len(e) >= 2 and isinstance(e[1], dict):
            conn = e[1]
        if isinstance(conn, dict):
            out.append(conn)
    return out


def _parse_messages(data, channel_id=None, channel_name=None):
    """Normalise /channels/{id}/messages en liste de messages de connecteur en erreur.

    Renvoie une liste de dicts {channel_id, channel_name, message_id, connector,
    meta_data_id, status, received_date, send_attempts, error_code, category,
    error, content}. Ne conserve que les connecteurs en statut ERROR ou portant un
    texte d'erreur.
    """
    if isinstance(data, dict):
        container = data.get("list", data)
        if isinstance(container, dict):
            messages = _as_list(container.get("message"))
        else:
            messages = _as_list(container)
    else:
        messages = _as_list(data)

    items = []
    for m in messages:
        if not isinstance(m, dict):
            continue
        msg_id = m.get("messageId")
        msg_date = _fmt_mirth_date(m.get("receivedDate"))
        for conn in _iter_connector_messages(m):
            status = (conn.get("status") or "").upper()
            errors, cats = [], []
            for field, label in _ERROR_FIELDS:
                txt = conn.get(field)
                if txt:
                    errors.append(txt if isinstance(txt, str) else str(txt))
                    cats.append(label)
            if status != "ERROR" and not errors:
                continue
            items.append({
                "channel_id": channel_id,
                "channel_name": channel_name,
                "message_id": msg_id,
                "connector": conn.get("connectorName") or "-",
                "meta_data_id": conn.get("metaDataId"),
                "status": conn.get("status") or status or "ERROR",
                "received_date": _fmt_mirth_date(conn.get("receivedDate")) or msg_date,
                "send_attempts": _coerce_int(conn.get("sendAttempts")),
                "error_code": _coerce_int(conn.get("errorCode")),
                "category": ", ".join(cats) if cats else (conn.get("connectorName") or "-"),
                "error": "\n\n".join(errors) if errors else None,
                "content": _content_text(conn, "raw", "encoded", "sent"),
            })
    return items


# --------------------------------------------------------------------------
# POINTS D'ENTRÉE HAUT NIVEAU (ne lèvent jamais)
# --------------------------------------------------------------------------
def _new_result(extra=None):
    cfg = get_config()
    base = {"reachable": False, "error": None, "base_url": cfg["MIRTH_BASE_URL"]}
    if extra:
        base.update(extra)
    return base


def _with_client(func, timeout=8):
    """Ouvre une session Mirth, exécute `func(client)`, ferme, sans jamais lever.

    Retourne (data, error_message). `data` est None si la connexion a échoué.
    """
    try:
        client = MirthClient(timeout=timeout)
        client.login()
    except Exception as e:
        return None, f"Connexion/authentification impossible : {e}"
    try:
        return func(client), None
    except Exception as e:
        return None, f"Erreur lors de la lecture des données : {e}"
    finally:
        client.logout()


def get_overview(timeout=8):
    """Interroge le serveur Mirth et renvoie une vue d'ensemble JSON-friendly.

    Ne lève jamais : en cas d'échec, renvoie {"reachable": False, "error": ...}.
    Contient version, infos/statistiques système (JVM), liste des canaux avec
    leurs statistiques, totaux agrégés et compteurs.
    """
    result = _new_result({
        "version": None, "system_stats": {}, "system_info": {},
        "channels": [], "channel_count": 0, "channels_started": 0,
        "totals": {"received": 0, "filtered": 0, "queued": 0, "sent": 0, "error": 0},
    })

    def _fetch(client):
        version = client.get_version()
        system_stats = client.get_system_stats()
        system_info = client.get_system_info()
        channels = client.get_channels()
        return version, system_stats, system_info, channels

    data, error = _with_client(_fetch, timeout=timeout)
    if data is None:
        result["error"] = error
        return result

    version, system_stats, system_info, channels = data
    result["version"] = version
    result["system_stats"] = system_stats
    result["system_info"] = system_info
    result["channels"] = channels
    result["channel_count"] = len(channels)
    result["channels_started"] = sum(
        1 for c in channels if (c.get("state") or "").upper() == "STARTED")
    result["totals"] = compute_totals(channels)
    result["reachable"] = True
    return result


def get_channels_overview(timeout=8):
    """Liste des canaux et de leurs statistiques (vue allégée, sans infos JVM)."""
    result = _new_result({"channels": [], "channel_count": 0,
                          "channels_started": 0,
                          "totals": {"received": 0, "filtered": 0, "queued": 0,
                                     "sent": 0, "error": 0}})
    data, error = _with_client(lambda c: c.get_channels(), timeout=timeout)
    if data is None:
        result["error"] = error
        return result
    result["channels"] = data
    result["channel_count"] = len(data)
    result["channels_started"] = sum(
        1 for c in data if (c.get("state") or "").upper() == "STARTED")
    result["totals"] = compute_totals(data)
    result["reachable"] = True
    return result


def get_global_statistics(timeout=8):
    """Statistiques agrégées sur l'ensemble des canaux (totaux + compteurs d'état)."""
    result = _new_result({
        "totals": {"received": 0, "filtered": 0, "queued": 0, "sent": 0, "error": 0},
        "channel_count": 0, "channels_started": 0, "channels_in_error": 0,
    })
    data, error = _with_client(lambda c: c.get_channels(), timeout=timeout)
    if data is None:
        result["error"] = error
        return result
    result["totals"] = compute_totals(data)
    result["channel_count"] = len(data)
    result["channels_started"] = sum(
        1 for c in data if (c.get("state") or "").upper() == "STARTED")
    result["channels_in_error"] = sum(
        1 for c in data if isinstance(c.get("error"), int) and c["error"] > 0)
    result["reachable"] = True
    return result


def get_server_info(timeout=8):
    """Version, informations JVM/OS et statistiques système du serveur Mirth."""
    result = _new_result({"version": None, "system_info": {}, "system_stats": {}})

    def _fetch(client):
        return (client.get_version(), client.get_system_info(),
                client.get_system_stats())

    data, error = _with_client(_fetch, timeout=timeout)
    if data is None:
        result["error"] = error
        return result
    result["version"], result["system_info"], result["system_stats"] = data
    result["reachable"] = True
    return result


def get_errors(timeout=8):
    """Canaux présentant des erreurs (statistique ERROR > 0) ou un état d'erreur."""
    result = _new_result({"channels": [], "error_count": 0, "total_errors": 0})
    data, error = _with_client(lambda c: c.get_channels(), timeout=timeout)
    if data is None:
        result["error"] = error
        return result

    faulty = []
    for c in data:
        err = c.get("error")
        state = (c.get("state") or "").upper()
        if (isinstance(err, int) and err > 0) or state in ("ERROR", "PAUSED"):
            faulty.append({
                "name": c.get("name"),
                "channel_id": c.get("channel_id"),
                "state": c.get("state"),
                "error": err,
                "received": c.get("received"),
                "sent": c.get("sent"),
            })
    faulty.sort(key=lambda x: (x["error"] or 0), reverse=True)
    result["channels"] = faulty
    result["error_count"] = len(faulty)
    result["total_errors"] = sum(c["error"] for c in faulty if isinstance(c["error"], int))
    result["reachable"] = True
    return result


def get_error_messages(channel_id=None, limit=50, timeout=8):
    """Messages en erreur d'un canal (ou de tous les canaux en erreur).

    Si `channel_id` est fourni, renvoie les messages en erreur de ce seul canal ;
    sinon, parcourt tous les canaux ayant au moins une erreur et agrège leurs
    messages. Chaque message renvoyé porte son `channel_id`/`channel_name`, ce qui
    permet d'afficher une liste unifiée côté client.

    Ne lève jamais : en cas d'échec, renvoie {"reachable": False, "error": ...}.
    Champs : messages (liste), count (total), channels (récapitulatif par canal),
    channel_name (si un canal unique a été demandé).
    """
    result = _new_result({"channel_id": channel_id, "channel_name": None,
                          "messages": [], "count": 0, "channels": []})

    def _fetch(client):
        channels = client.get_channels()
        names = {c.get("channel_id"): c.get("name") for c in channels}
        if channel_id:
            targets = [(channel_id, names.get(channel_id))]
        else:
            # Tous les canaux ayant au moins une erreur comptabilisée.
            targets = [(c.get("channel_id"), c.get("name")) for c in channels
                       if isinstance(c.get("error"), int) and c["error"] > 0]

        all_items, per_channel = [], []
        for cid, cname in targets:
            items = _parse_messages(client.get_messages_raw(cid, status="ERROR",
                                                            limit=limit),
                                    channel_id=cid, channel_name=cname)
            per_channel.append({"channel_id": cid, "name": cname,
                                "count": len(items)})
            all_items.extend(items)
        return all_items, per_channel, (names.get(channel_id) if channel_id else None)

    data, error = _with_client(_fetch, timeout=timeout)
    if data is None:
        result["error"] = error
        return result

    all_items, per_channel, cname = data
    # Tri décroissant par date d'erreur (les plus récentes en tête).
    all_items.sort(key=lambda x: x.get("received_date") or "", reverse=True)
    result["messages"] = all_items
    result["count"] = len(all_items)
    result["channels"] = per_channel
    result["channel_name"] = cname
    result["reachable"] = True
    return result


# ==============================================================================
# PARTIE 2 : SECTION MAIN (AFFICHAGE CLI)
# ==============================================================================

if __name__ == "__main__":
    import argparse
    from tabulate import tabulate

    def safe_print(text=""):
        """Affiche en gérant les terminaux Windows incapables d'encoder l'Unicode."""
        try:
            print(text)
        except UnicodeEncodeError:
            encoding = sys.stdout.encoding or "utf-8"
            try:
                print(text.encode(encoding, errors="replace").decode(encoding))
            except Exception:
                print(text.encode("ascii", errors="replace").decode("ascii"))

    def print_table(data, headers, tablefmt="fancy_grid"):
        """Tableau tabulate, repli ASCII si le terminal ne gère pas l'Unicode."""
        try:
            print(tabulate(data, headers=headers, tablefmt=tablefmt))
        except UnicodeEncodeError:
            try:
                safe_print(tabulate(data, headers=headers, tablefmt="grid"))
            except Exception:
                safe_print(tabulate(data, headers=headers, tablefmt="simple"))

    def display_header(title):
        safe_print(f"\n{'='*20} {title} {'='*20}")

    def fmt(v):
        return "-" if v is None else v

    parser = argparse.ArgumentParser(
        description="Interroge l'API REST de Mirth Connect et affiche un rapport.")
    parser.add_argument("-t", "--timeout", type=float, default=8,
                        help="Délai d'attente réseau en secondes (def: 8)")
    parser.add_argument("-s", "--sections", type=str, default="all",
                        help="Sections à afficher, séparées par des virgules : "
                             "server,channels,stats,errors (def: all)")
    parser.add_argument("-c", "--channel", type=str, default=None,
                        help="Filtre : n'affiche que les canaux dont le nom "
                             "contient ce texte (insensible à la casse)")
    parser.add_argument("-u", "--url", type=str, default=None,
                        help="URL de base de l'API REST Mirth "
                             "(déf: env / .mirth_config.py / https://localhost:8443/api)")
    parser.add_argument("--user", type=str, default=None,
                        help="Identifiant de connexion à l'API Mirth")
    parser.add_argument("-p", "--password", type=str, default=None,
                        help="Mot de passe de connexion à l'API Mirth")
    args = parser.parse_args()

    # Les arguments fournis priment : on les injecte dans l'environnement, source
    # de configuration la plus prioritaire pour get_config() (cf. MirthClient).
    if args.url:
        os.environ["MIRTH_BASE_URL"] = args.url
    if args.user:
        os.environ["MIRTH_USER"] = args.user
    if args.password:
        os.environ["MIRTH_PASSWORD"] = args.password

    wanted = [s.strip().lower() for s in args.sections.split(",") if s.strip()]
    if "all" in wanted or not wanted:
        wanted = ["server", "channels", "stats", "errors"]

    # Une seule session : on récupère tout via get_overview.
    ov = get_overview(timeout=args.timeout)

    display_header("CONNEXION MIRTH")
    print_table([
        ["URL de l'API", ov.get("base_url")],
        ["Joignable", "Oui" if ov.get("reachable") else "Non"],
        ["Version", fmt(ov.get("version"))],
    ], headers=["Indicateur", "Valeur"])

    if not ov.get("reachable"):
        safe_print(f"\nServeur Mirth injoignable : {ov.get('error')}")
        sys.exit(1)

    channels = ov.get("channels", [])
    if args.channel:
        flt = args.channel.lower()
        channels = [c for c in channels if flt in (c.get("name") or "").lower()]

    if "server" in wanted:
        display_header("INFOS SERVEUR")
        info = ov.get("system_info") or {}
        stats = ov.get("system_stats") or {}
        rows = [["Version Mirth", fmt(ov.get("version"))]]
        for label, key in (("OS", "jvmVersion"), ("OS (nom)", "osName"),
                            ("OS (version)", "osVersion"), ("Architecture", "osArchitecture")):
            if key in info:
                rows.append([label, info.get(key)])
        for label, key in (("CPU JVM (%)", "cpuUsagePct"),
                            ("Mémoire allouée", "allocatedMemoryBytes"),
                            ("Mémoire libre", "freeMemoryBytes"),
                            ("Mémoire max", "maxMemoryBytes")):
            if key in stats:
                rows.append([label, stats.get(key)])
        print_table(rows, headers=["Indicateur", "Valeur"])

    if "stats" in wanted:
        display_header("STATISTIQUES GLOBALES (TOUS CANAUX)")
        totals = compute_totals(channels)
        print_table([
            ["Canaux", f"{ov.get('channels_started', 0)} démarré(s) / {len(channels)}"],
            ["Reçus", totals["received"]],
            ["Filtrés", totals["filtered"]],
            ["En file", totals["queued"]],
            ["Envoyés", totals["sent"]],
            ["Erreurs", totals["error"]],
        ], headers=["Indicateur", "Valeur"])

    if "channels" in wanted:
        display_header("CANAUX")
        if channels:
            rows = sorted(channels, key=lambda c: (c.get("name") or "").lower())
            table = [[fmt(c.get("name")), fmt(c.get("state")), fmt(c.get("received")),
                      fmt(c.get("filtered")), fmt(c.get("queued")), fmt(c.get("sent")),
                      fmt(c.get("error"))] for c in rows]
            print_table(table, headers=["Canal", "État", "Reçus", "Filtrés",
                                        "En file", "Envoyés", "Erreurs"])
        else:
            safe_print("Aucun canal déployé.")

    if "errors" in wanted:
        display_header("CANAUX EN ERREUR")
        faulty = [c for c in channels
                  if (isinstance(c.get("error"), int) and c["error"] > 0)
                  or (c.get("state") or "").upper() in ("ERROR", "PAUSED")]
        if faulty:
            faulty.sort(key=lambda c: (c.get("error") or 0), reverse=True)
            table = [[fmt(c.get("name")), fmt(c.get("state")), fmt(c.get("error"))]
                     for c in faulty]
            print_table(table, headers=["Canal", "État", "Erreurs"])
        else:
            safe_print("Aucun canal en erreur.")
