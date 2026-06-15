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
    get_overview()             -> vue d'ensemble complète (serveur + canaux [+ connecteurs] + totaux)
    get_channels_overview()    -> liste des canaux et de leurs statistiques
    get_connectors_overview()  -> liste à plat des connecteurs (source + destinations)
    get_connector_endpoints()  -> périphériques (hôte/port) des connecteurs réseau
    get_global_statistics()    -> statistiques agrégées (tous canaux confondus)
    get_server_info()          -> version, infos JVM/OS, statistiques système
    get_errors()               -> canaux en erreur (statistique ERROR > 0 ou état d'erreur)
    get_error_messages()       -> messages en erreur (avec contenu), filtrables par connecteur
    list_error_message_keys()  -> liste légère (sans contenu) des messages en erreur (cache)
    get_message()              -> un message complet (avec contenu)
    build_full_report()        -> rapport détaillé complet (serveur + canaux + connecteurs + erreurs)
"""

import os
import sys
import ssl
import json
import socket
import datetime
import threading
import importlib.util
import urllib.parse
import urllib.request
import urllib.error
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
        # Dernière version serveur lue (par ping/get_version) ; réutilisée pour
        # éviter une requête /server/version redondante (cf. get_overview).
        self.server_version = None

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
    def _request(self, path, data=None, accept="application/json", _allow_relogin=True):
        """Requête HTTP. `data` (dict) => POST form-encoded, sinon GET.

        Retourne (status, body_text). Lève en cas d'erreur réseau/HTTP.

        Auto-relogin : si la requête échoue sur une session devenue invalide
        (401/403 par timeout d'inactivité, redémarrage de Mirth, ou coupure/reset
        TCP), on se ré-authentifie une fois puis on rejoue la requête. Le login
        passe lui-même `_allow_relogin=False` pour éviter toute récursion. Cet
        appel se faisant déjà sous le verrou de session partagée, deux re-logins
        concurrents sont impossibles.
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

        try:
            with self._opener.open(req, timeout=self.timeout) as resp:
                charset = resp.headers.get_content_charset() or "utf-8"
                return resp.status, resp.read().decode(charset, errors="replace")
        except Exception as e:
            if _allow_relogin and _is_recoverable_error(e):
                self.login()
                return self._request(path, data=data, accept=accept,
                                     _allow_relogin=False)
            raise

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
        """(Ré)ouvre une session (cookie JSESSIONID). Lève si l'authentification échoue.

        Un éventuel JSESSIONID périmé du CookieJar est remplacé par celui de la
        réponse (même nom/domaine/chemin), ce qui permet de réutiliser le même
        client à travers les re-logins.
        """
        self._request("/users/_login",
                      data={"username": self.user, "password": self.password},
                      _allow_relogin=False)

    def logout(self):
        try:
            self._request("/users/_logout", data={}, _allow_relogin=False)
        except Exception:
            pass

    def ping(self):
        """Sonde légère validant la session (GET /server/version). LÈVE si le
        serveur est injoignable. Mémorise au passage la version dans
        `self.server_version` : cette sonde précédant chaque appel sur une session
        réutilisée (cf. `_ensure_session`), la version est ainsi déjà disponible
        sans nouvelle requête /server/version (cf. get_overview).

        Sert à confirmer la joignabilité d'une session réutilisée : l'auto-relogin
        de `_request` ré-authentifie de lui-même une session expirée (timeout
        d'inactivité, redémarrage de Mirth, reset TCP) ; `ping` ne lève donc que
        lorsque le serveur est réellement hors d'atteinte.
        """
        _status, text = self._request("/server/version", accept="text/plain")
        self.server_version = text.strip() or None
        return self.server_version

    def get_version(self):
        """Version du serveur Mirth (chaîne), ou None. Mémorise le résultat dans
        `self.server_version`."""
        try:
            _status, text = self._request("/server/version", accept="text/plain")
            self.server_version = text.strip() or None
            return self.server_version
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

    def get_message_raw(self, channel_id, message_id, include_content=True):
        """Réponse brute de /channels/{id}/messages/{messageId} (un seul message),
        ou None. Sert au téléchargement incrémental d'un message manquant du cache.
        """
        query = urllib.parse.urlencode(
            [("includeContent", "true" if include_content else "false")])
        path = "/channels/%s/messages/%s?%s" % (
            urllib.parse.quote(str(channel_id), safe=""),
            urllib.parse.quote(str(message_id), safe=""), query)
        try:
            return self._get_json(path)
        except Exception:
            return None

    def get_channels_config_raw(self):
        """Réponse brute de /channels (définitions COMPLÈTES des canaux), ou None.

        Contrairement à /channels/statuses (statistiques temps réel), cet endpoint
        renvoie la configuration : chaque canal porte son `sourceConnector` et ses
        `destinationConnectors`, chaque connecteur son `transportName` (le « mode »)
        et un bloc `properties` contenant l'IP/port pour les transports réseau.
        Config peu changeante : une seule requête suffit.
        """
        try:
            return self._get_json("/channels")
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


def _parse_connectors(status):
    """Liste des connecteurs (source + destinations) d'un statut de canal.

    Lit `childStatuses.dashboardStatus` : chaque connecteur porte son propre bloc
    `statistics`. Renvoie une liste de dicts {meta_data_id, name, state, received,
    filtered, queued, sent, error}, triée par metaDataId (source = 0, destinations
    >= 1). Liste vide si le statut n'expose pas ses connecteurs.
    """
    child = status.get("childStatuses")
    if isinstance(child, dict):
        children = _as_list(child.get("dashboardStatus"))
    else:
        children = _as_list(child)

    connectors = []
    for c in children:
        if not isinstance(c, dict):
            continue
        cols = _stats_to_channel(_parse_statistics(c.get("statistics")))
        if cols["queued"] is None and c.get("queued") is not None:
            cols["queued"] = _coerce_int(c.get("queued"))
        connectors.append({
            "meta_data_id": _coerce_int(c.get("metaDataId")),
            "name": c.get("name"),
            "state": c.get("state") or status.get("state"),
            **cols,
        })
    connectors.sort(key=lambda x: x["meta_data_id"]
                    if isinstance(x["meta_data_id"], int) else 9999)
    return connectors


def _aggregate_connectors(connectors):
    """Somme (en colonnes) les statistiques d'une liste de connecteurs.

    Filet de secours quand le statut de canal ne porte pas de bloc `statistics`
    exploitable : on additionne celles de ses connecteurs (source + destinations).
    Une colonne reste None si aucun connecteur ne la renseigne.
    """
    cols = {"received": None, "filtered": None, "queued": None,
            "sent": None, "error": None}
    for c in connectors:
        for k in cols:
            v = c.get(k)
            if isinstance(v, int):
                cols[k] = (cols[k] or 0) + v
    return cols


def _parse_dashboard_statuses(data):
    """Normalise la réponse de /channels/statuses en liste de canaux.

    Chaque canal porte aussi le détail de ses `connectors` (source + destinations),
    désormais conservé au lieu d'être seulement agrégé.
    """
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
        connectors = _parse_connectors(s)
        stats = _parse_statistics(s.get("statistics"))
        if stats:
            cols = _stats_to_channel(stats)
        else:
            # Pas de statistiques au niveau canal : agrégation des connecteurs.
            cols = _aggregate_connectors(connectors)
        # `queued` peut être un champ propre au statut (hors bloc statistics).
        if cols["queued"] is None and s.get("queued") is not None:
            cols["queued"] = _coerce_int(s.get("queued"))
        channels.append({
            "name": s.get("name"),
            "channel_id": s.get("channelId"),
            "state": s.get("state"),
            **cols,
            "connectors": connectors,
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
# PARSING DÉFENSIF DE LA CONFIGURATION DES CONNECTEURS (GET /channels)
# --------------------------------------------------------------------------
# Adresses d'écoute « toutes interfaces » : un listener lié à 0.0.0.0/:: écoute
# sur l'hôte du serveur Mirth lui-même ; on les conserve (host renseigné) mais on
# les marque non-pingables (cibler le serveur relève des phases ultérieures).
_LOCAL_BIND_HOSTS = {"0.0.0.0", "::", "0:0:0:0:0:0:0:0", "*"}


def _coerce_port(value):
    """Renvoie un port entier valide (1..65535), sinon None."""
    p = _coerce_int(value)
    return p if isinstance(p, int) and 0 < p < 65536 else None


def _split_url(value):
    """Si `value` ressemble à une URL (scheme://host[:port]/...), renvoie (host, port)."""
    if not isinstance(value, str) or "://" not in value:
        return None, None
    try:
        parsed = urllib.parse.urlparse(value.strip())
        return parsed.hostname, parsed.port
    except Exception:
        return None, None


def _looks_like_host(value):
    """True si `value` ressemble à un hôte réseau (IP/nom) et non à un chemin local."""
    if not isinstance(value, str):
        return False
    v = value.strip()
    if not v or "://" in v:
        return False
    return not any(c in v for c in "\\/ ")


def _extract_host_port(props):
    """Extrait (host, port) du bloc `properties` d'un connecteur réseau.

    Renvoie (None, None) pour un connecteur non réseau (fichier local, base de
    données, JavaScript, canal/VM...). Défensif et tolérant inter-versions :

      1. Sender TCP/MLLP/LLP (`*Dispatcher*`) : `remoteAddress` + `remotePort`.
      2. Sender HTTP / Web Service : champ portant une URL complète
         (`wsdlUrl`/`locationURI`/`host`/`url`/...) → `urllib.parse`.
      3. Listener source (Receiver) : `listenerConnectorProperties.host` + `port`.
      4. Repli générique : hôte simple **accompagné d'un port numérique** (DICOM
         sender, autres versions). Le port obligatoire évite de prendre un chemin
         de fichier local ou une URL JDBC pour un hôte réseau.
    """
    if not isinstance(props, dict):
        return None, None

    # 1. Sender TCP / MLLP : adresse + port distants.
    host = props.get("remoteAddress")
    if _looks_like_host(host):
        return host.strip(), _coerce_port(props.get("remotePort"))

    # 2. Sender HTTP / Web Service : champ portant une URL complète.
    for key in ("wsdlUrl", "locationURI", "host", "url", "uri", "address"):
        u_host, u_port = _split_url(props.get(key))
        if u_host:
            return u_host, u_port

    # 3. Listener (source) : adresse/port d'écoute.
    lcp = props.get("listenerConnectorProperties")
    if isinstance(lcp, dict) and isinstance(lcp.get("host"), str) and lcp["host"]:
        return lcp["host"].strip(), _coerce_port(lcp.get("port"))

    # 4. Repli générique : hôte simple + port numérique obligatoire.
    for hk in ("host", "address", "serverAddress", "remoteAddress"):
        h = props.get(hk)
        if _looks_like_host(h):
            for pk in ("port", "remotePort", "serverPort"):
                p = _coerce_port(props.get(pk))
                if p is not None:
                    return h.strip(), p
    return None, None


def _connector_endpoint(channel_id, channel_name, connector, role):
    """Transforme un connecteur de config en endpoint normalisé, ou None."""
    if not isinstance(connector, dict):
        return None
    props = connector.get("properties")
    host, port = _extract_host_port(props if isinstance(props, dict) else {})
    pingable = bool(host) and str(host) not in _LOCAL_BIND_HOSTS
    if host:
        address = "%s:%s" % (host, port) if port is not None else str(host)
    else:
        address = None
    return {
        "channel_id": channel_id,
        "channel_name": channel_name,
        "meta_data_id": _coerce_int(connector.get("metaDataId")),
        "name": connector.get("name"),
        "role": role,                                   # "source" | "destination"
        "transport": connector.get("transportName"),
        "kind": "réseau" if host else "non-réseau",
        "host": host,
        "port": port,
        "address": address,
        "pingable": pingable,
        "enabled": _as_bool(connector.get("enabled"), default=True),
    }


def _parse_channel_endpoints(channel):
    """Endpoints (source + destinations) d'une définition de canal (GET /channels)."""
    if not isinstance(channel, dict):
        return []
    channel_id = channel.get("id") or channel.get("channelId")
    channel_name = channel.get("name")

    endpoints = []
    ep = _connector_endpoint(channel_id, channel_name,
                             channel.get("sourceConnector"), "source")
    if ep:
        endpoints.append(ep)

    dest = channel.get("destinationConnectors")
    if isinstance(dest, dict):
        destinations = _as_list(dest.get("connector"))
    else:
        destinations = _as_list(dest)
    for d in destinations:
        ep = _connector_endpoint(channel_id, channel_name, d, "destination")
        if ep:
            endpoints.append(ep)
    return endpoints


def _parse_channels_config(data):
    """Liste à plat des endpoints de tous les canaux à partir de /channels brut."""
    if isinstance(data, dict):
        container = data.get("list", data)
        if isinstance(container, dict):
            channels = _as_list(container.get("channel"))
        else:
            channels = _as_list(container)
    else:
        channels = _as_list(data)

    endpoints = []
    for ch in channels:
        endpoints.extend(_parse_channel_endpoints(ch))
    return endpoints


# --------------------------------------------------------------------------
# POINTS D'ENTRÉE HAUT NIVEAU (ne lèvent jamais)
# --------------------------------------------------------------------------
def _new_result(extra=None):
    cfg = get_config()
    base = {"reachable": False, "error": None, "base_url": cfg["MIRTH_BASE_URL"]}
    if extra:
        base.update(extra)
    return base


# --------------------------------------------------------------------------
# SESSION MIRTH DURABLE ET PARTAGÉE (collecteur de fond + routes web)
# --------------------------------------------------------------------------
# Un UNIQUE MirthClient (donc un seul JSESSIONID) est conservé entre les appels
# et réutilisé par la tâche collecteur comme par les routes API. On évite ainsi
# de repayer le coût d'authentification (lent sur le vrai serveur, ~7 s) à chaque
# accès. La session est « paresseuse » : on ne (re)logue que si nécessaire, et
# JAMAIS de logout() entre deux usages (cela invaliderait la session).
#
# Le CookieJar de `MirthClient` n'étant PAS thread-safe et les threads collecteur
# (daemon) et serveur web (ThreadingHTTPServer) pouvant y accéder en parallèle,
# tout accès — y compris le re-login — est sérialisé par `_SESSION_LOCK`.
_SESSION_LOCK = threading.RLock()
_shared_client = None


def _is_recoverable_error(exc):
    """True si l'erreur justifie une ré-authentification + 1 nouvelle tentative.

    Couvre les trois façons dont une session devient invalide :
      * timeout d'inactivité côté serveur -> 401/403 ;
      * redémarrage de Mirth / coupure réseau / reset TCP -> erreur de connexion.
    Un 500 (« serveur up mais en erreur ») n'est PAS rejouable.
    """
    if isinstance(exc, urllib.error.HTTPError):   # sous-classe de URLError : testé avant
        return exc.code in (401, 403)
    if isinstance(exc, urllib.error.URLError):
        return True
    if isinstance(exc, (socket.timeout, TimeoutError, ConnectionError)):
        return True
    return False


def _ensure_session(timeout=8):
    """Retourne le MirthClient partagé, connecté et VALIDÉ. LÈVE si injoignable.

      * Aucune session ouverte  -> création + login.
      * Session existante       -> validée par une sonde légère (`ping`) ;
        l'auto-relogin de `_request` la ré-authentifie d'elle-même si elle a
        expiré, si Mirth a redémarré, ou si la connexion TCP a été coupée.

    À appeler sous `_SESSION_LOCK` (repris ici par sécurité — RLock réentrant).
    """
    global _shared_client
    with _SESSION_LOCK:
        client = _shared_client
        if client is not None:
            client.timeout = timeout
            client.ping()                  # lève si le serveur est injoignable
            return client
        client = MirthClient(timeout=timeout)
        client.login()                     # lève si injoignable / identifiants invalides
        _shared_client = client
        return client


def close_session():
    """Ferme proprement la session partagée (logout + oubli du client).

    Optionnel — Mirth fait expirer ses sessions inactives de lui-même. Utile en
    fin de programme (CLI one-shot, arrêt du service) pour ne pas laisser une
    session pendante côté serveur.
    """
    global _shared_client
    with _SESSION_LOCK:
        if _shared_client is not None:
            _shared_client.logout()
            _shared_client = None


def _with_client(func, timeout=8):
    """Exécute `func(client)` sous la session Mirth durable et partagée, sans lever.

    L'accès à la session est sérialisé par `_SESSION_LOCK` (le CookieJar n'est pas
    thread-safe). La session est réutilisée d'un appel à l'autre et re-loguée
    automatiquement si elle a expiré (cf. `_ensure_session` / l'auto-relogin de
    `_request`). Aucun logout n'est fait ici.

    Retourne (data, error_message). `data` est None si la connexion a échoué.
    """
    with _SESSION_LOCK:
        try:
            client = _ensure_session(timeout=timeout)
        except Exception as e:
            return None, f"Connexion/authentification impossible : {e}"
        try:
            return func(client), None
        except Exception as e:
            return None, f"Erreur lors de la lecture des données : {e}"


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
        # `_ensure_session` vient de sonder /server/version (ping) sur une session
        # réutilisée et en a mémorisé la version : on la réutilise au lieu de
        # refaire la requête. Repli sur get_version() après un login frais (session
        # neuve : ping non joué, donc server_version encore None).
        version = client.server_version or client.get_version()
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


def get_status(timeout=8):
    """Statut minimal du serveur Mirth : joignabilité (login) + version.

    Appel volontairement léger destiné à l'affichage temps-réel de l'état et de
    la version lorsque le reste des données est servi depuis l'historique (cf.
    checker_service.api_mirth_api). Le login suffit à prouver la joignabilité ;
    on ne récupère ensuite que la version. Ne lève jamais : en cas d'échec,
    renvoie {"reachable": False, "error": ...}.
    """
    result = _new_result({"version": None})
    data, error = _with_client(lambda c: c.get_version(), timeout=timeout)
    if data is None:
        result["error"] = error
        return result
    result["version"] = data
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


def _matches_connector(item, connector_meta_id):
    """True si l'item correspond au connecteur ciblé (metaDataId), ou si pas de filtre."""
    if connector_meta_id is None:
        return True
    return str(item.get("meta_data_id")) == str(connector_meta_id)


def get_error_messages(channel_id=None, connector_meta_id=None, limit=50, timeout=8):
    """Messages en erreur d'un canal (ou de tous les canaux en erreur).

    Si `channel_id` est fourni, renvoie les messages en erreur de ce seul canal ;
    sinon, parcourt tous les canaux ayant au moins une erreur et agrège leurs
    messages. `connector_meta_id` (optionnel) restreint à un connecteur précis
    (0 = source, >= 1 = destinations). Chaque message renvoyé porte son
    `channel_id`/`channel_name`, ce qui permet d'afficher une liste unifiée.

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
            items = [it for it in items if _matches_connector(it, connector_meta_id)]
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


def get_connectors_overview(channel_id=None, timeout=8):
    """Liste à plat des connecteurs (source + destinations) de tous les canaux.

    Filtrable sur un canal via `channel_id`. Chaque connecteur porte son canal
    d'origine (`channel_id`, `channel_name`, `channel_state`) afin d'être affiché
    dans une liste unifiée. Ne lève jamais.
    """
    result = _new_result({"connectors": [], "connector_count": 0,
                          "channel_count": 0})
    data, error = _with_client(lambda c: c.get_channels(), timeout=timeout)
    if data is None:
        result["error"] = error
        return result

    channels = data
    if channel_id:
        channels = [c for c in channels if c.get("channel_id") == channel_id]

    flat = []
    for c in channels:
        for conn in c.get("connectors", []):
            flat.append({"channel_id": c.get("channel_id"),
                         "channel_name": c.get("name"),
                         "channel_state": c.get("state"),
                         **conn})
    result["connectors"] = flat
    result["connector_count"] = len(flat)
    result["channel_count"] = len(channels)
    result["reachable"] = True
    return result


def get_connector_endpoints(timeout=8):
    """Liste les périphériques (hôtes/ports) configurés sur les connecteurs.

    Lit la configuration des canaux (GET /channels) et en extrait, pour chaque
    connecteur source/destination, l'hôte et le port distants quand le transport
    est réseau (TCP/MLLP, HTTP/WS, DICOM...). Les connecteurs non réseau
    (fichier, base, JavaScript, canal) sont renvoyés avec `kind="non-réseau"` et
    `pingable=False`. Point d'entrée des phases de supervision ; ne lève jamais.

    Champs : reachable, error, base_url, count, endpoints[{channel_id,
    channel_name, meta_data_id, name, role, transport, kind, host, port, address,
    pingable, enabled}].
    """
    result = _new_result({"endpoints": [], "count": 0})
    data, error = _with_client(
        lambda c: _parse_channels_config(c.get_channels_config_raw()),
        timeout=timeout)
    if data is None:
        result["error"] = error
        return result
    result["endpoints"] = data
    result["count"] = len(data)
    result["reachable"] = True
    return result


def list_error_message_keys(channel_id=None, limit=50, timeout=8):
    """Liste LÉGÈRE (sans contenu) des messages en erreur — liste autoritaire.

    Même structure que `get_error_messages` mais interroge Mirth avec
    `includeContent=false` et retire les champs lourds `error`/`content`. C'est la
    référence consommée par le cache (`checker_service`) : Mirth fait foi sur les
    messages réellement en erreur à l'instant T, le cache ne fournit que le corps.

    Ne lève jamais. Champs : messages (clés), count, channels (récap par canal).
    """
    result = _new_result({"channel_id": channel_id, "messages": [], "count": 0,
                          "channels": []})

    def _fetch(client):
        channels = client.get_channels()
        names = {c.get("channel_id"): c.get("name") for c in channels}
        if channel_id:
            targets = [(channel_id, names.get(channel_id))]
        else:
            targets = [(c.get("channel_id"), c.get("name")) for c in channels
                       if isinstance(c.get("error"), int) and c["error"] > 0]

        all_items, per_channel = [], []
        for cid, cname in targets:
            raw = client.get_messages_raw(cid, status="ERROR", limit=limit,
                                          include_content=False)
            items = _parse_messages(raw, channel_id=cid, channel_name=cname)
            for it in items:
                it.pop("content", None)
                it.pop("error", None)
            per_channel.append({"channel_id": cid, "name": cname, "count": len(items)})
            all_items.extend(items)
        return all_items, per_channel

    data, error = _with_client(_fetch, timeout=timeout)
    if data is None:
        result["error"] = error
        return result

    all_items, per_channel = data
    all_items.sort(key=lambda x: x.get("received_date") or "", reverse=True)
    result["messages"] = all_items
    result["count"] = len(all_items)
    result["channels"] = per_channel
    result["reachable"] = True
    return result


def get_message(channel_id, message_id, timeout=8):
    """Un message complet (avec contenu), parsé en connecteurs en erreur.

    S'appuie sur l'endpoint message unique de Mirth. Ne lève jamais.
    """
    result = _new_result({"channel_id": channel_id, "message_id": message_id,
                          "messages": [], "count": 0})

    def _fetch(client):
        raw = client.get_message_raw(channel_id, message_id, include_content=True)
        return _parse_messages(raw, channel_id=channel_id)

    data, error = _with_client(_fetch, timeout=timeout)
    if data is None:
        result["error"] = error
        return result
    result["messages"] = data
    result["count"] = len(data)
    result["reachable"] = True
    return result


def build_full_report(include_messages=False, limit=50, timeout=8):
    """Rapport détaillé complet de l'état Mirth (JSON-friendly). Ne lève jamais.

    Rassemble en une structure unique : serveur (version + JVM/OS + stats système),
    totaux globaux, canaux **avec leurs connecteurs**, **périphériques** (endpoints
    réseau des connecteurs), canaux en erreur, et — sur demande (`include_messages`)
    — les messages en erreur. Source commune du rapport CLI exhaustif (`--full`) et
    de la route /api/mirth/report.
    """
    ov = get_overview(timeout=timeout)
    report = {
        "reachable": ov.get("reachable"),
        "error": ov.get("error"),
        "base_url": ov.get("base_url"),
        "generated_at": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "version": ov.get("version"),
        "system_info": ov.get("system_info"),
        "system_stats": ov.get("system_stats"),
        "channel_count": ov.get("channel_count"),
        "channels_started": ov.get("channels_started"),
        "totals": ov.get("totals"),
        "channels": ov.get("channels", []),
        "endpoints": [],
        "errors": [],
        "messages": [],
    }
    if not ov.get("reachable"):
        return report

    channels = ov.get("channels", [])
    faulty = [c for c in channels
              if (isinstance(c.get("error"), int) and c["error"] > 0)
              or (c.get("state") or "").upper() in ("ERROR", "PAUSED")]
    faulty.sort(key=lambda c: (c.get("error") or 0), reverse=True)
    report["errors"] = faulty

    # Périphériques : endpoints réseau des connecteurs (une requête /channels).
    report["endpoints"] = get_connector_endpoints(timeout=timeout).get("endpoints", [])

    if include_messages:
        msgs = get_error_messages(limit=limit, timeout=timeout)
        report["messages"] = msgs.get("messages", [])
    return report


# ==============================================================================
# PARTIE 2 : SECTION MAIN (AFFICHAGE CLI)
# ==============================================================================

if __name__ == "__main__":
    import atexit
    import argparse
    from tabulate import tabulate

    # CLI one-shot : on ferme la session durable à la sortie (quel que soit le
    # chemin d'exit) pour ne pas laisser de session pendante sur le serveur.
    atexit.register(close_session)

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
                             "server,channels,connectors,endpoints,stats,errors (def: all)")
    parser.add_argument("-c", "--channel", type=str, default=None,
                        help="Filtre : n'affiche que les canaux dont le nom "
                             "contient ce texte (insensible à la casse)")
    parser.add_argument("-f", "--full", action="store_true",
                        help="Rapport exhaustif : toutes les sections + le détail "
                             "des messages en erreur (avec contenu)")
    parser.add_argument("-j", "--json", action="store_true",
                        help="Sortie JSON brute du rapport complet (build_full_report) "
                             "— pratique pour l'exécutable one-shot")
    parser.add_argument("-l", "--limit", type=int, default=50,
                        help="Nombre maximum de messages en erreur par canal (def: 50)")
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

    # --json : dump JSON du rapport complet puis sortie (usage exe one-shot).
    if args.json:
        report = build_full_report(include_messages=True, limit=args.limit,
                                   timeout=args.timeout)
        print(json.dumps(report, ensure_ascii=False, indent=2, default=str))
        sys.exit(0 if report.get("reachable") else 1)

    wanted = [s.strip().lower() for s in args.sections.split(",") if s.strip()]
    if "all" in wanted or not wanted:
        wanted = ["server", "channels", "connectors", "endpoints", "stats", "errors"]
    # --full ajoute le détail (lourd) des messages en erreur aux sections de base.
    if args.full:
        wanted = ["server", "channels", "connectors", "endpoints", "stats",
                  "errors", "messages"]

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

    if "connectors" in wanted:
        display_header("CONNECTEURS (SOURCE + DESTINATIONS)")
        rows = sorted(channels, key=lambda c: (c.get("name") or "").lower())
        table = []
        for c in rows:
            for conn in c.get("connectors", []):
                role = "Source" if conn.get("meta_data_id") == 0 else "Destination"
                table.append([
                    fmt(c.get("name")), fmt(conn.get("meta_data_id")), role,
                    fmt(conn.get("name")), fmt(conn.get("state")),
                    fmt(conn.get("received")), fmt(conn.get("filtered")),
                    fmt(conn.get("queued")), fmt(conn.get("sent")),
                    fmt(conn.get("error"))])
        if table:
            print_table(table, headers=["Canal", "#", "Rôle", "Connecteur", "État",
                                        "Reçus", "Filtrés", "En file", "Envoyés",
                                        "Erreurs"])
        else:
            safe_print("Aucun connecteur exposé par le serveur.")

    if "endpoints" in wanted:
        display_header("PÉRIPHÉRIQUES (ENDPOINTS DES CONNECTEURS)")
        eps = get_connector_endpoints(timeout=args.timeout).get("endpoints", [])
        if args.channel:
            flt = args.channel.lower()
            eps = [e for e in eps if flt in (e.get("channel_name") or "").lower()]
        if eps:
            table = [[fmt(e.get("channel_name")), fmt(e.get("name")),
                      "Source" if e.get("role") == "source" else "Destination",
                      fmt(e.get("transport")), fmt(e.get("host")), fmt(e.get("port")),
                      "Oui" if e.get("pingable") else "Non"] for e in eps]
            print_table(table, headers=["Canal", "Connecteur", "Rôle", "Transport",
                                        "Hôte", "Port", "Pingable"])
            net = sum(1 for e in eps if e.get("pingable"))
            safe_print(f"\n{len(eps)} connecteur(s), dont {net} pingable(s).")
        else:
            safe_print("Aucun endpoint détecté (serveur sans canaux ?).")

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

    if "messages" in wanted:
        display_header("MESSAGES EN ERREUR (DÉTAIL)")
        # Filtre éventuel par canal : restreint aux canaux affichés ci-dessus.
        if args.channel:
            faulty_ids = [c.get("channel_id") for c in channels
                          if isinstance(c.get("error"), int) and c["error"] > 0]
            collected = []
            for cid in faulty_ids:
                res = get_error_messages(channel_id=cid, limit=args.limit,
                                         timeout=args.timeout)
                collected.extend(res.get("messages", []))
            msgs = sorted(collected, key=lambda x: x.get("received_date") or "",
                          reverse=True)
        else:
            msgs = get_error_messages(limit=args.limit,
                                      timeout=args.timeout).get("messages", [])
        if not msgs:
            safe_print("Aucun message en erreur.")
        else:
            table = [[fmt(m.get("received_date")), fmt(m.get("channel_name")),
                      fmt(m.get("connector")), fmt(m.get("message_id")),
                      fmt(m.get("send_attempts")), fmt(m.get("error_code")),
                      (m.get("error") or "").splitlines()[0] if m.get("error") else "-"]
                     for m in msgs]
            print_table(table, headers=["Date", "Canal", "Connecteur", "#Msg",
                                        "Retry", "Code", "Erreur (1re ligne)"])
            safe_print(f"\n{len(msgs)} message(s) en erreur.")
