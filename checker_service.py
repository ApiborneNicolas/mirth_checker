#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
checker_service.py
==================
Service web pour la boîte à outils Mirth_checker.

Réutilise les trois scripts existants comme librairies :
- system_state       : sondes système (CPU, mémoire, disque, réseau, ping...)
- mirth_logs_parser  : analyse des fichiers de log Mirth
- quickmail          : envoi d'email

Le service :
1. expose plusieurs API HTTP (JSON) autour de ces librairies ;
2. lance une tâche programmée toutes les 60 secondes qui enregistre un relevé
   CPU / mémoire / stockage dans une base SQLite (historique) ;
3. sert les pages statiques (web/index.html -> web/statistiques.html) affichant
   l'évolution des courbes.

Lancement :
    python checker_service.py [--host 0.0.0.0] [--port 8800] [--interval 60]
                              [--logfile chemin] [--no_output]
                              [--mirth-url URL] [--mirth-user ID] [--mirth-password PW]

`--no_output` coupe tout l'affichage console (ni tableau de bord rich ni journal),
pour un lancement en arrière-plan, fenêtre non accessible — le service et son API
restent actifs. Sans cette option, le tableau de bord rich (si disponible) est
rendu de façon ÉVÉNEMENTIELLE : redessiné uniquement quand son contenu change
(début/fin d'un relevé, nouveau message de log), et non plus en continu.
"""

import os
import re
import sys
import json
import math
import argparse
import datetime
import threading
import urllib.parse
import concurrent.futures

from tabulate import tabulate

# --- Import des librairies internes (dossier lib/) -------------------------
from lib import database, webserver, log, auth, security, tls
from lib.scheduler import RecurringTask, start_staggered

# --- Import des scripts existants en tant que librairies -------------------
import system_state
import mirth_logs_parser
import mirth_api
import quickmail

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
# Pages statiques (web/) : ressource en lecture seule. En build gelé (PyInstaller),
# elles sont embarquées dans le dossier temporaire d'extraction sys._MEIPASS
# (cf. --add-data "web;web" dans _compilation.bat) ; sinon, web/ à côté du script.
WEB_DIR = os.path.join(getattr(sys, "_MEIPASS", BASE_DIR), "web")
# Emplacement standard du log serveur de Mirth Connect sous Windows.
DEFAULT_LOGFILE = r"C:\Program Files\Mirth Connect\logs\mirth.log"

# Lecteur système surveillé pour le stockage (C:\ sous Windows, / sinon)
SYSTEM_DRIVE = os.environ.get("SystemDrive", "C:") + os.sep if os.name == "nt" else "/"

# Rétention max de l'historique en jours : les relevés plus anciens sont purgés
# automatiquement (tâche périodique `retention-purge`). Réglée au démarrage par
# main() depuis --retention-days. 0 = illimité (aucune purge automatique).
RETENTION_DAYS = 15
RETENTION_TASK_NAME = "retention-purge"
RETENTION_PURGE_INTERVAL = 24 * 3600   # cadence quotidienne (interval indicatif)
# Heure (murale) de la purge quotidienne : 03:00, à l'heure creuse de la nuit, pour
# ne pas peser sur la machine en journée (la rétention étant exprimée en jours, une
# seule purge par jour suffit largement).
RETENTION_PURGE_TIME = datetime.time(3, 0)

# URL de base publique du service (ex. http://serveur:8800), utilisée pour bâtir
# les liens profonds dans les e-mails d'alerte (ouverture du dashboard sur le
# connecteur concerné). Renseignée au démarrage par main() : variable
# d'environnement CHECKER_BASE_URL si fournie, sinon nom d'hôte + port. None tant
# que le service n'a pas démarré (les e-mails omettent alors les liens).
SERVICE_BASE_URL = None

# État de sécurité du service, renseigné par main() (utilisé par les handlers
# d'authentification pour poser le drapeau Secure du cookie et pour ouvrir les
# routes d'admin quand l'authentification est globalement désactivée).
SERVICE_HTTPS = False
SERVICE_AUTH_ENABLED = False
SESSION_CLEANUP_TIME = datetime.time(3, 15)   # purge quotidienne des sessions expirées


# ==========================================================================
# NORMALISATION DES NOMBRES DÉCIMAUX
# Toutes les valeurs à virgule de l'API sont arrondies au supérieur à 2 décimales.
# ==========================================================================
def _ceil2(x):
    """Arrondit `x` au supérieur (plafond) à 2 décimales."""
    return math.ceil(x * 100) / 100


def _fmt2(x):
    """Représentation texte d'un nombre à 2 décimales, arrondi au supérieur."""
    if x is None:
        return ""
    return f"{_ceil2(x):.2f}"


def _round_floats(obj):
    """Parcourt récursivement une charge utile JSON et arrondit tout flottant au
    supérieur à 2 décimales (les entiers, booléens et chaînes sont conservés)."""
    if isinstance(obj, bool):
        return obj
    if isinstance(obj, float):
        return _ceil2(obj)
    if isinstance(obj, dict):
        return {k: _round_floats(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_round_floats(v) for v in obj]
    return obj


# ==========================================================================
# TÂCHES PROGRAMMÉES : relevés périodiques (tagués par source)
#   - tag 'system' : CPU / mémoire / stockage de la machine
#   - tag 'mirth'  : CPU / mémoire / sockets du processus Mirth (mcservice.exe)
# Chaque relevé est étiqueté (`tag`) pour cohabiter dans la même table et pouvoir
# évoluer (ajout de nouvelles sources sans changement de schéma).
# ==========================================================================
TAG_SYSTEM = "system"
TAG_MIRTH = "mirth"

# Table dédiée aux relevés du processus Mirth (cf. lib/database). Les métriques
# Mirth ne cohabitent plus avec le système dans la table `metrics`.
TABLE_MIRTH_METRICS = "mirth_metrics"

# ==========================================================================
# ÉVÈNEMENTS / ALERTES (second jeu de données temporel)
# Stockés dans la table `events` (cf. lib/database) et superposés à TOUS les
# graphiques sous forme de barres verticales colorées + texte. Couleur par défaut
# dérivée de la catégorie ; un appelant (API /api/setevent) peut imposer la sienne.
# ==========================================================================
EVENT_COLORS = {
    "boot": "#ef4444",      # rouge — démarrage du système
    "service": "#22c55e",   # vert — start/stop du checker
    "systeme": "#ef4444",   # rouge — évènements système (boot + checker, regroupés)
    "mirth": "#a78bfa",     # violet — start/stop du processus Mirth
    "alarm": "#f59e0b",     # orange — alarme (mail ou autre)
    "mail": "#f472b6",      # rose — envoi d'email
    "cmd": "#eab308",       # jaune — commande exécutée
    "network": "#2dd4bf",   # turquoise — connexion/déconnexion
    "info": "#94a3b8",      # gris — divers
}


def _event_color(category, color=None):
    """Couleur d'un évènement : celle fournie, sinon dérivée de la catégorie."""
    if color:
        return color
    return EVENT_COLORS.get((category or "").strip().lower(), EVENT_COLORS["info"])


# ==========================================================================
# ALARMES & ALERTES (notifications sortantes)
# ==========================================================================
# Catalogue des ALARMES connues : les évènements que les tâches de relevé savent
# détecter et pour lesquels une alerte peut être émise. Chaque alarme porte :
#   - code         : identifiant stable (clé de configuration en base) ;
#   - title        : libellé lisible (page de configuration + e-mail) ;
#   - event_label  : libellé inscrit dans la table `events` (inchangé, pour que
#                    les barres verticales des graphes restent identiques) ;
#   - category     : catégorie d'évènement (couleur/regroupement) ;
#   - severity     : gravité indicative (critical / warning / info) ;
#   - default_email: état OUI/NON par défaut de la notification e-mail (proposé
#                    par la page tant que rien n'a été enregistré).
ALARM_CATALOG = [
    {"code": "system_boot",  "title": "Démarrage système",
     "event_label": "Démarrage système",     "category": "systeme",
     "severity": "critical", "default_email": True,  "default_mqtt": True},
    {"code": "checker_down", "title": "Arrêt du checker",
     "event_label": "Arrêt du checker",      "category": "systeme",
     "severity": "info",     "default_email": False, "default_mqtt": False},
    {"code": "checker_up",   "title": "Démarrage du checker",
     "event_label": "Démarrage du checker",  "category": "systeme",
     "severity": "warning",  "default_email": True,  "default_mqtt": True},
    {"code": "mirth_down",   "title": "Arrêt du processus Mirth",
     "event_label": "Arrêt Mirth",          "category": "mirth",
     "severity": "critical", "default_email": True,  "default_mqtt": True},
    {"code": "mirth_up",     "title": "Démarrage du processus Mirth",
     "event_label": "Démarrage Mirth",       "category": "mirth",
     "severity": "warning",  "default_email": True,  "default_mqtt": True},
    {"code": "mirth_message_error", "title": "Message en erreur",
     "event_label": "Message en erreur",     "category": "mirth",
     "severity": "warning",  "default_email": True,  "default_mqtt": True},
    {"code": "device_unreachable", "title": "Périphérique injoignable",
     "event_label": "Périphérique injoignable", "category": "network",
     "severity": "critical", "default_email": True,  "default_mqtt": True},
    {"code": "device_up",    "title": "Périphérique de retour en ligne",
     "event_label": "Périphérique en ligne", "category": "network",
     "severity": "info",     "default_email": False, "default_mqtt": False},
]
ALARM_BY_CODE = {a["code"]: a for a in ALARM_CATALOG}

# Méthodes de notification. Seul l'e-mail est opérationnel ; MQTT est un
# emplacement réservé (la page l'affiche désactivé, « à venir »). `active` pilote
# l'affichage côté page ; l'envoi réel n'est implémenté que pour 'email'.
ALERT_METHODS = [
    {"method": "email", "label": "E-mail", "active": True,
     "placeholder": "adresse1@exemple.fr, adresse2@exemple.fr"},
    {"method": "mqtt",  "label": "MQTT",  "active": False,
     "placeholder": "broker:port / topic (à venir)"},
]


def _split_recipients(raw):
    """Découpe une liste de destinataires (virgule / point-virgule / saut de ligne)."""
    if not raw:
        return []
    parts = re.split(r"[,;\n\r]+", str(raw))
    return [p.strip() for p in parts if p.strip()]


def _build_alarm_context(code, timestamp):
    """Rassemble un contexte détaillé pour le corps de l'alerte (lectures locales).

    N'effectue aucun appel réseau : l'état système et Mirth provient des derniers
    relevés historisés en base, afin que la construction du message reste rapide et
    fiable même au moment d'un incident.
    """
    entry = ALARM_BY_CODE.get(code, {})
    ctx = {
        "code": code,
        "title": entry.get("title", code),
        "severity": entry.get("severity", "info"),
        "category": entry.get("category", "info"),
        "timestamp": timestamp,
        "hostname": system_state.get_hostname(),
        "os": f"{system_state.get_os_name()} ({system_state.get_os_version()})",
    }
    # Dernier relevé système connu (machine hôte).
    sysm = database.get_last_valid(tag=TAG_SYSTEM) or {}
    ctx["system"] = {
        "timestamp": sysm.get("timestamp"),
        "boot_time": system_state.get_boot_time(),
        "cpu_percent": sysm.get("cpu_percent"),
        "mem_percent": sysm.get("mem_percent"),
        "disk_percent": sysm.get("disk_percent"),
    }
    # Dernier relevé du processus Mirth + vue d'ensemble historisée.
    procm = database.get_last_valid(table=TABLE_MIRTH_METRICS, tag=None) or {}
    ctx["mirth_process"] = {
        "timestamp": procm.get("timestamp"),
        "cpu_percent": procm.get("cpu_percent"),
        "mem_percent": procm.get("mem_percent"),
        "sockets": procm.get("sockets"),
    }
    ov = database.get_mirth_overview_latest() or {}
    totals = ov.get("totals") or {}
    ctx["mirth_overview"] = {
        "version": ov.get("version"),
        "channel_count": ov.get("channel_count"),
        "channels_started": ov.get("channels_started"),
        "errors": totals.get("error"),
        "snapshot_at": ov.get("snapshot_at"),
    }
    # Alarme « message en erreur » : joint un aperçu des derniers messages en
    # erreur mis en cache (lecture locale). Pour une alarme réelle, ce champ est
    # remplacé par la liste exacte des nouveaux messages détectés (cf. emit_alarm/
    # dispatch_alerts) ; pour un test depuis la page, il fournit un échantillon.
    if code == "mirth_message_error":
        ctx["error_messages"] = database.get_recent_error_messages(limit=20)
    # Alarmes périphériques : joint l'état courant des cibles (lecture locale).
    # Pour une alarme réelle, ce champ est remplacé par la liste exacte des cibles
    # concernées via le `context` transmis à emit_alarm/dispatch_alerts.
    if code in ("device_unreachable", "device_up"):
        ctx["devices"] = database.get_device_status()
    return ctx


def _pct(v):
    """Formate un pourcentage (ou '—' si inconnu)."""
    return f"{v:.0f} %" if isinstance(v, (int, float)) else "—"


# Longueur maximale du texte d'erreur intégral inclus PAR message dans l'e-mail
# (au-delà, tronqué — le texte complet reste consultable via le lien dashboard).
_ALERT_ERROR_MAXLEN = 4000


def _html_escape(s):
    """Échappe le texte pour une insertion HTML (corps de l'e-mail)."""
    return (str("" if s is None else s)
            .replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;"))


def _attr_escape(s):
    """Échappe une valeur destinée à un attribut HTML entre guillemets (href)."""
    return _html_escape(s).replace('"', "&quot;")


def _dashboard_error_link(m):
    """Lien profond vers le dashboard, modale d'erreur ouverte sur le connecteur.

    Construit `…/statistiques.html?errors=1&channel=&connector=&name=&msg=` à partir
    d'un message en erreur, afin que le destinataire de l'alerte ouvre directement
    le dashboard sur le connecteur concerné avec la modale du message. Renvoie None
    si l'URL publique du service est inconnue (non démarré) ou le canal non identifié.
    """
    if not SERVICE_BASE_URL or not m.get("channel_id"):
        return None
    params = {"errors": "1", "channel": m["channel_id"]}
    if m.get("meta_data_id") is not None:
        params["connector"] = m["meta_data_id"]
    if m.get("channel_name"):
        params["name"] = m["channel_name"]
    if m.get("message_id") is not None:
        params["msg"] = m["message_id"]
    return f"{SERVICE_BASE_URL}/statistiques.html?{urllib.parse.urlencode(params)}"


def build_alert_email(ctx):
    """Construit (sujet, message, html) d'une alerte à partir de son contexte.

    Renvoie un corps texte riche et une variante HTML. La pièce jointe (PJ) n'est
    pas générée pour l'instant ; le contexte rassemblé ici la rendrait possible
    plus tard (cf. quickmail.sendmail(attachment_name, attachment_content)).
    """
    sev = {"critical": "🔴 CRITIQUE", "warning": "🟠 AVERTISSEMENT",
           "info": "🔵 INFORMATION"}.get(ctx["severity"], ctx["severity"])
    subject = f"[Mirth Checker] {ctx['title']} — {ctx['hostname']}"

    s = ctx["system"]
    p = ctx["mirth_process"]
    o = ctx["mirth_overview"]
    lines = [
        f"ALERTE MIRTH CHECKER — {ctx['title']}",
        "=" * 52,
        f"Gravité        : {sev}",
        f"Horodatage     : {ctx['timestamp']}",
        f"Hôte           : {ctx['hostname']}",
        f"Système        : {ctx['os']}",
        f"Démarrage      : {s.get('boot_time') or '—'}",
        "",
        "— État système (dernier relevé) —————————————————————",
        f"  Relevé       : {s.get('timestamp') or '—'}",
        f"  CPU          : {_pct(s.get('cpu_percent'))}",
        f"  Mémoire      : {_pct(s.get('mem_percent'))}",
        f"  Disque       : {_pct(s.get('disk_percent'))}",
        "",
        "— Processus Mirth (dernier relevé) ——————————————————",
        f"  Relevé       : {p.get('timestamp') or '—'}",
        f"  CPU          : {_pct(p.get('cpu_percent'))}",
        f"  Mémoire      : {_pct(p.get('mem_percent'))}",
        f"  Sockets      : {p.get('sockets') if p.get('sockets') is not None else '—'}",
        "",
        "— Serveur Mirth (dernier instantané) ————————————————",
        f"  Version      : {o.get('version') or '—'}",
        f"  Canaux       : {o.get('channels_started') if o.get('channels_started') is not None else '—'}"
        f" / {o.get('channel_count') if o.get('channel_count') is not None else '—'} démarrés",
        f"  Erreurs      : {o.get('errors') if o.get('errors') is not None else '—'}",
        f"  Instantané   : {o.get('snapshot_at') or '—'}",
    ]
    # Détail des messages en erreur (alarme `mirth_message_error`) : liste les
    # canaux/connecteurs concernés AVEC le texte d'erreur intégral, et — si l'URL
    # publique du service est connue — un lien profond ouvrant le dashboard sur le
    # connecteur visé (modale du message). Présent seulement si le contexte le porte.
    msgs = ctx.get("error_messages") or []
    urls = []   # URL complètes affichées dans le corps (rendues cliquables en HTML)
    if msgs:
        lines += ["", f"— Messages en erreur ({len(msgs)}) ————————————————————"]
        for m in msgs[:20]:
            who = m.get("channel_name") or m.get("channel_id") or "?"
            conn = m.get("connector") or "-"
            head = (f"  - {who} / {conn} — msg #{m.get('message_id')} "
                    f"({m.get('received_date') or '—'})")
            if m.get("error_code"):
                head += f" [code {m.get('error_code')}]"
            lines.append(head)
            err = (m.get("error") or "").strip()
            if err:
                if len(err) > _ALERT_ERROR_MAXLEN:
                    err = err[:_ALERT_ERROR_MAXLEN] + " […]"
                lines.append("    Erreur (texte intégral) :")
                lines += [f"      {ln}" for ln in err.splitlines()]
            url = _dashboard_error_link(m)
            if url:
                # URL complète affichée directement sous le message ; elle reste
                # visible/copiable en texte et est rendue cliquable en HTML.
                lines.append(f"    ↳ {url}")
                urls.append(url)
        if len(msgs) > 20:
            lines.append(f"  ... et {len(msgs) - 20} autre(s).")
    # Détail des périphériques (alarmes `device_unreachable`/`device_up`) : liste
    # les cibles (host:port) avec leur état et les canaux/connecteurs qui les visent.
    devs = ctx.get("devices") or []
    if devs:
        lines += ["", f"— Périphériques ({len(devs)}) ——————————————————————"]
        for d in devs[:30]:
            addr = d.get("address") or (f"{d.get('host')}:{d.get('port')}"
                                        if d.get("host") else "?")
            state = ("HORS LIGNE" if d.get("reachable") is False
                     else "en ligne" if d.get("reachable") is True else "non testé")
            probe = []
            if d.get("tcp_ok") is not None:
                probe.append("port " + ("ouvert" if d.get("tcp_ok") else "fermé"))
            if d.get("icmp_ok") is not None:
                probe.append("ICMP " + (f"{d.get('icmp_ms')} ms" if d.get("icmp_ok") else "✗"))
            lines.append(f"  - [{state}] {addr}"
                         + (f"  ({', '.join(probe)})" if probe else ""))
            for c in (d.get("connectors") or [])[:6]:
                who = c.get("channel_name") or c.get("channel_id") or "?"
                lines.append(f"      ↳ {who} / {c.get('name') or '-'}")
        if len(devs) > 30:
            lines.append(f"  ... et {len(devs) - 30} autre(s).")
    lines += ["", "—",
              "Message automatique du service de supervision Mirth Checker."]
    message = "\n".join(lines)

    # Corps HTML : on échappe le texte puis on rend cliquables les URL affichées en
    # clair (le texte visible reste l'URL complète, garantie cliquable même si le
    # client de messagerie n'auto-lie pas les liens en texte brut).
    body_html = _html_escape(message)
    for url in urls:
        esc = _html_escape(url)
        body_html = body_html.replace(
            esc, f"<a href=\"{_attr_escape(url)}\">{esc}</a>")
    html = "".join([
        "<div style=\"font-family:'Segoe UI',Arial,sans-serif;color:#1e293b\">",
        f"<h2 style='margin:0 0 4px'>{_html_escape(ctx['title'])}</h2>",
        f"<p style='margin:0 0 14px;color:#64748b'>{sev} · {ctx['timestamp']} · "
        f"{_html_escape(ctx['hostname'])}</p>",
        "<pre style=\"background:#f1f5f9;border:1px solid #cbd5e1;border-radius:8px;"
        "padding:12px 14px;font-size:13px;white-space:pre-wrap\">"
        f"{body_html}</pre>",
        "</div>",
    ])
    return subject, message, html


def _deliver(method, recipient, ctx):
    """Émet une notification via `method` vers `recipient` pour le contexte `ctx`.

    Retourne une liste de résultats {method, dest, ok[, error]} (un par
    destinataire pour l'e-mail). Seul l'e-mail est implémenté ; les autres méthodes
    renvoient un résultat d'échec « non implémentée ».
    """
    if method == "email":
        recipients = _split_recipients(recipient)
        if not recipients:
            return [{"method": "email", "dest": None, "ok": False,
                     "error": "aucun destinataire e-mail configuré"}]
        subject, message, html = build_alert_email(ctx)
        out = []
        for dest in recipients:
            ok = quickmail.sendmail(sujet=subject, message=message, dest=dest, html=html)
            out.append({"method": "email", "dest": dest, "ok": bool(ok)})
        return out
    return [{"method": method, "dest": recipient or None, "ok": False,
             "error": f"méthode « {method} » non implémentée"}]


def dispatch_alerts(code, timestamp, config=None, test=False, context_extra=None):
    """Émet l'ensemble des notifications activées pour une alarme (envoi multiple).

    N'écrit JAMAIS en base : l'évènement associé est inséré séparément par
    `emit_alarm`. Cette fonction est donc réutilisée telle quelle par le bouton
    « tester l'alerte » (envoi seul, sans enregistrement).

    Args:
        code (str): code d'alarme du catalogue.
        timestamp (str): horodatage utilisé dans le message.
        config (dict|None): configuration {methods, rules} à imposer (instantané
            courant de la page, éventuellement non enregistré). Si None, la
            configuration enregistrée est lue en base.
        test (bool): préfixe le titre par [TEST].
        context_extra (dict|None): éléments de contexte propres à l'occurrence de
            l'alarme (ex. la liste exacte des messages en erreur détectés), fusionnés
            dans le contexte du message — priment sur les valeurs construites en base.

    Returns:
        list[dict]: résultats d'envoi {method, dest, ok[, error]}.
    """
    try:
        cfg = config if config is not None else database.get_alert_config()
    except Exception as e:
        log.log(f"[checker_service] Alerte {code} : lecture config impossible ({e}).")
        return []

    rules = (cfg.get("rules") or {}).get(code, {})
    methods = cfg.get("methods") or {}

    ctx = _build_alarm_context(code, timestamp)
    if context_extra:
        ctx.update(context_extra)
    if test:
        ctx["title"] = "[TEST] " + ctx["title"]

    results = []
    for method, enabled in rules.items():
        if not enabled:
            continue
        m = methods.get(method) or {}
        if not m.get("enabled"):
            continue  # méthode globalement désactivée
        for res in _deliver(method, m.get("recipient"), ctx):
            results.append(res)
            tag = "TEST " if test else ""
            state = "envoyée" if res["ok"] else ("ECHEC : " + res.get("error", "erreur"))
            log.log(f"[checker_service] Alerte {tag}{code} via {res['method']} -> "
                  f"{res.get('dest') or '-'} : {state}.")
    return results


def emit_alarm(code, timestamp=None, dedup=False, detail=None, context=None):
    """Enregistre l'évènement d'une alarme et déclenche ses notifications.

    Point d'entrée unique des alarmes : insère la barre d'évènement (table
    `events`, libellé/couleur inchangés) puis, si un NOUVEL évènement a réellement
    été créé, lance l'envoi des notifications dans un thread dédié (non bloquant
    pour la tâche de relevé). `dedup=True` évite de ré-alerter pour un évènement
    déjà enregistré (ex. même boot système au redémarrage du checker).

    Args:
        code (str): code d'alarme du catalogue.
        timestamp (str|None): horodatage de l'évènement ; courant si None.
        dedup (bool): ignore l'émission si un évènement identique existe déjà.
        detail (str|None): complément libre stocké dans la colonne `details` de
            l'évènement (ex. récapitulatif des messages en erreur détectés).
        context (dict|None): contexte propre à l'occurrence, transmis à
            `dispatch_alerts` pour enrichir le corps de la notification.

    Returns:
        int|None: l'identifiant de l'évènement inséré, ou None si ignoré (dedup).
    """
    entry = ALARM_BY_CODE.get(code)
    if not entry:
        log.log(f"[checker_service] Alarme inconnue : {code!r} — ignorée.")
        return None
    ts = timestamp or datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    eid = database.insert_event(
        timestamp=ts, category=entry["category"], label=entry["event_label"],
        color=_event_color(entry["category"]), source="scheduler",
        details=detail, dedup=dedup)
    # Un évènement réellement créé (non dédupliqué) déclenche la notification.
    if eid is not None:
        threading.Thread(target=dispatch_alerts, args=(code, ts),
                         kwargs={"context_extra": context},
                         name=f"alert-{code}", daemon=True).start()
    return eid


# État précédent du processus Mirth (présent/absent) entre deux relevés, afin de
# détecter les transitions start/stop et d'en émettre une alerte. None = inconnu.
_mirth_prev_found = None


# Valeur représentative de chaque tâche du planificateur, affichée dans la colonne
# « Valeur » du tableau de bord console (cf. lib/dashboard.py). Chaque collecteur y
# dépose, à la fin de son tick, un couple (texte, style_rich). Lecture mémoire pure :
# aucun accès DB/réseau n'a lieu dans la boucle de rendu du dashboard.
_task_summaries = {}


def _pct(v):
    """Pourcentage compact pour la colonne « Valeur » ('?' si indisponible)."""
    return f"{v:.0f}%" if isinstance(v, (int, float)) else "?"


def task_summary(name):
    """Couple (texte, style rich) représentatif d'une tâche pour le tableau de bord.

    Renvoyé au dashboard via le `summary_provider` ; ('—', 'dim') tant que la tâche
    n'a pas encore produit de relevé.
    """
    return _task_summaries.get(name) or ("—", "dim")


def collect_sample():
    """Construit un échantillon système (tag 'system') à partir de system_state."""
    cpu = system_state.get_cpu_usage_global(delay=0.2)
    mem = system_state.get_mem("all")
    try:
        disk = system_state.get_disk_usage(SYSTEM_DRIVE)
    except Exception:
        disk = {"percent": None, "used": None, "total": None}

    return {
        "tag": TAG_SYSTEM,
        "timestamp": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "cpu_percent": cpu,
        "mem_percent": mem.get("percent"),
        "mem_used_gb": round(mem.get("total_gb", 0) - mem.get("available_gb", 0), 2),
        "mem_total_gb": mem.get("total_gb"),
        "disk_percent": disk.get("percent"),
        "disk_used_gb": disk.get("used"),
        "disk_total_gb": disk.get("total"),
    }


def probe_mirth_process():
    """Sonde le(s) processus Mirth et agrège CPU / mémoire / sockets.

    Renvoie un dict {found, pids, cpu_percent, mem_percent, mem_used_gb,
    mem_total_gb, sockets}. `found` est False si aucun processus ne correspond.
    """
    proc_name = mirth_api.get_process_name()
    procs = system_state.get_processes_info([proc_name])
    mem_total = system_state.get_mem("all").get("total_gb")

    if not procs:
        return {"found": False, "process": proc_name, "pids": [],
                "cpu_percent": None, "mem_percent": None, "mem_used_gb": None,
                "mem_total_gb": mem_total, "sockets": None}

    cpu = sum((p.get("cpu") or 0) for p in procs)
    mem_pct = sum((p.get("mem_percent") or 0) for p in procs)
    mem_mb = sum((p.get("mem") or 0) for p in procs)

    # Sockets du processus : ports en écoute + connexions TCP établies.
    try:
        sockets = len(system_state.get_socket(proc_name)) + \
            len(system_state.get_active_connections(proc_name))
    except Exception:
        sockets = None

    return {
        "found": True,
        "process": proc_name,
        "pids": [p.get("pid") for p in procs],
        "cpu_percent": round(cpu, 2),
        "mem_percent": round(mem_pct, 2),
        "mem_used_gb": round(mem_mb / 1024.0, 3),
        "mem_total_gb": mem_total,
        "sockets": sockets,
    }


def collect_mirth_sample():
    """Construit un échantillon du processus Mirth (tag 'mirth').

    Si le processus est absent, le relevé est nul (cpu None) : la courbe se brise
    sur la période d'arrêt, comme pour les coupures système.
    """
    p = probe_mirth_process()
    return {
        "tag": TAG_MIRTH,
        "timestamp": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "cpu_percent": p["cpu_percent"],
        "mem_percent": p["mem_percent"],
        "mem_used_gb": p["mem_used_gb"],
        "mem_total_gb": p["mem_total_gb"],
        "disk_percent": None,
        "disk_used_gb": None,
        "disk_total_gb": None,
        "sockets": p["sockets"],
    }


def scheduled_check():
    """Tâche programmée 'system' : relève et enregistre l'état de la machine."""
    sample = collect_sample()
    database.insert_metric(sample)
    cpu, mem = sample.get("cpu_percent"), sample.get("mem_percent")
    alert = ((isinstance(cpu, (int, float)) and cpu >= 85)
             or (isinstance(mem, (int, float)) and mem >= 90))
    _task_summaries["metrics-collector"] = (
        f"CPU {_pct(cpu)} · Mém {_pct(mem)}", "yellow" if alert else "")


def scheduled_mirth_check():
    """Tâche programmée 'mirth' : relève et enregistre l'état du processus Mirth.

    Détecte aussi les transitions de présence du processus (démarrage/arrêt) entre
    deux relevés et en émet une alerte (barre verticale superposée aux graphes).
    """
    global _mirth_prev_found
    sample = collect_mirth_sample()
    database.insert_metric(sample, table=TABLE_MIRTH_METRICS)

    # Présence déduite du relevé : cpu nul => processus introuvable sur ce tick.
    found = sample["cpu_percent"] is not None
    if found:
        sock = sample.get("sockets")
        sock_txt = f" · {sock} sock" if isinstance(sock, int) else ""
        _task_summaries["mirth-collector"] = (
            f"CPU {_pct(sample['cpu_percent'])} · Mém {_pct(sample['mem_percent'])}{sock_txt}", "")
    else:
        _task_summaries["mirth-collector"] = ("processus absent", "red")
    if _mirth_prev_found is not None and found != _mirth_prev_found:
        if found:
            emit_alarm("mirth_up")
            log.log("[checker_service] Alerte : démarrage du processus Mirth détecté.")
        else:
            emit_alarm("mirth_down")
            log.log("[checker_service] Alerte : arrêt du processus Mirth détecté.")
    _mirth_prev_found = found


def scheduled_mirth_overview():
    """Tâche programmée : historise l'overview Mirth complet (un seul appel API).

    Réutilise l'unique appel `get_overview()` pour persister, via
    `database.insert_mirth_snapshot`, l'instantané serveur (totaux + version +
    stats JVM + joignabilité) et — si joignable — l'état/les compteurs de chaque
    canal et connecteur. Sert ensuite la page sans nouvel appel API live. Si
    l'API est injoignable, seule la ligne serveur (reachable=0) est écrite : la
    série se brise visiblement sur la coupure.

    Détecte aussi, sur le même tick, l'arrivée de nouveaux messages en erreur
    (canal/connecteur) afin d'en émettre l'alarme `mirth_message_error`.
    """
    ov = mirth_api.get_overview(timeout=8)
    database.insert_mirth_snapshot(ov)
    detect_mirth_error_messages(ov)
    if ov.get("reachable"):
        nch = ov.get("channel_count") or 0
        err = (ov.get("totals") or {}).get("error") or 0
        _task_summaries["mirth-overview-collector"] = (
            f"{nch} canaux · {err} err", "red" if err else "")
    else:
        _task_summaries["mirth-overview-collector"] = ("injoignable", "red")


# Jeu des clés (channel_id, message_id, meta_data_id) des messages signalés en
# erreur par Mirth au tick précédent. None = inconnu (avant le premier relevé) :
# le premier tick établit la base SANS alerter, afin que les erreurs déjà présentes
# au démarrage ne soient pas comptées comme « nouvelles ».
_mirth_error_keys_prev = None


def detect_mirth_error_messages(overview, timeout=8):
    """Détecte les nouveaux messages en erreur depuis le tick précédent.

    Compare le jeu autoritaire des clés de messages en erreur (fourni par Mirth,
    `list_error_message_keys` — léger, sans contenu) à celui du tick précédent.
    Toute clé nouvelle correspond à un message fraîchement passé en erreur : on en
    met le contenu en cache puis on émet l'alarme `mirth_message_error` (un seul
    évènement par tick, agrégeant les messages concernés).

    L'overview courant sert de filet rapide : si AUCUN canal n'affiche d'erreur et
    qu'aucune erreur n'était suivie, on évite l'appel supplémentaire. En cas de
    serveur injoignable, on ne fait rien (l'alarme `mirth_down` couvre la coupure ;
    la base de comparaison est conservée pour le retour en ligne).
    """
    global _mirth_error_keys_prev
    if not overview.get("reachable"):
        return

    # Filet rapide : pas d'erreur maintenant ni précédemment => rien à comparer.
    has_errors_now = any(
        isinstance(c.get("error"), int) and c["error"] > 0
        for c in overview.get("channels", []))
    if not has_errors_now and not _mirth_error_keys_prev:
        _mirth_error_keys_prev = set()
        return

    keylist = mirth_api.list_error_message_keys(timeout=timeout)
    if not keylist.get("reachable"):
        return

    current = {(m.get("channel_id"), m.get("message_id"), m.get("meta_data_id")): m
               for m in keylist.get("messages", [])}
    current_keys = set(current.keys())

    # Premier tick : on établit la base sans alerter (erreurs déjà présentes).
    if _mirth_error_keys_prev is None:
        _mirth_error_keys_prev = current_keys
        return

    new_keys = current_keys - _mirth_error_keys_prev
    _mirth_error_keys_prev = current_keys
    if new_keys:
        _handle_new_error_messages([current[k] for k in new_keys], timeout=timeout)


def _format_error_messages_detail(msgs):
    """Récapitulatif court (colonne `details` de l'évènement) des messages."""
    parts = []
    for m in msgs[:20]:
        who = m.get("channel_name") or m.get("channel_id") or "?"
        parts.append(f"{who} / {m.get('connector') or '-'} (msg #{m.get('message_id')})")
    extra = f" +{len(msgs) - 20}" if len(msgs) > 20 else ""
    return f"{len(msgs)} nouveau(x) message(s) en erreur : " + " ; ".join(parts) + extra


def _handle_new_error_messages(new_msgs, timeout=8):
    """Met en cache les nouveaux messages en erreur puis émet l'alarme associée.

    Le contenu (texte d'erreur intégral + corps brut) des nouveaux messages est
    téléchargé par canal concerné et ajouté au cache `mirth_messages` (INSERT OR
    IGNORE). Les messages détectés (liste légère, sans texte d'erreur) sont ensuite
    enrichis depuis le cache afin que l'e-mail d'alerte porte le texte d'erreur
    intégral. L'alarme passe par le processus standard (`emit_alarm` => évènement +
    notification selon config).
    """
    for cid in {m.get("channel_id") for m in new_msgs if m.get("channel_id")}:
        res = mirth_api.get_error_messages(channel_id=cid, timeout=timeout)
        if res.get("reachable"):
            database.upsert_mirth_messages(
                [_to_cache_row(m) for m in res.get("messages", [])])

    # Enrichit chaque message détecté avec le texte d'erreur/contenu mis en cache
    # (la liste autoritaire est légère : sans `error` ni `content`).
    new_keys = [(m.get("channel_id"), m.get("message_id"), m.get("meta_data_id"))
                for m in new_msgs]
    cached = {(str(r.get("channel_id")), str(r.get("message_id")),
               str(r.get("meta_data_id"))): r
              for r in database.get_cached_messages(new_keys)}
    enriched = []
    for m in new_msgs:
        k = (str(m.get("channel_id")), str(m.get("message_id")),
             str(m.get("meta_data_id")))
        full = cached.get(k)
        if full:
            m = {**m, "error": full.get("error"), "content": full.get("content")}
        enriched.append(m)

    detail = _format_error_messages_detail(enriched)
    log.log(f"[checker_service] Alerte : {detail}")
    emit_alarm("mirth_message_error", detail=detail,
               context={"error_messages": enriched})


# ==========================================================================
# SUPERVISION DES PÉRIPHÉRIQUES (« clients Mirth ») — collecteur de fond
# Teste périodiquement la connectivité des cibles réseau (host, port) visées par
# les connecteurs Mirth. On ne teste QUE les couples ip/port, et JAMAIS deux fois
# la même IP (ICMP dédupliqué par hôte ; port TCP par couple host/port). Historise
# l'agrégat (en ligne / en erreur) pour le graphe et émet une alarme paramétrable
# quand une cible devient injoignable / revient en ligne.
# ==========================================================================
# Nom de la tâche du planificateur (partagé entre main() et la ligne d'état).
DEVICE_TASK_NAME = "device-ping-collector"

# Timeouts COURTS dédiés au collecteur de fond : ping < 1 s + test de port bref,
# pour ne pas retarder la grille du planificateur sur une cible injoignable. Avec
# les sondes lancées EN PARALLÈLE (cf. _probe_targets), la durée totale d'un tick
# reste bornée par la cible la plus lente (~1 s) et non par leur somme.
_DEVICE_PING_TIMEOUT = 0.8
_DEVICE_TCP_TIMEOUT = 0.8

# Pool de sondes concurrentes (borne le nombre de threads créés par tick).
_DEVICE_PROBE_WORKERS = 16

# Config des connecteurs mise en cache (GET /channels est plus lourd et la config
# change peu) : rafraîchie tous les _DEVICE_ENDPOINTS_REFRESH_EVERY ticks, ou tant
# qu'aucune lecture n'a encore abouti.
_DEVICE_ENDPOINTS_REFRESH_EVERY = 10
_device_endpoints_cache = None
_device_endpoints_tick = 0

# Jeu des cibles (host, port) injoignables au tick précédent. None = inconnu :
# 1er tick / post-redémarrage => baseline silencieuse (pas d'alarme rétroactive),
# comme `_mirth_error_keys_prev`.
_device_down_prev = None

# Dernier résumé de connectivité, affiché dans la ligne d'état console.
_device_last_summary = ""


def _device_targets(endpoints):
    """Cibles réseau UNIQUES (host, port) à tester, agrégeant leurs connecteurs.

    Ne retient que les connecteurs réseau ayant À LA FOIS un hôte et un port (« on
    ne teste que les ip/port »). Les cibles sont dédupliquées sur (host, port) : un
    même équipement visé par plusieurs connecteurs n'apparaît qu'une fois, avec la
    liste des connecteurs qui le visent.
    """
    targets = {}
    for ep in endpoints or []:
        host = ep.get("host")
        port = ep.get("port")
        if not host or port is None or not ep.get("pingable"):
            continue
        key = (host, port)
        t = targets.get(key)
        if t is None:
            t = {"host": host, "port": port,
                 "address": ep.get("address") or f"{host}:{port}",
                 "transport": ep.get("transport"), "connectors": []}
            targets[key] = t
        t["connectors"].append({
            "channel_id": ep.get("channel_id"),
            "channel_name": ep.get("channel_name"),
            "meta_data_id": ep.get("meta_data_id"),
            "name": ep.get("name"),
            "role": ep.get("role"),
        })
    return list(targets.values())


def _probe_targets(targets, ping_timeout=_DEVICE_PING_TIMEOUT,
                   tcp_timeout=_DEVICE_TCP_TIMEOUT):
    """Sonde des cibles (host, port) UNIQUES : ICMP + port TCP, EN PARALLÈLE.

    Les sondes s'exécutent dans un petit pool de threads, si bien que la durée
    totale d'un relevé reste bornée par la cible la PLUS LENTE (~1 s) et non par
    leur somme — le tick tient ainsi largement dans l'intervalle du planificateur,
    même avec de nombreuses cibles. L'ICMP est dédupliqué PAR HÔTE (jamais deux
    pings sur la même IP) et le port TCP par couple (host, port). Le port TCP
    (signal applicatif fiable) pilote `reachable`. Fonction PURE ; ne lève jamais.
    """
    if not targets:
        return []
    hosts = list({t.get("host") for t in targets if t.get("host")})
    couples = list({(t.get("host"), t.get("port")) for t in targets})

    def _do_ping(host):
        try:
            ms = system_state.run_ping(host, timeout=ping_timeout)
        except Exception:
            ms = None
        return ms if isinstance(ms, (int, float)) else None

    def _do_tcp(couple):
        try:
            return system_state.check_tcp_port(couple[0], couple[1], timeout=tcp_timeout)
        except Exception:
            return None

    # Ping (par hôte) ET test de port (par couple) lancés ensemble dans le pool :
    # tout le balayage s'effectue en une seule vague concurrente.
    icmp, tcp = {}, {}
    workers = max(1, min(_DEVICE_PROBE_WORKERS, len(hosts) + len(couples)))
    submitted = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as ex:
        for h in hosts:
            submitted[ex.submit(_do_ping, h)] = ("icmp", h)
        for c in couples:
            submitted[ex.submit(_do_tcp, c)] = ("tcp", c)
        for fut in concurrent.futures.as_completed(submitted):
            kind, key = submitted[fut]
            (icmp if kind == "icmp" else tcp)[key] = fut.result()

    out = []
    for t in targets:
        host, port = t.get("host"), t.get("port")
        icmp_ms = icmp.get(host)
        tcp_ms = tcp.get((host, port))
        tcp_ok = tcp_ms is not None
        out.append({**t, "icmp_ok": icmp_ms is not None, "icmp_ms": icmp_ms,
                    "tcp_ok": tcp_ok, "tcp_ms": tcp_ms,
                    "reachable": tcp_ok, "tested": True})
    return out


def _device_label(d):
    """Étiquette courte d'une cible pour les messages (adresse + 1er canal visé)."""
    addr = d.get("address") or (f"{d.get('host')}:{d.get('port')}"
                                if d.get("host") else "?")
    conns = d.get("connectors") or []
    who = conns[0].get("channel_name") if conns else None
    extra = f" +{len(conns) - 1}" if len(conns) > 1 else ""
    return addr + (f" ({who}{extra})" if who else "")


def _format_device_detail(devices, what):
    """Récapitulatif court (colonne `details` de l'évènement) des cibles."""
    parts = [_device_label(d) for d in devices[:20]]
    extra = f" +{len(devices) - 20}" if len(devices) > 20 else ""
    return f"{len(devices)} peripherique(s) {what} : " + " ; ".join(parts) + extra


def scheduled_device_check():
    """Tâche programmée : teste la connectivité des cibles (host, port) Mirth.

    Réutilise le contrat d'endpoint (mirth_api.get_connector_endpoints, mis en
    cache). Ne teste QUE les couples ip/port, et jamais deux fois la même IP.
    Historise l'état courant (device_status) + l'agrégat du tick (device_history),
    rafraîchit la ligne d'état console, puis émet les alarmes de transition.
    """
    global _device_endpoints_cache, _device_endpoints_tick, _device_last_summary
    # 1. (Re)lecture de la config des connecteurs (cache module, rafraîchi par N).
    if (_device_endpoints_cache is None
            or _device_endpoints_tick % _DEVICE_ENDPOINTS_REFRESH_EVERY == 0):
        ov = mirth_api.get_connector_endpoints(timeout=8)
        if ov.get("reachable"):
            _device_endpoints_cache = ov.get("endpoints", [])
    _device_endpoints_tick += 1

    targets = _device_targets(_device_endpoints_cache or [])
    ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    devices = _probe_targets(targets)
    online = sum(1 for d in devices if d.get("reachable") is True)
    offline = sum(1 for d in devices if d.get("reachable") is False)

    # 2. Persistance : état courant (recap + référence d'alarme) + point de série.
    database.upsert_device_status(devices, timestamp=ts)
    database.insert_device_history(timestamp=ts, total=len(devices), online=online,
                                   offline=offline, detail=devices)

    # 3. Ligne d'état console : résumé compact accolé au collecteur (OK:n/KO:m).
    _device_last_summary = (f"(OK:{online}/KO:{offline})" if devices
                            else "(aucune cible)")
    _task_summaries[DEVICE_TASK_NAME] = (
        (f"OK {online} · KO {offline}", "red" if offline else "")
        if devices else ("aucune cible", "dim"))

    # 4. Détection des transitions (alarmes), clé = (host, port).
    _detect_device_transitions(devices, ts)


def scheduled_purge():
    """Tâche programmée : applique la rétention en purgeant l'historique trop ancien.

    Supprime les relevés de plus de `RETENTION_DAYS` jours dans toutes les tables
    horodatées (cf. `database.purge_older_than`). No-op si la rétention est
    désactivée (RETENTION_DAYS <= 0). Exécutée une fois par jour à 03:00 (heure
    creuse), plus une fois au démarrage du service (la rétention étant en jours,
    une seule purge quotidienne suffit).
    """
    if RETENTION_DAYS <= 0:
        _task_summaries[RETENTION_TASK_NAME] = ("illimitée", "dim")
        return
    deleted = database.purge_older_than(days=RETENTION_DAYS)
    _task_summaries[RETENTION_TASK_NAME] = (
        f"{RETENTION_DAYS} j · {deleted} purgés", "yellow" if deleted else "")
    if deleted:
        log.log(f"[checker_service] Rétention {RETENTION_DAYS} j : "
                f"{deleted} ligne(s) ancienne(s) purgée(s).")


SESSION_CLEANUP_TASK_NAME = "session-cleanup"


def scheduled_session_cleanup():
    """Tâche programmée : purge les sessions web expirées (une fois/jour).

    Les sessions sont validées à chaque requête (fenêtre glissante) ; ce ménage
    ne fait que supprimer les lignes déjà expirées pour ne pas laisser grossir la
    table `web_sessions`.
    """
    n = database.purge_expired_sessions()
    _task_summaries[SESSION_CLEANUP_TASK_NAME] = (
        f"{n} expirée(s)" if n else "à jour", "" if n else "dim")


def _detect_device_transitions(devices, ts):
    """Diffe les cibles injoignables vs le tick précédent et émet les alarmes.

    Clé = (host, port) — un périphérique, pas un connecteur — pour éviter les
    doublons. 1er tick / post-redémarrage : baseline silencieuse (pas d'alarme).
    """
    global _device_down_prev
    down_now, up_now = {}, {}
    for d in devices:
        if not d.get("tested"):
            continue
        key = (d.get("host"), d.get("port"))
        if d.get("reachable") is False:
            down_now[key] = d
        elif d.get("reachable") is True:
            up_now[key] = d
    down_keys = set(down_now.keys())

    if _device_down_prev is None:
        _device_down_prev = down_keys
        return

    new_down = down_keys - _device_down_prev
    recovered = _device_down_prev - down_keys
    _device_down_prev = down_keys

    if new_down:
        affected = [down_now[k] for k in new_down]
        detail = _format_device_detail(affected, "injoignable(s)")
        log.log("[checker_service] Alerte : " + detail)
        emit_alarm("device_unreachable", timestamp=ts, detail=detail,
                   context={"devices": affected})
    if recovered:
        # Retour en ligne : seulement les cibles re-sondées joignables ce tick (une
        # cible disparue de la config sort aussi du set, sans déclencher d'alarme « up »).
        affected = [up_now[k] for k in recovered if k in up_now]
        if affected:
            detail = _format_device_detail(affected, "de retour en ligne")
            log.log("[checker_service] Alerte : " + detail)
            emit_alarm("device_up", timestamp=ts, detail=detail,
                       context={"devices": affected})


def mark_startup_events():
    """Au démarrage du service, matérialise dans la base les interruptions afin
    qu'elles soient visibles sur le graphe (la courbe se brise sur ces points) :

    - un marqueur 'restart' (relevé nul) à la minute suivant le dernier relevé
      valide, pour signaler l'arrêt du logiciel pendant la coupure ;
    - un marqueur 'boot' (relevé nul) à l'heure de démarrage du système, lorsque
      celui-ci a redémarré depuis le dernier relevé valide.
    """
    fmt = "%Y-%m-%d %H:%M:%S"
    now = datetime.datetime.now()
    last = database.get_last_valid()

    last_dt = None
    if last:
        try:
            last_dt = datetime.datetime.strptime(last["timestamp"], fmt)
        except (ValueError, KeyError, TypeError):
            last_dt = None

    # 1. Marqueur d'arrêt logiciel : minute suivant le dernier relevé valide.
    #    - relevé nul tagué 'restart' => la courbe système se brise sur la coupure ;
    #    - alerte 'service' => barre verticale « Arrêt du checker » sur tous les graphes.
    if last_dt:
        gap_dt = last_dt + datetime.timedelta(minutes=1)
        gap_str = gap_dt.strftime(fmt)
        if gap_dt < now and database.insert_event_marker(gap_str, "restart"):
            log.log(f"[checker_service] Marqueur d'arrêt inséré à {gap_str}.")
            emit_alarm("checker_down", timestamp=gap_str, dedup=True)

    # 1bis. Même marqueur d'arrêt sur la courbe du processus Mirth (table dédiée),
    #       pour qu'elle se brise aussi pendant la coupure du checker.
    mirth_last = database.get_last_valid(table=TABLE_MIRTH_METRICS, tag=None)
    if mirth_last:
        try:
            mlast_dt = datetime.datetime.strptime(mirth_last["timestamp"], fmt)
        except (ValueError, KeyError, TypeError):
            mlast_dt = None
        if mlast_dt:
            mgap_dt = mlast_dt + datetime.timedelta(minutes=1)
            mgap_str = mgap_dt.strftime(fmt)
            if mgap_dt < now and database.insert_event_marker(
                    mgap_str, "restart", tag=TAG_MIRTH, table=TABLE_MIRTH_METRICS):
                log.log(f"[checker_service] Marqueur d'arrêt Mirth inséré à {mgap_str}.")

    # 2. Alerte de démarrage du checker (toujours, à l'instant présent).
    emit_alarm("checker_up", timestamp=now.strftime(fmt))

    # 3. Démarrage système, si un boot a eu lieu depuis le dernier relevé : marqueur
    #    nul 'boot' (brise la courbe système) + alerte 'boot' (barre sur tous les graphes).
    try:
        boot_dt = datetime.datetime.strptime(system_state.get_boot_time(), fmt)
    except ValueError:
        boot_dt = None
    if boot_dt and (last_dt is None or boot_dt > last_dt):
        boot_str = boot_dt.strftime(fmt)
        if database.insert_event_marker(boot_str, "boot"):
            log.log(f"[checker_service] Marqueur de démarrage système inséré à {boot_str}.")
        emit_alarm("system_boot", timestamp=boot_str, dedup=True)


# ==========================================================================
# LIGNE D'ÉTAT CONSOLE (auto-écrasée à chaque exécution d'une tâche)
# ==========================================================================
def make_scheduler_status_line(tasks):
    """Construit le hook `on_complete` qui rafraîchit la LIGNE D'ÉTAT console.

    À chaque fin d'exécution d'une tâche du planificateur, réaffiche (via
    ``lib.log.status``, qui réécrit la ligne en place) la durée de la dernière
    exécution de CHAQUE tâche, sous la forme :

        [metrics-collector] 0.213s / [mirth-collector] 1.041s / [device-ping-collector] 0.812s (OK:1/KO:2)

    Une tâche qui n'a pas encore tourné est affichée « ----- ». Le collecteur de
    périphériques porte en suffixe un résumé compact de son dernier balayage
    (OK = cibles en ligne / KO = en erreur, cf. ``_device_last_summary``). La
    sérialisation (verrou) et la cohabitation avec les logs persistants sont gérées
    par ``lib.log`` ; inactif hors terminal.
    """
    def render(_task=None):
        parts = []
        for t in tasks:
            d = t.last_duration
            seg = f"[{t.name}] {d:.3f}s" if d is not None else f"[{t.name}] -----"
            # Résumé de connectivité accolé au collecteur de périphériques.
            if t.name == DEVICE_TASK_NAME and _device_last_summary:
                seg += " " + _device_last_summary
            parts.append(seg)
        log.status(" / ".join(parts))

    return render


# ==========================================================================
# API : ÉTAT SYSTÈME (system_state)
# ==========================================================================
def api_system(req):
    """Instantané système complet (global)."""
    cpu = system_state.get_cpu("all", delay=0.2)
    mem = system_state.get_mem("all")
    counts = system_state.get_system_counts()
    net = system_state.get_network_io()
    sockets = system_state.get_tcp_udp_count()

    disks = []
    for part in system_state.get_storage_partitions():
        try:
            disks.append(system_state.get_disk_usage(part.mountpoint))
        except Exception:
            continue

    return {
        "datetime": system_state.get_now_datetime(),
        "boot_time": system_state.get_boot_time(),
        "os": f"{system_state.get_os_name()} ({system_state.get_os_version()})",
        "cpu": cpu,
        "memory": mem,
        "counts": counts,
        "network_io": net,
        "connections": sockets,
        "disks": disks,
        "vpn": system_state.get_vpn_status(),
    }


def api_processes(req):
    """Liste des processus filtrés par cible (?target=chrome,python) ou top global."""
    target = req.get("target")
    if target:
        targets = [t.strip() for t in target.split(",") if t.strip()]
        return {"target": target, "processes": system_state.get_processes_info(targets)}
    return {"target": "all", "processes": system_state.get_cpu("LISTALL")["processes"]}


def api_ping(req):
    """Ping d'un hôte (?host=8.8.8.8)."""
    host = req.get("host", "8.8.8.8")
    value = system_state.run_ping(host)
    return {"host": host, "latency_ms": round(value, 2) if value else None,
            "reachable": value is not None}


def api_sockets(req):
    """Ports en écoute, filtrables par processus (?target=...)."""
    target = req.get("target", "ALL")
    return {"target": target, "sockets": system_state.get_socket(target)}


# ==========================================================================
# API : INFOS À LA DEMANDE (system_state) — multi-format json/text/html
# ==========================================================================
# Modèle de page HTML pour les sorties format=html (tableaux tabulate).
_HTML_HEAD = (
    "<!DOCTYPE html><html lang='fr'><head><meta charset='utf-8'>"
    "<meta name='viewport' content='width=device-width, initial-scale=1'>"
    "<title>{title}</title><style>"
    "body{{font-family:'Segoe UI',Arial,sans-serif;margin:24px;color:#1e293b;background:#f8fafc;}}"
    "h1{{font-size:22px;}}"
    "h2{{margin-top:26px;color:#0f172a;border-bottom:2px solid #e2e8f0;padding-bottom:4px;}}"
    "table{{border-collapse:collapse;margin-top:8px;background:#fff;}}"
    "table,th,td{{border:1px solid #cbd5e1;}}"
    "th,td{{padding:6px 11px;text-align:left;vertical-align:top;white-space:pre-line;}}"
    "th{{background:#f1f5f9;}}"
    "</style></head><body><h1>{title}</h1>"
)


def _render_sections(sections, fmt, title="Infos"):
    """Rend une liste de sections {key,title,headers,rows,data} au format demandé.

    - json (défaut) : dict {key: data} sérialisé par le serveur ;
    - text          : tableaux tabulate ASCII (Response text/plain) ;
    - html          : page HTML avec tableaux tabulate (Response text/html).
    """
    fmt = (fmt or "json").lower()

    def _rows(s):
        # Filet de sécurité : tout flottant résiduel est affiché à 2 décimales.
        return [[_fmt2(c) if isinstance(c, float) else c for c in row]
                for row in s["rows"]]

    if fmt == "text":
        parts = []
        for s in sections:
            parts.append(f"=== {s['title']} ===")
            try:
                parts.append(tabulate(_rows(s), headers=s["headers"],
                                      tablefmt="grid", floatfmt=".2f"))
            except Exception:
                parts.append(tabulate(_rows(s), headers=s["headers"],
                                      tablefmt="simple", floatfmt=".2f"))
            parts.append("")
        return webserver.Response("\n".join(parts),
                                  content_type="text/plain; charset=utf-8")

    if fmt == "html":
        parts = [_HTML_HEAD.format(title=title)]
        for s in sections:
            parts.append(f"<h2>{s['title']}</h2>")
            parts.append(tabulate(_rows(s), headers=s["headers"],
                                  tablefmt="html", floatfmt=".2f"))
        parts.append("</body></html>")
        return webserver.Response("\n".join(parts),
                                  content_type="text/html; charset=utf-8")

    return {s["key"]: s["data"] for s in sections}


# Types d'informations système disponibles (ordre = ordre d'affichage par défaut).
SYSINFO_TYPES = ["datetime", "boottime", "os", "cpu", "mem", "counts",
                 "storage", "network", "socket", "connections",
                 "interfaces", "vpn", "ping"]


def _sysinfo_sections(types, host="8.8.8.8"):
    """Construit les sections demandées à partir des sondes de system_state."""
    sections = []
    for key in types:
        if key == "datetime":
            v = system_state.get_now_datetime()
            sections.append({"key": key, "title": "Date / Heure",
                             "headers": ["Indicateur", "Valeur"],
                             "rows": [["Date/Heure", v]], "data": v})
        elif key == "boottime":
            v = system_state.get_boot_time()
            sections.append({"key": key, "title": "Démarrage",
                             "headers": ["Indicateur", "Valeur"],
                             "rows": [["Dernier boot", v]], "data": v})
        elif key == "os":
            data = {"name": system_state.get_os_name(),
                    "version": system_state.get_os_version()}
            sections.append({"key": key, "title": "Système d'exploitation",
                             "headers": ["Indicateur", "Valeur"],
                             "rows": [["OS", f"{data['name']} ({data['version']})"]],
                             "data": data})
        elif key == "cpu":
            d = system_state.get_cpu("all", delay=0.2)
            sections.append({"key": key, "title": "CPU",
                             "headers": ["Indicateur", "Valeur"],
                             "rows": [["Usage global", f"{_fmt2(d['usage_global'])} %"],
                                      ["Cœurs physiques", d['coeurs_phys']],
                                      ["Cœurs logiques", d['coeurs_logiq']]],
                             "data": d})
        elif key == "mem":
            d = system_state.get_mem("all")
            sections.append({"key": key, "title": "Mémoire",
                             "headers": ["Indicateur", "Valeur"],
                             "rows": [["Utilisation", f"{_fmt2(d['percent'])} %"],
                                      ["Disponible", f"{_fmt2(d['available_gb'])} Go"],
                                      ["Total", f"{_fmt2(d['total_gb'])} Go"]],
                             "data": d})
        elif key == "counts":
            d = system_state.get_system_counts()
            sections.append({"key": key, "title": "Compteurs système",
                             "headers": ["Indicateur", "Valeur"],
                             "rows": [["Processus", d['processes']],
                                      ["Threads", d['threads']],
                                      ["Handles", d['handles']]],
                             "data": d})
        elif key == "storage":
            disks, rows = [], []
            for part in system_state.get_storage_partitions():
                try:
                    u = system_state.get_disk_usage(part.mountpoint)
                except Exception:
                    continue
                disks.append(u)
                rows.append([u['path'], f"{_fmt2(u['percent'])} %",
                             _fmt2(u['free']), _fmt2(u['total'])])
            sections.append({"key": key, "title": "Stockage",
                             "headers": ["Lecteur", "Utilisé", "Libre (Go)", "Total (Go)"],
                             "rows": rows, "data": disks})
        elif key == "network":
            d = system_state.get_network_io()
            sections.append({"key": key, "title": "Réseau (E/S cumulées)",
                             "headers": ["Indicateur", "Valeur"],
                             "rows": [["Envoyé", f"{_fmt2(d['sent_mb'])} Mo"],
                                      ["Reçu", f"{_fmt2(d['recv_mb'])} Mo"]],
                             "data": d})
        elif key == "socket":
            d = system_state.get_socket("ALL")
            rows = [[s['proto'], s['port'], s['ip'], s['pid'], s['proc_name']] for s in d]
            sections.append({"key": key, "title": "Ports en écoute",
                             "headers": ["Proto", "Port", "IP", "PID", "Processus"],
                             "rows": rows, "data": d})
        elif key == "connections":
            d = system_state.get_active_connections("ALL")
            rows = [[c['pid'], c['proc_name'], c['laddr'], c['raddr']] for c in d]
            sections.append({"key": key, "title": "Connexions actives (ESTABLISHED)",
                             "headers": ["PID", "Processus", "Locale", "Distante"],
                             "rows": rows, "data": d})
        elif key == "interfaces":
            d = system_state.get_vpn_interfaces()
            rows = [[i['name'], i['stats'], i['ips']] for i in d]
            sections.append({"key": key, "title": "Interfaces réseau",
                             "headers": ["Adaptateur", "Stats", "IP"],
                             "rows": rows, "data": d})
        elif key == "vpn":
            d = system_state.get_vpn_status()
            rows = [[n] for n in d] or [["(aucun VPN actif)"]]
            sections.append({"key": key, "title": "VPN actifs",
                             "headers": ["Interface"], "rows": rows, "data": d})
        elif key == "ping":
            v = system_state.run_ping(host)
            val = _ceil2(v) if v else None
            sections.append({"key": key, "title": f"Ping {host}",
                             "headers": ["Hôte", "Latence (ms)"],
                             "rows": [[host, _fmt2(val) if val is not None else "TIMEOUT"]],
                             "data": {"host": host, "latency_ms": val,
                                      "reachable": val is not None}})
    return sections


def api_getsysteminfo(req):
    """Infos système à la demande.

    Paramètres :
      ?type=cpu,mem,storage,...  (liste d'infos ; vide => toutes)
      &host=8.8.8.8              (hôte ciblé par 'ping')
      &format=json|text|html     (json par défaut)
    """
    raw = req.get("type", "")
    types = [t.strip().lower() for t in raw.split(",") if t.strip()]
    types = [t for t in types if t in SYSINFO_TYPES] or SYSINFO_TYPES
    host = req.get("host") or "8.8.8.8"
    fmt = req.get("format", "json")
    sections = _sysinfo_sections(types, host=host)
    return _render_sections(sections, fmt, title="Infos système")


# Colonnes disponibles pour /api/getprocessinfo.
PROCINFO_COLUMNS = ["pid", "name", "cpu", "mem", "mem_percent", "ports"]
PROCINFO_LABELS = {"pid": "PID", "name": "Nom", "cpu": "CPU %",
                   "mem": "Mémoire (Mo)", "mem_percent": "Mémoire %",
                   "ports": "Ports TCP"}


def api_getprocessinfo(req):
    """Infos processus à la demande.

    Paramètres :
      ?target=chrome,python   (cibles ; vide => top processus par CPU)
      &type=pid,name,cpu,...   (colonnes à renvoyer ; vide => toutes)
      &limit=10                (taille du top si pas de cible)
      &format=json|text|html
    """
    target = req.get("target")
    cols_raw = req.get("type", "")
    cols = [c.strip().lower() for c in cols_raw.split(",") if c.strip()]
    cols = [c for c in cols if c in PROCINFO_COLUMNS] or PROCINFO_COLUMNS
    try:
        limit = int(req.get("limit", 10))
    except ValueError:
        limit = 10
    fmt = req.get("format", "json")

    # Les valeurs restent brutes ; le plafond à 2 décimales est appliqué par le
    # transform JSON (réponses json) et par _fmt2 (tableaux text/html) ci-dessous.
    if target:
        targets = [t.strip() for t in target.split(",") if t.strip()]
        raw = system_state.get_processes_info(targets)
        records = [{"pid": p["pid"], "name": p["name"], "cpu": p["cpu"],
                    "mem": p["mem"], "mem_percent": p["mem_percent"],
                    "ports": p["ports"]} for p in raw]
        records.sort(key=lambda r: (r["cpu"] or 0, r["mem"] or 0), reverse=True)
        scope = target
    else:
        raw = system_state.get_process_list()
        records = [{"pid": p["pid"], "name": p["name"], "cpu": p["cpu_percent"],
                    "mem": p["memory_rss_mb"],
                    "mem_percent": p["memory_percent"], "ports": []}
                   for p in raw if p["pid"] != 0]
        records.sort(key=lambda r: r["cpu"] or 0, reverse=True)
        records = records[:limit]
        scope = f"top {limit} CPU"

    headers = [PROCINFO_LABELS[c] for c in cols]
    rows = []
    for r in records:
        row = []
        for c in cols:
            v = r[c]
            if c == "ports":
                v = ", ".join(map(str, v)) if v else "-"
            elif isinstance(v, float):
                v = _fmt2(v)
            row.append("" if v is None else v)
        rows.append(row)

    section = {"key": "processes", "title": f"Processus ({scope})",
               "headers": headers, "rows": rows, "data": records}
    return _render_sections([section], fmt, title="Infos processus")


# ==========================================================================
# API : HISTORIQUE (base SQLite)
# ==========================================================================
def api_history(req):
    """Historique des métriques, selon deux modes.

    - Intervalle de dates : ?date_deb=YYYY-MM-DD[ HH:MM:SS]&date_fin=...
      (une date seule couvre la journée entière). Prioritaire si date_deb ou
      date_fin est fourni ; chaque borne est optionnelle.
    - Dernières heures : ?hours=24 (hours=0 => tout l'historique).

    Filtre de source : ?tag=system (défaut) | mirth | ... ; ?tag=all =>
    toutes les sources confondues.
    """
    date_deb = (req.get("date_deb") or "").strip() or None
    date_fin = (req.get("date_fin") or "").strip() or None
    # tag absent/vide => 'system' (compatibilité) ; tag=all => toutes sources.
    tag = (req.get("tag") or "system").strip() or "system"
    out_tag = tag
    # Rétro-compatibilité : tag=mirth lit désormais la table dédiée 'mirth_metrics'.
    table = "metrics"
    if tag.lower() == "mirth":
        table, tag = TABLE_MIRTH_METRICS, None
    elif tag.lower() == "all":
        tag = ""   # get_history interprète '' comme « toutes sources »

    if date_deb or date_fin:
        rows = database.get_history(date_deb=date_deb, date_fin=date_fin,
                                    tag=tag, table=table)
        return {"tag": out_tag, "date_deb": date_deb, "date_fin": date_fin,
                "count": len(rows), "samples": rows}

    try:
        hours = float(req.get("hours", 24))
    except ValueError:
        hours = 24
    rows = database.get_history(hours=hours, tag=tag, table=table)
    return {"tag": out_tag, "hours": hours, "count": len(rows), "samples": rows}


def api_history_latest(req):
    """Dernier relevé enregistré pour la source ?tag=system (défaut) | mirth | ..."""
    tag = (req.get("tag") or "system").strip() or "system"
    out_tag = tag
    table = "metrics"
    if tag.lower() == "mirth":
        table, tag = TABLE_MIRTH_METRICS, None
    elif tag.lower() == "all":
        tag = ""
    return {"tag": out_tag, "latest": database.get_latest(tag=tag, table=table)}


# ==========================================================================
# API : ÉVÈNEMENTS / ALERTES (second jeu de données temporel, table `events`)
# Superposables à TOUS les graphiques (barres verticales colorées + texte).
# ==========================================================================
def api_events(req):
    """Liste les évènements/alertes à superposer aux graphes.

    Paramètres (mêmes conventions que /api/history) :
      ?date_deb=...&date_fin=...   (intervalle de dates, prioritaire)
      ?hours=24                    (dernières heures ; 0 => tout l'historique)
      &category=boot|service|mirth|alarm|cmd|network|mail|...  (filtre optionnel)
    """
    date_deb = (req.get("date_deb") or "").strip() or None
    date_fin = (req.get("date_fin") or "").strip() or None
    category = (req.get("category") or "").strip() or None

    if date_deb or date_fin:
        rows = database.get_events(date_deb=date_deb, date_fin=date_fin, category=category)
    else:
        try:
            hours = float(req.get("hours", 24))
        except ValueError:
            hours = 24
        rows = database.get_events(hours=hours, category=category)

    # Garantit une couleur d'affichage même pour les évènements enregistrés sans.
    for r in rows:
        r["color"] = _event_color(r.get("category"), r.get("color"))
    return {"count": len(rows), "events": rows}


def api_setevent(req):
    """Enregistre un évènement/alerte (barre verticale superposée aux graphes).

    Accepte les paramètres en JSON (POST) ou en query string (GET, pratique pour
    tester depuis le navigateur) :
      category (def 'info'), label, color (sinon dérivée), timestamp (sinon maintenant),
      source (def 'api'), details.
    """
    data = req.json() if req.body else {}

    def pick(key, default=None):
        v = data.get(key)
        if v is None:
            v = req.get(key)
        return v if v not in (None, "") else default

    category = pick("category", "info")
    label = pick("label")
    # Horodatage : celui fourni, sinon l'instant présent (résolu ici pour que la
    # réponse renvoie la valeur réellement enregistrée).
    timestamp = pick("timestamp") or datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    source = pick("source", "api")
    details = pick("details")
    if not label:
        return (400, {"ok": False, "error": "Champ requis : label."})

    color = _event_color(category, pick("color"))
    eid = database.insert_event(timestamp=timestamp, category=category, label=label,
                                color=color, source=source, details=details)
    return {"ok": True, "id": eid,
            "event": {"id": eid, "timestamp": timestamp, "category": category,
                      "label": label, "color": color, "source": source,
                      "details": details}}


# ==========================================================================
# API : SUPERVISION & MAINTENANCE DE LA BASE (base SQLite)
# ==========================================================================
def api_db_info(req):
    """État détaillé de la base : taille, fragmentation, tables, bornes temporelles.

    Y joint la rétention automatique configurée (`retention_days`) afin que la page
    affiche la politique en vigueur et propose la même valeur par défaut à la purge.
    """
    stats = database.get_db_stats()
    stats["retention_days"] = RETENTION_DAYS
    return stats


def api_db_integrity(req):
    """Vérification d'intégrité de la base (PRAGMA integrity_check)."""
    return database.integrity_check()


def api_db_export(req):
    """Téléchargement d'une copie cohérente du fichier SQLite (.db)."""
    data = database.export_bytes()
    stamp = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
    filename = f"checker_history_{stamp}.db"
    return webserver.Response(
        data,
        content_type="application/x-sqlite3",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


def api_db_export_csv(req):
    """Export CSV de la table des relevés (metrics)."""
    text = database.export_csv()
    stamp = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
    filename = f"metrics_{stamp}.csv"
    return webserver.Response(
        text,
        content_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


def api_db_vacuum(req):
    """Défragmente/compacte la base (VACUUM)."""
    result = database.vacuum()
    return {"ok": True, "operation": "vacuum", **result}


def api_db_reset(req):
    """Réinitialise la base : supprime tous les relevés puis compacte.

    Garde-fou : nécessite ?confirm=1 (ou {"confirm": true} dans le corps).
    """
    data = req.json() if req.body else {}
    confirmed = req.get("confirm") in ("1", "true", "yes") or data.get("confirm") is True
    if not confirmed:
        return (400, {"ok": False,
                      "error": "Confirmation requise (confirm=1) pour réinitialiser."})
    result = database.reset_db()
    return {"ok": True, "operation": "reset", **result}


def api_db_purge(req):
    """Supprime les relevés plus vieux que ?days=N jours (défaut = rétention configurée)."""
    default_days = RETENTION_DAYS if RETENTION_DAYS > 0 else 30
    try:
        days = int(req.get("days", default_days))
    except ValueError:
        days = default_days
    if days < 0:
        return (400, {"ok": False, "error": "Le nombre de jours doit être positif."})
    deleted = database.purge_older_than(days=days)
    return {"ok": True, "operation": "purge", "days": days, "deleted_rows": deleted}


def api_db_import(req):
    """Importe/remplace la base par un fichier SQLite envoyé dans le corps brut.

    Le client POSTe les octets du fichier `.db` (Content-Type quelconque).
    L'ancienne base est sauvegardée en `.bak` avant remplacement.
    """
    raw = req.body or b""
    if not raw:
        return (400, {"ok": False, "error": "Corps vide : aucun fichier reçu."})
    try:
        result = database.import_db(raw)
    except ValueError as e:
        return (400, {"ok": False, "error": str(e)})
    return {"operation": "import", **result}


# ==========================================================================
# API : ANALYSE DES LOGS MIRTH (mirth_logs_parser)
# ==========================================================================
def _build_mirth_summary(logfile, date=1, trait_rotatelog=False):
    """Analyse un fichier de log et renvoie un résumé structuré (JSON friendly)."""
    from collections import Counter

    parsed_files = mirth_logs_parser.mirth_file_parser(
        logfile, date=date, trait_rotatelog=trait_rotatelog
    )

    decoded = []
    files_info = []
    for fi in parsed_files:
        files_info.append({
            "filename": os.path.basename(fi["filename"]),
            "raw_lines": fi["raw_lines"],
            "parsed_logs": fi["parsed_logs"],
            "size_bytes": fi["filestat"]["size_bytes"],
            "mtime": fi["filestat"]["mtime"],
        })
        for entry in fi["parsed_lines"]:
            decoded.append(mirth_logs_parser.mirth_log_decoder(entry))

    total = len(decoded)

    # Répartition par niveau
    level_counts = Counter(e["type"] or "UNKNOWN" for e in decoded)
    levels = [{"level": lvl, "count": c,
               "percent": round(c / total * 100, 1) if total else 0}
              for lvl, c in level_counts.most_common()]

    # Répartition par canal
    channels = {}
    for e in decoded:
        name = e["channel_name"] or "Global / Serveur"
        ch = channels.setdefault(name, {"channel": name, "channel_id": e["channel_id"],
                                        "total": 0, "info": 0, "error": 0, "other": 0,
                                        "last": None})
        ch["total"] += 1
        if e["type"] == "INFO":
            ch["info"] += 1
        elif e["type"] == "ERROR":
            ch["error"] += 1
        else:
            ch["other"] += 1
        if e["datetime"] and (ch["last"] is None or e["datetime"] > ch["last"]):
            ch["last"] = e["datetime"]
    channel_list = sorted(channels.values(), key=lambda x: x["total"], reverse=True)

    # Erreurs regroupées par cause / message
    error_groups = {}
    for e in decoded:
        if e["type"] != "ERROR":
            continue
        detail = e["cause"] or e["message"] or "(sans détail)"
        grp = error_groups.setdefault(detail, {"detail": detail, "count": 0,
                                               "channel": e["channel_name"] or "Global / Serveur",
                                               "last": None})
        grp["count"] += 1
        if e["datetime"] and (grp["last"] is None or e["datetime"] > grp["last"]):
            grp["last"] = e["datetime"]
    errors = sorted(error_groups.values(), key=lambda x: x["count"], reverse=True)

    timestamps = [e["datetime"] for e in decoded if e["datetime"]]

    return {
        "logfile": os.path.abspath(logfile),
        "exists": True,
        "date_filter": date,
        "files": files_info,
        "total_entries": total,
        "oldest": min(timestamps) if timestamps else None,
        "newest": max(timestamps) if timestamps else None,
        "levels": levels,
        "channels": channel_list,
        "errors": errors,
    }


def api_mirth(req):
    """
    Analyse des logs Mirth.
    Paramètres : ?logfile=...&date=1&rotate=0
    date : 1=tout, 0=aujourd'hui, -1=J-1, -X=J-X.
    """
    logfile = req.get("logfile") or DEFAULT_LOGFILE
    try:
        date = int(req.get("date", 1))
    except ValueError:
        date = 1
    rotate = req.get("rotate", "0") in ("1", "true", "yes")

    if not os.path.exists(logfile):
        return (404, {"error": f"Fichier introuvable : {logfile}", "exists": False})

    return _build_mirth_summary(logfile, date=date, trait_rotatelog=rotate)


# ==========================================================================
# API : SUPERVISION MIRTH (API REST Mirth + processus mcservice.exe)
# ==========================================================================
def _mirth_timeout(req):
    """Lit le paramètre ?timeout=… (secondes), avec repli sur 8 s."""
    try:
        return float(req.get("timeout", 8))
    except ValueError:
        return 8


def api_mirth_api(req):
    """Vue d'ensemble du serveur Mirth, servie INTÉGRALEMENT depuis l'HISTORIQUE.

    Les canaux/connecteurs/totaux/version proviennent du dernier instantané
    historisé par le collecteur `mirth-overview-collector` ; la JOIGNABILITÉ et la
    version « courantes » proviennent de la toute dernière relève de ce même
    collecteur (`get_mirth_server_latest`). AUCUN appel réseau à Mirth n'est fait
    ici : la page se charge donc à la vitesse de SQLite. L'état affiché reflète le
    dernier tick du collecteur (au plus l'intervalle de collecte de retard), ce qui
    est suffisant pour de la supervision. Le champ `snapshot_at` indique l'âge des
    données canaux ; `reachable_at` celui de l'état de joignabilité.

    En l'absence d'instantané (service tout juste démarré), bascule sur un appel
    `get_overview()` live pour ne pas afficher une page vide.
    """
    data = database.get_mirth_overview_latest()
    base_url = mirth_api.get_config()["MIRTH_BASE_URL"]

    # Pas encore d'instantané historisé : repli sur un appel live complet.
    if not data.get("snapshot_at"):
        ov = mirth_api.get_overview(timeout=_mirth_timeout(req))
        ov["snapshot_at"] = None
        return ov

    data["base_url"] = base_url
    # Joignabilité/version « courantes » = dernière relève du collecteur (toute,
    # pas seulement joignable), lue en base — pas de login Mirth synchrone.
    latest = database.get_mirth_server_latest()
    if latest:
        data["reachable"] = latest["reachable"]
        if latest.get("version"):
            data["version"] = latest["version"]
        data["error"] = latest.get("error")
        data["reachable_at"] = latest["timestamp"]
    else:
        data["reachable"] = True
        data["reachable_at"] = data.get("snapshot_at")
    return data


def api_mirth_kpi_baseline(req):
    """Repère (baseline) des KPI cumulés Mirth — lecture (GET).

    Renvoie la dernière photo enregistrée des totaux « reçus »/« erreurs » (fixée
    via le bouton de la page statistiques), ou des champs nuls si aucun repère n'a
    encore été posé. La page s'en sert pour afficher l'écart (actuel − repère).
    """
    return database.get_kpi_baseline() or {"received": None, "error": None,
                                            "saved_at": None}


def api_mirth_kpi_baseline_save(req):
    """Repère (baseline) des KPI cumulés Mirth — enregistrement (POST).

    Mémorise les totaux cumulés « reçus » et « erreurs » du dernier instantané
    historisé (les mêmes valeurs que celles affichées par la page) afin de pouvoir
    afficher ensuite l'écart depuis ce repère. Les valeurs sont relues CÔTÉ SERVEUR
    (aucune confiance au corps de requête) ; repli sur un appel live si aucun
    instantané n'existe encore. Renvoie 503 si Mirth n'a fourni aucune donnée.
    """
    ov = database.get_mirth_overview_latest()
    if not ov.get("snapshot_at"):
        # Pas encore d'instantané historisé : repli sur un appel live. `get_overview`
        # renvoie des totaux à zéro même injoignable, d'où le contrôle `reachable`.
        live = mirth_api.get_overview(timeout=_mirth_timeout(req))
        if not live.get("reachable"):
            return (503, {"ok": False, "error": "Aucune donnee Mirth disponible : "
                          "impossible de fixer un repere."})
        ov = live
    totals = ov.get("totals") or {}
    received = int(totals.get("received") or 0)
    error = int(totals.get("error") or 0)
    saved = database.set_kpi_baseline(received, error)
    log.log(f"[mirth-kpi] Repere fixe : recus={received}, erreurs={error}")
    return {"ok": True, "baseline": saved}


def api_mirth_channels(req):
    """Liste des canaux Mirth et de leurs statistiques (vue allégée).

    Filtre optionnel ?channel=<texte> sur le nom du canal (insensible à la casse).
    """
    data = mirth_api.get_channels_overview(timeout=_mirth_timeout(req))
    flt = (req.get("channel") or "").strip().lower()
    if flt and data.get("channels"):
        data["channels"] = [c for c in data["channels"]
                            if flt in (c.get("name") or "").lower()]
        data["channel_count"] = len(data["channels"])
    return data


def api_mirth_connectors(req):
    """Liste à plat des connecteurs (source + destinations) de tous les canaux.

    Filtre optionnel ?channel=<channelId> pour ne renvoyer que les connecteurs
    d'un canal donné.
    """
    channel = (req.get("channel") or "").strip() or None
    return mirth_api.get_connectors_overview(channel_id=channel,
                                             timeout=_mirth_timeout(req))


def api_mirth_stats(req):
    """Statistiques agrégées sur l'ensemble des canaux (totaux + compteurs)."""
    return mirth_api.get_global_statistics(timeout=_mirth_timeout(req))


def api_mirth_server(req):
    """Version, infos JVM/OS et statistiques système du serveur Mirth."""
    return mirth_api.get_server_info(timeout=_mirth_timeout(req))


def api_mirth_errors(req):
    """Canaux Mirth en erreur (statistique ERROR > 0 ou état d'erreur)."""
    return mirth_api.get_errors(timeout=_mirth_timeout(req))


def _to_cache_row(m):
    """Projette un message (mirth_api) sur les colonnes du cache `mirth_messages`."""
    return {k: m.get(k) for k in database._MSG_FIELDS}


def cached_error_messages(channel_id=None, connector=None, limit=50, timeout=8):
    """Messages en erreur, servis via le cache SQLite avec l'API Mirth pour autorité.

    Stratégie « Mirth fait foi » :
      1. Mirth fournit la liste LÉGÈRE (sans contenu) des messages actuellement en
         erreur — l'état provient toujours de l'API.
      2. On détermine ceux absents du cache (par leur clé stable).
      3. On ne télécharge (avec contenu) que les canaux ayant des messages manquants,
         puis on les ajoute au cache.
      4. On renvoie le contenu depuis le cache, restreint au jeu de clés autoritaire
         (les entrées de cache obsolètes ne ressortent pas).

    En cas de serveur Mirth injoignable : on renvoie tel quel le diagnostic de
    l'API (pas de service de cache aveugle), l'API restant prioritaire pour l'état.
    """
    keylist = mirth_api.list_error_message_keys(channel_id=channel_id, limit=limit,
                                                timeout=timeout)
    if not keylist.get("reachable"):
        return keylist

    light = keylist.get("messages", [])
    if connector is not None:
        light = [m for m in light if str(m.get("meta_data_id")) == str(connector)]

    # Clés autoritaires (ce que Mirth signale en erreur à l'instant T).
    light_by_key = {(m.get("channel_id"), m.get("message_id"), m.get("meta_data_id")): m
                    for m in light}
    auth_keys = list(light_by_key.keys())
    auth_set = set(auth_keys)

    # Manquants au cache => téléchargement (par canal concerné, avec contenu).
    have = database.get_cached_message_keys(channel_id=channel_id)
    missing_channels = {k[0] for k in auth_keys if k not in have}
    downloaded = 0
    for cid in missing_channels:
        res = mirth_api.get_error_messages(channel_id=cid, limit=limit, timeout=timeout)
        if res.get("reachable"):
            downloaded += database.upsert_mirth_messages(
                [_to_cache_row(m) for m in res.get("messages", [])])

    # Restitution depuis le cache, filtrée au jeu autoritaire.
    cached = database.get_cached_messages(auth_set)
    messages = []
    for row in cached:
        k = (row.get("channel_id"), row.get("message_id"), row.get("meta_data_id"))
        light_meta = light_by_key.get(k, {})
        # Les méta légères (état/retry à jour) priment sur celles, figées, du cache.
        for fld in ("status", "send_attempts", "error_code", "received_date",
                    "connector", "category", "channel_name"):
            if light_meta.get(fld) is not None:
                row[fld] = light_meta[fld]
        row.pop("cached_at", None)
        row.pop("id", None)
        messages.append(row)
    messages.sort(key=lambda x: x.get("received_date") or "", reverse=True)

    # Récapitulatif par canal (noms issus de la liste légère).
    names = {m.get("channel_id"): m.get("channel_name") for m in light}
    counts = {}
    for m in messages:
        counts[m["channel_id"]] = counts.get(m["channel_id"], 0) + 1
    channels = [{"channel_id": cid, "name": names.get(cid), "count": n}
                for cid, n in counts.items()]

    return {
        "reachable": True, "error": None, "base_url": keylist.get("base_url"),
        "channel_id": channel_id, "connector": connector,
        "channel_name": names.get(channel_id) if channel_id else None,
        "messages": messages, "count": len(messages), "channels": channels,
        "cache": {"authoritative": len(auth_keys), "downloaded": downloaded},
    }


def api_mirth_messages(req):
    """Messages en erreur d'un canal Mirth (ou de tous les canaux en erreur).

    Servis via le cache SQLite (`cached_error_messages`) afin de ne pas re-télécharger
    le contenu à chaque consultation — l'API Mirth restant l'autorité sur l'état.

    Paramètres :
      ?channel=<channel_id>   (absent => tous les canaux ayant des erreurs)
      &connector=<metaDataId> (optionnel : restreint à un connecteur — 0=source)
      &limit=50               (nombre max de messages remontés par canal)
      &timeout=8              (délai réseau en secondes)

    Chaque message renvoyé porte son canal d'origine, son horodatage, le nombre de
    tentatives d'envoi (retry), la catégorie d'erreur, le texte d'erreur intégral
    et le contenu brut du message.
    """
    channel_id = (req.get("channel") or "").strip() or None
    connector = (req.get("connector") or "").strip()
    connector = connector if connector != "" else None
    try:
        limit = int(req.get("limit", 50))
    except ValueError:
        limit = 50
    return cached_error_messages(channel_id=channel_id, connector=connector,
                                 limit=limit, timeout=_mirth_timeout(req))


def api_mirth_process(req):
    """Instantané live du processus Mirth (CPU / mémoire / sockets)."""
    p = probe_mirth_process()
    p["latest"] = database.get_latest(table=TABLE_MIRTH_METRICS, tag=None)
    return p


def api_mirth_history(req):
    """Historique des relevés du processus Mirth (table dédiée `mirth_metrics`).

    Mêmes conventions que /api/history : ?date_deb=&date_fin= (intervalle,
    prioritaire) ou ?hours=24 (0 => tout l'historique).
    """
    date_deb = (req.get("date_deb") or "").strip() or None
    date_fin = (req.get("date_fin") or "").strip() or None
    if date_deb or date_fin:
        rows = database.get_history(date_deb=date_deb, date_fin=date_fin,
                                    tag=None, table=TABLE_MIRTH_METRICS)
        return {"tag": TAG_MIRTH, "date_deb": date_deb, "date_fin": date_fin,
                "count": len(rows), "samples": rows}
    try:
        hours = float(req.get("hours", 24))
    except ValueError:
        hours = 24
    rows = database.get_history(hours=hours, tag=None, table=TABLE_MIRTH_METRICS)
    return {"tag": TAG_MIRTH, "hours": hours, "count": len(rows), "samples": rows}


def api_mirth_history_latest(req):
    """Dernier relevé enregistré du processus Mirth (table `mirth_metrics`)."""
    return {"tag": TAG_MIRTH, "latest": database.get_latest(
        table=TABLE_MIRTH_METRICS, tag=None)}


def api_mirth_throughput(req):
    """Historique de débit Mirth (compteurs cumulatifs reçus/envoyés/erreurs).

    Mêmes conventions que /api/history : ?date_deb=&date_fin= (intervalle,
    prioritaire) ou ?hours=24. Sélection de la série :
      - sans ?channel              => série GLOBALE (totaux serveur) ;
      - ?channel=<id>              => série du canal ;
      - ?channel=<id>&connector=<m> => série d'un connecteur précis (0=source).
    Le débit (msg/min) se calcule par delta côté client.
    """
    date_deb = (req.get("date_deb") or "").strip() or None
    date_fin = (req.get("date_fin") or "").strip() or None
    channel_id = (req.get("channel") or "").strip() or None
    connector = (req.get("connector") or "").strip()
    meta_data_id = None
    if channel_id and connector != "":
        try:
            meta_data_id = int(connector)
        except ValueError:
            meta_data_id = None
    if date_deb or date_fin:
        rows = database.get_mirth_series(date_deb=date_deb, date_fin=date_fin,
                                         channel_id=channel_id,
                                         meta_data_id=meta_data_id)
        return {"channel_id": channel_id, "connector": meta_data_id,
                "date_deb": date_deb, "date_fin": date_fin,
                "count": len(rows), "samples": rows}
    try:
        hours = float(req.get("hours", 24))
    except ValueError:
        hours = 24
    rows = database.get_mirth_series(hours=hours, channel_id=channel_id,
                                     meta_data_id=meta_data_id)
    return {"channel_id": channel_id, "connector": meta_data_id, "hours": hours,
            "count": len(rows), "samples": rows}


def api_mirth_report(req):
    """Rapport Mirth détaillé complet (serveur + canaux + connecteurs + erreurs).

    Paramètres : ?messages=1 (inclure le détail des messages en erreur),
    &limit=50 (max par canal), &timeout=8.
    """
    include = req.get("messages", "0") in ("1", "true", "yes")
    try:
        limit = int(req.get("limit", 50))
    except ValueError:
        limit = 50
    return mirth_api.build_full_report(include_messages=include, limit=limit,
                                       timeout=_mirth_timeout(req))


# Types d'informations Mirth disponibles pour /api/getmirthinfo.
MIRTHINFO_TYPES = ["server", "stats", "channels", "errors"]


def api_getmirthinfo(req):
    """Infos Mirth (API REST) à la demande — multi-format json/text/html.

    Paramètres :
      ?type=server,stats,channels,errors  (liste ; vide => toutes)
      &channel=<texte>                     (filtre sur le nom de canal)
      &timeout=8                           (délai réseau en secondes)
      &format=json|text|html               (json par défaut)

    Même modèle que /api/getsysteminfo : une seule interrogation du serveur
    Mirth alimente toutes les sections demandées.
    """
    raw = req.get("type", "")
    types = [t.strip().lower() for t in raw.split(",") if t.strip()]
    types = [t for t in types if t in MIRTHINFO_TYPES] or MIRTHINFO_TYPES
    fmt = req.get("format", "json")

    ov = mirth_api.get_overview(timeout=_mirth_timeout(req))
    if not ov.get("reachable"):
        section = {"key": "mirth", "title": "Serveur Mirth injoignable",
                   "headers": ["Indicateur", "Valeur"],
                   "rows": [["URL", ov.get("base_url")], ["Erreur", ov.get("error")]],
                   "data": ov}
        return _render_sections([section], fmt, title="Infos Mirth")

    channels = ov.get("channels", [])
    flt = (req.get("channel") or "").strip().lower()
    if flt:
        channels = [c for c in channels if flt in (c.get("name") or "").lower()]

    sections = []
    for key in types:
        if key == "server":
            info = ov.get("system_info") or {}
            rows = [["URL de l'API", ov.get("base_url")],
                    ["Version Mirth", ov.get("version") or "-"]]
            for label, k in (("OS", "osName"), ("OS (version)", "osVersion"),
                             ("JVM", "jvmVersion")):
                if k in info:
                    rows.append([label, info.get(k)])
            sections.append({"key": "server", "title": "Serveur Mirth",
                             "headers": ["Indicateur", "Valeur"], "rows": rows,
                             "data": {"base_url": ov.get("base_url"),
                                      "version": ov.get("version"),
                                      "system_info": ov.get("system_info"),
                                      "system_stats": ov.get("system_stats")}})
        elif key == "stats":
            totals = mirth_api.compute_totals(channels)
            rows = [["Canaux", f"{ov.get('channels_started', 0)} / {len(channels)}"],
                    ["Reçus", totals["received"]], ["Filtrés", totals["filtered"]],
                    ["En file", totals["queued"]], ["Envoyés", totals["sent"]],
                    ["Erreurs", totals["error"]]]
            sections.append({"key": "stats", "title": "Statistiques globales",
                             "headers": ["Indicateur", "Valeur"], "rows": rows,
                             "data": {"totals": totals,
                                      "channel_count": len(channels),
                                      "channels_started": ov.get("channels_started")}})
        elif key == "channels":
            ordered = sorted(channels, key=lambda c: (c.get("name") or "").lower())
            rows = [[c.get("name") or "-", c.get("state") or "-",
                     c.get("received"), c.get("filtered"), c.get("queued"),
                     c.get("sent"), c.get("error")] for c in ordered]
            sections.append({"key": "channels", "title": "Canaux",
                             "headers": ["Canal", "État", "Reçus", "Filtrés",
                                         "En file", "Envoyés", "Erreurs"],
                             "rows": rows, "data": ordered})
        elif key == "errors":
            faulty = [c for c in channels
                      if (isinstance(c.get("error"), int) and c["error"] > 0)
                      or (c.get("state") or "").upper() in ("ERROR", "PAUSED")]
            faulty.sort(key=lambda c: (c.get("error") or 0), reverse=True)
            rows = [[c.get("name") or "-", c.get("state") or "-", c.get("error")]
                    for c in faulty] or [["(aucun canal en erreur)", "", ""]]
            sections.append({"key": "errors", "title": "Canaux en erreur",
                             "headers": ["Canal", "État", "Erreurs"],
                             "rows": rows, "data": faulty})

    return _render_sections(sections, fmt, title="Infos Mirth")


# ==========================================================================
# API : ENVOI D'EMAIL (quickmail)
# ==========================================================================
def api_mail(req):
    """
    Envoi d'un email. Corps JSON attendu : {"subject", "message", "dest"}.
    """
    data = req.json()
    subject = data.get("subject")
    message = data.get("message")
    dest = data.get("dest")
    if not (subject and message and dest):
        return (400, {"error": "Champs requis : subject, message, dest"})

    ok = quickmail.sendmail(sujet=subject, message=message, dest=dest)
    return ({"sent": True, "dest": dest} if ok
            else (502, {"sent": False, "error": "Échec de l'envoi (voir logs serveur)"}))


# ==========================================================================
# API : CONFIGURATION DES ALERTES (page alerte.html)
# ==========================================================================
def api_alerts_config(req):
    """Catalogue des alarmes + méthodes + configuration enregistrée.

    Sert à peupler la page de configuration : la liste des alarmes connues, la
    liste des méthodes de notification (e-mail actif ; MQTT/SMS/Slack réservés) et
    l'état courant (destinataires + matrice OUI/NON) lu en base.
    """
    return {
        "alarms": ALARM_CATALOG,
        "methods": ALERT_METHODS,
        "config": database.get_alert_config(),
    }


def api_alerts_save(req):
    """Enregistre la configuration des alertes : destinataires + matrice OUI/NON.

    Corps JSON attendu :
      {
        "methods": {"email": {"enabled": true, "recipient": "a@b, c@d"}, ...},
        "rules":   {"mirth_down": {"email": true}, ...}
      }
    """
    data = req.json() if req.body else {}
    methods = data.get("methods")
    rules = data.get("rules")
    if not isinstance(methods, dict) and not isinstance(rules, dict):
        return (400, {"ok": False, "error": "Corps invalide : 'methods' et/ou 'rules' attendus."})
    # Ne conserve que des codes/méthodes connus (anti-pollution de la table).
    valid_methods = {m["method"] for m in ALERT_METHODS}
    methods = {k: v for k, v in (methods or {}).items()
               if k in valid_methods and isinstance(v, dict)}
    rules = {code: {m: bool(en) for m, en in by.items() if m in valid_methods}
             for code, by in (rules or {}).items()
             if code in ALARM_BY_CODE and isinstance(by, dict)}
    database.save_alert_config(methods=methods, rules=rules)
    return {"ok": True, "config": database.get_alert_config()}


def api_alerts_test(req):
    """Teste l'envoi d'une MÉTHODE (bouton de fin de ligne du 1er tableau).

    N'écrit pas en base : envoie un message de test via une seule méthode, vers son
    destinataire courant (valeur éventuellement non enregistrée, saisie dans la
    page ; à défaut, celle enregistrée). Envoi synchrone, résultat par destinataire.

    Corps JSON : {"method": "email"|"mqtt", "recipient": "...", "code": "..."}.
    `code` ne sert qu'à bâtir un message représentatif (défaut : 1re alarme).
    """
    data = req.json() if req.body else {}
    method = data.get("method") or "email"
    if method not in {m["method"] for m in ALERT_METHODS}:
        return (400, {"ok": False, "error": f"Méthode inconnue : {method}."})
    code = data.get("code")
    if code not in ALARM_BY_CODE:
        code = ALARM_CATALOG[0]["code"]

    recipient = data.get("recipient")
    if not recipient:
        recipient = (database.get_alert_methods().get(method) or {}).get("recipient")

    ctx = _build_alarm_context(code, datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    ctx["title"] = "[TEST] " + ctx["title"]
    results = _deliver(method, recipient, ctx)
    return {"ok": bool(results) and all(r["ok"] for r in results), "results": results}


def api_alerts_test_alarm(req):
    """Teste une ALARME complète (bouton de fin de ligne du 2e tableau).

    Envoi MULTIPLE via toutes les méthodes activées pour cette alarme, SANS aucun
    enregistrement en base (ni évènement, ni relevé) — utile en test/dev. Réutilise
    `dispatch_alerts` (qui n'écrit jamais en base ; seul `emit_alarm` insère
    l'évènement, et il n'est pas appelé ici).

    Corps JSON : {"code": "...", "methods": {...}, "rules": {...}}. Si `methods`/
    `rules` sont fournis (instantané courant de la page, possiblement non
    enregistré), ils priment ; sinon la configuration enregistrée est utilisée.
    """
    data = req.json() if req.body else {}
    code = data.get("code")
    if code not in ALARM_BY_CODE:
        return (400, {"ok": False, "error": "Code d'alarme inconnu."})

    config = None
    if isinstance(data.get("methods"), dict) or isinstance(data.get("rules"), dict):
        config = {"methods": data.get("methods") or {}, "rules": data.get("rules") or {}}

    ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    results = dispatch_alerts(code, ts, config=config, test=True)
    if not results:
        return {"ok": False, "results": [],
                "error": "Aucune méthode activée pour cette alarme (ou aucun destinataire)."}
    return {"ok": all(r["ok"] for r in results), "results": results}


# ==========================================================================
# API : IDENTITÉ MACHINE (hostname / boot / heure locale)
# ==========================================================================
def api_hostinfo(req):
    """Identité de la machine : nom d'hôte, démarrage système, heure locale."""
    return {
        "hostname": system_state.get_hostname(),
        "boot_time": system_state.get_boot_time(),
        "now": system_state.get_now_datetime(),
        "os": f"{system_state.get_os_name()} ({system_state.get_os_version()})",
    }


# ==========================================================================
# API : STATUT DU SERVICE
# ==========================================================================
def make_api_status(tasks, started_at):
    def api_status(req):
        statuses = [t.status() for t in tasks]
        return {
            "service": "checker_service",
            "started_at": started_at,
            "now": system_state.get_now_datetime(),
            # Rétro-compatibilité : `scheduler` = première tâche ; `schedulers` = toutes.
            "scheduler": statuses[0] if statuses else None,
            "schedulers": statuses,
            "db_path": database.DEFAULT_DB_PATH,
            "system_drive": SYSTEM_DRIVE,
        }
    return api_status


# ==========================================================================
# CONSTRUCTION DES ROUTES
# ==========================================================================
# ==========================================================================
# API : SUPERVISION DES PÉRIPHÉRIQUES (connectivité ICMP + port TCP, à la demande)
# ==========================================================================
def _probe_target(host, port, tcp_timeout=2.0):
    """Sonde UN couple (hôte, port) : ICMP + port TCP, et renvoie son verdict.

    Cœur partagé entre le balayage complet `probe_endpoints` et le test incrémental
    `api_devices_probe_one` (un spinner par ligne, mise à jour au fil de l'eau) :
      * ICMP via system_state.run_ping (indicatif, souvent filtré par les pare-feux) ;
      * test du port TCP via system_state.check_tcp_port (signal fiable applicatif).
    Le port TCP, quand il est connu, pilote `reachable` ; sinon l'ICMP fait foi.
    Renvoie {icmp_ok, icmp_ms, tcp_ok, tcp_ms, reachable, tested}. Ne lève jamais.
    """
    try:
        icmp_ms = system_state.run_ping(host)   # ms, None (timeout) ou False (inconnu)
    except Exception:
        icmp_ms = None
    if not isinstance(icmp_ms, (int, float)):
        icmp_ms = None
    icmp_ok = icmp_ms is not None
    if port is not None:
        try:
            tcp_ms = system_state.check_tcp_port(host, port, timeout=tcp_timeout)
        except Exception:
            tcp_ms = None
        tcp_ok = tcp_ms is not None
        reachable = tcp_ok
    else:
        tcp_ms, tcp_ok, reachable = None, None, icmp_ok
    return {"icmp_ok": icmp_ok, "icmp_ms": icmp_ms, "tcp_ok": tcp_ok,
            "tcp_ms": tcp_ms, "reachable": reachable, "tested": True}


def probe_endpoints(endpoints, tcp_timeout=2.0):
    """Teste la connectivité d'une liste d'endpoints (cf. mirth_api.get_connector_endpoints).

    Pour chaque connecteur réseau « pingable », délègue à `_probe_target` (ICMP + port
    TCP). Les connecteurs non réseau / en écoute locale (0.0.0.0) ne sont pas testés
    (`reachable=None`, `tested=False`). Les couples (host, port) sont dédupliqués : un
    même équipement visé par plusieurs connecteurs n'est sondé qu'une fois. Fonction
    PURE : ne lit ni n'écrit la base et ne lève jamais.
    """
    cache = {}

    def _probe(host, port):
        key = (host, port)
        if key not in cache:
            cache[key] = _probe_target(host, port, tcp_timeout=tcp_timeout)
        return cache[key]

    results = []
    for ep in endpoints:
        row = dict(ep)
        if not ep.get("pingable"):
            row.update({"icmp_ok": None, "icmp_ms": None, "tcp_ok": None,
                        "tcp_ms": None, "reachable": None, "tested": False})
        else:
            row.update(_probe(ep.get("host"), ep.get("port")))
        results.append(row)
    return results


def api_mirth_endpoints(req):
    """Liste live des périphériques (endpoints réseau) déduits de la config Mirth.

    Lit GET /channels (via mirth_api.get_connector_endpoints) — sert à (re)scanner la
    configuration sans lancer de test de connectivité. Ne sonde rien.
    """
    return mirth_api.get_connector_endpoints(timeout=_mirth_timeout(req))


def api_devices_probe_one(req):
    """Sonde UN seul équipement réseau (ICMP + port TCP) — test incrémental « à la volée ».

    Le tableau de bord charge d'abord la liste des connecteurs (config Mirth, rapide)
    via /api/mirth/endpoints, l'affiche, puis sonde chaque cible séparément avec cette
    route : un spinner par ligne, mise à jour au fil de l'eau, sans bloquer l'affichage
    sur les hôtes lents/injoignables. Paramètres : host (obligatoire), port (optionnel).
    Ne lève jamais.
    """
    host = (req.get("host") or "").strip()
    if not host:
        return (400, {"error": "host requis"})
    raw_port = req.get("port")
    if raw_port in (None, ""):
        port = None
    else:
        try:
            port = int(raw_port)
        except (TypeError, ValueError):
            port = None
    result = _probe_target(host, port)
    result.update({"host": host, "port": port})
    return result


def api_devices_probe(req):
    """Balayage live de connectivité : endpoints + ICMP + port TCP (« Tester maintenant »).

    Récupère les endpoints (config Mirth) puis lance `probe_endpoints`. En phase 2,
    aucune persistance : on renvoie les résultats frais. Si Mirth est injoignable, on
    relaie tel quel le diagnostic de l'API.
    """
    ov = mirth_api.get_connector_endpoints(timeout=_mirth_timeout(req))
    if not ov.get("reachable"):
        return ov
    devices = probe_endpoints(ov.get("endpoints", []))
    online = sum(1 for d in devices if d.get("reachable") is True)
    offline = sum(1 for d in devices if d.get("reachable") is False)
    untested = sum(1 for d in devices if d.get("reachable") is None)
    return {"reachable": True, "base_url": ov.get("base_url"),
            "count": len(devices), "online": online, "offline": offline,
            "untested": untested, "devices": devices,
            "probed_at": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}


def api_devices(req):
    """Dernier état historisé de chaque cible (host, port) — sans appel réseau.

    Servi depuis `device_status` (alimenté par le collecteur `device-ping-collector`).
    Tableau récapitulatif des derniers tests : chaque cible porte ses résultats
    ICMP/port TCP, son état, et la liste des canaux/connecteurs qui la visent.
    """
    devices = database.get_device_status()
    online = sum(1 for d in devices if d.get("reachable") is True)
    offline = sum(1 for d in devices if d.get("reachable") is False)
    updated = max((d.get("updated_at") or "" for d in devices), default=None)
    return {"count": len(devices), "online": online, "offline": offline,
            "updated_at": updated or None, "devices": devices}


def api_devices_history(req):
    """Série temporelle agrégée pour le graphe « clients Mirth » (2 courbes).

    Chaque point : {timestamp, total, online (connexions actives), offline
    (connexions en erreur)}. Mêmes conventions que /api/history : ?date_deb=&date_fin=
    (intervalle, prioritaire) ou ?hours=24 (0 => tout l'historique).
    """
    date_deb = (req.get("date_deb") or "").strip() or None
    date_fin = (req.get("date_fin") or "").strip() or None
    if date_deb or date_fin:
        rows = database.get_device_history(date_deb=date_deb, date_fin=date_fin)
        return {"date_deb": date_deb, "date_fin": date_fin,
                "count": len(rows), "samples": rows}
    try:
        hours = float(req.get("hours", 24))
    except ValueError:
        hours = 24
    rows = database.get_device_history(hours=hours)
    return {"hours": hours, "count": len(rows), "samples": rows}


def api_devices_history_at(req):
    """Détail (par cible) d'un point précis de la courbe, par horodatage.

    Paramètre ?ts=YYYY-MM-DD HH:MM:SS (horodatage exact du relevé, tel que renvoyé
    par /api/devices/history). Renvoie {timestamp, total, online, offline, devices}
    — les résultats par cible figés à ce tick (clic sur un point du graphe).
    """
    ts = (req.get("ts") or "").strip()
    if not ts:
        return (400, {"error": "ts requis (YYYY-MM-DD HH:MM:SS)"})
    return database.get_device_history_at(ts)


# ==========================================================================
# AUTHENTIFICATION, COMPTES & CLÉS API
# ==========================================================================
def _session_cookie_header(token, clear=False):
    """En-tête Set-Cookie du jeton de session (HttpOnly, SameSite=Strict)."""
    parts = [f"{security.SESSION_COOKIE}={token if not clear else ''}",
             "HttpOnly", "SameSite=Strict", "Path=/"]
    if SERVICE_HTTPS:
        parts.append("Secure")
    if clear:
        parts.append("Max-Age=0")
    return "; ".join(parts)


def _json_response(payload, status=200, headers=None):
    body = json.dumps(payload, ensure_ascii=False, default=str).encode("utf-8")
    return webserver.Response(body, status=status, headers=headers or {},
                              content_type="application/json; charset=utf-8")


def _request_token(req):
    """Jeton porté par la requête : cookie de session, sinon en-tête Bearer."""
    tok = security._read_cookie(req.headers.get("Cookie"), security.SESSION_COOKIE)
    if tok:
        return tok
    authz = req.headers.get("Authorization", "") or ""
    if authz.startswith("Bearer "):
        return authz[7:].strip()
    return None


def _require_admin(req):
    """None si l'appelant est admin (ou auth globalement désactivée), sinon 403."""
    if not SERVICE_AUTH_ENABLED:
        return None
    u = req.user
    if not u or u.get("role") != "admin":
        return (403, {"error": "forbidden", "detail": "Réservé aux administrateurs."})
    return None


def _html_escape(s):
    return (str(s).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;"))


def _send_account_email(username, email, password, renew=False):
    """Envoie le mot de passe généré au titulaire du compte. Renvoie True si envoyé.

    Le mot de passe n'est JAMAIS affiché à l'admin : ce mail est le seul vecteur.
    """
    base = SERVICE_BASE_URL or ""
    action = "renouvelé" if renew else "créé"
    subject = "Mirth_checker — vos identifiants de connexion"
    message = (
        f"Bonjour,\n\n"
        f"Un compte d'accès à la supervision Mirth_checker a été {action} pour vous.\n\n"
        f"  Identifiant : {username}\n"
        f"  Mot de passe : {password}\n"
        + (f"  Adresse du service : {base}\n" if base else "")
        + "\nPour la supervision à distance de vos serveurs, utilisez le logiciel "
        "« Superviseur ».\n\n"
        "Ce mot de passe est personnel et ne pourra pas vous être communiqué de "
        "nouveau : conservez-le en lieu sûr (un renouvellement en générera un autre).\n"
    )
    link = (f'<p>Adresse du service : <a href="{_html_escape(base)}">{_html_escape(base)}</a></p>'
            if base else "")
    html = (
        f"<p>Bonjour,</p>"
        f"<p>Un compte d'accès à la supervision <b>Mirth_checker</b> a été {action} pour vous.</p>"
        f"<ul><li>Identifiant : <b>{_html_escape(username)}</b></li>"
        f"<li>Mot de passe : <code>{_html_escape(password)}</code></li></ul>"
        f"{link}"
        f"<p>Pour la supervision à distance de vos serveurs, utilisez le logiciel "
        f"« <b>Superviseur</b> ».</p>"
        f"<p style='color:#888'>Ce mot de passe est personnel et ne pourra pas vous être "
        f"communiqué de nouveau : conservez-le en lieu sûr (un renouvellement en générera "
        f"un autre).</p>"
    )
    try:
        return bool(quickmail.sendmail(subject, message, email, html=html))
    except Exception as e:
        log.log(f"[checker_service] Échec d'envoi du mot de passe à {email} : {e}")
        return False


def api_auth_login(req):
    """POST /api/auth/login {username, password} -> jeton de session + cookie."""
    data = req.json()
    username = (data.get("username") or "").strip()
    password = data.get("password") or ""
    if not username or not password:
        return _json_response({"ok": False, "error": "Identifiant et mot de passe requis."},
                              status=400)
    user = database.get_user(username)
    if not user or not user.get("enabled") or not auth.verify_password(password, user.get("password_hash")):
        return _json_response({"ok": False, "error": "Identifiants invalides."}, status=401)
    token, expires = auth.create_session(username, ip=req.client_ip)
    database.touch_user_login(username)
    log.log(f"[checker_service] Connexion : {username} ({req.client_ip}).")
    return _json_response(
        {"ok": True, "username": username, "role": user.get("role") or "technicien",
         "expires_at": expires, "token": token},
        headers={"Set-Cookie": _session_cookie_header(token)})


def api_auth_logout(req):
    """POST /api/auth/logout -> révoque la session courante et efface le cookie."""
    tok = _request_token(req)
    if tok:
        auth.revoke_session(tok)
    return _json_response({"ok": True},
                          headers={"Set-Cookie": _session_cookie_header("", clear=True)})


def api_auth_whoami(req):
    """GET /api/auth/whoami -> identité de la session courante (route protégée)."""
    u = req.user or {}
    return {"authenticated": bool(u), "username": u.get("username"),
            "role": u.get("role"), "expires_at": u.get("expires_at"),
            "bootstrap": bool(u.get("bootstrap"))}


def api_auth_token(req):
    """POST /api/auth/token -> frappe un jeton Bearer lié au compte courant (page API).

    Comme la base ne stocke que le hash du jeton de navigation, on ne peut pas le
    « relire » : on émet un nouveau jeton de session (même TTL 24 h) pour l'usage API.
    """
    u = req.user or {}
    username = u.get("username")
    if not username or not database.get_user(username):
        return (400, {"ok": False,
                      "error": "Jeton disponible uniquement pour un compte réel connecté."})
    token, expires = auth.create_session(username, ip=req.client_ip)
    return {"ok": True, "token": token, "expires_at": expires, "username": username}


# --- Comptes (admin) -------------------------------------------------------
def api_users_list(req):
    """GET /api/users -> liste des comptes (admin). Jamais de hash/mot de passe."""
    guard = _require_admin(req)
    if guard:
        return guard
    return {"users": database.list_users()}


def api_users_create(req):
    """POST /api/users {username, email, role} -> crée + envoie le mdp par e-mail."""
    guard = _require_admin(req)
    if guard:
        return guard
    data = req.json()
    username = (data.get("username") or "").strip()
    email = (data.get("email") or "").strip()
    role = (data.get("role") or "technicien").strip()
    if role not in ("admin", "technicien"):
        role = "technicien"
    if not username or not email:
        return (400, {"ok": False, "error": "Identifiant et e-mail requis."})
    password = auth.generate_password()
    if not database.create_user(username, email, auth.hash_password(password), role=role):
        return (409, {"ok": False, "error": "Cet identifiant existe déjà."})
    mailed = _send_account_email(username, email, password, renew=False)
    log.log(f"[checker_service] Compte créé : {username} ({role}) — e-mail {'envoyé' if mailed else 'NON envoyé'} à {email}.")
    return {"ok": True, "mailed": mailed, "username": username}


def api_users_renew(req):
    """POST /api/users/{name}/renew -> régénère le mdp + e-mail + coupe les sessions."""
    guard = _require_admin(req)
    if guard:
        return guard
    username = req.params.get("name")
    user = database.get_user(username)
    if not user:
        return (404, {"ok": False, "error": "Compte introuvable."})
    password = auth.generate_password()
    database.set_password(username, auth.hash_password(password))
    database.delete_user_sessions(username)   # force la reconnexion
    mailed = _send_account_email(username, user.get("email"), password, renew=True)
    log.log(f"[checker_service] Mot de passe renouvelé : {username} — e-mail {'envoyé' if mailed else 'NON envoyé'}.")
    return {"ok": True, "mailed": mailed, "username": username}


def api_users_enable(req):
    guard = _require_admin(req)
    if guard:
        return guard
    username = req.params.get("name")
    if not database.set_user_enabled(username, True):
        return (404, {"ok": False, "error": "Compte introuvable."})
    return {"ok": True, "username": username, "enabled": True}


def api_users_disable(req):
    guard = _require_admin(req)
    if guard:
        return guard
    username = req.params.get("name")
    if not database.set_user_enabled(username, False):
        return (404, {"ok": False, "error": "Compte introuvable."})
    return {"ok": True, "username": username, "enabled": False}


def api_users_delete(req):
    guard = _require_admin(req)
    if guard:
        return guard
    username = req.params.get("name")
    if not database.delete_user(username):
        return (404, {"ok": False, "error": "Compte introuvable."})
    log.log(f"[checker_service] Compte supprimé : {username}.")
    return {"ok": True, "username": username}


# --- Clés API (admin) : clients machine (superviseur) ----------------------
def api_keys_list(req):
    guard = _require_admin(req)
    if guard:
        return guard
    return {"keys": database.list_api_keys()}


def api_keys_create(req):
    """POST /api/keys {label, username?} -> crée une clé (renvoyée UNE seule fois)."""
    guard = _require_admin(req)
    if guard:
        return guard
    data = req.json()
    label = (data.get("label") or "").strip() or None
    username = (data.get("username") or "").strip() or None
    raw = auth.new_api_key()
    database.insert_api_key(auth.token_fingerprint(raw), label=label, username=username)
    log.log(f"[checker_service] Clé API créée : {label or '(sans label)'}.")
    return {"ok": True, "key": raw, "key_id": auth.token_fingerprint(raw), "label": label}


def api_keys_delete(req):
    guard = _require_admin(req)
    if guard:
        return guard
    key_id = req.params.get("id")
    if not database.delete_api_key(key_id):
        return (404, {"ok": False, "error": "Clé introuvable."})
    return {"ok": True}


def build_router(tasks, started_at):
    router = webserver.Router(static_dir=WEB_DIR, index_route="/",
                              json_transform=_round_floats)

    # Authentification, comptes & clés API (sécurité)
    router.post("/api/auth/login", api_auth_login)
    router.post("/api/auth/logout", api_auth_logout)
    router.get("/api/auth/whoami", api_auth_whoami)
    router.post("/api/auth/token", api_auth_token)
    router.get("/api/users", api_users_list)
    router.post("/api/users", api_users_create)
    router.post("/api/users/{name}/renew", api_users_renew)
    router.post("/api/users/{name}/enable", api_users_enable)
    router.post("/api/users/{name}/disable", api_users_disable)
    router.post("/api/users/{name}/delete", api_users_delete)
    router.get("/api/keys", api_keys_list)
    router.post("/api/keys", api_keys_create)
    router.post("/api/keys/{id}/delete", api_keys_delete)

    # API système
    router.get("/api/system", api_system)
    router.get("/api/processes", api_processes)
    router.get("/api/ping", api_ping)
    router.get("/api/sockets", api_sockets)

    # API infos à la demande (multi-format json/text/html)
    router.get("/api/getsysteminfo", api_getsysteminfo)
    router.get("/api/getprocessinfo", api_getprocessinfo)

    # API historique
    router.get("/api/history", api_history)
    router.get("/api/history/latest", api_history_latest)

    # API évènements / alertes (second jeu de données superposable aux graphes)
    router.get("/api/events", api_events)
    router.get("/api/setevent", api_setevent)   # GET : pratique pour tester
    router.post("/api/setevent", api_setevent)

    # API supervision & maintenance de la base
    router.get("/api/db/info", api_db_info)
    router.get("/api/db/integrity", api_db_integrity)
    router.get("/api/db/export", api_db_export)
    router.get("/api/db/export/csv", api_db_export_csv)
    router.post("/api/db/vacuum", api_db_vacuum)
    router.post("/api/db/reset", api_db_reset)
    router.post("/api/db/purge", api_db_purge)
    router.post("/api/db/import", api_db_import)

    # API identité machine
    router.get("/api/hostinfo", api_hostinfo)

    # API logs Mirth
    router.get("/api/mirth", api_mirth)

    # API supervision Mirth (API REST + processus)
    router.get("/api/mirth/api", api_mirth_api)
    router.get("/api/mirth/kpi-baseline", api_mirth_kpi_baseline)
    router.post("/api/mirth/kpi-baseline", api_mirth_kpi_baseline_save)
    router.get("/api/mirth/history", api_mirth_history)
    router.get("/api/mirth/history/latest", api_mirth_history_latest)
    router.get("/api/mirth/throughput", api_mirth_throughput)
    router.get("/api/mirth/report", api_mirth_report)
    router.get("/api/mirth/channels", api_mirth_channels)
    router.get("/api/mirth/connectors", api_mirth_connectors)
    router.get("/api/mirth/stats", api_mirth_stats)
    router.get("/api/mirth/server", api_mirth_server)
    router.get("/api/mirth/errors", api_mirth_errors)
    router.get("/api/mirth/messages", api_mirth_messages)
    router.get("/api/mirth/process", api_mirth_process)
    router.get("/api/mirth/endpoints", api_mirth_endpoints)
    router.get("/api/devices/probe", api_devices_probe)
    router.get("/api/devices/probe-one", api_devices_probe_one)
    # Supervision périphériques historisée (collecteur device-ping)
    router.get("/api/devices", api_devices)
    router.get("/api/devices/history", api_devices_history)
    router.get("/api/devices/history/at", api_devices_history_at)
    router.get("/api/getmirthinfo", api_getmirthinfo)

    # API email
    router.post("/api/mail", api_mail)

    # API configuration des alertes (page alerte.html)
    router.get("/api/alerts/config", api_alerts_config)
    router.post("/api/alerts/save", api_alerts_save)
    router.post("/api/alerts/test", api_alerts_test)
    router.post("/api/alerts/test-alarm", api_alerts_test_alarm)

    # API statut
    router.get("/api/status", make_api_status(tasks, started_at))

    return router


# ==========================================================================
# GESTION DE COMPTES EN LIGNE DE COMMANDE (bootstrap sans UI)
# ==========================================================================
def _run_account_cli(args):
    """Exécute --add-admin / --list-users / --del-user puis rend la main.

    Sert à amorcer le premier compte administrateur (y compris sur l'exe) sans
    passer par l'interface web.
    """
    database.init_db()

    if args.list_users:
        users = database.list_users()
        if not users:
            print("Aucun compte enregistré.")
        else:
            rows = [[u["username"], u["email"], u["role"],
                     "oui" if u["enabled"] else "non",
                     u.get("last_login_at") or "-"] for u in users]
            print(tabulate(rows, headers=["Identifiant", "E-mail", "Rôle",
                                          "Activé", "Dernière connexion"]))
        return

    if args.del_user:
        ok = database.delete_user(args.del_user)
        print(f"Compte '{args.del_user}' {'supprimé' if ok else 'introuvable'}.")
        return

    if args.add_admin:
        username = args.add_admin.strip()
        email = (args.email or "").strip()
        password = auth.generate_password()
        if not database.create_user(username, email or "-",
                                    auth.hash_password(password), role="admin"):
            print(f"Erreur : le compte '{username}' existe déjà.")
            return
        sent = _send_account_email(username, email, password, renew=False) if email else False
        if sent:
            print(f"Compte administrateur '{username}' créé. "
                  f"Mot de passe envoyé à {email}.")
        else:
            print(f"Compte administrateur '{username}' créé.")
            print(f"  Mot de passe (à transmettre de façon sûre) : {password}")
            if email:
                print("  (Envoi e-mail impossible — SMTP non configuré ?)")


# ==========================================================================
# MAIN
# ==========================================================================
def main():
    global DEFAULT_LOGFILE
    parser = argparse.ArgumentParser(
        description="Service web Mirth_checker (API système, historique SQLite, logs Mirth)."
    )
    parser.add_argument("--host", default="0.0.0.0", help="Adresse d'écoute (def: 0.0.0.0)")
    parser.add_argument("--port", type=int, default=8800, help="Port d'écoute (def: 8800)")
    parser.add_argument("--interval", type=int, default=60,
                        help="Intervalle des relevés en secondes (def: 60)")
    parser.add_argument("--stagger", type=float, default=5.0,
                        help="Décalage initial entre les tâches du planificateur "
                             "en secondes (def: 5) — évite un pic de charge au tick")
    parser.add_argument("--retention-days", type=int, default=15,
                        help="Rétention max de l'historique en jours : les relevés "
                             "plus anciens sont purgés automatiquement (purge quotidienne "
                             "à 03:00). 0 = illimité. (def: 15)")
    parser.add_argument("--no_output", "--no-output", action="store_true",
                        dest="no_output",
                        help="N'affiche RIEN dans la console (ni tableau de bord rich "
                             "ni journal). Pour un lancement en arrière-plan, fenêtre "
                             "non accessible. Le service et son API restent actifs.")
    parser.add_argument("--logfile", default=DEFAULT_LOGFILE,
                        help="Fichier de log Mirth par défaut pour /api/mirth")
    parser.add_argument("--mirth-url", default=None,
                        help="URL de l'API REST Mirth (sinon env / .mirth_config.py / défaut)")
    parser.add_argument("--mirth-user", default=None,
                        help="Identifiant de connexion à l'API Mirth")
    parser.add_argument("--mirth-password", default=None,
                        help="Mot de passe de connexion à l'API Mirth")

    # --- Sécurité (HTTPS / filtre IP / authentification) -------------------
    sec = parser.add_argument_group("Sécurité")
    sec.add_argument("--https", choices=["auto", "on", "off"], default=None,
                     help="HTTPS : 'auto' (activé hors localhost), 'on', 'off'. "
                          "Défaut : .mirth_config.py/HTTPS_MODE (auto).")
    sec.add_argument("--cert", default=None,
                     help="Chemin du certificat TLS (PEM). Vide => auto-signé généré.")
    sec.add_argument("--key", default=None, help="Chemin de la clé privée TLS (PEM).")
    sec.add_argument("--allow-ips", dest="allow_ips", default=None,
                     help="Liste blanche d'IP/CIDR séparées par des virgules "
                          "(vide => tout autorisé). Ex: 127.0.0.1,192.168.0.0/16")
    sec.add_argument("--auth", choices=["on", "off"], default=None,
                     help="Authentification par comptes/session. "
                          "Défaut : .mirth_config.py/AUTH_ENABLED (on).")

    # --- Gestion de comptes en ligne de commande (bootstrap, sans UI) ------
    acc = parser.add_argument_group("Comptes (CLI)")
    acc.add_argument("--add-admin", dest="add_admin", metavar="IDENTIFIANT", default=None,
                     help="Crée un compte administrateur (mot de passe généré, envoyé "
                          "par e-mail — requiert --email — ou affiché ici si SMTP absent).")
    acc.add_argument("--email", default=None, help="E-mail du compte (avec --add-admin).")
    acc.add_argument("--list-users", dest="list_users", action="store_true",
                     help="Affiche la liste des comptes et quitte.")
    acc.add_argument("--del-user", dest="del_user", metavar="IDENTIFIANT", default=None,
                     help="Supprime un compte et quitte.")

    args = parser.parse_args()

    # Opérations de gestion de comptes en ligne de commande : elles initialisent la
    # base, exécutent l'action puis quittent (pas de démarrage du serveur).
    if args.add_admin or args.list_users or args.del_user:
        _run_account_cli(args)
        return

    # Mode silencieux (--no_output) : coupe toute sortie console (tableau de bord
    # rich ET logs). Posé AVANT le premier log / le démarrage du tableau de bord
    # pour qu'aucun caractère ne soit écrit (utile en arrière-plan, sans fenêtre).
    if args.no_output:
        log.set_quiet(True)

    DEFAULT_LOGFILE = args.logfile

    global RETENTION_DAYS
    RETENTION_DAYS = args.retention_days

    # --- Sécurité : configuration + TLS + politique d'accès --------------------
    # Chargée tôt (avant le tableau de bord) pour connaître le schéma http/https et
    # décider de la tâche de purge des sessions. Les messages sont différés dans
    # `_sec_log` puis journalisés une fois le dashboard démarré.
    global SERVICE_HTTPS, SERVICE_AUTH_ENABLED
    sec_cfg = security.load_security_config(args)
    auth.set_session_ttl_hours(sec_cfg["SESSION_TTL_H"])
    SERVICE_AUTH_ENABLED = sec_cfg["AUTH_ENABLED"]
    _sec_log = []

    tls_context = None
    if security.https_enabled_for_host(sec_cfg["HTTPS_MODE"], args.host):
        cert_dir = os.path.dirname(database.DEFAULT_DB_PATH)
        cert = sec_cfg["HTTPS_CERT"] or os.path.join(cert_dir, "checker_cert.pem")
        key = sec_cfg["HTTPS_KEY"] or os.path.join(cert_dir, "checker_key.pem")
        provided = bool(sec_cfg["HTTPS_CERT"] and sec_cfg["HTTPS_KEY"])
        try:
            if not provided:
                tls.ensure_self_signed_cert(
                    cert, key, hostname=system_state.get_hostname() or None)
            tls_context = tls.build_ssl_context(cert, key)
            _sec_log.append(f"[checker_service] HTTPS activé (certificat : {cert}).")
        except Exception as e:
            if sec_cfg["HTTPS_MODE"] == "on":
                # HTTPS explicitement exigé : échec bloquant, pas de repli silencieux.
                print(f"[checker_service] HTTPS demandé mais impossible à activer : {e}",
                      file=sys.stderr)
                sys.exit(1)
            _sec_log.append(
                f"[checker_service] HTTPS 'auto' indisponible ({e}) — repli en HTTP.")
            tls_context = None
    SERVICE_HTTPS = tls_context is not None

    security_policy = security.SecurityPolicy(
        networks=security.parse_networks(sec_cfg["ALLOWED_IPS"]),
        tls_context=tls_context,
        auth_enabled=sec_cfg["AUTH_ENABLED"],
        db_path=database.DEFAULT_DB_PATH,
    )
    scheme = "https" if SERVICE_HTTPS else "http"

    # URL publique du service pour les liens profonds des e-mails (alertes,
    # identifiants de compte) : CHECKER_BASE_URL si fournie, sinon l'IP routable
    # de la machine + port d'écoute (on privilégie l'IP au nom d'hôte car il n'y a
    # pas forcément d'entrée DNS locale ; derrière un NAT, poser CHECKER_BASE_URL).
    global SERVICE_BASE_URL
    env_url = (os.environ.get("CHECKER_BASE_URL") or "").strip().rstrip("/")
    if env_url:
        SERVICE_BASE_URL = env_url
    else:
        SERVICE_BASE_URL = f"{scheme}://{security.primary_ip(args.host)}:{args.port}"

    # Identifiants Mirth fournis en ligne de commande : injectés dans
    # l'environnement, source la plus prioritaire pour mirth_api.get_config().
    if args.mirth_url:
        os.environ["MIRTH_BASE_URL"] = args.mirth_url
    if args.mirth_user:
        os.environ["MIRTH_USER"] = args.mirth_user
    if args.mirth_password:
        os.environ["MIRTH_PASSWORD"] = args.mirth_password

    # 1. Tâches programmées (relevés toutes les `interval` secondes). Créées AVANT
    #    le tableau de bord console pour qu'il affiche leur état dès la 1re frame ;
    #    elles ne démarrent qu'à start_staggered (plus bas), de façon échelonnée
    #    (+stagger s entre chaque) pour ne pas sonder le système simultanément.
    tasks = [
        RecurringTask(args.interval, scheduled_check, name="metrics-collector"),
        RecurringTask(args.interval, scheduled_mirth_check, name="mirth-collector"),
        RecurringTask(args.interval, scheduled_mirth_overview, name="mirth-overview-collector"),
        RecurringTask(args.interval, scheduled_device_check, name=DEVICE_TASK_NAME),
    ]
    # Tâche de rétention (purge de l'historique trop ancien) — exécutée une fois
    # par jour à 03:00 (heure creuse), indépendamment de --interval. Ajoutée
    # seulement si la rétention est active ; run_immediately applique en plus la
    # rétention dès le démarrage (sans attendre 03:00).
    if RETENTION_DAYS > 0:
        tasks.append(RecurringTask(RETENTION_PURGE_INTERVAL, scheduled_purge,
                                   name=RETENTION_TASK_NAME,
                                   daily_at=RETENTION_PURGE_TIME))
    # Purge quotidienne des sessions web expirées (seulement si l'auth est active).
    if SERVICE_AUTH_ENABLED:
        tasks.append(RecurringTask(RETENTION_PURGE_INTERVAL, scheduled_session_cleanup,
                                   name=SESSION_CLEANUP_TASK_NAME,
                                   daily_at=SESSION_CLEANUP_TIME))

    # 2. Tableau de bord console (rich) : tâches du planificateur en haut, journal
    #    défilant en bas. Démarré tôt pour capturer les logs d'initialisation. Si
    #    rich est absent ou hors terminal, retombe sur l'affichage texte (ligne
    #    d'état réécrite en place + logs persistants).
    log.start_dashboard(tasks, summary_provider=task_summary)
    log.log("[checker_service] Démarrage du service.")

    # 3. Initialisation de la base
    database.init_db()
    log.log(f"[checker_service] Base SQLite : {database.DEFAULT_DB_PATH}")

    # 3ter. Journalise l'état de sécurité (différé depuis le chargement de la config).
    for line in _sec_log:
        log.log(line)
    log.log(
        f"[checker_service] Sécurité : HTTPS={'oui' if SERVICE_HTTPS else 'non'}, "
        f"authentification={'oui' if SERVICE_AUTH_ENABLED else 'non'}, "
        f"filtre IP={len(security_policy.networks) or 'désactivé'}.")
    if SERVICE_AUTH_ENABLED and database.count_users() == 0:
        log.log("[checker_service] Aucun compte : accès toléré depuis localhost pour "
                "créer le 1er administrateur (UI ou --add-admin).")

    # 3bis. Marqueurs d'interruption (arrêt logiciel / boot système) avant reprise
    mark_startup_events()

    # 4. Démarrage échelonné des tâches. Le hook on_complete (ligne d'état console)
    #    ne sert qu'au repli texte : sous le tableau de bord rich, la table des
    #    tâches est redessinée de façon événementielle par le planificateur (cf.
    #    log.dashboard_refresh) et log.status() est neutralisé.
    status_line = make_scheduler_status_line(tasks)
    for t in tasks:
        t.on_complete = status_line
    start_staggered(tasks, step=args.stagger)
    log.log(f"[checker_service] {len(tasks)} tâches programmées démarrées "
          f"(relevés toutes les {args.interval}s, décalage {args.stagger}s entre chacune).")
    if RETENTION_DAYS > 0:
        log.log(f"[checker_service] Rétention de l'historique : {RETENTION_DAYS} jours "
                f"(purge automatique quotidienne à "
                f"{RETENTION_PURGE_TIME.strftime('%H:%M')}).")
    else:
        log.log("[checker_service] Rétention de l'historique : illimitée (aucune purge auto).")

    # 3. Serveur web
    started_at = system_state.get_now_datetime()
    router = build_router(tasks, started_at)
    try:
        httpd = webserver.serve(router, host=args.host, port=args.port,
                                security=security_policy)
    except OSError as e:
        # Port déjà pris : très probablement une autre instance de checker_service
        # tourne déjà sur ce port. On refuse de démarrer plutôt que d'alimenter la
        # même base en double (relevés trop fréquents/incohérents).
        for task in tasks:
            task.stop()
        log.stop_dashboard()   # restaure l'écran pour que l'erreur reste visible
        log.log(f"[checker_service] Impossible d'écouter sur {args.host}:{args.port} "
              f"— le port est déjà utilisé (une autre instance tourne ?). [{e}]")
        sys.exit(1)

    display_host = "localhost" if args.host in ("0.0.0.0", "") else args.host
    url = f"{scheme}://{display_host}:{args.port}/"
    log.log(f"[checker_service] Serveur web : {url}")
    log.log(f"[checker_service] Page statistiques : {url}statistiques.html")
    log.log("[checker_service] Ctrl+C pour arrêter.")
    # Pas d'ouverture automatique du navigateur : l'URL ci-dessus est affichée
    # dans le journal, à ouvrir manuellement.

    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        log.log("[checker_service] Arrêt en cours...")
    finally:
        # Restaure l'écran normal du terminal AVANT les messages finaux, pour
        # qu'ils s'affichent (sinon ils disparaîtraient avec l'écran du dashboard).
        log.stop_dashboard()
        log.clear()   # repli texte : retire la ligne d'état flottante éventuelle
        for task in tasks:
            task.stop()
        httpd.shutdown()
        httpd.server_close()
        mirth_api.close_session()   # logout de la session Mirth durable partagée
        log.log("[checker_service] Arrêté.")


if __name__ == "__main__":
    main()
