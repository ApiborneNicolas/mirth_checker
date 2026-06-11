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
                              [--logfile chemin] [--no-browser]
"""

import os
import sys
import math
import argparse
import datetime

from tabulate import tabulate

# --- Import des librairies internes (dossier lib/) -------------------------
from lib import database, webserver, mirth_api
from lib.scheduler import RecurringTask, start_staggered

# --- Import des scripts existants en tant que librairies -------------------
import system_state
import mirth_logs_parser
import quickmail

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
WEB_DIR = os.path.join(BASE_DIR, "web")
DEFAULT_LOGFILE = os.path.join(BASE_DIR, "Ressources", "mirth-exemple.log")

# Lecteur système surveillé pour le stockage (C:\ sous Windows, / sinon)
SYSTEM_DRIVE = os.environ.get("SystemDrive", "C:") + os.sep if os.name == "nt" else "/"


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

# ==========================================================================
# ÉVÈNEMENTS / ALERTES (second jeu de données temporel)
# Stockés dans la table `events` (cf. lib/database) et superposés à TOUS les
# graphiques sous forme de barres verticales colorées + texte. Couleur par défaut
# dérivée de la catégorie ; un appelant (API /api/setevent) peut imposer la sienne.
# ==========================================================================
EVENT_COLORS = {
    "boot": "#ef4444",      # rouge — démarrage du système
    "service": "#22c55e",   # vert — start/stop du checker
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


# État précédent du processus Mirth (présent/absent) entre deux relevés, afin de
# détecter les transitions start/stop et d'en émettre une alerte. None = inconnu.
_mirth_prev_found = None


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
    database.insert_metric(collect_sample())


def scheduled_mirth_check():
    """Tâche programmée 'mirth' : relève et enregistre l'état du processus Mirth.

    Détecte aussi les transitions de présence du processus (démarrage/arrêt) entre
    deux relevés et en émet une alerte (barre verticale superposée aux graphes).
    """
    global _mirth_prev_found
    sample = collect_mirth_sample()
    database.insert_metric(sample)

    # Présence déduite du relevé : cpu nul => processus introuvable sur ce tick.
    found = sample["cpu_percent"] is not None
    if _mirth_prev_found is not None and found != _mirth_prev_found:
        if found:
            database.insert_event(category="mirth", label="Démarrage Mirth",
                                  color=_event_color("mirth"), source="scheduler")
            print("[checker_service] Alerte : démarrage du processus Mirth détecté.")
        else:
            database.insert_event(category="mirth", label="Arrêt Mirth",
                                  color=_event_color("mirth"), source="scheduler")
            print("[checker_service] Alerte : arrêt du processus Mirth détecté.")
    _mirth_prev_found = found


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
            print(f"[checker_service] Marqueur d'arrêt inséré à {gap_str}.")
            database.insert_event(timestamp=gap_str, category="service",
                                  label="Arrêt du checker", color=_event_color("service"),
                                  source="scheduler", dedup=True)

    # 2. Alerte de démarrage du checker (toujours, à l'instant présent).
    database.insert_event(timestamp=now.strftime(fmt), category="service",
                          label="Démarrage du checker", color=_event_color("service"),
                          source="scheduler")

    # 3. Démarrage système, si un boot a eu lieu depuis le dernier relevé : marqueur
    #    nul 'boot' (brise la courbe système) + alerte 'boot' (barre sur tous les graphes).
    try:
        boot_dt = datetime.datetime.strptime(system_state.get_boot_time(), fmt)
    except ValueError:
        boot_dt = None
    if boot_dt and (last_dt is None or boot_dt > last_dt):
        boot_str = boot_dt.strftime(fmt)
        if database.insert_event_marker(boot_str, "boot"):
            print(f"[checker_service] Marqueur de démarrage système inséré à {boot_str}.")
        database.insert_event(timestamp=boot_str, category="boot",
                              label="Démarrage système", color=_event_color("boot"),
                              source="scheduler", dedup=True)


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
    if tag.lower() == "all":
        tag = ""   # get_history interprète '' comme « toutes sources »

    if date_deb or date_fin:
        rows = database.get_history(date_deb=date_deb, date_fin=date_fin, tag=tag)
        return {"tag": tag, "date_deb": date_deb, "date_fin": date_fin,
                "count": len(rows), "samples": rows}

    try:
        hours = float(req.get("hours", 24))
    except ValueError:
        hours = 24
    rows = database.get_history(hours=hours, tag=tag)
    return {"tag": tag, "hours": hours, "count": len(rows), "samples": rows}


def api_history_latest(req):
    """Dernier relevé enregistré pour la source ?tag=system (défaut) | mirth | ..."""
    tag = (req.get("tag") or "system").strip() or "system"
    if tag.lower() == "all":
        tag = ""
    return {"tag": tag, "latest": database.get_latest(tag=tag)}


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
    """État détaillé de la base : taille, fragmentation, tables, bornes temporelles."""
    return database.get_db_stats()


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
    """Supprime les relevés plus vieux que ?days=N jours (défaut 30)."""
    try:
        days = int(req.get("days", 30))
    except ValueError:
        days = 30
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
def api_mirth_api(req):
    """Vue d'ensemble du serveur Mirth via son API REST.

    Renvoie version, statistiques système (JVM) et statuts des canaux. En cas de
    serveur injoignable : {"reachable": false, "error": ...} (HTTP 200).
    """
    try:
        timeout = float(req.get("timeout", 8))
    except ValueError:
        timeout = 8
    return mirth_api.get_overview(timeout=timeout)


def api_mirth_process(req):
    """Instantané live du processus Mirth (CPU / mémoire / sockets)."""
    p = probe_mirth_process()
    p["latest"] = database.get_latest(tag=TAG_MIRTH)
    return p


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
def build_router(tasks, started_at):
    router = webserver.Router(static_dir=WEB_DIR, index_route="/",
                              json_transform=_round_floats)

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
    router.get("/api/mirth/process", api_mirth_process)

    # API email
    router.post("/api/mail", api_mail)

    # API statut
    router.get("/api/status", make_api_status(tasks, started_at))

    return router


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
    parser.add_argument("--logfile", default=DEFAULT_LOGFILE,
                        help="Fichier de log Mirth par défaut pour /api/mirth")
    parser.add_argument("--no-browser", action="store_true",
                        help="Ne pas ouvrir le navigateur au démarrage")
    args = parser.parse_args()

    DEFAULT_LOGFILE = args.logfile

    # 1. Initialisation de la base
    database.init_db()
    print(f"[checker_service] Base SQLite : {database.DEFAULT_DB_PATH}")

    # 1bis. Marqueurs d'interruption (arrêt logiciel / boot système) avant reprise
    mark_startup_events()

    # 2. Tâches programmées (relevés toutes les `interval` secondes), démarrées de
    #    façon échelonnée (+stagger s entre chaque) pour ne pas sonder le système
    #    simultanément et lisser la charge à chaque tick du planificateur.
    tasks = [
        RecurringTask(args.interval, scheduled_check, name="metrics-collector"),
        RecurringTask(args.interval, scheduled_mirth_check, name="mirth-collector"),
    ]
    start_staggered(tasks, step=args.stagger)
    print(f"[checker_service] {len(tasks)} tâches programmées démarrées "
          f"(toutes les {args.interval}s, décalage {args.stagger}s entre chacune).")

    # 3. Serveur web
    started_at = system_state.get_now_datetime()
    router = build_router(tasks, started_at)
    try:
        httpd = webserver.serve(router, host=args.host, port=args.port)
    except OSError as e:
        # Port déjà pris : très probablement une autre instance de checker_service
        # tourne déjà sur ce port. On refuse de démarrer plutôt que d'alimenter la
        # même base en double (relevés trop fréquents/incohérents).
        for task in tasks:
            task.stop()
        print(f"[checker_service] Impossible d'écouter sur {args.host}:{args.port} "
              f"— le port est déjà utilisé (une autre instance tourne ?). [{e}]")
        sys.exit(1)

    display_host = "localhost" if args.host in ("0.0.0.0", "") else args.host
    url = f"http://{display_host}:{args.port}/"
    print(f"[checker_service] Serveur web : {url}")
    print(f"[checker_service] Page statistiques : {url}statistiques.html")
    print("[checker_service] Ctrl+C pour arrêter.")

    if not args.no_browser:
        try:
            import webbrowser
            webbrowser.open(url)
        except Exception:
            pass

    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\n[checker_service] Arrêt en cours...")
    finally:
        for task in tasks:
            task.stop()
        httpd.shutdown()
        httpd.server_close()
        print("[checker_service] Arrêté.")


if __name__ == "__main__":
    main()
