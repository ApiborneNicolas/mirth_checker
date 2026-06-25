#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
superviseur.py
==============
Méta-superviseur des instances `checker_service` déployées.

Là où `checker_service` supervise UNE machine (et expose son API + ses
dashboards), `superviseur` agrège l'état de PLUSIEURS `checker_service` :

1. il sert un mini-site web (dossier `web_superviseur/`) sur le port 8799 ;
2. il maintient la liste des sites supervisés dans une base SQLite
   (`superviseur.db`, à côté du script / de l'exécutable) — gérée depuis la page
   d'administration (ajout / désactivation / suppression) ;
3. une tâche de fond interroge chaque site actif toutes les `interval` secondes
   (défaut 60) via ses API (`/api/hostinfo`, `/api/mirth/api`,
   `/api/history/latest`...) et enregistre un instantané d'état par site ;
4. la page « Tableau de bord » liste les sites avec un résumé lisible de leur état
   (Mirth en priorité). Chaque ligne se déplie À LA DEMANDE (une à la fois, sans
   préchargement) pour afficher des graphiques tirés en direct des API du site
   distant — relayées par le proxy du superviseur (évite les soucis CORS et
   fonctionne même si le navigateur n'a pas d'accès direct au site).

Réutilise les librairies internes de `checker_service` :
- lib.webserver  : mini-serveur HTTP avec routage + fichiers statiques ;
- lib.scheduler  : tâche récurrente sur thread daemon ;
- lib.log + lib.dashboard : console coordonnée (tableau de bord rich, repli texte).

Lancement :
    python superviseur.py [--host 0.0.0.0] [--port 8799] [--interval 60]
                          [--timeout 5] [--no_output]

`--no_output` coupe tout l'affichage console (lancement en arrière-plan) ; le
service et son API restent actifs.
"""

import os
import sys
import json
import time
import socket
import argparse
import datetime
import threading
import urllib.error
import urllib.request
import concurrent.futures

# --- Librairies internes (dossier lib/) ------------------------------------
from lib import superviseur_db as db
from lib import webserver, log
from lib.scheduler import RecurringTask

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
# Pages statiques : embarquées dans sys._MEIPASS en build gelé (cf. --add-data
# "web_superviseur;web_superviseur"), sinon le dossier à côté du script.
WEB_DIR = os.path.join(getattr(sys, "_MEIPASS", BASE_DIR), "web_superviseur")

DEFAULT_PORT = 8799
POLL_TASK_NAME = "sites-poller"

# Délai (s) des requêtes HTTP vers les sites distants (relève de fond). Réglé au
# démarrage par main() depuis --timeout.
_POLL_TIMEOUT = 5.0
# Délai (s) des requêtes proxy (graphiques à la demande) : plus généreux car la
# fenêtre demandée peut renvoyer beaucoup de points.
_PROXY_TIMEOUT = 15.0
# Concurrence max du collecteur (sondes de plusieurs sites en parallèle).
_POLL_WORKERS = 8

_HDRS = {"User-Agent": "Superviseur/1.0", "Accept": "application/json"}

# Résumé du dernier balayage (alimente la colonne « Valeur » du tableau de bord
# console et le repli texte). Mis à jour à chaque tick par le collecteur.
_poll_summary = {"sites": 0, "ok": 0, "ko": 0}
_poll_summary_lock = threading.Lock()


# ==========================================================================
# OUTILS
# ==========================================================================
def _now():
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _base_url(site):
    """URL de base d'un site supervisé (http://host:port)."""
    return f"http://{site['host']}:{site['port']}"


def _err_str(exc):
    """Message d'erreur concis et lisible pour une exception réseau."""
    if isinstance(exc, urllib.error.HTTPError):
        return f"HTTP {exc.code}"
    if isinstance(exc, urllib.error.URLError):
        reason = exc.reason
        if isinstance(reason, (socket.timeout, TimeoutError)):
            return "Délai dépassé"
        return str(reason)
    if isinstance(exc, (socket.timeout, TimeoutError)):
        return "Délai dépassé"
    return str(exc) or exc.__class__.__name__


def _http_get_raw(base, path, timeout):
    """GET brut vers `base + path`. Renvoie (status, content_type, body_bytes).

    Une réponse non-2xx AVEC corps (ex. 404 JSON du site) est renvoyée telle
    quelle (pour le proxy). Une erreur de connexion/délai lève l'exception.
    """
    url = base + path
    req = urllib.request.Request(url, headers=_HDRS)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            body = r.read()
            ctype = r.headers.get("Content-Type", "application/json; charset=utf-8")
            return r.getcode(), ctype, body
    except urllib.error.HTTPError as e:
        body = e.read() if hasattr(e, "read") else b""
        ctype = (e.headers.get("Content-Type") if e.headers else None) \
            or "application/json; charset=utf-8"
        return e.code, ctype, body


def _fetch_json(base, path, timeout):
    """GET JSON vers `base + path`. Lève sur erreur réseau ou statut >= 400."""
    code, _ctype, body = _http_get_raw(base, path, timeout)
    if code >= 400:
        raise RuntimeError(f"HTTP {code}")
    return json.loads(body.decode("utf-8"))


# ==========================================================================
# RELÈVE D'UN SITE
# ==========================================================================
def poll_site(site, timeout=None):
    """Interroge les API d'un site et renvoie son instantané d'état.

    Stratégie : `/api/hostinfo` sert de test de joignabilité (et donne le nom
    d'hôte / l'OS). Si le site répond, on agrège — au mieux, chaque appel étant
    isolé — la vue Mirth (`/api/mirth/api`), le dernier relevé système
    (`/api/history/latest`) et le dernier relevé du processus Mirth
    (`/api/mirth/history/latest`). Ne lève jamais : un échec se traduit par
    `ok=False` + `error`.
    """
    timeout = _POLL_TIMEOUT if timeout is None else timeout
    base = _base_url(site)
    t0 = time.monotonic()
    st = {"polled_at": _now(), "ok": False, "error": None,
          "hostname": None, "os": None, "cpu_percent": None, "mem_percent": None,
          "proc_cpu_percent": None, "proc_mem_percent": None,
          "mirth_reachable": None, "mirth_version": None,
          "channel_count": None, "channels_started": None,
          "error_count": None, "channels_in_error": None, "channels": []}

    # 1. Joignabilité + identité (obligatoire ; un échec ici => site KO).
    try:
        info = _fetch_json(base, "/api/hostinfo", timeout)
        st["ok"] = True
        st["hostname"] = info.get("hostname")
        st["os"] = info.get("os")
    except Exception as e:
        st["error"] = _err_str(e)
        st["latency_ms"] = round((time.monotonic() - t0) * 1000, 1)
        return st

    # 2. Vue d'ensemble Mirth (priorité de la supervision).
    try:
        ov = _fetch_json(base, "/api/mirth/api", timeout)
        st["mirth_reachable"] = bool(ov.get("reachable", True))
        st["mirth_version"] = ov.get("version")
        st["channel_count"] = ov.get("channel_count")
        st["channels_started"] = ov.get("channels_started")
        totals = ov.get("totals") or {}
        st["error_count"] = int(totals.get("error") or 0)
        in_error = 0
        channels = []
        for c in (ov.get("channels") or []):
            err = int(c.get("error") or 0)
            if err > 0:
                in_error += 1
            channels.append({
                "channel_id": c.get("channel_id"), "name": c.get("name"),
                "state": c.get("state"), "error": err,
                "received": c.get("received"), "sent": c.get("sent"),
                "queued": c.get("queued"),
            })
        st["channels_in_error"] = in_error
        st["channels"] = channels
    except Exception as e:
        st["error"] = f"Mirth : {_err_str(e)}"

    # 3. Dernier relevé système de la machine hôte (CPU / mémoire).
    try:
        sl = _fetch_json(base, "/api/history/latest?tag=system", timeout)
        latest = (sl or {}).get("latest") or {}
        st["cpu_percent"] = latest.get("cpu_percent")
        st["mem_percent"] = latest.get("mem_percent")
    except Exception:
        pass

    # 4. Dernier relevé du processus Mirth (CPU / mémoire du process).
    try:
        ml = _fetch_json(base, "/api/mirth/history/latest", timeout)
        latest = (ml or {}).get("latest") or {}
        st["proc_cpu_percent"] = latest.get("cpu_percent")
        st["proc_mem_percent"] = latest.get("mem_percent")
    except Exception:
        pass

    st["latency_ms"] = round((time.monotonic() - t0) * 1000, 1)
    return st


def _set_summary(sites, ok, ko):
    with _poll_summary_lock:
        _poll_summary["sites"] = sites
        _poll_summary["ok"] = ok
        _poll_summary["ko"] = ko


def scheduled_poll():
    """Tâche de fond : relève en parallèle tous les sites actifs et stocke l'état."""
    sites = db.list_sites(enabled_only=True)
    if not sites:
        _set_summary(0, 0, 0)
        return

    ok = ko = 0
    workers = min(_POLL_WORKERS, len(sites))
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as ex:
        futures = {ex.submit(poll_site, s): s for s in sites}
        for fut in concurrent.futures.as_completed(futures):
            s = futures[fut]
            try:
                st = fut.result()
            except Exception as e:  # défensif : poll_site ne lève normalement pas
                st = {"polled_at": _now(), "ok": False, "error": _err_str(e)}
            try:
                db.upsert_status(s["id"], st)
            except Exception as e:
                log.log(f"[sites-poller] Erreur d'enregistrement pour "
                        f"{s['name']} : {e}")
            if st.get("ok"):
                ok += 1
            else:
                ko += 1

    _set_summary(len(sites), ok, ko)
    log.log(f"[sites-poller] {len(sites)} site(s) relevé(s) : {ok} OK, {ko} KO.")


def task_summary(name):
    """Valeur représentative d'une tâche pour la colonne « Valeur » du dashboard."""
    if name == POLL_TASK_NAME:
        with _poll_summary_lock:
            s = dict(_poll_summary)
        txt = f"{s['sites']} sites · {s['ok']} OK / {s['ko']} KO"
        return (txt, "red") if s["ko"] > 0 else txt
    return ""


def make_status_line(tasks):
    """Hook `on_complete` : ligne d'état console (repli texte, hors tableau rich)."""
    def render(_task=None):
        with _poll_summary_lock:
            s = dict(_poll_summary)
        log.status(f"[{POLL_TASK_NAME}] {s['sites']} sites · "
                   f"{s['ok']} OK / {s['ko']} KO")
    return render


# ==========================================================================
# API : SITES (configuration + état)
# ==========================================================================
def _parse_id(req):
    """Extrait l'id de site du chemin (paramètre {id}). None si invalide."""
    try:
        return int(req.params.get("id"))
    except (TypeError, ValueError):
        return None


def _require_site(req):
    """Renvoie le site visé par {id}, ou un tuple (status, payload) d'erreur."""
    site_id = _parse_id(req)
    if site_id is None:
        return (400, {"error": "Identifiant de site invalide."})
    site = db.get_site(site_id)
    if not site:
        return (404, {"error": "Site introuvable."})
    return site


def api_sites_list(req):
    """Liste de configuration des sites (sans leur état)."""
    return {"sites": db.list_sites()}


def api_sites_summary(req):
    """Sites + dernier instantané d'état (alimente le tableau de bord)."""
    return {"now": _now(), "sites": db.get_summary()}


def api_sites_add(req):
    """Ajoute un site. Corps JSON : {name, host, port}."""
    body = req.json()
    try:
        site = db.add_site(body.get("name"), body.get("host"), body.get("port"))
    except ValueError as e:
        return (400, {"ok": False, "error": str(e)})
    log.log(f"[superviseur] Site ajouté : {site['name']} "
            f"({site['host']}:{site['port']}).")
    return (201, {"ok": True, "site": site})


def api_sites_update(req):
    """Met à jour un site. Corps JSON : tout sous-ensemble de
    {name, host, port, enabled}."""
    site = _require_site(req)
    if isinstance(site, tuple):
        return site
    body = req.json()
    try:
        updated = db.update_site(
            site["id"],
            name=body.get("name"), host=body.get("host"),
            port=body.get("port"), enabled=body.get("enabled"),
        )
    except ValueError as e:
        return (400, {"ok": False, "error": str(e)})
    return {"ok": True, "site": updated}


def api_sites_toggle(req):
    """Active/désactive un site (bascule de `enabled`)."""
    site = _require_site(req)
    if isinstance(site, tuple):
        return site
    updated = db.set_enabled(site["id"], not site["enabled"])
    state = "activé" if updated["enabled"] else "désactivé"
    log.log(f"[superviseur] Site {state} : {updated['name']}.")
    return {"ok": True, "site": updated}


def api_sites_delete(req):
    """Supprime un site (et son instantané d'état)."""
    site = _require_site(req)
    if isinstance(site, tuple):
        return site
    db.delete_site(site["id"])
    log.log(f"[superviseur] Site supprimé : {site['name']}.")
    return {"ok": True, "deleted": site["id"]}


def api_sites_poll(req):
    """Relève immédiate d'UN site (bouton « rafraîchir » d'une ligne)."""
    site = _require_site(req)
    if isinstance(site, tuple):
        return site
    st = poll_site(site)
    try:
        db.upsert_status(site["id"], st)
    except Exception as e:
        log.log(f"[superviseur] Erreur d'enregistrement (poll manuel) : {e}")
    return {"ok": True, "site": site, "status": db.get_status(site["id"])}


def api_sites_proxy(req):
    """Relaie un GET vers une API du site distant (graphiques à la demande).

    ?path=/api/...  (le chemin, query comprise, du site distant). Restreint aux
    chemins `/api/` du site visé (anti-SSRF : l'hôte/port viennent de la config,
    le chemin est borné). La réponse distante est renvoyée telle quelle.
    """
    site = _require_site(req)
    if isinstance(site, tuple):
        return site
    path = req.get("path") or ""
    if not path.startswith("/api/"):
        return (400, {"error": "Chemin proxy invalide (doit commencer par /api/)."})
    base = _base_url(site)
    try:
        code, ctype, body = _http_get_raw(base, path, _PROXY_TIMEOUT)
    except Exception as e:
        return (502, {"error": f"Site injoignable : {_err_str(e)}"})
    return webserver.Response(body, status=code, content_type=ctype)


# ==========================================================================
# API : STATUT DU SUPERVISEUR
# ==========================================================================
def make_api_status(tasks, started_at):
    def api_status(req):
        return {
            "service": "superviseur",
            "started_at": started_at,
            "now": _now(),
            "schedulers": [t.status() for t in tasks],
            "db_path": db.DEFAULT_DB_PATH,
            "site_count": len(db.list_sites()),
        }
    return api_status


# ==========================================================================
# ROUTAGE
# ==========================================================================
def build_router(tasks, started_at):
    router = webserver.Router(static_dir=WEB_DIR, index_route="/")

    # Sites : configuration + état
    router.get("/api/sites", api_sites_list)
    router.post("/api/sites", api_sites_add)
    router.get("/api/sites/summary", api_sites_summary)
    router.post("/api/sites/{id}", api_sites_update)
    router.post("/api/sites/{id}/toggle", api_sites_toggle)
    router.post("/api/sites/{id}/delete", api_sites_delete)
    router.get("/api/sites/{id}/poll", api_sites_poll)
    router.post("/api/sites/{id}/poll", api_sites_poll)
    router.get("/api/sites/{id}/proxy", api_sites_proxy)

    # Statut du superviseur lui-même
    router.get("/api/status", make_api_status(tasks, started_at))
    return router


# ==========================================================================
# MAIN
# ==========================================================================
def main():
    parser = argparse.ArgumentParser(
        description="Méta-superviseur des instances checker_service "
                    "(tableau de bord agrégé, supervision multi-sites)."
    )
    parser.add_argument("--host", default="0.0.0.0",
                        help="Adresse d'écoute (def: 0.0.0.0)")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT,
                        help=f"Port d'écoute (def: {DEFAULT_PORT})")
    parser.add_argument("--interval", type=int, default=60,
                        help="Intervalle des relèves de sites en secondes (def: 60)")
    parser.add_argument("--timeout", type=float, default=5.0,
                        help="Délai (s) des requêtes HTTP vers chaque site (def: 5)")
    parser.add_argument("--no_output", "--no-output", action="store_true",
                        dest="no_output",
                        help="N'affiche RIEN dans la console (lancement en "
                             "arrière-plan). Le service et son API restent actifs.")
    args = parser.parse_args()

    if args.no_output:
        log.set_quiet(True)

    global _POLL_TIMEOUT
    _POLL_TIMEOUT = args.timeout

    # 1. Tâche programmée (créée avant le tableau de bord pour qu'il l'affiche
    #    dès la 1re frame).
    tasks = [RecurringTask(args.interval, scheduled_poll, name=POLL_TASK_NAME)]

    # 2. Tableau de bord console (rich) ; repli texte si rich absent / hors terminal.
    log.start_dashboard(tasks, title="Superviseur — relève des sites",
                        summary_provider=task_summary)
    log.log("[superviseur] Démarrage du superviseur.")

    # 3. Base
    db.init_db()
    log.log(f"[superviseur] Base SQLite : {db.DEFAULT_DB_PATH}")
    n_sites = len(db.list_sites())
    log.log(f"[superviseur] {n_sites} site(s) configuré(s).")

    # 4. Démarrage de la tâche
    status_line = make_status_line(tasks)
    for t in tasks:
        t.on_complete = status_line
        t.start()
    log.log(f"[superviseur] Relève des sites toutes les {args.interval}s "
            f"(délai HTTP {args.timeout}s).")

    # 5. Serveur web
    started_at = _now()
    router = build_router(tasks, started_at)
    try:
        httpd = webserver.serve(router, host=args.host, port=args.port)
    except OSError as e:
        for task in tasks:
            task.stop()
        log.stop_dashboard()
        log.log(f"[superviseur] Impossible d'écouter sur {args.host}:{args.port} "
                f"— le port est déjà utilisé (une autre instance ?). [{e}]")
        sys.exit(1)

    display_host = "localhost" if args.host in ("0.0.0.0", "") else args.host
    url = f"http://{display_host}:{args.port}/"
    log.log(f"[superviseur] Serveur web : {url}")
    log.log(f"[superviseur] Tableau de bord : {url}dashboard.html")
    log.log(f"[superviseur] Administration : {url}admin.html")
    log.log("[superviseur] Ctrl+C pour arrêter.")

    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        log.log("[superviseur] Arrêt en cours...")
    finally:
        log.stop_dashboard()
        log.clear()
        for task in tasks:
            task.stop()
        httpd.shutdown()
        httpd.server_close()
        log.log("[superviseur] Arrêté.")


if __name__ == "__main__":
    main()
