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
import ssl
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
from lib import webserver, log, auth, database, security, authroutes, tls
from lib.scheduler import RecurringTask

# quickmail (racine du projet) : envoi des mots de passe générés par e-mail.
try:
    import quickmail
except Exception:   # pragma: no cover - environnement minimal
    quickmail = None

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

# État de sécurité du superviseur (renseigné par main()).
SUP_HTTPS = False
SUP_AUTH_ENABLED = False
SUP_BASE_URL = None
SESSION_CLEANUP_TASK_NAME = "session-cleanup"
SESSION_CLEANUP_TIME = datetime.time(3, 20)

# Jetons de session par site distant (échange login/mdp -> jeton, réutilisé 24 h).
# {site_id: {"token": str, "expires": monotonic}}. Les clés API sont utilisées
# directement comme Bearer (pas d'échange).
_site_tokens = {}
_site_tokens_lock = threading.Lock()


def _truthy(v):
    if isinstance(v, bool):
        return v
    if v is None:
        return False
    return str(v).strip().lower() in ("1", "true", "yes", "on")


# ==========================================================================
# OUTILS
# ==========================================================================
def _now():
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _base_url(site):
    """URL de base d'un site supervisé (http(s)://host:port selon le schéma)."""
    scheme = site.get("scheme") or "http"
    return f"{scheme}://{site['host']}:{site['port']}"


def _mask_site(s):
    """Copie d'un site SANS les secrets (pour les réponses API).

    Le mot de passe et la clé API ne sont jamais renvoyés ; on expose seulement
    des drapeaux `has_password` / `has_api_key`. `username` (non secret) est gardé.
    """
    s = dict(s)
    pwd = s.pop("password", None)
    key = s.pop("api_key", None)
    s["has_password"] = bool(pwd)
    s["has_api_key"] = bool(key)
    return s


def _ssl_ctx_for(site):
    """Contexte SSL pour joindre un site : non-vérifiant si https + verify_ssl=0."""
    if (site or {}).get("scheme") != "https":
        return None
    if site.get("verify_ssl"):
        return None                       # vérification standard (contexte défaut)
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    return ctx


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


def _post_json(base, path, payload, timeout, ctx=None):
    """POST JSON vers `base + path`. Renvoie (status, body_bytes). Ne lève pas sur
    statut HTTP (renvoie le code), lève seulement sur erreur réseau/délai."""
    data = json.dumps(payload).encode("utf-8")
    headers = dict(_HDRS)
    headers["Content-Type"] = "application/json"
    req = urllib.request.Request(base + path, data=data, headers=headers, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=timeout, context=ctx) as r:
            return r.getcode(), r.read()
    except urllib.error.HTTPError as e:
        return e.code, (e.read() if hasattr(e, "read") else b"")


def _site_bearer(site, timeout, force=False):
    """Jeton Bearer pour joindre un site.

    - clé API : utilisée directement (Bearer statique, révocable côté site) ;
    - sinon username/mdp : échangés UNE fois contre un jeton de session (24 h),
      mis en cache et réutilisés ; `force=True` refait l'échange (self-healing 401).
    None si le site n'a aucun identifiant (site sans authentification).
    """
    if not site:
        return None
    key = site.get("api_key")
    if key:
        return key
    user, pwd = site.get("username"), site.get("password")
    if not (user and pwd):
        return None
    sid = site["id"]
    if not force:
        with _site_tokens_lock:
            cached = _site_tokens.get(sid)
            if cached and cached["expires"] > time.monotonic():
                return cached["token"]
    base = _base_url(site)
    ctx = _ssl_ctx_for(site)
    try:
        code, body = _post_json(base, "/api/auth/login",
                                {"username": user, "password": pwd}, timeout, ctx)
        if code == 200:
            tok = (json.loads(body.decode("utf-8")) or {}).get("token")
            if tok:
                with _site_tokens_lock:
                    # Rafraîchi bien avant l'expiration réelle (fenêtre 24 h glissante).
                    _site_tokens[sid] = {"token": tok,
                                         "expires": time.monotonic() + 23 * 3600}
                return tok
        else:
            log.log(f"[superviseur] Login refusé sur {site.get('name')} (HTTP {code}).")
    except Exception as e:
        log.log(f"[superviseur] Login échoué sur {site.get('name')} : {_err_str(e)}")
    return None


def _http_get_raw(base, path, timeout, site=None):
    """GET brut vers `base + path`. Renvoie (status, content_type, body_bytes).

    Joint le jeton Bearer du site (clé API ou session) et gère le TLS selon le
    schéma/verify_ssl. Une réponse non-2xx AVEC corps (ex. 404 JSON du site) est
    renvoyée telle quelle (pour le proxy). Sur 401 avec un compte (username/mdp),
    ré-authentifie une fois et rejoue (self-healing, comme le client Mirth).
    Une erreur de connexion/délai lève l'exception.
    """
    url = base + path
    ctx = _ssl_ctx_for(site) if site else None

    def _do(bearer):
        headers = dict(_HDRS)
        if bearer:
            headers["Authorization"] = "Bearer " + bearer
        req = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(req, timeout=timeout, context=ctx) as r:
            ctype = r.headers.get("Content-Type", "application/json; charset=utf-8")
            return r.getcode(), ctype, r.read()

    bearer = _site_bearer(site, timeout) if site else None
    try:
        return _do(bearer)
    except urllib.error.HTTPError as e:
        # Session expirée (compte username/mdp) : ré-auth une fois et on rejoue.
        if e.code == 401 and site and not site.get("api_key") and site.get("username"):
            fresh = _site_bearer(site, timeout, force=True)
            if fresh:
                try:
                    return _do(fresh)
                except urllib.error.HTTPError as e2:
                    e = e2
        body = e.read() if hasattr(e, "read") else b""
        ctype = (e.headers.get("Content-Type") if e.headers else None) \
            or "application/json; charset=utf-8"
        return e.code, ctype, body


def _fetch_json(base, path, timeout, site=None):
    """GET JSON vers `base + path`. Lève sur erreur réseau ou statut >= 400."""
    code, _ctype, body = _http_get_raw(base, path, timeout, site=site)
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
        info = _fetch_json(base, "/api/hostinfo", timeout, site=site)
        st["ok"] = True
        st["hostname"] = info.get("hostname")
        st["os"] = info.get("os")
    except Exception as e:
        st["error"] = _err_str(e)
        st["latency_ms"] = round((time.monotonic() - t0) * 1000, 1)
        return st

    # 2. Vue d'ensemble Mirth (priorité de la supervision).
    try:
        ov = _fetch_json(base, "/api/mirth/api", timeout, site=site)
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
        sl = _fetch_json(base, "/api/history/latest?tag=system", timeout, site=site)
        latest = (sl or {}).get("latest") or {}
        st["cpu_percent"] = latest.get("cpu_percent")
        st["mem_percent"] = latest.get("mem_percent")
    except Exception:
        pass

    # 4. Dernier relevé du processus Mirth (CPU / mémoire du process).
    try:
        ml = _fetch_json(base, "/api/mirth/history/latest", timeout, site=site)
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
    """Liste de configuration des sites (sans leur état, secrets masqués)."""
    return {"sites": [_mask_site(s) for s in db.list_sites()]}


def api_sites_summary(req):
    """Sites + dernier instantané d'état (alimente le tableau de bord)."""
    return {"now": _now(), "sites": [_mask_site(s) for s in db.get_summary()]}


def api_sites_add(req):
    """Ajoute un site. Corps JSON : {name, host, port, scheme, verify_ssl,
    api_key | username+password}."""
    body = req.json()
    try:
        site = db.add_site(
            body.get("name"), body.get("host"), body.get("port"),
            scheme=body.get("scheme", "http"),
            verify_ssl=_truthy(body.get("verify_ssl")),
            api_key=body.get("api_key"), username=body.get("username"),
            password=body.get("password"))
    except ValueError as e:
        return (400, {"ok": False, "error": str(e)})
    log.log(f"[superviseur] Site ajouté : {site['name']} "
            f"({site.get('scheme')}://{site['host']}:{site['port']}).")
    return (201, {"ok": True, "site": _mask_site(site)})


def api_sites_update(req):
    """Met à jour un site. Corps JSON : tout sous-ensemble de {name, host, port,
    enabled, scheme, verify_ssl, api_key, username, password} (secrets : "" efface)."""
    site = _require_site(req)
    if isinstance(site, tuple):
        return site
    body = req.json()
    vs = body.get("verify_ssl")
    try:
        updated = db.update_site(
            site["id"],
            name=body.get("name"), host=body.get("host"),
            port=body.get("port"), enabled=body.get("enabled"),
            scheme=body.get("scheme"),
            verify_ssl=(None if vs is None else _truthy(vs)),
            api_key=body.get("api_key"), username=body.get("username"),
            password=body.get("password"),
        )
    except ValueError as e:
        return (400, {"ok": False, "error": str(e)})
    # Un changement d'identifiants invalide le jeton de session mémorisé du site.
    with _site_tokens_lock:
        _site_tokens.pop(site["id"], None)
    return {"ok": True, "site": _mask_site(updated)}


def api_sites_toggle(req):
    """Active/désactive un site (bascule de `enabled`)."""
    site = _require_site(req)
    if isinstance(site, tuple):
        return site
    updated = db.set_enabled(site["id"], not site["enabled"])
    state = "activé" if updated["enabled"] else "désactivé"
    log.log(f"[superviseur] Site {state} : {updated['name']}.")
    return {"ok": True, "site": _mask_site(updated)}


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
    return {"ok": True, "site": _mask_site(site), "status": db.get_status(site["id"])}


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
        code, ctype, body = _http_get_raw(base, path, _PROXY_TIMEOUT, site=site)
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
# AUTHENTIFICATION DE L'INTERFACE DU SUPERVISEUR
# ==========================================================================
def _send_account_email(username, email, password, renew=False):
    """Envoie le mot de passe généré au titulaire d'un compte du superviseur."""
    if quickmail is None:
        return False
    base = SUP_BASE_URL or ""
    action = "renouvelé" if renew else "créé"
    subject = "Superviseur Mirth_checker — vos identifiants"
    message = (
        f"Bonjour,\n\n"
        f"Un compte d'accès au logiciel « Superviseur » (supervision à distance des "
        f"serveurs Mirth_checker) a été {action} pour vous.\n\n"
        f"  Identifiant : {username}\n"
        f"  Mot de passe : {password}\n"
        + (f"  Adresse du superviseur : {base}\n" if base else "")
        + "\nCe mot de passe est personnel et ne pourra pas vous être communiqué de "
        "nouveau : conservez-le en lieu sûr.\n"
    )
    esc = lambda s: str(s).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    link = (f'<p>Adresse du superviseur : <a href="{esc(base)}">{esc(base)}</a></p>'
            if base else "")
    html = (
        f"<p>Bonjour,</p><p>Un compte d'accès au logiciel « <b>Superviseur</b> » a été "
        f"{action} pour vous.</p><ul><li>Identifiant : <b>{esc(username)}</b></li>"
        f"<li>Mot de passe : <code>{esc(password)}</code></li></ul>{link}"
        f"<p style='color:#888'>Ce mot de passe est personnel et ne pourra pas vous être "
        f"communiqué de nouveau.</p>"
    )
    try:
        return bool(quickmail.sendmail(subject, message, email, html=html))
    except Exception as e:
        log.log(f"[superviseur] Échec d'envoi du mot de passe à {email} : {e}")
        return False


def _build_auth_routes():
    return authroutes.make_auth_routes(
        db.DEFAULT_DB_PATH,
        is_https=lambda: SUP_HTTPS,
        base_url=lambda: SUP_BASE_URL or "",
        mailer=_send_account_email,
        log_fn=log.log,
        auth_enabled=lambda: SUP_AUTH_ENABLED,
    )


def scheduled_session_cleanup():
    """Purge quotidienne des sessions web expirées du superviseur."""
    database.purge_expired_sessions(db_path=db.DEFAULT_DB_PATH)


# ==========================================================================
# ROUTAGE
# ==========================================================================
def build_router(tasks, started_at):
    router = webserver.Router(static_dir=WEB_DIR, index_route="/")

    # Authentification + comptes + clés (mêmes routes que checker_service, sur
    # superviseur.db) — protège la propre interface du superviseur.
    _build_auth_routes().register(router)

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


def _run_account_cli(args):
    """--add-admin / --list-users / --del-user sur la base du superviseur."""
    db.init_db()
    if args.list_users:
        users = database.list_users(db_path=db.DEFAULT_DB_PATH)
        if not users:
            print("Aucun compte enregistré.")
        else:
            for u in users:
                print(f"  {u['username']:<20} {u['email']:<28} {u['role']:<12} "
                      f"{'actif' if u['enabled'] else 'désactivé'}")
        return
    if args.del_user:
        ok = database.delete_user(args.del_user, db_path=db.DEFAULT_DB_PATH)
        print(f"Compte '{args.del_user}' {'supprimé' if ok else 'introuvable'}.")
        return
    if args.add_admin:
        username = args.add_admin.strip()
        email = (args.email or "").strip()
        password = auth.generate_password()
        if not database.create_user(username, email or "-", auth.hash_password(password),
                                    role="admin", db_path=db.DEFAULT_DB_PATH):
            print(f"Erreur : le compte '{username}' existe déjà.")
            return
        sent = _send_account_email(username, email, password) if email else False
        if sent:
            print(f"Compte administrateur '{username}' créé. Mot de passe envoyé à {email}.")
        else:
            print(f"Compte administrateur '{username}' créé.")
            print(f"  Mot de passe (à transmettre de façon sûre) : {password}")


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

    sec = parser.add_argument_group("Sécurité")
    sec.add_argument("--https", choices=["auto", "on", "off"], default=None,
                     help="HTTPS de l'interface superviseur : auto/on/off.")
    sec.add_argument("--cert", default=None, help="Certificat TLS (PEM).")
    sec.add_argument("--key", default=None, help="Clé privée TLS (PEM).")
    sec.add_argument("--allow-ips", dest="allow_ips", default=None,
                     help="Liste blanche d'IP/CIDR (vide => tout autorisé).")
    sec.add_argument("--auth", choices=["on", "off"], default=None,
                     help="Authentification par comptes/session de l'interface.")

    acc = parser.add_argument_group("Comptes (CLI)")
    acc.add_argument("--add-admin", dest="add_admin", metavar="IDENTIFIANT", default=None,
                     help="Crée un compte administrateur (mdp généré, e-mail avec --email).")
    acc.add_argument("--email", default=None, help="E-mail du compte (avec --add-admin).")
    acc.add_argument("--list-users", dest="list_users", action="store_true",
                     help="Affiche la liste des comptes et quitte.")
    acc.add_argument("--del-user", dest="del_user", metavar="IDENTIFIANT", default=None,
                     help="Supprime un compte et quitte.")

    args = parser.parse_args()

    if args.add_admin or args.list_users or args.del_user:
        _run_account_cli(args)
        return

    if args.no_output:
        log.set_quiet(True)

    global _POLL_TIMEOUT
    _POLL_TIMEOUT = args.timeout

    # --- Sécurité : configuration + TLS + politique d'accès --------------------
    global SUP_HTTPS, SUP_AUTH_ENABLED, SUP_BASE_URL
    sec_cfg = security.load_security_config(args)
    auth.set_session_ttl_hours(sec_cfg["SESSION_TTL_H"])
    SUP_AUTH_ENABLED = sec_cfg["AUTH_ENABLED"]
    _sec_log = []

    tls_context = None
    if security.https_enabled_for_host(sec_cfg["HTTPS_MODE"], args.host):
        cert_dir = os.path.dirname(db.DEFAULT_DB_PATH)
        cert = sec_cfg["HTTPS_CERT"] or os.path.join(cert_dir, "superviseur_cert.pem")
        key = sec_cfg["HTTPS_KEY"] or os.path.join(cert_dir, "superviseur_key.pem")
        provided = bool(sec_cfg["HTTPS_CERT"] and sec_cfg["HTTPS_KEY"])
        try:
            if not provided:
                tls.ensure_self_signed_cert(cert, key, hostname=socket.gethostname() or None)
            tls_context = tls.build_ssl_context(cert, key)
            _sec_log.append(f"[superviseur] HTTPS activé (certificat : {cert}).")
        except Exception as e:
            if sec_cfg["HTTPS_MODE"] == "on":
                print(f"[superviseur] HTTPS demandé mais impossible à activer : {e}",
                      file=sys.stderr)
                sys.exit(1)
            _sec_log.append(f"[superviseur] HTTPS 'auto' indisponible ({e}) — repli en HTTP.")
    SUP_HTTPS = tls_context is not None

    security_policy = security.SecurityPolicy(
        networks=security.parse_networks(sec_cfg["ALLOWED_IPS"]),
        tls_context=tls_context,
        auth_enabled=sec_cfg["AUTH_ENABLED"],
        db_path=db.DEFAULT_DB_PATH,
    )
    scheme = "https" if SUP_HTTPS else "http"
    # URL de base pour les e-mails : IP routable (pas le nom d'hôte, faute de DNS
    # local) ; derrière un NAT, poser SUPERVISEUR_BASE_URL.
    SUP_BASE_URL = (os.environ.get("SUPERVISEUR_BASE_URL") or "").strip().rstrip("/") \
        or f"{scheme}://{security.primary_ip(args.host)}:{args.port}"

    # 1. Tâche programmée (créée avant le tableau de bord pour qu'il l'affiche
    #    dès la 1re frame).
    tasks = [RecurringTask(args.interval, scheduled_poll, name=POLL_TASK_NAME)]
    if SUP_AUTH_ENABLED:
        tasks.append(RecurringTask(24 * 3600, scheduled_session_cleanup,
                                   name=SESSION_CLEANUP_TASK_NAME,
                                   daily_at=SESSION_CLEANUP_TIME))

    # 2. Tableau de bord console (rich) ; repli texte si rich absent / hors terminal.
    log.start_dashboard(tasks, title="Superviseur — relève des sites",
                        summary_provider=task_summary)
    log.log("[superviseur] Démarrage du superviseur.")

    # 3. Base
    db.init_db()
    log.log(f"[superviseur] Base SQLite : {db.DEFAULT_DB_PATH}")
    n_sites = len(db.list_sites())
    log.log(f"[superviseur] {n_sites} site(s) configuré(s).")

    # 3bis. État de sécurité (différé depuis le chargement de la config).
    for line in _sec_log:
        log.log(line)
    log.log(
        f"[superviseur] Sécurité : HTTPS={'oui' if SUP_HTTPS else 'non'}, "
        f"authentification={'oui' if SUP_AUTH_ENABLED else 'non'}, "
        f"filtre IP={len(security_policy.networks) or 'désactivé'}.")
    if SUP_AUTH_ENABLED and database.count_users(db_path=db.DEFAULT_DB_PATH) == 0:
        log.log("[superviseur] Aucun compte : accès toléré depuis localhost pour créer "
                "le 1er administrateur (UI ou --add-admin).")

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
        httpd = webserver.serve(router, host=args.host, port=args.port,
                                security=security_policy)
    except OSError as e:
        for task in tasks:
            task.stop()
        log.stop_dashboard()
        log.log(f"[superviseur] Impossible d'écouter sur {args.host}:{args.port} "
                f"— le port est déjà utilisé (une autre instance ?). [{e}]")
        sys.exit(1)

    display_host = "localhost" if args.host in ("0.0.0.0", "") else args.host
    url = f"{scheme}://{display_host}:{args.port}/"
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
