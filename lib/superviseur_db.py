# -*- coding: utf-8 -*-
"""
Couche d'accès SQLite pour le SUPERVISEUR (superviseur.db).

Contrairement à `lib/database.py` (qui historise les relevés d'UNE machine pour
`checker_service`), ce module ne gère que :

- `sites`        : la liste des instances `checker_service` à superviser
                   (nom + hôte + port + activation) — gérée depuis la page
                   d'administration (ajout / désactivation / suppression) ;
- `site_status`  : le DERNIER instantané d'état de chaque site (une ligne par
                   site, remplacée à chaque relève par le collecteur de fond).

Aucune série temporelle n'est stockée ici : les graphiques de la page de
détail sont tirés À LA DEMANDE des API du site distant (qui historise déjà tout),
relayées par le proxy du superviseur. La base reste donc petite et légère.

Le module est sans dépendance externe (uniquement `sqlite3`). Chaque fonction
ouvre/ferme sa propre connexion (WAL) pour gérer proprement les accès concurrents
entre le thread du collecteur et les threads du serveur web.
"""

import os
import sys
import json
import sqlite3
import datetime
import threading

from . import database

# Emplacement de la base : à côté du script en exécution normale ; à côté de
# l'exécutable en build gelé (PyInstaller --onefile, où __file__ pointe dans le
# dossier temporaire _MEIxxxx effacé à la sortie). Même logique que
# lib/database.py pour rester cohérent.
if getattr(sys, "frozen", False):
    _DB_BASE_DIR = os.path.dirname(os.path.abspath(sys.executable))
else:
    _DB_BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEFAULT_DB_PATH = os.path.join(_DB_BASE_DIR, "superviseur.db")

# Verrou de fichier (réinitialisation, etc.). Les écritures concurrentes
# courantes sont gérées par WAL ; ce verrou couvre les opérations au niveau
# système si besoin futur.
_FILE_LOCK = threading.RLock()


def _now():
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _connect(db_path):
    conn = sqlite3.connect(db_path, timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    # Cascade de suppression site -> site_status (active uniquement si foreign_keys=ON).
    conn.execute("PRAGMA foreign_keys=ON;")
    return conn


def init_db(db_path=DEFAULT_DB_PATH):
    """Crée les tables et index si nécessaire. Idempotent."""
    conn = _connect(db_path)
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS sites (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                name        TEXT    NOT NULL,
                host        TEXT    NOT NULL,
                port        INTEGER NOT NULL,
                enabled     INTEGER NOT NULL DEFAULT 1,
                created_at  TEXT    NOT NULL,
                UNIQUE(host, port)
            )
            """
        )
        # Migration : colonnes de sécurité du lien superviseur -> site distant.
        #   scheme     : 'http' | 'https' (protocole d'accès au site) ;
        #   verify_ssl : 0 = accepter le certificat auto-signé du site (défaut) ;
        #   api_key    : clé API (Bearer statique) ; OU
        #   username/password : compte du site (échangé une fois contre un jeton).
        scols = [r["name"] for r in conn.execute("PRAGMA table_info(sites)").fetchall()]
        if "scheme" not in scols:
            conn.execute("ALTER TABLE sites ADD COLUMN scheme TEXT NOT NULL DEFAULT 'http'")
        if "verify_ssl" not in scols:
            conn.execute("ALTER TABLE sites ADD COLUMN verify_ssl INTEGER NOT NULL DEFAULT 0")
        if "api_key" not in scols:
            conn.execute("ALTER TABLE sites ADD COLUMN api_key TEXT")
        if "username" not in scols:
            conn.execute("ALTER TABLE sites ADD COLUMN username TEXT")
        if "password" not in scols:
            conn.execute("ALTER TABLE sites ADD COLUMN password TEXT")
        # Instantané courant : une seule ligne par site (PRIMARY KEY = site_id),
        # remplacée à chaque relève. ON DELETE CASCADE supprime l'état d'un site
        # supprimé.
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS site_status (
                site_id           INTEGER PRIMARY KEY,
                polled_at         TEXT,
                ok                INTEGER,
                latency_ms        REAL,
                error             TEXT,
                hostname          TEXT,
                os                TEXT,
                cpu_percent       REAL,
                mem_percent       REAL,
                proc_cpu_percent  REAL,
                proc_mem_percent  REAL,
                mirth_reachable   INTEGER,
                mirth_version     TEXT,
                channel_count     INTEGER,
                channels_started  INTEGER,
                error_count       INTEGER,
                channels_in_error INTEGER,
                channels_json     TEXT,
                FOREIGN KEY(site_id) REFERENCES sites(id) ON DELETE CASCADE
            )
            """
        )
        conn.commit()
    finally:
        conn.close()
    # Tables d'authentification (mêmes que checker : comptes/sessions/clés) pour
    # protéger la PROPRE interface du superviseur avec le même mécanisme.
    database.init_auth_tables(db_path)


def _norm_scheme(value, default="http"):
    v = (str(value).strip().lower() if value is not None else "")
    return v if v in ("http", "https") else default


# ==========================================================================
# SITES (configuration)
# ==========================================================================
def _site_dict(row):
    d = dict(row)
    d["enabled"] = bool(d.get("enabled"))
    if "verify_ssl" in d:
        d["verify_ssl"] = bool(d.get("verify_ssl"))
    return d


def list_sites(enabled_only=False, db_path=DEFAULT_DB_PATH):
    """Renvoie tous les sites configurés (triés par nom), ou seulement les actifs."""
    conn = _connect(db_path)
    try:
        sql = "SELECT * FROM sites"
        if enabled_only:
            sql += " WHERE enabled = 1"
        sql += " ORDER BY name COLLATE NOCASE, id"
        rows = conn.execute(sql).fetchall()
        return [_site_dict(r) for r in rows]
    finally:
        conn.close()


def get_site(site_id, db_path=DEFAULT_DB_PATH):
    """Renvoie un site par son id, ou None."""
    conn = _connect(db_path)
    try:
        row = conn.execute("SELECT * FROM sites WHERE id = ?", (site_id,)).fetchone()
        return _site_dict(row) if row else None
    finally:
        conn.close()


def add_site(name, host, port, enabled=True, scheme="http", verify_ssl=False,
             api_key=None, username=None, password=None, db_path=DEFAULT_DB_PATH):
    """Ajoute un site. Lève `ValueError` si (host, port) existe déjà ou si les
    champs sont invalides. Renvoie le site créé."""
    name = (name or "").strip()
    host = (host or "").strip()
    if not name:
        raise ValueError("Le nom du site est obligatoire.")
    if not host:
        raise ValueError("L'hôte (IP ou nom) est obligatoire.")
    try:
        port = int(port)
    except (TypeError, ValueError):
        raise ValueError("Le port doit être un entier.")
    if not (1 <= port <= 65535):
        raise ValueError("Le port doit être compris entre 1 et 65535.")

    scheme = _norm_scheme(scheme)
    api_key = (api_key or "").strip() or None
    username = (username or "").strip() or None
    password = (password or "") or None

    conn = _connect(db_path)
    try:
        try:
            cur = conn.execute(
                "INSERT INTO sites (name, host, port, enabled, created_at, scheme, "
                "verify_ssl, api_key, username, password) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (name, host, port, 1 if enabled else 0, _now(), scheme,
                 1 if verify_ssl else 0, api_key, username, password),
            )
        except sqlite3.IntegrityError:
            raise ValueError(f"Un site existe déjà pour {host}:{port}.")
        conn.commit()
        new_id = cur.lastrowid
        row = conn.execute("SELECT * FROM sites WHERE id = ?", (new_id,)).fetchone()
        return _site_dict(row)
    finally:
        conn.close()


def update_site(site_id, name=None, host=None, port=None, enabled=None,
                scheme=None, verify_ssl=None, api_key=None, username=None,
                password=None, db_path=DEFAULT_DB_PATH):
    """Met à jour les champs fournis (non None) d'un site. Renvoie le site mis à
    jour, ou None si l'id est inconnu. Lève `ValueError` sur conflit/invalidité.

    Pour les secrets (api_key/username/password) : None = inchangé ; chaîne vide
    = effacement (valeur mise à NULL)."""
    site = get_site(site_id, db_path=db_path)
    if not site:
        return None

    new_name = site["name"] if name is None else (name or "").strip()
    new_host = site["host"] if host is None else (host or "").strip()
    new_port = site["port"] if port is None else port
    new_enabled = site["enabled"] if enabled is None else bool(enabled)
    new_scheme = site.get("scheme", "http") if scheme is None else _norm_scheme(scheme)
    new_verify = site.get("verify_ssl", False) if verify_ssl is None else bool(verify_ssl)
    # Secrets : None = inchangé ; "" = effacer.
    new_key = site.get("api_key") if api_key is None else ((api_key or "").strip() or None)
    new_user = site.get("username") if username is None else ((username or "").strip() or None)
    new_pwd = site.get("password") if password is None else ((password or "") or None)

    if not new_name:
        raise ValueError("Le nom du site est obligatoire.")
    if not new_host:
        raise ValueError("L'hôte (IP ou nom) est obligatoire.")
    try:
        new_port = int(new_port)
    except (TypeError, ValueError):
        raise ValueError("Le port doit être un entier.")
    if not (1 <= new_port <= 65535):
        raise ValueError("Le port doit être compris entre 1 et 65535.")

    conn = _connect(db_path)
    try:
        try:
            conn.execute(
                "UPDATE sites SET name = ?, host = ?, port = ?, enabled = ?, "
                "scheme = ?, verify_ssl = ?, api_key = ?, username = ?, password = ? "
                "WHERE id = ?",
                (new_name, new_host, new_port, 1 if new_enabled else 0, new_scheme,
                 1 if new_verify else 0, new_key, new_user, new_pwd, site_id),
            )
        except sqlite3.IntegrityError:
            raise ValueError(f"Un autre site existe déjà pour {new_host}:{new_port}.")
        conn.commit()
        row = conn.execute("SELECT * FROM sites WHERE id = ?", (site_id,)).fetchone()
        return _site_dict(row) if row else None
    finally:
        conn.close()


def set_enabled(site_id, enabled, db_path=DEFAULT_DB_PATH):
    """Active/désactive un site. Renvoie le site mis à jour, ou None."""
    return update_site(site_id, enabled=enabled, db_path=db_path)


def delete_site(site_id, db_path=DEFAULT_DB_PATH):
    """Supprime un site (et son instantané d'état via la cascade). Renvoie True
    si une ligne a été supprimée."""
    conn = _connect(db_path)
    try:
        cur = conn.execute("DELETE FROM sites WHERE id = ?", (site_id,))
        conn.commit()
        return cur.rowcount > 0
    finally:
        conn.close()


# ==========================================================================
# STATUS (instantané courant, une ligne par site)
# ==========================================================================
# Colonnes acceptées par upsert_status (anti-erreur de frappe + filtrage).
_STATUS_FIELDS = (
    "polled_at", "ok", "latency_ms", "error", "hostname", "os",
    "cpu_percent", "mem_percent", "proc_cpu_percent", "proc_mem_percent",
    "mirth_reachable", "mirth_version", "channel_count", "channels_started",
    "error_count", "channels_in_error", "channels_json",
)


def _status_dict(row):
    if row is None:
        return None
    d = dict(row)
    d["ok"] = bool(d.get("ok"))
    if d.get("mirth_reachable") is not None:
        d["mirth_reachable"] = bool(d.get("mirth_reachable"))
    # Décode la liste compacte des canaux (stockée en JSON) pour l'API.
    raw = d.pop("channels_json", None)
    try:
        d["channels"] = json.loads(raw) if raw else []
    except (ValueError, TypeError):
        d["channels"] = []
    return d


def upsert_status(site_id, status, db_path=DEFAULT_DB_PATH):
    """Enregistre (remplace) l'instantané d'état d'un site.

    `status` est un dict dont seules les clés de `_STATUS_FIELDS` sont prises en
    compte. `channels` (liste) est sérialisée en `channels_json`.
    """
    data = {k: status.get(k) for k in _STATUS_FIELDS}
    if "channels" in status and status.get("channels") is not None:
        data["channels_json"] = json.dumps(status["channels"], ensure_ascii=False)
    cols = ["site_id"] + list(_STATUS_FIELDS)
    placeholders = ", ".join("?" for _ in cols)
    values = [site_id] + [data.get(k) for k in _STATUS_FIELDS]
    conn = _connect(db_path)
    try:
        conn.execute(
            f"INSERT OR REPLACE INTO site_status ({', '.join(cols)}) "
            f"VALUES ({placeholders})",
            values,
        )
        conn.commit()
    finally:
        conn.close()


def get_status(site_id, db_path=DEFAULT_DB_PATH):
    """Renvoie l'instantané d'état d'un site (dict), ou None."""
    conn = _connect(db_path)
    try:
        row = conn.execute(
            "SELECT * FROM site_status WHERE site_id = ?", (site_id,)
        ).fetchone()
        return _status_dict(row)
    finally:
        conn.close()


def get_summary(db_path=DEFAULT_DB_PATH):
    """Renvoie tous les sites avec leur dernier instantané d'état (jointure).

    Chaque élément : la config du site (`id`, `name`, `host`, `port`, `enabled`,
    `created_at`) augmentée d'une clé `status` (le dict d'état, ou None si le site
    n'a pas encore été relevé).
    """
    sites = list_sites(db_path=db_path)
    conn = _connect(db_path)
    try:
        rows = conn.execute("SELECT * FROM site_status").fetchall()
    finally:
        conn.close()
    by_id = {r["site_id"]: _status_dict(r) for r in rows}
    out = []
    for s in sites:
        s = dict(s)
        s["status"] = by_id.get(s["id"])
        out.append(s)
    return out
