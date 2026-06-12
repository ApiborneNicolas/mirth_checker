# -*- coding: utf-8 -*-
"""
Couche d'accès SQLite pour l'historique des métriques système.

La base contient une table `metrics` qui stocke un échantillon (CPU, mémoire,
stockage) à chaque exécution de la tâche programmée. Le module est volontairement
sans dépendance externe (uniquement le module standard `sqlite3`).

Toutes les fonctions ouvrent/ferment leur propre connexion : SQLite gère ainsi
proprement les accès concurrents entre le thread de la tâche programmée et les
threads du serveur web (mode WAL activé).
"""

import os
import io
import csv
import shutil
import sqlite3
import datetime
import tempfile
import threading

# Emplacement par défaut de la base : à la racine du projet (un niveau au-dessus de lib/)
DEFAULT_DB_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "checker_history.db"
)

# Tables de relevés partageant le même schéma et les mêmes fonctions d'accès :
#   - 'metrics'        : relevés système de la machine hôte ;
#   - 'mirth_metrics'  : relevés du processus Mirth (table dédiée, accès facilité).
# Liste blanche : seules ces valeurs peuvent être interpolées dans le SQL.
_METRIC_TABLES = ("metrics", "mirth_metrics")


def _check_table(table):
    """Valide un nom de table de relevés (anti-injection) et le renvoie."""
    if table not in _METRIC_TABLES:
        raise ValueError(f"Table de relevés inconnue : {table!r}")
    return table

# Verrou protégeant les opérations qui manipulent le fichier au niveau système
# (remplacement par import, VACUUM, réinitialisation). La tâche programmée et les
# threads du serveur web peuvent écrire en parallèle : ce verrou évite qu'un
# `os.replace` survienne pendant qu'une connexion est ouverte sur l'ancien fichier.
_FILE_LOCK = threading.RLock()


def _connect(db_path):
    conn = sqlite3.connect(db_path, timeout=10)
    conn.row_factory = sqlite3.Row
    # WAL : permet la lecture pendant l'écriture (tâche programmée + serveur web)
    conn.execute("PRAGMA journal_mode=WAL;")
    return conn


def init_db(db_path=DEFAULT_DB_PATH):
    """Crée la table et les index si nécessaire. Idempotent."""
    conn = _connect(db_path)
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS metrics (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp       TEXT    NOT NULL,
                tag             TEXT    NOT NULL DEFAULT 'system',
                cpu_percent     REAL,
                mem_percent     REAL,
                mem_used_gb     REAL,
                mem_total_gb    REAL,
                disk_percent    REAL,
                disk_used_gb    REAL,
                disk_total_gb   REAL,
                sockets         INTEGER,
                event           TEXT
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_metrics_timestamp ON metrics(timestamp)"
        )
        # Migrations : ajoute les colonnes apparues après la création initiale.
        # (Effectuées avant l'index composite ci-dessous, qui référence `tag`.)
        cols = [r["name"] for r in conn.execute("PRAGMA table_info(metrics)").fetchall()]
        if "event" not in cols:
            conn.execute("ALTER TABLE metrics ADD COLUMN event TEXT")
        # `tag` : étiquette la provenance d'un relevé ('system', 'mirth', ...) afin
        # de pouvoir faire cohabiter plusieurs sources dans la même table et de
        # filtrer l'historique par source. Les relevés antérieurs sont 'system'.
        if "tag" not in cols:
            conn.execute("ALTER TABLE metrics ADD COLUMN tag TEXT DEFAULT 'system'")
            conn.execute("UPDATE metrics SET tag = 'system' WHERE tag IS NULL")
        # `sockets` : nombre de sockets (ports en écoute + connexions) — utilisé
        # pour le suivi temporel d'un processus (ex. mcservice.exe).
        if "sockets" not in cols:
            conn.execute("ALTER TABLE metrics ADD COLUMN sockets INTEGER")
        # Index composite : l'historique est presque toujours filtré par `tag`
        # ('system', 'mirth', ...) puis trié/borné par horodatage.
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_metrics_tag_ts ON metrics(tag, timestamp)"
        )

        # Table dédiée aux relevés du PROCESSUS MIRTH (cpu/mem/sockets). Schéma
        # identique à `metrics` pour réutiliser telles quelles les fonctions
        # d'accès (insert_metric/get_history/... via le paramètre `table`). Les
        # colonnes disque restent nulles ici. Table séparée => accès direct sans
        # filtrer par `tag` (les métriques Mirth ne sont plus mêlées au système).
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS mirth_metrics (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp       TEXT    NOT NULL,
                tag             TEXT    NOT NULL DEFAULT 'mirth',
                cpu_percent     REAL,
                mem_percent     REAL,
                mem_used_gb     REAL,
                mem_total_gb    REAL,
                disk_percent    REAL,
                disk_used_gb    REAL,
                disk_total_gb   REAL,
                sockets         INTEGER,
                event           TEXT
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_mirth_metrics_ts ON mirth_metrics(timestamp)"
        )

        # Cache des MESSAGES EN ERREUR de Mirth (par connecteur). Sert uniquement de
        # cache de contenu : l'API Mirth reste prioritaire pour savoir QUELS messages
        # sont en erreur (cf. checker_service.cached_error_messages). On ne re-télécharge
        # que les messages absents du cache, identifiés par le triplet stable
        # (channel_id, message_id, meta_data_id). Le contenu d'un message est immuable
        # une fois capté => INSERT OR IGNORE.
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS mirth_messages (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                channel_id      TEXT    NOT NULL,
                channel_name    TEXT,
                message_id      INTEGER,
                meta_data_id    INTEGER,
                connector       TEXT,
                status          TEXT,
                received_date   TEXT,
                send_attempts   INTEGER,
                error_code      INTEGER,
                category        TEXT,
                error           TEXT,
                content         TEXT,
                cached_at       TEXT    NOT NULL,
                UNIQUE(channel_id, message_id, meta_data_id)
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_mirth_messages_channel "
            "ON mirth_messages(channel_id)"
        )

        # Historique de DÉBIT Mirth : à chaque relevé, les compteurs CUMULATIFS
        # (reçus/filtrés/en file/envoyés/erreurs) globaux (channel_id NULL) et par
        # canal. Le débit (msg/min) se calcule par delta entre points côté client.
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS mirth_throughput (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp     TEXT    NOT NULL,
                channel_id    TEXT,
                channel_name  TEXT,
                received      INTEGER,
                filtered      INTEGER,
                queued        INTEGER,
                sent          INTEGER,
                error         INTEGER
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_mirth_throughput_ts "
            "ON mirth_throughput(timestamp)"
        )

        # Second jeu de données temporel : les ÉVÈNEMENTS/ALERTES. Indépendant des
        # relevés `metrics`, il se superpose à TOUS les graphiques sous forme de
        # barres verticales colorées et légendées. `category` regroupe les types
        # (boot, service, mirth, alarm, cmd, network, mail, ...) ; `color` et
        # `label` pilotent l'affichage ; `source` indique l'origine (scheduler,
        # api, ...) ; `details` est un complément libre optionnel.
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS events (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp   TEXT    NOT NULL,
                category    TEXT    NOT NULL DEFAULT 'info',
                label       TEXT,
                color       TEXT,
                source      TEXT,
                details     TEXT
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_events_timestamp ON events(timestamp)"
        )
        conn.commit()
    finally:
        conn.close()
    return db_path


def insert_metric(sample, table="metrics", db_path=DEFAULT_DB_PATH):
    """
    Insère un échantillon de métriques dans `table` ('metrics' ou 'mirth_metrics').

    Args:
        sample (dict): clés attendues (toutes optionnelles sauf cohérence) :
            tag, cpu_percent, mem_percent, mem_used_gb, mem_total_gb,
            disk_percent, disk_used_gb, disk_total_gb, sockets.
            'tag' identifie la source du relevé ('system' par défaut, 'mirth', ...).
            Si 'timestamp' absent, l'horodatage courant est utilisé.
        table (str): table de relevés cible (liste blanche `_METRIC_TABLES`).
    """
    table = _check_table(table)
    ts = sample.get("timestamp") or datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    conn = _connect(db_path)
    try:
        conn.execute(
            f"""
            INSERT INTO {table}
                (timestamp, tag, cpu_percent, mem_percent, mem_used_gb, mem_total_gb,
                 disk_percent, disk_used_gb, disk_total_gb, sockets, event)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                ts,
                sample.get("tag") or "system",
                sample.get("cpu_percent"),
                sample.get("mem_percent"),
                sample.get("mem_used_gb"),
                sample.get("mem_total_gb"),
                sample.get("disk_percent"),
                sample.get("disk_used_gb"),
                sample.get("disk_total_gb"),
                sample.get("sockets"),
                sample.get("event"),
            ),
        )
        conn.commit()
    finally:
        conn.close()


def _normalize_bound(value, end=False):
    """Normalise une borne d'intervalle au format 'YYYY-MM-DD HH:MM:SS'.

    Une date seule ('YYYY-MM-DD') est étendue à la journée entière : début à
    00:00:00, fin (end=True) à 23:59:59. Toute autre chaîne est renvoyée telle
    quelle (après strip). Retourne None pour une valeur vide.
    """
    if not value:
        return None
    v = str(value).strip()
    if not v:
        return None
    # Date seule (10 caractères : YYYY-MM-DD) => borne de journée.
    if len(v) == 10:
        return v + (" 23:59:59" if end else " 00:00:00")
    return v


def get_history(hours=24, date_deb=None, date_fin=None, tag="system", limit=5000,
                table="metrics", db_path=DEFAULT_DB_PATH):
    """
    Retourne des échantillons triés du plus ancien au plus récent.

    Deux modes de sélection :
      - intervalle de dates : si `date_deb` et/ou `date_fin` sont fournis, filtre
        sur les bornes (incluses). Format accepté : 'YYYY-MM-DD' (étendu à la
        journée entière) ou 'YYYY-MM-DD HH:MM:SS'. Ce mode est prioritaire.
      - dernières heures : sinon, fenêtre des `hours` dernières heures
        (`hours` <= 0 => tout l'historique).

    Args:
        hours (float): fenêtre temporelle en heures (mode "dernières heures").
        date_deb (str|None): borne de début de l'intervalle.
        date_fin (str|None): borne de fin de l'intervalle.
        tag (str|None): filtre sur la source du relevé ('system' par défaut,
            'mirth', ...). None ou '' => toutes les sources confondues.
        limit (int): nombre maximum de lignes retournées.

    Returns:
        list[dict]: échantillons triés par horodatage croissant.
    """
    table = _check_table(table)
    deb = _normalize_bound(date_deb, end=False)
    fin = _normalize_bound(date_fin, end=True)
    tag = (tag or "").strip() or None

    conn = _connect(db_path)
    try:
        clauses, params = [], []
        if tag:
            clauses.append("tag = ?")
            params.append(tag)

        if deb or fin:
            if deb:
                clauses.append("timestamp >= ?")
                params.append(deb)
            if fin:
                clauses.append("timestamp <= ?")
                params.append(fin)
        elif hours and hours > 0:
            since = (
                datetime.datetime.now() - datetime.timedelta(hours=hours)
            ).strftime("%Y-%m-%d %H:%M:%S")
            clauses.append("timestamp >= ?")
            params.append(since)

        where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
        params.append(limit)
        cur = conn.execute(
            f"""
            SELECT * FROM (
                SELECT * FROM {table}
                {where}
                ORDER BY timestamp DESC
                LIMIT ?
            ) ORDER BY timestamp ASC
            """,
            params,
        )
        return [dict(row) for row in cur.fetchall()]
    finally:
        conn.close()


def get_latest(tag="system", table="metrics", db_path=DEFAULT_DB_PATH):
    """Retourne le dernier échantillon enregistré (pour `tag`), ou None si vide.

    `tag=None`/'' => pas de filtre de source (pratique pour une table dédiée comme
    'mirth_metrics' dont tous les relevés partagent la même source).
    """
    table = _check_table(table)
    tag = (tag or "").strip() or None
    conn = _connect(db_path)
    try:
        if tag:
            cur = conn.execute(
                f"SELECT * FROM {table} WHERE tag = ? ORDER BY id DESC LIMIT 1", (tag,)
            )
        else:
            cur = conn.execute(f"SELECT * FROM {table} ORDER BY id DESC LIMIT 1")
        row = cur.fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def get_last_valid(tag="system", table="metrics", db_path=DEFAULT_DB_PATH):
    """Retourne le dernier échantillon réel (relevé non nul) pour `tag`, en
    ignorant les marqueurs d'évènement (boot/restart). None si aucun relevé.

    `tag=None`/'' => pas de filtre de source (table dédiée mono-source)."""
    table = _check_table(table)
    tag = (tag or "").strip() or None
    conn = _connect(db_path)
    try:
        if tag:
            cur = conn.execute(
                f"SELECT * FROM {table} WHERE cpu_percent IS NOT NULL AND tag = ? "
                "ORDER BY id DESC LIMIT 1", (tag,)
            )
        else:
            cur = conn.execute(
                f"SELECT * FROM {table} WHERE cpu_percent IS NOT NULL "
                "ORDER BY id DESC LIMIT 1"
            )
        row = cur.fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def insert_event_marker(timestamp, event, tag="system", table="metrics",
                        db_path=DEFAULT_DB_PATH):
    """Insère un marqueur (métriques nulles) tagué `event` à l'horodatage donné.

    N'insère rien si un enregistrement portant déjà ce triplet (timestamp, event,
    tag) existe, afin d'éviter les doublons lors de redémarrages rapprochés.

    Returns:
        bool: True si un marqueur a effectivement été inséré.
    """
    table = _check_table(table)
    conn = _connect(db_path)
    try:
        cur = conn.execute(
            f"SELECT 1 FROM {table} WHERE timestamp = ? AND event = ? AND tag = ? LIMIT 1",
            (timestamp, event, tag),
        )
        if cur.fetchone():
            return False
        conn.execute(
            f"INSERT INTO {table} (timestamp, event, tag) VALUES (?, ?, ?)",
            (timestamp, event, tag),
        )
        conn.commit()
        return True
    finally:
        conn.close()


# ==========================================================================
# CACHE DES MESSAGES EN ERREUR MIRTH (table `mirth_messages`)
# Cache de CONTENU uniquement : l'API Mirth reste l'autorité sur les messages
# réellement en erreur. Clé stable = (channel_id, message_id, meta_data_id).
# ==========================================================================
# Colonnes (hors id/cached_at) renseignées lors d'un upsert de message.
_MSG_FIELDS = ("channel_id", "channel_name", "message_id", "meta_data_id",
               "connector", "status", "received_date", "send_attempts",
               "error_code", "category", "error", "content")


def get_cached_message_keys(channel_id=None, db_path=DEFAULT_DB_PATH):
    """Ensemble des clés (channel_id, message_id, meta_data_id) déjà en cache.

    Filtrable sur un canal. Sert à déterminer les messages à télécharger (ceux
    absents du cache) sans relire leur contenu.
    """
    conn = _connect(db_path)
    try:
        if channel_id:
            cur = conn.execute(
                "SELECT channel_id, message_id, meta_data_id FROM mirth_messages "
                "WHERE channel_id = ?", (channel_id,))
        else:
            cur = conn.execute(
                "SELECT channel_id, message_id, meta_data_id FROM mirth_messages")
        return {(r[0], r[1], r[2]) for r in cur.fetchall()}
    finally:
        conn.close()


def get_cached_messages(keys, db_path=DEFAULT_DB_PATH):
    """Retourne les messages en cache correspondant EXACTEMENT au jeu de `keys`.

    `keys` est un itérable de triplets (channel_id, message_id, meta_data_id). On
    lit par canal puis on filtre sur le jeu exact, afin que seuls les messages
    encore signalés en erreur par Mirth (les clés fournies) soient renvoyés —
    les entrées de cache obsolètes restent en base mais ne ressortent pas.
    """
    keys = list(keys)
    if not keys:
        return []
    channel_ids = {str(k[0]) for k in keys}
    keyset = {(str(k[0]), str(k[1]), str(k[2])) for k in keys}
    conn = _connect(db_path)
    try:
        placeholders = ",".join("?" * len(channel_ids))
        cur = conn.execute(
            f"SELECT * FROM mirth_messages WHERE channel_id IN ({placeholders})",
            tuple(channel_ids))
        out = []
        for r in cur.fetchall():
            d = dict(r)
            k = (str(d["channel_id"]), str(d["message_id"]), str(d["meta_data_id"]))
            if k in keyset:
                out.append(d)
        return out
    finally:
        conn.close()


def upsert_mirth_messages(items, db_path=DEFAULT_DB_PATH):
    """Insère en cache les messages fournis (INSERT OR IGNORE sur la clé stable).

    Le contenu d'un message étant immuable, un message déjà présent est ignoré.
    Retourne le nombre de lignes réellement insérées.
    """
    items = [it for it in items if it and it.get("channel_id") is not None]
    if not items:
        return 0
    now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    cols = ", ".join(_MSG_FIELDS) + ", cached_at"
    ph = ", ".join("?" * (len(_MSG_FIELDS) + 1))
    conn = _connect(db_path)
    try:
        inserted = 0
        for it in items:
            values = [it.get(f) for f in _MSG_FIELDS] + [now]
            cur = conn.execute(
                f"INSERT OR IGNORE INTO mirth_messages ({cols}) VALUES ({ph})", values)
            inserted += cur.rowcount
        conn.commit()
        return inserted
    finally:
        conn.close()


# ==========================================================================
# HISTORIQUE DE DÉBIT MIRTH (table `mirth_throughput`)
# Compteurs cumulatifs relevés périodiquement ; le débit se déduit par delta.
# ==========================================================================
def insert_throughput(rows, db_path=DEFAULT_DB_PATH):
    """Insère un lot de relevés de débit (globaux et/ou par canal).

    Chaque ligne est un dict {timestamp, channel_id (None=global), channel_name,
    received, filtered, queued, sent, error}. Retourne le nombre de lignes insérées.
    """
    rows = [r for r in rows if r]
    if not rows:
        return 0
    now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    conn = _connect(db_path)
    try:
        for r in rows:
            conn.execute(
                "INSERT INTO mirth_throughput "
                "(timestamp, channel_id, channel_name, received, filtered, queued, sent, error) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (r.get("timestamp") or now, r.get("channel_id"), r.get("channel_name"),
                 r.get("received"), r.get("filtered"), r.get("queued"),
                 r.get("sent"), r.get("error")),
            )
        conn.commit()
        return len(rows)
    finally:
        conn.close()


def get_throughput(hours=24, date_deb=None, date_fin=None, channel_id=None,
                   limit=20000, db_path=DEFAULT_DB_PATH):
    """Retourne les relevés de débit triés du plus ancien au plus récent.

    Mêmes modes de sélection que `get_history` (intervalle prioritaire, sinon
    dernières `hours` heures). `channel_id=None` => série GLOBALE (lignes dont
    channel_id est NULL) ; sinon, série du canal indiqué.
    """
    deb = _normalize_bound(date_deb, end=False)
    fin = _normalize_bound(date_fin, end=True)

    conn = _connect(db_path)
    try:
        clauses, params = [], []
        if channel_id:
            clauses.append("channel_id = ?")
            params.append(channel_id)
        else:
            clauses.append("channel_id IS NULL")

        if deb or fin:
            if deb:
                clauses.append("timestamp >= ?")
                params.append(deb)
            if fin:
                clauses.append("timestamp <= ?")
                params.append(fin)
        elif hours and hours > 0:
            since = (datetime.datetime.now()
                     - datetime.timedelta(hours=hours)).strftime("%Y-%m-%d %H:%M:%S")
            clauses.append("timestamp >= ?")
            params.append(since)

        where = "WHERE " + " AND ".join(clauses)
        params.append(limit)
        cur = conn.execute(
            f"""
            SELECT * FROM (
                SELECT * FROM mirth_throughput
                {where}
                ORDER BY timestamp DESC
                LIMIT ?
            ) ORDER BY timestamp ASC
            """,
            params,
        )
        return [dict(row) for row in cur.fetchall()]
    finally:
        conn.close()


# ==========================================================================
# ÉVÈNEMENTS / ALERTES (table `events`) — second jeu de données temporel
# ==========================================================================
def insert_event(timestamp=None, category="info", label=None, color=None,
                 source=None, details=None, dedup=False, db_path=DEFAULT_DB_PATH):
    """Enregistre un évènement/alerte (barre verticale superposable aux graphes).

    Args:
        timestamp (str|None): horodatage 'YYYY-MM-DD HH:MM:SS' ; courant si None.
        category (str): type d'évènement (boot, service, mirth, alarm, cmd, ...).
        label (str|None): texte affiché sur la barre.
        color (str|None): couleur de la barre (sinon dérivée côté présentation).
        source (str|None): origine (scheduler, api, ...).
        details (str|None): complément libre optionnel.
        dedup (bool): si True, n'insère pas un doublon (même timestamp+category+label).

    Returns:
        int|None: l'identifiant inséré, ou None si ignoré (dedup).
    """
    ts = timestamp or datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    conn = _connect(db_path)
    try:
        if dedup:
            cur = conn.execute(
                "SELECT 1 FROM events WHERE timestamp = ? AND category = ? "
                "AND COALESCE(label, '') = COALESCE(?, '') LIMIT 1",
                (ts, category, label),
            )
            if cur.fetchone():
                return None
        cur = conn.execute(
            "INSERT INTO events (timestamp, category, label, color, source, details) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (ts, category, label, color, source, details),
        )
        conn.commit()
        return cur.lastrowid
    finally:
        conn.close()


def get_events(hours=24, date_deb=None, date_fin=None, category=None, limit=2000,
               db_path=DEFAULT_DB_PATH):
    """Retourne les évènements/alertes, triés du plus ancien au plus récent.

    Mêmes modes de sélection que `get_history` : intervalle de dates (prioritaire)
    ou dernières `hours` heures (`hours` <= 0 => tout). Filtre optionnel par
    `category`.
    """
    deb = _normalize_bound(date_deb, end=False)
    fin = _normalize_bound(date_fin, end=True)
    category = (category or "").strip() or None

    conn = _connect(db_path)
    try:
        clauses, params = [], []
        if category:
            clauses.append("category = ?")
            params.append(category)
        if deb or fin:
            if deb:
                clauses.append("timestamp >= ?")
                params.append(deb)
            if fin:
                clauses.append("timestamp <= ?")
                params.append(fin)
        elif hours and hours > 0:
            since = (
                datetime.datetime.now() - datetime.timedelta(hours=hours)
            ).strftime("%Y-%m-%d %H:%M:%S")
            clauses.append("timestamp >= ?")
            params.append(since)

        where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
        params.append(limit)
        cur = conn.execute(
            f"""
            SELECT * FROM (
                SELECT * FROM events
                {where}
                ORDER BY timestamp DESC
                LIMIT ?
            ) ORDER BY timestamp ASC
            """,
            params,
        )
        return [dict(row) for row in cur.fetchall()]
    finally:
        conn.close()


def purge_older_than(days=30, db_path=DEFAULT_DB_PATH):
    """Supprime les échantillons plus vieux que `days` jours. Retourne le nombre supprimé."""
    cutoff = (datetime.datetime.now() - datetime.timedelta(days=days)).strftime(
        "%Y-%m-%d %H:%M:%S"
    )
    conn = _connect(db_path)
    try:
        cur = conn.execute("DELETE FROM metrics WHERE timestamp < ?", (cutoff,))
        # Mêmes rétentions pour les autres jeux de données horodatés par `timestamp`
        # (relevés Mirth, débit, évènements/alertes). Tables possiblement absentes
        # sur d'anciennes bases : on ignore l'erreur correspondante. (Le cache des
        # messages en erreur `mirth_messages` est purgé par sa propre colonne plus bas.)
        for tbl in ("mirth_metrics", "mirth_throughput", "events"):
            try:
                conn.execute(f"DELETE FROM {tbl} WHERE timestamp < ?", (cutoff,))
            except sqlite3.Error:
                pass
        # Cache des messages en erreur : rétention sur la date de réception du message.
        try:
            conn.execute("DELETE FROM mirth_messages WHERE received_date < ?", (cutoff,))
        except sqlite3.Error:
            pass
        conn.commit()
        return cur.rowcount
    finally:
        conn.close()


# ==========================================================================
# SUPERVISION & MAINTENANCE DE LA BASE
# ==========================================================================
def _sidecar_paths(db_path):
    """Chemins des fichiers annexes WAL/SHM associés à une base SQLite."""
    return db_path + "-wal", db_path + "-shm"


def _file_size(path):
    """Taille d'un fichier en octets (0 s'il n'existe pas)."""
    try:
        return os.path.getsize(path)
    except OSError:
        return 0


def checkpoint(db_path=DEFAULT_DB_PATH):
    """Bascule le contenu du journal WAL dans le fichier principal `.db`.

    À appeler avant de lire/copier le fichier brut afin qu'il soit autonome
    (le `-wal` est alors vidé). Sans effet si la base n'est pas en mode WAL.
    """
    with _FILE_LOCK:
        conn = _connect(db_path)
        try:
            conn.execute("PRAGMA wal_checkpoint(TRUNCATE);")
            conn.commit()
        finally:
            conn.close()


def get_db_stats(db_path=DEFAULT_DB_PATH):
    """Retourne un état détaillé de la base : taille, pages, fragmentation,
    mode de journalisation, tables et bornes temporelles des relevés.
    """
    exists = os.path.isfile(db_path)
    wal_path, shm_path = _sidecar_paths(db_path)

    stats = {
        "path": os.path.abspath(db_path),
        "exists": exists,
        "file_size_bytes": _file_size(db_path),
        "wal_size_bytes": _file_size(wal_path),
        "shm_size_bytes": _file_size(shm_path),
        "modified": None,
        "page_size": None,
        "page_count": None,
        "freelist_count": None,
        "free_bytes": 0,
        "fragmentation_percent": 0.0,
        "journal_mode": None,
        "schema_version": None,
        "tables": [],
        "total_rows": 0,
        "metrics_rows": 0,
        "metrics_valid": 0,
        "metrics_events": 0,
        "by_tag": [],
        "oldest": None,
        "newest": None,
    }

    if not exists:
        return stats

    try:
        stats["modified"] = datetime.datetime.fromtimestamp(
            os.path.getmtime(db_path)
        ).strftime("%Y-%m-%d %H:%M:%S")
    except OSError:
        pass

    conn = _connect(db_path)
    try:
        page_size = conn.execute("PRAGMA page_size").fetchone()[0]
        page_count = conn.execute("PRAGMA page_count").fetchone()[0]
        freelist = conn.execute("PRAGMA freelist_count").fetchone()[0]
        stats["page_size"] = page_size
        stats["page_count"] = page_count
        stats["freelist_count"] = freelist
        stats["free_bytes"] = (freelist or 0) * (page_size or 0)
        if page_count:
            stats["fragmentation_percent"] = round(freelist / page_count * 100, 2)
        stats["journal_mode"] = conn.execute("PRAGMA journal_mode").fetchone()[0]
        stats["schema_version"] = conn.execute("PRAGMA schema_version").fetchone()[0]

        # Liste des tables utilisateur + nombre de lignes de chacune.
        tables = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' "
            "AND name NOT LIKE 'sqlite_%' ORDER BY name"
        ).fetchall()
        total = 0
        for t in tables:
            name = t["name"]
            try:
                n = conn.execute(f'SELECT COUNT(*) FROM "{name}"').fetchone()[0]
            except sqlite3.Error:
                n = None
            total += n or 0
            stats["tables"].append({"name": name, "rows": n})
        stats["total_rows"] = total

        # Détails spécifiques à la table `metrics`.
        if any(t["name"] == "metrics" for t in tables):
            row = conn.execute(
                "SELECT COUNT(*) AS total, "
                "SUM(CASE WHEN cpu_percent IS NOT NULL THEN 1 ELSE 0 END) AS valid, "
                "MIN(timestamp) AS oldest, MAX(timestamp) AS newest "
                "FROM metrics"
            ).fetchone()
            stats["metrics_rows"] = row["total"] or 0
            stats["metrics_valid"] = row["valid"] or 0
            stats["metrics_events"] = (row["total"] or 0) - (row["valid"] or 0)
            stats["oldest"] = row["oldest"]
            stats["newest"] = row["newest"]

            # Répartition par source (tag) — facilite le suivi quand plusieurs
            # collecteurs ('system', 'mirth', ...) cohabitent dans la table.
            for r in conn.execute(
                "SELECT COALESCE(tag, 'system') AS tag, COUNT(*) AS rows, "
                "MIN(timestamp) AS oldest, MAX(timestamp) AS newest "
                "FROM metrics GROUP BY COALESCE(tag, 'system') ORDER BY tag"
            ).fetchall():
                stats["by_tag"].append({
                    "tag": r["tag"], "rows": r["rows"],
                    "oldest": r["oldest"], "newest": r["newest"],
                })
    finally:
        conn.close()

    return stats


def integrity_check(db_path=DEFAULT_DB_PATH):
    """Exécute `PRAGMA integrity_check` et `PRAGMA foreign_key_check`.

    Returns:
        dict: {"ok": bool, "integrity": [...], "foreign_keys": [...]}.
    """
    if not os.path.isfile(db_path):
        return {"ok": False, "integrity": ["base introuvable"], "foreign_keys": []}
    conn = _connect(db_path)
    try:
        integ = [r[0] for r in conn.execute("PRAGMA integrity_check").fetchall()]
        fk = [dict(zip(("table", "rowid", "parent", "fkid"), r))
              for r in conn.execute("PRAGMA foreign_key_check").fetchall()]
        ok = integ == ["ok"] and not fk
        return {"ok": ok, "integrity": integ, "foreign_keys": fk}
    finally:
        conn.close()


def vacuum(db_path=DEFAULT_DB_PATH):
    """Défragmente/compacte la base (VACUUM). Retourne les tailles avant/après."""
    with _FILE_LOCK:
        before = _file_size(db_path)
        conn = _connect(db_path)
        try:
            # Un checkpoint préalable évite un WAL résiduel après le VACUUM.
            conn.execute("PRAGMA wal_checkpoint(TRUNCATE);")
            conn.execute("VACUUM;")
            conn.commit()
        finally:
            conn.close()
        after = _file_size(db_path)
        return {
            "size_before_bytes": before,
            "size_after_bytes": after,
            "reclaimed_bytes": max(before - after, 0),
        }


def reset_db(db_path=DEFAULT_DB_PATH):
    """Vide tous les relevés et recompacte la base (table conservée).

    Returns:
        dict: nombre de lignes supprimées et tailles avant/après.
    """
    with _FILE_LOCK:
        before = _file_size(db_path)
        deleted = 0
        conn = _connect(db_path)
        try:
            try:
                deleted = conn.execute("SELECT COUNT(*) FROM metrics").fetchone()[0]
            except sqlite3.Error:
                deleted = 0
            conn.execute("DELETE FROM metrics")
            # Vide aussi les autres jeux de données (relevés Mirth, cache des
            # messages en erreur, débit, évènements/alertes).
            for tbl in ("mirth_metrics", "mirth_messages", "mirth_throughput", "events"):
                try:
                    conn.execute(f"DELETE FROM {tbl}")
                except sqlite3.Error:
                    pass
            # Remet les compteurs d'auto-incrément à zéro s'ils existent.
            try:
                conn.execute(
                    "DELETE FROM sqlite_sequence WHERE name IN "
                    "('metrics', 'mirth_metrics', 'mirth_messages', 'mirth_throughput', 'events')"
                )
            except sqlite3.Error:
                pass
            conn.commit()
            conn.execute("PRAGMA wal_checkpoint(TRUNCATE);")
            conn.execute("VACUUM;")
            conn.commit()
        finally:
            conn.close()
        # Garantit que le schéma est complet après l'opération.
        init_db(db_path)
        after = _file_size(db_path)
        return {
            "deleted_rows": deleted,
            "size_before_bytes": before,
            "size_after_bytes": after,
        }


def export_bytes(db_path=DEFAULT_DB_PATH):
    """Retourne le contenu binaire d'une copie cohérente de la base.

    Effectue d'abord un checkpoint WAL puis utilise l'API de sauvegarde SQLite
    (`Connection.backup`) pour produire un fichier `.db` autonome, sans risque de
    capturer une écriture concurrente à mi-chemin.
    """
    with _FILE_LOCK:
        checkpoint(db_path)
        src = _connect(db_path)
        tmp_fd, tmp_path = tempfile.mkstemp(suffix=".db")
        os.close(tmp_fd)
        try:
            dst = sqlite3.connect(tmp_path)
            try:
                src.backup(dst)
            finally:
                dst.close()
            with open(tmp_path, "rb") as f:
                return f.read()
        finally:
            src.close()
            try:
                os.remove(tmp_path)
            except OSError:
                pass


def export_csv(db_path=DEFAULT_DB_PATH):
    """Exporte toute la table `metrics` au format CSV (chaîne de caractères)."""
    conn = _connect(db_path)
    try:
        cur = conn.execute("SELECT * FROM metrics ORDER BY timestamp ASC, id ASC")
        cols = [d[0] for d in cur.description]
        buf = io.StringIO()
        writer = csv.writer(buf)
        writer.writerow(cols)
        for row in cur.fetchall():
            writer.writerow([row[c] for c in cols])
        return buf.getvalue()
    finally:
        conn.close()


def validate_sqlite(raw_bytes):
    """Vérifie qu'un binaire est une base SQLite valide contenant la table `metrics`.

    Returns:
        dict: {"valid": bool, "error": str|None, "metrics_rows": int|None}.
    """
    # En-tête magique d'un fichier SQLite 3.
    if not raw_bytes.startswith(b"SQLite format 3\x00"):
        return {"valid": False, "error": "En-tête SQLite invalide.", "metrics_rows": None}

    tmp_fd, tmp_path = tempfile.mkstemp(suffix=".db")
    os.close(tmp_fd)
    try:
        with open(tmp_path, "wb") as f:
            f.write(raw_bytes)
        conn = sqlite3.connect(tmp_path)
        try:
            integ = conn.execute("PRAGMA integrity_check").fetchone()[0]
            if integ != "ok":
                return {"valid": False, "error": f"Intégrité : {integ}",
                        "metrics_rows": None}
            has_metrics = conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='metrics'"
            ).fetchone()
            if not has_metrics:
                return {"valid": False, "error": "Table 'metrics' absente.",
                        "metrics_rows": None}
            n = conn.execute("SELECT COUNT(*) FROM metrics").fetchone()[0]
            return {"valid": True, "error": None, "metrics_rows": n}
        finally:
            conn.close()
    except sqlite3.Error as e:
        return {"valid": False, "error": str(e), "metrics_rows": None}
    finally:
        try:
            os.remove(tmp_path)
        except OSError:
            pass


def import_db(raw_bytes, db_path=DEFAULT_DB_PATH, backup=True):
    """Remplace la base courante par le binaire SQLite fourni.

    Le binaire est d'abord validé (en-tête, intégrité, présence de `metrics`).
    L'ancien fichier est sauvegardé en `.bak` avant remplacement (si `backup`).

    Returns:
        dict: {"ok": bool, "error": str|None, "metrics_rows": int|None,
               "backup_path": str|None}.

    Raises:
        ValueError: si le binaire fourni n'est pas une base valide.
    """
    check = validate_sqlite(raw_bytes)
    if not check["valid"]:
        raise ValueError(check["error"] or "Base SQLite invalide.")

    with _FILE_LOCK:
        # Vide le WAL de la base actuelle puis libère toute connexion résiduelle.
        try:
            checkpoint(db_path)
        except sqlite3.Error:
            pass

        wal_path, shm_path = _sidecar_paths(db_path)
        backup_path = None

        # Écrit d'abord le nouveau contenu dans un temporaire du même répertoire,
        # afin que `os.replace` soit atomique (même système de fichiers).
        dest_dir = os.path.dirname(os.path.abspath(db_path)) or "."
        tmp_fd, tmp_path = tempfile.mkstemp(suffix=".db", dir=dest_dir)
        os.close(tmp_fd)
        try:
            with open(tmp_path, "wb") as f:
                f.write(raw_bytes)

            if backup and os.path.isfile(db_path):
                backup_path = db_path + ".bak"
                shutil.copy2(db_path, backup_path)

            # Les fichiers annexes de l'ancienne base n'ont plus de sens.
            for sidecar in (wal_path, shm_path):
                try:
                    if os.path.isfile(sidecar):
                        os.remove(sidecar)
                except OSError:
                    pass

            os.replace(tmp_path, db_path)
            tmp_path = None  # consommé par os.replace
        finally:
            if tmp_path and os.path.isfile(tmp_path):
                try:
                    os.remove(tmp_path)
                except OSError:
                    pass

        # Réapplique le schéma (migrations) et le mode WAL sur la base importée.
        init_db(db_path)
        return {
            "ok": True,
            "error": None,
            "metrics_rows": check["metrics_rows"],
            "backup_path": backup_path,
        }
