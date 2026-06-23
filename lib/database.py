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
import json
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
    # Contraintes de clés étrangères (mirth_stats -> mirth_entity) : non actives
    # par défaut dans SQLite, on les active explicitement à chaque connexion.
    conn.execute("PRAGMA foreign_keys=ON;")
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

        # Historisation de l'OVERVIEW Mirth (remplace l'ancienne table
        # `mirth_throughput`, supprimée — pas de migration). Modèle relationnel
        # « dimension + faits » alimenté à chaque tick par le même appel
        # get_overview() : on conserve désormais l'état des canaux, le détail des
        # connecteurs, la version et les stats JVM (et plus seulement le débit).
        conn.execute("DROP TABLE IF EXISTS mirth_throughput")

        # Dimension : identité STABLE des canaux et de leurs connecteurs.
        # meta_data_id NULL = le canal lui-même ; 0 = connecteur source ;
        # 1+ = connecteurs de destination. `name` est rafraîchi si renommé.
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS mirth_entity (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                channel_id    TEXT    NOT NULL,
                meta_data_id  INTEGER,
                name          TEXT,
                UNIQUE(channel_id, meta_data_id)
            )
            """
        )

        # Faits temporels : une ligne par entité (canal + connecteurs) à chaque
        # relevé. Compteurs CUMULATIFS (le débit msg/min se déduit par delta côté
        # client). Reliée à la dimension par clé étrangère.
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS mirth_stats (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp   TEXT    NOT NULL,
                entity_id   INTEGER NOT NULL REFERENCES mirth_entity(id),
                state       TEXT,
                received    INTEGER,
                filtered    INTEGER,
                queued      INTEGER,
                sent        INTEGER,
                error       INTEGER
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_mirth_stats_entity_ts "
            "ON mirth_stats(entity_id, timestamp)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_mirth_stats_ts ON mirth_stats(timestamp)"
        )

        # Instantané SERVEUR par tick : totaux globaux + version + stats JVM
        # (JSON) + joignabilité. La dernière ligne joignable sert à reconstruire
        # la vue d'ensemble servie à la page (sans appel API live).
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS mirth_server (
                id                INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp         TEXT    NOT NULL,
                reachable         INTEGER,
                version           TEXT,
                channel_count     INTEGER,
                channels_started  INTEGER,
                received          INTEGER,
                filtered          INTEGER,
                queued            INTEGER,
                sent              INTEGER,
                error             INTEGER,
                system_stats      TEXT,
                error_text        TEXT
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_mirth_server_ts ON mirth_server(timestamp)"
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

        # Configuration des ALERTES (notifications sortantes). Indépendante des
        # relevés : ce sont des réglages persistants pilotés par la page
        # `alerte.html`, lus par checker_service au moment où une alarme survient.
        #
        #  - `alert_methods` : une ligne par canal de notification (email, mqtt,
        #    sms, slack, ...). `enabled` = méthode globalement active ; `recipient`
        #    = cible (ex. adresses e-mail séparées par des virgules) ; `config` =
        #    réglages additionnels au format JSON (réservé aux méthodes futures).
        #  - `alert_rules` : matrice OUI/NON (une ligne par couple alarme×méthode)
        #    indiquant si une alarme donnée doit être notifiée via une méthode.
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS alert_methods (
                method      TEXT    PRIMARY KEY,
                enabled     INTEGER NOT NULL DEFAULT 0,
                recipient   TEXT,
                config      TEXT,
                updated_at  TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS alert_rules (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                alarm_code  TEXT    NOT NULL,
                method      TEXT    NOT NULL,
                enabled     INTEGER NOT NULL DEFAULT 0,
                UNIQUE(alarm_code, method)
            )
            """
        )

        # Repère (baseline) des KPI cumulés de l'API Mirth (« Total reçus » /
        # « Total erreurs »). Les compteurs Mirth sont cumulatifs depuis le
        # démarrage du serveur ; cette table mémorise une photo des totaux à un
        # instant T pour que la page statistiques affiche l'écart (actuel − repère)
        # en gros et la valeur réelle en petit. Une seule ligne (id = 1), réécrite à
        # chaque clic sur le bouton « repère ». Réglage persistant (non purgé).
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS mirth_kpi_baseline (
                id          INTEGER PRIMARY KEY CHECK (id = 1),
                received    INTEGER,
                error       INTEGER,
                saved_at    TEXT
            )
            """
        )

        # ------------------------------------------------------------------
        # SUPERVISION DES PÉRIPHÉRIQUES (« clients Mirth ») — phase 3.
        # Un périphérique = une cible réseau UNIQUE (host, port) visée par un ou
        # plusieurs connecteurs Mirth. On ne teste QUE les couples ip/port et on
        # ne sonde jamais deux fois la même cible : la dimension est donc le couple
        # (host, port), pas le connecteur.
        #   - `device_status` : dernier état par cible (recap + référence d'alarme).
        #     `connectors` (JSON) liste les canaux/connecteurs qui visent cette cible.
        #   - `device_history` : agrégat par tick (total / en ligne / en erreur) pour
        #     le graphe, + `detail` (JSON des résultats par cible à ce tick) afin de
        #     pouvoir afficher le détail d'un point précis de la courbe (par horodatage).
        # ------------------------------------------------------------------
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS device_status (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                host          TEXT    NOT NULL,
                port          INTEGER,
                address       TEXT,
                transport     TEXT,
                icmp_ok       INTEGER,
                icmp_ms       REAL,
                tcp_ok        INTEGER,
                tcp_ms        REAL,
                reachable     INTEGER,
                tested        INTEGER,
                connectors    TEXT,
                last_change   TEXT,
                updated_at    TEXT,
                UNIQUE(host, port)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS device_history (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp   TEXT    NOT NULL,
                total       INTEGER,
                online      INTEGER,
                offline     INTEGER,
                detail      TEXT
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_device_history_ts ON device_history(timestamp)"
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


def get_recent_error_messages(limit=20, db_path=DEFAULT_DB_PATH):
    """Derniers messages en erreur mis en cache (aperçu, sans appel réseau).

    Sert à bâtir un corps d'alerte représentatif (ex. test de l'alarme
    `mirth_message_error` depuis la page). Le contenu peut être obsolète — c'est
    un simple échantillon du cache, l'API Mirth restant l'autorité sur l'état.
    """
    conn = _connect(db_path)
    try:
        cur = conn.execute(
            "SELECT * FROM mirth_messages "
            "ORDER BY received_date DESC, id DESC LIMIT ?", (limit,))
        out = []
        for r in cur.fetchall():
            d = dict(r)
            d.pop("cached_at", None)
            d.pop("id", None)
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
# HISTORISATION DE L'OVERVIEW MIRTH (tables `mirth_entity` / `mirth_stats` /
# `mirth_server`). Modèle « dimension + faits » : la dernière ligne serveur
# joignable + ses faits reconstruisent la vue d'ensemble servie à la page,
# sans appel API live. Compteurs cumulatifs ; le débit se déduit par delta.
# ==========================================================================
# Colonnes de compteurs partagées par les faits canal/connecteur et les totaux.
_STAT_FIELDS = ("received", "filtered", "queued", "sent", "error")


def _get_or_create_entity(conn, channel_id, meta_data_id, name):
    """Retourne l'id d'une entité (canal/connecteur), en la créant au besoin.

    Clé d'identité : (channel_id, meta_data_id). meta_data_id None = le canal.
    Rafraîchit `name` s'il a changé. (SELECT puis INSERT — pas d'UPSERT/RETURNING,
    pour rester compatible avec toutes les versions de SQLite embarquées.)
    """
    row = conn.execute(
        "SELECT id, name FROM mirth_entity WHERE channel_id IS ? AND meta_data_id IS ?",
        (channel_id, meta_data_id),
    ).fetchone()
    if row:
        if name is not None and row["name"] != name:
            conn.execute("UPDATE mirth_entity SET name = ? WHERE id = ?",
                         (name, row["id"]))
        return row["id"]
    cur = conn.execute(
        "INSERT INTO mirth_entity (channel_id, meta_data_id, name) VALUES (?, ?, ?)",
        (channel_id, meta_data_id, name),
    )
    return cur.lastrowid


def insert_mirth_snapshot(overview, timestamp=None, db_path=DEFAULT_DB_PATH):
    """Historise un instantané complet de l'overview Mirth (un seul tick).

    `overview` est la structure renvoyée par mirth_api.get_overview() :
    {reachable, error, version, system_stats, channels:[{channel_id, name, state,
    received…error, connectors:[{meta_data_id, name, state, received…error}]}],
    channel_count, channels_started, totals}.

    Écrit en une seule transaction : une ligne `mirth_server` (totaux + version +
    stats JVM + joignabilité), et — si joignable — une ligne `mirth_stats` par
    entité (canal + connecteurs), les entités étant créées/mises à jour dans
    `mirth_entity`. Si l'API est injoignable, seule la ligne serveur est écrite
    (reachable=0 + error_text) : la série se brise visiblement sur la coupure.
    """
    ts = timestamp or datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    reachable = bool(overview.get("reachable"))
    totals = overview.get("totals") or {}
    conn = _connect(db_path)
    try:
        conn.execute(
            "INSERT INTO mirth_server "
            "(timestamp, reachable, version, channel_count, channels_started, "
            " received, filtered, queued, sent, error, system_stats, error_text) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (ts, 1 if reachable else 0, overview.get("version"),
             overview.get("channel_count"), overview.get("channels_started"),
             totals.get("received"), totals.get("filtered"), totals.get("queued"),
             totals.get("sent"), totals.get("error"),
             json.dumps(overview.get("system_stats") or {}),
             None if reachable else overview.get("error")),
        )

        if reachable:
            for c in overview.get("channels", []):
                cid = c.get("channel_id")
                if cid is None:
                    continue
                # Le canal lui-même (meta_data_id NULL).
                eid = _get_or_create_entity(conn, cid, None, c.get("name"))
                _insert_stat_row(conn, ts, eid, c)
                # Ses connecteurs (source 0, destinations 1+).
                for conn_row in c.get("connectors", []):
                    mid = conn_row.get("meta_data_id")
                    ceid = _get_or_create_entity(conn, cid, mid, conn_row.get("name"))
                    _insert_stat_row(conn, ts, ceid, conn_row)
        conn.commit()
    finally:
        conn.close()


def _insert_stat_row(conn, ts, entity_id, src):
    """Insère une ligne de faits (`mirth_stats`) pour une entité donnée."""
    conn.execute(
        "INSERT INTO mirth_stats "
        "(timestamp, entity_id, state, received, filtered, queued, sent, error) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (ts, entity_id, src.get("state"), src.get("received"), src.get("filtered"),
         src.get("queued"), src.get("sent"), src.get("error")),
    )


def get_mirth_overview_latest(db_path=DEFAULT_DB_PATH):
    """Reconstruit la dernière vue d'ensemble Mirth historisée (sans appel API).

    S'appuie sur la dernière ligne `mirth_server` JOIGNABLE (reachable=1) et sur
    les `mirth_stats` de son horodatage, jointes à `mirth_entity`. Renvoie une
    structure compatible avec mirth_api.get_overview() (version, totaux, canaux
    avec leurs connecteurs et états) augmentée de `snapshot_at` (horodatage de
    l'instantané). Renvoie {reachable: False, snapshot_at: None, …} si aucun
    instantané joignable n'est encore disponible.
    """
    empty = {"version": None, "channel_count": 0, "channels_started": 0,
             "totals": {k: 0 for k in _STAT_FIELDS}, "channels": [],
             "system_stats": {}, "snapshot_at": None}
    conn = _connect(db_path)
    try:
        srv = conn.execute(
            "SELECT * FROM mirth_server WHERE reachable = 1 ORDER BY id DESC LIMIT 1"
        ).fetchone()
        if not srv:
            return empty

        ts = srv["timestamp"]
        rows = conn.execute(
            "SELECT e.channel_id AS channel_id, e.meta_data_id AS meta_data_id, "
            "       e.name AS name, s.state AS state, s.received AS received, "
            "       s.filtered AS filtered, s.queued AS queued, s.sent AS sent, "
            "       s.error AS error "
            "FROM mirth_stats s JOIN mirth_entity e ON e.id = s.entity_id "
            "WHERE s.timestamp = ?", (ts,),
        ).fetchall()
    finally:
        conn.close()

    # Regroupe les faits par canal : la ligne meta_data_id NULL est le canal,
    # les autres sont ses connecteurs.
    channels = {}
    connectors = {}
    for r in rows:
        cid = r["channel_id"]
        if r["meta_data_id"] is None:
            channels[cid] = {
                "channel_id": cid, "name": r["name"], "state": r["state"],
                **{k: r[k] for k in _STAT_FIELDS}, "connectors": [],
            }
        else:
            connectors.setdefault(cid, []).append({
                "meta_data_id": r["meta_data_id"], "name": r["name"],
                "state": r["state"], **{k: r[k] for k in _STAT_FIELDS},
            })
    for cid, conns in connectors.items():
        conns.sort(key=lambda c: (c["meta_data_id"] is None, c["meta_data_id"]))
        if cid in channels:
            channels[cid]["connectors"] = conns

    return {
        "version": srv["version"],
        "channel_count": srv["channel_count"],
        "channels_started": srv["channels_started"],
        "totals": {k: srv[k] for k in _STAT_FIELDS},
        "channels": sorted(channels.values(),
                           key=lambda c: (c.get("name") or "").lower()),
        "system_stats": json.loads(srv["system_stats"] or "{}"),
        "snapshot_at": ts,
    }


def get_mirth_server_latest(db_path=DEFAULT_DB_PATH):
    """Dernière ligne `mirth_server` historisée, QUEL QUE SOIT son état.

    Contrairement à `get_mirth_overview_latest` (qui ne retient que le dernier
    instantané JOIGNABLE pour reconstruire les canaux), cette fonction renvoie la
    toute dernière relève du collecteur de fond afin de refléter la joignabilité
    « courante » (reachable/version/error) sans aucun appel réseau. Renvoie None
    si aucune relève n'existe encore.
    """
    conn = _connect(db_path)
    try:
        row = conn.execute(
            "SELECT * FROM mirth_server ORDER BY id DESC LIMIT 1"
        ).fetchone()
    finally:
        conn.close()
    if not row:
        return None
    return {
        "timestamp": row["timestamp"],
        "reachable": bool(row["reachable"]),
        "version": row["version"],
        "error": row["error_text"],
    }


def get_mirth_series(hours=24, date_deb=None, date_fin=None, channel_id=None,
                     meta_data_id=None, limit=20000, db_path=DEFAULT_DB_PATH):
    """Série temporelle de compteurs Mirth, du plus ancien au plus récent.

    Mêmes modes de sélection que `get_history` (intervalle prioritaire, sinon
    dernières `hours` heures). Sélection de la série :
      - channel_id None  => série GLOBALE (totaux de `mirth_server`) ;
      - channel_id + meta_data_id None => série du canal (`mirth_stats`) ;
      - channel_id + meta_data_id      => série d'un connecteur précis.

    Chaque point : {timestamp, received, filtered, queued, sent, error}.
    """
    deb = _normalize_bound(date_deb, end=False)
    fin = _normalize_bound(date_fin, end=True)

    def _time_clause(prefix=""):
        clauses, params = [], []
        if deb or fin:
            if deb:
                clauses.append(f"{prefix}timestamp >= ?")
                params.append(deb)
            if fin:
                clauses.append(f"{prefix}timestamp <= ?")
                params.append(fin)
        elif hours and hours > 0:
            since = (datetime.datetime.now()
                     - datetime.timedelta(hours=hours)).strftime("%Y-%m-%d %H:%M:%S")
            clauses.append(f"{prefix}timestamp >= ?")
            params.append(since)
        return clauses, params

    cols = ", ".join(_STAT_FIELDS)
    conn = _connect(db_path)
    try:
        if not channel_id:
            # Série globale : totaux de la table serveur (relevés joignables).
            clauses, params = _time_clause()
            clauses.append("reachable = 1")
            where = "WHERE " + " AND ".join(clauses)
            params.append(limit)
            cur = conn.execute(
                f"SELECT * FROM (SELECT timestamp, {cols} FROM mirth_server "
                f"{where} ORDER BY timestamp DESC LIMIT ?) ORDER BY timestamp ASC",
                params,
            )
        else:
            # Série d'une entité (canal ou connecteur) via la dimension.
            clauses, params = _time_clause("s.")
            clauses.append("e.channel_id IS ?")
            params.append(channel_id)
            clauses.append("e.meta_data_id IS ?")
            params.append(meta_data_id)
            where = "WHERE " + " AND ".join(clauses)
            params.append(limit)
            cur = conn.execute(
                f"SELECT * FROM (SELECT s.timestamp AS timestamp, "
                f"{', '.join('s.' + f for f in _STAT_FIELDS)} "
                f"FROM mirth_stats s JOIN mirth_entity e ON e.id = s.entity_id "
                f"{where} ORDER BY s.timestamp DESC LIMIT ?) ORDER BY timestamp ASC",
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


# ==========================================================================
# SUPERVISION DES PÉRIPHÉRIQUES (tables `device_status` / `device_history`)
# Un périphérique = une cible réseau UNIQUE (host, port). `device_status` garde
# le dernier état de chaque cible (recap + référence d'alarme) ; `device_history`
# trace l'agrégat par tick (en ligne / en erreur) + le détail de chaque tick.
# ==========================================================================
def _to_bool(v):
    """Convertit une valeur SQLite (0/1/NULL) en bool, en conservant None."""
    return None if v is None else bool(v)


def upsert_device_status(rows, timestamp=None, db_path=DEFAULT_DB_PATH):
    """Met à jour l'état courant de chaque cible (host, port), un upsert par cible.

    `rows` est une liste de résultats de sonde — chacun {host, port, address,
    transport, icmp_ok, icmp_ms, tcp_ok, tcp_ms, reachable, tested, connectors}.
    `connectors` (liste des canaux/connecteurs visant la cible) est sérialisé en
    JSON. `last_change` n'est mis à jour que si la joignabilité (`reachable`) a
    changé depuis le dernier relevé (sinon la valeur précédente est conservée),
    comme `save_alert_config` : SELECT-puis-INSERT/UPDATE (compat. vieilles SQLite).
    """
    now = timestamp or datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    conn = _connect(db_path)
    try:
        for r in rows or []:
            host = r.get("host")
            if not host:
                continue
            port = r.get("port")
            reachable = None if r.get("reachable") is None else (1 if r.get("reachable") else 0)
            icmp_ok = None if r.get("icmp_ok") is None else (1 if r.get("icmp_ok") else 0)
            tcp_ok = None if r.get("tcp_ok") is None else (1 if r.get("tcp_ok") else 0)
            tested = 1 if r.get("tested") else 0
            connectors = json.dumps(r.get("connectors") or [])
            existing = conn.execute(
                "SELECT id, reachable, last_change FROM device_status "
                "WHERE host = ? AND port IS ?", (host, port)).fetchone()
            if existing:
                # last_change conservé tant que la joignabilité ne change pas.
                last_change = existing["last_change"]
                if existing["reachable"] != reachable:
                    last_change = now
                conn.execute(
                    "UPDATE device_status SET address=?, transport=?, icmp_ok=?, "
                    "icmp_ms=?, tcp_ok=?, tcp_ms=?, reachable=?, tested=?, "
                    "connectors=?, last_change=?, updated_at=? WHERE id=?",
                    (r.get("address"), r.get("transport"), icmp_ok, r.get("icmp_ms"),
                     tcp_ok, r.get("tcp_ms"), reachable, tested, connectors,
                     last_change, now, existing["id"]))
            else:
                conn.execute(
                    "INSERT INTO device_status (host, port, address, transport, "
                    "icmp_ok, icmp_ms, tcp_ok, tcp_ms, reachable, tested, connectors, "
                    "last_change, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (host, port, r.get("address"), r.get("transport"), icmp_ok,
                     r.get("icmp_ms"), tcp_ok, r.get("tcp_ms"), reachable, tested,
                     connectors, now, now))
        conn.commit()
    finally:
        conn.close()


def _device_row_to_dict(r):
    """Normalise une ligne `device_status` en dict JSON (bools + connecteurs)."""
    d = dict(r)
    for k in ("reachable", "icmp_ok", "tcp_ok", "tested"):
        d[k] = _to_bool(d.get(k))
    try:
        d["connectors"] = json.loads(d.get("connectors") or "[]")
    except (ValueError, TypeError):
        d["connectors"] = []
    d.pop("id", None)
    return d


def get_device_status(db_path=DEFAULT_DB_PATH):
    """Dernier état connu de chaque cible (host, port), trié par hôte/port."""
    conn = _connect(db_path)
    try:
        cur = conn.execute(
            "SELECT * FROM device_status ORDER BY host, port")
        return [_device_row_to_dict(r) for r in cur.fetchall()]
    finally:
        conn.close()


def insert_device_history(timestamp=None, total=0, online=0, offline=0,
                          detail=None, db_path=DEFAULT_DB_PATH):
    """Enregistre l'agrégat d'un tick de sonde + le détail par cible (JSON).

    `detail` est la liste des résultats par cible (host, port, états...) : elle
    permet d'afficher le détail d'un point précis de la courbe via son horodatage
    (cf. get_device_history_at). Une ligne par tick.
    """
    ts = timestamp or datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    conn = _connect(db_path)
    try:
        conn.execute(
            "INSERT INTO device_history (timestamp, total, online, offline, detail) "
            "VALUES (?, ?, ?, ?, ?)",
            (ts, total, online, offline, json.dumps(detail or [])))
        conn.commit()
    finally:
        conn.close()


def get_device_history(hours=24, date_deb=None, date_fin=None, limit=20000,
                       db_path=DEFAULT_DB_PATH):
    """Série temporelle agrégée (sans le détail) pour le graphe « clients Mirth ».

    Mêmes modes de sélection que `get_history` (intervalle prioritaire, sinon
    dernières `hours` heures). Chaque point : {timestamp, total, online, offline}.
    """
    deb = _normalize_bound(date_deb, end=False)
    fin = _normalize_bound(date_fin, end=True)
    conn = _connect(db_path)
    try:
        clauses, params = [], []
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
        where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
        params.append(limit)
        cur = conn.execute(
            f"SELECT * FROM (SELECT timestamp, total, online, offline FROM device_history "
            f"{where} ORDER BY timestamp DESC LIMIT ?) ORDER BY timestamp ASC", params)
        return [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()


def get_device_history_at(timestamp, db_path=DEFAULT_DB_PATH):
    """Détail (par cible) du tick de sonde à l'horodatage donné.

    Sert au clic sur un point de la courbe : renvoie {timestamp, total, online,
    offline, devices:[...]} pour ce relevé précis (dernier si plusieurs partagent
    l'horodatage). devices vide si aucun relevé à cet horodatage.
    """
    conn = _connect(db_path)
    try:
        row = conn.execute(
            "SELECT * FROM device_history WHERE timestamp = ? ORDER BY id DESC LIMIT 1",
            (timestamp,)).fetchone()
    finally:
        conn.close()
    if not row:
        return {"timestamp": timestamp, "total": 0, "online": 0, "offline": 0,
                "devices": []}
    try:
        devices = json.loads(row["detail"] or "[]")
    except (ValueError, TypeError):
        devices = []
    return {"timestamp": row["timestamp"], "total": row["total"],
            "online": row["online"], "offline": row["offline"], "devices": devices}


# ==========================================================================
# CONFIGURATION DES ALERTES (tables `alert_methods` / `alert_rules`)
# Réglages persistants de la notification sortante : quelles alarmes notifier,
# par quelle(s) méthode(s), et vers quel destinataire. Lus par checker_service
# au moment où une alarme survient ; écrits par la page `alerte.html`.
# ==========================================================================
def get_alert_methods(db_path=DEFAULT_DB_PATH):
    """Retourne la configuration des méthodes de notification.

    Returns:
        dict[str, dict]: {method: {"enabled": bool, "recipient": str|None,
        "config": dict}}. `config` est désérialisé depuis sa colonne JSON.
    """
    conn = _connect(db_path)
    try:
        out = {}
        for r in conn.execute("SELECT * FROM alert_methods"):
            try:
                cfg = json.loads(r["config"]) if r["config"] else {}
            except (ValueError, TypeError):
                cfg = {}
            out[r["method"]] = {
                "enabled": bool(r["enabled"]),
                "recipient": r["recipient"],
                "config": cfg,
            }
        return out
    finally:
        conn.close()


def get_alert_rules(db_path=DEFAULT_DB_PATH):
    """Retourne la matrice alarme×méthode sous forme {alarm_code: {method: bool}}."""
    conn = _connect(db_path)
    try:
        out = {}
        for r in conn.execute("SELECT alarm_code, method, enabled FROM alert_rules"):
            out.setdefault(r["alarm_code"], {})[r["method"]] = bool(r["enabled"])
        return out
    finally:
        conn.close()


def get_alert_config(db_path=DEFAULT_DB_PATH):
    """Vue complète de la configuration des alertes : {methods, rules}."""
    return {"methods": get_alert_methods(db_path=db_path),
            "rules": get_alert_rules(db_path=db_path)}


def save_alert_config(methods=None, rules=None, db_path=DEFAULT_DB_PATH):
    """Enregistre (remplace) la configuration des alertes en une transaction.

    Args:
        methods (dict|None): {method: {"enabled": bool, "recipient": str,
            "config": dict}}. Chaque méthode fournie est insérée/mise à jour
            (UPSERT). Les méthodes non fournies sont laissées intactes.
        rules (dict|None): {alarm_code: {method: bool}}. Chaque couple fourni est
            inséré/mis à jour. Les couples non fournis sont laissés intacts.
    """
    now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    conn = _connect(db_path)
    try:
        # SELECT-puis-INSERT/UPDATE plutôt qu'un UPSERT `ON CONFLICT`, par souci de
        # compatibilité avec les SQLite embarquées anciennes (cf. _get_or_create_entity).
        for method, m in (methods or {}).items():
            enabled = 1 if m.get("enabled") else 0
            cfg = json.dumps(m.get("config") or {})
            cur = conn.execute(
                "UPDATE alert_methods SET enabled=?, recipient=?, config=?, updated_at=? "
                "WHERE method=?", (enabled, m.get("recipient"), cfg, now, method))
            if cur.rowcount == 0:
                conn.execute(
                    "INSERT INTO alert_methods (method, enabled, recipient, config, updated_at) "
                    "VALUES (?, ?, ?, ?, ?)",
                    (method, enabled, m.get("recipient"), cfg, now))
        for alarm_code, by_method in (rules or {}).items():
            for method, enabled in by_method.items():
                val = 1 if enabled else 0
                cur = conn.execute(
                    "UPDATE alert_rules SET enabled=? WHERE alarm_code=? AND method=?",
                    (val, alarm_code, method))
                if cur.rowcount == 0:
                    conn.execute(
                        "INSERT INTO alert_rules (alarm_code, method, enabled) VALUES (?, ?, ?)",
                        (alarm_code, method, val))
        conn.commit()
    finally:
        conn.close()


# ==========================================================================
# REPÈRE DES KPI CUMULÉS MIRTH (table `mirth_kpi_baseline`)
# Photo des totaux « reçus » / « erreurs » à un instant T, pour afficher l'écart
# sur la page statistiques. Une seule ligne (id = 1) ; réglage persistant.
# ==========================================================================
def get_kpi_baseline(db_path=DEFAULT_DB_PATH):
    """Retourne le repère KPI enregistré, ou None s'il n'a jamais été fixé.

    Returns:
        dict|None: {"received": int, "error": int, "saved_at": str} ou None.
    """
    conn = _connect(db_path)
    try:
        row = conn.execute(
            "SELECT received, error, saved_at FROM mirth_kpi_baseline WHERE id = 1"
        ).fetchone()
    finally:
        conn.close()
    if not row:
        return None
    return {"received": row["received"], "error": row["error"],
            "saved_at": row["saved_at"]}


def set_kpi_baseline(received, error, timestamp=None, db_path=DEFAULT_DB_PATH):
    """Mémorise (remplace) le repère des KPI cumulés Mirth.

    Args:
        received (int): total cumulé des messages reçus à mémoriser.
        error (int): total cumulé des erreurs à mémoriser.
        timestamp (str|None): horodatage du repère (défaut : maintenant).

    Returns:
        dict: le repère enregistré ({received, error, saved_at}).
    """
    now = timestamp or datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    conn = _connect(db_path)
    try:
        cur = conn.execute(
            "UPDATE mirth_kpi_baseline SET received=?, error=?, saved_at=? WHERE id = 1",
            (received, error, now))
        if cur.rowcount == 0:
            conn.execute(
                "INSERT INTO mirth_kpi_baseline (id, received, error, saved_at) "
                "VALUES (1, ?, ?, ?)", (received, error, now))
        conn.commit()
    finally:
        conn.close()
    return {"received": received, "error": error, "saved_at": now}


def purge_older_than(days=30, db_path=DEFAULT_DB_PATH):
    """Supprime les relevés plus vieux que `days` jours dans toutes les tables
    horodatées. Retourne le nombre TOTAL de lignes supprimées (toutes tables)."""
    cutoff = (datetime.datetime.now() - datetime.timedelta(days=days)).strftime(
        "%Y-%m-%d %H:%M:%S"
    )
    conn = _connect(db_path)
    try:
        deleted = 0
        cur = conn.execute("DELETE FROM metrics WHERE timestamp < ?", (cutoff,))
        deleted += cur.rowcount or 0
        # Mêmes rétentions pour les autres jeux de données horodatés par `timestamp`
        # (relevés Mirth, overview historisé, évènements/alertes, connectivité). Tables
        # possiblement absentes sur d'anciennes bases : on ignore l'erreur. La dimension
        # `mirth_entity` (stable, petite) et les tables de config/état courant
        # (`device_status`, `alert_*`) ne sont jamais purgées. (Le cache des messages en
        # erreur `mirth_messages` est purgé par sa propre colonne plus bas.)
        for tbl in ("mirth_metrics", "mirth_stats", "mirth_server", "events",
                    "device_history"):
            try:
                cur = conn.execute(f"DELETE FROM {tbl} WHERE timestamp < ?", (cutoff,))
                deleted += cur.rowcount or 0
            except sqlite3.Error:
                pass
        # Cache des messages en erreur : rétention sur la date de réception du message.
        try:
            cur = conn.execute("DELETE FROM mirth_messages WHERE received_date < ?", (cutoff,))
            deleted += cur.rowcount or 0
        except sqlite3.Error:
            pass
        conn.commit()
        return deleted   # total de lignes supprimées, toutes tables confondues
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
            # messages en erreur, overview historisé, évènements/alertes). Les
            # faits (`mirth_stats`) avant la dimension (`mirth_entity`) à cause de
            # la clé étrangère.
            for tbl in ("mirth_metrics", "mirth_messages", "mirth_stats",
                        "mirth_server", "mirth_entity", "events",
                        "device_status", "device_history"):
                try:
                    conn.execute(f"DELETE FROM {tbl}")
                except sqlite3.Error:
                    pass
            # Remet les compteurs d'auto-incrément à zéro s'ils existent.
            try:
                conn.execute(
                    "DELETE FROM sqlite_sequence WHERE name IN "
                    "('metrics', 'mirth_metrics', 'mirth_messages', 'mirth_stats', "
                    "'mirth_server', 'mirth_entity', 'events', "
                    "'device_status', 'device_history')"
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
