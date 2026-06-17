# Plan — Supervision des « périphériques » Mirth (config des connecteurs + ping/port)

> **État d'avancement — ✅ Phases 1, 2 & 3 OPÉRATIONNELLES.**
> - **Phase 1** (lecture/parsing de la config des connecteurs) : **livrée et vérifiée**
>   (`mirth_api.get_connector_endpoints`, rapport + CLI `-s endpoints`, simulateur servant `GET /channels`).
> - **Phase 2** (vérif. temps réel à la demande) : **livrée et vérifiée** (`system_state.check_tcp_port`,
>   `checker_service.probe_endpoints`, routes `/api/mirth/endpoints` + `/api/devices/probe`,
>   bouton **🛰️ Connectivité** + modale dans `statistiques.html`).
> - **Phase 3** (historisation + onglet + alarmes paramétrables) : **livrée et vérifiée**, avec
>   deux écarts validés par rapport au plan initial ci-dessous :
>   1. L'UI n'est **pas** une page séparée `clients.html` mais un **nouvel onglet « 🛰️ Mirth Dest »**
>      (2ᵉ position) dans `statistiques.html` : KPI + graphe 2 courbes (connexions **actives** vs
>      **en erreur**) + barres d'évènements + tableau récapitulatif des derniers tests ; un clic sur
>      un point du graphe affiche le détail de CE test (par horodatage).
>   2. L'entité supervisée est la **cible réseau unique `(host, port)`** (et non le connecteur) :
>      on **ne teste que les couples ip/port** et **jamais deux fois la même IP** (ICMP dédupliqué par
>      hôte, port TCP par couple). Tables `device_status` (clé `(host, port)`) + `device_history`
>      (agrégat + `detail` JSON par tick). Ping de fond à **timeout < 1 s** (`run_ping(timeout=…)`).
>      Collecteur `device-ping-collector` (4ᵉ tâche de `start_staggered`), ligne d'état console enrichie,
>      alarmes `device_unreachable`/`device_up` (catégorie `network`), routes `/api/devices`,
>      `/api/devices/history`, `/api/devices/history/at` (toutes documentées dans `api.html`).

## Contexte

Aujourd'hui le projet ne lit de Mirth que les **statistiques** des canaux/connecteurs
(`/channels/statuses` + `/channels/statistics`). Le besoin : récupérer la
**configuration** des connecteurs (IP destinataire, port, mode/transport) pour
identifier les équipements distants, puis tester s'ils sont en ligne — d'abord à
la demande, ensuite en continu avec historisation et alarmes.

Faisabilité confirmée : l'API REST Mirth expose la config complète via
`GET /channels` — chaque canal porte `sourceConnector` + `destinationConnectors`,
et chaque connecteur a un `transportName` (le « mode ») et un bloc `properties`
contenant l'IP/port pour les transports réseau (TCP/MLLP/HTTP/DICOM…). Les
connecteurs non-réseau (Database/File/Channel/JS) n'ont pas d'IP pingable et
seront marqués comme tels.

Choix validés :
1. **Périmètre** : destinations **et** sources (connecteurs réseau host/port).
2. **Méthode de test** : **ICMP (ping) ET test du port TCP réel** (le port TCP
   est le signal fiable de joignabilité applicative ; l'ICMP reste indicatif et
   peut être bloqué par le pare-feu Windows / exiger des privilèges).
3. **Surveillance** : à la demande (phase 2) **puis** surveillance de fond +
   historisation + alarme paramétrable « périphérique injoignable » (phase 3).

## Réutilisation (existant à exploiter, ne pas réécrire)

- `system_state.run_ping(ip)` (ping3) — déjà là ; route `/api/ping?host=` (`api_ping`).
- Parsing défensif façon `_parse_statistics`/`_parse_connectors` dans `mirth_api.py`
  (helpers `_as_list`, `_coerce_int`) — le parseur de config suit le même style.
- Système d'alarmes **data-driven** : `ALARM_CATALOG` + `ALARM_BY_CODE` + `emit_alarm`
  + `dispatch_alerts`/`_build_alarm_context` (`checker_service.py`). Ajouter une
  entrée au catalogue suffit pour que `alerte.html` affiche la nouvelle alarme
  (matrice + bouton Test) **sans éditer la page**.
- `EVENT_COLORS` contient **déjà** `"network": "#2dd4bf"` (turquoise) — réutiliser
  la catégorie `network` pour les barres d'évènement ; **aucun ajout** à `EVENT_COLORS`.
- Pattern collecteur de fond : `RecurringTask` + `start_staggered` (`main()`),
  détection par diff d'un set en mémoire (`_mirth_error_keys_prev`,
  re-baseline silencieuse au 1er tick / après redémarrage).
- Pattern table « dernier état » + config SELECT-then-UPSERT (`save_alert_config`,
  `get_mirth_overview_latest`). Pattern table série temporelle + graphe
  (`metrics`/`mirth_metrics` + `buildChart`).
- Boilerplate page : `theme.css`/`theme.js`, nav `.brand`+`<nav>` (cf. `alerte.html`).

---

## Vue d'ensemble des 3 phases

| Phase | Statut | Livrable visible | Couche | Dépend de |
|-------|--------|------------------|--------|-----------|
| **1 — Lecture de config** | ✅ **Fait** | Liste des périphériques dans le rapport (`mirth_api`) | Données / parsing | — |
| **2 — Vérif. temps réel** | ✅ **Fait** | Modale « connectivité » (ping + port à la demande) | API live + UI | Phase 1 |
| **3 — Historisation & alarmes** | ⏳ **À faire** | Onglet « Clients Mirth » (graphe + alarmes) | Collecteur + DB + alertes | Phases 1 & 2 |

**Fil conducteur** : la phase 1 définit le **contrat d'endpoint**
(`get_connector_endpoints()` → liste de `{channel, connecteur, host, port,
transport, pingable…}`) que **toutes** les phases suivantes consomment. La phase 2
ajoute les **primitives de sonde** (`check_tcp_port`, `probe_endpoints`) qui sont
ensuite **réutilisées telles quelles** par le collecteur de fond de la phase 3.
Rien en phase 1 ni 2 n'écrit en base ni ne déclenche d'alarme : ces deux aspects
sont concentrés en phase 3, ce qui garde les phases indépendantes et testables.

---

## ✅ PHASE 1 — Lecture & parsing de la configuration des connecteurs (`mirth_api.py`) — **RÉALISÉE**

**Objectif** : exposer une fonction qui liste/identifie les périphériques distants
à partir de la config des connecteurs, et **agrémenter le rapport** existant avec
cette liste. Aucune dépendance web/DB/ping — c'est la fondation des phases 2 et 3.

> **Réalisé** : `MirthClient.get_channels_config_raw()` + parseur défensif
> (`_extract_host_port`/`_split_url`/`_looks_like_host`/`_connector_endpoint`/
> `_parse_channel_endpoints`/`_parse_channels_config`) + **`get_connector_endpoints()`**.
> Rapport enrichi (`build_full_report` → clé `endpoints`) et CLI **`-s endpoints`**.
> `mirth_simulator.py` sert désormais `GET /channels` avec des profils de test
> (MLLP/TCP, HTTP, DICOM, non-réseau). Vérifié de bout en bout contre le simulateur.

### Fichier : `mirth_api.py`
- `MirthClient.get_channels_config_raw()` : `GET /channels` (JSON, définitions
  complètes). Une seule requête, config peu changeante.
- `_parse_channel_endpoints(channel)` (défensif, dans le style de `_parse_connectors`
  l.421) :
  - source = `sourceConnector` ; destinations = `destinationConnectors.connector`
    (`_as_list`) ; chaque connecteur : `metaDataId`, `name`, `transportName`,
    `enabled`, `properties` (`@class`).
  - Extraction host/port par type, avec repli générique :
    - Sender TCP/MLLP/LLP (`*Dispatcher*`) → `properties.remoteAddress` + `remotePort`.
    - Sender HTTP / WebService → URL (`host`/`wsdlUrl`/`locationURI`) → `urllib.parse`.
    - Sender DICOM → `host` + `port`.
    - Listener source (Receiver) → `properties.listenerConnectorProperties.host`+`port`
      (adresse d'écoute ; `0.0.0.0`/vide ⇒ hôte local du serveur).
    - Repli : scanner `properties` pour `remoteAddress|host|address` + `remotePort|port`
      et `listenerConnectorProperties` → robustesse inter-versions.
    - Aucun host trouvé ⇒ `kind="non-réseau"`, `pingable=False`.
- **`get_connector_endpoints(timeout=8)`** — point d'entrée **qui ne lève jamais**
  (renvoie `{reachable: False, error}` en cas d'échec, comme les autres `get_*`).
  Renvoie le **contrat d'endpoint** partagé :
  ```
  {reachable, error, base_url, count,
   endpoints: [{channel_id, channel_name, meta_data_id, name, role,
                transport, kind, host, port, address, pingable, enabled}]}
  ```

### Agrément du rapport (le « pour agrémenter le rapport » demandé)
- Brancher la liste d'endpoints dans **`build_full_report()`** (l.1101) — nouvelle
  section « Périphériques / endpoints » à côté de server/channels/connectors/errors.
- Côté CLI (`python mirth_api.py`) : ajouter `endpoints` aux sections `-s`
  (tableau `tabulate` : Canal · Connecteur · Rôle · Transport · Hôte · Port ·
  Pingable). `--json` le dump déjà via `build_full_report`.

### Vérification phase 1 — ✅ faite (contre le simulateur)
```
python -c "import mirth_api,json;print(json.dumps(mirth_api.get_connector_endpoints(),indent=2,default=str))"
```
`python mirth_api.py -s endpoints` affiche le tableau. Validé contre `mirth_simulator.py`
(8 endpoints, tous les cas TCP/HTTP/DICOM/listener/non-réseau).
**Reste optionnel** : rejouer la même commande contre le **vrai Mirth** (`localhost:8443`)
quand il tourne, pour confirmer host/port/transport sur les connecteurs réels et
ajuster le parseur si des `@class`/champs diffèrent (il n'était pas démarré lors du dev).

---

## ✅ PHASE 2 — Vérification temps réel à la demande (sonde + modale) — **RÉALISÉE**

**Objectif** : une **modale** (déclenchée depuis `statistiques.html`) qui liste les
périphériques (via la phase 1) et teste **en temps réel** leur connectivité
(ICMP + port TCP), sans rien persister ni alarmer. Introduit les primitives de
sonde que la phase 3 réutilisera.

> **Réalisé** : `system_state.check_tcp_port()`, `checker_service.probe_endpoints()`
> (pure, sans DB), routes **`/api/mirth/endpoints`** + **`/api/devices/probe`**, et
> dans `statistiques.html` un bouton **🛰️ Connectivité** à côté de « 📡 Canaux Mirth
> (API REST) » ouvrant la modale (tableau + badges en ligne/hors ligne/non-réseau,
> filtre, boutons ⟳ Rescanner / ↻ Tester). Vérifié (unitaire, routes, HTTP bout-en-bout).
> Le profil 1 du simulateur vise `127.0.0.1:8443` → un périphérique « en ligne » de démo.

### Fichier : `system_state.py` — sonde de port TCP (style `run_ping`, l.321)
- **`check_tcp_port(host, port, timeout=2.0)`** : `socket.create_connection` →
  latence ms si le port accepte la connexion, sinon `None`. Ne lève pas.

### Fichier : `checker_service.py` — primitive de balayage + routes
- **`probe_endpoints(endpoints)`** : pour chaque hôte unique → `run_ping` (ICMP) ;
  pour chaque `(host, port)` réseau → `check_tcp_port`. `reachable` global =
  port TCP (signal réel de l'endpoint), ICMP en complément. Timeouts courts,
  **dédup des hôtes** (un device = un `(host,port)`, même si plusieurs connecteurs
  le visent). Renvoie une liste de résultats frais. **Pure** : pas d'écriture DB.
- **Routes** (`build_router`) :
  - `GET /api/mirth/endpoints` (`api_mirth_endpoints`) → `get_connector_endpoints`
    (liste live de la config, pour (re)scanner).
  - `GET /api/devices/probe` (`api_devices_probe`) → `get_connector_endpoints`
    puis `probe_endpoints`, renvoi des résultats frais (bouton « Tester maintenant »).
    En phase 2, **pas d'upsert** (la persistance arrive en phase 3).

### Fichier : `web/statistiques.html` — modale « Connectivité des périphériques »
- Bouton/entrée ouvrant une modale (réutiliser le pattern de la modale d'erreurs
  existante). Chargement via `/api/mirth/endpoints`, bouton « Tester maintenant »
  (`/api/devices/probe`).
- Tableau : Canal · Connecteur · Rôle · Mode (transport) · Hôte · Port ·
  ICMP (ms/✗) · Port TCP (ouvert/fermé) · **État** (badge en ligne/hors ligne).
  Connecteurs non-réseau grisés (« non-pingable »). Bandeau résumé
  (N en ligne / M hors ligne) + filtre texte.

### Vérification phase 2 — ✅ faite
```
python -c "import system_state;print(system_state.check_tcp_port('127.0.0.1',8443))"
```
Validé : `check_tcp_port` (port ouvert → latence ms, fermé → None), `probe_endpoints`
(le port TCP pilote l'état ; listeners 0.0.0.0 / non-réseau non testés), routes
résolues et **test HTTP bout-en-bout** (`/api/mirth/endpoints`, `/api/devices/probe`,
page statique servant le bouton + la modale). En usage réel : démarrer
`python checker_service.py`, ouvrir `statistiques.html`, cliquer **🛰️ Connectivité**.

---

## ⏳ PHASE 3 — Historisation, onglet « Clients Mirth » & alarmes paramétrables — **SEULE PHASE RESTANTE**

**Objectif** : un collecteur de fond historise l'état des périphériques ; un
nouvel onglet **« Clients Mirth »** visualise le **nombre de clients en ligne au
fil du temps** ; une **alarme paramétrable** depuis `alerte.html` se déclenche
quand un périphérique devient injoignable.

> Tout l'amont est prêt : le **contrat d'endpoint** (`get_connector_endpoints`,
> phase 1) et la **primitive de sonde** (`probe_endpoints`, phase 2) sont à
> réutiliser **tels quels**. Il ne reste qu'à brancher persistance + collecteur +
> UI + alarmes ci-dessous.

### Fichier : `lib/database.py` — état courant + série temporelle
- `init_db` — deux tables :
  - **`device_status`** (dernier état par connecteur, `UNIQUE(channel_id,
    meta_data_id)`) : `channel_id/channel_name, meta_data_id, connector_name,
    role, transport, host, port, kind, icmp_ok, icmp_ms, tcp_ok, tcp_ms,
    reachable, last_change, updated_at`. Sert la baseline d'alarme + l'état instantané.
  - **`device_history`** (série temporelle agrégée par tick, pour le graphe) :
    `ts, total, online, offline` (1 ligne/tick). Alimente l'onglet « clients en
    ligne au fil du temps ». (Option : `reachable` NULL = marqueur d'indispo,
    pour casser la ligne du graphe, comme les event markers de `metrics`.)
- Accesseurs : `upsert_device_status(rows)` (SELECT-then-INSERT/UPDATE comme
  `save_alert_config` ; conserve `last_change` si `reachable` inchangé),
  `get_device_status()` (toutes les lignes courantes), `insert_device_history(...)`,
  `get_device_history(hours=…|date_deb=…)` (style `get_history`).
- `reset_db` : ajouter la suppression de `device_status` + `device_history`.
  (Tables « état/série » : `device_history` entre dans `purge_older_than` ;
  `device_status` reste hors purge comme les tables de config.)

### Fichier : `checker_service.py` — collecteur de fond + alarmes
- **`ALARM_CATALOG`** : ajouter deux entrées (même schéma que les existantes :
  `code/title/event_label/category/severity/default_email/default_mqtt`) :
  - `device_unreachable` — catégorie **`network`** (turquoise déjà dispo),
    severity `critical`, `default_email=True`.
  - `device_up` (retour en ligne) — catégorie `network`, severity `info`/`warning`.
  > L'ajout au catalogue suffit : `alerte.html` affiche automatiquement les 2
  > nouvelles lignes (matrice OUI/NON + bouton Test). **Aucune** édition de page,
  > **aucun** ajout à `EVENT_COLORS` (catégorie `network` déjà présente l.121).
- **`_build_alarm_context`** : branche `device_unreachable`/`device_up` → attacher
  la liste des périphériques concernés (lecture locale `get_device_status`, **no
  network**), écrasée par le `context` exact de l'alarme réelle (comme `error_messages`).
- **`scheduled_device_check()`** (nouveau collecteur, pattern `scheduled_mirth_overview`) :
  endpoints en cache module (`get_connector_endpoints`, rafraîchis tous les N ticks),
  `probe_endpoints` (réutilisé de la phase 2), `upsert_device_status` +
  `insert_device_history`, puis diff vs `_device_state_prev` (set en mémoire des
  endpoints injoignables ; **1er tick / post-restart = baseline silencieuse**) →
  `emit_alarm("device_unreachable", detail, context={devices:[…]})` agrégé (un
  évènement/tick), et `emit_alarm("device_up", …)` pour les retours en ligne.
  Clé de diff = `(host, port)` (un device, pas un connecteur) → pas de doublon.
- **`main()`** : ajouter
  `RecurringTask(args.interval, scheduled_device_check, name="device-ping-collector")`
  à la liste passée à `start_staggered` (apparaît alors dans `/api/status`).
- **Routes** :
  - `GET /api/devices` (`api_devices`) → `get_device_status` (dernier état, instantané ;
    bascule la modale phase 2 sur l'historique au lieu du live à chaque ouverture).
  - `GET /api/devices/history` (`api_devices_history`) → `get_device_history`
    (série temporelle du nombre de clients en ligne, pour le graphe).

### Fichier : `web/clients.html` — nouvel onglet « Clients Mirth »
- Boilerplate identique aux autres pages (`theme.css`/`theme.js`, anti-FOUC, nav).
- **Graphe** clients en ligne au fil du temps (réutiliser `buildChart` +
  `eventLinesPlugin` ; sélecteur de période `#range`) alimenté par
  `/api/devices/history` — avec barres turquoise `network` aux évènements
  `device_unreachable`/`device_up`.
- Tableau « dernier état » via `/api/devices`, auto-refresh (`date_deb=last`).
- **Nav** : ajouter `🛰️ Clients Mirth → clients.html` dans la nav de
  `statistiques.html`, `database.html`, `alerte.html`, `api.html` et la page elle-même.

### Vérification phase 3
1. Démarrer `python checker_service.py` → `device-ping-collector` apparaît dans
   `/api/status` ; `device_status`/`device_history` se peuplent ; `clients.html`
   trace la courbe.
2. Pointer une destination vers un hôte injoignable (ou couper un service cible)
   → au tick suivant : évènement `device_unreachable` (barre turquoise sur les
   graphes), ligne d'alarme auto-affichée dans `alerte.html` testable (bouton Test).
3. Remettre l'hôte en ligne → `device_up` au tick suivant.

---

## Notes techniques transverses
- Le **test du port TCP** est le signal fiable de joignabilité applicative → il
  pilote l'`État` global ; l'ICMP (ping3) reste **indicatif** (peut être bloqué
  par le pare-feu Windows / exiger des privilèges).
- L'alarme est clé-ée par **endpoint `(host,port)`** (un device, pas un connecteur)
  pour éviter les doublons quand plusieurs connecteurs visent le même hôte.
- Console cp1252-safe (ASCII `->`, pas de `→`) pour les prints du service.
- Les phases 1 et 2 (livrées) n'écrivent **rien** en base et ne déclenchent
  **aucune** alarme : ces responsabilités restent isolées en phase 3 → les phases
  1 et 2 ont pu être livrées/testées sans toucher au schéma DB ni au système d'alertes.
