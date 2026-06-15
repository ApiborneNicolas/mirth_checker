# TODO

## Performance — `/api/mirth/process` (~3,2 s sous Windows)

`api_mirth_process` → `probe_mirth_process()` (`checker_service.py:406`) est lent
parce qu'il énumère **deux fois** toutes les sockets de la machine, ce qui est
coûteux sous Windows (`psutil.net_connections()` interroge l'API IpHlp pour TOUT
le système) :

- `system_state.get_socket(proc_name)` → `psutil.net_connections(kind='inet')` (`system_state.py:408`)
- `system_state.get_active_connections(proc_name)` → `psutil.net_connections(kind='tcp')` (`system_state.py:172`)
- + `get_processes_info()` impose un `time.sleep(0.1)` pour la mesure CPU process (`system_state.py:366`)

### Optimisation à faire

1. **Une seule énumération de sockets.** Appeler `psutil.net_connections(kind='inet')`
   **une fois**, puis filtrer en mémoire les sockets `LISTEN` + `ESTABLISHED` du/des
   PID Mirth, au lieu des deux appels système séparés. → coût socket divisé par 2.
2. **Évaluer la suppression du `sleep(0.1)`** sur ce chemin si la précision du CPU
   process n'est pas critique pour les cartes (l'historique CPU vient déjà du
   collecteur de fond, pas de cette route live).

Gain attendu : ~3,2 s → ~1,2 s.

> Note : contrairement à `/api/mirth/api` (déjà servi depuis SQLite, plus aucun
> login Mirth synchrone), cette route reste un instantané **live** psutil — il n'y
> a pas d'optimisation possible côté DB, c'est psutil qui est lent sous Windows.

## ~~Session Mirth durable et partagée (collecteur + API)~~ — FAIT

Implémenté dans `mirth_api.py` : un unique `MirthClient` / `JSESSIONID` partagé
(`_shared_client`) réutilisé par le collecteur et les routes web, sérialisé par
`_SESSION_LOCK` (RLock). « Session paresseuse » via `_ensure_session()` (sonde
`ping` validant une session réutilisée) + auto-relogin **+ 1 retry** dans
`MirthClient._request` (couvre timeout d'inactivité, redémarrage Mirth, reset
TCP — 401/403 ou erreur de connexion). Plus aucun `logout()` entre deux usages ;
`close_session()` ferme proprement la session (arrêt du service / CLI one-shot).
