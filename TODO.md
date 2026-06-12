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

## Session Mirth durable et partagée (collecteur + API)

Aujourd'hui chaque accès Mirth refait un cycle **complet** `login() → requêtes →
logout()` (`mirth_api._with_client`, `mirth_api.py:658`) : une session jetable par
appel, donc le coût d'authentification (lent sur le vrai serveur, ~7 s) est payé à
chaque fois.

### Objectif

Maintenir **au maximum une session Mirth durable** (un seul `MirthClient` /
`JSESSIONID`, `mirth_api.py:114`) réutilisée :

- par la **tâche collecteur** (`scheduled_mirth_overview`, toutes les minutes) ;
- par les **routes API** qui auraient encore besoin d'un appel live (repli du
  premier démarrage, sondes ponctuelles, etc.).

On ne se reconnecte que **si nécessaire** : « session paresseuse » avec **re-login
automatique + 1 retry** sur échec d'authentification (401/403) ou erreur de
connexion — c'est ce qui couvre les **redémarrages de Mirth** et les coupures
réseau (cf. les 3 cas : timeout d'inactivité, restart Mirth, reset TCP). Ne PAS
faire `logout()` entre deux usages (cela invalide la session).

### Contrainte de concurrence — IMPÉRATIF

Le `MirthClient` / son `CookieJar` **n'est pas thread-safe**, et le collecteur
(thread daemon) et les routes web (threads du `ThreadingHTTPServer`) peuvent y
accéder en parallèle. Il faut donc **protéger la session par un mutex**
(`threading.Lock`/`RLock`) : tout appel attend que la session soit **libre** avant
de l'utiliser, exécute sa série de requêtes, puis relâche le verrou. Le re-login
sur échec doit aussi se faire sous le même verrou (éviter deux re-logins
concurrents).

> Depuis le point 1, les routes web ne touchent plus Mirth (tout vient de SQLite) :
> le collecteur est en pratique le seul consommateur réseau, ce qui rend ce
> partage verrouillé d'autant plus simple à mettre en place proprement.
