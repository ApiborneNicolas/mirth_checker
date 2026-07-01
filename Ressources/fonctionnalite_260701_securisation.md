# Sécurisation de Mirth_checker (service web + superviseur)

## Contexte

Le service `checker_service.py` et le nouveau superviseur `superviseur.py` exposent
aujourd'hui une API **totalement ouverte, en HTTP nu, sans aucune authentification** :
n'importe quel client sur le réseau peut lire toutes les métriques, la config des
alertes, l'état Mirth, etc. Tant que ces services tournent en réseau privé/VPN, le
risque est faible — mais certains serveurs sont **directement exposés sur Internet**,
et le superviseur est appelé à piloter plusieurs serveurs à distance.

On ajoute donc quatre couches de sécurité, **activables et faciles à déployer sur un
`.exe` Windows** :

1. **HTTPS** automatique hors localhost (certificat auto-signé généré au 1er lancement,
   ou certificat fourni).
2. **Filtre IP** (liste blanche CIDR) configurable dans `.mirth_config.py`.
3. **Comptes web** (id/mdp en base) avec **session 24 h glissante**.
4. **Accès API par jeton** : on ne fait transiter le login/mdp (ou une clé API) **qu'une
   fois**, puis tous les échanges suivants portent un **jeton de session temporaire**
   (24 h glissantes). C'est le même mécanisme qui protège l'UI web et le lien
   superviseur↔service.

**Décisions déjà arbitrées avec l'utilisateur :** socle API = *jeton de session sur
HTTPS* (pas de HMAC ni challenge-response pour l'instant) ; certificat =
*auto-signé + option certificat fourni* ; périmètre = *les 4 points d'un bloc*.

Principe directeur : **HTTPS est la vraie frontière anti-interception** ; le jeton de
session est la commodité qui évite de re-saisir/renvoyer le mot de passe. Tout tient en
**stdlib** (`secrets`, `hashlib`, `hmac`, `ssl`, `ipaddress`, `http.cookies`), la seule
dépendance ajoutée (`cryptography`) ne servant qu'à **générer** le certificat auto-signé.

---

## Architecture : défense en profondeur

Cinq couches, évaluées dans cet ordre au tout début de `_dispatch()` (le point de
passage unique de toute requête, `lib/webserver.py:180`) :

| # | Couche | Comportement | Emplacement |
|---|--------|--------------|-------------|
| 0 | **localhost de confiance** | `127.0.0.1` / `::1` → **jamais** bloqués par IP/auth, HTTPS non exigé. Garantit l'accès interne permanent du service et le bootstrap admin. Les collecteurs de fond sont *in-process* (pas de HTTP), donc non concernés. | hook sécurité |
| 1 | **Filtre IP (CIDR)** | Refuse `403` si l'IP client n'est dans aucun réseau autorisé. Liste vide = tout autorisé (rétro-compatible). | hook sécurité |
| 2 | **HTTPS/TLS** | Enveloppe le socket serveur. Auto hors localhost. | `serve()` |
| 3 | **Session (comptes)** | Requiert un jeton de session valide sur `/api/*` (sauf `/api/auth/login`). `401` sinon. Fenêtre 24 h glissante. | hook sécurité |
| 4 | **Jeton pour le superviseur** | Le superviseur s'authentifie **une fois** par site (clé API ou id/mdp), reçoit un jeton, le réutilise 24 h, se ré-authentifie sur `401`. Calqué sur `mirth_api._ensure_session`. | `superviseur.py` |

### Modules partagés (nouveaux, dans `lib/`) — réutilisés par les DEUX services

- **`lib/tls.py`** — `ensure_self_signed_cert(cert_path, key_path)` (génère un certificat
  auto-signé via `cryptography` si absent) + `build_ssl_context(cert, key)` (stdlib
  `ssl.SSLContext`). Le serveur en cours d'exécution n'utilise que `ssl` (stdlib).
- **`lib/auth.py`** — cœur de l'authentification, sans I/O réseau :
  - `hash_password(pwd) -> str` / `verify_password(pwd, stored) -> bool` via
    `hashlib.pbkdf2_hmac('sha256', …, salt, 200_000)` + `secrets.token_bytes(16)` de sel,
    comparaison `hmac.compare_digest`. Format stocké : `pbkdf2$iters$salt_hex$hash_hex`.
  - `new_token() -> str` (`secrets.token_urlsafe(32)`) et `token_fingerprint(tok)`
    (SHA-256 hex — **seul le hash est stocké** en base).
  - `create_session(username)`, `resolve_session(raw_token)` (valide + coulisse la
    fenêtre 24 h), `revoke_session(raw_token)` — s'appuient sur `lib/database.py`.
  - `verify_api_key(raw_key)` (compare au hash stocké dans `api_keys`).
- **`lib/security.py`** — `SecurityPolicy` : objet unique passé à `webserver.serve()`.
  Porte la liste blanche CIDR (parsée via `ipaddress.ip_network`), le contexte TLS, et la
  logique du hook `check(handler, path) -> Response|None` qui compose couches 0/1/3.
  Fonction `load_security_config()` : lit les nouvelles clés depuis `.mirth_config.py`
  (même chargeur `importlib` que `mirth_api.get_config()`, `mirth_api.py:82`) puis
  applique la précédence **env > fichier > arguments CLI > défauts**.

Cette factorisation dans `lib/` est essentielle : `checker_service.py` **et**
`superviseur.py` protègent leur propre UI avec exactement le même code.

---

## Configuration & secrets

Les nouvelles clés vont dans **`.mirth_config.py`** (comme demandé pour les points 1–2 ;
git-ignoré) et sont documentées dans `.mirth_config.py.template`. Précédence identique à
l'existant : **variables d'environnement > `.mirth_config.py` > arguments CLI > défauts**.

```python
# --- Sécurité (nouvelles clés .mirth_config.py) ---
HTTPS_MODE      = "auto"          # "auto" | "on" | "off"  (auto = HTTPS si bind ≠ loopback)
HTTPS_CERT      = ""              # chemin cert.pem (vide → auto-signé généré à côté de l'exe)
HTTPS_KEY       = ""              # chemin key.pem
ALLOWED_IPS     = ["127.0.0.1", "::1", "192.168.0.0/16"]   # [] = tout autorisé
AUTH_ENABLED    = True            # False → API ouverte (rétro-compatible / réseau de confiance)
SESSION_TTL_H   = 24              # durée de la session glissante (heures)
```

Arguments CLI équivalents ajoutés à `main()` (`checker_service.py` ~2564 et
`superviseur.py` ~431) : `--https {auto,on,off}`, `--cert`, `--key`, `--allow-ips`
(CSV), `--auth {on,off}`. Les args surchargent le fichier, l'env surcharge tout.

Fichiers cert par défaut : `checker_cert.pem` / `checker_key.pem` dans le dossier de
l'exe (résolu comme `DEFAULT_DB_PATH`, `lib/database.py:25`), donc **persistants** entre
lancements d'un `.exe`.

---

## Point 1 — HTTPS activable et facile à déployer

**`lib/tls.py`** (nouveau). `ensure_self_signed_cert()` génère, si les fichiers manquent,
un certificat auto-signé (CN = hostname, SAN incluant l'IP/hostname, validité ~10 ans)
via `cryptography.x509`. `build_ssl_context()` renvoie un `ssl.SSLContext` chargé.

**`lib/webserver.py`** — `serve()` (ligne 234) reçoit un `security=None` :

```python
def serve(router, host="0.0.0.0", port=8800, security=None):
    handler_class = _build_handler_class(router, security)
    httpd = _ExclusiveHTTPServer((host, port), handler_class)
    httpd.daemon_threads = True
    if security and security.tls_context:
        httpd.socket = security.tls_context.wrap_socket(httpd.socket, server_side=True)
    return httpd
```

**Logique `auto`** (dans `load_security_config` / `main`) : HTTPS activé si
`HTTPS_MODE == "on"`, ou si `"auto"` et `host` ∉ {`127.0.0.1`, `::1`, `localhost`}
(donc `0.0.0.0` et toute IP externe → HTTPS). Si HTTPS requis mais `cryptography` absent
**et** aucun cert fourni → message clair au démarrage (« fournissez `HTTPS_CERT`/`HTTPS_KEY`
ou installez `cryptography` ») et arrêt, plutôt qu'un fallback HTTP silencieux.

`cryptography` ajouté à `requirements.txt` (seule nouvelle dépendance). Fallback
documenté : un `_cmd_helper/gen_cert.bat` (openssl) pour générer le cert hors-ligne si on
ne veut pas embarquer `cryptography`.

## Point 2 — Filtre IP (liste blanche CIDR)

Dans **`lib/security.py`**, `SecurityPolicy` pré-parse `ALLOWED_IPS` en
`[ipaddress.ip_network(x, strict=False)]`. Le hook, tout en haut de `_dispatch`, fait :

```python
ip = ipaddress.ip_address(handler.client_address[0])
if not self.networks:            # liste vide → tout autorisé (rétro-compatible)
    pass
elif ip.is_loopback or any(ip in net for net in self.networks):
    pass
else:
    return Response('{"error":"forbidden"}', status=403,
                    content_type="application/json")
```

`loopback` toujours autorisé (couche 0). Support IPv4 + IPv6 + CIDR gratuit via
`ipaddress`. Optionnel : lire `X-Forwarded-For` seulement si un `--trust-proxy` explicite
est posé (sinon on ignore, pour ne pas être contournable par en-tête forgé).

## Point 3 — Comptes web + session 24 h glissante

### Base de données (`lib/database.py`)

Trois tables ajoutées dans `init_db()` (motif `CREATE TABLE IF NOT EXISTS`, comme
`alert_methods`/`alert_rules`). **Tables de config → jamais purgées** par
`purge_older_than()` ; **`web_sessions` est nettoyée par une tâche dédiée** (pas par le
purge d'historique). Aucune n'est vidée par `reset_db()` (préserver les comptes).

```sql
CREATE TABLE IF NOT EXISTS users (
    username      TEXT PRIMARY KEY,
    email         TEXT NOT NULL,          -- destinataire du mdp généré
    password_hash TEXT NOT NULL,          -- pbkdf2$iters$salt$hash (jamais en clair, jamais affiché)
    role          TEXT NOT NULL DEFAULT 'technicien',   -- 'admin' | 'technicien'
    enabled       INTEGER NOT NULL DEFAULT 1,
    created_at    TEXT NOT NULL,
    last_login_at TEXT,                   -- MAJ à chaque login réussi
    last_seen_at  TEXT,                   -- MAJ (throttlée) à chaque requête authentifiée
    updated_at    TEXT
);
CREATE TABLE IF NOT EXISTS web_sessions (
    token_hash    TEXT PRIMARY KEY,       -- SHA-256 du jeton (jamais le jeton en clair)
    username      TEXT NOT NULL,
    created_at    TEXT NOT NULL,
    expires_at    TEXT NOT NULL,          -- glissé à now+TTL à chaque requête valide
    last_seen_ip  TEXT
);
CREATE TABLE IF NOT EXISTS api_keys (
    key_hash      TEXT PRIMARY KEY,       -- SHA-256 de la clé (jamais la clé en clair)
    label         TEXT,
    enabled       INTEGER NOT NULL DEFAULT 1,
    created_at    TEXT NOT NULL,
    last_used_at  TEXT
);
```

Accesseurs suivant le style existant (`_connect`, `sqlite3.Row`, `finally: conn.close()`,
UPSERT en SELECT-puis-INSERT/UPDATE comme `save_alert_config`, `lib/database.py:1243`) :
`create_user(username, email, password_hash, role)` / `get_user` /
`list_users` (renvoie username/email/role/enabled/`last_login_at`/`last_seen_at`, **jamais**
le hash) / `set_password(username, password_hash)` / `set_user_enabled(username, bool)` /
`delete_user` / `touch_user_login(username)` / `touch_user_seen(username)` ;
`insert_session` / `get_session(token_hash)` / `touch_session(token_hash, expires_at)` /
`delete_session` / `purge_expired_sessions()` ; `insert_api_key` / `get_api_key(key_hash)` /
`list_api_keys` / `delete_api_key`.

### Routes d'authentification (`checker_service.py`, dans `build_router` ~2481)

- `POST /api/auth/login` `{username, password}` → si OK : crée une session, renvoie
  `{ok:true, token, expires_at}` **et** pose le cookie
  `Set-Cookie: mc_session=<token>; HttpOnly; SameSite=Strict; Path=/; Secure` (`Secure`
  seulement en HTTPS). Le corps porte le jeton pour les clients non-navigateur
  (superviseur). `401` si échec.
- `POST /api/auth/logout` → révoque la session (cookie + Bearer).
- `GET  /api/auth/whoami` → `{username, role, expires_at}` (le front sait s'il est
  connecté et s'il est admin — pour afficher/masquer l'onglet « Comptes »).

### Hook de session (couche 3, dans le hook `SecurityPolicy.check`)

Après IP OK et si `AUTH_ENABLED` : pour toute route `/api/*` **sauf** `/api/auth/login`,
extraire le jeton depuis le cookie `mc_session` (`http.cookies.SimpleCookie` sur
`handler.headers.get("Cookie")`) **ou** l'en-tête `Authorization: Bearer <token>`, puis
`auth.resolve_session()` :
- introuvable / expiré → `Response(401)` ;
- valide → **coulisse** `expires_at = now + TTL` (via `touch_session`, avec throttle : on
  ne réécrit en base que si la dernière glissée date de > 5 min, pour éviter une écriture
  SQLite par requête) et laisse passer.

**Choix de périmètre :** on protège les **données** (`/api/*`), pas les fichiers statiques.
Les pages `.html`/`.css`/`.js` ne contiennent aucun secret ; c'est le `web/auth.js`
côté client qui, sur un `401`, redirige vers `login.html`. Cela réduit l'exemption à un
seul point (`/api/auth/login`) et laisse tout le reste de `/api/*` protégé.

### Gestion des comptes (admin) — mot de passe généré, jamais visible

Principe : **l'admin ne connaît jamais le mot de passe.** À la création (ou au
renouvellement), le serveur **génère un mot de passe aléatoire complexe**, en stocke
uniquement le hash, et l'**envoie par e-mail** au titulaire du compte. La valeur en clair
n'est ni renvoyée à l'admin ni consultable ensuite.

- **Génération** : `auth.generate_password()` — ~16 caractères tirés via `secrets.choice`
  d'un alphabet mixte (majuscules/minuscules/chiffres/symboles), en garantissant au moins
  un de chaque classe.
- **Envoi e-mail** : réutilise `quickmail.sendmail(subject, message, dest, html=…)`
  (déjà la dépendance e-mail du parser et du système d'alertes). Le corps contient
  l'identifiant, le mot de passe, l'URL du service, et **invite l'utilisateur à utiliser
  le logiciel « superviseur »** pour la supervision à distance. Si l'envoi échoue (SMTP
  non configuré/injoignable), l'API renvoie `{ok:false, mailed:false}` : le compte est
  créé mais **inutilisable tant que l'admin ne relance pas un « renouveler »** (le mdp
  n'est jamais affiché en repli).

**Routes (réservées au rôle `admin`)** — le hook de session vérifie `req.user.role ==
'admin'`, sinon `403` :
- `GET  /api/users` → liste `{username, email, role, enabled, last_login_at, last_seen_at}`
  (jamais de hash/mdp).
- `POST /api/users` `{username, email, role}` → génère+hash+**e-mail**, insère. Réponse
  sans mot de passe.
- `POST /api/users/{name}/renew` → régénère le mdp, met à jour le hash, **renvoie par
  e-mail**. Réponse sans mot de passe.
- `POST /api/users/{name}/disable` / `POST /api/users/{name}/enable` → bascule `enabled`
  (un compte désactivé : login refusé + sessions existantes invalidées).
- `POST /api/users/{name}/delete` → supprime le compte et ses sessions.

`last_login_at` mis à jour dans `/api/auth/login` ; `last_seen_at` mis à jour (throttlé,
> 5 min) par le hook de session — alimente la colonne « dernière activité » de la page.

### Bootstrap du premier admin

- CLI : `checker_service.py --add-admin NOM --email X` → crée un compte **admin** avec mdp
  généré, envoyé par e-mail (ou imprimé en console si SMTP absent, car exécution locale de
  confiance) ; plus `--list-users`, `--del-user NOM`. Permet d'amorcer sans UI, y compris
  sur l'exe.
- **Filet de sécurité localhost** : si `AUTH_ENABLED` et **aucun** utilisateur en base,
  l'accès depuis loopback reste autorisé (couche 0) afin de créer le premier admin via
  l'UI locale. Dès qu'un compte existe, l'auth s'applique aussi pour les requêtes
  non-loopback.
- Tâche planifiée `session-cleanup` (via `RecurringTask`, comme `retention-purge`,
  `lib/scheduler.py`) appelant `purge_expired_sessions()` une fois/jour.

### Front (`web/`)

- **`web/login.html`** (nouveau) : formulaire → `POST /api/auth/login` → sur succès,
  redirige vers `statistiques.html`. Style repris de `theme.css`.
- **`web/comptes.html`** (nouveau, onglet 👤 Comptes visible aux admins seulement) :
  tableau des comptes (identifiant, e-mail, rôle, activé, dernière connexion, dernière
  activité) + actions **Créer** (id + e-mail + rôle), **Renouveler le mdp**,
  **Désactiver/Activer**, **Supprimer**. Aucune colonne/champ mot de passe (jamais
  affiché) ; un toast confirme seulement « mot de passe envoyé à `<email>` ».
- **`web/auth.js`** (nouveau, inclus dans chaque page) : enveloppe `window.fetch` pour
  intercepter tout `401` → redirection vers `login.html`. Ajoute un bouton **Déconnexion**
  dans la barre de nav (appelle `/api/auth/logout`) et masque l'onglet Comptes si
  `whoami.role != 'admin'`. **Aucune modification des appels `fetch(API+url)` existants** :
  le cookie `HttpOnly` est joint automatiquement par le navigateur (avantage majeur du
  cookie sur le localStorage).
- `SameSite=Strict` couvre le risque CSRF sur les `POST` (pas de jeton anti-CSRF séparé
  nécessaire pour cet usage).

### Page API (`web/api.html`) & jeton Bearer de session

Exigence : la page API (playground) doit **toujours** permettre d'appeler les API en
utilisant le **bearer token de la session en cours**.

Route ajoutée : `POST /api/auth/token` (authentifiée par le cookie de session) → émet et
renvoie **un jeton Bearer lié à l'utilisateur courant**, même TTL 24 h glissantes. Comme
la base ne stocke que le *hash* du jeton de navigation (jamais sa valeur en clair), on ne
peut pas « relire » le jeton du cookie ; on en **frappe un à la demande** pour l'usage API
— fonctionnellement le jeton de la session en cours (même compte, même fenêtre 24 h).

`web/api.html` reste pleinement fonctionnel une fois connecté :
- ses appels « Essayer » partent en `Authorization: Bearer <token>` (jeton obtenu via
  `/api/auth/token`), démontrant explicitement l'usage du bearer et pas seulement du cookie ;
- un panneau **« Jeton Bearer de session »** affiche le jeton (bouton copier) + des exemples
  `curl -H "Authorization: Bearer …"` prêts à l'emploi — utiles pour piloter l'API depuis
  l'extérieur ou depuis le superviseur.

Compromis assumé : un jeton exposé dans une console API est, par nature, visible du script
de la page et de l'utilisateur ; HTTPS (transit), TTL 24 h et frappe **à la demande**
limitent l'exposition. La déconnexion révoque la session (option « révoquer tous mes
jetons » via `delete_session`/purge des sessions de l'utilisateur).

## Point 4 — Accès API par jeton (superviseur ↔ service)

Le concept « n'échanger le secret qu'une fois puis utiliser un jeton temporaire »
appliqué au lien machine-à-machine, en **réutilisant le pattern déjà éprouvé** de
`mirth_api.py` (`_shared_client` / `_ensure_session` / self-healing sur 401,
`mirth_api.py:118+`).

### Côté service (checker_service) — déjà couvert par le point 3

Le superviseur peut s'authentifier de deux façons, au choix de l'admin par site :
- **id/mdp d'un compte** (`POST /api/auth/login`) → jeton ; ou
- **clé API** (`api_keys`) via `Authorization: Bearer <clé>` — pratique car révocable
  sans divulguer de mot de passe. Route `POST /api/keys` (créer, renvoyée **une seule
  fois**), `GET /api/keys`, `POST /api/keys/{id}/delete`, réservées aux sessions admin.

Pour coller au concept « la clé ne circule qu'une fois » : le superviseur **échange** sa
clé API contre un **jeton de session** au premier appel, puis n'envoie plus que le jeton
(24 h). Sur `401`, il ré-échange. Ainsi la clé longue durée ne traverse le réseau qu'une
fois par 24 h (et de toute façon sous TLS).

### Côté superviseur (`superviseur.py` + `lib/superviseur_db.py`)

- **Schéma `sites`** enrichi (`lib/superviseur_db.py`) : `scheme` (`http`/`https`),
  `verify_ssl` (0/1, défaut 0 pour accepter le cert auto-signé distant — comme le client
  Mirth actuel, `mirth_api MIRTH_VERIFY_SSL=False`), et `api_key` (ou `username`+`password`).
  Migration `ALTER TABLE` idempotente (motif `PRAGMA table_info` déjà présent dans
  `init_db`).
- **`_base_url(site)`** utilise `scheme` au lieu de `http://` en dur.
- **Session par site en mémoire** : un dict `{site_id: (token, expires)}` protégé par un
  lock, alimenté par un `_ensure_site_session(site)` calqué sur `mirth_api._ensure_session`.
- **`_http_get_raw(base, path, timeout, site)`** (ligne ~105) ajoute
  `Authorization: Bearer <token>` ; si la réponse est `401`, il ré-authentifie une fois et
  rejoue (self-healing, comme `MirthClient._request`, `mirth_api.py`), et gère
  `verify_ssl=False` via un `ssl.SSLContext` non vérifiant pour `urllib`.
- **UI `web_superviseur/admin.html`** : champs par site `scheme` (http/https),
  `verify_ssl`, et `clé API` (ou id/mdp). L'endpoint `POST /api/sites` / `/api/sites/{id}`
  persiste ces champs.
- **Le superviseur protège aussi sa PROPRE UI** avec le même `SecurityPolicy` +
  `web_superviseur/login.html` + `auth.js` (base de comptes dans `superviseur.db`, mêmes
  tables/accesseurs — les fonctions `lib/auth.py`/`lib/database.py` prennent déjà un
  `db_path`).

---

## Fichiers concernés (récapitulatif)

**Nouveaux :** `lib/tls.py`, `lib/auth.py`, `lib/security.py`, `web/login.html`,
`web/comptes.html`, `web/auth.js`, `web_superviseur/login.html`,
`web_superviseur/auth.js`, `_cmd_helper/gen_cert.bat` (optionnel).

**Modifiés :**
- `lib/webserver.py` — `serve(..., security=)`, `_build_handler_class(router, security)`,
  appel du hook en tête de `_dispatch`, `req.client_ip`/`req.user` posés sur la `Request`.
- `lib/database.py` — tables `users`/`web_sessions`/`api_keys` + accesseurs ; exclusion de
  `purge_older_than`/`reset_db` ; ajout de `purge_expired_sessions`.
- `lib/scheduler.py` — (rien d'obligatoire ; la tâche `session-cleanup` est ajoutée dans
  `main()` du service).
- `lib/superviseur_db.py` — colonnes `scheme`/`verify_ssl`/`api_key` sur `sites`.
- `checker_service.py` — argparse (`--https/--cert/--key/--allow-ips/--auth`), CLI comptes
  (`--add-admin`/`--list-users`/`--del-user`), `load_security_config()`, `build_router`
  (routes `/api/auth/*`, `/api/users*` admin, `/api/keys*`), `serve(..., security=policy)`,
  tâche `session-cleanup`. Envoi des mdp générés via `quickmail.sendmail` (déjà importé).
- `web/api.html` — panneau « Jeton Bearer de session » (via `POST /api/auth/token`),
  appels du playground en `Authorization: Bearer`, exemples `curl` copiables.
- `superviseur.py` — mêmes args/sécurité serveur + session par site côté client.
- `.mirth_config.py.template`, `requirements.txt` (`cryptography`),
  `_cmd_helper/_compilation.bat` (voir ci-dessous), `CLAUDE.md`.

**Réutilisations clés :** chargeur `importlib` (`mirth_api.py:82`) ; pattern session
self-healing (`mirth_api.py` `_ensure_session`/`_request`) ; UPSERT config
(`database.save_alert_config`, `database.py:1243`) ; `RecurringTask`/`daily_at`
(`lib/scheduler.py`) ; `Response(headers=…)` pour les cookies (`webserver.py:73`) ;
`quickmail.sendmail(html=…)` pour l'envoi des mdp générés (déjà utilisé par les alertes).

## Compilation `.exe` (Windows)

- `cryptography` sera embarqué par PyInstaller (hooks standards) → cert auto-signé
  fonctionnel dans l'exe autonome.
- Le service génère `checker_cert.pem`/`checker_key.pem` **à côté de l'exe** au 1er
  lancement (dossier persistant, pas `sys._MEIPASS`).
- `_compilation.bat` : vérifier que `web/login.html`, `auth.js` sont bien à côté de l'exe
  (le service ne bundle pas `web/`, cf. CLAUDE.md) ; pas de secret nouveau à `--add-data`
  (les comptes vivent dans la DB, pas dans le binaire).

## Ordre d'implémentation suggéré (phases livrables)

1. **Socle transport & filtre** : `lib/tls.py` + `lib/security.py` (IP + TLS) +
   `serve(security=)` + args/config. → HTTPS + liste blanche opérationnels, API encore
   ouverte (`AUTH_ENABLED=False`). *Testable seul.*
2. **Comptes & session** : tables DB + `lib/auth.py` + routes `/api/auth/*` + hook session
   + CLI `--add-user` + `login.html`/`auth.js`. → UI web protégée.
3. **Clés API & superviseur** : tables/route `api_keys` + colonnes `sites` + session par
   site dans `superviseur.py` + UI admin + protection de l'UI superviseur.
4. **Finitions** : tâche `session-cleanup`, doc `.template`/`CLAUDE.md`, `_compilation.bat`.

Chaque couche est **désactivable** (liste IP vide, `AUTH_ENABLED=False`, `HTTPS_MODE=off`)
→ zéro régression pour un déploiement en réseau de confiance.

---

## Vérification (bout-en-bout)

Env de test : venv chargé (`_cmd_helper\venv_load.bat`). Attention — un vrai Mirth tourne
sur `localhost:8443` et un service peut déjà écouter un port ; utiliser `127.0.0.1` (pas
`localhost`, cf. stall IPv6) et un port dédié.

1. **HTTP local inchangé** : `python checker_service.py --host 127.0.0.1 --port 8801
   --auth off` → `http://127.0.0.1:8801/statistiques.html` s'ouvre sans login (rétro-compat).
2. **HTTPS auto** : `--host 0.0.0.0 --port 8801` sans `--auth off` → au 1er lancement,
   `checker_cert.pem`/`.key` créés ; `https://127.0.0.1:8801` répond (avertissement cert
   auto-signé attendu). `curl -k https://127.0.0.1:8801/api/status` = 200.
3. **Filtre IP** : `ALLOWED_IPS=["127.0.0.1"]` → accès loopback OK ; depuis une autre IP
   de la machine → `403`.
4. **Auth** : `--add-user admin` (mdp), puis `curl -k https://127.0.0.1:8801/api/status`
   sans cookie → `401` ; `POST /api/auth/login` → `200` + cookie ; rappel avec le cookie →
   `200`. Vérifier la **glisse** : `expires_at` avance à chaque appel. `whoami` cohérent.
   Dans le navigateur : accès direct → redirigé vers `login.html`, connexion → dashboard,
   déconnexion → 401/redirection.
5. **Session 24 h** : forcer un `expires_at` passé en base → l'appel suivant renvoie `401`.
5b. **Gestion de comptes (admin)** : connecté en admin, ouvrir `comptes.html` → créer un
   compte technicien (id + e-mail) ; vérifier qu'aucun mot de passe n'est affiché et qu'un
   e-mail part (SMTP de test / journal quickmail). Le hash est en base, jamais le clair.
   Se connecter avec le mdp reçu → OK, `last_login_at` renseigné. Tester **Renouveler**
   (nouveau mail, l'ancien mdp ne marche plus), **Désactiver** (login refusé + session
   coupée), **Supprimer**. Vérifier qu'un **technicien** reçoit `403` sur `/api/users` et
   ne voit pas l'onglet Comptes.
5c. **Page API** : connecté, ouvrir `api.html` → le panneau affiche un jeton Bearer ;
   « Essayer » une route renvoie `200` (en-tête `Authorization: Bearer`) ; copier le
   `curl` fourni et l'exécuter en shell (`-k` en HTTPS auto-signé) → `200`. Sans jeton
   (ou jeton révoqué après déconnexion) → `401`.
6. **Superviseur** : ajouter un site `https` `verify_ssl=0` avec la clé API du service ;
   vérifier que la 1ʳᵉ requête s'authentifie, que les suivantes portent le Bearer, et que
   couper/redémarrer le service distant déclenche une **ré-authentification automatique**
   (self-healing) sans intervention. Confirmer que le login/mdp (ou la clé) ne repart
   qu'après expiration/`401`, pas à chaque tick.
7. **`.exe`** : `_cmd_helper\_compilation.bat` puis lancer l'exe hors venv → cert généré,
   HTTPS + login fonctionnels, DB de comptes créée à côté de l'exe.