# -*- coding: utf-8 -*-
"""
Fabrique de routes d'authentification réutilisable (comptes / sessions / clés API).

`make_auth_routes(...)` renvoie un objet dont la méthode `register(router)` ajoute
les routes suivantes à un `webserver.Router` :

    POST /api/auth/login | logout | token      GET /api/auth/whoami
    GET/POST /api/users[...]  (admin)           GET/POST /api/keys[...] (admin)

Tout est paramétré par `db_path` (base cible : checker_history.db OU superviseur.db,
mêmes tables via `database.init_auth_tables`), un accès HTTPS (drapeau Secure du
cookie), l'URL de base (liens e-mail) et un `mailer` d'envoi de mot de passe. Ainsi
le superviseur protège sa PROPRE interface avec exactement le même mécanisme que
`checker_service`, sans dupliquer la logique.
"""

import json

from . import auth, database, security, webserver


def _json_response(payload, status=200, headers=None):
    body = json.dumps(payload, ensure_ascii=False, default=str).encode("utf-8")
    return webserver.Response(body, status=status, headers=headers or {},
                              content_type="application/json; charset=utf-8")


class AuthRoutes:
    def __init__(self, db_path, is_https=None, base_url=None, mailer=None,
                 log_fn=None, auth_enabled=None):
        self.db_path = db_path
        self._is_https = is_https or (lambda: False)
        self._base_url = base_url or (lambda: "")
        self._mailer = mailer            # (username, email, password, renew) -> bool
        self._log = log_fn or (lambda *_: None)
        self._auth_enabled = auth_enabled or (lambda: True)

    # -- utilitaires -------------------------------------------------------
    def _cookie(self, token, clear=False):
        parts = [f"{security.SESSION_COOKIE}={token if not clear else ''}",
                 "HttpOnly", "SameSite=Strict", "Path=/"]
        if self._is_https():
            parts.append("Secure")
        if clear:
            parts.append("Max-Age=0")
        return "; ".join(parts)

    def _token(self, req):
        tok = security._read_cookie(req.headers.get("Cookie"), security.SESSION_COOKIE)
        if tok:
            return tok
        authz = req.headers.get("Authorization", "") or ""
        if authz.startswith("Bearer "):
            return authz[7:].strip()
        return None

    def _require_admin(self, req):
        if not self._auth_enabled():
            return None
        u = req.user
        if not u or u.get("role") != "admin":
            return (403, {"error": "forbidden", "detail": "Réservé aux administrateurs."})
        return None

    def _send_pwd(self, username, email, password, renew):
        if not self._mailer:
            return False
        try:
            return bool(self._mailer(username, email, password, renew))
        except Exception as e:
            self._log(f"[auth] Échec d'envoi du mot de passe à {email} : {e}")
            return False

    # -- authentification --------------------------------------------------
    def login(self, req):
        data = req.json()
        username = (data.get("username") or "").strip()
        password = data.get("password") or ""
        if not username or not password:
            return _json_response({"ok": False, "error": "Identifiant et mot de passe requis."},
                                  status=400)
        user = database.get_user(username, db_path=self.db_path)
        if not user or not user.get("enabled") or \
                not auth.verify_password(password, user.get("password_hash")):
            return _json_response({"ok": False, "error": "Identifiants invalides."}, status=401)
        token, expires = auth.create_session(username, ip=req.client_ip, db_path=self.db_path)
        database.touch_user_login(username, db_path=self.db_path)
        self._log(f"[auth] Connexion : {username} ({req.client_ip}).")
        return _json_response(
            {"ok": True, "username": username, "role": user.get("role") or "technicien",
             "expires_at": expires, "token": token},
            headers={"Set-Cookie": self._cookie(token)})

    def logout(self, req):
        tok = self._token(req)
        if tok:
            auth.revoke_session(tok, db_path=self.db_path)
        return _json_response({"ok": True},
                              headers={"Set-Cookie": self._cookie("", clear=True)})

    def whoami(self, req):
        u = req.user or {}
        return {"authenticated": bool(u), "username": u.get("username"),
                "role": u.get("role"), "expires_at": u.get("expires_at"),
                "bootstrap": bool(u.get("bootstrap"))}

    def token(self, req):
        u = req.user or {}
        username = u.get("username")
        if not username or not database.get_user(username, db_path=self.db_path):
            return (400, {"ok": False,
                          "error": "Jeton disponible uniquement pour un compte réel connecté."})
        tok, expires = auth.create_session(username, ip=req.client_ip, db_path=self.db_path)
        return {"ok": True, "token": tok, "expires_at": expires, "username": username}

    # -- comptes (admin) ---------------------------------------------------
    def users_list(self, req):
        guard = self._require_admin(req)
        if guard:
            return guard
        return {"users": database.list_users(db_path=self.db_path)}

    def users_create(self, req):
        guard = self._require_admin(req)
        if guard:
            return guard
        data = req.json()
        username = (data.get("username") or "").strip()
        email = (data.get("email") or "").strip()
        role = (data.get("role") or "technicien").strip()
        if role not in ("admin", "technicien"):
            role = "technicien"
        if not username or not email:
            return (400, {"ok": False, "error": "Identifiant et e-mail requis."})
        password = auth.generate_password()
        if not database.create_user(username, email, auth.hash_password(password),
                                    role=role, db_path=self.db_path):
            return (409, {"ok": False, "error": "Cet identifiant existe déjà."})
        mailed = self._send_pwd(username, email, password, False)
        self._log(f"[auth] Compte créé : {username} ({role}) — e-mail "
                  f"{'envoyé' if mailed else 'NON envoyé'} à {email}.")
        return {"ok": True, "mailed": mailed, "username": username}

    def users_renew(self, req):
        guard = self._require_admin(req)
        if guard:
            return guard
        username = req.params.get("name")
        user = database.get_user(username, db_path=self.db_path)
        if not user:
            return (404, {"ok": False, "error": "Compte introuvable."})
        password = auth.generate_password()
        database.set_password(username, auth.hash_password(password), db_path=self.db_path)
        database.delete_user_sessions(username, db_path=self.db_path)
        mailed = self._send_pwd(username, user.get("email"), password, True)
        self._log(f"[auth] Mot de passe renouvelé : {username} — e-mail "
                  f"{'envoyé' if mailed else 'NON envoyé'}.")
        return {"ok": True, "mailed": mailed, "username": username}

    def users_enable(self, req):
        guard = self._require_admin(req)
        if guard:
            return guard
        username = req.params.get("name")
        if not database.set_user_enabled(username, True, db_path=self.db_path):
            return (404, {"ok": False, "error": "Compte introuvable."})
        return {"ok": True, "username": username, "enabled": True}

    def users_disable(self, req):
        guard = self._require_admin(req)
        if guard:
            return guard
        username = req.params.get("name")
        if not database.set_user_enabled(username, False, db_path=self.db_path):
            return (404, {"ok": False, "error": "Compte introuvable."})
        return {"ok": True, "username": username, "enabled": False}

    def users_delete(self, req):
        guard = self._require_admin(req)
        if guard:
            return guard
        username = req.params.get("name")
        if not database.delete_user(username, db_path=self.db_path):
            return (404, {"ok": False, "error": "Compte introuvable."})
        self._log(f"[auth] Compte supprimé : {username}.")
        return {"ok": True, "username": username}

    # -- clés API (admin) --------------------------------------------------
    def keys_list(self, req):
        guard = self._require_admin(req)
        if guard:
            return guard
        return {"keys": database.list_api_keys(db_path=self.db_path)}

    def keys_create(self, req):
        guard = self._require_admin(req)
        if guard:
            return guard
        data = req.json()
        label = (data.get("label") or "").strip() or None
        username = (data.get("username") or "").strip() or None
        raw = auth.new_api_key()
        database.insert_api_key(auth.token_fingerprint(raw), label=label,
                                username=username, db_path=self.db_path)
        self._log(f"[auth] Clé API créée : {label or '(sans label)'}.")
        return {"ok": True, "key": raw, "key_id": auth.token_fingerprint(raw), "label": label}

    def keys_delete(self, req):
        guard = self._require_admin(req)
        if guard:
            return guard
        if not database.delete_api_key(req.params.get("id"), db_path=self.db_path):
            return (404, {"ok": False, "error": "Clé introuvable."})
        return {"ok": True}

    # -- enregistrement ----------------------------------------------------
    def register(self, router):
        router.post("/api/auth/login", self.login)
        router.post("/api/auth/logout", self.logout)
        router.get("/api/auth/whoami", self.whoami)
        router.post("/api/auth/token", self.token)
        router.get("/api/users", self.users_list)
        router.post("/api/users", self.users_create)
        router.post("/api/users/{name}/renew", self.users_renew)
        router.post("/api/users/{name}/enable", self.users_enable)
        router.post("/api/users/{name}/disable", self.users_disable)
        router.post("/api/users/{name}/delete", self.users_delete)
        router.get("/api/keys", self.keys_list)
        router.post("/api/keys", self.keys_create)
        router.post("/api/keys/{id}/delete", self.keys_delete)
        return router


def make_auth_routes(db_path, **kwargs):
    return AuthRoutes(db_path, **kwargs)
