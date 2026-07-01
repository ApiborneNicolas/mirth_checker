/*
 * auth.js — couche d'authentification côté navigateur (partagée par toutes les pages).
 *
 * 1. Enveloppe window.fetch : toute réponse 401 (session absente/expirée) redirige
 *    vers login.html (sauf sur la page de login elle-même). Le cookie de session
 *    (HttpOnly) est joint automatiquement par le navigateur : les appels
 *    `fetch(API + url)` existants n'ont PAS besoin d'être modifiés.
 * 2. Au chargement, interroge /api/auth/whoami et injecte dynamiquement dans la
 *    barre de navigation : le nom d'utilisateur, un bouton « Déconnexion », et —
 *    pour un administrateur — l'onglet « 👤 Comptes ». Aucune modification du HTML
 *    de chaque page n'est nécessaire.
 *
 * À inclure dans le <head> AVANT tout autre script (sans defer), afin que le
 * wrapper soit posé avant les fetch de la page.
 */
(function () {
    "use strict";
    var onLogin = /login\.html($|\?)/.test(location.pathname + location.search)
        || /\/login\.html$/.test(location.pathname);
    var origFetch = window.fetch.bind(window);

    function toLogin() {
        var next = encodeURIComponent(location.pathname.replace(/^\//, "") + location.search);
        location.replace("login.html?next=" + next);
    }

    // 1. Wrapper fetch : 401 -> redirection vers la page de connexion.
    if (!onLogin) {
        window.fetch = function () {
            return origFetch.apply(null, arguments).then(function (resp) {
                if (resp && resp.status === 401) { toLogin(); }
                return resp;
            });
        };
    }

    if (onLogin) { return; }

    // 2. Injection de la nav (nom, déconnexion, onglet Comptes si admin).
    function injectNav(info) {
        var nav = document.querySelector("header.topbar nav");
        if (nav && info.role === "admin" && !nav.querySelector('[data-mc="comptes"]')) {
            var a = document.createElement("a");
            a.href = "comptes.html";
            a.setAttribute("data-mc", "comptes");
            a.textContent = "👤 Comptes";
            if (/comptes\.html$/.test(location.pathname)) {
                a.setAttribute("aria-current", "page");
            }
            nav.appendChild(a);
        }
        var actions = document.querySelector("header.topbar .actions");
        if (actions && !actions.querySelector('[data-mc="logout"]')) {
            var toggle = actions.querySelector(".theme-toggle");
            if (info.username) {
                var span = document.createElement("span");
                span.className = "mc-user";
                span.textContent = info.username;
                span.style.cssText = "color:var(--muted);font-size:12px;align-self:center;margin-right:4px";
                actions.insertBefore(span, toggle);
            }
            var btn = document.createElement("button");
            btn.type = "button";
            btn.className = "small";
            btn.setAttribute("data-mc", "logout");
            btn.textContent = "Déconnexion";
            btn.addEventListener("click", function () {
                origFetch("/api/auth/logout", { method: "POST" })
                    .catch(function () {})
                    .then(function () { location.replace("login.html"); });
            });
            actions.insertBefore(btn, toggle);
        }
    }

    document.addEventListener("DOMContentLoaded", function () {
        // Utilise origFetch pour piloter nous-mêmes la redirection (message propre).
        origFetch("/api/auth/whoami").then(function (r) {
            if (r.status === 401) { toLogin(); return null; }
            if (!r.ok) { return null; }
            return r.json();
        }).then(function (d) {
            // d.authenticated=false => authentification désactivée côté serveur :
            // rien à injecter, la page fonctionne en accès libre.
            if (d && d.authenticated) { injectNav(d); }
        }).catch(function () {});
    });
})();
