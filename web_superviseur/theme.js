/* =====================================================================
   Mirth Checker — Gestion du thème clair/sombre (partagé)
   - Applique le thème mémorisé (localStorage) ou la préférence système.
   - Câble le bouton .theme-toggle.
   - Expose window.MCTheme.chartColors() pour que Chart.js suive le thème,
     et émet l'évènement "mc-theme-change" à chaque bascule.
   Pour éviter tout flash, le data-theme initial est posé par un petit
   script inline dans le <head> de chaque page (voir applyInitialTheme).
   ===================================================================== */
(function () {
    const KEY = "mc-theme";
    const root = document.documentElement;

    function systemPref() {
        return window.matchMedia && window.matchMedia("(prefers-color-scheme: light)").matches
            ? "light" : "dark";
    }

    function current() {
        return root.getAttribute("data-theme") || "dark";
    }

    function apply(theme, persist) {
        root.setAttribute("data-theme", theme);
        if (persist) { try { localStorage.setItem(KEY, theme); } catch (e) {} }
        document.dispatchEvent(new CustomEvent("mc-theme-change", { detail: { theme } }));
    }

    function toggle() {
        apply(current() === "dark" ? "light" : "dark", true);
    }

    // Palette pour les graphiques Chart.js selon le thème courant.
    function chartColors() {
        const light = current() === "light";
        return {
            tick: light ? "#5b6b85" : "#93a3bb",
            grid: light ? "rgba(91,107,133,.14)" : "rgba(148,163,184,.10)",
            text: light ? "#10192b" : "#e6edf7",
        };
    }

    window.MCTheme = { current, apply, toggle, chartColors };

    // Câblage du bouton + suivi de la préférence système (si non figée).
    document.addEventListener("DOMContentLoaded", function () {
        const btn = document.querySelector(".theme-toggle");
        if (btn) btn.addEventListener("click", toggle);

        if (window.matchMedia) {
            window.matchMedia("(prefers-color-scheme: light)").addEventListener("change", e => {
                let saved = null;
                try { saved = localStorage.getItem(KEY); } catch (_) {}
                if (!saved) apply(e.matches ? "light" : "dark", false);  // suit l'OS tant que non figé
            });
        }
    });
})();
