# -*- coding: utf-8 -*-
r"""
Console coordonnée : logs persistants + une ligne d'état réécrite en place.

Ce module concilie deux affichages sur la même sortie standard :

- des **logs persistants** (``log(...)``) qui défilent normalement, chacun
  terminé par un saut de ligne et donc CONSERVÉS à l'écran ;
- une **ligne d'état** unique (``status(...)``) réécrite en place à l'aide d'un
  retour chariot (``\r``), sans saut de ligne — typiquement la progression des
  tâches du planificateur.

Le problème résolu : si l'on écrit un log « normal » alors que le curseur est posé
sur la ligne d'état (``\r`` sans ``\n``), le texte du log écrase partiellement
cette ligne. Ici, ``log`` EFFACE d'abord la ligne d'état, écrit le log suivi d'un
``\n`` (le log reste donc à l'écran), puis REDESSINE la ligne d'état juste en
dessous. La ligne d'état « flotte » ainsi toujours en bas.

Toutes les écritures sont sérialisées par un verrou (les tâches du planificateur
tournent sur des threads distincts). Si la sortie n'est pas un terminal, la ligne
d'état est désactivée (pas de ``\r`` dans un journal redirigé) et ``log`` se
comporte comme un simple ``print``.

Hypothèse : la ligne d'état tient sur une seule ligne du terminal (l'effacement
repose sur un unique ``\r`` ; une ligne d'état plus large que le terminal ne
serait pas entièrement nettoyée).
"""

import sys
import threading

from . import dashboard as _dashboard_mod

_lock = threading.Lock()
_status = ""        # contenu courant de la ligne d'état (sans le \r de tête)
_drawn_len = 0      # longueur de la ligne d'état réellement dessinée (pour l'effacer)

# Tableau de bord rich actif (cf. start_dashboard). Quand il est posé, log()/
# status()/clear() lui délèguent et l'affichage texte « ligne d'état » ci-dessous
# est court-circuité ; sinon (rich absent / pas un terminal) on garde l'ancien
# comportement.
_dashboard = None

# Mode silencieux (cf. set_quiet) : quand il est actif, log()/status()/clear() et
# start_dashboard() ne produisent plus aucune sortie. Utilisé par l'option CLI
# --no_output lorsque le service tourne en arrière-plan, fenêtre non accessible.
_quiet = False


def set_quiet(quiet=True):
    """Active (ou non) le mode silencieux : plus aucune sortie console.

    Quand il est actif, ``log()``, ``status()`` et ``clear()`` deviennent des
    no-op et ``start_dashboard()`` renvoie False sans rien afficher. À poser AVANT
    le premier log / le démarrage du tableau de bord (cf. --no_output).
    """
    global _quiet
    _quiet = bool(quiet)


def start_dashboard(tasks, title="Scheduler — tâches périodiques",
                    summary_provider=None):
    """Active le tableau de bord rich (tâches en haut, journal en bas).

    `tasks` est la liste de `RecurringTask` à afficher (leur état est lu à chaque
    rendu). `summary_provider` est un callable optionnel `nom_tâche -> str |
    (texte, style)` alimentant la colonne « Valeur ». Renvoie True si le tableau
    de bord a démarré, False sinon (mode silencieux, rich indisponible ou sortie
    non interactive) — dans ce cas l'affichage texte classique reste en vigueur.
    """
    global _dashboard
    if _quiet:
        return False
    dash = _dashboard_mod.RichDashboard(tasks, title=title,
                                        summary_provider=summary_provider)
    if dash.start():
        _dashboard = dash
        return True
    return False


def stop_dashboard():
    """Arrête le tableau de bord rich et restaure l'écran normal du terminal.

    Sans effet si aucun tableau de bord n'est actif. À appeler avant les derniers
    messages d'arrêt pour qu'ils s'affichent sur l'écran restauré.
    """
    global _dashboard
    if _dashboard is not None:
        _dashboard.stop()
        _dashboard = None


def dashboard_refresh():
    """Redessine le tableau de bord rich s'il est actif (rendu événementiel).

    Le tableau de bord ne se rafraîchit plus en continu : il est redessiné UNIQUEMENT
    quand son contenu change. Le planificateur appelle cette fonction aux transitions
    d'état d'une tâche (début/fin d'exécution) et le journal le fait à chaque nouveau
    message. Sans tableau de bord actif (repli texte / mode silencieux), c'est un no-op.
    """
    d = _dashboard
    if d is not None:
        d.refresh()


def _stream():
    # Résolu à chaque appel (et non capturé) pour rester correct si sys.stdout
    # est réaffecté (redirection, capture de tests...).
    return sys.stdout


def _enabled():
    """La ligne d'état (réécriture ``\\r``) n'a de sens que sur un vrai terminal."""
    try:
        return bool(_stream().isatty())
    except Exception:
        return False


def _erase(stream):
    """Efface la ligne d'état actuellement affichée (curseur ramené en début)."""
    global _drawn_len
    if _drawn_len:
        stream.write("\r" + " " * _drawn_len + "\r")
        _drawn_len = 0


def _draw(stream):
    """(Re)dessine la ligne d'état en place, sans saut de ligne."""
    global _drawn_len
    if _status:
        stream.write("\r" + _status)
        _drawn_len = len(_status)


def status(text):
    """Met à jour la ligne d'état (réécriture en place via ``\\r``, sans ``\\n``).

    `text` remplace intégralement la ligne précédente ; les résidus d'une ligne
    plus longue sont effacés. Sans effet si la sortie n'est pas un terminal.
    """
    global _status
    if _quiet:
        return
    if _dashboard is not None:
        # La ligne d'état est remplacée par le tableau des tâches du tableau de bord.
        return
    if not _enabled():
        return
    with _lock:
        s = _stream()
        _erase(s)              # efface l'ancienne (gère un rétrécissement)
        _status = text or ""
        _draw(s)
        s.flush()


def log(text="", newline=True, refresh=False, statusline=None):
    """Écrit un message console en cohabitation avec la ligne d'état.

    Args:
        text (str): message à écrire.
        newline (bool): termine le log persistant par un saut de ligne (défaut),
            de sorte qu'il RESTE à l'écran. Ignoré si ``refresh=True``.
        refresh (bool): si True, `text` n'est pas un log persistant mais le
            nouveau contenu de la LIGNE D'ÉTAT (réécriture en place via ``\\r`` ;
            équivalent à ``status(text)``).
        statusline (str|None): pour un log persistant (``refresh=False``), nouveau
            contenu de la ligne d'état à mémoriser et réafficher SOUS le log. Si
            None, la ligne d'état mémorisée est conservée et réaffichée telle quelle.

    Log persistant : efface la ligne d'état courante, écrit `text` (+ ``\\n``),
    puis redessine la ligne d'état juste en dessous — le log reste donc visible et
    la ligne d'état « flotte » toujours en bas.
    """
    global _status
    if _quiet:
        return
    if _dashboard is not None:
        # `refresh` (ancienne maj de ligne d'état) est sans objet : la table des
        # tâches est redessinée par le planificateur. Tout le reste va au journal
        # du tableau de bord (qui se redessine à chaque nouveau message).
        if not refresh:
            _dashboard.log(text, newline=newline)
        return
    if refresh:
        status(text)
        return

    with _lock:
        s = _stream()
        if not _enabled():
            # Pas de terminal : aucune ligne d'état, simple écriture persistante.
            s.write(text + ("\n" if newline else ""))
            s.flush()
            return
        _erase(s)                                    # 1. efface la ligne d'état
        s.write(text + ("\n" if newline else ""))    # 2. log persistant (reste à l'écran)
        if statusline is not None:                   # 3. maj éventuelle du contenu d'état
            _status = statusline
        _draw(s)                                     # 4. redessine la ligne d'état dessous
        s.flush()


def clear():
    """Efface définitivement la ligne d'état (sans la réafficher).

    À appeler avant un bloc de logs final (arrêt du service) pour ne pas laisser
    une ligne d'état orpheline en bas de l'écran.
    """
    global _status
    if _quiet:
        return
    if _dashboard is not None:
        return
    if not _enabled():
        return
    with _lock:
        s = _stream()
        _erase(s)
        _status = ""
        s.flush()
