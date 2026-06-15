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

_lock = threading.Lock()
_status = ""        # contenu courant de la ligne d'état (sans le \r de tête)
_drawn_len = 0      # longueur de la ligne d'état réellement dessinée (pour l'effacer)


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
    if not _enabled():
        return
    with _lock:
        s = _stream()
        _erase(s)
        _status = ""
        s.flush()
