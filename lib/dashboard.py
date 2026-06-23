# -*- coding: utf-8 -*-
"""
Tableau de bord console (rich) : deux sections empilées dans le terminal.

- **En haut** : un tableau des tâches périodiques du planificateur (module, état,
  durée du dernier relevé, délai avant la prochaine exécution), rafraîchi en
  continu.
- **En bas** : le « Journal » — les messages de log défilent dans un panneau borné
  (les plus anciens sortent par le haut), ne gardant que les lignes qui tiennent
  dans la hauteur disponible (effet « tail »).

Le rendu repose sur ``rich.live.Live`` en mode plein écran (``screen=True``) avec
un ``Layout`` à deux régions. Les renderables (``_TaskTable`` / ``_Journal``)
lisent l'état courant à CHAQUE frame, si bien que l'auto-rafraîchissement de
``Live`` suffit à animer le tableau (décompte « prochaine exéc. ») et le journal.

Le module est tolérant : si ``rich`` est absent ou si la sortie n'est pas un
terminal (service gelé, sortie redirigée, exécution headless),
``RichDashboard.start()`` renvoie ``False`` et l'appelant (``lib.log``) retombe
sur l'affichage texte classique (logs persistants + ligne d'état réécrite).
"""

import re
import math
import datetime
import threading
from collections import deque

try:
    from rich.console import Console
    from rich.layout import Layout
    from rich.live import Live
    from rich.panel import Panel
    from rich.table import Table
    from rich.text import Text
    from rich import box
    _RICH_OK = True
except Exception:  # rich non installé : l'appelant retombera sur lib.log texte
    _RICH_OK = False


# --- Niveaux de log -------------------------------------------------------
# Les appels existants à ``log.log()`` ne portent pas de niveau explicite ; on le
# déduit grossièrement du texte (purement cosmétique : couleur de la ligne).
_LEVEL_STYLE = {
    "INFO": "bright_cyan",
    "WARNING": "yellow",
    "ERROR": "bold red",
}
_ERR_RE = re.compile(r"erreur|exception|traceback|impossible|échec|introuvable|refus",
                     re.IGNORECASE)
_WARN_RE = re.compile(r"alerte|alarme|dépassement|attention|warning", re.IGNORECASE)

# Préfixe « [tag] message » des logs existants -> (logger, message) ; ex.
# "[checker_service] Base SQLite : ..." ou "[scheduler:metrics-collector] ...".
_TAG_RE = re.compile(r"^\s*\[([^\]]+)\]\s*(.*)$", re.DOTALL)

_JOURNAL_MAXLEN = 2000   # nb max d'entrées conservées en mémoire


def _fmt_duration(d):
    """Durée (s) -> texte compact : « 344 ms » sous la seconde, « 3.3 s » au-delà."""
    if d is None:
        return "—"
    if d < 1:
        return f"{d * 1000:.0f} ms"
    return f"{d:.1f} s"


def _fmt_next(seconds):
    """Délai avant prochaine exécution -> « 18 s » / « 42 min » / « 1.5 h » (None -> « — »)."""
    if seconds is None:
        return "—"
    s = int(math.ceil(seconds))
    if s < 90:
        return f"{s} s"
    if s < 5400:        # < 90 min
        return f"{round(s / 60)} min"
    return f"{round(s / 3600, 1)} h"


class _TaskTable:
    """Renderable rich : le panneau supérieur (tableau des tâches du planificateur)."""

    def __init__(self, dashboard):
        self._dash = dashboard

    def __rich_console__(self, console, options):
        table = Table(box=None, expand=True, show_edge=False, pad_edge=False)
        table.add_column("Module", style="bold cyan", no_wrap=True)
        table.add_column("État")
        table.add_column("Valeur", no_wrap=True)
        table.add_column("Durée", justify="right", style="dim")
        table.add_column("Prochaine exéc.", justify="right")

        for t in self._dash.tasks:
            try:
                st = t.status()
            except Exception:
                continue
            if st.get("executing"):
                etat = Text("en cours", style="bold yellow")
            elif st.get("last_error"):
                etat = Text("erreur", style="bold red")
            elif st.get("running"):
                etat = Text("en attente", style="green")
            else:
                etat = Text("arrêté", style="dim")
            # Valeur représentative fournie par l'appelant : str ou (texte, style).
            val = self._dash.summary(st.get("name"))
            vtext, vstyle = val if isinstance(val, tuple) else (val, "")
            # Le nom est passé en Text (et non en str) : rich interpréterait
            # « [nom] » comme une balise de markup et masquerait la cellule.
            table.add_row(
                Text(f"[{st.get('name', '?')}]"),
                etat,
                Text(str(vtext), style=vstyle or ""),
                _fmt_duration(st.get("last_duration")),
                _fmt_next(st.get("next_run_in")),
            )

        yield Panel(table, title=self._dash.title, border_style="blue",
                    box=box.SQUARE, padding=(0, 1))


class _Journal:
    """Renderable rich : le panneau « Journal » dimensionné à sa région.

    ``__rich_console__`` reçoit la hauteur allouée par le ``Layout`` ; on n'affiche
    que les dernières entrées qui tiennent dans le panneau (bordure comprise),
    de sorte que le journal « tail » sans jamais déborder de sa région.
    """

    def __init__(self, dashboard):
        self._dash = dashboard

    def __rich_console__(self, console, options):
        height = options.height or options.max_height or console.size.height
        inner = max(1, height - 2)        # 2 lignes de bordure (haut + bas)
        with self._dash._lock:
            entries = list(self._dash._journal)[-inner:]

        body = Text(no_wrap=True, overflow="crop")  # 1 entrée = 1 ligne
        for i, e in enumerate(entries):
            if i:
                body.append("\n")
            body.append(e["time"] + " ", style="dim")
            body.append(f"[{e['level']}] ", style=_LEVEL_STYLE.get(e["level"], ""))
            if e["logger"]:
                body.append(e["logger"] + ": ", style="bold bright_white")
            body.append(e["message"])

        yield Panel(body, title="Journal", border_style="green",
                    box=box.SQUARE, padding=(0, 1), height=height)


class RichDashboard:
    """Coordonne le ``Live`` plein écran et l'alimentation du journal."""

    def __init__(self, tasks, title="Scheduler — tâches périodiques",
                 summary_provider=None):
        self.tasks = list(tasks or [])
        self.title = title
        # Callable(nom_tâche) -> str | (texte, style) : valeur représentative
        # affichée dans la colonne « Valeur ». None => colonne vide.
        self._summary_provider = summary_provider
        self._journal = deque(maxlen=_JOURNAL_MAXLEN)
        self._lock = threading.Lock()
        self._console = None
        self._live = None

    def summary(self, name):
        """Valeur représentative d'une tâche (via summary_provider), jamais levante."""
        if self._summary_provider is None:
            return ""
        try:
            return self._summary_provider(name) or ""
        except Exception:
            return ""

    # -- cycle de vie ------------------------------------------------------
    def start(self):
        """Démarre le ``Live`` plein écran. Renvoie False si impossible (pas de
        rich / pas un terminal) : l'appelant retombe alors sur l'affichage texte."""
        if not _RICH_OK:
            return False
        self._console = Console()
        if not self._console.is_terminal:
            return False
        self._build_layout()
        self._live = Live(self._layout, console=self._console, screen=True,
                          refresh_per_second=4, transient=False)
        try:
            self._live.start()
        except Exception:
            self._live = None
            return False
        return True

    def stop(self):
        """Arrête le ``Live`` et restaure l'écran normal du terminal."""
        if self._live is not None:
            try:
                self._live.stop()
            except Exception:
                pass
            self._live = None

    @property
    def is_active(self):
        return self._live is not None

    def _build_layout(self):
        # Hauteur du panneau supérieur : bordures (2) + en-tête (1) + une ligne
        # par tâche. Le journal occupe tout le reste.
        top_size = max(1, len(self.tasks)) + 3
        layout = Layout()
        layout.split_column(
            Layout(name="scheduler", size=top_size),
            Layout(name="journal"),
        )
        layout["scheduler"].update(_TaskTable(self))
        layout["journal"].update(_Journal(self))
        self._layout = layout

    # -- alimentation du journal ------------------------------------------
    def log(self, text, newline=True):
        """Ajoute un message au journal (affiché à la prochaine frame du ``Live``).

        Le texte est découpé en lignes (une entrée par ligne, p. ex. un traceback)
        et le préfixe « [tag] » des logs existants devient le nom du logger.
        """
        text = (text or "").strip("\n")
        lines = text.split("\n") if text else [""]
        now = datetime.datetime.now().strftime("%H:%M:%S")
        with self._lock:
            for ln in lines:
                self._journal.append(self._parse_line(ln, now))

    @staticmethod
    def _parse_line(line, now):
        logger, message = "", line
        m = _TAG_RE.match(line)
        if m:
            logger, message = m.group(1), m.group(2)
        if _ERR_RE.search(line):
            level = "ERROR"
        elif _WARN_RE.search(line):
            level = "WARNING"
        else:
            level = "INFO"
        return {"time": now, "level": level, "logger": logger, "message": message}
