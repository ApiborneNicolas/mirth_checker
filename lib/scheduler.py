# -*- coding: utf-8 -*-
"""
Tâche programmée récurrente exécutée dans un thread daemon.

`RecurringTask` appelle une fonction à intervalle régulier sans bloquer le
programme principal (le serveur web). La première exécution a lieu immédiatement
au démarrage, puis toutes les `interval` secondes. L'attente est interruptible
via un `threading.Event`, ce qui permet un arrêt propre (pas d'attente du délai
complet à la fermeture).
"""

import time
import threading
import traceback
import datetime

from . import log


class RecurringTask:
    def __init__(self, interval, func, name="recurring-task", run_immediately=True,
                 start_delay=0.0, anchor=None, on_complete=None):
        """
        Args:
            interval (float): délai en secondes entre deux exécutions.
            func (callable): fonction à exécuter (sans argument).
            name (str): nom du thread (utile pour le débogage).
            run_immediately (bool): exécuter une première fois dès le démarrage.
            start_delay (float): décalage initial (s) avant la toute première
                exécution. Sert à étaler le démarrage de plusieurs tâches afin
                qu'elles ne sondent pas le système exactement au même instant
                (cf. start_staggered).
            anchor (float|None): origine de temps (référence `time.monotonic()`)
                sur laquelle aligner la grille des déclenchements. Partagé entre
                plusieurs tâches, il garantit que toutes suivent le MÊME rythme
                `interval` et conservent un décalage `start_delay` constant à
                chaque cycle (pas de dérive). Si None, l'ancre est posée au
                démarrage du thread.
            on_complete (callable|None): hook appelé après CHAQUE exécution (succès
                ou échec), une fois `last_duration` mis à jour, avec la tâche en
                argument. Sert p. ex. à rafraîchir une ligne d'état console. Les
                exceptions qu'il lève sont ignorées (ne doivent pas tuer la boucle).
        """
        self.interval = interval
        self.func = func
        self.name = name
        self.run_immediately = run_immediately
        self.start_delay = start_delay
        self.anchor = anchor
        self.on_complete = on_complete
        self._stop_event = threading.Event()
        self._thread = None
        self.last_run = None
        self.last_error = None
        self.run_count = 0
        self.last_duration = None      # durée (s) de la dernière exécution de func
        self.overruns = 0              # nb de cycles plus longs que l'intervalle
        self.executing = False         # func() est en cours d'exécution (ce tick)
        self._next_run_monotonic = None  # échéance (time.monotonic) du prochain relevé

    def _loop(self):
        # Grille de déclenchement absolue : chaque relevé est calé sur
        # `anchor + start_delay + k * interval` (k = 0, 1, 2, ...). Contrairement à
        # une attente « interval - durée » qui dérive au fil des cycles (jitter de
        # l'ordonnanceur, granularité du réveil), cette grille tient l'intervalle
        # exact et conserve le décalage `start_delay` constant entre tâches
        # partageant la même `anchor` (cf. start_staggered).
        anchor = self.anchor if self.anchor is not None else time.monotonic()
        next_run = anchor + self.start_delay
        if not self.run_immediately:
            next_run += self.interval
        self._next_run_monotonic = next_run

        while not self._stop_event.is_set():
            # Attente interruptible jusqu'à l'échéance absolue du prochain relevé.
            delay = next_run - time.monotonic()
            if delay > 0 and self._stop_event.wait(delay):
                break
            if self._stop_event.is_set():
                break

            self.executing = True
            # Rendu événementiel : bascule l'affichage de la tâche en « en cours ».
            # (no-op hors tableau de bord rich actif.)
            log.dashboard_refresh()
            start = time.monotonic()
            try:
                self.func()
                self.last_run = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                self.last_error = None
                self.run_count += 1
            except Exception as e:  # une tâche qui échoue ne doit pas tuer la boucle
                self.last_error = str(e)
                # Logs persistants (effacent/redessinent la ligne d'état console).
                log.log(f"[scheduler:{self.name}] Erreur : {e}")
                log.log(traceback.format_exc(), newline=False)
            finally:
                self.last_duration = round(time.monotonic() - start, 4)
                self.executing = False

            # Hook de fin d'exécution (ligne d'état console, etc.). Toute erreur
            # ici est avalée pour ne pas interrompre la boucle de relevé.
            if self.on_complete is not None:
                try:
                    self.on_complete(self)
                except Exception:
                    pass

            # Échéance suivante sur la grille. Si l'exécution a débordé d'un ou
            # plusieurs créneaux, on saute les ticks manqués pour se resynchroniser
            # sur le prochain créneau futur (et on le signale) — on garde ainsi
            # l'alignement de la grille plutôt que d'accumuler du retard.
            next_run += self.interval
            now = time.monotonic()
            if next_run <= now:
                missed = 0
                while next_run <= now:
                    next_run += self.interval
                    missed += 1
                self.overruns += missed
                log.log(f"[scheduler:{self.name}] Dépassement : relevé plus long que "
                        f"l'intervalle {self.interval}s — {missed} créneau(x) sauté(s), "
                        f"resynchronisation sur la grille.")
            self._next_run_monotonic = next_run

            # Rendu événementiel : l'état de la tâche vient de changer (en attente,
            # durée, valeur, heure de la prochaine exécution) — on redessine le
            # tableau de bord UNE fois, au lieu de le rafraîchir en continu.
            log.dashboard_refresh()

    def start(self):
        """Démarre la boucle dans un thread daemon. Sans effet si déjà lancée."""
        if self._thread and self._thread.is_alive():
            return self
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._loop, name=self.name, daemon=True)
        self._thread.start()
        return self

    def stop(self, wait=False, timeout=5):
        """Demande l'arrêt de la boucle (interruption immédiate de l'attente)."""
        self._stop_event.set()
        if wait and self._thread:
            self._thread.join(timeout)

    @property
    def is_running(self):
        return self._thread is not None and self._thread.is_alive()

    @property
    def next_run_in(self):
        """Secondes restantes avant le prochain relevé (None si pas encore planifié)."""
        if self._next_run_monotonic is None:
            return None
        return max(0.0, self._next_run_monotonic - time.monotonic())

    @property
    def next_run_at(self):
        """Heure murale (datetime) de la prochaine exécution (None si non planifiée).

        Obtenue en ramenant le délai monotone restant (`next_run_in`) à l'horloge
        murale. La valeur ABSOLUE ainsi calculée est stable d'un instant à l'autre
        (elle ne « défile » pas), ce qui permet au tableau de bord console de
        l'afficher en « HH:MM:SS » sans rafraîchissement continu.
        """
        secs = self.next_run_in
        if secs is None:
            return None
        return datetime.datetime.now() + datetime.timedelta(seconds=secs)

    def status(self):
        nra = self.next_run_at
        return {
            "name": self.name,
            "interval": self.interval,
            "start_delay": self.start_delay,
            "running": self.is_running,
            "executing": self.executing,
            "next_run_in": self.next_run_in,
            # Heure absolue (chaîne) du prochain relevé : affichée telle quelle par
            # le tableau de bord console et exposée via /api/status.
            "next_run_at": nra.strftime("%Y-%m-%d %H:%M:%S") if nra else None,
            "run_count": self.run_count,
            "last_run": self.last_run,
            "last_error": self.last_error,
            "last_duration": self.last_duration,
            "overruns": self.overruns,
        }


def start_staggered(tasks, step=5.0):
    """Démarre une liste de tâches sur une grille de temps commune et étalée.

    Toutes les tâches partagent une même origine de temps (`anchor`) ; la n-ième
    reçoit un décalage `n * step` secondes. Chaque tâche se déclenche donc aux
    instants `anchor + n*step + k*interval` : même intervalle pour toutes, et
    décalage `step` constant entre deux tâches successives à CHAQUE cycle (pas
    seulement au premier). Cela évite que plusieurs sondes système (CPU, mémoire,
    processus...) s'exécutent au même instant — donc un pic de charge et des
    relevés incohérents — sur toute la durée de vie du service, sans dérive.

    Args:
        tasks (Iterable[RecurringTask]): tâches à démarrer, dans l'ordre voulu.
        step (float): décalage ajouté entre deux tâches successives (s).

    Returns:
        list[RecurringTask]: les tâches démarrées (mêmes objets).
    """
    anchor = time.monotonic()
    started = []
    for i, task in enumerate(tasks):
        task.start_delay = i * step
        task.anchor = anchor
        task.start()
        started.append(task)
    return started
