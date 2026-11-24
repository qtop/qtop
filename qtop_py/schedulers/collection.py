## Copyright (c) 2026 qtop contributors
## SPDX-License-Identifier: MIT
"""Single-writer scheduler polling independent of terminal redraws."""

import logging
import os
import tempfile
import threading
import time
from typing import Any, Callable, Dict, NamedTuple, Optional, Tuple

from qtop_py.core_errors import OperationalError

Observation = NamedTuple("Observation", [("state", Any), ("system", Any), ("backend", Any), ("traces", Dict[str, Tuple[str, bytes]]), ("started", float), ("completed", float)])


class CollectionPoller:
    """Publish complete observations atomically, retaining the previous success on failure."""

    def __init__(self, collect: Callable[[str, threading.Event], Any], INTERVAL: float, clock: Callable[[], float] = time.monotonic, SAVEPATH: Optional[str] = None) -> None:
        if INTERVAL <= 0:
            raise ValueError("collection interval must be positive")
        self.collect, self.INTERVAL, self.clock = collect, INTERVAL, clock
        self.SAVEPATH = SAVEPATH
        self.stop = threading.Event()
        self.lock = threading.Lock()
        self.collection_lock = threading.Lock()
        self.error = ""
        self.thread: Optional[threading.Thread] = None
        self.latest = self._collect()
        self.deadline = self.latest.started + INTERVAL

    def _collect(self) -> Observation:
        started = self.clock()
        with tempfile.TemporaryDirectory(prefix="qtop-collection-", dir=self.SAVEPATH) as directory:
            state, system, backend, paths = self.collect(directory, self.stop)
            traces = {}
            for key, path in paths.items():
                with open(path, "rb") as stream:
                    traces[key] = (os.path.basename(path), stream.read())
        return Observation(state, system, backend, traces, started, self.clock())

    def refresh(self) -> bool:
        """Attempt at most one due collection; never publish a partial result."""
        with self.collection_lock:
            if self.stop.is_set() or self.clock() < self.deadline:
                return False
            try:
                observed = self._collect()
            except (OperationalError, OSError, ValueError, TypeError) as error:
                if not self.stop.is_set():
                    logging.warning("Scheduler collection failed; retaining last successful snapshot: %s", error)
                    with self.lock:
                        self.error = str(error)
            else:
                with self.lock:
                    self.latest, self.error = observed, ""
            # Keep the original cadence, skipping missed slots rather than overlapping.
            elapsed = max(0, self.clock() - self.deadline)
            self.deadline += (int(elapsed // self.INTERVAL) + 1) * self.INTERVAL
            return True

    def read(self) -> Tuple[Observation, str]:
        """Return one coherent publication and its most recent failure diagnostic."""
        with self.lock:
            return self.latest, self.error

    def _run(self) -> None:
        while not self.stop.wait(max(0, self.deadline - self.clock())):
            self.refresh()

    def start(self) -> None:
        """Start the sole collector once; redraws only read its published result."""
        if self.thread is None:
            self.thread = threading.Thread(target=self._run, name="qtop-collector", daemon=True)
            self.thread.start()

    def close(self) -> None:
        """Cancel in-flight commands and reap the worker before releasing its data."""
        self.stop.set()
        if self.thread is not None:
            self.thread.join()
