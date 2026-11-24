## Copyright (c) 2026 qtop contributors
## SPDX-License-Identifier: MIT
"""Scheduler input reading for the backends: open a trace, check it is not empty.

Core module: it never prints; an optional observer on options (``_input_progress``)
sees each source open and close, so the application can report startup progress.
"""

import contextlib
import logging
import os
from typing import Any, Generator


def input_source(options: Any, path: str) -> Any:
    """Observe a scheduler source when startup reporting is enabled."""
    progress = getattr(options, "_input_progress", None)
    return progress.source(path) if progress is not None else contextlib.ExitStack()


@contextlib.contextmanager
def open_input(path: str, mode: str = "r", options: Any = None, **kwargs: Any) -> Generator[Any, None, None]:
    """Open scheduler input and report its first startup open and close."""
    with contextlib.ExitStack() as reporting:
        # Sonar path finding: this path is the caller's chosen input, by design; see docs/sonar-review.md
        with open(path, mode, **kwargs) as stream:
            reporting.enter_context(input_source(options, path))
            yield stream


def check_empty_file(orig_file: str) -> None:
    """Raise FileEmptyError if the scheduler output file is empty."""
    if os.path.getsize(orig_file) == 0:
        raise FileEmptyError(orig_file)


class FileEmptyError(Exception):
    """A scheduler output file is empty; the message is also logged as a warning."""

    def __init__(self, fn: str) -> None:
        msg = "File %s is empty.\nIs your batch scheduler loaded with jobs?" % fn
        Exception.__init__(self, msg)
        logging.warning(msg)
        self.fn = fn
