## Copyright (c) 2026 qtop contributors
## SPDX-License-Identifier: MIT
"""Fixtures shared across test modules."""

import itertools
import logging
import sys
from pathlib import Path

import pytest

from qtop_py import qtop, utils

ROOT = Path(__file__).resolve().parents[1]
MISSING = object()
# Module attributes the retired legacy adapters read via globals(); a run must not bring any of them back.
MODULE_STATE = ("QTOPPATH", "HELP_FP", "scheduler", "config", "dynamic_config", "viewport", "user_to_color", "args", "snapshot", "transposed_sections")


@pytest.fixture
def runtime(tmp_path, monkeypatch):
    """Run qtop's CLI inside the test process: runtime(*args) -> (exit code, output directory).

    Always -c OFF -E (and -O unless save_only=False), with HOME, the log and config paths in tmp_path.
    Much cheaper than a subprocess; it asserts that a run leaves qtop's module-level state alone.
    """
    home = tmp_path / "home"
    home.mkdir()
    monkeypatch.setenv("HOME", str(home))
    monkeypatch.setenv("USER", "runtime-test")
    monkeypatch.delenv("QTOP_SCHEDULER", raising=False)
    monkeypatch.delenv("QTOP_COLOR", raising=False)
    monkeypatch.setattr(qtop, "USERPATH", str(home / ".local/qtop"))
    monkeypatch.setattr(qtop, "SYSTEMCONFDIR", str(tmp_path / "etc"))
    log = str(tmp_path / "logs/qtop.log")
    monkeypatch.setattr(utils, "QTOP_LOGFILE", log)
    sequence = itertools.count()
    views = []  # every MVU_query result, so tests can inspect the derived cluster
    real_query = qtop.MVU_query
    monkeypatch.setattr(qtop, "MVU_query", lambda model: views.append(real_query(model)) or views[-1])

    def run(*arguments, save_only=True):
        output = tmp_path / ("output-%s" % next(sequence))
        output.mkdir()
        monkeypatch.chdir(ROOT)
        monkeypatch.setattr(sys, "argv", ["qtop", "-c", "OFF", "-E"] + (["-O"] if save_only else []) + list(arguments) + ["-o", "savepath=" + str(output)])
        logger = logging.getLogger()
        handlers = logger.handlers[:]
        level, disabled = logger.level, logger.disabled
        before = {name: vars(qtop).get(name, MISSING) for name in MODULE_STATE}
        try:
            return qtop.cli_main(), output
        finally:
            assert {name: vars(qtop).get(name, MISSING) for name in MODULE_STATE} == before, "main() changed module-level state"
            for handler in logger.handlers[:]:
                if handler not in handlers:
                    logger.removeHandler(handler)
                    handler.close()
            logger.setLevel(level)
            logger.disabled = disabled

    run.views = views
    return run
