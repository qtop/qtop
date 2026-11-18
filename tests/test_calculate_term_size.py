##
## qtop is a tool to monitor queuing systems - https://github.com/qtop/qtop
##
## Copyright (c) 2016 Fotis Georgatos
## Copyright (c) 2016 Sotiris Fragkiskos
## Copyright (c) 2026 Utkarsh Sinha
##
## SPDX-License-Identifier: MIT
##

"""Regression tests for ``qtop.calculate_term_size`` fallbacks."""

import pytest

import qtop_py.qtop as qtop
from qtop_py.ui import terminal as terminal_io


class _FakeStty:
    """Stand-in for ``subprocess.Popen(['/bin/stty', 'size'])``."""

    def __init__(self, stdout=b"", stderr=b""):
        self._stdout = stdout
        self._stderr = stderr

    def __call__(self, *args, **kwargs):  # subprocess.Popen(...) call
        return self

    def communicate(self):
        return self._stdout, self._stderr


class _FakeViewport:
    def __init__(self, size):
        self._size = size

    def get_term_size(self):
        return self._size


@pytest.fixture(autouse=True)
def resolved_stty(monkeypatch):
    monkeypatch.setattr(terminal_io.shutil, "which", lambda command: "/test/stty")


def test_stty_success_is_used(monkeypatch):
    monkeypatch.setattr(qtop.subprocess, "Popen", _FakeStty(stdout=b"40 100\n", stderr=b""))
    assert terminal_io.calculate_term_size({}, [53, 176], _FakeViewport((11, 22))) == (40, 100)


def test_fallback_uses_viewport_when_stty_fails(monkeypatch):
    monkeypatch.setattr(qtop.subprocess, "Popen", _FakeStty(stderr=b"stty: not a tty"))
    assert terminal_io.calculate_term_size({}, [53, 176], _FakeViewport((40, 100))) == (40, 100)


def test_fallback_to_hardcoded_when_viewport_invalid(monkeypatch):
    # A falsy viewport dimension must fall back to the hardcoded size, not crash.
    monkeypatch.setattr(qtop.subprocess, "Popen", _FakeStty(stderr=b"stty: not a tty"))
    assert terminal_io.calculate_term_size({}, [53, 176], _FakeViewport((0, 0))) == (53, 176)


def test_config_term_size_overrides_hardcoded_fallback(monkeypatch):
    monkeypatch.setattr(qtop.subprocess, "Popen", _FakeStty(stderr=b"stty: not a tty"))
    assert terminal_io.calculate_term_size({"term_size": [70, 200]}, [53, 176], _FakeViewport((0, 0))) == (70, 200)


def test_fallback_uses_explicit_viewport(monkeypatch):
    monkeypatch.setattr(qtop.subprocess, "Popen", _FakeStty(stderr=b"err"))
    assert terminal_io.calculate_term_size({}, [53, 176], _FakeViewport((24, 80))) == (24, 80)


def test_fallback_when_stty_is_not_installed(monkeypatch):
    monkeypatch.setattr(terminal_io.shutil, "which", lambda command: None)
    assert terminal_io.calculate_term_size({}, [53, 176], _FakeViewport((24, 80))) == (24, 80)


def test_fallback_when_stty_cannot_start(monkeypatch):
    def unavailable_stty(*args, **kwargs):
        raise OSError("cannot execute stty")

    monkeypatch.setattr(qtop.subprocess, "Popen", unavailable_stty)
    assert terminal_io.calculate_term_size({}, [53, 176], _FakeViewport((24, 80))) == (24, 80)


def test_fallback_when_stty_output_is_invalid(monkeypatch):
    monkeypatch.setattr(qtop.subprocess, "Popen", _FakeStty(stdout=b"not-a-size", stderr=b""))
    assert terminal_io.calculate_term_size({}, [53, 176], _FakeViewport((24, 80))) == (24, 80)


def test_fallback_when_viewport_is_unavailable(monkeypatch):
    monkeypatch.setattr(terminal_io.shutil, "which", lambda command: None)
    assert terminal_io.calculate_term_size({}, [53, 176], None) == (53, 176)
