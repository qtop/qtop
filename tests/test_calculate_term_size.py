##
## qtop is a tool to monitor queuing systems - https://github.com/qtop/qtop
##
## Copyright (c) 2016 Fotis Georgatos
## Copyright (c) 2016 Sotiris Fragkiskos
## Copyright (c) 2026 Utkarsh Sinha
##
## SPDX-License-Identifier: MIT
##

"""Regression tests for ``qtop.calculate_term_size``.

Before this fix, the terminal-size autodetection *fallback* branch referenced a
module-global ``viewport`` that does not exist (it is a local inside ``main``),
so any environment where ``stty size`` fails -- an IDE, a pipe, a headless CI
shell -- raised ``NameError``. The misused ``all(a, b)`` guard (``all`` takes a
single iterable) compounded it. ``viewport`` is now injected explicitly and the
guard is ``all([...])``.
"""

import qtop_py.qtop as qtop


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


def test_stty_success_is_used(monkeypatch):
    monkeypatch.setattr(qtop.subprocess, "Popen", _FakeStty(stdout=b"40 100\n", stderr=b""))
    assert qtop.calculate_term_size({}, [53, 176], _FakeViewport((11, 22))) == (40, 100)


def test_fallback_uses_viewport_when_stty_fails(monkeypatch):
    monkeypatch.setattr(qtop.subprocess, "Popen", _FakeStty(stderr=b"stty: not a tty"))
    assert qtop.calculate_term_size({}, [53, 176], _FakeViewport((40, 100))) == (40, 100)


def test_fallback_to_hardcoded_when_viewport_invalid(monkeypatch):
    # A falsy viewport dimension must fall back to the hardcoded size, not crash.
    monkeypatch.setattr(qtop.subprocess, "Popen", _FakeStty(stderr=b"stty: not a tty"))
    assert qtop.calculate_term_size({}, [53, 176], _FakeViewport((0, 0))) == (53, 176)


def test_config_term_size_overrides_hardcoded_fallback(monkeypatch):
    monkeypatch.setattr(qtop.subprocess, "Popen", _FakeStty(stderr=b"stty: not a tty"))
    assert qtop.calculate_term_size({"term_size": [70, 200]}, [53, 176], _FakeViewport((0, 0))) == (70, 200)


def test_fallback_path_does_not_raise_nameerror(monkeypatch):
    # This exact call raised NameError before the fix; it must now return cleanly.
    monkeypatch.setattr(qtop.subprocess, "Popen", _FakeStty(stderr=b"err"))
    assert qtop.calculate_term_size({}, [53, 176], _FakeViewport((24, 80))) == (24, 80)
