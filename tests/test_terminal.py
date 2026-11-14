# Copyright (c) 2026 qtop contributors
# SPDX-License-Identifier: MIT
import os

import pytest


@pytest.mark.skipif(os.name != "posix", reason="PTY watch tests require POSIX; use WSL on Windows")
@pytest.mark.parametrize("shutdown", ("q", "SIGINT", "SIGTERM"))
def test_real_watch_keys_resize_capture_and_restore_terminal(tmp_path, shutdown):
    from tools.terminal_probe import probe

    report = probe(tmp_path, 2.2, shutdown=shutdown)
    assert report["frames"] >= 2
    assert report["terminal_restored"]
    assert report["snapshots"] and report["captures"]


def test_terminal_harness_rejects_native_windows_before_opening_pty(tmp_path, monkeypatch):
    from tools import terminal_probe

    monkeypatch.setattr(terminal_probe.os, "name", "nt")
    with pytest.raises(RuntimeError, match="Windows use WSL"):
        terminal_probe.probe(tmp_path, 2)
