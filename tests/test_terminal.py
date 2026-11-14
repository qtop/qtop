# Copyright (c) 2026 qtop contributors
# SPDX-License-Identifier: MIT
import contextlib
import os
import subprocess

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


@pytest.mark.skipif(os.name != "posix", reason="PTY watch tests require POSIX; use WSL on Windows")
def test_probe_waits_for_keyboard_acknowledgement_before_sending_resize(tmp_path, monkeypatch):
    import signal

    from tools import terminal_probe

    observed = bytearray()
    read = terminal_probe.os.read
    kill = terminal_probe.os.kill
    resized = []

    def record_read(*args):
        chunk = read(*args)
        observed.extend(chunk)
        return chunk

    def check_resize(pid, signum):
        if signum == signal.SIGWINCH:
            assert b"Toggling full-name" in observed
            resized.append(pid)
        kill(pid, signum)

    monkeypatch.setattr(terminal_probe.os, "read", record_read)
    monkeypatch.setattr(terminal_probe.os, "kill", check_resize)
    terminal_probe.probe(tmp_path, 2)
    assert len(resized) == 1


@pytest.mark.skipif(os.name != "posix", reason="PTY watch tests require POSIX; use WSL on Windows")
@pytest.mark.parametrize("failure", [FileNotFoundError("git"), subprocess.CalledProcessError(128, ["git", "rev-parse", "HEAD"])], ids=["missing-git", "unreadable-checkout"])
def test_probe_survives_unavailable_git_revision(tmp_path, monkeypatch, failure):
    from tools import terminal_probe

    def unavailable(*args, **kwargs):
        raise failure

    monkeypatch.setattr(terminal_probe.subprocess, "check_output", unavailable)
    report = terminal_probe.probe(tmp_path, 2)
    assert report["revision"] is None
    assert report["terminal_restored"]


@pytest.mark.skipif(os.name != "posix", reason="PTY watch tests require POSIX; use WSL on Windows")
@pytest.mark.parametrize("stage", ("home", "process"), ids=["mkdir-failure", "spawn-failure"])
def test_probe_closes_pty_when_setup_fails(tmp_path, monkeypatch, stage):
    import pty

    from tools import terminal_probe

    descriptors = []
    openpty = pty.openpty

    def record_openpty():
        pair = openpty()
        descriptors.extend(pair)
        return pair

    def fail_spawn(*args, **kwargs):
        raise OSError("spawn failed")

    monkeypatch.setattr(pty, "openpty", record_openpty)
    if stage == "home":
        (tmp_path / "home").mkdir()
    else:
        monkeypatch.setattr(terminal_probe.subprocess, "Popen", fail_spawn)
    try:
        with pytest.raises(FileExistsError if stage == "home" else OSError):
            terminal_probe.probe(tmp_path, 2)
        assert len(descriptors) == 2
        for descriptor in descriptors:
            with pytest.raises(OSError):
                os.fstat(descriptor)
    finally:
        for descriptor in descriptors:
            with contextlib.suppress(OSError):
                os.close(descriptor)
