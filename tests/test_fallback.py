## Copyright (c) 2026 qtop contributors
## SPDX-License-Identifier: MIT
"""Best-effort /dev/shm fallback when savepath or the log fails the private-directory checks."""

import logging
import os
import stat
from types import SimpleNamespace

import pytest

from qtop_py import fileutils, utils

pytestmark = pytest.mark.skipif(os.name != "posix", reason="POSIX directory ownership")


@pytest.fixture
def shm(tmp_path, monkeypatch):
    """A sticky world-writable stand-in for /dev/shm, and no other fallback root."""
    root = tmp_path / "shm"
    root.mkdir()
    root.chmod(0o1777)
    monkeypatch.setattr(fileutils, "FALLBACK_ROOTS", (str(root),))
    monkeypatch.setattr(fileutils.tempfile, "gettempdir", lambda: str(root))
    monkeypatch.setattr(fileutils, "_fallback_warnings", set())
    monkeypatch.delenv("QTOP_FALLBACK_DIR", raising=False)
    return root / ("qtop_fallback_%d" % os.geteuid())


@pytest.fixture
def refused(tmp_path):
    """A savepath the no-follow walk refuses: its last component is a symlink."""
    (tmp_path / "elsewhere").mkdir()
    link = tmp_path / "results"
    link.symlink_to(tmp_path / "elsewhere", target_is_directory=True)
    return str(link)


def test_unusable_savepath_falls_back_to_private_shm_with_one_warning(shm, refused, capsys, tmp_path):
    for _ in range(2):  # -w reloads the configuration on every refresh
        assert fileutils.with_fallback("savepath", refused, fileutils.private_save_directory) == str(shm)
    assert stat.S_IMODE(shm.stat().st_mode) == 0o700
    assert list((tmp_path / "elsewhere").iterdir()) == []
    warnings = [line for line in capsys.readouterr().err.splitlines() if "warning" in line]
    assert len(warnings) == 1
    assert refused in warnings[0] and str(shm) in warnings[0] and "--strict-parents" in warnings[0]


def test_fallback_dir_override_replaces_shm_and_temp(shm, refused, monkeypatch, tmp_path):
    scratch = tmp_path / "scratch"
    scratch.mkdir()
    monkeypatch.setenv("QTOP_FALLBACK_DIR", str(scratch))
    expected = scratch / shm.name
    assert fileutils.with_fallback("savepath", refused, fileutils.private_save_directory) == str(expected)
    assert not shm.exists()


def test_usable_savepath_never_falls_back(shm, tmp_path, capsys):
    target = tmp_path / "ok"
    assert fileutils.with_fallback("savepath", str(target), fileutils.private_save_directory) == str(target)
    assert not shm.exists() and capsys.readouterr().err == ""


def test_strict_parents_keeps_the_original_error(shm, refused):
    with pytest.raises(OSError):
        fileutils.with_fallback("savepath", refused, fileutils.private_save_directory, strict=True)
    assert not shm.exists()


def test_original_error_when_every_fallback_fails(shm, refused, monkeypatch):
    monkeypatch.setattr(fileutils, "FALLBACK_ROOTS", (str(shm.parent / "missing"),))
    monkeypatch.setattr(fileutils.tempfile, "gettempdir", lambda: str(shm.parent / "missing"))
    with pytest.raises(OSError) as raised:
        fileutils.with_fallback("savepath", refused, fileutils.private_save_directory)
    assert raised.value.filename != str(shm)


def test_squatted_fallback_directory_is_refused(shm, refused, monkeypatch):
    """Another user's pre-created qtop_fallback_<uid> must not be used."""
    shm.mkdir(mode=0o755)
    real_fstat = os.fstat
    inode = shm.stat().st_ino

    def foreign(fd):
        info = real_fstat(fd)
        if info.st_ino == inode:
            values = list(info)
            values[4] = os.geteuid() + 1
            return os.stat_result(values)
        return info

    monkeypatch.setattr(fileutils.os, "fstat", foreign)
    with pytest.raises(OSError):
        fileutils.with_fallback("savepath", refused, fileutils.private_save_directory)


def test_log_handler_falls_back_and_reports_the_actual_log(shm, tmp_path, capsys):
    (tmp_path / "target").mkdir()
    (tmp_path / "logs").symlink_to(tmp_path / "target", target_is_directory=True)
    handler = utils.PrivateFileHandler(str(tmp_path / "logs" / "qtop.log"), encoding="utf-8")
    try:
        handler.emit(logging.LogRecord("test", logging.WARNING, __file__, 1, "kept", (), None))
    finally:
        handler.close()
    assert handler.baseFilename == str(shm / "qtop.log")
    assert "kept" in (shm / "qtop.log").read_text()
    assert stat.S_IMODE((shm / "qtop.log").stat().st_mode) == 0o600
    assert "warning: cannot use log" in capsys.readouterr().err
    assert list((tmp_path / "target").iterdir()) == []


def test_init_logging_runs_without_a_log_file_when_nothing_is_usable(monkeypatch, capsys):
    def unusable(path, strict=False):
        raise NotADirectoryError(20, "Not a directory", "home")

    monkeypatch.setattr(fileutils, "private_log_stream", unusable)
    logger = logging.getLogger()
    old_handlers, old_level, old_disabled = list(logger.handlers), logger.level, logger.disabled
    try:
        utils.init_logging(SimpleNamespace(verbose=0, DEBUG=False))
        assert fileutils.active_log_path() is None or fileutils.active_log_path() in [h.baseFilename for h in old_handlers if isinstance(h, logging.FileHandler)]
        assert "qtop: warning: no log file: Not a directory (home)" in capsys.readouterr().err
    finally:
        for handler in list(logger.handlers):
            if handler not in old_handlers:
                logger.removeHandler(handler)
                handler.close()
        logger.setLevel(old_level)
        logger.disabled = old_disabled


def test_init_logging_strict_parents_still_stops(monkeypatch):
    def unusable(path, strict=False):
        raise NotADirectoryError(20, "Not a directory", "home")

    monkeypatch.setattr(fileutils, "private_log_stream", unusable)
    logger = logging.getLogger()
    old_handlers = list(logger.handlers)
    try:
        with pytest.raises(NotADirectoryError):
            utils.init_logging(SimpleNamespace(verbose=0, DEBUG=False, STRICT_PARENTS=True))
    finally:
        for handler in list(logger.handlers):
            if handler not in old_handlers:
                logger.removeHandler(handler)
                handler.close()


def test_the_log_follows_home_at_run_time_not_at_import(tmp_path, monkeypatch):
    """constants.QTOP_LOGFILE is a $HOME template: importing qtop reads no environment."""
    monkeypatch.setenv("HOME", str(tmp_path))
    logger = logging.getLogger()
    old_handlers, old_level, old_disabled = list(logger.handlers), logger.level, logger.disabled
    try:
        utils.init_logging(SimpleNamespace(verbose=0, DEBUG=False))
        added = [handler.baseFilename for handler in logger.handlers if handler not in old_handlers and isinstance(handler, logging.FileHandler)]
        assert added == [str(tmp_path / ".local" / "qtop" / "logs" / "qtop.log")]
    finally:
        for handler in list(logger.handlers):
            if handler not in old_handlers:
                logger.removeHandler(handler)
                handler.close()
        logger.setLevel(old_level)
        logger.disabled = old_disabled
