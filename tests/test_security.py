## Copyright (c) 2026 qtop contributors
## SPDX-License-Identifier: MIT

import logging
import os
import stat
import subprocess
import sys
from pathlib import Path

import pytest

from qtop_py import fileutils, utils

ROOT = Path(__file__).resolve().parents[1]
pytestmark = pytest.mark.skipif(os.name != "posix", reason="POSIX directory ownership and isolated launcher")


def test_save_directory_is_private_even_with_permissive_umask(tmp_path):
    target = tmp_path / "new" / "results"
    previous = os.umask(0)
    try:
        assert fileutils.private_save_directory(str(target)) == str(target)
    finally:
        os.umask(previous)
    for directory in (target.parent, target):
        info = directory.stat()
        assert stat.S_IMODE(info.st_mode) == 0o700
        assert info.st_uid == os.geteuid()


@pytest.mark.parametrize("mode", (0o755, 0o770, 0o777), ids=("readable", "group-writable", "world-writable"))
def test_owned_save_directory_becomes_private_and_keeps_its_files(tmp_path, mode):
    target = tmp_path / "results"
    target.mkdir()
    target.chmod(mode)
    (target / "previous.out").write_text("previous state")
    assert fileutils.private_save_directory(str(target)) == str(target)
    assert stat.S_IMODE(target.stat().st_mode) == 0o700
    assert (target / "previous.out").read_text() == "previous state"


@pytest.mark.parametrize("kind", ("save-directory", "log-file"))
def test_foreign_owner_is_rejected_before_chmod(tmp_path, monkeypatch, kind):
    """A path whose descriptor reports another owner is refused, and keeps its mode and contents."""
    if kind == "save-directory":
        path = tmp_path / "results"
        path.mkdir(mode=0o755)
        refused, message = lambda: fileutils.private_save_directory(str(path)), "owned by the current user"
    else:
        path = tmp_path / "qtop.log"
        path.write_text("untouched")
        path.chmod(0o644)
        refused, message = lambda: fileutils.private_log_stream(str(path)), "user-owned"
    mode, inode, real_fstat = stat.S_IMODE(path.stat().st_mode), path.stat().st_ino, os.fstat

    def foreign_owner(descriptor):
        info = real_fstat(descriptor)
        if info.st_ino != inode:
            return info
        values = list(info)
        values[4] = os.geteuid() + 1
        return os.stat_result(values)

    monkeypatch.setattr(fileutils.os, "fstat", foreign_owner)
    with pytest.raises(PermissionError, match=message):
        refused()
    assert stat.S_IMODE(path.stat().st_mode) == mode
    if kind == "log-file":
        assert path.read_text() == "untouched"


@pytest.mark.parametrize("suffix", ("", "/results"))
def test_save_directory_rejects_symlink_components(tmp_path, suffix):
    target = tmp_path / "target"
    target.mkdir(mode=0o755)
    link = tmp_path / "link"
    link.symlink_to(target, target_is_directory=True)
    if suffix and os.geteuid() == 0:  # root's own link in a 0700 parent is a trusted site link (#615)
        assert fileutils.private_save_directory(str(link) + suffix) == os.path.realpath(str(target / "results"))
        return
    with pytest.raises(OSError):
        fileutils.private_save_directory(str(link) + suffix)
    assert not (target / "results").exists()
    assert stat.S_IMODE(target.stat().st_mode) == 0o755


def test_save_directory_rejects_writable_nonsticky_parent_when_strict(tmp_path):
    parent = tmp_path / "shared"
    parent.mkdir()
    parent.chmod(0o777)
    with pytest.raises(PermissionError, match="untrusted parent"):
        fileutils.private_save_directory(str(parent / "results"), strict=True)
    assert not (parent / "results").exists()


@pytest.mark.parametrize("mode", (0o770, 0o775, 0o777))
def test_save_directory_accepts_writable_nonsticky_parent_by_default(tmp_path, caplog, mode):
    parent = tmp_path / "shared"
    parent.mkdir()
    parent.chmod(mode)
    target = parent / "results"
    with caplog.at_level(logging.INFO, logger="qtop_py.fileutils"):
        assert fileutils.private_save_directory(str(target)) == str(target)
    assert stat.S_IMODE(target.stat().st_mode) == 0o700
    assert stat.S_IMODE(parent.stat().st_mode) == mode
    assert str(parent) in caplog.text and "--strict-parents" in caplog.text


def test_log_stream_accepts_writable_nonsticky_parent_by_default(tmp_path):
    parent = tmp_path / "home"
    parent.mkdir()
    parent.chmod(0o775)
    with fileutils.private_log_stream(str(parent / "logs" / "qtop.log")) as stream:
        stream.write("ok\n")
    assert stat.S_IMODE((parent / "logs").stat().st_mode) == 0o700


def test_lenient_parents_still_reject_symlinked_save_directory(tmp_path):
    parent = tmp_path / "shared"
    parent.mkdir()
    parent.chmod(0o777)
    (tmp_path / "elsewhere").mkdir()
    (parent / "results").symlink_to(tmp_path / "elsewhere", target_is_directory=True)
    with pytest.raises(OSError):
        fileutils.private_save_directory(str(parent / "results"))


@pytest.mark.parametrize(
    "alias, destination, owner, allowed",
    [
        ("/tmp", "private/tmp", 0, True),
        ("/tmp", "/private/tmp", 0, True),
        ("/var", "private/var", 0, True),
        ("/var", "/private/var", 0, True),
        ("/tmp", "private/elsewhere", 0, "site"),  # not an Apple alias, but a root-owned site link (#615)
        ("/tmp", "private/tmp", 12345, False),
    ],
)
def test_macos_save_directory_handles_only_root_owned_system_aliases(tmp_path, monkeypatch, alias, destination, owner, allowed):
    """The faked owner applies to every ownership check, so the outcome does not depend on who runs the tests."""
    root = tmp_path / "root"
    parent = root / "private" / alias.lstrip("/")
    parent.mkdir(parents=True)
    parent.chmod(0o1777)
    (root / alias.lstrip("/")).symlink_to(destination, target_is_directory=True)
    real_open, real_lstat = os.open, os.lstat

    def virtual_open(path, *args, **kwargs):
        return real_open(str(root) if path == os.sep else path, *args, **kwargs)

    def virtual_lstat(path, *args, **kwargs):
        if path == alias:
            values = list(real_lstat(str(root / alias.lstrip("/"))))
            values[4] = owner
            return os.stat_result(values)
        return real_lstat(path, *args, **kwargs)

    real_stat = os.stat

    def virtual_stat(path, *args, **kwargs):
        result = real_stat(path, *args, **kwargs)
        if path == alias.lstrip("/") and kwargs.get("dir_fd") is not None and not kwargs.get("follow_symlinks", True):
            values = list(result)
            values[4] = owner
            return os.stat_result(values)
        return result

    real_readlink = os.readlink
    monkeypatch.setattr(fileutils.sys, "platform", "darwin")
    monkeypatch.delattr(fileutils.os, "O_PATH", raising=False)
    monkeypatch.setattr(fileutils.os, "open", virtual_open)
    monkeypatch.setattr(fileutils.os, "lstat", virtual_lstat)
    monkeypatch.setattr(fileutils.os, "stat", virtual_stat)
    monkeypatch.setattr(fileutils.os, "readlink", lambda path, **kwargs: real_readlink(str(root / alias.lstrip("/"))) if path == alias else real_readlink(path, **kwargs))
    if allowed == "site":
        assert fileutils.private_save_directory(alias + "/results") == "/private/elsewhere/results"
        assert stat.S_IMODE((root / "private" / "elsewhere" / "results").stat().st_mode) == 0o700
        assert not (parent / "results").exists()
    elif allowed:
        assert fileutils.private_save_directory(alias + "/results") == "/private" + alias + "/results"
        assert stat.S_IMODE((parent / "results").stat().st_mode) == 0o700
        (parent / "redirect").symlink_to(parent / "results", target_is_directory=True)
        with pytest.raises(OSError):
            fileutils.private_save_directory(alias + "/redirect/child")
        assert not (parent / "results" / "child").exists()
    else:
        with pytest.raises(OSError):
            fileutils.private_save_directory(alias + "/results")
        assert not (parent / "results").exists()


@pytest.mark.parametrize(
    "mode, strict, allowed",
    ((0o755, True, True), (0o1777, True, True), (0o770, True, False), (0o777, True, False), (0o770, False, True), (0o777, False, True)),
)
def test_save_directory_handles_site_owned_parents(tmp_path, monkeypatch, mode, strict, allowed):
    parent = tmp_path / "site-owned"
    parent.mkdir()
    parent.chmod(mode)
    target = parent / "results"
    inode = parent.stat().st_ino
    real_fstat = os.fstat

    def site_owner(fd):
        info = real_fstat(fd)
        if info.st_ino == inode:
            values = list(info)
            values[4] = os.geteuid() + 1
            return os.stat_result(values)
        return info

    monkeypatch.setattr(fileutils.os, "fstat", site_owner)
    if allowed:
        assert fileutils.private_save_directory(str(target), strict) == str(target)
        assert stat.S_IMODE(target.stat().st_mode) == 0o700
        assert target.stat().st_uid == os.geteuid()
    else:
        with pytest.raises(PermissionError, match="untrusted parent") as caught:
            fileutils.private_save_directory(str(target), strict)
        assert caught.value.filename == str(parent)
        assert not target.exists()
    assert stat.S_IMODE(parent.stat().st_mode) == mode


def test_save_directory_allows_sticky_shared_parent(tmp_path):
    parent = tmp_path / "shared"
    parent.mkdir()
    parent.chmod(0o1777)
    target = parent / "results"
    fileutils.private_save_directory(str(target))
    assert stat.S_IMODE(target.stat().st_mode) == 0o700


@pytest.mark.parametrize("path", ("", " ", None, "/"))
def test_save_directory_rejects_invalid_destinations(path):
    with pytest.raises((ValueError, PermissionError)):
        fileutils.private_save_directory(path)


@pytest.mark.parametrize("optimized", (False, True))
def test_runtime_validation_survives_optimization(tmp_path, optimized):
    empty = tmp_path / "empty.yaml"
    empty.write_text("")
    code = """
import sys
from qtop_py import fileutils, qtop, utils, core_yaml_parser as yaml_parser
from qtop_py.core_api import BackendOptions
from qtop_py.plugins.oar import OARBatchSystem

oar = OARBatchSystem({}, {}, BackendOptions())
def invalid_watch():
    original = sys.argv
    try:
        sys.argv = ['qtop', '-w', 'invalid']
        utils.parse_qtop_cmdline_args()
    except SystemExit as error:
        if error.code != 2:
            raise RuntimeError('Invalid watch interval did not fail argument parsing')
        raise
    finally:
        sys.argv = original
checks = [
    (lambda: fileutils.parse_time_input('2x'), ValueError),
    (lambda: fileutils.add_to_sample((), None), TypeError),
    (lambda: yaml_parser.parse(sys.argv[1]), ValueError),
    (lambda: oar._read_oarnodes_s_yaml(sys.argv[1]), ValueError),
    (lambda: oar._read_oarnodes_s_yaml(sys.argv[1] + '.missing'), FileNotFoundError),
    (invalid_watch, SystemExit),
]
class Plugin:
    @staticmethod
    def get_mnemonic():
        return 'duplicate'
from qtop_py.schedulers import registry
registry.PLUGIN_BATCH_SYSTEMS = [Plugin, Plugin]
checks.append((qtop.discover_qtop_batch_systems, qtop.OperationalError))
for operation, expected in checks:
    try:
        operation()
    except expected:
        continue
    raise SystemExit('Runtime validation was bypassed')
"""
    command = [sys.executable] + (["-O"] if optimized else []) + ["-c", code, str(empty)]
    result = subprocess.run(command, cwd=str(ROOT), stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True, timeout=15)
    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize("args, status", [(["--version"], 0), (["-b", "unsupported"], 1)])
def test_launcher_ignores_untrusted_cwd_and_pythonpath(tmp_path, args, status):
    marker = tmp_path / "injected"
    payload = "from pathlib import Path\nPath(%r).write_text('executed')\nraise RuntimeError('untrusted import')\n" % str(marker)
    (tmp_path / "logging.py").write_text(payload)
    (tmp_path / "sitecustomize.py").write_text(payload)
    (tmp_path / "qtop_py").mkdir()
    (tmp_path / "qtop_py" / "__init__.py").write_text(payload)
    env = dict(os.environ, PYTHONPATH=str(tmp_path))
    env["PATH"] = str(Path(sys.executable).parent) + os.pathsep + env.get("PATH", "")
    result = subprocess.run(["bash", str(ROOT / "qtop")] + args, cwd=str(tmp_path), env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True, timeout=15)
    assert result.returncode == status, result.stderr
    assert not marker.exists()
    assert "untrusted import" not in result.stderr
    if status == 0:
        assert "qtop current version:" in result.stdout


@pytest.mark.skipif(not hasattr(os, "O_PATH"), reason="Linux path-only directory descriptors")
def test_save_directory_can_traverse_a_nonreadable_parent(tmp_path):
    parent = tmp_path / "search-only"
    parent.mkdir()
    target = parent / "results"
    target.mkdir(mode=0o700)
    parent.chmod(0o111)
    try:
        assert fileutils.private_save_directory(str(target)) == str(target)
    finally:
        parent.chmod(0o700)


@pytest.mark.parametrize("mode", [0o000, 0o400, 0o644, 0o660, 0o666])
def test_private_log_appends_and_tightens_existing_permissions(tmp_path, mode):
    path = tmp_path / "logs" / "qtop.log"
    path.parent.mkdir(mode=0o755)
    path.write_text("previous\n")
    path.chmod(mode)
    previous = os.umask(0)
    try:
        handler = utils.PrivateFileHandler(str(path), encoding="utf-8")
        try:
            assert isinstance(handler, logging.FileHandler)
            handler.emit(logging.LogRecord("test", logging.WARNING, __file__, 1, "next", (), None))
        finally:
            handler.close()
    finally:
        os.umask(previous)
    assert path.read_text() == "previous\nnext\n"
    assert stat.S_IMODE(path.stat().st_mode) == 0o600
    assert stat.S_IMODE(path.parent.stat().st_mode) == 0o700


@pytest.mark.parametrize("kind", ["symlink", "hardlink", "directory"])
@pytest.mark.parametrize("target_mode", [0o400, 0o644])
def test_private_log_refuses_unsafe_targets_without_altering_them(tmp_path, kind, target_mode):
    path = tmp_path / "logs" / "qtop.log"
    path.parent.mkdir(mode=0o700)
    target = tmp_path / "victim"
    target.write_text("untouched")
    target.chmod(target_mode)
    if kind == "symlink":
        path.symlink_to(target)
    elif kind == "hardlink":
        os.link(str(target), str(path))
    else:
        path.mkdir()
    with pytest.raises(OSError):
        fileutils.private_log_stream(str(path))
    assert target.read_text() == "untouched"
    assert stat.S_IMODE(target.stat().st_mode) == target_mode


@pytest.mark.parametrize("directory_mode", [0o000, 0o100, 0o300, 0o777])
def test_private_log_repairs_owned_qtop_tree_without_changing_home(tmp_path, monkeypatch, directory_mode):
    monkeypatch.setenv("HOME", str(tmp_path))
    root = tmp_path / ".local" / "qtop"
    path = root / "logs" / "nested" / "qtop.log"
    path.parent.mkdir(parents=True)
    path.write_text("previous\n")
    path.chmod(0o400)
    directories = [root, root / "logs", path.parent]
    for directory in reversed(directories):
        directory.chmod(directory_mode)
    home_mode = stat.S_IMODE(tmp_path.stat().st_mode)
    local_mode = stat.S_IMODE(root.parent.stat().st_mode)
    try:
        with fileutils.private_log_stream(str(path)) as stream:
            stream.write("next\n")
        assert path.read_text() == "previous\nnext\n"
        assert all(stat.S_IMODE(directory.stat().st_mode) == 0o700 for directory in directories)
    finally:
        for directory in directories:
            directory.chmod(0o700)
    assert stat.S_IMODE(path.stat().st_mode) == 0o600
    assert stat.S_IMODE(tmp_path.stat().st_mode) == home_mode
    assert stat.S_IMODE(root.parent.stat().st_mode) == local_mode


def test_private_log_rejects_symlinked_parent(tmp_path):
    target = tmp_path / "target"
    target.mkdir()
    link = tmp_path / "logs"
    link.symlink_to(target, target_is_directory=True)
    with pytest.raises(OSError):
        fileutils.private_log_stream(str(link / "qtop.log"))
    assert not (target / "qtop.log").exists()


def _as_root_owned(monkeypatch, *links):
    """Report these symlinks as root-owned, as a site's autofs/NFS links are."""
    inodes = {os.lstat(str(link)).st_ino for link in links}
    real_stat = os.stat

    def stat_as_root(path, *args, **kwargs):
        info = real_stat(path, *args, **kwargs)
        if kwargs.get("follow_symlinks") is False and stat.S_ISLNK(info.st_mode) and info.st_ino in inodes:
            values = list(info)
            values[4] = 0
            return os.stat_result(values)
        return info

    monkeypatch.setattr(fileutils.os, "stat", stat_as_root)


def _site_home(tmp_path, target):
    site = tmp_path / "site"
    site.mkdir()
    site.chmod(0o755)
    (tmp_path / "export" / "home" / "alice").mkdir(parents=True)
    home = site / "home"
    home.symlink_to(target, target_is_directory=True)
    return site, home


@pytest.mark.parametrize("relative", (False, True))
def test_private_log_follows_root_owned_site_home_link(tmp_path, monkeypatch, relative):
    """#615: an autofs/NFS /home -> /export/home no longer stops qtop with 'Not a directory (home)'."""
    _, home = _site_home(tmp_path, "../export/home" if relative else tmp_path / "export" / "home")
    _as_root_owned(monkeypatch, home)
    monkeypatch.setenv("HOME", str(home / "alice"))
    qtop_root = tmp_path / "export" / "home" / "alice" / ".local" / "qtop"
    qtop_root.mkdir(parents=True)
    qtop_root.chmod(0o755)
    with fileutils.private_log_stream(str(home / "alice" / ".local" / "qtop" / "logs" / "qtop.log")) as stream:
        stream.write("ok\n")
    assert (qtop_root / "logs" / "qtop.log").read_text() == "ok\n"
    assert stat.S_IMODE(qtop_root.stat().st_mode) == 0o700


def test_save_directory_resolves_root_owned_site_link(tmp_path, monkeypatch):
    _, home = _site_home(tmp_path, tmp_path / "export" / "home")
    _as_root_owned(monkeypatch, home)
    target = tmp_path / "export" / "home" / "alice" / "results"
    assert fileutils.private_save_directory(str(home / "alice" / "results")) == str(target)
    assert stat.S_IMODE(target.stat().st_mode) == 0o700


@pytest.mark.parametrize("mode", (0o775, 0o777, 0o1777))
def test_root_owned_link_in_writable_parent_is_refused(tmp_path, monkeypatch, mode):
    site, home = _site_home(tmp_path, tmp_path / "export" / "home")
    _as_root_owned(monkeypatch, home)
    site.chmod(mode)
    with pytest.raises(OSError):
        fileutils.private_save_directory(str(home / "alice" / "results"))
    assert not (tmp_path / "export" / "home" / "alice" / "results").exists()


def test_root_owned_link_as_qtop_owned_directory_is_refused(tmp_path, monkeypatch):
    _, home = _site_home(tmp_path, tmp_path / "export" / "home")
    _as_root_owned(monkeypatch, home)
    exported = tmp_path / "export" / "home"
    exported.chmod(0o755)
    with pytest.raises(OSError):
        fileutils.private_save_directory(str(home))
    assert stat.S_IMODE(exported.stat().st_mode) == 0o755


def test_root_owned_link_loop_is_bounded(tmp_path, monkeypatch):
    site = tmp_path / "site"
    site.mkdir()
    site.chmod(0o755)
    (site / "a").symlink_to("b", target_is_directory=True)
    (site / "b").symlink_to("a", target_is_directory=True)
    _as_root_owned(monkeypatch, site / "a", site / "b")
    with pytest.raises(OSError):
        fileutils.private_save_directory(str(site / "a" / "results"))


@pytest.mark.parametrize("directory", [False, True])
def test_permission_repair_does_not_chmod_foreign_entries(tmp_path, monkeypatch, directory):
    path = tmp_path / "blocked"
    if directory:
        path.mkdir()
    else:
        path.write_text("untouched")
    inode = path.stat().st_ino
    path.chmod(0o000)
    native_stat, native_open = os.stat, os.open

    def deny_target_open(name, *args, **kwargs):
        # Root can open mode-000 entries; force the permission-repair path.
        info = native_stat(name, dir_fd=kwargs.get("dir_fd"), follow_symlinks=False)
        if info.st_ino == inode:
            raise PermissionError("simulated access denial")
        return native_open(name, *args, **kwargs)

    def foreign_owner(*args, **kwargs):
        info = native_stat(*args, **kwargs)
        if info.st_ino == inode:
            values = list(info)
            values[4] = os.geteuid() + 1
            return os.stat_result(values)
        return info

    monkeypatch.setattr(fileutils.os, "stat", foreign_owner)
    monkeypatch.setattr(fileutils.os, "open", deny_target_open)
    try:
        with pytest.raises(PermissionError, match="simulated access denial"):
            if directory:
                fileutils.private_save_directory(str(path))
            else:
                fileutils.private_log_stream(str(path))
        assert stat.S_IMODE(path.stat().st_mode) == 0o000
    finally:
        path.chmod(0o700 if directory else 0o600)
    if not directory:
        assert path.read_text() == "untouched"


def test_private_log_closes_descriptor_when_stream_creation_fails(tmp_path, monkeypatch):
    descriptors = []

    def fail(descriptor, *args, **kwargs):
        descriptors.append(descriptor)
        raise OSError("stream allocation failed")

    monkeypatch.setattr(fileutils.os, "fdopen", fail)
    with pytest.raises(OSError, match="allocation"):
        fileutils.private_log_stream(str(tmp_path / "qtop.log"))
    with pytest.raises(OSError):
        os.fstat(descriptors[0])


def test_private_log_directory_swap_does_not_redirect_writes(tmp_path, monkeypatch):
    directory = tmp_path / "logs"
    directory.mkdir(mode=0o700)
    moved = tmp_path / "original-logs"
    native_open = os.open

    def swap_before_file_open(path, flags, *args, **kwargs):
        if path == "qtop.log" and "dir_fd" in kwargs:
            directory.rename(moved)
            directory.mkdir(mode=0o700)
            (directory / "qtop.log").write_text("decoy")
        return native_open(path, flags, *args, **kwargs)

    monkeypatch.setattr(fileutils.os, "open", swap_before_file_open)
    with fileutils.private_log_stream(str(directory / "qtop.log")) as stream:
        stream.write("private log")
    assert (moved / "qtop.log").read_text() == "private log"
    assert (directory / "qtop.log").read_text() == "decoy"


def test_private_log_rejects_fifo_without_blocking(tmp_path):
    path = tmp_path / "qtop.log"
    os.mkfifo(str(path))
    reader = os.open(str(path), os.O_RDWR | os.O_NONBLOCK)
    try:
        with pytest.raises(PermissionError, match="regular file"):
            fileutils.private_log_stream(str(path))
    finally:
        os.close(reader)
