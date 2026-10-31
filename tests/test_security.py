# SPDX-License-Identifier: MIT

import os
import stat
import subprocess
import sys
from pathlib import Path

import pytest

from qtop_py import fileutils

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


def test_owned_readable_directory_becomes_private(tmp_path):
    target = tmp_path / "results"
    target.mkdir(mode=0o755)
    (target / "previous.out").write_text("previous state")
    fileutils.private_save_directory(str(target))
    assert stat.S_IMODE(target.stat().st_mode) == 0o700
    assert (target / "previous.out").read_text() == "previous state"


@pytest.mark.parametrize("mode", (0o770, 0o777))
def test_save_directory_rejects_existing_shared_writes(tmp_path, mode):
    target = tmp_path / "results"
    target.mkdir()
    target.chmod(mode)
    with pytest.raises(PermissionError, match="writable by others"):
        fileutils.private_save_directory(str(target))
    assert stat.S_IMODE(target.stat().st_mode) == mode


def test_save_directory_rejects_foreign_owner_before_chmod(tmp_path, monkeypatch):
    target = tmp_path / "results"
    target.mkdir(mode=0o755)
    inode = target.stat().st_ino
    real_fstat = os.fstat

    def foreign_owner(fd):
        info = real_fstat(fd)
        if info.st_ino == inode:
            values = list(info)
            values[4] = os.geteuid() + 1
            return os.stat_result(values)
        return info

    monkeypatch.setattr(fileutils.os, "fstat", foreign_owner)
    with pytest.raises(PermissionError, match="owned by the current user"):
        fileutils.private_save_directory(str(target))
    assert stat.S_IMODE(target.stat().st_mode) == 0o755


@pytest.mark.parametrize("suffix", ("", "/results"))
def test_save_directory_rejects_symlink_components(tmp_path, suffix):
    target = tmp_path / "target"
    target.mkdir(mode=0o755)
    link = tmp_path / "link"
    link.symlink_to(target, target_is_directory=True)
    with pytest.raises(OSError):
        fileutils.private_save_directory(str(link) + suffix)
    assert not (target / "results").exists()
    assert stat.S_IMODE(target.stat().st_mode) == 0o755


def test_save_directory_rejects_writable_nonsticky_parent(tmp_path):
    parent = tmp_path / "shared"
    parent.mkdir()
    parent.chmod(0o777)
    with pytest.raises(PermissionError, match="untrusted parent"):
        fileutils.private_save_directory(str(parent / "results"))
    assert not (parent / "results").exists()


@pytest.mark.parametrize(
    "alias, destination, owner, allowed",
    [
        ("/tmp", "private/tmp", 0, True),
        ("/tmp", "/private/tmp", 0, True),
        ("/var", "private/var", 0, True),
        ("/var", "/private/var", 0, True),
        ("/tmp", "private/elsewhere", 0, False),
        ("/tmp", "private/tmp", 12345, False),
    ],
)
def test_macos_save_directory_handles_only_root_owned_system_aliases(tmp_path, monkeypatch, alias, destination, owner, allowed):
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

    real_readlink = os.readlink
    monkeypatch.setattr(fileutils.sys, "platform", "darwin")
    monkeypatch.delattr(fileutils.os, "O_PATH", raising=False)
    monkeypatch.setattr(fileutils.os, "open", virtual_open)
    monkeypatch.setattr(fileutils.os, "lstat", virtual_lstat)
    monkeypatch.setattr(fileutils.os, "readlink", lambda path: real_readlink(str(root / alias.lstrip("/"))) if path == alias else real_readlink(path))
    if allowed:
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


@pytest.mark.parametrize("mode, allowed", ((0o755, True), (0o1777, True), (0o770, False), (0o777, False)))
def test_save_directory_handles_site_owned_parents(tmp_path, monkeypatch, mode, allowed):
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
        assert fileutils.private_save_directory(str(target)) == str(target)
        assert stat.S_IMODE(target.stat().st_mode) == 0o700
        assert target.stat().st_uid == os.geteuid()
    else:
        with pytest.raises(PermissionError, match="untrusted parent") as caught:
            fileutils.private_save_directory(str(target))
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
from types import SimpleNamespace
from qtop_py import fileutils, qtop, utils, yaml_parser
from qtop_py.plugins.oar import OARBatchSystem

oar = OARBatchSystem({}, {}, SimpleNamespace(ANONYMIZE=False))
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
qtop.PLUGIN_BATCH_SYSTEMS = [Plugin, Plugin]
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
