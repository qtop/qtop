##
## qtop is a tool to monitor queuing systems - https://github.com/qtop/qtop
##
## Copyright (c) 2026 Evangelos Papadopoulos
##
## SPDX-License-Identifier: MIT
##

import errno
import io
import sys
import tarfile
from types import SimpleNamespace

import pytest

from qtop_py import fileutils


@pytest.mark.parametrize("sampling", (0, 1))
@pytest.mark.parametrize("redirected", (False, True))
def test_safe_exit_preserves_original_stdout_and_leaves_archive_to_caller(monkeypatch, sampling, redirected):
    original = io.StringIO()
    current = io.StringIO() if redirected else original
    with monkeypatch.context() as patch:
        patch.setattr(sys, "stdout", current)
        with pytest.raises(SystemExit) as exited:
            fileutils.safe_exit_with_file_close(-1, "unused", original, SimpleNamespace(SAMPLE=sampling), "unused", "unused.log", "unused.tar")
        assert exited.value.code == 0
        assert sys.stdout is original
        assert not original.closed
        assert current.closed == redirected


def test_init_sample_file_returns_none_when_sampling_is_disabled(tmp_path):
    options = SimpleNamespace(SAMPLE=0)

    assert fileutils.init_sample_file(options, str(tmp_path), "sample.tar", {}, "qtopconf.yaml", str(tmp_path)) is None


def test_init_sample_file_adds_config_and_python_sources(tmp_path):
    options = SimpleNamespace(SAMPLE=2)
    (tmp_path / "qtopconf.yaml").write_text("scheduler: demo\n", encoding="utf-8")
    (tmp_path / "example.py").write_text("VALUE = 1\n", encoding="utf-8")

    archive = fileutils.init_sample_file(options, str(tmp_path), "sample.tar", {}, "qtopconf.yaml", str(tmp_path))
    archive.close()

    with tarfile.open(tmp_path / "sample.tar") as saved_archive:
        assert set(saved_archive.getnames()) == {"qtopconf.yaml", "qtop_py/example.py", "qtop"}


def test_add_to_sample_logs_and_skips_archive_errors(caplog):
    class BrokenArchive:
        def add(self, *_args, **_kwargs):
            raise tarfile.TarError("damaged archive")

    archive = BrokenArchive()
    with caplog.at_level("ERROR"):
        result = fileutils.add_to_sample(["scheduler-output.txt"], archive)

    assert result is archive
    assert "scheduler-output.txt" in caplog.text
    assert "damaged archive" in caplog.text


def test_deprecate_old_output_files_ignores_concurrent_removal(monkeypatch, tmp_path):
    monkeypatch.setattr(fileutils.os, "listdir", lambda path: ["stale.out"])

    def vanished(_path):
        raise OSError(errno.ENOENT, "already removed")

    monkeypatch.setattr(fileutils.os.path, "getmtime", vanished)

    fileutils.deprecate_old_output_files({"savepath": str(tmp_path), "auto_delete_old_output_files_after": "1h"})


def test_deprecate_old_output_files_keeps_unexpected_errors(monkeypatch, tmp_path):
    monkeypatch.setattr(fileutils.os, "listdir", lambda path: ["stale.out"])

    def denied(_path):
        raise OSError(errno.EACCES, "denied")

    monkeypatch.setattr(fileutils.os.path, "getmtime", denied)

    with pytest.raises(OSError) as error:
        fileutils.deprecate_old_output_files({"savepath": str(tmp_path), "auto_delete_old_output_files_after": "1h"})

    assert error.value.errno == errno.EACCES


@pytest.mark.parametrize("installed", (False, True))
def test_extended_sample_preserves_package_source_paths(tmp_path, installed):
    root = tmp_path / "distribution"
    source = root if installed else root / "qtop_py"
    (source / "plugins").mkdir(parents=True)
    (source / "ui").mkdir()
    (source / "web").mkdir()
    (source / "web" / "index.html").write_text("<html></html>", encoding="utf-8")
    (root / "helpfile.txt").write_text("Help", encoding="utf-8")
    license_dir = root.parent / "qtop-1.dist-info" / "licenses" if installed else root
    license_dir.mkdir(parents=True, exist_ok=True)
    (license_dir / "LICENSE").write_text("MIT", encoding="utf-8")
    (root / "qtopconf.yaml").write_text("scheduler: demo\n", encoding="utf-8")
    (source / "qtop.py").write_text("VALUE = 1\n", encoding="utf-8")
    (source / "plugins" / "slurm.py").write_text("VALUE = 2\n", encoding="utf-8")
    (source / "ui" / "viewport.py").write_text("VALUE = 3\n", encoding="utf-8")
    if not installed:
        (root / "unrelated.py").write_text("PRIVATE = 1\n", encoding="utf-8")
    archive = fileutils.init_sample_file(SimpleNamespace(SAMPLE=2), str(tmp_path), "sample.tar", {}, "qtopconf.yaml", str(root))
    archive.close()
    with tarfile.open(tmp_path / "sample.tar") as saved:
        assert set(saved.getnames()) == {
            "qtopconf.yaml",
            "qtop_py/qtop.py",
            "qtop_py/plugins/slurm.py",
            "qtop_py/ui/viewport.py",
            "qtop_py/web/index.html",
            "helpfile.txt",
            "LICENSE",
            "qtop",
        }
        assert saved.extractfile("qtop_py/plugins/slurm.py").read() == b"VALUE = 2\n"
        assert saved.getmember("qtop").mode == 0o755


def test_expanded_capture_closes_archive_if_initial_population_fails(tmp_path, monkeypatch):
    archives = []
    original = fileutils.tarfile.open

    def opened(*args, **kwargs):
        archive = original(*args, **kwargs)
        archives.append(archive)
        return archive

    def fail(*args, **kwargs):
        raise RuntimeError("capture population failed")

    monkeypatch.setattr(fileutils.tarfile, "open", opened)
    monkeypatch.setattr(fileutils, "add_to_sample", fail)
    with pytest.raises(RuntimeError, match="capture population failed"):
        fileutils.init_sample_file(SimpleNamespace(SAMPLE=2, ANONYMIZE=False), str(tmp_path), "sample.tar", {}, "qtopconf.yaml", str(tmp_path))
    assert len(archives) == 1 and archives[0].closed
