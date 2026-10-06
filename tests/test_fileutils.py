##
## qtop is a tool to monitor queuing systems - https://github.com/qtop/qtop
##
## Copyright (c) 2026 Evangelos Papadopoulos
##
## SPDX-License-Identifier: MIT
##

import errno
import tarfile
from types import SimpleNamespace

import pytest

from qtop_py import fileutils


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
        assert set(saved_archive.getnames()) == {"qtopconf.yaml", "qtop_py/example.py"}


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
