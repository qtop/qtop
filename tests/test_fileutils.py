##
## qtop is a tool to monitor queuing systems - https://github.com/qtop/qtop
##
## Copyright (c) 2026 Evangelos Papadopoulos
##
## SPDX-License-Identifier: MIT
##

import tarfile
from types import SimpleNamespace

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
