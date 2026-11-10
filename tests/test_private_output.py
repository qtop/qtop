##
## qtop is a tool to monitor queuing systems - https://github.com/qtop/qtop
##
## Copyright (c) 2026 qtop contributors
##
## SPDX-License-Identifier: MIT
##

import os
import stat
import subprocess
import sys
from pathlib import Path

import pytest

from tools import validate_pbs_samples
from tools.private_output import private_output_dir

ROOT = Path(__file__).resolve().parents[1]
posix_only = pytest.mark.skipif(os.name != "posix", reason="ownership and mode checks need POSIX")


@posix_only
def test_new_output_directories_are_private(tmp_path):
    created = private_output_dir(tmp_path / "a" / "rendered")
    assert created.is_dir() and stat.S_IMODE(created.stat().st_mode) == 0o700


def test_existing_own_directory_is_accepted(tmp_path):
    existing = tmp_path / "rendered"
    existing.mkdir(mode=0o755)
    assert private_output_dir(existing) == existing


@posix_only
@pytest.mark.parametrize("mode", [0o775, 0o757, 0o777])
def test_group_or_world_writable_directories_are_refused(tmp_path, mode):
    shared = tmp_path / "rendered"
    shared.mkdir()
    shared.chmod(mode)
    with pytest.raises(ValueError, match="writable"):
        private_output_dir(shared)


@posix_only
def test_symlinked_output_is_refused_without_touching_its_target(tmp_path):
    target = tmp_path / "victim"
    target.mkdir()
    (tmp_path / "rendered").symlink_to(target)
    with pytest.raises(ValueError, match="symlink"):
        private_output_dir(tmp_path / "rendered")
    assert list(target.iterdir()) == []


@posix_only
def test_foreign_owned_directory_is_refused(tmp_path, monkeypatch):
    monkeypatch.setattr(os, "geteuid", lambda: os.lstat(str(tmp_path)).st_uid + 1)
    with pytest.raises(ValueError, match="another user"):
        private_output_dir(tmp_path)


def test_files_are_not_mistaken_for_directories(tmp_path):
    (tmp_path / "rendered").write_text("")
    with pytest.raises(ValueError, match="not a directory"):
        private_output_dir(tmp_path / "rendered")


def test_defaults_are_repo_local_artifacts():
    assert str(validate_pbs_samples.DEFAULT_OUTPUT) == "artifacts/pbs-rendered"
    makefile = (ROOT / "Makefile").read_text()
    assert "PBS_OUTPUT_DIR ?= artifacts/pbs-rendered" in makefile and "SLURM_OUTPUT_DIR ?= artifacts/slurm-rendered" in makefile


@posix_only
@pytest.mark.parametrize("tool, flag", [("validate_pbs_samples.py", "--output"), ("validate_scheduler_samples.py", "--artifact-dir")])
def test_tools_run_as_scripts_reject_a_planted_symlink(tmp_path, tool, flag):
    victim = tmp_path / "victim"
    victim.mkdir()
    planted = tmp_path / "rendered"
    planted.symlink_to(victim)
    result = subprocess.run(
        [sys.executable, str(ROOT / "tools" / tool)] + ([str(tmp_path)] if flag == "--output" else []) + [flag, str(planted)],
        cwd=str(ROOT),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        universal_newlines=True,
    )
    assert result.returncode == 2 and "symlink" in result.stderr
    assert list(victim.iterdir()) == []
