# Copyright (c) 2026 qtop contributors
# SPDX-License-Identifier: MIT
import os
import shutil
import subprocess
import sys

import pytest

from qtop_py import __version__
from tools import prepare_release
from tools.prepare_release import ROOT, release_plan


def test_release_preview_preserves_history_and_does_not_write():
    before = (ROOT / "CHANGELOG.md").read_text()
    edits = release_plan(ROOT, "0.9.20990101")
    assert (ROOT / "CHANGELOG.md").read_text() == before
    assert edits["CHANGELOG.md"].split("## " + __version__, 1)[1] == before.split("## " + __version__, 1)[1]
    assert '"2099-01-01" "qtop 0.9.20990101"' in edits["docs/qtop.1"]
    assert edits["docs/qtop-%s-GCCcore-14.3.0.eb" % __version__] is None


@pytest.mark.parametrize("version", ("0.9.20260230", __version__, "bad"))
def test_invalid_or_nonadvancing_releases_are_rejected(version):
    with pytest.raises(ValueError):
        release_plan(ROOT, version)


@pytest.mark.parametrize("write", (False, True))
def test_release_cli_writes_only_when_requested(tmp_path, monkeypatch, write):
    monkeypatch.setattr(prepare_release, "ROOT", tmp_path)
    monkeypatch.setattr(prepare_release, "release_plan", lambda *args: {"version.txt": "new"})
    monkeypatch.setattr(prepare_release.subprocess, "check_output", lambda *args, **kwargs: "")
    monkeypatch.setattr(prepare_release.subprocess, "call", lambda *args, **kwargs: 1)
    monkeypatch.setattr("sys.argv", ["prepare-release", "--version", "0.9.20990101"] + (["--write"] if write else []))
    prepare_release.main()
    assert (tmp_path / "version.txt").exists() is write


@pytest.mark.parametrize(
    ("tag", "passes"),
    (("v" + __version__, True), (__version__, False), ("v0.9.20990101", False), ("", False)),
    ids=("matching", "missing-v", "other-version", "unset"),
)
def test_release_tag_check_requires_v_package_version(tag, passes):
    make = shutil.which("make")
    if make is None:
        pytest.skip("make is not installed")
    env = dict(os.environ, RELEASE_TAG=tag)
    result = subprocess.run(
        [make, "--no-print-directory", "release-tag-check", "PYTHON=" + sys.executable],
        cwd=str(ROOT),
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        universal_newlines=True,
    )
    assert (result.returncode == 0) is passes, result.stdout
    if tag and not passes:
        assert "does not match package version " + __version__ in result.stdout


def test_release_write_stages_recipe_rename_with_git_mv(tmp_path, monkeypatch):
    git = shutil.which("git")
    if git is None:
        pytest.skip("git is not installed")
    old, new = "recipe-0.9.20260101.eb", "recipe-0.9.20990101.eb"
    subprocess.check_call([git, "init", "-q", str(tmp_path)])
    (tmp_path / old).write_text("version = '0.9.20260101'\n")
    subprocess.check_call([git, "add", old], cwd=str(tmp_path))
    identity = ["-c", "user.name=qtop", "-c", "user.email=qtop@example.invalid", "-c", "commit.gpgsign=false"]
    subprocess.check_call([git] + identity + ["commit", "-q", "-m", "init"], cwd=str(tmp_path))
    check_output = subprocess.check_output
    monkeypatch.setattr(prepare_release, "ROOT", tmp_path)
    monkeypatch.setattr(prepare_release, "release_plan", lambda *args: {old: None, new: "version = '0.9.20990101'\n"})
    monkeypatch.setattr(prepare_release.subprocess, "check_output", lambda *args, **kwargs: "")
    monkeypatch.setattr("sys.argv", ["prepare-release", "--version", "0.9.20990101", "--write"])
    prepare_release.main()
    status = check_output([git, "status", "--porcelain"], cwd=str(tmp_path), universal_newlines=True)
    assert status.startswith("R") and "%s -> %s" % (old, new) in status
    assert (tmp_path / new).read_text() == "version = '0.9.20990101'\n"
    assert not (tmp_path / old).exists()
