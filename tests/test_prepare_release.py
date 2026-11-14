# Copyright (c) 2026 qtop contributors
# SPDX-License-Identifier: MIT
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
