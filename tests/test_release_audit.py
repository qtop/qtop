## Copyright (c) 2026 qtop contributors
## SPDX-License-Identifier: MIT
import shutil
import subprocess
import sys

import pytest

from tools import release_audit
from tools.release_audit import audit


def test_audit_reports_each_kind_of_release_drift():
    headings = ["0.9.20261125", "0.9.20261124", "0.9.20261123", "0.9.20261010", "0.9.20261001", "0.8.3"]
    tags = {
        "v0.9.20261123": (True, "0.9.20261123"),  # clean
        "0.9.20261011": (False, "0.9.20261001"),  # bare, lightweight, no heading, stale version
        "0.9.20261001": (True, "0.9.20261001"),  # bare only
        "0.9.test": (False, "0.9.20161130"),
        "v0.8.4": (False, None),  # before since: not audited
    }
    assert audit(headings, tags, "0.9.20261125", since="0.9.20161130") == [
        "0.9.20261124: CHANGELOG section has no tag",
        "0.9.20261010: CHANGELOG section has no tag",
        "0.9.20261001: tagged only as bare 0.9.20261001, no v0.9.20261001",
        "0.9.test: not a release tag name (v<version>)",
        "0.9.20261011: tag has no CHANGELOG section 0.9.20261011",
        "0.9.20261011: lightweight tag (release tags are annotated: git tag -a)",
        "0.9.20261011: __version__ inside the tag is 0.9.20261001",
    ]


def test_audit_skips_the_untagged_top_section_and_history_before_since():
    assert audit(["0.9.20261125", "0.8.3"], {}, "0.9.20261125", since="0.9.20161130") == []


@pytest.mark.parametrize("strict", (False, True))
def test_release_audit_cli_is_non_blocking_unless_strict(monkeypatch, capsys, strict):
    monkeypatch.setattr(release_audit, "git", lambda *args: "v0.9.20990101 tag\n" if args[0] == "for-each-ref" else '__version__ = "0.9.20990101"')
    monkeypatch.setattr(release_audit, "audit", lambda headings, tags, *rest: ["%d tag: %s" % (len(tags), tags["v0.9.20990101"])])
    monkeypatch.setattr(sys, "argv", ["release-audit"] + (["--strict"] if strict else []))
    assert release_audit.main() == int(strict)
    out = capsys.readouterr().out
    assert "1 tag: (True, '0.9.20990101')" in out and ("(non-blocking)" in out) is not strict


@pytest.mark.parametrize(
    "show, grep, expected",
    (('__version__ = "1.2"', None, "1.2"), (None, 'v1:qtop.py:__version__="1.1"', "1.1"), ("", 'v1:qtop.py:__version__="1.1"', "1.1"), (None, None, None)),
    ids=("package", "old-layout", "empty-package", "none"),
)
def test_tag_version_falls_back_to_older_layouts(monkeypatch, show, grep, expected):
    def git(*args):
        output = show if args[0] == "show" else grep
        if output is None:
            raise subprocess.CalledProcessError(128, "git")
        return output

    monkeypatch.setattr(release_audit, "git", git)
    assert release_audit.tag_version("v1") == expected


@pytest.mark.skipif(shutil.which("git") is None, reason="git is not installed")
def test_git_helper_returns_text_output():
    assert release_audit.git("--version").startswith("git version")
