## Copyright (c) 2026 qtop contributors
## SPDX-License-Identifier: MIT
import argparse
import errno
import json
import os
import subprocess

import pytest

from tools import repo_sanity


def test_run_audit_scans_tracked_file_under_formerly_skipped_path(monkeypatch, tmp_path):
    source = tmp_path / "artifacts" / "planted.py"
    source.parent.mkdir()
    source.write_text('access = "user\u202e"\n', encoding="utf-8")
    monkeypatch.setattr(
        repo_sanity.subprocess,
        "check_output",
        lambda *args, **kwargs: b"artifacts/planted.py\0",
    )

    report_dir = tmp_path / "report"
    assert repo_sanity.run_audit(tmp_path, report_dir, strict=False) == 1

    findings = json.loads((report_dir / "findings.json").read_text(encoding="utf-8"))
    assert any(finding["path"] == "artifacts/planted.py" and finding["severity"] == "CRITICAL" and "bidirectional control character" in finding["message"] for finding in findings)


def test_tracked_files_reports_git_command_failure(monkeypatch, tmp_path):
    def fail(*args, **kwargs):
        raise subprocess.CalledProcessError(128, ["git", "ls-files", "-z"])

    monkeypatch.setattr(repo_sanity.subprocess, "check_output", fail)

    with pytest.raises(repo_sanity.TrackedFileDiscoveryError, match="valid Git worktree"):
        repo_sanity.tracked_files(tmp_path)


def test_tracked_files_preserves_non_utf8_filename_bytes(monkeypatch, tmp_path):
    monkeypatch.setattr(
        repo_sanity.subprocess,
        "check_output",
        lambda *args, **kwargs: b"tracked-\xff.py\0",
    )

    paths = repo_sanity.tracked_files(tmp_path)

    assert len(paths) == 1
    assert os.fsencode(paths[0].name) == b"tracked-\xff.py"
    try:
        paths[0].write_text('access = "user\u202e"\n', encoding="utf-8")
    except OSError as exc:
        if exc.errno != errno.EILSEQ:
            raise
        pytest.skip("filesystem rejects non-UTF-8 filenames (e.g. macOS APFS)")
    report_dir = tmp_path / "report"
    assert repo_sanity.run_audit(tmp_path, report_dir, strict=False) == 1
    report = (report_dir / "report.txt").read_text(encoding="utf-8")
    assert "tracked-\\udcff.py" in report


def test_reports_use_neutral_repository_label(tmp_path):
    report_dir = tmp_path / "report"

    _, report = repo_sanity.write_reports([], report_dir, scanned=0)

    assert "root: <repo>\n" in report
    assert str(repo_sanity.ROOT) not in report
    assert str(repo_sanity.ROOT) not in (report_dir / "findings.json").read_text(encoding="utf-8")


def test_main_reports_missing_git_without_traceback(monkeypatch, tmp_path, capsys):
    args = argparse.Namespace(base_ref=None, report_dir=str(tmp_path / "report"), strict=False, selftest=False)
    monkeypatch.setattr(repo_sanity, "parse_args", lambda: args)

    def fail(*args, **kwargs):
        raise OSError("git executable not found")

    monkeypatch.setattr(repo_sanity.subprocess, "check_output", fail)

    assert repo_sanity.main() == 2
    assert capsys.readouterr().err == "repo-sanity error: Git is required to enumerate tracked files: git executable not found\n"


@pytest.mark.parametrize(
    "codepoint",
    [
        0x08,
        0x0B,
        0x0C,
        0x1B,
        0x1C,
        0x1D,
        0x1E,
        0x7F,
        0x80,
        0x85,
        0x9F,
        0xAD,
        0x34F,
        0x61C,
        0x180E,
        *range(0x200B, 0x2010),
        *range(0x2028, 0x202F),
        0x2060,
        *range(0x2066, 0x206A),
        0xFEFF,
    ],
    ids=lambda codepoint: "U+%04X" % codepoint,
)
def test_hidden_controls_cannot_disappear_during_line_splitting(tmp_path, codepoint):
    path = tmp_path / "source.py"
    path.write_bytes(("visible" + chr(codepoint) + "hidden\n").encode("utf-8"))
    findings = []
    repo_sanity.scan_file(path, "source.py", findings)
    assert any(row[0] == "CRITICAL" and "U+%04X" % codepoint in row[4] for row in findings)


@pytest.mark.parametrize(
    "text,severity",
    [("p\u0430ssword", "WARNING"), ("p\u03b1ssword", "WARNING"), ("p\uff41ssword", "WARNING"), ("\ufeffsafe", "WARNING"), ("plain ASCII\r\n", "INFO"), ("bad\ufffd", "INFO")],
    ids=["cyrillic", "greek", "fullwidth", "leading-bom", "crlf", "replacement"],
)
def test_text_policy_keeps_confusables_and_encoding_distinct(tmp_path, text, severity):
    path = tmp_path / "source.py"
    path.write_bytes(text.encode("utf-8"))
    findings = []
    repo_sanity.scan_file(path, "source.py", findings)
    assert findings and findings[0][0] == severity


def test_terminal_fixture_exceptions_reject_planted_attacks(tmp_path):
    assert repo_sanity.selftest(tmp_path / "proof") == 0
