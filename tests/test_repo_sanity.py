import argparse
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


@pytest.mark.skipif(os.name == "nt", reason="Non-UTF-8 byte filenames are unsupported on Windows NTFS")
def test_tracked_files_preserves_non_utf8_filename_bytes(monkeypatch, tmp_path):
    source = tmp_path / os.fsdecode(b"tracked-\xff.py")
    source.write_text('access = "user\u202e"\n', encoding="utf-8")
    monkeypatch.setattr(
        repo_sanity.subprocess,
        "check_output",
        lambda *args, **kwargs: b"tracked-\xff.py\0",
    )

    paths = repo_sanity.tracked_files(tmp_path)

    assert len(paths) == 1
    assert os.fsencode(paths[0].name) == b"tracked-\xff.py"
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
    args = argparse.Namespace(report_dir=str(tmp_path / "report"), strict=False, selftest=False)
    monkeypatch.setattr(repo_sanity, "parse_args", lambda: args)

    def fail(*args, **kwargs):
        raise OSError("git executable not found")

    monkeypatch.setattr(repo_sanity.subprocess, "check_output", fail)

    assert repo_sanity.main() == 2
    assert capsys.readouterr().err == "repo-sanity error: Git is required to enumerate tracked files: git executable not found\n"
