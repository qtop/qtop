## Copyright (c) 2026 qtop contributors
## SPDX-License-Identifier: MIT
import subprocess
import sys

import pytest

from tools import repo_sanity as fortifications


@pytest.mark.parametrize(
    "filename,source,rule",
    [
        ("qtop_py/bad.py", "from qtop_py.colormap import *", "F403"),
        ("qtop_py/good.py", "from qtop_py.colormap import color_to_code", None),
        ("tests/helper.py", "from qtop_py.colormap import *", "F403"),
        ("tools/broken.py", "def broken(:", "invalid-syntax"),
        ("tools/unsafe.py", "eval('payload')", "S307"),
        ("tools/unsafe.py", "eval('payload')  # noqa: S307", "S307"),
        ("tools/unsafe.py", "from module import *  # noqa: F403", "F403"),
        ("tools/unsafe.py", "exec('payload')", "S102"),
        ("tools/unsafe.py", "import subprocess; subprocess.run('payload', shell=True)", "S602"),
    ],
    ids=["wildcard-runtime", "explicit-import", "wildcard-test", "syntax", "eval", "suppressed-eval", "suppressed-wildcard", "exec", "shell"],
)
def test_ruff_owns_source_protections(tmp_path, filename, source, rule):
    result = subprocess.run(
        [sys.executable, "-m", "ruff", "check", "--select", "F403,S102,S307,S602", "--ignore-noqa", "--stdin-filename", filename, "-"],
        input=source,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        universal_newlines=True,
        cwd=str(tmp_path),
    )
    assert result.returncode == (1 if rule else 0), result.stderr
    assert rule in result.stdout if rule else "All checks passed" in result.stdout


def test_diff_reports_all_locations_and_escapes_unsafe_text():
    lines = [
        "diff --git a/docs/a file.md b/docs/a file.md",
        "--- a/docs/a file.md",
        "+++ b/docs/a file.md",
        "@@ -2,3 +7,4 @@",
        "-removed",
        "+first\u202e",
        " context",
        "+next\u00e9",
        "\\ No newline at end of file",
        "@@ -20 +30 @@",
        "+++ tricky\u2014",
        "diff --git a/docs/tab b/docs/tab",
        '--- "a/docs/tab\\tname.md"',
        '+++ "b/docs/tab\\tname.md"',
        "@@ -0,0 +42 @@",
        "+\u00e9",
    ]
    problems = fortifications.find_diff_problems(lines)
    assert len(problems) == 4
    assert problems[0].startswith("docs/a file.md:7:6:")
    assert "U+202E" in problems[0]
    assert problems[1].startswith("docs/a file.md:9:5:")
    assert problems[2].startswith("docs/a file.md:30:10:")
    assert problems[3].startswith("docs/tab\\tname.md:42:1:")
    assert all(problem.isascii() for problem in problems)
    assert "\\u202e" in problems[0]


def test_diff_ignores_deleted_nonascii_and_file_metadata():
    lines = [
        "diff --git a/deleted.md b/deleted.md",
        "--- a/deleted.md",
        "+++ /dev/null",
        "@@ -1 +0,0 @@",
        "-\u2014",
        "diff --git a/notes.md b/notes.md",
        "--- a/notes.md",
        "+++ b/notes.md",
        "@@ -1 +1 @@",
        "-\u2014",
        "+plain ASCII",
    ]
    assert fortifications.find_diff_problems(lines) == []


@pytest.mark.parametrize("staged", (False, True))
def test_main_checks_pending_fixes_and_pending_violations(monkeypatch, tmp_path, capsys, staged):
    def git(*args):
        return subprocess.check_output(
            [
                "git",
                "-c",
                "user.name=Qtop test",
                "-c",
                "user.email=test@example.invalid",
                "-c",
                "commit.gpgsign=false",
                "-c",
                "core.hooksPath=" + str(tmp_path / "no-hooks"),
                *args,
            ],
            cwd=str(tmp_path),
            universal_newlines=True,
        ).strip()

    git("init", "-q")
    source = tmp_path / "AGENTS.md"
    source.write_text("heading-\n")
    git("add", "AGENTS.md")
    git("commit", "-qm", "base")
    base = git("rev-parse", "HEAD")
    source.write_text("heading\u2014\n", encoding="utf-8")
    git("commit", "-qam", "branch violation")
    monkeypatch.setattr(fortifications, "ROOT", tmp_path)
    assert fortifications.check_diff(base) == 1
    assert "AGENTS.md:1:8:" in capsys.readouterr().err

    source.write_text("heading-\n")
    if staged:
        git("add", "AGENTS.md")
    assert fortifications.check_diff(base) == 0
    assert "fortifications: ok" in capsys.readouterr().out

    source.write_text("heading\u202e\n", encoding="utf-8")
    if staged:
        git("add", "AGENTS.md")
    assert fortifications.check_diff(base) == 1
    assert "U+202E" in capsys.readouterr().err

    source.write_text("heading\u2028end\n", encoding="utf-8")
    assert fortifications.check_diff(base) == 1
    diagnostic = capsys.readouterr().err
    assert "AGENTS.md:1:8:" in diagnostic
    assert "U+2028" in diagnostic
