import subprocess
from types import SimpleNamespace

import pytest

from tools import fortifications


def test_find_star_imports_reports_wildcard_imports(monkeypatch, tmp_path):
    source = tmp_path / "qtop_py" / "bad_import.py"
    source.parent.mkdir()
    source.write_text("from qtop_py.colormap import *\n", encoding="utf-8")

    monkeypatch.setattr(fortifications, "ROOT", tmp_path)
    monkeypatch.setattr(fortifications, "iter_python_files", lambda: [source])

    assert fortifications.find_star_imports() == ["qtop_py/bad_import.py:1 wildcard import is not allowed"]


def test_find_star_imports_accepts_explicit_imports(monkeypatch, tmp_path):
    source = tmp_path / "qtop_py" / "good_import.py"
    source.parent.mkdir()
    source.write_text("from qtop_py.colormap import color_to_code\n", encoding="utf-8")

    monkeypatch.setattr(fortifications, "ROOT", tmp_path)
    monkeypatch.setattr(fortifications, "iter_python_files", lambda: [source])

    assert fortifications.find_star_imports() == []


def test_find_star_imports_ignores_tests(monkeypatch, tmp_path):
    source = tmp_path / "tests" / "helper.py"
    source.parent.mkdir()
    source.write_text("from qtop_py.colormap import *\n", encoding="utf-8")

    monkeypatch.setattr(fortifications, "ROOT", tmp_path)
    monkeypatch.setattr(fortifications, "iter_python_files", lambda: [source])

    assert fortifications.find_star_imports() == []


def test_find_star_imports_reports_syntax_errors(monkeypatch, tmp_path):
    source = tmp_path / "tools" / "broken.py"
    source.parent.mkdir()
    source.write_text("def broken(:\n", encoding="utf-8")

    monkeypatch.setattr(fortifications, "ROOT", tmp_path)
    monkeypatch.setattr(fortifications, "iter_python_files", lambda: [source])

    problems = fortifications.find_star_imports()
    assert len(problems) == 1
    assert problems[0].startswith("tools/broken.py:1 syntax error while scanning imports: ")


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
    monkeypatch.setattr(fortifications, "parse_args", lambda: SimpleNamespace(base_ref=base))
    assert fortifications.main() == 1
    assert "AGENTS.md:1:8:" in capsys.readouterr().err

    source.write_text("heading-\n")
    if staged:
        git("add", "AGENTS.md")
    assert fortifications.main() == 0
    assert "fortifications: ok" in capsys.readouterr().out

    source.write_text("heading\u202e\n", encoding="utf-8")
    if staged:
        git("add", "AGENTS.md")
    assert fortifications.main() == 1
    assert "U+202E" in capsys.readouterr().err

    source.write_text("heading\u2028end\n", encoding="utf-8")
    assert fortifications.main() == 1
    diagnostic = capsys.readouterr().err
    assert "AGENTS.md:1:8:" in diagnostic
    assert "U+2028" in diagnostic
