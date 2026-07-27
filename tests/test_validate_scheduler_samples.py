import json
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

from tools.validate_scheduler_samples import (
    CASE_COLOUR_EXPECTATIONS,
    COMMON_COLOUR_EXPECTATIONS,
    ROOT,
    analyse_ansi_evidence,
    discover_cases,
    inspect_ansi,
    run_case,
    sanitize_artifact_text,
    write_svg_screenshot,
)


def coloured_span(sgr, text):
    return "\x1b[%sm%s\x1b[0;m" % (sgr, text)


def expected_colour_output(overrides=None, omitted=None):
    overrides = overrides or {}
    omitted = set(omitted or [])
    spans = []
    for expectation in COMMON_COLOUR_EXPECTATIONS:
        if expectation["semantic"] in omitted:
            continue
        sgr = overrides.get(expectation["semantic"], expectation["sgr"])
        spans.append(coloured_span(sgr, expectation["text"]))
    return "\n".join(spans) + "\n"


def test_discover_cases_covers_all_shared_validation_backends():
    cases = discover_cases(["pbs", "sge", "slurm", "oar", "demo"], ROOT / "tests/plugins/slurm_samples")

    schedulers = {case["scheduler"] for case in cases}
    names = {case["name"] for case in cases}

    assert schedulers == {"pbs", "sge", "slurm", "oar", "demo"}
    assert {"pbs-contrib", "sge-contrib", "oar-contrib", "demo-generated"} <= names
    assert any(name.startswith("slurm-") for name in names)
    assert all(case["colour_expectations"][: len(COMMON_COLOUR_EXPECTATIONS)] == COMMON_COLOUR_EXPECTATIONS for case in cases)
    sge_case = next(case for case in cases if case["name"] == "sge-contrib")
    assert sge_case["colour_expectations"] == COMMON_COLOUR_EXPECTATIONS + CASE_COLOUR_EXPECTATIONS["sge-contrib"]


def test_shared_validation_cases_request_coloured_output():
    cases = discover_cases(["pbs", "sge", "slurm", "oar", "demo"], ROOT / "tests/plugins/slurm_samples")

    for case in cases:
        args = case.get("args", ["-c", "ON"])
        assert args[args.index("-c") + 1] == "ON"


def test_ansi_evidence_proves_each_expected_semantic_mapping():
    evidence = analyse_ansi_evidence(expected_colour_output(), COMMON_COLOUR_EXPECTATIONS)

    assert evidence["ok"] is True
    assert evidence["syntax_ok"] is True
    assert evidence["mappings_ok"] is True
    assert evidence["missing_mappings"] == []
    assert evidence["wrong_mappings"] == []
    assert all(mapping["expected_occurrences"] == 1 for mapping in evidence["expected_mappings"])


def test_ansi_evidence_reports_missing_and_wrong_semantic_mappings():
    output = expected_colour_output(
        overrides={"summary label": "1;35"},
        omitted={"job-count label"},
    )

    evidence = analyse_ansi_evidence(output, COMMON_COLOUR_EXPECTATIONS)

    assert evidence["syntax_ok"] is True
    assert evidence["ok"] is False
    assert evidence["missing_mappings"] == ["job-count label"]
    assert evidence["wrong_mappings"] == [
        {
            "semantic": "summary label",
            "expected_sgr": "1;36",
            "observed_sgr": ["1;35"],
        }
    ]


@pytest.mark.parametrize(
    "output",
    [
        "\x1b[2Jcursor movement",
        "\x1b]0;title\x07OSC title",
        "\x1bPdata\x1b\\DCS payload",
        "\x9b31mC1 CSI",
        "bell\x07",
        "\x1b[999munknown SGR\x1b[0;m",
        "\x1b[31incomplete SGR",
    ],
)
def test_ansi_inspection_rejects_non_sgr_unknown_and_incomplete_controls(output):
    evidence = inspect_ansi(output)

    assert evidence["syntax_ok"] is False
    assert evidence["invalid_sequences"]


def test_ansi_inspection_rejects_unclosed_nested_and_unmatched_styles():
    unclosed = inspect_ansi("\x1b[1;31mred")
    nested = inspect_ansi("\x1b[1;31mred\x1b[1;32mgreen\x1b[0;m")
    unmatched = inspect_ansi("\x1b[0;m")

    assert unclosed["unclosed_style"] == {"bold": True, "foreground": 31, "background": None}
    assert nested["syntax_ok"] is False
    assert "previous style was reset" in nested["invalid_sequences"][0]["reason"]
    assert unmatched["syntax_ok"] is False
    assert "no matching open style" in unmatched["invalid_sequences"][0]["reason"]


@pytest.mark.parametrize("sgr", [";41", "0;30;41", "1;31;40", "0;35;40"])
def test_ansi_inspection_accepts_qtop_foreground_background_combinations(sgr):
    evidence = inspect_ansi(coloured_span(sgr, "styled"))

    assert evidence["syntax_ok"] is True


def test_svg_preserves_foreground_background_and_newline_state_safely(tmp_path):
    svg_path = tmp_path / "terminal.svg"
    output = "\x1b[1;31;40m<&\nnext\x1b[0;m\n"

    write_svg_screenshot(svg_path, output)

    svg = svg_path.read_text(encoding="utf-8")
    assert 'xml:space="preserve"' in svg
    assert svg.count('fill="#f14c4c"') == 2
    assert svg.count('fill="#000000"') == 2
    assert "&lt;&amp;" in svg
    assert ">next</text>" in svg
    assert "\x1b" not in svg
    assert "<&" not in svg


def test_svg_stops_at_line_limit_without_concatenating_later_lines(tmp_path):
    svg_path = tmp_path / "terminal.svg"
    output = "\n".join("line-%02d" % index for index in range(45)) + "\n"

    write_svg_screenshot(svg_path, output, max_lines=3)

    svg = svg_path.read_text(encoding="utf-8")
    assert "line-00" in svg
    assert "line-02" in svg
    assert "line-03" not in svg
    assert "line-44" not in svg


def test_artifact_path_sanitizer_uses_neutral_placeholders(tmp_path):
    qtop_home = ROOT / "artifacts" / "sample-gate" / "case" / "home"
    text = "%s/qtop.py %s/.local/qtop/logs/qtop.log executable=%s" % (
        ROOT,
        qtop_home,
        sys.executable,
    )

    sanitized = sanitize_artifact_text(text, qtop_home)

    assert sanitized == "<repo>/qtop.py <qtop-home>/.local/qtop/logs/qtop.log executable=<python>"
    assert str(ROOT) not in sanitized
    assert str(sys.executable) not in sanitized


def test_run_case_writes_path_neutral_per_case_ansi_evidence(tmp_path, monkeypatch):
    expectation = {
        "semantic": "summary label",
        "text": "Summary",
        "colour": "Cyan_L",
        "sgr": "1;36",
    }

    observed_homes = []

    def fake_run(command, **kwargs):
        observed_homes.append(kwargs["env"]["HOME"])
        stdout = "repo=%s home=%s\n%s\nmarker\n" % (
            ROOT,
            kwargs["env"]["HOME"],
            coloured_span("1;36", "Summary"),
        )
        return SimpleNamespace(returncode=0, stdout=stdout, stderr="root=%s\n" % ROOT)

    monkeypatch.setattr("tools.validate_scheduler_samples.subprocess.run", fake_run)
    case = {
        "name": "synthetic",
        "scheduler": "demo",
        "source": ROOT,
        "args": ["-c", "ON", "-b", "demo"],
        "markers": ["marker"],
        "colour_expectations": [expectation],
    }

    result = run_case(case, tmp_path / "artifacts", timeout=1)
    case_dir = tmp_path / "artifacts" / "synthetic"
    case_summary = json.loads((case_dir / "summary.json").read_text(encoding="utf-8"))

    assert result["ok"] is True
    assert case_summary == result
    assert case_summary["ansi_evidence"]["ok"] is True
    assert result["command"][0] == "<python>"
    assert not (case_dir / "home").exists()
    assert len(observed_homes) == 1
    assert not Path(observed_homes[0]).exists()
    for artifact_name in [
        "stdout.ans",
        "rendered.normalized.txt",
        "stderr.log",
        "command.txt",
        "screenshot.svg",
        "summary.json",
    ]:
        artifact = (case_dir / artifact_name).read_text(encoding="utf-8")
        assert str(ROOT) not in artifact
        assert observed_homes[0] not in artifact


def test_run_case_fails_when_semantic_colour_is_wrong(tmp_path, monkeypatch):
    expectation = {
        "semantic": "summary label",
        "text": "Summary",
        "colour": "Cyan_L",
        "sgr": "1;36",
    }

    def fake_run(command, **kwargs):
        return SimpleNamespace(
            returncode=0,
            stdout=coloured_span("1;35", "Summary") + "\nmarker\n",
            stderr="",
        )

    monkeypatch.setattr("tools.validate_scheduler_samples.subprocess.run", fake_run)
    case = {
        "name": "wrong-colour",
        "scheduler": "demo",
        "source": ROOT,
        "args": ["-c", "ON", "-b", "demo"],
        "markers": ["marker"],
        "colour_expectations": [expectation],
    }

    result = run_case(case, tmp_path / "artifacts", timeout=1)

    assert result["ok"] is False
    assert result["error"] == "ANSI colour evidence failed"
    assert result["ansi_evidence"]["wrong_mappings"][0]["observed_sgr"] == ["1;35"]


def test_run_case_escapes_unsafe_output_and_refuses_to_render_it(tmp_path, monkeypatch):
    def fake_run(command, **kwargs):
        return SimpleNamespace(
            returncode=0,
            stdout="marker \x1b]0;unexpected title\x07\n",
            stderr="",
        )

    monkeypatch.setattr("tools.validate_scheduler_samples.subprocess.run", fake_run)
    case = {
        "name": "unsafe-output",
        "scheduler": "demo",
        "source": ROOT,
        "args": ["-c", "ON", "-b", "demo"],
        "markers": ["marker"],
        "colour_expectations": [],
    }

    result = run_case(case, tmp_path / "artifacts", timeout=1)
    case_dir = tmp_path / "artifacts" / "unsafe-output"
    stdout_artifact = (case_dir / "stdout.ans").read_text(encoding="utf-8")
    svg = (case_dir / "screenshot.svg").read_text(encoding="utf-8")

    assert result["ok"] is False
    assert result["ansi_evidence"]["syntax_ok"] is False
    assert "\x1b" not in stdout_artifact
    assert "\\x1b]0;unexpected title\\x07" in stdout_artifact
    assert "unexpected title" not in svg
    assert "terminal output was not rendered" in svg


def test_run_case_removes_temporary_home_after_timeout(tmp_path, monkeypatch):
    observed_homes = []

    def fake_run(command, **kwargs):
        qtop_home = Path(kwargs["env"]["HOME"])
        observed_homes.append(qtop_home)
        (qtop_home / "private.log").write_text("private runner data", encoding="utf-8")
        raise subprocess.TimeoutExpired(command, 1, output="", stderr="")

    monkeypatch.setattr("tools.validate_scheduler_samples.subprocess.run", fake_run)
    case = {
        "name": "timeout",
        "scheduler": "demo",
        "source": ROOT,
        "args": ["-c", "ON", "-b", "demo"],
        "markers": [],
        "colour_expectations": [],
    }

    result = run_case(case, tmp_path / "artifacts", timeout=1)

    assert result["error"] == "timeout after 1 seconds"
    assert len(observed_homes) == 1
    assert not observed_homes[0].exists()
    assert not (tmp_path / "artifacts" / "timeout" / "home").exists()
