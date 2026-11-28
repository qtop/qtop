#!/usr/bin/env python3
##
## qtop is a tool to monitor queuing systems - https://github.com/qtop/qtop
##
## Copyright (c) 2026 Jacob Hatchett
##
## SPDX-License-Identifier: MIT
##

"""Shared fast qtop sample gate for CI and local review.

The gate intentionally uses small committed scheduler traces, so GitHub and
GitLab can run the same command without access to the larger artifact corpus.
"""

import argparse
import html
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

try:
    from tools.private_output import private_output_dir
except ImportError:  # run as a script: tools/ itself is sys.path[0]
    from private_output import private_output_dir

ROOT = Path(__file__).resolve().parents[1]
ANSI_SGR_RE = re.compile(r"\x1b\[([0-9;]*)m")
ANSI_RE = ANSI_SGR_RE
UNSAFE_CONTROL_RE = re.compile(r"[\x00-\x08\x0b-\x1a\x1c-\x1f\x7f-\x9f]")

ORANGE_BACKGROUND = 48208  # 48;5;208, folded into one parameter by sgr_parameters
ALLOWED_SGR_PARAMETERS = set([0, 1, ORANGE_BACKGROUND] + list(range(30, 38)) + list(range(40, 48)))

# Stable presentation semantics shared by every committed backend sample.
# Each witness includes its closing reset, so a colour code elsewhere cannot
# make the case pass.
COMMON_COLOUR_EXPECTATIONS = [
    {"semantic": "section opening delimiter", "text": "===> ", "colour": "Gray_D", "sgr": "1;30"},
    {"semantic": "section closing delimiter", "text": " <=== ", "colour": "Gray_D", "sgr": "1;30"},
    {"semantic": "accounting section title", "text": "1. Job accounting summary", "colour": "White", "sgr": "1;37"},
    {"semantic": "summary label", "text": "Summary", "colour": "Cyan_L", "sgr": "1;36"},
    {"semantic": "node-count label", "text": "Nodes", "colour": "Red_L", "sgr": "1;31"},
    {"semantic": "cpu-count label", "text": "cpus", "colour": "Green_L", "sgr": "1;32"},
    {"semantic": "job-count label", "text": "jobs", "colour": "Blue_L", "sgr": "1;34"},
    {"semantic": "blocked-queue warning", "text": "* implies blocked", "colour": "Red", "sgr": "0;31"},
]

ANSI_NORMAL_COLOURS = {
    30: "#000000",
    31: "#cd3131",
    32: "#0dbc79",
    33: "#e5e510",
    34: "#2472c8",
    35: "#bc3fbc",
    36: "#11a8cd",
    37: "#e5e5e5",
}

ANSI_BRIGHT_COLOURS = {
    30: "#666666",
    31: "#f14c4c",
    32: "#23d18b",
    33: "#f5f543",
    34: "#3b8eea",
    35: "#d670d6",
    36: "#29b8db",
    37: "#ffffff",
}

# Each sample directory, tests/plugins/<scheduler>_samples/<case>/, carries its own case in
# SAMPLE_GATE_FILE, so a backend brings its samples without editing this tool:
#   {"source": "qtop_py/contrib" or null (optional), "args": ["-F"], "markers": [...],
#    "colour_expectations": [...] (optional, added to COMMON_COLOUR_EXPECTATIONS)}
# source defaults to the case directory; null runs without -s (generated demo data).
SAMPLES_ROOT = ROOT / "tests" / "plugins"
SAMPLE_GATE_FILE = "sample-gate.json"


def sgr_parameters(raw_parameters):
    """Return the integer parameters of an SGR escape; empty means reset (0)."""
    parameters = [0 if item == "" else int(item) for item in re.sub(r"(^|;)48;5;208(?=;|$)", r"\g<1>%d" % ORANGE_BACKGROUND, raw_parameters).split(";")]
    return parameters if raw_parameters else [0]


def sgr_parameters_allowed(raw_parameters):
    """Return True if every SGR parameter is one qtop may emit."""
    return all(parameter in ALLOWED_SGR_PARAMETERS for parameter in sgr_parameters(raw_parameters))


def reset_style():
    """Return the default terminal style."""
    return {"bold": False, "foreground": None, "background": None}


def apply_sgr(style, parameters):
    """Return style updated by SGR parameters."""
    updated = dict(style)
    for parameter in parameters:
        if parameter == 0:
            updated = reset_style()
        elif parameter == 1:
            updated["bold"] = True
        elif 30 <= parameter <= 37:
            updated["foreground"] = parameter
        elif 40 <= parameter <= 47 or parameter == ORANGE_BACKGROUND:
            updated["background"] = parameter
    return updated


def escaped_control(value):
    """Return a control character as a visible escape."""
    return value.encode("unicode_escape").decode("ascii")


def inspect_ansi(text):
    """Allow only the small ANSI SGR subset emitted by qtop."""
    invalid_sequences = []
    sequence_counts = {}
    style = reset_style()

    for match in UNSAFE_CONTROL_RE.finditer(text):
        invalid_sequences.append(
            {
                "offset": match.start(),
                "sequence": escaped_control(match.group(0)),
                "reason": "non-SGR control character",
            }
        )

    cursor = 0
    sequence_count = 0
    while True:
        offset = text.find("\x1b", cursor)
        if offset < 0:
            break
        match = ANSI_SGR_RE.match(text, offset)
        if match is None:
            invalid_sequences.append(
                {
                    "offset": offset,
                    "sequence": escaped_control(text[offset : offset + 12]),
                    "reason": "escape sequence is not a complete SGR sequence",
                }
            )
            cursor = offset + 1
            continue

        raw_parameters = match.group(1)
        sequence_count += 1
        sequence_counts[raw_parameters] = sequence_counts.get(raw_parameters, 0) + 1
        parameters = sgr_parameters(raw_parameters)
        unknown = [parameter for parameter in parameters if parameter not in ALLOWED_SGR_PARAMETERS]
        if unknown:
            invalid_sequences.append(
                {
                    "offset": offset,
                    "sequence": escaped_control(match.group(0)),
                    "reason": "unsupported SGR parameter(s): %s" % ", ".join(str(item) for item in unknown),
                }
            )
        else:
            previous_style = style
            style = apply_sgr(style, parameters)
            was_active = previous_style != reset_style()
            is_active = style != reset_style()
            if was_active and is_active:
                invalid_sequences.append(
                    {
                        "offset": offset,
                        "sequence": escaped_control(match.group(0)),
                        "reason": "SGR style opened before the previous style was reset",
                    }
                )
            elif not was_active and not is_active:
                invalid_sequences.append(
                    {
                        "offset": offset,
                        "sequence": escaped_control(match.group(0)),
                        "reason": "SGR reset has no matching open style",
                    }
                )
        cursor = match.end()

    unclosed_style = None
    if style != reset_style():
        unclosed_style = dict(style)

    return {
        "syntax_ok": not invalid_sequences and unclosed_style is None,
        "sgr_sequence_count": sequence_count,
        "sgr_sequence_counts": sequence_counts,
        "invalid_sequences": invalid_sequences,
        "unclosed_style": unclosed_style,
    }


def analyse_ansi_evidence(text, expectations):
    """Check that each expected semantic colour appears with its SGR codes; return the findings."""
    inspection = inspect_ansi(text)
    mapping_results = []
    missing_mappings = []
    wrong_mappings = []

    for expectation in expectations:
        span_re = re.compile(r"\x1b\[([0-9;]*)m%s\x1b\[0;m" % re.escape(expectation["text"]))
        observed = [match.group(1) for match in span_re.finditer(text)]
        expected_sgr = expectation["sgr"]
        unexpected = sorted(set(item for item in observed if item != expected_sgr))
        item_ok = expected_sgr in observed and not unexpected
        mapping_result = {
            "semantic": expectation["semantic"],
            "text": expectation["text"],
            "colour": expectation["colour"],
            "expected_sgr": expected_sgr,
            "expected_occurrences": observed.count(expected_sgr),
            "observed_sgr": sorted(set(observed)),
            "ok": item_ok,
        }
        mapping_results.append(mapping_result)
        if not observed:
            missing_mappings.append(expectation["semantic"])
        elif not item_ok:
            wrong_mappings.append(
                {
                    "semantic": expectation["semantic"],
                    "expected_sgr": expected_sgr,
                    "observed_sgr": sorted(set(observed)),
                }
            )

    inspection["expected_mappings"] = mapping_results
    inspection["missing_mappings"] = missing_mappings
    inspection["wrong_mappings"] = wrong_mappings
    inspection["mappings_ok"] = not missing_mappings and not wrong_mappings
    inspection["ok"] = inspection["syntax_ok"] and inspection["mappings_ok"]
    return inspection


def normalize_output(text):
    """Return text without ANSI codes and with whitespace collapsed."""
    text = ANSI_RE.sub("", text)
    return re.sub(r"\s+", " ", text)


def normalize_for_artifact(text):
    """Return text with allowed SGR codes kept and other control characters escaped."""
    safe_characters = []
    cursor = 0
    while cursor < len(text):
        char = text[cursor]
        if char == "\x1b":
            match = ANSI_SGR_RE.match(text, cursor)
            if match is not None and sgr_parameters_allowed(match.group(1)):
                cursor = match.end()
                continue
            safe_characters.append(escaped_control(char))
        elif char in "\n\t":
            safe_characters.append(char)
        elif ord(char) < 32 or 127 <= ord(char) <= 159:
            safe_characters.append(escaped_control(char))
        else:
            safe_characters.append(char)
        cursor += 1
    lines = "".join(safe_characters).splitlines()
    return "\n".join(line.rstrip() for line in lines).strip() + ("\n" if lines else "")


def write_text(path, text):
    """Write text to path as UTF-8, creating parent directories."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def output_text(value):
    """Return a subprocess output as text."""
    if value is None:
        return ""
    if isinstance(value, bytes):
        return value.decode("utf-8", "replace")
    return value


def display_path(path):
    """Return path relative to the repository when possible."""
    try:
        return str(path.relative_to(ROOT))
    except ValueError:
        return str(path)


def sanitize_artifact_text(text, qtop_home=None):
    """Replace the temporary home, Python and repository paths with placeholders."""
    replacements = []
    if qtop_home is not None:
        replacements.append((str(qtop_home), "<qtop-home>"))
    if sys.executable:
        replacements.append((str(sys.executable), "<python>"))
    replacements.append((str(ROOT), "<repo>"))
    for original, replacement in sorted(set(replacements), key=lambda item: len(item[0]), reverse=True):
        text = text.replace(original, replacement)
    return text


def styled_rows(text, max_lines, max_columns):
    """Split ANSI text into rows of (style, text) runs; raise ValueError on unsafe ANSI."""
    inspection = inspect_ansi(text)
    if not inspection["syntax_ok"]:
        raise ValueError("terminal output contains unsafe or incomplete ANSI state")

    rows = [[]]
    line_index = 0
    column = 0
    style = reset_style()
    cursor = 0

    def append_character(char):
        current_row = rows[-1]
        style_key = (style["bold"], style["foreground"], style["background"])
        if current_row and current_row[-1][0] == style_key:
            current_row[-1] = (style_key, current_row[-1][1] + char)
        else:
            current_row.append((style_key, char))

    for match in ANSI_SGR_RE.finditer(text):
        segment = text[cursor : match.start()]
        for char in segment:
            if char == "\n":
                line_index += 1
                if line_index < max_lines:
                    rows.append([])
                column = 0
            elif line_index < max_lines and column < max_columns:
                expanded = " " * (8 - column % 8) if char == "\t" else char
                for expanded_char in expanded:
                    if column < max_columns:
                        append_character(expanded_char)
                        column += 1
        style = apply_sgr(style, sgr_parameters(match.group(1)))
        cursor = match.end()

    for char in text[cursor:]:
        if char == "\n":
            line_index += 1
            if line_index < max_lines:
                rows.append([])
            column = 0
        elif line_index < max_lines and column < max_columns:
            expanded = " " * (8 - column % 8) if char == "\t" else char
            for expanded_char in expanded:
                if column < max_columns:
                    append_character(expanded_char)
                    column += 1

    return rows[:max_lines]


def style_colours(style_key):
    """Return the foreground and background colours and boldness for a style."""
    bold, foreground, background = style_key
    foreground_palette = ANSI_BRIGHT_COLOURS if bold else ANSI_NORMAL_COLOURS
    foreground_colour = foreground_palette.get(foreground, "#f2f2f2")
    background_colour = "#ff8700" if background == ORANGE_BACKGROUND else ANSI_NORMAL_COLOURS.get(background - 10) if background is not None else None
    return foreground_colour, background_colour, bold


def write_svg_screenshot(path, text, max_lines=38, max_columns=132):
    """Write the ANSI text as an SVG terminal screenshot."""
    lines = styled_rows(text, max_lines, max_columns)
    max_width = max([sum(len(run[1]) for run in line) for line in lines] or [0])
    width = max(760, min(1320, 22 + max_width * 8))
    height = 36 + max(1, len(lines)) * 17
    rows = []
    for index, line in enumerate(lines):
        column = 0
        baseline = 28 + index * 17
        for style_key, value in line:
            foreground, background, bold = style_colours(style_key)
            if background:
                rows.append('<rect x="%s" y="%s" width="%s" height="17" fill="%s"/>' % (14 + column * 8, baseline - 13, len(value) * 8, background))
            rows.append(
                '<text x="%s" y="%s" fill="%s"%s>%s</text>'
                % (
                    14 + column * 8,
                    baseline,
                    foreground,
                    ' font-weight="bold"' if bold else "",
                    html.escape(value, quote=True),
                )
            )
            column += len(value)
    svg = """<svg xmlns="http://www.w3.org/2000/svg" xml:space="preserve" width="%s" height="%s" viewBox="0 0 %s %s">
<rect width="100%%" height="100%%" fill="#111111"/>
<g font-family="Menlo, Consolas, monospace" font-size="13">
%s
</g>
</svg>
""" % (
        width,
        height,
        width,
        height,
        "\n".join(rows),
    )
    write_text(path, svg)


def registered_schedulers():
    """Return every backend the registry finds; each must have sample cases here."""
    if str(ROOT) not in sys.path:  # run as a script, tools/ is sys.path[0]
        sys.path.insert(0, str(ROOT))
    from qtop_py.schedulers.registry import discover_qtop_batch_systems

    return list(discover_qtop_batch_systems())


def sample_case(scheduler, case_dir):
    """Return the gate case that case_dir's SAMPLE_GATE_FILE describes."""
    try:
        spec = json.loads((case_dir / SAMPLE_GATE_FILE).read_text(encoding="utf-8"))
    except (OSError, ValueError) as error:
        raise SystemExit("Missing or unreadable %s in %s: %s" % (SAMPLE_GATE_FILE, case_dir, error)) from error
    if not isinstance(spec, dict) or not isinstance(spec.get("markers"), list) or not isinstance(spec.get("args", []), list):
        raise SystemExit("%s in %s needs a markers list (and args as a list)" % (SAMPLE_GATE_FILE, case_dir))
    source = spec.get("source", "")
    replay = source is not None
    source = (ROOT / source if source else case_dir) if replay else ROOT
    return {
        "name": "%s-%s" % (scheduler.replace("_", "-"), case_dir.name),
        "scheduler": scheduler,
        "source": source,
        "args": ["-b", scheduler] + (["-s", str(source)] if replay else []) + ["-c", "ON"] + list(spec.get("args", [])),
        "markers": list(spec["markers"]),
        "colour_expectations": list(COMMON_COLOUR_EXPECTATIONS) + list(spec.get("colour_expectations", [])),
    }


def discover_cases(schedulers, slurm_samples_dir):
    """Return the sample cases of the given schedulers, from tests/plugins/<scheduler>_samples/*/.

    slurm_samples_dir replaces the Slurm sample root, e.g. for a larger local corpus.
    """
    cases = []
    for scheduler in schedulers:
        sample_root = Path(slurm_samples_dir) if scheduler == "slurm" else SAMPLES_ROOT / (scheduler + "_samples")
        case_dirs = sorted(path for path in sample_root.iterdir() if path.is_dir()) if sample_root.is_dir() else []
        if not case_dirs:
            raise SystemExit("No sample cases for %s: add %s/<case>/%s" % (scheduler, display_path(sample_root), SAMPLE_GATE_FILE))
        cases.extend(sample_case(scheduler, case_dir) for case_dir in case_dirs)
    return cases


def write_case_artifacts(case_dir, qtop_home, command, stdout, stderr, ansi_evidence):
    """Write a case's sanitised command, output, ANSI evidence and screenshot."""
    safe_stdout = sanitize_artifact_text(stdout, qtop_home)
    safe_stderr = sanitize_artifact_text(stderr, qtop_home)
    safe_command = [sanitize_artifact_text(item, qtop_home) for item in command]
    safe_command[0] = "<python>"

    # Preserve ANSI only after it passes the SGR allowlist.  Rejected output is
    # escaped so reviewing the artifact cannot execute a terminal control.
    stdout_artifact = safe_stdout if ansi_evidence["syntax_ok"] else normalize_for_artifact(safe_stdout)
    write_text(case_dir / "stdout.ans", stdout_artifact)
    write_text(case_dir / "rendered.normalized.txt", normalize_for_artifact(safe_stdout))
    write_text(case_dir / "stderr.log", normalize_for_artifact(safe_stderr))
    write_text(case_dir / "command.txt", " ".join(safe_command) + "\n")

    if ansi_evidence["syntax_ok"]:
        write_svg_screenshot(case_dir / "screenshot.svg", safe_stdout)
    else:
        write_svg_screenshot(
            case_dir / "screenshot.svg",
            "ANSI validation failed; terminal output was not rendered. See summary.json.\n",
        )
    return safe_command


def write_case_summary(case_dir, result):
    """Write a case's result as summary.json."""
    write_text(case_dir / "summary.json", json.dumps(result, indent=2, sort_keys=True) + "\n")


def run_case(case, artifact_dir, timeout):
    """Run one case with a temporary qtop home; return its result."""
    case_dir = artifact_dir / case["name"]
    if case_dir.exists():
        shutil.rmtree(str(case_dir))
    with tempfile.TemporaryDirectory(prefix="qtop-sample-home-") as temporary_home:
        return run_case_with_home(case, artifact_dir, timeout, Path(temporary_home))


def run_case_with_home(case, artifact_dir, timeout, qtop_home):
    """Run one case with qtop_home as HOME and check its output; return its result."""
    case_dir = artifact_dir / case["name"]

    command = [sys.executable, "-m", "qtop_py.qtop"] + case.get(
        "args",
        ["-s", str(case["source"]), "-c", "ON", "-F", "-b", case["scheduler"]],
    )
    expectations = case.get("colour_expectations", COMMON_COLOUR_EXPECTATIONS)
    env = os.environ.copy()
    env["HOME"] = str(qtop_home)
    try:
        completed = subprocess.run(
            command,
            cwd=str(ROOT),
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True,
            timeout=timeout,
        )
    except subprocess.TimeoutExpired as exc:
        stdout, stderr, returncode = output_text(exc.stdout), output_text(exc.stderr), None
    else:
        stdout, stderr, returncode = completed.stdout, completed.stderr, completed.returncode

    ansi_evidence = analyse_ansi_evidence(stdout, expectations)
    safe_command = write_case_artifacts(case_dir, qtop_home, command, stdout, stderr, ansi_evidence)
    result = {
        "name": case["name"],
        "scheduler": case["scheduler"],
        "source": display_path(case["source"]),
        "command": safe_command,
        "returncode": returncode,
        "artifact": display_path(case_dir),
        "screenshot": display_path(case_dir / "screenshot.svg"),
        "ok": False,
        "missing_markers": [],
        "ansi_evidence": ansi_evidence,
    }
    if returncode != 0:
        result["error"] = "timeout after %s seconds" % timeout if returncode is None else "qtop exited non-zero"
        write_case_summary(case_dir, result)
        return result

    normalized = normalize_output(stdout)
    missing = [marker for marker in case["markers"] if marker not in normalized]
    result["missing_markers"] = missing
    result["ok"] = not missing and ansi_evidence["ok"]
    if missing:
        write_text(case_dir / "missing-markers.txt", "\n".join(missing) + "\n")
    if not ansi_evidence["ok"]:
        result["error"] = "ANSI colour evidence failed"
    write_case_summary(case_dir, result)
    return result


def parse_args():
    """Parse the command-line options."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--schedulers", default="all", help="Comma-separated scheduler gates to run; all (default) is every registered backend")
    parser.add_argument("--max-failures", type=int, default=0, help="Allowed failed cases before returning non-zero")
    parser.add_argument("--timeout", type=int, default=20, help="Per-case timeout in seconds")
    parser.add_argument("--artifact-dir", default="artifacts/sample-gate", help="Output directory for rendered qtop artifacts")
    parser.add_argument("--slurm-samples-dir", default="tests/plugins/slurm_samples", help="Committed Slurm sample directory")
    return parser.parse_args()


def main():
    """Run the selected cases and return non-zero beyond --max-failures."""
    args = parse_args()
    schedulers = [item.strip() for item in args.schedulers.split(",") if item.strip()]
    if schedulers == ["all"]:
        schedulers = registered_schedulers()
    artifact_dir = Path(args.artifact_dir)
    if not artifact_dir.is_absolute():
        artifact_dir = ROOT / artifact_dir
    try:
        artifact_dir = private_output_dir(artifact_dir)  # refuses symlinked or shared directories (CWE-377)
    except ValueError as error:
        print("error: %s" % error, file=sys.stderr)
        return 2
    cases = discover_cases(schedulers, ROOT / args.slurm_samples_dir)
    results = []

    for case in cases:
        result = run_case(case, artifact_dir, args.timeout)
        results.append(result)
        print("%s: %s" % (result["name"], "ok" if result["ok"] else "failed"))

    failures = [result for result in results if not result["ok"]]
    summary = {
        "cases": len(results),
        "passed": len(results) - len(failures),
        "failed": len(failures),
        "max_failures": args.max_failures,
        "artifact_dir": display_path(artifact_dir),
        "results": results,
    }
    write_text(artifact_dir / "summary.json", json.dumps(summary, indent=2, sort_keys=True) + "\n")
    print("sample-gate: passed=%s failed=%s artifact_dir=%s" % (summary["passed"], summary["failed"], summary["artifact_dir"]))

    return 0 if len(failures) <= args.max_failures else 1


if __name__ == "__main__":
    raise SystemExit(main())
