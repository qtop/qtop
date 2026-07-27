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


ROOT = Path(__file__).resolve().parents[1]
ANSI_SGR_RE = re.compile(r"\x1b\[([0-9;]*)m")
ANSI_RE = ANSI_SGR_RE
UNSAFE_CONTROL_RE = re.compile(r"[\x00-\x08\x0b-\x1a\x1c-\x1f\x7f-\x9f]")
CONTRIB = ROOT / "qtop_py" / "contrib"

ALLOWED_SGR_PARAMETERS = set([0, 1] + list(range(30, 38)) + list(range(40, 48)))

# Stable presentation semantics shared by every committed backend sample.
# Each witness includes its closing reset, so a colour code elsewhere cannot
# make the case pass.
COMMON_COLOUR_EXPECTATIONS = [
    {"semantic": "section opening delimiter", "text": "===> ", "colour": "Gray_D", "sgr": "1;30"},
    {"semantic": "section closing delimiter", "text": " <=== ", "colour": "Gray_D", "sgr": "1;30"},
    {"semantic": "accounting section title", "text": "Job accounting summary", "colour": "White", "sgr": "1;37"},
    {"semantic": "summary label", "text": "Summary", "colour": "Cyan_L", "sgr": "1;36"},
    {"semantic": "node-count label", "text": "Nodes", "colour": "Red_L", "sgr": "1;31"},
    {"semantic": "core-count label", "text": "cores", "colour": "Green_L", "sgr": "1;32"},
    {"semantic": "job-count label", "text": "jobs", "colour": "Blue_L", "sgr": "1;34"},
    {"semantic": "blocked-queue warning", "text": "* implies blocked", "colour": "Red", "sgr": "0;31"},
]

# The fixed SGE capture exercises all three configurable mapping domains in
# natural qtop output. These witnesses are intentionally case-specific:
# generated demo values are not stable enough for an exact semantic contract.
CASE_COLOUR_EXPECTATIONS = {
    "sge-contrib": [
        {"semantic": "mapped user", "text": "alicesgm", "colour": "Cyan", "sgr": "0;36"},
        {"semantic": "mapped queue", "text": "alice", "colour": "Red_L", "sgr": "1;31"},
        {"semantic": "running node state", "text": "r", "colour": "Blue", "sgr": "0;34"},
        {
            "semantic": "held node state",
            "text": "hqw",
            "colour": "PurpleOnGrayBG",
            "sgr": "0;35;40",
        },
        {
            "semantic": "unavailable node state",
            "text": "au",
            "colour": "BlackOnRed",
            "sgr": "0;30;41",
        },
    ]
}

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

STATIC_CASES = {
    "pbs": [
        {
            "name": "pbs-contrib",
            "source": CONTRIB,
            "args": ["-c", "ON", "-s", str(CONTRIB), "-raF", "-b", "pbs"],
            "markers": [
                "Summary: Total:829 Up:819 Free:91 Nodes",
                "7629/7872 cores",
                "7590+3365 jobs",
                "Worker Nodes occupancy",
                "User accounts and pool mappings",
            ],
        }
    ],
    "sge": [
        {
            "name": "sge-contrib",
            "source": CONTRIB,
            "args": ["-s", str(CONTRIB), "-c", "ON", "-Fadvv", "-b", "sge"],
            "markers": [
                "Summary: Total:17 Up:17 Free:4 Nodes",
                "61/408 cores",
                "61+31 jobs",
                "Worker Nodes occupancy",
                "User accounts and pool mappings",
            ],
        }
    ],
    "oar": [
        {
            "name": "oar-contrib",
            "source": CONTRIB,
            "args": ["-s", str(CONTRIB), "-c", "ON", "-F", "-b", "oar"],
            "markers": [
                "Summary: Total:183 Up:172 Free:167 Nodes",
                "1349/2520 cores",
                "0+0 jobs",
                "Worker Nodes occupancy",
                "User accounts and pool mappings",
            ],
        }
    ],
    "demo": [
        {
            "name": "demo-generated",
            "source": ROOT,
            "args": ["-c", "ON", "-F", "-b", "demo"],
            "markers": [
                "This data is simulated",
                "Summary: Total:",
                "Nodes |",
                "jobs (R + Q)",
                "Worker Nodes occupancy",
                "User accounts and pool mappings",
            ],
        }
    ],
}

SLURM_MARKERS_BY_SAMPLE = {
    "basic": ["Summary: Total:3 Up:3 Free:2 Nodes", "4/16 cores", "2+1 jobs"],
    "large_cluster": ["Summary: Total:18 Up:17 Free:9 Nodes", "120/288 cores", "3+1 jobs"],
    "large_mixed": ["Summary: Total:20 Up:19 Free:9 Nodes", "160/320 cores", "3+1 jobs"],
    "large_multi_partition": ["Summary: Total:18 Up:17 Free:10 Nodes", "104/288 cores", "3+1 jobs"],
    "mixed": ["Summary: Total:2 Up:1 Free:0 Nodes", "4/16 cores", "2+0 jobs"],
    "multi_partition": ["Summary: Total:2 Up:2 Free:1 Nodes", "4/32 cores", "2+1 jobs"],
}

COMMON_RENDER_MARKERS = [
    "Worker Nodes occupancy",
    "User accounts and pool mappings",
]


def sgr_parameters(raw_parameters):
    parameters = [0 if item == "" else int(item) for item in raw_parameters.split(";")]
    return parameters if raw_parameters else [0]


def sgr_parameters_allowed(raw_parameters):
    return all(parameter in ALLOWED_SGR_PARAMETERS for parameter in sgr_parameters(raw_parameters))


def reset_style():
    return {"bold": False, "foreground": None, "background": None}


def apply_sgr(style, parameters):
    updated = dict(style)
    for parameter in parameters:
        if parameter == 0:
            updated = reset_style()
        elif parameter == 1:
            updated["bold"] = True
        elif 30 <= parameter <= 37:
            updated["foreground"] = parameter
        elif 40 <= parameter <= 47:
            updated["background"] = parameter
    return updated


def escaped_control(value):
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
    text = ANSI_RE.sub("", text)
    return re.sub(r"\s+", " ", text)


def normalize_for_artifact(text):
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
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def output_text(value):
    if value is None:
        return ""
    if isinstance(value, bytes):
        return value.decode("utf-8", "replace")
    return value


def display_path(path):
    try:
        return str(path.relative_to(ROOT))
    except ValueError:
        return str(path)


def sanitize_artifact_text(text, qtop_home=None):
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
    bold, foreground, background = style_key
    foreground_palette = ANSI_BRIGHT_COLOURS if bold else ANSI_NORMAL_COLOURS
    foreground_colour = foreground_palette.get(foreground, "#f2f2f2")
    background_colour = ANSI_NORMAL_COLOURS.get(background - 10) if background is not None else None
    return foreground_colour, background_colour, bold


def write_svg_screenshot(path, text, max_lines=38, max_columns=132):
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


def discover_cases(schedulers, slurm_samples_dir):
    cases = []
    for scheduler in schedulers:
        if scheduler in STATIC_CASES:
            for case in STATIC_CASES[scheduler]:
                item = dict(case)
                item["scheduler"] = scheduler
                item["colour_expectations"] = list(COMMON_COLOUR_EXPECTATIONS) + list(CASE_COLOUR_EXPECTATIONS.get(item["name"], []))
                cases.append(item)
        elif scheduler == "slurm":
            sample_root = Path(slurm_samples_dir)
            for sample_dir in sorted(path for path in sample_root.iterdir() if path.is_dir()):
                if sample_dir.name not in SLURM_MARKERS_BY_SAMPLE:
                    raise SystemExit("Missing Slurm marker expectations for sample: %s" % sample_dir.name)
                cases.append(
                    {
                        "name": "slurm-%s" % sample_dir.name,
                        "scheduler": "slurm",
                        "source": sample_dir,
                        "markers": SLURM_MARKERS_BY_SAMPLE[sample_dir.name] + COMMON_RENDER_MARKERS,
                        "colour_expectations": list(COMMON_COLOUR_EXPECTATIONS),
                    }
                )
        else:
            raise SystemExit("Unknown scheduler: %s" % scheduler)
    return cases


def write_case_artifacts(case_dir, qtop_home, command, stdout, stderr, ansi_evidence):
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
    write_text(case_dir / "summary.json", json.dumps(result, indent=2, sort_keys=True) + "\n")


def run_case(case, artifact_dir, timeout):
    case_dir = artifact_dir / case["name"]
    if case_dir.exists():
        shutil.rmtree(str(case_dir))
    with tempfile.TemporaryDirectory(prefix="qtop-sample-home-") as temporary_home:
        return run_case_with_home(case, artifact_dir, timeout, Path(temporary_home))


def run_case_with_home(case, artifact_dir, timeout, qtop_home):
    case_dir = artifact_dir / case["name"]

    command = [sys.executable, "-m", "qtop_py.cli"] + case.get(
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
        stdout = output_text(exc.stdout)
        stderr = output_text(exc.stderr)
        ansi_evidence = analyse_ansi_evidence(stdout, expectations)
        safe_command = write_case_artifacts(case_dir, qtop_home, command, stdout, stderr, ansi_evidence)
        result = {
            "name": case["name"],
            "scheduler": case["scheduler"],
            "source": display_path(case["source"]),
            "command": safe_command,
            "returncode": None,
            "artifact": display_path(case_dir),
            "screenshot": display_path(case_dir / "screenshot.svg"),
            "ok": False,
            "missing_markers": [],
            "ansi_evidence": ansi_evidence,
            "error": "timeout after %s seconds" % timeout,
        }
        write_case_summary(case_dir, result)
        return result

    ansi_evidence = analyse_ansi_evidence(completed.stdout, expectations)
    safe_command = write_case_artifacts(
        case_dir,
        qtop_home,
        command,
        completed.stdout,
        completed.stderr,
        ansi_evidence,
    )

    result = {
        "name": case["name"],
        "scheduler": case["scheduler"],
        "source": display_path(case["source"]),
        "command": safe_command,
        "returncode": completed.returncode,
        "artifact": display_path(case_dir),
        "screenshot": display_path(case_dir / "screenshot.svg"),
        "ok": False,
        "missing_markers": [],
        "ansi_evidence": ansi_evidence,
    }

    if completed.returncode != 0:
        result["error"] = "qtop exited non-zero"
        write_case_summary(case_dir, result)
        return result

    normalized = normalize_output(completed.stdout)
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
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--schedulers", default="pbs,sge,slurm,oar,demo", help="Comma-separated scheduler gates to run")
    parser.add_argument("--max-failures", type=int, default=0, help="Allowed failed cases before returning non-zero")
    parser.add_argument("--timeout", type=int, default=20, help="Per-case timeout in seconds")
    parser.add_argument("--artifact-dir", default="artifacts/sample-gate", help="Output directory for rendered qtop artifacts")
    parser.add_argument("--slurm-samples-dir", default="tests/plugins/slurm_samples", help="Committed Slurm sample directory")
    return parser.parse_args()


def main():
    args = parse_args()
    schedulers = [item.strip() for item in args.schedulers.split(",") if item.strip()]
    artifact_dir = Path(args.artifact_dir)
    if not artifact_dir.is_absolute():
        artifact_dir = ROOT / artifact_dir
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
