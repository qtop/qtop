#!/usr/bin/env python3
##
## qtop is a tool to monitor queuing systems - https://github.com/qtop/qtop
##
## Copyright (c) 2026 Nicola Trozzi
##
## SPDX-License-Identifier: MIT
##
"""Run the committed qtop scheduler sample gate and write review artifacts."""

import argparse
import html
import json
import os
import re
import shutil
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")
DYNAMIC_LINE_RE = re.compile(r"WORKDIR|Please try it with watch|Log file created in")

CONTRIB = ROOT / "qtop_py" / "contrib"
STATIC_CASES = {
    "pbs": {
        "name": "pbs-contrib",
        "source": CONTRIB,
        "reference": CONTRIB / "pbs_dvv_out.ref",
        "args": ["-c", "ON", "-s", str(CONTRIB), "-raF", "-b", "pbs"],
        "markers": [
            "Summary: Total:829 Up:819 Free:91 Nodes",
            "7629/7872 cores",
            "7590+3365 jobs",
            "Worker Nodes occupancy",
            "User accounts and pool mappings",
        ],
    },
    "sge": {
        "name": "sge-contrib",
        "source": CONTRIB,
        "reference": CONTRIB / "sger_dvv_out.ref",
        "args": ["-s", str(CONTRIB), "-c", "ON", "-Fadvv", "-b", "sge"],
        "markers": [
            "Summary: Total:17 Up:17 Free:4 Nodes",
            "61/408 cores",
            "61+31 jobs",
            "Worker Nodes occupancy",
            "User accounts and pool mappings",
        ],
    },
}


def write_text(path, text):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def strip_dynamic_lines(text):
    lines = []
    for line in text.splitlines():
        if DYNAMIC_LINE_RE.search(line):
            continue
        lines.append(ANSI_RE.sub("", line.rstrip()))
    return "\n".join(lines).strip() + "\n"


def display_path(path):
    try:
        return str(Path(path).relative_to(ROOT))
    except ValueError:
        return str(path)


def write_svg(path, text, max_lines=40, max_columns=132):
    clean_lines = ANSI_RE.sub("", text).splitlines()[:max_lines]
    clean_lines = [line[:max_columns] for line in clean_lines]
    width = max(800, min(1360, 24 + max([len(line) for line in clean_lines] or [0]) * 8))
    height = 36 + max(1, len(clean_lines)) * 17
    rows = []
    for index, line in enumerate(clean_lines):
        rows.append('<text x="14" y="%s">%s</text>' % (28 + index * 17, html.escape(line.rstrip())))
    svg = """<svg xmlns="http://www.w3.org/2000/svg" width="%s" height="%s" viewBox="0 0 %s %s">
<rect width="100%%" height="100%%" fill="#111"/>
<g font-family="Menlo, Consolas, monospace" font-size="13" fill="#f4f4f4">
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


def run_command(command, case_dir, timeout):
    env = os.environ.copy()
    env["HOME"] = str(case_dir / "home")
    os.makedirs(env["HOME"], exist_ok=True)
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
        stdout = exc.stdout or ""
        stderr = exc.stderr or ""
        if isinstance(stdout, bytes):
            stdout = stdout.decode("utf-8", "replace")
        if isinstance(stderr, bytes):
            stderr = stderr.decode("utf-8", "replace")
        return None, stdout, stderr, "timeout after %s seconds" % timeout
    return completed.returncode, completed.stdout, completed.stderr, None


def static_case_result(scheduler, artifact_dir, timeout):
    case = STATIC_CASES[scheduler]
    case_dir = artifact_dir / case["name"]
    if case_dir.exists():
        shutil.rmtree(str(case_dir))
    command = [sys.executable, "-m", "qtop_py.cli"] + case["args"]
    returncode, stdout, stderr, error = run_command(command, case_dir, timeout)
    rendered = strip_dynamic_lines(stdout)
    expected = strip_dynamic_lines(case["reference"].read_text(encoding="utf-8"))
    normalized = re.sub(r"\s+", " ", rendered)
    missing_markers = [marker for marker in case.get("markers", []) if marker not in normalized]

    write_text(case_dir / "command.txt", " ".join(command) + "\n")
    write_text(case_dir / "stdout.ans", stdout)
    write_text(case_dir / "stderr.log", stderr)
    write_text(case_dir / "rendered.normalized.txt", rendered)
    write_text(case_dir / "expected.normalized.txt", expected)
    write_svg(case_dir / "screenshot.svg", stdout)

    ok = returncode == 0 and error is None and not missing_markers
    if not ok and error is None and missing_markers:
        write_text(case_dir / "missing-markers.txt", "\n".join(missing_markers) + "\n")
        error = "missing stable qtop markers"
    elif not ok and error is None:
        error = "qtop exited with status %s" % returncode

    return {
        "name": case["name"],
        "scheduler": scheduler,
        "source": display_path(case["source"]),
        "reference": display_path(case["reference"]),
        "artifact": display_path(case_dir),
        "screenshot": display_path(case_dir / "screenshot.svg"),
        "returncode": returncode,
        "missing_markers": missing_markers,
        "ok": ok,
        "error": error,
    }


def slurm_result(artifact_dir, timeout, slurm_samples_dir):
    case_dir = artifact_dir / "slurm-committed-samples"
    if case_dir.exists():
        shutil.rmtree(str(case_dir))
    rendered_dir = case_dir / "rendered"
    command = [
        sys.executable,
        "tools/validate_slurm_samples.py",
        str(slurm_samples_dir),
        "--output",
        str(rendered_dir),
    ]
    returncode, stdout, stderr, error = run_command(command, case_dir, timeout)
    write_text(case_dir / "command.txt", " ".join(command) + "\n")
    write_text(case_dir / "stdout.ans", stdout)
    write_text(case_dir / "stderr.log", stderr)
    write_svg(case_dir / "screenshot.svg", stdout)
    ok = returncode == 0 and error is None
    if not ok and error is None:
        error = "Slurm validation exited with status %s" % returncode
    return {
        "name": "slurm-committed-samples",
        "scheduler": "slurm",
        "source": display_path(slurm_samples_dir),
        "artifact": display_path(case_dir),
        "screenshot": display_path(case_dir / "screenshot.svg"),
        "returncode": returncode,
        "ok": ok,
        "error": error,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--schedulers", default="pbs,sge,slurm", help="Comma-separated schedulers to gate")
    parser.add_argument("--max-failures", type=int, default=0, help="Allowed failing cases before returning non-zero")
    parser.add_argument("--timeout", type=int, default=20, help="Per-case timeout in seconds")
    parser.add_argument("--artifact-dir", default="artifacts/sample-gate", help="Directory for rendered outputs and logs")
    parser.add_argument("--slurm-samples-dir", default="tests/plugins/slurm_samples", help="Committed Slurm sample directory")
    args = parser.parse_args()

    artifact_dir = Path(args.artifact_dir)
    if not artifact_dir.is_absolute():
        artifact_dir = ROOT / artifact_dir
    artifact_dir.mkdir(parents=True, exist_ok=True)

    results = []
    for scheduler in [item.strip() for item in args.schedulers.split(",") if item.strip()]:
        if scheduler in STATIC_CASES:
            result = static_case_result(scheduler, artifact_dir, args.timeout)
        elif scheduler == "slurm":
            result = slurm_result(artifact_dir, args.timeout, ROOT / args.slurm_samples_dir)
        else:
            raise SystemExit("unknown scheduler: %s" % scheduler)
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
    print("sample-gate: passed=%s failed=%s artifacts=%s" % (summary["passed"], summary["failed"], summary["artifact_dir"]))
    return 0 if len(failures) <= args.max_failures else 1


if __name__ == "__main__":
    raise SystemExit(main())
