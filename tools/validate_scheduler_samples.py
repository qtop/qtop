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
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")
CONTRIB = ROOT / "qtop_py" / "contrib"

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


def normalize_output(text):
    text = ANSI_RE.sub("", text)
    return re.sub(r"\s+", " ", text)


def normalize_for_artifact(text):
    lines = ANSI_RE.sub("", text).splitlines()
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


def write_svg_screenshot(path, text, max_lines=38, max_columns=132):
    lines = ANSI_RE.sub("", text).splitlines()[:max_lines]
    clipped = [line[:max_columns] for line in lines]
    width = max(760, min(1320, 22 + max([len(line) for line in clipped] or [0]) * 8))
    height = 36 + max(1, len(clipped)) * 17
    rows = []
    for index, line in enumerate(clipped):
        rows.append(
            '<text x="14" y="%s">%s</text>' % (
                28 + index * 17,
                html.escape(line.rstrip()),
            )
        )
    svg = """<svg xmlns="http://www.w3.org/2000/svg" width="%s" height="%s" viewBox="0 0 %s %s">
<rect width="100%%" height="100%%" fill="#111111"/>
<g font-family="Menlo, Consolas, monospace" font-size="13" fill="#f2f2f2">
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
                    }
                )
        else:
            raise SystemExit("Unknown scheduler: %s" % scheduler)
    return cases


def run_case(case, artifact_dir, timeout):
    case_dir = artifact_dir / case["name"]
    if case_dir.exists():
        shutil.rmtree(str(case_dir))
    qtop_home = case_dir / "home"
    qtop_home.mkdir(parents=True, exist_ok=True)

    command = [sys.executable, "-m", "qtop_py.cli"] + case.get(
        "args",
        ["-s", str(case["source"]), "-c", "ON", "-F", "-b", case["scheduler"]],
    )
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
        write_text(case_dir / "stdout.ans", stdout)
        write_text(case_dir / "rendered.normalized.txt", normalize_for_artifact(stdout))
        write_text(case_dir / "stderr.log", stderr)
        write_text(case_dir / "command.txt", " ".join(command) + "\n")
        write_svg_screenshot(case_dir / "screenshot.svg", stdout)
        return {
            "name": case["name"],
            "scheduler": case["scheduler"],
            "source": display_path(case["source"]),
            "command": command,
            "returncode": None,
            "artifact": display_path(case_dir),
            "screenshot": display_path(case_dir / "screenshot.svg"),
            "ok": False,
            "missing_markers": [],
            "error": "timeout after %s seconds" % timeout,
        }

    write_text(case_dir / "stdout.ans", completed.stdout)
    write_text(case_dir / "rendered.normalized.txt", normalize_for_artifact(completed.stdout))
    write_text(case_dir / "stderr.log", completed.stderr)
    write_text(case_dir / "command.txt", " ".join(command) + "\n")
    write_svg_screenshot(case_dir / "screenshot.svg", completed.stdout)

    result = {
        "name": case["name"],
        "scheduler": case["scheduler"],
        "source": display_path(case["source"]),
        "command": command,
        "returncode": completed.returncode,
        "artifact": display_path(case_dir),
        "screenshot": display_path(case_dir / "screenshot.svg"),
        "ok": False,
        "missing_markers": [],
    }

    if completed.returncode != 0:
        result["error"] = "qtop exited non-zero"
        return result

    normalized = normalize_output(completed.stdout)
    missing = [marker for marker in case["markers"] if marker not in normalized]
    result["missing_markers"] = missing
    result["ok"] = not missing
    if missing:
        write_text(case_dir / "missing-markers.txt", "\n".join(missing) + "\n")
    return result


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--schedulers", default="pbs,sge,slurm", help="Comma-separated scheduler gates to run")
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
