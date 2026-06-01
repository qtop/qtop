#!/usr/bin/env python3
"""Run qtop against checked-in scheduler samples and publish artifacts."""

import argparse
import json
import os
import re
import subprocess
import sys
import time
from pathlib import Path


ANSI_RE = re.compile(r"\x1b\[[0-?]*[ -/]*[@-~]")
VOLATILE_MARKERS = (
    "Please try it with watch:",
    "Log file created in",
    "WORKDIR =",
)

SAMPLES = {
    "sge": {
        "args": ["-s", "qtop_py/contrib", "-c", "ON", "-Fadvv", "-b", "sge"],
        "inputs": ["qtop_py/contrib/qstat.F.xml.stdout"],
        "expect": ["Summary: Total:", "Worker Nodes occupancy", "User accounts and pool mappings"],
    },
    "oar": {
        "args": ["-e", "-c", "ON", "-s", "qtop_py/contrib", "-FAardvvv", "-b", "oar"],
        "inputs": ["qtop_py/contrib/oarnodes_s_Y.txt", "qtop_py/contrib/oarnodes_Y.txt", "qtop_py/contrib/oarstat.txt"],
        "expect": ["Summary: Total:", "Worker Nodes occupancy", "User accounts and pool mappings"],
    },
    "pbs": {
        "args": ["-c", "ON", "-s", "qtop_py/contrib", "-raF", "-b", "pbs"],
        "inputs": ["qtop_py/contrib/pbsnodes_a.txt", "qtop_py/contrib/qstat_q.txt", "qtop_py/contrib/qstat.txt"],
        "expect": ["Summary: Total:", "Worker Nodes occupancy", "User accounts and pool mappings"],
    },
}


def normalize(output):
    lines = []
    for line in output.replace("\r", "").splitlines():
        if any(marker in line for marker in VOLATILE_MARKERS):
            continue
        stripped = ANSI_RE.sub("", line).rstrip()
        if stripped:
            lines.append(stripped)
    return "\n".join(lines) + "\n"


def write_text(path, text):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def run_sample(name, sample, repo_root, artifact_dir, python):
    started = time.time()
    missing = [path for path in sample["inputs"] if not (repo_root / path).exists()]
    command = [python, "-m", "qtop_py.cli"] + list(sample["args"])
    env = os.environ.copy()
    env["HOME"] = str(artifact_dir / "home")
    env["PYTHONPATH"] = str(repo_root)
    env.setdefault("TERM", "xterm")
    env.setdefault("USER", "qtop-ci")

    result = subprocess.run(
        command,
        cwd=str(repo_root),
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        universal_newlines=True,
    )

    raw_path = artifact_dir / ("%s.raw.txt" % name)
    err_path = artifact_dir / ("%s.stderr.txt" % name)
    normalized_path = artifact_dir / ("%s.normalized.txt" % name)
    write_text(raw_path, result.stdout)
    write_text(err_path, result.stderr)
    normalized = normalize(result.stdout)
    write_text(normalized_path, normalized)

    failures = []
    if missing:
        failures.append("missing inputs: %s" % ", ".join(missing))
    if result.returncode != 0:
        failures.append("exit code %s" % result.returncode)
    if not normalized.strip():
        failures.append("no normalized qtop output")
    for expected in sample["expect"]:
        if expected not in normalized:
            failures.append("missing expected output marker: %s" % expected)

    return {
        "scheduler": name,
        "command": command,
        "inputs": sample["inputs"],
        "returncode": result.returncode,
        "duration_seconds": round(time.time() - started, 3),
        "artifacts": {
            "raw_stdout": str(raw_path.relative_to(repo_root)),
            "stderr": str(err_path.relative_to(repo_root)),
            "normalized_stdout": str(normalized_path.relative_to(repo_root)),
        },
        "failures": failures,
    }


def parse_args():
    parser = argparse.ArgumentParser(description="Validate qtop scheduler samples.")
    parser.add_argument("--repo-root", default=".", help="Repository root. Defaults to the current directory.")
    parser.add_argument("--artifact-dir", default="sample-artifacts", help="Directory for raw output, stderr, and summary files.")
    parser.add_argument("--max-failures", default=0, type=int, help="Allowed failures before the gate exits non-zero.")
    parser.add_argument("--scheduler", action="append", choices=sorted(SAMPLES), help="Run only the named scheduler. May be passed more than once.")
    parser.add_argument("--python", default=sys.executable, help="Python executable used to invoke qtop.")
    return parser.parse_args()


def main():
    args = parse_args()
    repo_root = Path(args.repo_root).resolve()
    artifact_dir = (repo_root / args.artifact_dir).resolve()
    artifact_dir.mkdir(parents=True, exist_ok=True)

    selected = args.scheduler or sorted(SAMPLES)
    results = [run_sample(name, SAMPLES[name], repo_root, artifact_dir, args.python) for name in selected]
    failure_count = sum(1 for result in results if result["failures"])

    summary = {
        "failure_count": failure_count,
        "max_failures": args.max_failures,
        "note": "qtop has checked-in samples for PBS, OAR, and SGE; no SLURM plugin or fixture exists in this repository yet.",
        "results": results,
    }
    summary_path = artifact_dir / "summary.json"
    write_text(summary_path, json.dumps(summary, indent=2, sort_keys=True) + "\n")

    for result in results:
        status = "PASS" if not result["failures"] else "FAIL"
        print("%s %s (%ss)" % (status, result["scheduler"], result["duration_seconds"]))
        for failure in result["failures"]:
            print("  - %s" % failure)
    print("summary: %s" % summary_path.relative_to(repo_root))

    return 1 if failure_count > args.max_failures else 0


if __name__ == "__main__":
    sys.exit(main())
