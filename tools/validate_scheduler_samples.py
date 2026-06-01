#!/usr/bin/env python3
"""Run the fast bundled scheduler sample gate and write review artifacts."""

import argparse
import difflib
import json
import os
import subprocess
import sys
from pathlib import Path


VOLATILE_PATTERNS = (
    "WORKDIR",
    "Please try it with watch",
    "Log file created in",
)


REFERENCE_CASES = (
    {
        "name": "sge-bundled",
        "scheduler": "sge",
        "args": ["-s", "qtop_py/contrib", "-c", "ON", "-Fadvv", "-b", "sge"],
        "reference": "qtop_py/contrib/sger_dvv_out.ref",
    },
    {
        "name": "oar-bundled",
        "scheduler": "oar",
        "args": ["-c", "ON", "-s", "qtop_py/contrib", "-FAardvvv", "-e", "-b", "oar"],
        "reference": "qtop_py/contrib/oar1_dvv_out.ref",
    },
    {
        "name": "pbs-bundled",
        "scheduler": "pbs",
        "args": ["-c", "ON", "-s", "qtop_py/contrib", "-raF", "-b", "pbs"],
        "reference": "qtop_py/contrib/pbs_dvv_out.ref",
    },
)


def filtered_lines(text):
    return [
        line.rstrip()
        for line in text.splitlines()
        if not any(pattern in line for pattern in VOLATILE_PATTERNS)
    ]


def write_text(path, text):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def run_reference_case(repo_root, output_dir, case):
    command = [sys.executable, "-m", "qtop_py.cli"] + case["args"]
    completed = subprocess.run(
        command,
        cwd=str(repo_root),
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=30,
    )
    stdout_path = output_dir / f"{case['name']}.out"
    stderr_path = output_dir / f"{case['name']}.err"
    diff_path = output_dir / f"{case['name']}.diff"
    write_text(stdout_path, completed.stdout)
    write_text(stderr_path, completed.stderr)

    reference = filtered_lines((repo_root / case["reference"]).read_text(encoding="utf-8"))
    observed = filtered_lines(completed.stdout)
    diff = list(
        difflib.unified_diff(
            reference,
            observed,
            fromfile=case["reference"],
            tofile=str(stdout_path),
            lineterm="",
        )
    )
    write_text(diff_path, "\n".join(diff) + ("\n" if diff else ""))
    passed = completed.returncode == 0 and bool(filtered_lines(completed.stdout))
    return {
        "name": case["name"],
        "scheduler": case["scheduler"],
        "command": command,
        "returncode": completed.returncode,
        "passed": passed,
        "reference_matched": not diff,
        "reference_diff_lines": len(diff),
        "stdout": str(stdout_path),
        "stderr": str(stderr_path),
        "diff": str(diff_path),
    }


def run_slurm_samples(repo_root, output_dir, samples_dir):
    slurm_output = output_dir / "slurm-rendered"
    command = [
        sys.executable,
        "tools/validate_slurm_samples.py",
        str(samples_dir),
        "--output",
        str(slurm_output),
    ]
    completed = subprocess.run(
        command,
        cwd=str(repo_root),
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=30,
    )
    stdout_path = output_dir / "slurm-command-traces.out"
    stderr_path = output_dir / "slurm-command-traces.err"
    write_text(stdout_path, completed.stdout)
    write_text(stderr_path, completed.stderr)
    return {
        "name": "slurm-command-traces",
        "scheduler": "slurm",
        "command": command,
        "returncode": completed.returncode,
        "passed": completed.returncode == 0,
        "stdout": str(stdout_path),
        "stderr": str(stderr_path),
        "rendered_output": str(slurm_output),
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", default="qtop-sample-artifacts", type=Path)
    parser.add_argument("--max-failures", default=0, type=int)
    parser.add_argument("--slurm-samples-dir", default=Path("tests/plugins/slurm_samples"), type=Path)
    args = parser.parse_args()

    repo_root = Path(__file__).resolve().parents[1]
    output_dir = args.output if args.output.is_absolute() else repo_root / args.output
    output_dir.mkdir(parents=True, exist_ok=True)

    env_path = os.environ.get("PYTHONPATH", "")
    os.environ["PYTHONPATH"] = str(repo_root) if not env_path else str(repo_root) + os.pathsep + env_path

    results = [run_reference_case(repo_root, output_dir, case) for case in REFERENCE_CASES]
    slurm_samples = args.slurm_samples_dir
    if not slurm_samples.is_absolute():
        slurm_samples = repo_root / slurm_samples
    results.append(run_slurm_samples(repo_root, output_dir, slurm_samples))

    manifest_path = output_dir / "manifest.json"
    write_text(manifest_path, json.dumps(results, indent=2) + "\n")
    failures = [result for result in results if not result["passed"]]
    print("scheduler samples: %s passed, %s failed" % (len(results) - len(failures), len(failures)))
    print("artifacts: %s" % output_dir)
    if len(failures) > args.max_failures:
        for failure in failures:
            print("FAILED: {name} stderr={stderr}".format(**failure))
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
