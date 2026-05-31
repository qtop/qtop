#!/usr/bin/env python3
##
## qtop is a tool to monitor queuing systems - https://github.com/qtop/qtop
##
## Copyright (c) 2026 Jamil Ur Rehman Ahmadzai
##
## SPDX-License-Identifier: MIT
##

import argparse
import json
import os
import subprocess
import sys


ROOT = os.path.realpath(os.path.join(os.path.dirname(__file__), ".."))
CONTRIB = os.path.join(ROOT, "qtop_py", "contrib")
SLURM_SAMPLES = os.path.join(ROOT, "tests", "plugins", "slurm_samples")

REQUIRED_MARKERS = (
    "Job accounting summary",
    "Worker Nodes occupancy",
    "User accounts and pool mappings",
)

BUILTIN_CASES = (
    ("sge", ("-s", CONTRIB, "-c", "ON", "-Fadvv", "-b", "sge")),
    ("oar", ("-s", CONTRIB, "-c", "ON", "-e", "-FAardvvv", "-b", "oar")),
    ("pbs", ("-s", CONTRIB, "-c", "ON", "-raF", "-b", "pbs")),
)


def mkdir_p(path):
    if not os.path.isdir(path):
        os.makedirs(path)


def write_text(path, content):
    with open(path, "w") as handle:
        handle.write(content)


def run_command(command, output_dir, name):
    proc = subprocess.run(
        command,
        cwd=ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        universal_newlines=True,
    )
    stdout_path = os.path.join(output_dir, "%s.out" % name)
    stderr_path = os.path.join(output_dir, "%s.err" % name)
    write_text(stdout_path, proc.stdout)
    write_text(stderr_path, proc.stderr)
    return proc.returncode, stdout_path, stderr_path, proc.stdout, proc.stderr


def validate_builtin_scheduler(name, args, output_dir):
    command = [sys.executable, "-m", "qtop_py.cli"] + list(args)
    returncode, stdout_path, stderr_path, stdout, stderr = run_command(command, output_dir, name)
    missing = [marker for marker in REQUIRED_MARKERS if marker not in stdout]
    passed = returncode == 0 and not missing
    return {
        "name": name,
        "kind": "bundled-contrib",
        "passed": passed,
        "returncode": returncode,
        "missing_markers": missing,
        "stdout": stdout_path,
        "stderr": stderr_path,
        "stdout_lines": len(stdout.splitlines()),
        "stderr_lines": len(stderr.splitlines()),
    }


def validate_slurm_samples(output_dir):
    slurm_output_dir = os.path.join(output_dir, "slurm-rendered")
    command = [
        sys.executable,
        os.path.join(ROOT, "tools", "validate_slurm_samples.py"),
        SLURM_SAMPLES,
        "--output",
        slurm_output_dir,
    ]
    returncode, stdout_path, stderr_path, stdout, stderr = run_command(command, output_dir, "slurm")
    rendered = []
    if os.path.isdir(slurm_output_dir):
        rendered = sorted(name for name in os.listdir(slurm_output_dir) if name.endswith(".out"))
    return {
        "name": "slurm",
        "kind": "command-trace-render",
        "passed": returncode == 0 and bool(rendered),
        "returncode": returncode,
        "rendered_outputs": rendered,
        "stdout": stdout_path,
        "stderr": stderr_path,
        "stdout_lines": len(stdout.splitlines()),
        "stderr_lines": len(stderr.splitlines()),
    }


def main():
    parser = argparse.ArgumentParser(description="Run the qtop scheduler sample gate used by CI.")
    parser.add_argument("--output", default=os.path.join(ROOT, "artifacts", "sample-gate"), help="Directory for rendered qtop outputs and logs.")
    parser.add_argument("--max-failures", type=int, default=0, help="Maximum scheduler sample failures allowed before this gate fails.")
    args = parser.parse_args()

    output_dir = os.path.realpath(args.output)
    mkdir_p(output_dir)

    results = [validate_builtin_scheduler(name, case_args, output_dir) for name, case_args in BUILTIN_CASES]
    results.append(validate_slurm_samples(output_dir))

    manifest_path = os.path.join(output_dir, "manifest.json")
    write_text(manifest_path, json.dumps(results, indent=2, sort_keys=True))

    failures = [result for result in results if not result["passed"]]
    for result in results:
        status = "PASS" if result["passed"] else "FAIL"
        print("%s %s (%s)" % (status, result["name"], result["kind"]))
    print("sample-gate failures=%s max_failures=%s manifest=%s" % (len(failures), args.max_failures, manifest_path))

    return 0 if len(failures) <= args.max_failures else 1


if __name__ == "__main__":
    raise SystemExit(main())
