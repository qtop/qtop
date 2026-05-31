#!/usr/bin/env python3
"""Run the small scheduler sample gate shared by local and hosted CI."""

import argparse
import json
import os
import signal
import subprocess
import sys


def run_step(name, command, output_dir):
    print("== %s ==" % name)
    completed = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
    stdout_path = os.path.join(output_dir, "%s.stdout.log" % name)
    stderr_path = os.path.join(output_dir, "%s.stderr.log" % name)
    with open(stdout_path, "w") as handle:
        handle.write(completed.stdout)
    with open(stderr_path, "w") as handle:
        handle.write(completed.stderr)
    print(completed.stdout, end="")
    if completed.stderr:
        print(completed.stderr, file=sys.stderr, end="")
    return {
        "name": name,
        "command": command,
        "returncode": completed.returncode,
        "stdout": os.path.basename(stdout_path),
        "stderr": os.path.basename(stderr_path),
    }


def skipped_step(name, reason):
    print("== %s ==" % name)
    print("SKIPPED: %s" % reason)
    return {"name": name, "status": "skipped", "reason": reason}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", default="artifacts/sample-gate", help="Directory for logs and rendered scheduler output")
    parser.add_argument("--max-failures", type=int, default=0, help="Maximum failed gate steps allowed")
    parser.add_argument("--pbs-samples", help="Optional external archived PBS sample directory")
    parser.add_argument("--pbs-limit", type=int, default=10, help="External PBS samples to render when --pbs-samples is set")
    args = parser.parse_args()

    os.makedirs(args.output, exist_ok=True)
    slurm_output = os.path.join(args.output, "slurm-rendered")
    steps = [
        run_step(
            "scheduler-unit-tests",
            [sys.executable, "-m", "pytest", "tests/plugins/test_pbs.py", "tests/plugins/test_sge.py", "tests/plugins/test_slurm.py", "-q"],
            args.output,
        ),
    ]
    if hasattr(signal, "SIGPIPE"):
        steps.append(
            run_step(
                "slurm-render",
                [sys.executable, "tools/validate_slurm_samples.py", "tests/plugins/slurm_samples", "--output", slurm_output],
                args.output,
            )
        )
    else:
        steps.append(skipped_step("slurm-render", "qtop CLI requires SIGPIPE; render remains mandatory on Linux CI"))

    if args.pbs_samples:
        steps.append(
            run_step(
                "pbs-archived-render",
                [
                    sys.executable,
                    "tools/validate_pbs_samples.py",
                    args.pbs_samples,
                    "--limit",
                    str(args.pbs_limit),
                    "--output",
                    os.path.join(args.output, "pbs-rendered"),
                ],
                args.output,
            )
        )

    failures = sum(step.get("returncode", 0) != 0 for step in steps)
    manifest = {
        "failure_policy": {"max_failures": args.max_failures, "observed_failures": failures},
        "pbs_archived_samples": args.pbs_samples or "not configured; bundled PBS parser fixtures still run",
        "steps": steps,
    }
    with open(os.path.join(args.output, "manifest.json"), "w") as handle:
        json.dump(manifest, handle, indent=2)
        handle.write("\n")

    print("sample-gate failures=%s max_failures=%s output=%s" % (failures, args.max_failures, args.output))
    return 0 if failures <= args.max_failures else 1


if __name__ == "__main__":
    raise SystemExit(main())
