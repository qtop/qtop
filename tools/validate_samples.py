#!/usr/bin/env python3
"""Run qtop sample validation through one CI-friendly entry point."""

import argparse
import json
import os
import subprocess
import sys


REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def relpath(*parts):
    return os.path.join(REPO_ROOT, *parts)


def run_command(command):
    completed = subprocess.run(command, cwd=REPO_ROOT, universal_newlines=True)
    return completed.returncode


def scheduler_list(value):
    if value == "all":
        return ["slurm", "pbs"]
    return [part.strip() for part in value.split(",") if part.strip()]


def validate_slurm(args):
    output = os.path.join(args.output, "slurm")
    command = [
        sys.executable,
        relpath("tools", "validate_slurm_samples.py"),
        args.slurm_samples_dir,
        "--output",
        output,
        "--max-failures",
        str(args.max_failures),
    ]
    if args.slurm_limit:
        command.extend(["--limit", str(args.slurm_limit)])
    return run_command(command), output


def validate_pbs(args):
    output = os.path.join(args.output, "pbs")
    if not os.path.isdir(args.pbs_samples_dir):
        print("skip pbs: samples directory not found: %s" % args.pbs_samples_dir)
        return 0, output
    command = [
        sys.executable,
        relpath("tools", "validate_pbs_samples.py"),
        args.pbs_samples_dir,
        "--limit",
        str(args.pbs_limit),
        "--output",
        output,
        "--max-failures",
        str(args.max_failures),
    ]
    return run_command(command), output


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scheduler", default=os.environ.get("SAMPLE_SCHEDULERS", "slurm"), help="slurm, pbs, comma list, or all")
    parser.add_argument("--slurm-samples-dir", default=os.environ.get("SLURM_SAMPLES_DIR", relpath("tests", "plugins", "slurm_samples")))
    parser.add_argument("--pbs-samples-dir", default=os.environ.get("PBS_SAMPLES_DIR", os.path.join(REPO_ROOT, "..", "qtop-test-repo", "qtop5", "results")))
    parser.add_argument("--slurm-limit", type=int, default=int(os.environ.get("SLURM_SAMPLE_LIMIT", "0")))
    parser.add_argument("--pbs-limit", type=int, default=int(os.environ.get("PBS_SAMPLE_LIMIT", "10")))
    parser.add_argument("--max-failures", type=int, default=int(os.environ.get("SAMPLE_MAX_FAILURES", "0")))
    parser.add_argument("--output", default=os.environ.get("SAMPLE_OUTPUT_DIR", os.path.join(REPO_ROOT, "artifacts", "qtop-sample-gate")))
    args = parser.parse_args()

    if not os.path.isdir(args.output):
        os.makedirs(args.output)

    results = []
    failed = 0
    validators = {
        "slurm": validate_slurm,
        "pbs": validate_pbs,
    }
    for scheduler in scheduler_list(args.scheduler):
        if scheduler not in validators:
            print("unknown scheduler: %s" % scheduler)
            failed += 1
            continue
        returncode, output = validators[scheduler](args)
        results.append({"scheduler": scheduler, "returncode": returncode, "output": output})
        if returncode:
            failed += 1

    manifest_path = os.path.join(args.output, "sample-gate.json")
    with open(manifest_path, "w") as manifest:
        json.dump(results, manifest, indent=2)
    print("sample-gate schedulers=%s failed=%s output=%s" % (",".join(scheduler_list(args.scheduler)), failed, args.output))
    return 0 if failed <= args.max_failures else 1


if __name__ == "__main__":
    sys.exit(main())
