#!/usr/bin/env python3
##
## qtop is a tool to monitor queuing systems - https://github.com/qtop/qtop
##
## Copyright (c) 2026 Nicola Trozzi
##
## SPDX-License-Identifier: MIT
##

import argparse
import json
import os
import subprocess
import sys


def iter_sample_dirs(samples_dir):
    for name in sorted(os.listdir(samples_dir)):
        sample_dir = os.path.join(samples_dir, name)
        if not os.path.isdir(sample_dir):
            continue
        if os.path.isfile(os.path.join(sample_dir, "squeue.txt")) and os.path.isfile(os.path.join(sample_dir, "sinfo.txt")):
            yield name, sample_dir


def run_qtop(sample_name, sample_dir, output_dir):
    env = os.environ.copy()
    env["QTOP_SCHEDULER"] = "slurm"
    os.makedirs(output_dir, exist_ok=True)
    command = [sys.executable, "-m", "qtop_py.cli", "-b", "slurm", "-s", sample_dir, "-O", "-o", "savepath=%s" % output_dir]
    completed = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env, universal_newlines=True)
    if completed.returncode:
        raise RuntimeError("qtop failed for %s:\nSTDOUT:\n%s\nSTDERR:\n%s" % (sample_name, completed.stdout, completed.stderr))


def main():
    parser = argparse.ArgumentParser(description="Render all Slurm qtop command-trace samples.")
    parser.add_argument("samples_dir", help="Directory containing Slurm sample subdirectories")
    parser.add_argument("--output", default="/tmp/qtop-slurm-rendered", help="Directory for qtop rendered output")
    parser.add_argument("--max-failures", type=int, default=0, help="Maximum sample failures allowed before the gate fails")
    args = parser.parse_args()

    sample_dirs = list(iter_sample_dirs(args.samples_dir))
    if not sample_dirs:
        raise RuntimeError("No Slurm samples found in %s" % args.samples_dir)

    os.makedirs(args.output, exist_ok=True)
    failures = []
    for sample_name, sample_dir in sample_dirs:
        try:
            run_qtop(sample_name, sample_dir, args.output)
        except RuntimeError as err:
            failures.append({"sample": sample_name, "error": str(err)})
            if len(failures) > args.max_failures:
                break

    with open(os.path.join(args.output, "failures.json"), "w") as failure_file:
        json.dump(failures, failure_file, indent=2)

    validated = len(sample_dirs) - len(failures)
    print("Validated %s Slurm samples; failures=%s; output=%s" % (validated, len(failures), args.output))
    if len(failures) > args.max_failures:
        raise RuntimeError("Slurm sample gate failed with %s failures" % len(failures))


if __name__ == "__main__":
    main()
