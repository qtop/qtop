#!/usr/bin/env python3
##
## qtop is a tool to monitor queuing systems - https://github.com/qtop/qtop
##
## Copyright (c) 2026 Nicola Trozzi
##
## SPDX-License-Identifier: MIT
##

import argparse
import os
import subprocess
import sys


def qtop_env(output_dir):
    """Keep qtop logs/config under the rendered-output tree for repeatable gates."""
    runtime_home = os.path.join(output_dir, ".qtop-home")
    os.makedirs(runtime_home, exist_ok=True)
    env = os.environ.copy()
    env["HOME"] = runtime_home
    env["QTOP_SCHEDULER"] = "slurm"
    return env


def iter_sample_dirs(samples_dir):
    for name in sorted(os.listdir(samples_dir)):
        sample_dir = os.path.join(samples_dir, name)
        if not os.path.isdir(sample_dir):
            continue
        if os.path.isfile(os.path.join(sample_dir, "squeue.txt")) and os.path.isfile(os.path.join(sample_dir, "sinfo.txt")):
            yield name, sample_dir


def run_qtop(sample_name, sample_dir, output_dir):
    os.makedirs(output_dir, exist_ok=True)
    env = qtop_env(output_dir)
    command = [sys.executable, "-m", "qtop_py.cli", "-b", "slurm", "-s", sample_dir, "-O", "-o", "savepath=%s" % output_dir]
    completed = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env, universal_newlines=True)
    if completed.returncode:
        return "qtop failed for %s:\nSTDOUT:\n%s\nSTDERR:\n%s" % (sample_name, completed.stdout, completed.stderr)
    return None


def main():
    parser = argparse.ArgumentParser(description="Render all Slurm qtop command-trace samples.")
    parser.add_argument("samples_dir", help="Directory containing Slurm sample subdirectories")
    parser.add_argument("--output", default="/tmp/qtop-slurm-rendered", help="Directory for qtop rendered output")
    parser.add_argument("--max-failures", type=int, default=0, help="Maximum tolerated sample failures before returning non-zero")
    args = parser.parse_args()

    sample_dirs = list(iter_sample_dirs(args.samples_dir))
    if not sample_dirs:
        raise RuntimeError("No Slurm samples found in %s" % args.samples_dir)

    failures = []
    for sample_name, sample_dir in sample_dirs:
        failure = run_qtop(sample_name, sample_dir, args.output)
        if failure:
            failures.append(failure)

    for failure in failures:
        print(failure, file=sys.stderr)

    passed = len(sample_dirs) - len(failures)
    print("Validated %s/%s Slurm samples" % (passed, len(sample_dirs)))
    if len(failures) > args.max_failures:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
