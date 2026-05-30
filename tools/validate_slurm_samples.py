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
    parser.add_argument("--max-failures", type=int, default=0, help="Allowed failed sample renders")
    args = parser.parse_args()

    sample_dirs = list(iter_sample_dirs(args.samples_dir))
    if not sample_dirs:
        raise RuntimeError("No Slurm samples found in %s" % args.samples_dir)

    failures = []
    for sample_name, sample_dir in sample_dirs:
        try:
            run_qtop(sample_name, sample_dir, args.output)
        except RuntimeError as exc:
            failures.append("%s: %s" % (sample_name, exc))
            if len(failures) > args.max_failures:
                raise

    print("Validated %s Slurm samples with %s failures" % (len(sample_dirs) - len(failures), len(failures)))


if __name__ == "__main__":
    main()
