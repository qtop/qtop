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
        return "qtop failed for %s:\nSTDOUT:\n%s\nSTDERR:\n%s" % (sample_name, completed.stdout, completed.stderr)
    return None


def main():
    parser = argparse.ArgumentParser(description="Render all Slurm qtop command-trace samples.")
    parser.add_argument("samples_dir", help="Directory containing Slurm sample subdirectories")
    parser.add_argument("--limit", type=int, default=0, help="Maximum number of samples to render; 0 means all")
    parser.add_argument("--max-failures", type=int, default=0, help="Allowed failed sample renders before exiting non-zero")
    parser.add_argument("--output", default="/tmp/qtop-slurm-rendered", help="Directory for qtop rendered output")
    args = parser.parse_args()

    sample_dirs = list(iter_sample_dirs(args.samples_dir))
    if not sample_dirs:
        raise RuntimeError("No Slurm samples found in %s" % args.samples_dir)
    if args.limit:
        sample_dirs = sample_dirs[: args.limit]

    manifest = []
    failures = []
    for sample_name, sample_dir in sample_dirs:
        error = run_qtop(sample_name, sample_dir, args.output)
        if error:
            failures.append({"sample": sample_name, "error": error})
            if len(failures) > args.max_failures:
                break
            continue
        manifest.append({"sample": sample_name, "source": sample_dir, "output": args.output})

    if not os.path.isdir(args.output):
        os.makedirs(args.output)
    with open(os.path.join(args.output, "manifest.json"), "w") as manifest_file:
        json.dump(manifest, manifest_file, indent=2)
    if failures:
        with open(os.path.join(args.output, "failures.json"), "w") as failures_file:
            json.dump(failures, failures_file, indent=2)

    print("validated=%s failures=%s output=%s" % (len(manifest), len(failures), args.output))
    if len(failures) > args.max_failures:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
