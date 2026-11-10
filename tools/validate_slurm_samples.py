#!/usr/bin/env python3
##
## qtop is a tool to monitor queuing systems - https://github.com/qtop/qtop
##
## Copyright (c) 2026 Nicola Trozzi
##
## SPDX-License-Identifier: MIT
##
"""Render every Slurm command-trace sample with qtop, as a smoke test."""

import argparse
import os
import subprocess
import sys

try:
    from tools.private_output import private_output_dir
except ImportError:  # run as a script: tools/ itself is sys.path[0]
    from private_output import private_output_dir

DEFAULT_OUTPUT = "artifacts/slurm-rendered"


def iter_sample_dirs(samples_dir):
    """Yield (name, path) for each sample directory with squeue.txt and sinfo.txt."""
    for name in sorted(os.listdir(samples_dir)):
        sample_dir = os.path.join(samples_dir, name)
        if not os.path.isdir(sample_dir):
            continue
        if os.path.isfile(os.path.join(sample_dir, "squeue.txt")) and os.path.isfile(os.path.join(sample_dir, "sinfo.txt")):
            yield name, sample_dir


def run_qtop(sample_name, sample_dir, output_dir):
    """Render one sample with an isolated HOME; return the result."""
    env = os.environ.copy()
    env["QTOP_SCHEDULER"] = "slurm"
    rendered_output_dir = os.path.abspath(output_dir)
    os.makedirs(rendered_output_dir, exist_ok=True)
    # qtop writes user-local logs under HOME, so each sample gets an isolated
    # HOME inside the rendered-output tree: <output>/.home/<sample-name>.
    qtop_home = os.path.join(rendered_output_dir, ".home", sample_name)
    os.makedirs(qtop_home, exist_ok=True)
    env["HOME"] = qtop_home
    command = [sys.executable, "-m", "qtop_py.qtop", "-b", "slurm", "-s", sample_dir, "-O", "-o", "savepath=%s" % rendered_output_dir]
    completed = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env, universal_newlines=True)
    if completed.returncode:
        raise RuntimeError("qtop failed for %s:\nSTDOUT:\n%s\nSTDERR:\n%s" % (sample_name, completed.stdout, completed.stderr))


def main():
    """Render all samples and return non-zero on any failure."""
    parser = argparse.ArgumentParser(description="Render all Slurm qtop command-trace samples.")
    parser.add_argument("samples_dir", help="Directory containing Slurm sample subdirectories")
    parser.add_argument("--output", default=DEFAULT_OUTPUT, help="Private directory for qtop rendered output (default: %(default)s)")
    args = parser.parse_args()
    try:
        private_output_dir(args.output)
    except ValueError as error:
        parser.error(str(error))

    sample_dirs = list(iter_sample_dirs(args.samples_dir))
    if not sample_dirs:
        raise RuntimeError("No Slurm samples found in %s" % args.samples_dir)

    for sample_name, sample_dir in sample_dirs:
        run_qtop(sample_name, sample_dir, args.output)

    print("Validated %s Slurm samples" % len(sample_dirs))


if __name__ == "__main__":
    main()
