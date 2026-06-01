#!/usr/bin/env python3
##
## qtop is a tool to monitor queuing systems - https://github.com/qtop/qtop
##
## Copyright (c) 2026 San Phan
##
## SPDX-License-Identifier: MIT
##

"""Render built-in qtop scheduler samples and save their output."""

import argparse
import subprocess
import sys
from pathlib import Path


SAMPLES = (
    ("sge", ("-Fadvv", "-b", "sge")),
    ("pbs", ("-raF", "-b", "pbs")),
    ("oar", ("--experimental", "-FAardvvv", "-b", "oar")),
)


def run_sample(name, qtop_args, contrib_dir, output_dir):
    command = [
        sys.executable,
        "-m",
        "qtop_py.qtop",
        "-s",
        str(contrib_dir),
        "-c",
        "ON",
    ] + list(qtop_args)
    completed = subprocess.run(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        universal_newlines=True,
    )
    if completed.returncode:
        raise RuntimeError(
            "qtop failed for %s:\nSTDOUT:\n%s\nSTDERR:\n%s"
            % (name, completed.stdout, completed.stderr)
        )
    if not completed.stdout.strip():
        raise RuntimeError("qtop produced no output for %s" % name)
    (output_dir / ("%s.ans" % name)).write_text(completed.stdout)
    (output_dir / ("%s.stderr" % name)).write_text(completed.stderr)


def main():
    parser = argparse.ArgumentParser(description="Render built-in PBS, SGE, and OAR qtop samples.")
    parser.add_argument("--contrib-dir", type=Path, default=Path("qtop_py/contrib"))
    parser.add_argument("--output", type=Path, default=Path("/tmp/qtop-contrib-rendered"))
    args = parser.parse_args()

    if not args.contrib_dir.is_dir():
        raise RuntimeError("contrib sample directory not found: %s" % args.contrib_dir)
    args.output.mkdir(parents=True, exist_ok=True)

    for name, qtop_args in SAMPLES:
        run_sample(name, qtop_args, args.contrib_dir, args.output)

    print("Validated %s contrib scheduler samples" % len(SAMPLES))


if __name__ == "__main__":
    main()
