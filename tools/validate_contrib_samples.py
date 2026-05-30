#!/usr/bin/env python3
"""Render bundled qtop contrib samples for the fast CI sample gate."""

import argparse
import os
import subprocess
import sys


CONTRIB_CASES = (
    ("pbs", ["-c", "ON", "-raF", "-b", "pbs"]),
    ("sge", ["-c", "ON", "-Fadvv", "-b", "sge"]),
)


def qtop_env(output_dir):
    runtime_home = os.path.join(output_dir, ".qtop-home")
    os.makedirs(runtime_home, exist_ok=True)
    env = os.environ.copy()
    env["HOME"] = runtime_home
    return env


def run_case(repo_root, samples_dir, output_dir, scheduler):
    case_output = os.path.join(output_dir, scheduler)
    os.makedirs(case_output, exist_ok=True)
    args = dict(CONTRIB_CASES)[scheduler]
    command = [sys.executable, "-m", "qtop_py.cli", "-s", samples_dir, "-O", "-o", "savepath=%s" % case_output] + args
    completed = subprocess.run(
        command,
        cwd=repo_root,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=qtop_env(output_dir),
        universal_newlines=True,
    )
    if completed.returncode:
        return "qtop failed for bundled %s sample:\nSTDOUT:\n%s\nSTDERR:\n%s" % (scheduler, completed.stdout, completed.stderr)
    return None


def main():
    parser = argparse.ArgumentParser(description="Render bundled PBS and SGE qtop contrib samples.")
    parser.add_argument("samples_dir", help="Directory containing bundled qtop contrib samples")
    parser.add_argument("--output", default="build/qtop-contrib-rendered", help="Directory for qtop rendered output")
    parser.add_argument("--max-failures", type=int, default=0, help="Maximum tolerated sample failures before returning non-zero")
    args = parser.parse_args()

    repo_root = os.getcwd()
    failures = []
    for scheduler, _case_args in CONTRIB_CASES:
        failure = run_case(repo_root, args.samples_dir, args.output, scheduler)
        if failure:
            failures.append(failure)

    for failure in failures:
        print(failure, file=sys.stderr)

    passed = len(CONTRIB_CASES) - len(failures)
    print("Validated %s/%s bundled contrib samples" % (passed, len(CONTRIB_CASES)))
    if len(failures) > args.max_failures:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
