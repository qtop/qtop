#!/usr/bin/env python3
"""Fast sample validation gate for CI systems.

The gate intentionally uses existing sample fixtures and focused tests so GitHub
and GitLab can fail on the same small, reproducible signal.
"""

from __future__ import print_function

import argparse
import os
import subprocess
import sys


SAMPLE_FILES = [
    "qtop_py/contrib/pbsnodes_a.txt",
    "qtop_py/contrib/pbs_dvv_out.ref",
    "qtop_py/contrib/qstat.txt",
    "qtop_py/contrib/qstat_q.txt",
    "qtop_py/contrib/oarstat.txt",
    "qtop_py/contrib/oarnodes_s_Y.txt",
    "qtop_py/contrib/sger_dvv_out.ref",
    "qtop_py/contrib/qstat.F.xml.stdout",
]

FOCUSED_TESTS = [
    "tests/plugins/test_pbs.py",
]


def write_artifact(path, lines):
    with open(path, "w", encoding="utf-8") as handle:
        handle.write("\n".join(lines))
        handle.write("\n")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--max-failures", type=int, default=0)
    parser.add_argument("--artifact-dir", default="artifacts/qtop-sample-gate")
    args = parser.parse_args()

    os.makedirs(args.artifact_dir, exist_ok=True)
    summary_path = os.path.join(args.artifact_dir, "summary.txt")
    pytest_log_path = os.path.join(args.artifact_dir, "pytest.log")

    lines = [
        "qtop sample validation gate",
        "max_failures={}".format(args.max_failures),
        "sample_source=qtop_py/contrib",
        "tests={}".format(" ".join(FOCUSED_TESTS)),
        "",
        "sample files:",
    ]

    failures = 0
    for sample in SAMPLE_FILES:
        exists = os.path.exists(sample)
        lines.append("- {}: {}".format(sample, "present" if exists else "missing"))
        if not exists:
            failures += 1

    cmd = [sys.executable, "-m", "pytest", "-q"] + FOCUSED_TESTS
    proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    write_artifact(pytest_log_path, proc.stdout.splitlines())
    lines.extend(["", "pytest_exit={}".format(proc.returncode), "pytest_log={}".format(pytest_log_path)])

    if proc.returncode != 0:
        failures += 1

    lines.extend(["", "failures={}".format(failures)])
    write_artifact(summary_path, lines)
    print("\n".join(lines))

    if failures > args.max_failures:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
