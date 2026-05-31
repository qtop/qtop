#!/usr/bin/env python3
"""Lightweight CI guard for risky diff content."""

from __future__ import print_function

import argparse
import os
import re
import subprocess
import sys


CONTROL_OR_BIDI = re.compile(
    r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f\u202a\u202b\u202c\u202d\u202e\u2066\u2067\u2068\u2069]"
)
GENERATED_OR_BINARY = re.compile(
    r"(^|/)(m4|autogen|configure|Makefile\.in|cmake|tests?/files|fixtures?)/|"
    r"\.(xz|lzma|gz|bin|dat)$",
    re.IGNORECASE,
)


def git_output(args):
    process = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
    stdout, stderr = process.communicate()
    if process.returncode != 0:
        raise RuntimeError(stderr.strip() or "git command failed: {}".format(" ".join(args)))
    return stdout


def changed_files(base_ref):
    return [line for line in git_output(["git", "diff", "--name-only", "{}...HEAD".format(base_ref)]).splitlines() if line]


def changed_lines(base_ref):
    for line in git_output(["git", "diff", "-U0", "{}...HEAD".format(base_ref)]).splitlines():
        if line.startswith("+") and not line.startswith("+++"):
            yield line[1:]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-ref", default=os.environ.get("QTOP_FORTIFY_BASE_REF", "origin/develop"))
    args = parser.parse_args()

    problems = []

    for path in changed_files(args.base_ref):
        if GENERATED_OR_BINARY.search(path.replace("\\", "/")):
            problems.append("manual review required for generated/binary-looking path: {}".format(path))

    for line_number, line in enumerate(changed_lines(args.base_ref), start=1):
        if CONTROL_OR_BIDI.search(line):
            problems.append("control or bidi character in added diff line {}".format(line_number))

    if problems:
        for problem in problems:
            print(problem)
        return 1

    print("fortify-diff: OK against {}".format(args.base_ref))
    return 0


if __name__ == "__main__":
    sys.exit(main())
