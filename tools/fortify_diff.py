#!/usr/bin/env python3
##
## qtop is a tool to monitor queuing systems - https://github.com/qtop/qtop
##
## Copyright (c) 2026 San Phan
##
## SPDX-License-Identifier: MIT
##

"""Inspect a contribution diff for review hazards before CI runs tests."""

import argparse
import re
import subprocess
import sys


BIDI_CONTROLS = set(range(0x202A, 0x202F)) | set(range(0x2066, 0x206A))
GENERATED_OR_BINARY_PATH = re.compile(
    r"(^|/)(m4|autogen|configure|Makefile\.in|cmake|tests?/files|fixtures?)/"
    r"|(\.xz|\.lzma|\.gz|\.bin|\.dat)$",
    re.IGNORECASE,
)
EVAL_CALL = re.compile(r"\beval\s*\(")


def git_output(args):
    completed = subprocess.run(
        ["git"] + args,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        universal_newlines=True,
    )
    if completed.returncode:
        raise RuntimeError(
            "git %s failed:\n%s" % (" ".join(args), completed.stderr.strip())
        )
    return completed.stdout


def resolve_base(preferred_base):
    candidates = [preferred_base, "origin/develop", "upstream/develop", "HEAD~1"]
    seen = set()
    for candidate in candidates:
        if candidate in seen:
            continue
        seen.add(candidate)
        completed = subprocess.run(
            ["git", "rev-parse", "--verify", "%s^{commit}" % candidate],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True,
        )
        if completed.returncode == 0:
            return candidate
    raise RuntimeError("could not resolve a base ref for fortification checks")


def has_control_or_non_ascii(text):
    for char in text:
        codepoint = ord(char)
        if codepoint in (0x09, 0x0A, 0x0D):
            continue
        if 0x20 <= codepoint <= 0x7E and codepoint not in BIDI_CONTROLS:
            continue
        return True
    return False


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base", default="origin/develop")
    args = parser.parse_args()

    base = resolve_base(args.base)
    changed_files = [
        line
        for line in git_output(["diff", "--name-only", "%s...HEAD" % base]).splitlines()
        if line
    ]
    failures = []

    for path in changed_files:
        if GENERATED_OR_BINARY_PATH.search(path):
            failures.append("manual review required for generated/binary path: %s" % path)

    diff = git_output(["diff", "-U0", "%s...HEAD" % base])
    for line_number, line in enumerate(diff.splitlines(), 1):
        if not line.startswith("+") or line.startswith("+++"):
            continue
        if has_control_or_non_ascii(line):
            failures.append("non-ASCII/control character in added diff line %s" % line_number)
        if EVAL_CALL.search(line):
            failures.append("new eval() call in added diff line %s" % line_number)

    if failures:
        print("== fortify failures ==")
        for failure in failures:
            print(failure)
        return 1

    print("Fortify OK against %s (%s changed files)" % (base, len(changed_files)))
    return 0


if __name__ == "__main__":
    sys.exit(main())
