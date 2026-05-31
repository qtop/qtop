#!/usr/bin/env python3
##
## qtop is a tool to monitor queuing systems - https://github.com/qtop/qtop
##
## Copyright (c) 2026 Jamil Ur Rehman Ahmadzai
##
## SPDX-License-Identifier: MIT
##

import argparse
import os
import re
import subprocess
import sys


CONTROL_CHARS = re.compile(r"[\u202a-\u202e\u2066-\u2069]")
GENERATED_PATHS = re.compile(r"(^|/)(m4|autogen|configure|Makefile\.in|cmake|fixtures?)/|(\.xz|\.lzma|\.gz|\.bin|\.dat)$", re.I)


def git_output(args):
    proc = subprocess.run(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
    if proc.returncode:
        raise RuntimeError(proc.stderr.strip() or "command failed: %s" % " ".join(args))
    return proc.stdout


def changed_paths(base_ref):
    paths = set()
    try:
        paths.update(path for path in git_output(["git", "diff", "--name-only", "%s...HEAD" % base_ref]).splitlines() if path)
    except RuntimeError:
        pass
    paths.update(path for path in git_output(["git", "diff", "--name-only"]).splitlines() if path)
    paths.update(path for path in git_output(["git", "diff", "--cached", "--name-only"]).splitlines() if path)
    return sorted(paths)


def has_non_ascii_added_text(base_ref):
    diffs = []
    try:
        diffs.append(git_output(["git", "diff", "-U0", "%s...HEAD" % base_ref]))
    except RuntimeError:
        pass
    diffs.append(git_output(["git", "diff", "-U0"]))
    diffs.append(git_output(["git", "diff", "--cached", "-U0"]))

    offenders = []
    for diff in diffs:
        for line in diff.splitlines():
            if not line.startswith("+") or line.startswith("+++"):
                continue
            text = line[1:]
            if CONTROL_CHARS.search(text):
                offenders.append("bidi/control: %s" % text)
                continue
            for char in text:
                codepoint = ord(char)
                if char in ("\t", "\n", "\r"):
                    continue
                if codepoint < 32 or codepoint > 126:
                    offenders.append("non-ascii/control: %s" % text)
                    break
    return offenders


def main():
    parser = argparse.ArgumentParser(description="Check changed files for CI/review hazards.")
    parser.add_argument("--base-ref", default=os.environ.get("BASE_REF", "origin/develop"))
    args = parser.parse_args()

    paths = changed_paths(args.base_ref)
    path_offenders = [path for path in paths if GENERATED_PATHS.search(path)]
    text_offenders = has_non_ascii_added_text(args.base_ref)

    if path_offenders:
        print("Unexpected generated/binary-looking changed paths:")
        for path in path_offenders:
            print("  %s" % path)
    if text_offenders:
        print("Unexpected unicode/control characters in added lines:")
        for offender in text_offenders:
            print("  %s" % offender)

    if path_offenders or text_offenders:
        return 1

    print("fortifications: checked %s changed files" % len(paths))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
