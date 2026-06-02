#!/usr/bin/env python3
"""Lightweight repository health checks for CI and review."""

import argparse
import os
import re
import subprocess
import sys


CONTROL_OR_BIDI = re.compile(
    b"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]"
    b"|\xe2\x80\xaa|\xe2\x80\xab|\xe2\x80\xac|\xe2\x80\xad|\xe2\x80\xae"
    b"|\xe2\x81\xa6|\xe2\x81\xa7|\xe2\x81\xa8|\xe2\x81\xa9"
)
GENERATED_OR_BINARY = re.compile(r"(^|/)(m4|autogen|configure|Makefile\.in|cmake|tests?/files|fixtures?)/|\.(xz|lzma|gz|bin|dat)$", re.I)
EVAL_ADDITION = re.compile(r"^\+.*\b" + "eval" + r"\s*\(")


def run_git(args):
    try:
        out = subprocess.check_output(["git"] + args, stderr=subprocess.DEVNULL)
    except subprocess.CalledProcessError:
        return ""
    return out.decode("utf-8", "replace")


def changed_files(base_ref):
    names = set()
    if base_ref:
        names.update(run_git(["diff", "--name-only", "%s...HEAD" % base_ref]).splitlines())
    names.update(run_git(["diff", "--name-only"]).splitlines())
    names.update(run_git(["diff", "--name-only", "--cached"]).splitlines())
    names.update(run_git(["ls-files", "--others", "--exclude-standard"]).splitlines())
    return sorted(name for name in names if name)


def diff_text(base_ref):
    pieces = []
    if base_ref:
        pieces.append(run_git(["diff", "-U0", "%s...HEAD" % base_ref]))
    pieces.append(run_git(["diff", "-U0"]))
    pieces.append(run_git(["diff", "-U0", "--cached"]))
    return "\n".join(piece for piece in pieces if piece)


def check_text_file(path):
    try:
        with open(path, "rb") as handle:
            data = handle.read()
    except IOError:
        return []
    if CONTROL_OR_BIDI.search(data):
        return ["control or bidirectional marker found in %s" % path]
    return []


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-ref", default=os.environ.get("QTOP_FORTIFY_BASE", "origin/develop"))
    args = parser.parse_args()

    errors = []
    for path in changed_files(args.base_ref):
        normalized = path.replace(os.sep, "/")
        if GENERATED_OR_BINARY.search(normalized):
            errors.append("manual review required for generated/binary path: %s" % path)
        if os.path.isfile(path):
            errors.extend(check_text_file(path))

    for line in diff_text(args.base_ref).splitlines():
        if line.startswith("+++") or not line.startswith("+"):
            continue
        if EVAL_ADDITION.search(line):
            errors.append("new dynamic evaluation usage in diff: %s" % line[:160])

    if errors:
        for error in errors:
            print("fortification: %s" % error)
        return 1
    print("fortifications OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
