#!/usr/bin/env python3
"""Inspect changed files for suspicious text and generated artifacts."""

import argparse
import os
import re
import subprocess
import sys


CONTROL_CHARACTERS = re.compile(
    "[\u202a-\u202e\u2066-\u2069]"
)
GENERATED_PATH = re.compile(
    r"(^|/)(m4|autogen|configure|Makefile\.in|cmake|tests?/files|fixtures?)/|"
    r"\.(xz|lzma|gz|bin|dat)$",
    re.IGNORECASE,
)


def git_changed_files(base_ref):
    commands = []
    if base_ref:
        commands.append(["git", "diff", "--name-only", "%s...HEAD" % base_ref])
    commands.append(["git", "diff", "--name-only", "HEAD^", "HEAD"])
    for command in commands:
        try:
            completed = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
        except OSError:
            break
        if completed.returncode == 0:
            return [line.strip() for line in completed.stdout.splitlines() if line.strip()]
    return []


def inspect(paths):
    findings = []
    for path in paths:
        normalized = path.replace("\\", "/")
        if GENERATED_PATH.search(normalized):
            findings.append("%s: generated or binary-looking path requires manual review" % path)
        if not os.path.isfile(path):
            continue
        try:
            with open(path, "r", encoding="utf-8") as handle:
                for line_number, line in enumerate(handle, 1):
                    if CONTROL_CHARACTERS.search(line):
                        findings.append("%s:%s: bidi control character found" % (path, line_number))
        except (UnicodeDecodeError, OSError):
            continue
    return findings


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-ref", default=os.environ.get("QTOP_FORTIFICATIONS_BASE", "origin/develop"))
    parser.add_argument("paths", nargs="*", help="Explicit paths for local checks without git metadata")
    args = parser.parse_args()
    paths = args.paths or git_changed_files(args.base_ref)
    if not paths:
        print("fortifications: no changed files detected; pass paths explicitly outside a git checkout")
        return 0
    findings = inspect(paths)
    if findings:
        print("\n".join(findings), file=sys.stderr)
        return 1
    print("fortifications: checked %s changed files" % len(paths))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
