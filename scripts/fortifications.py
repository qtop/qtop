#!/usr/bin/env python3
"""Lightweight repository health checks for CI review gates."""

from __future__ import print_function

import os
import re
import subprocess
import sys


GENERATED_OR_BINARY = re.compile(
    r"(^|/)(m4|autogen|configure|Makefile\.in|cmake|tests?/files|fixtures?)/"
    r"|(\.xz|\.lzma|\.gz|\.bin|\.dat)$",
    re.IGNORECASE,
)
BIDI_OR_CONTROL = re.compile(
    u"[\u202A\u202B\u202C\u202D\u202E\u2066\u2067\u2068\u2069]"
    u"|[^\x09\x0A\x0D\x20-\x7E]"
)


def run_git(args):
    try:
        output = subprocess.check_output(["git"] + args, stderr=subprocess.DEVNULL)
    except (OSError, subprocess.CalledProcessError):
        return []
    return output.decode("utf-8", "replace").splitlines()


def changed_files():
    files = run_git(["diff", "--name-only", "origin/main...HEAD"])
    if files:
        return files
    return run_git(["ls-files"])


def is_text_file(path):
    try:
        with open(path, "rb") as handle:
            chunk = handle.read(4096)
    except IOError:
        return False
    return b"\0" not in chunk


def scan_unicode(paths):
    print("== weird unicode/control chars ==")
    findings = []
    for path in paths:
        if not os.path.isfile(path) or not is_text_file(path):
            continue
        with open(path, "r", encoding="utf-8", errors="replace") as handle:
            for line_number, line in enumerate(handle, 1):
                if BIDI_OR_CONTROL.search(line):
                    findings.append("{}:{}".format(path, line_number))
    if findings:
        for finding in findings:
            print(finding)
        return 1
    print("OK")
    return 0


def scan_generated(paths):
    print("== unexpected binary/generated/build changes ==")
    findings = [path for path in paths if GENERATED_OR_BINARY.search(path.replace(os.sep, "/"))]
    if findings:
        for finding in findings:
            print(finding)
        print("Manual review required")
        return 1
    print("OK")
    return 0


def scan_eval_inventory():
    print("== eval inventory ==")
    matches = []
    for root, _, files in os.walk("qtop_py"):
        for filename in files:
            if not filename.endswith(".py"):
                continue
            path = os.path.join(root, filename)
            with open(path, "r", encoding="utf-8", errors="replace") as handle:
                for line_number, line in enumerate(handle, 1):
                    if "eval(" in line and not line.lstrip().startswith("#"):
                        matches.append("{}:{}".format(path, line_number))
    if matches:
        for match in matches:
            print(match)
        if os.environ.get("FORTIFY_FAIL_ON_EVAL") == "1":
            return 1
        print("Existing eval usage is reported but not failed by default.")
    else:
        print("OK")
    return 0


def main():
    paths = changed_files()
    status = 0
    status |= scan_unicode(paths)
    status |= scan_generated(paths)
    status |= scan_eval_inventory()
    return status


if __name__ == "__main__":
    sys.exit(main())
