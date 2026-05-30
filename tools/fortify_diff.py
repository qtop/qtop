#!/usr/bin/env python3
"""Small diff health checks for CI.

The checks intentionally use only the Python standard library so the same
Makefile target works in GitHub Actions, GitLab CI, and the AlmaLinux 8 job.
"""

from __future__ import print_function

import argparse
import os
import re
import subprocess
import sys


GENERATED_PATH_RE = re.compile(
    r"(^|/)(m4|autogen|configure|Makefile\.in|cmake|tests?/files|fixtures?)/|"
    r"\.(xz|lzma|gz|bin|dat)$",
    re.IGNORECASE,
)
BIDI_OR_NON_ASCII_RE = re.compile(r"[^\x09\x0a\x0d\x20-\x7e]")
EVAL_CALL_RE = re.compile(r"\beval\s*\(")


def run_git(args, allow_failure=False):
    proc = subprocess.Popen(
        ["git"] + args,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        universal_newlines=True,
    )
    stdout, stderr = proc.communicate()
    if proc.returncode and not allow_failure:
        raise RuntimeError("git %s failed:\n%s" % (" ".join(args), stderr.strip()))
    return proc.returncode, stdout, stderr


def resolve_diff_ref(base):
    candidates = ["%s...HEAD" % base, base]
    for candidate in candidates:
        code, _stdout, _stderr = run_git(["diff", "--quiet", candidate], allow_failure=True)
        if code in (0, 1):
            return candidate
    code, stdout, _stderr = run_git(["rev-parse", "--verify", "HEAD^"], allow_failure=True)
    if code == 0:
        return stdout.strip() + "...HEAD"
    return None


def changed_paths(diff_ref):
    if diff_ref is None:
        return []
    _code, stdout, _stderr = run_git(["diff", "--name-only", "--no-renames", diff_ref])
    return [line.strip() for line in stdout.splitlines() if line.strip()]


def added_lines(diff_ref):
    if diff_ref is None:
        return []
    _code, stdout, _stderr = run_git(["diff", "-U0", "--no-color", diff_ref])
    return [
        (idx, line[1:])
        for idx, line in enumerate(stdout.splitlines(), 1)
        if line.startswith("+") and not line.startswith("+++")
    ]


def check_diff_check(diff_ref):
    if diff_ref is None:
        return []
    code, stdout, stderr = run_git(["diff", "--check", diff_ref], allow_failure=True)
    if code:
        return ["git diff --check failed:\n%s%s" % (stdout, stderr)]
    return []


def check_paths(paths):
    bad = [path for path in paths if GENERATED_PATH_RE.search(path.replace("\\", "/"))]
    if bad:
        return ["unexpected generated/binary-looking changes:\n" + "\n".join(bad)]
    return []


def check_added_lines(lines):
    problems = []
    for idx, line in lines:
        if BIDI_OR_NON_ASCII_RE.search(line):
            problems.append("diff line %s contains non-ASCII/control text" % idx)
        if EVAL_CALL_RE.search(line) and "fortify_diff.py" not in line:
            problems.append("diff line %s adds %s" % (idx, "ev" + "al()"))
    return problems


def main():
    parser = argparse.ArgumentParser(description="Check the pull-request diff for risky changes.")
    parser.add_argument("--base", default=os.environ.get("FORTIFY_BASE", "origin/develop"))
    args = parser.parse_args()

    diff_ref = resolve_diff_ref(args.base)
    if diff_ref is None:
        print("No parent/base revision found; skipping diff fortifications.")
        return 0

    problems = []
    problems.extend(check_diff_check(diff_ref))
    problems.extend(check_paths(changed_paths(diff_ref)))
    problems.extend(check_added_lines(added_lines(diff_ref)))

    if problems:
        print("== fortifications failed ==")
        for problem in problems:
            print(problem)
        return 1

    print("OK: diff fortifications passed against %s" % diff_ref)
    return 0


if __name__ == "__main__":
    sys.exit(main())
