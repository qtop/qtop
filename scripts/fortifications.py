#!/usr/bin/env python
"""Check PR diff health before expensive CI work runs."""

import argparse
from pathlib import Path
import re
import subprocess
import sys


ROOT = Path(__file__).resolve().parents[1]
CONTROL_RE = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f\u202a-\u202e\u2066-\u2069]")
GENERATED_RE = re.compile(
    r"(^|/)(m4|autogen|configure|Makefile\.in|cmake|tests?/files|fixtures?)/|"
    r"\.(xz|lzma|gz|bin|dat)$",
    re.IGNORECASE,
)


def run_git(*args, check=True):
    proc = subprocess.run(
        ["git", *args],
        cwd=str(ROOT),
        universal_newlines=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    if check and proc.returncode != 0:
        print(proc.stdout, end="")
        print(proc.stderr, end="", file=sys.stderr)
        raise SystemExit(proc.returncode)
    return proc


def diff_range(base_ref):
    proc = run_git("rev-parse", "--verify", base_ref, check=False)
    if proc.returncode == 0:
        return f"{base_ref}...HEAD"
    return "HEAD"


def changed_files(base_ref):
    proc = run_git("diff", "--name-only", diff_range(base_ref), check=False)
    if proc.returncode != 0:
        return []
    return [line.strip() for line in proc.stdout.splitlines() if line.strip()]


def added_lines(base_ref):
    proc = run_git("diff", "-U0", diff_range(base_ref), check=False)
    if proc.returncode != 0:
        return []
    return [line[1:] for line in proc.stdout.splitlines() if line.startswith("+") and not line.startswith("+++")]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-ref", default="origin/main")
    args = parser.parse_args()

    failures = []

    print("== git diff --check ==")
    diff_check = run_git("diff", "--check", diff_range(args.base_ref), check=False)
    if diff_check.stdout:
        print(diff_check.stdout, end="")
    if diff_check.stderr:
        print(diff_check.stderr, end="", file=sys.stderr)
    if diff_check.returncode != 0:
        failures.append("git diff --check failed")

    files = changed_files(args.base_ref)
    print("== changed files ==")
    for path in files:
        print(path)

    generated = [path for path in files if GENERATED_RE.search(path.replace("\\", "/"))]
    print("== generated/binary-looking changes ==")
    if generated:
        for path in generated:
            print(path)
        failures.append("manual review required for generated/binary-looking changes")
    else:
        print("none")

    lines = added_lines(args.base_ref)
    print("== control/bidi chars in added lines ==")
    control_lines = [line for line in lines if CONTROL_RE.search(line)]
    if control_lines:
        for line in control_lines[:20]:
            print(line.encode("unicode_escape").decode("ascii"))
        failures.append("control/bidi characters found in added lines")
    else:
        print("none")

    print("== new eval usage ==")
    new_evals = [line for line in lines if "eval(" in line]
    if new_evals:
        for line in new_evals:
            print(line)
        failures.append("new eval usage found")
    else:
        print("none")

    if failures:
        print("\nFortification failures:")
        for failure in failures:
            print(f"- {failure}")
        return 1

    print("OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
