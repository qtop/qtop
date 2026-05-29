#!/usr/bin/env python
"""Small diff-health checks for reviewable CI changes."""

from __future__ import annotations

import argparse
from pathlib import Path
import re
import subprocess
import sys


CONTROL_OR_BIDI = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f\u202a-\u202e\u2066-\u2069]")
GENERATED_OR_BINARY = re.compile(
    r"(^|/)(m4|autogen|configure|Makefile\.in|cmake|tests?/files|fixtures?)/|"
    r"\.(xz|lzma|gz|bin|dat|png|jpg|jpeg|gif|webp)$",
    re.IGNORECASE,
)
EVAL_ADDITION = re.compile(r"^\+.*\beval\s*\(")


def git(args: list[str]) -> str:
    return subprocess.check_output(["git", *args], text=True, stderr=subprocess.STDOUT)


def changed_files(base_ref: str) -> list[str]:
    return [line for line in git(["diff", "--name-only", f"{base_ref}...HEAD"]).splitlines() if line]


def diff_lines(base_ref: str) -> list[str]:
    return git(["diff", "--unified=0", f"{base_ref}...HEAD"]).splitlines()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-ref", default="origin/main")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    problems: list[str] = []

    for filename in changed_files(args.base_ref):
        if GENERATED_OR_BINARY.search(filename):
            problems.append(f"manual review required for generated/binary-looking path: {filename}")
        path = Path(filename)
        if path.exists() and path.is_file():
            data = path.read_text(encoding="utf-8", errors="ignore")
            if CONTROL_OR_BIDI.search(data):
                problems.append(f"control or bidi character found in: {filename}")

    for line in diff_lines(args.base_ref):
        if EVAL_ADDITION.search(line):
            problems.append("new eval() usage found in diff")
            break

    if problems:
        for problem in problems:
            print(problem, file=sys.stderr)
        return 1

    print("fortifications: ok")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

