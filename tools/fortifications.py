#!/usr/bin/env python3
##
## qtop is a tool to monitor queuing systems - https://github.com/qtop/qtop
##
## Copyright (c) 2026 Jacob Hatchett
##
## SPDX-License-Identifier: MIT
##

"""Diff and source-health checks used by local and CI validation."""

import argparse
import ast
import os
import re
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
CONTROL_OR_BIDI = re.compile(r"[^\x09\x0a\x0d\x20-\x7e]|\u202a|\u202b|\u202c|\u202d|\u202e|\u2066|\u2067|\u2068|\u2069")
GENERATED_OR_BINARY = re.compile(
    r"(^|/)(m4|autogen|configure|Makefile\.in|cmake|tests?/files|fixtures?)/|\.(xz|lzma|gz|bin|dat|png|jpg|jpeg|gif|webp)$",
    re.IGNORECASE,
)
SKIP_DIRS = set([".git", ".tox", ".venv", "venv", "__pycache__", "artifacts", "build", "dist", "qtop.egg-info"])


def run_git(args):
    return subprocess.check_output(["git"] + args, cwd=str(ROOT), stderr=subprocess.STDOUT, universal_newlines=True)


def changed_files(base_ref):
    output = run_git(["diff", "--name-only", "%s...HEAD" % base_ref])
    return [line for line in output.splitlines() if line]


def diff_lines(base_ref):
    output = run_git(["diff", "--unified=0", "%s...HEAD" % base_ref])
    return output.splitlines()


def iter_python_files():
    for dirpath, dirnames, filenames in os.walk(str(ROOT)):
        dirnames[:] = [name for name in dirnames if name not in SKIP_DIRS]
        for filename in filenames:
            if filename.endswith(".py"):
                yield Path(dirpath) / filename


def find_eval_calls():
    problems = []
    for path in iter_python_files():
        relative = str(path.relative_to(ROOT))
        try:
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=relative)
        except SyntaxError as exc:
            problems.append("%s:%s syntax error while scanning for eval: %s" % (relative, exc.lineno, exc.msg))
            continue
        for node in ast.walk(tree):
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == "eval":
                problems.append("%s:%s eval() call is not allowed" % (relative, getattr(node, "lineno", "?")))
    return problems


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-ref", default="origin/develop", help="Diff base for changed-file checks")
    return parser.parse_args()


def main():
    args = parse_args()
    problems = []

    try:
        files = changed_files(args.base_ref)
        lines = diff_lines(args.base_ref)
    except subprocess.CalledProcessError as exc:
        sys.stderr.write(exc.output)
        sys.stderr.write("Unable to compare against %s\n" % args.base_ref)
        return 2

    for filename in files:
        if GENERATED_OR_BINARY.search(filename):
            problems.append("manual review required for generated/binary-looking path: %s" % filename)

    for line in lines:
        if not line.startswith("+") or line.startswith("+++"):
            continue
        if CONTROL_OR_BIDI.search(line[1:]):
            problems.append("control, bidi, or non-ASCII character found in an added diff line")
            break

    problems.extend(find_eval_calls())

    if problems:
        for problem in problems:
            print(problem, file=sys.stderr)
        return 1

    print("fortifications: ok")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
