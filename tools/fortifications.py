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
    return subprocess.check_output(["git"] + args, cwd=str(ROOT), stderr=subprocess.STDOUT, universal_newlines=True, encoding="utf-8", errors="replace")


def changed_files(base_ref):
    output = run_git(["diff", "--no-ext-diff", "--name-only", "-z", base_ref, "--"])
    return [name for name in output.split("\0") if name]


def diff_lines(base_ref):
    output = run_git(["-c", "core.quotePath=false", "diff", "--no-ext-diff", "--no-textconv", "--no-color", "--src-prefix=a/", "--dst-prefix=b/", "--unified=0", base_ref, "--"])
    return output.split("\n")


def find_diff_problems(lines):
    problems = []
    filename, number = None, None
    for line in lines:
        if line.startswith("diff --git "):
            filename, number = None, None
            continue
        if line.startswith("+++ ") and number is None:
            filename = line[4:]
            if filename.startswith('"'):
                filename = ast.literal_eval(filename)
            filename = filename[2:] if filename.startswith("b/") else filename
            if filename == "/dev/null":
                filename = None
            continue
        hunk = re.match(r"^@@ -\d+(?:,\d+)? \+(\d+)(?:,\d+)? @@", line)
        if hunk:
            number = int(hunk.group(1))
            continue
        if filename is None or number is None:
            continue
        if line.startswith("+"):
            text = line[1:]
            match = CONTROL_OR_BIDI.search(text)
            if match:
                safe_filename = filename.encode("unicode_escape").decode("ascii")
                problems.append(
                    "%s:%s:%s: control, bidi, or non-ASCII character U+%04X in added line: %s" % (safe_filename, number, match.start() + 1, ord(match.group()), ascii(text))
                )
            number += 1
        elif line.startswith(" "):
            number += 1
    return problems


def iter_python_files():
    for dirpath, dirnames, filenames in os.walk(str(ROOT)):
        dirnames[:] = [name for name in dirnames if name not in SKIP_DIRS]
        for filename in filenames:
            if filename.endswith(".py"):
                yield Path(dirpath) / filename


def find_eval_calls():
    problems = []
    for path in iter_python_files():
        relative = path.relative_to(ROOT).as_posix()
        try:
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=relative)
        except SyntaxError as exc:
            problems.append("%s:%s syntax error while scanning for eval: %s" % (relative, exc.lineno, exc.msg))
            continue
        for node in ast.walk(tree):
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == "eval":
                problems.append("%s:%s eval() call is not allowed" % (relative, getattr(node, "lineno", "?")))
    return problems


def find_star_imports():
    problems = []
    checked_roots = ("qtop_py", "tools")
    for path in iter_python_files():
        relative = path.relative_to(ROOT).as_posix()
        if not relative.startswith(checked_roots):
            continue
        try:
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=relative)
        except SyntaxError as exc:
            problems.append("%s:%s syntax error while scanning imports: %s" % (relative, exc.lineno, exc.msg))
            continue
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom) and any(alias.name == "*" for alias in node.names):
                problems.append("%s:%s wildcard import is not allowed" % (relative, getattr(node, "lineno", "?")))
    return problems


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-ref", default="origin/develop", help="Diff base for changed-file checks")
    return parser.parse_args()


def main():
    args = parse_args()
    problems = []

    try:
        # Compare the branch merge base with tracked files, including local fixes.
        base = run_git(["merge-base", args.base_ref, "HEAD"]).strip()
        files = changed_files(base)
        lines = diff_lines(base)
    except subprocess.CalledProcessError as exc:
        sys.stderr.write(exc.output)
        sys.stderr.write("Unable to compare against %s\n" % args.base_ref)
        return 2

    for filename in files:
        if GENERATED_OR_BINARY.search(filename):
            problems.append("manual review required for generated/binary-looking path: %s" % filename)

    problems.extend(find_diff_problems(lines))

    problems.extend(find_eval_calls())
    problems.extend(find_star_imports())

    if problems:
        for problem in problems:
            print(problem, file=sys.stderr)
        return 1

    print("fortifications: ok")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
