#!/usr/bin/env python3
"""Small repository health checks for CI and local review."""

import argparse
import ast
import os
import sys


def iter_python_files(paths):
    for path in paths:
        if os.path.isfile(path) and path.endswith(".py"):
            yield path
            continue
        for root, dirnames, filenames in os.walk(path):
            dirnames[:] = [dirname for dirname in dirnames if dirname not in {".git", ".tox", ".venv", "__pycache__"}]
            for filename in filenames:
                if filename.endswith(".py"):
                    yield os.path.join(root, filename)


def find_eval_calls(path):
    with open(path, "r", encoding="utf-8") as handle:
        tree = ast.parse(handle.read(), filename=path)
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == "eval":
            yield node.lineno


def main():
    parser = argparse.ArgumentParser(description="Run qtop fortification checks.")
    parser.add_argument("paths", nargs="+", help="Files or directories to inspect")
    args = parser.parse_args()

    findings = []
    for path in iter_python_files(args.paths):
        for lineno in find_eval_calls(path):
            findings.append("%s:%s runtime eval() call" % (path, lineno))

    if findings:
        for finding in findings:
            print(finding, file=sys.stderr)
        return 1

    print("fortifications ok: no runtime eval() calls found")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
