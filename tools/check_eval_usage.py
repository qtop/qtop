#!/usr/bin/env python3
##
## qtop is a tool to monitor queuing systems - https://github.com/qtop/qtop
##
## Copyright (c) 2026 Daniel Cabezas
##
## SPDX-License-Identifier: MIT
##

"""Fail when Python source contains runtime eval() calls."""

import ast
import os


ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def iter_python_files():
    for base, dirs, files in os.walk(ROOT):
        dirs[:] = [name for name in dirs if name not in {".git", ".pytest_cache", "__pycache__"}]
        for name in files:
            if name.endswith(".py"):
                yield os.path.join(base, name)


def find_eval_calls(path):
    with open(path, "r", encoding="utf-8") as source:
        tree = ast.parse(source.read(), filename=path)
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == "eval":
            yield node.lineno


def main():
    failures = []
    for path in iter_python_files():
        for lineno in find_eval_calls(path):
            failures.append((path, lineno))

    if failures:
        for path, lineno in failures:
            print("%s:%s: runtime eval() is not allowed" % (os.path.relpath(path, ROOT), lineno))
        return 1
    print("No runtime eval() calls found.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
