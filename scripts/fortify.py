#!/usr/bin/env python3
"""Small repository health checks used by CI."""

import ast
import subprocess
import sys
from pathlib import Path


BIDI_MARKERS = ("\u202a", "\u202b", "\u202c", "\u202d", "\u202e", "\u2066", "\u2067", "\u2068", "\u2069")
SCAN_SUFFIXES = (".py", ".sh", ".yml", ".yaml", "Makefile")


def tracked_files(repo_root):
    result = subprocess.run(
        ["git", "ls-files"],
        cwd=str(repo_root),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        universal_newlines=True,
    )
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip())
    return [repo_root / line for line in result.stdout.splitlines()]


def is_scanned(path):
    if path.name == "Makefile":
        return True
    return path.suffix in SCAN_SUFFIXES


def check_bidi(files):
    failures = []
    for path in files:
        if not is_scanned(path):
            continue
        text = path.read_text(encoding="utf-8", errors="ignore")
        for line_no, line in enumerate(text.splitlines(), 1):
            if any(marker in line for marker in BIDI_MARKERS):
                failures.append("%s:%s contains bidirectional text marker" % (path, line_no))
    return failures


def check_eval_calls(files):
    failures = []
    for path in files:
        if path.suffix != ".py":
            continue
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == "eval":
                failures.append("%s:%s uses an eval call" % (path, node.lineno))
    return failures


def check_compiled_artifacts(files):
    return ["tracked compiled artifact: %s" % path for path in files if path.suffix == ".pyc" or "__pycache__" in path.parts]


def main():
    repo_root = Path(__file__).resolve().parents[1]
    files = tracked_files(repo_root)
    failures = []
    failures.extend(check_bidi(files))
    failures.extend(check_eval_calls(files))
    failures.extend(check_compiled_artifacts(files))

    if failures:
        for failure in failures:
            print(failure)
        return 1

    print("fortify checks passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
