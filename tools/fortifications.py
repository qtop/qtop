#!/usr/bin/env python3
"""Inspect codebase healthiness — unicode chars, binary changes, etc."""

import os
import re
import subprocess
import sys


def check_unicode_control_chars(target: str = "HEAD") -> int:
    """Check for weird unicode/control characters in the diff."""
    errors = 0
    try:
        result = subprocess.run(
            ["git", "diff", "-U0", f"origin/main...{target}"],
            capture_output=True, text=True, timeout=30,
        )
        for i, line in enumerate(result.stdout.splitlines(), 1):
            cleaned = line.rstrip("\n")
            for ch in cleaned:
                cp = ord(ch)
                if cp < 0x20 and cp not in (0x09, 0x0A, 0x0D):
                    print(f"  [UNICODE] line {i}: control char U+{cp:04X}")
                    errors += 1
                    break
                if 0x202A <= cp <= 0x202E or 0x2066 <= cp <= 0x2069:
                    print(f"  [UNICODE] line {i}: bidi char U+{cp:04X}")
                    errors += 1
                    break
    except subprocess.TimeoutExpired:
        print("  [WARN] git diff timed out, skipping unicode check")
    except FileNotFoundError:
        print("  [WARN] git not available, skipping unicode check")
    return errors


def check_binary_changes(target: str = "HEAD") -> int:
    """Check for unexpected binary/generated/build changes in the diff."""
    errors = 0
    binary_pattern = re.compile(
        r'(^|/)('
        r'm4|autogen|configure|Makefile\.in|cmake'
        r'|tests?/files|fixtures?'
        r')/'
        r'|\.(xz|lzma|gz|bin|dat)$',
        re.I,
    )
    try:
        result = subprocess.run(
            ["git", "diff", "--name-only", f"origin/main...{target}"],
            capture_output=True, text=True, timeout=30,
        )
        for line in result.stdout.splitlines():
            if binary_pattern.search(line):
                print(f"  [BINARY] unexpected file: {line}")
                errors += 1
    except subprocess.TimeoutExpired:
        print("  [WARN] git diff timed out, skipping binary check")
    except FileNotFoundError:
        print("  [WARN] git not available, skipping binary check")
    return errors


def check_eval_calls() -> int:
    """Scan for remaining dangerous eval() calls in source."""
    errors = 0
    source_dirs = ["qtop_py"]
    for source_dir in source_dirs:
        for root, _dirs, files in os.walk(source_dir):
            for fname in files:
                if not fname.endswith(".py"):
                    continue
                path = os.path.join(root, fname)
                with open(path, encoding="utf-8", errors="replace") as fh:
                    for i, line in enumerate(fh, 1):
                        stripped = line.split("#")[0]
                        if "eval(" in stripped and "literal_eval" not in stripped:
                            print(f"  [EVAL] {path}:{i}: {line.rstrip()[:100]}")
                            errors += 1
    return errors


def check_trailing_whitespace() -> int:
    """Check for trailing whitespace in Python files."""
    errors = 0
    source_dirs = ["qtop_py", "tests", "tools"]
    for source_dir in source_dirs:
        for root, _dirs, files in os.walk(source_dir):
            for fname in files:
                if not fname.endswith(".py"):
                    continue
                path = os.path.join(root, fname)
                with open(path, encoding="utf-8", errors="replace") as fh:
                    for i, line in enumerate(fh, 1):
                        if line.rstrip("\n").endswith((" ", "\t")):
                            print(f"  [TRAILING] {path}:{i}")
                            errors += 1
                            break
    return errors


def main() -> int:
    errors = 0
    print("=== Fortifications Check ===")

    print("\n--- Unicode/Control Characters ---")
    errors += check_unicode_control_chars()

    print("\n--- Binary/Generated Files ---")
    errors += check_binary_changes()

    print("\n--- Eval() Calls ---")
    errors += check_eval_calls()

    print("\n--- Trailing Whitespace ---")
    errors += check_trailing_whitespace()

    report_path = "fortify-report.txt"
    with open(report_path, "w") as f:
        f.write(f"Fortifications Check Report\n")
        f.write(f"Errors found: {errors}\n")

    print(f"\nSummary: {errors} issue(s) found")
    return 1 if errors > 0 else 0


if __name__ == "__main__":
    raise SystemExit(main())
