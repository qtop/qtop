#!/usr/bin/env python3
"""Lightweight PR diff checks for qtop contributors.

The checks intentionally use only the Python standard library so they can run
in early cluster-like environments where optional developer tools may be absent.
"""

import argparse
import re
import subprocess
import sys
from typing import Iterable, List, NamedTuple, Optional, Sequence, Set


BIDI_CODEPOINTS = {
    0x202A,
    0x202B,
    0x202C,
    0x202D,
    0x202E,
    0x2066,
    0x2067,
    0x2068,
    0x2069,
}

ARTIFACT_RE = re.compile(
    r"(^|/)(build|dist|\.eggs|qtop\.egg-info|htmlcov|\.pytest_cache|__pycache__)(/|$)"
    r"|(^|/)(m4|autogen|configure|Makefile\.in|cmake|tests?/files|fixtures?)/"
    r"|\.(xz|lzma|gz|bin|dat|pyc|pyo|so|whl|egg)$",
    re.IGNORECASE,
)


class SuspiciousText(NamedTuple):
    line_no: int
    reason: str
    text: str


def run_git(args: Sequence[str], check: bool = True) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["git", *args],
        check=check,
        universal_newlines=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )


def ref_exists(ref: str) -> bool:
    return run_git(["rev-parse", "--verify", "--quiet", ref], check=False).returncode == 0


def diff_args(base: Optional[str]) -> List[str]:
    if base and ref_exists(base):
        return [f"{base}...HEAD"]
    return []


def unique_lines(lines: Iterable[str]) -> List[str]:
    seen: Set[str] = set()
    unique: List[str] = []
    for line in lines:
        if line and line not in seen:
            seen.add(line)
            unique.append(line)
    return unique


def changed_files(base: Optional[str]) -> List[str]:
    outputs = [
        run_git(["diff", "--name-only", "--diff-filter=ACMRT", *diff_args(base)]).stdout,
        run_git(["diff", "--name-only", "--diff-filter=ACMRT"]).stdout,
        run_git(["diff", "--cached", "--name-only", "--diff-filter=ACMRT"]).stdout,
        run_git(["ls-files", "--others", "--exclude-standard"]).stdout,
    ]
    return unique_lines(line for output in outputs for line in output.splitlines())


def artifact_paths(paths: Iterable[str]) -> List[str]:
    return [path for path in paths if ARTIFACT_RE.search(path)]


def classify_suspicious_char(ch: str, allow_non_ascii: bool) -> Optional[str]:
    codepoint = ord(ch)
    if codepoint in BIDI_CODEPOINTS:
        return "bidirectional unicode control"
    if ch not in "\t\n\r" and codepoint < 32:
        return "control character"
    if not allow_non_ascii and codepoint > 126:
        return "non-ascii character"
    return None


def suspicious_added_lines(diff_text: str, allow_non_ascii: bool = False) -> List[SuspiciousText]:
    findings: List[SuspiciousText] = []
    for line_no, line in enumerate(diff_text.splitlines(), start=1):
        if not line.startswith("+") or line.startswith("+++"):
            continue
        text = line[1:]
        for ch in text:
            reason = classify_suspicious_char(ch, allow_non_ascii)
            if reason:
                findings.append(SuspiciousText(line_no, reason, text))
                break
    return findings


def branch_diff(base: Optional[str]) -> str:
    outputs = [
        run_git(["diff", "--unified=0", "--no-ext-diff", *diff_args(base)]).stdout,
        run_git(["diff", "--unified=0", "--no-ext-diff"]).stdout,
        run_git(["diff", "--cached", "--unified=0", "--no-ext-diff"]).stdout,
    ]
    return "\n".join(output for output in outputs if output)


def diff_check(base: Optional[str]) -> str:
    outputs: List[str] = []
    for args in (["diff", "--check", *diff_args(base)], ["diff", "--check"], ["diff", "--cached", "--check"]):
        result = run_git(args, check=False)
        if result.returncode:
            outputs.append(result.stdout.rstrip())
    return "\n".join(output for output in outputs if output)


def parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base", default="origin/develop", help="base ref for branch diff checks")
    parser.add_argument(
        "--allow-non-ascii",
        action="store_true",
        help="allow non-ASCII additions while still checking control and bidi characters",
    )
    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv or sys.argv[1:])
    failures: List[str] = []

    whitespace_errors = diff_check(args.base)
    if whitespace_errors:
        failures.append("git diff --check reported whitespace errors:\n" + whitespace_errors.rstrip())

    artifacts = artifact_paths(changed_files(args.base))
    if artifacts:
        failures.append("unexpected generated or binary-looking paths:\n" + "\n".join(f"  {path}" for path in artifacts))

    suspicious = suspicious_added_lines(branch_diff(args.base), allow_non_ascii=args.allow_non_ascii)
    if suspicious:
        rendered = "\n".join(f"  diff line {item.line_no}: {item.reason}: {item.text!r}" for item in suspicious[:20])
        extra = "" if len(suspicious) <= 20 else f"\n  ... {len(suspicious) - 20} more"
        failures.append("suspicious text in added lines:\n" + rendered + extra)

    if failures:
        print("\n\n".join(failures), file=sys.stderr)
        return 1

    print("OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
