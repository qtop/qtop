#!/usr/bin/env python3
"""Run qtop against bundled scheduler samples and compare reference output."""

from __future__ import annotations

import argparse
import difflib
import os
import subprocess
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
CONTRIB = REPO_ROOT / "qtop_py" / "contrib"
IGNORED_LINE_MARKERS = (
    "WORKDIR",
    "Please try it with watch",
    "Log file created in",
)

CASES = (
    (
        "sge",
        "sger_dvv_out.ref",
        ("-s", str(CONTRIB), "-c", "ON", "-Fadvv", "-b", "sge"),
    ),
    (
        "oar",
        "oar1_dvv_out.ref",
        ("-c", "ON", "-s", str(CONTRIB), "-FAardvvv", "-b", "oar"),
    ),
    (
        "pbs",
        "pbs_dvv_out.ref",
        ("-c", "ON", "-s", str(CONTRIB), "-raF", "-b", "pbs"),
    ),
)


def normalize_output(output: str) -> list[str]:
    lines = []
    for line in output.splitlines():
        if any(marker in line for marker in IGNORED_LINE_MARKERS):
            continue
        lines.append(line.rstrip())
    return lines


def run_case(name: str, reference_name: str, qtop_args: tuple[str, ...]) -> str | None:
    command = [sys.executable, "-m", "qtop_py.cli", *qtop_args]
    env = os.environ.copy()
    env["PYTHONPATH"] = str(REPO_ROOT)

    result = subprocess.run(
        command,
        cwd=REPO_ROOT,
        env=env,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    if result.returncode != 0:
        return f"{name}: qtop exited with {result.returncode}\n{result.stderr}"

    expected = normalize_output((CONTRIB / reference_name).read_text(encoding="utf-8"))
    actual = normalize_output(result.stdout)
    if expected == actual:
        return None

    diff = "\n".join(
        difflib.unified_diff(
            expected,
            actual,
            fromfile=f"{name}.expected",
            tofile=f"{name}.actual",
            lineterm="",
        )
    )
    return f"{name}: output differs from {reference_name}\n{diff}"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--max-failures",
        type=int,
        default=0,
        help="maximum allowed sample failures before the gate exits non-zero",
    )
    args = parser.parse_args()

    failures = []
    for case in CASES:
        error = run_case(*case)
        if error:
            failures.append(error)
            print(error, file=sys.stderr)

    if failures:
        print(f"{len(failures)} sample gate failure(s)", file=sys.stderr)
    else:
        print("sample gate passed")

    return 1 if len(failures) > args.max_failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
