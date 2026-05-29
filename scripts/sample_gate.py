#!/usr/bin/env python
"""Run qtop against committed scheduler samples and compare expected output."""

from __future__ import annotations

import argparse
import os
from pathlib import Path
import re
import subprocess
import sys


ROOT = Path(__file__).resolve().parents[1]
CONTRIB = ROOT / "qtop_py" / "contrib"
ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")

CASES = {
    "sge": {
        "args": ["-s", str(CONTRIB), "-c", "ON", "-Fadvv", "-b", "sge"],
        "checks": (
            "Summary: Total:17 Up:17 Free:4 Nodes",
            "61/408 cores",
            "61+31 jobs",
            "Worker Nodes occupancy",
            "User accounts and pool mappings",
        ),
    },
    "oar": {
        "args": ["-s", str(CONTRIB), "-c", "ON", "-Fardvvv", "-b", "oar"],
        "checks": (
            "Summary: Total:183 Up:172 Free:167 Nodes",
            "1349/2520 cores",
            "0+0 jobs",
            "Worker Nodes occupancy",
            "User accounts and pool mappings",
        ),
    },
    "pbs": {
        "args": ["-s", str(CONTRIB), "-c", "ON", "-raF", "-b", "pbs"],
        "checks": (
            "Summary: Total:829 Up:819 Free:91 Nodes",
            "7629/7872 cores",
            "7590+3365 jobs",
            "Worker Nodes occupancy",
            "User accounts and pool mappings",
        ),
    },
}


def normalize(text: str) -> str:
    text = ANSI_RE.sub("", text)
    return re.sub(r"\s+", " ", text)


def run_case(name: str, artifact_dir: Path) -> tuple[bool, str]:
    case = CASES[name]
    env = os.environ.copy()
    env.setdefault("QTOP_COLOR", "ON")
    result = subprocess.run(
        [sys.executable, "-m", "qtop_py.cli", *case["args"]],
        cwd=str(ROOT),
        env=env,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    artifact_dir.mkdir(parents=True, exist_ok=True)
    (artifact_dir / f"{name}.out").write_text(result.stdout, encoding="utf-8")
    (artifact_dir / f"{name}.err").write_text(result.stderr, encoding="utf-8")

    if result.returncode != 0:
        return False, f"{name}: qtop exited with {result.returncode}; see {artifact_dir / (name + '.err')}"

    actual = normalize(result.stdout)
    missing = [check for check in case["checks"] if check not in actual]
    if missing:
        (artifact_dir / f"{name}.missing").write_text("\n".join(missing), encoding="utf-8")
        return False, f"{name}: missing expected markers; see {artifact_dir / (name + '.missing')}"

    return True, f"{name}: ok"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--schedulers", default="pbs,oar,sge", help="Comma-separated schedulers to validate")
    parser.add_argument("--max-failures", type=int, default=0, help="Maximum failures allowed before exiting non-zero")
    parser.add_argument("--artifact-dir", default="artifacts/sample-gate", help="Directory for captured output")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    names = [name.strip() for name in args.schedulers.split(",") if name.strip()]
    unknown = sorted(set(names) - set(CASES))
    if unknown:
        raise SystemExit(f"Unknown scheduler(s): {', '.join(unknown)}")

    failures = 0
    for name in names:
        ok, message = run_case(name, ROOT / args.artifact_dir)
        print(message)
        failures += 0 if ok else 1

    return 0 if failures <= args.max_failures else 1


if __name__ == "__main__":
    raise SystemExit(main())
