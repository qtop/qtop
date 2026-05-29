#!/usr/bin/env python
"""Run qtop against committed scheduler samples and compare stable output."""

import argparse
import difflib
import os
from pathlib import Path
import subprocess
import sys


ROOT = Path(__file__).resolve().parents[1]
CONTRIB = ROOT / "qtop_py" / "contrib"
VOLATILE_MARKERS = (
    "WORKDIR",
    "Please try it with watch",
    "Log file created in",
)

CASES = {
    "sge": {
        "args": ["-s", str(CONTRIB), "-c", "ON", "-Fadvv", "-b", "sge"],
        "reference": CONTRIB / "sger_dvv_out.ref",
        "markers": ("Summary", "Worker Nodes occupancy", "User accounts"),
    },
    "pbs": {
        "args": ["-c", "ON", "-s", str(CONTRIB), "-raF", "-b", "pbs"],
        "reference": CONTRIB / "pbs_dvv_out.ref",
        "markers": ("Summary", "Worker Nodes occupancy", "User accounts"),
    },
    "oar": {
        "args": ["-c", "ON", "-s", str(CONTRIB), "-FAardvvv", "-b", "oar"],
        "reference": CONTRIB / "oar1_dvv_out.ref",
        "markers": ("Summary", "Worker Nodes occupancy", "User accounts"),
    },
}


def prepare_env(artifact_dir):
    env = os.environ.copy()
    env.setdefault("PYTHONUTF8", "1")

    home = artifact_dir / "home"
    qtop_home = home / ".local" / "qtop"
    qtop_home.mkdir(parents=True, exist_ok=True)
    (qtop_home / "getent_passwd.txt").write_text("", encoding="utf-8")
    env["HOME"] = str(home)

    if os.name == "nt":
        shim_dir = artifact_dir / "shims"
        shim_dir.mkdir(parents=True, exist_ok=True)
        cat_py = shim_dir / "cat_shim.py"
        cat_py.write_text(
            "import pathlib, sys\n"
            "for arg in sys.argv[1:]:\n"
            "    if arg.startswith('-'):\n"
            "        continue\n"
            "    path = pathlib.Path(arg)\n"
            "    if path.exists():\n"
            "        sys.stdout.write(path.read_text(encoding='utf-8', errors='replace'))\n",
            encoding="utf-8",
        )
        cat_cmd = shim_dir / "cat.cmd"
        cat_cmd.write_text(f'@echo off\r\n"{sys.executable}" "{cat_py}" %*\r\n', encoding="utf-8")
        env["PATH"] = str(shim_dir) + os.pathsep + env.get("PATH", "")

    return env


def normalize(text):
    lines = []
    for line in text.splitlines():
        if any(marker in line for marker in VOLATILE_MARKERS):
            continue
        lines.append(line.rstrip())
    return lines


def run_case(name, artifact_dir, strict_reference=False):
    case = CASES[name]
    cmd = [sys.executable, "-m", "qtop_py.cli", *case["args"]]
    proc = subprocess.run(
        cmd,
        cwd=str(ROOT),
        env=prepare_env(artifact_dir),
        universal_newlines=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=60,
    )

    expected = normalize(case["reference"].read_text(encoding="utf-8", errors="replace"))
    actual = normalize(proc.stdout)
    diff = list(
        difflib.unified_diff(
            expected,
            actual,
            fromfile=f"{name}.expected",
            tofile=f"{name}.actual",
            lineterm="",
        )
    )

    artifact_dir.mkdir(parents=True, exist_ok=True)
    (artifact_dir / f"{name}.stdout.txt").write_text(proc.stdout, encoding="utf-8", errors="replace")
    (artifact_dir / f"{name}.stderr.txt").write_text(proc.stderr, encoding="utf-8", errors="replace")
    (artifact_dir / f"{name}.diff.txt").write_text("\n".join(diff), encoding="utf-8")

    if proc.returncode != 0:
        return False, f"{name}: command exited {proc.returncode}"
    missing_markers = [marker for marker in case["markers"] if marker not in proc.stdout]
    if missing_markers:
        return False, f"{name}: missing marker(s): {', '.join(missing_markers)}"
    if strict_reference and diff:
        return False, f"{name}: output drifted from {case['reference'].relative_to(ROOT)}"
    if diff:
        return True, f"{name}: OK, reference drift recorded in artifacts"
    return True, f"{name}: OK"


def parse_schedulers(value):
    selected = [item.strip().lower() for item in value.split(",") if item.strip()]
    unknown = sorted(set(selected) - set(CASES))
    if unknown:
        raise SystemExit("Unknown sample scheduler(s): " + ", ".join(unknown))
    return selected


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--schedulers", default="pbs,sge", help="Comma-separated sample cases to run: pbs,sge,oar")
    parser.add_argument("--max-failures", type=int, default=0, help="Number of sample failures tolerated before non-zero exit")
    parser.add_argument("--artifact-dir", default="artifacts/sample-gate", help="Directory for stdout/stderr/diff artifacts")
    parser.add_argument("--strict-reference", action="store_true", help="Fail when rendered output differs from the checked-in reference")
    args = parser.parse_args()

    artifact_dir = ROOT / args.artifact_dir
    failures = []
    for scheduler in parse_schedulers(args.schedulers):
        ok, message = run_case(scheduler, artifact_dir, strict_reference=args.strict_reference)
        print(message)
        if not ok:
            failures.append(message)

    if failures:
        print("\nSample gate failures:")
        for failure in failures:
            print(f"- {failure}")
    return 1 if len(failures) > args.max_failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
