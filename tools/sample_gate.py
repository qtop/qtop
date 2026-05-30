#!/usr/bin/env python3
"""Shared sample-validation entry point for PBS, SGE, and SLURM.

Called by both GitHub Actions and GitLab CI to prevent drift between
platforms.  Exit code reflects --max-failures threshold.

Sample sources and limits documented in docs/ci-sample-gate.md.
"""

import argparse
import json
import subprocess
import sys
from pathlib import Path


def run_pbs_gate(samples_dir: Path, limit: int, output_dir: Path) -> dict:
    validate = Path(__file__).resolve().parent / "validate_pbs_samples.py"
    proc = subprocess.run(
        [sys.executable, str(validate), str(samples_dir),
         "--limit", str(limit), "--output", str(output_dir)],
        capture_output=True, text=True, timeout=300,
    )
    return {
        "scheduler": "pbs",
        "returncode": proc.returncode,
        "stdout": proc.stdout.strip(),
        "stderr_tail": proc.stderr.strip().splitlines()[-10:],
    }


def run_slurm_gate(samples_dir: Path, output_dir: Path) -> dict:
    validate = Path(__file__).resolve().parent / "validate_slurm_samples.py"
    proc = subprocess.run(
        [sys.executable, str(validate), str(samples_dir),
         "--output", str(output_dir)],
        capture_output=True, text=True, timeout=120,
    )
    return {
        "scheduler": "slurm",
        "returncode": proc.returncode,
        "stdout": proc.stdout.strip(),
        "stderr_tail": proc.stderr.strip().splitlines()[-10:],
    }


def run_sge_gate(samples_dir: Path, output_dir: Path) -> dict:
    output_dir.mkdir(parents=True, exist_ok=True)
    proc = subprocess.run(
        [sys.executable, "-m", "pytest", "tests/plugins/test_sge.py", "-v"],
        capture_output=True, text=True, timeout=120,
    )
    return {
        "scheduler": "sge",
        "returncode": proc.returncode,
        "stdout": proc.stdout.strip().splitlines()[-20:],
        "stderr_tail": proc.stderr.strip().splitlines()[-5:] if proc.stderr else [],
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Shared sample-validation gate")
    parser.add_argument("--pbs-samples", type=Path, default=Path("../qtop-test-repo/qtop5/results"))
    parser.add_argument("--pbs-limit", type=int, default=100)
    parser.add_argument("--pbs-output", type=Path, default=Path("/tmp/qtop-pbs-rendered"))
    parser.add_argument("--slurm-samples", type=Path, default=Path("tests/plugins/slurm_samples"))
    parser.add_argument("--slurm-output", type=Path, default=Path("/tmp/qtop-slurm-rendered"))
    parser.add_argument("--sge-samples", type=Path, default=Path("tests/plugins/sge_samples"))
    parser.add_argument("--sge-output", type=Path, default=Path("/tmp/qtop-sge-rendered"))
    parser.add_argument("--max-failures", type=int, default=0,
                        help="Maximum allowed scheduler gate failures (default: 0)")
    args = parser.parse_args()

    results = []
    failures = 0

    print("=== Sample Gate ===")

    if args.pbs_samples.is_dir():
        print(f"\n--- PBS samples ({args.pbs_samples}) ---")
        result = run_pbs_gate(args.pbs_samples, args.pbs_limit, args.pbs_output)
        results.append(result)
        if result["returncode"] != 0:
            failures += 1
            print(f"  FAILED (exit {result['returncode']})")
        else:
            print(f"  OK: {result['stdout']}")
    else:
        print(f"\n--- PBS samples skipped (not found: {args.pbs_samples}) ---")

    if args.slurm_samples.is_dir():
        print(f"\n--- SLURM samples ({args.slurm_samples}) ---")
        result = run_slurm_gate(args.slurm_samples, args.slurm_output)
        results.append(result)
        if result["returncode"] != 0:
            failures += 1
            print(f"  FAILED (exit {result['returncode']})")
        else:
            print(f"  OK: {result['stdout']}")
    else:
        print(f"\n--- SLURM samples skipped (not found: {args.slurm_samples}) ---")

    if args.sge_samples.is_dir():
        print(f"\n--- SGE samples ({args.sge_samples}) ---")
        result = run_sge_gate(args.sge_samples, args.sge_output)
        results.append(result)
        if result["returncode"] != 0:
            failures += 1
            print(f"  FAILED (exit {result['returncode']})")
        else:
            print(f"  OK")
    else:
        print(f"\n--- SGE samples skipped (not found: {args.sge_samples}) ---")

    manifest = {"results": results, "failures": failures, "max_failures": args.max_failures}
    manifest_path = Path("/tmp") / "sample-gate-manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2))
    print(f"\nManifest: {manifest_path}")
    print(f"Failures: {failures}/{args.max_failures}")

    if failures > args.max_failures:
        print("FAILED: too many failures")
        return 1
    print("PASSED")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
