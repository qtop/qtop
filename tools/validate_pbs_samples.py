#!/usr/bin/env python3
"""Run qtop against archived PBS samples and save rendered output.

Usage:
    python tools/validate_pbs_samples.py /path/to/qtop-test-repo/qtop5/results --limit 100 --output /tmp/qtop-pbs-rendered
    python tools/validate_pbs_samples.py /path/to/qtop-test-repo/qtop5/results --limit 10 --max-failures 50 --timeout 20
"""

import argparse
import json
import os
import signal
import subprocess
from pathlib import Path


GOLDEN_PBS_SAMPLES = (
    "gef_yBVifVBTyE44AKnehzSrvA",
    "gef_y2pQK8d9fstnQElgx8wuCw",
    "gef_xwILYNMpoabX4PDGYXIZAA",
    "gef_wd3MkxlAScZhd6-vEQakrg",
    "gef_kbgwjKhQ6rWy2ZFn_JkHgA",
    "gef_chUMytd1bwcB5Cbc59FHRA",
    "gef_i2OQxcmsYH3GAQnonKVqoA",
    "gef_nYSEKsd7-JpjqvOfI8UyNg",
    "gef_Ab0XLTIet2_e9SoSN-TV1g",
    "gef_BiUgx8_5M6to8nP2BPBcfg",
)


def render_sample(sample_dir, output_dir, save_output=True, timeout=8):
    proc = subprocess.Popen(
        ["./qtop", "-b", "pbs", "-s", str(sample_dir), "-c", "ON"],
        universal_newlines=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        start_new_session=True,
    )
    try:
        stdout, stderr = proc.communicate(timeout=timeout)
    except subprocess.TimeoutExpired:
        try:
            os.killpg(proc.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        proc.communicate()
        return None
    if proc.returncode != 0 or not stdout.strip():
        return None

    output_file = output_dir / f"{sample_dir.name}.ans" if save_output else None
    if output_file is not None:
        output_file.write_text(stdout)
    return {
        "sample": sample_dir.name,
        "output": output_file.name if output_file is not None else None,
        "stderr_tail": stderr.splitlines()[-5:],
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("samples_dir", type=Path)
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--output", type=Path, default=Path("/tmp/qtop-pbs-rendered"))
    parser.add_argument(
        "--skip-golden-samples",
        action="store_true",
        help="Do not force the curated 10-sample golden set into the rendered output.",
    )
    parser.add_argument(
        "--max-failures",
        type=int,
        default=None,
        help="Scan the full sample collection and fail if more than this many samples do not render.",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=8,
        help="Per-sample qtop timeout in seconds.",
    )
    args = parser.parse_args()

    sample_dirs = sorted(path for path in args.samples_dir.iterdir() if path.is_dir())
    args.output.mkdir(parents=True, exist_ok=True)
    manifest = []
    failures = []
    passed = 0
    scanned = 0
    seen = set()

    if not args.skip_golden_samples:
        for sample in GOLDEN_PBS_SAMPLES[: args.limit]:
            sample_dir = args.samples_dir / sample
            if not sample_dir.is_dir():
                raise FileNotFoundError(f"golden PBS sample not found: {sample_dir}")
            entry = render_sample(sample_dir, args.output, timeout=args.timeout)
            if entry is None:
                raise RuntimeError(f"golden PBS sample failed to render: {sample_dir}")
            passed += 1
            entry["golden"] = True
            manifest.append(entry)
            seen.add(sample_dir.name)

    for sample_dir in sample_dirs:
        if args.max_failures is None and len(manifest) >= args.limit:
            break
        if sample_dir.name in seen:
            continue
        scanned += 1
        entry = render_sample(
            sample_dir,
            args.output,
            save_output=len(manifest) < args.limit,
            timeout=args.timeout,
        )
        if entry is None:
            failures.append(sample_dir.name)
            continue
        passed += 1
        if entry["output"] is not None:
            manifest.append(entry)
        if args.max_failures is None and len(manifest) >= args.limit:
            break

    summary = {
        "rendered": len(manifest),
        "passed": passed,
        "failed": len(failures),
        "scanned": len(seen) + scanned,
    }
    (args.output / "manifest.json").write_text(json.dumps(manifest, indent=2))
    (args.output / "failures.json").write_text(json.dumps(failures, indent=2))
    (args.output / "summary.json").write_text(json.dumps(summary, indent=2))
    print(
        f"rendered={summary['rendered']} passed={summary['passed']} "
        f"failed={summary['failed']} scanned={summary['scanned']} output={args.output}"
    )
    if len(manifest) < args.limit:
        return 1
    if args.max_failures is not None and len(failures) > args.max_failures:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
