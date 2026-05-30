#!/usr/bin/env python3
"""Run qtop against archived PBS samples and save rendered output.

Usage:
    python tools/validate_pbs_samples.py /path/to/qtop-test-repo/qtop5/results --limit 100 --output /tmp/qtop-pbs-rendered
"""

import argparse
import json
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


def render_sample(sample_dir, output_dir):
    proc = subprocess.run(
        ["./qtop", "-b", "pbs", "-s", str(sample_dir), "-c", "ON"],
        text=True,
        capture_output=True,
        timeout=8,
    )
    if proc.returncode != 0 or not proc.stdout.strip():
        return None

    output_file = output_dir / f"{sample_dir.name}.ans"
    output_file.write_text(proc.stdout)
    return {
        "sample": sample_dir.name,
        "output": output_file.name,
        "stderr_tail": proc.stderr.splitlines()[-5:],
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
    args = parser.parse_args()

    sample_dirs = sorted(path for path in args.samples_dir.iterdir() if path.is_dir())
    args.output.mkdir(parents=True, exist_ok=True)
    manifest = []
    seen = set()

    if not args.skip_golden_samples:
        for sample in GOLDEN_PBS_SAMPLES[: args.limit]:
            sample_dir = args.samples_dir / sample
            if not sample_dir.is_dir():
                raise FileNotFoundError(f"golden PBS sample not found: {sample_dir}")
            entry = render_sample(sample_dir, args.output)
            if entry is None:
                raise RuntimeError(f"golden PBS sample failed to render: {sample_dir}")
            entry["golden"] = True
            manifest.append(entry)
            seen.add(sample_dir.name)

    for sample_dir in sample_dirs:
        if len(manifest) >= args.limit:
            break
        if sample_dir.name in seen:
            continue
        entry = render_sample(sample_dir, args.output)
        if entry is None:
            continue
        manifest.append(entry)
        if len(manifest) >= args.limit:
            break

    (args.output / "manifest.json").write_text(json.dumps(manifest, indent=2))
    print(f"validated={len(manifest)} output={args.output}")
    return 0 if len(manifest) >= args.limit else 1


if __name__ == "__main__":
    raise SystemExit(main())
