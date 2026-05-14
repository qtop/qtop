#!/usr/bin/env python

import argparse
import csv
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path


REQUIRED_SAMPLE_FILES = ("pbsnodes_a.txt", "qstat.txt", "qstat_q.txt")


def sample_dirs(samples_root):
    samples_root = Path(samples_root)
    for sample_dir in sorted(path for path in samples_root.iterdir() if path.is_dir()):
        if all((sample_dir / filename).exists() for filename in REQUIRED_SAMPLE_FILES):
            yield sample_dir


def count_worker_nodes(sample_dir):
    count = 0
    with open(sample_dir / "pbsnodes_a.txt", encoding="utf-8", errors="replace") as fin:
        for line in fin:
            if line.strip() and not line.startswith((" ", "\t")):
                count += 1
    return count


def newest_qtop_output(savepath):
    outputs = sorted(savepath.glob("qtop_fullview_*.out"), key=lambda path: path.stat().st_mtime)
    return outputs[-1] if outputs else None


def run_sample(repo_root, sample_dir, run_root, rendered_root):
    savepath = run_root / sample_dir.name
    savepath.mkdir(parents=True, exist_ok=True)
    cmd = [
        sys.executable,
        "-m",
        "qtop_py.cli",
        "-b",
        "pbs",
        "-c",
        "ON",
        "-O",
        "-s",
        str(sample_dir),
        "-o",
        "savepath=%s" % savepath,
    ]

    started = time.time()
    proc = subprocess.run(
        cmd,
        cwd=repo_root,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        timeout=60,
    )
    elapsed = time.time() - started

    rendered_output = ""
    if proc.returncode == 0:
        latest = newest_qtop_output(savepath)
        if latest is None:
            proc.returncode = 1
            error = "qtop completed without producing a qtop_fullview output"
        else:
            rendered_output_path = rendered_root / ("%s.ansi.txt" % sample_dir.name)
            shutil.copyfile(latest, rendered_output_path)
            rendered_output = str(rendered_output_path)
            error = ""
    else:
        error = (proc.stderr or proc.stdout).strip().splitlines()[-1] if (proc.stderr or proc.stdout).strip() else "qtop failed"

    return {
        "sample": sample_dir.name,
        "worker_nodes": count_worker_nodes(sample_dir),
        "status": "pass" if proc.returncode == 0 else "fail",
        "seconds": "%.3f" % elapsed,
        "rendered_output": rendered_output,
        "error": error,
    }


def write_manifest(manifest_path, rows):
    with open(manifest_path, "w", newline="", encoding="utf-8") as fout:
        writer = csv.DictWriter(fout, fieldnames=["sample", "worker_nodes", "status", "seconds", "rendered_output", "error"])
        writer.writeheader()
        writer.writerows(rows)


def parse_args():
    parser = argparse.ArgumentParser(description="Render archived PBS qtop samples and save ANSI-coloured output.")
    parser.add_argument("samples_root", help="Directory containing qtop-test-repo/qtop5/results samples.")
    parser.add_argument("--limit", type=int, default=int(os.environ.get("PBS_SAMPLE_LIMIT", "100")), help="Number of samples to validate. Use 0 for all samples.")
    parser.add_argument("--output", default=os.environ.get("PBS_OUTPUT_DIR", ".work/pbs-samples"), help="Directory for manifest, logs and rendered ANSI output.")
    return parser.parse_args()


def main():
    args = parse_args()
    repo_root = Path(__file__).resolve().parents[1]
    output_root = Path(args.output).resolve()
    run_root = output_root / "runs"
    rendered_root = output_root / "rendered"
    rendered_root.mkdir(parents=True, exist_ok=True)
    run_root.mkdir(parents=True, exist_ok=True)

    samples = list(sample_dirs(args.samples_root))
    selected = samples if args.limit == 0 else samples[: args.limit]
    if args.limit and len(selected) < args.limit:
        print("Only found %s PBS samples, need %s." % (len(selected), args.limit), file=sys.stderr)
        return 2

    rows = []
    for idx, sample_dir in enumerate(selected, 1):
        row = run_sample(repo_root, sample_dir, run_root, rendered_root)
        rows.append(row)
        print("%s/%s %s %s nodes %s" % (idx, len(selected), row["status"].upper(), row["worker_nodes"], row["sample"]))

    manifest_path = output_root / "manifest.csv"
    write_manifest(manifest_path, rows)

    failures = [row for row in rows if row["status"] != "pass"]
    print("Validated %s PBS samples: %s passed, %s failed." % (len(rows), len(rows) - len(failures), len(failures)))
    print("Manifest: %s" % manifest_path)
    print("Rendered ANSI output: %s" % rendered_root)

    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
