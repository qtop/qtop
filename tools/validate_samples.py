#!/usr/bin/env python3
"""Fast scheduler sample gate shared by local, GitHub, and GitLab CI."""

from __future__ import print_function

import argparse
import json
import os
import subprocess
import sys


def ensure_dir(path):
    if not os.path.isdir(path):
        os.makedirs(path)


def write_text(path, text):
    with open(path, "w") as handle:
        handle.write(text)


def run_command(command, env=None, timeout=12):
    return subprocess.run(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=env,
        universal_newlines=True,
        timeout=timeout,
    )


def record(scheduler, sample, status, output=None, stdout=None, stderr=None, reason=None):
    return {
        "scheduler": scheduler,
        "sample": sample,
        "status": status,
        "output": output,
        "stdout_tail": (stdout or "").splitlines()[-8:],
        "stderr_tail": (stderr or "").splitlines()[-8:],
        "reason": reason,
    }


def iter_dirs(root):
    if not root or not os.path.isdir(root):
        return
    for name in sorted(os.listdir(root)):
        path = os.path.join(root, name)
        if os.path.isdir(path):
            yield name, path


def validate_slurm(args, output_root):
    scheduler_output = os.path.join(output_root, "slurm")
    ensure_dir(scheduler_output)
    entries = []
    sample_dirs = [
        (name, path)
        for name, path in iter_dirs(args.slurm_samples_dir)
        if os.path.isfile(os.path.join(path, "squeue.txt")) and os.path.isfile(os.path.join(path, "sinfo.txt"))
    ]
    if not sample_dirs:
        return [record("slurm", None, "skipped", reason="no bundled Slurm samples found")]

    for name, path in sample_dirs[: args.limit]:
        sample_output = os.path.join(scheduler_output, name)
        ensure_dir(sample_output)
        env = os.environ.copy()
        env["QTOP_SCHEDULER"] = "slurm"
        proc = run_command([sys.executable, "-m", "qtop_py.cli", "-b", "slurm", "-s", path, "-O", "-o", "savepath=%s" % sample_output], env=env)
        status = "passed" if proc.returncode == 0 else "failed"
        entries.append(record("slurm", name, status, output=sample_output, stdout=proc.stdout, stderr=proc.stderr))
    return entries


def validate_pbs(args, output_root):
    scheduler_output = os.path.join(output_root, "pbs")
    ensure_dir(scheduler_output)
    if not args.pbs_samples_dir or not os.path.isdir(args.pbs_samples_dir):
        return [record("pbs", None, "skipped", reason="external PBS sample directory not available")]

    entries = []
    for name, path in list(iter_dirs(args.pbs_samples_dir))[: args.limit]:
        proc = run_command(["./qtop", "-b", "pbs", "-s", path, "-c", "ON"])
        output_file = os.path.join(scheduler_output, "%s.ans" % name)
        if proc.returncode == 0 and proc.stdout.strip():
            write_text(output_file, proc.stdout)
            entries.append(record("pbs", name, "passed", output=output_file, stdout=proc.stdout, stderr=proc.stderr))
        else:
            entries.append(record("pbs", name, "failed", stdout=proc.stdout, stderr=proc.stderr))
    return entries or [record("pbs", None, "skipped", reason="no PBS samples found")]


def validate_sge(args, output_root):
    if not args.sge_samples_dir or not os.path.isdir(args.sge_samples_dir):
        return [record("sge", None, "skipped", reason="SGE sample directory not available")]

    # SGE archived command traces are not bundled in this repository yet. This
    # hook keeps the shared gate ready for the qtop-test-repo samples once they
    # are mirrored, while keeping today's CI deterministic.
    return [record("sge", None, "skipped", reason="SGE sample hook configured; no runnable bundled samples yet")]


VALIDATORS = {
    "slurm": validate_slurm,
    "pbs": validate_pbs,
    "sge": validate_sge,
}


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--schedulers", default="slurm,pbs,sge", help="comma-separated schedulers to validate")
    parser.add_argument("--limit", type=int, default=6, help="maximum samples per scheduler")
    parser.add_argument("--max-failures", type=int, default=0, help="maximum failed samples allowed")
    parser.add_argument("--output", default="artifacts/sample-gate", help="directory for rendered outputs and manifest")
    parser.add_argument("--pbs-samples-dir", default="../qtop-test-repo/qtop5/results")
    parser.add_argument("--slurm-samples-dir", default="tests/plugins/slurm_samples")
    parser.add_argument("--sge-samples-dir", default="")
    return parser.parse_args()


def main():
    args = parse_args()
    ensure_dir(args.output)
    schedulers = [item.strip().lower() for item in args.schedulers.split(",") if item.strip()]
    entries = []

    for scheduler in schedulers:
        if scheduler not in VALIDATORS:
            entries.append(record(scheduler, None, "failed", reason="unknown scheduler"))
            continue
        entries.extend(VALIDATORS[scheduler](args, args.output))

    manifest_path = os.path.join(args.output, "manifest.json")
    write_text(manifest_path, json.dumps(entries, indent=2, sort_keys=True))

    failures = [entry for entry in entries if entry["status"] == "failed"]
    passed = [entry for entry in entries if entry["status"] == "passed"]
    skipped = [entry for entry in entries if entry["status"] == "skipped"]
    print("sample-gate passed=%s failed=%s skipped=%s manifest=%s" % (len(passed), len(failures), len(skipped), manifest_path))
    if len(failures) > args.max_failures:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
