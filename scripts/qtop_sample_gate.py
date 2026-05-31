#!/usr/bin/env python3
"""Run qtop against bundled scheduler samples and keep CI artifacts."""

from __future__ import print_function

import argparse
import os
import subprocess
import sys
import time


ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
CONTRIB_DIR = os.path.join(ROOT, "qtop_py", "contrib")
DEFAULT_ARTIFACT_DIR = os.path.join(ROOT, "artifacts", "qtop-sample-gate")


def run_command(command, cwd, log_path, timeout):
    started = time.time()
    with open(log_path, "w") as log:
        log.write("$ {}\n\n".format(" ".join(command)))
        log.flush()
        process = subprocess.Popen(
            command,
            cwd=cwd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            universal_newlines=True,
        )
        output = []
        while True:
            if process.poll() is not None:
                break
            if time.time() - started > timeout:
                process.kill()
                log.write("\nTimed out after {} seconds\n".format(timeout))
                return 124
            line = process.stdout.readline()
            if line:
                output.append(line)
                log.write(line)
                log.flush()
            else:
                time.sleep(0.05)

        remaining = process.stdout.read()
        if remaining:
            output.append(remaining)
            log.write(remaining)
        return process.returncode


def run_scheduler(scheduler, artifact_dir, timeout):
    scheduler_dir = os.path.join(artifact_dir, scheduler)
    if not os.path.isdir(scheduler_dir):
        os.makedirs(scheduler_dir)

    command = [
        sys.executable,
        "-m",
        "qtop_py.cli",
        "-b",
        scheduler,
        "-s",
        CONTRIB_DIR,
        "-O",
        "-c",
        "OFF",
        "-o",
        "savepath={}".format(scheduler_dir),
    ]
    log_path = os.path.join(scheduler_dir, "qtop-{}.log".format(scheduler))
    return run_command(command, ROOT, log_path, timeout), log_path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--schedulers",
        default="pbs,oar,sge",
        help="Comma-separated sample schedulers to run.",
    )
    parser.add_argument(
        "--artifact-dir",
        default=DEFAULT_ARTIFACT_DIR,
        help="Directory that stores logs and rendered qtop output.",
    )
    parser.add_argument(
        "--max-failures",
        type=int,
        default=0,
        help="Fail if the number of scheduler sample failures exceeds this value.",
    )
    parser.add_argument("--timeout", type=int, default=45, help="Seconds per scheduler run.")
    args = parser.parse_args()

    artifact_dir = os.path.abspath(args.artifact_dir)
    if not os.path.isdir(artifact_dir):
        os.makedirs(artifact_dir)

    failures = []
    schedulers = [item.strip() for item in args.schedulers.split(",") if item.strip()]
    for scheduler in schedulers:
        returncode, log_path = run_scheduler(scheduler, artifact_dir, args.timeout)
        status = "ok" if returncode == 0 else "failed"
        print("{}: {} (log: {})".format(scheduler, status, log_path))
        if returncode != 0:
            failures.append((scheduler, returncode))

    if failures:
        print("Sample gate failures: {}".format(", ".join("{}={}".format(name, code) for name, code in failures)))

    if len(failures) > args.max_failures:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
