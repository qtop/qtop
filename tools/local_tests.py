# Copyright (c) 2026 qtop contributors
# SPDX-License-Identifier: MIT
"""Bootstrap and test a fresh Linux-native snapshot of the current working tree."""

import argparse
import hashlib
import json
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any, Dict

ROOT = Path(__file__).resolve().parents[1]


def snapshot_checkout(source: Path, destination: Path) -> Dict[str, Any]:
    """Clone committed history and overlay tracked edits and untracked source files."""
    subprocess.check_call(["git", "clone", "--no-hardlinks", str(source), str(destination)])
    subprocess.check_call(["git", "config", "core.autocrlf", "false"], cwd=str(destination))
    patch = subprocess.check_output(["git", "diff", "--binary", "HEAD", "--"], cwd=str(source))
    if patch:
        subprocess.run(["git", "apply", "--binary", "-"], input=patch, cwd=str(destination), check=True)
    untracked = subprocess.check_output(["git", "ls-files", "--others", "--exclude-standard", "-z"], cwd=str(source))
    for name in untracked.split(b"\0"):
        if not name:
            continue
        relative = os.fsdecode(name)
        target = destination / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(str(source / relative), str(target), follow_symlinks=False)
    digest = hashlib.sha256()
    names = subprocess.check_output(["git", "ls-files", "--cached", "--others", "--exclude-standard", "-z"], cwd=str(destination))
    for name in sorted(set(names.split(b"\0")) - {b""}):
        path = destination / os.fsdecode(name)
        digest.update(name + b"\0")
        if path.is_symlink():
            digest.update(os.fsencode(os.readlink(str(path))))
        elif path.is_file():
            digest.update(path.read_bytes())
        else:
            digest.update(b"deleted")
    return {
        "revision": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=str(source), universal_newlines=True).strip(),
        "working_tree_sha256": digest.hexdigest(),
        "source_status": subprocess.check_output(["git", "status", "--short"], cwd=str(source), universal_newlines=True),
    }


def bootstrap(ENV: Path) -> None:
    """Install a managed Python 3.12 and the repository's exact CI requirements."""
    BOOTSTRAP = Path.home() / ".local" / "share" / "qtop-bootstrap"
    subprocess.check_call([sys.executable, "-m", "venv", str(BOOTSTRAP)])
    subprocess.check_call([str(BOOTSTRAP / "bin" / "pip"), "install", "uv==0.13.8"])
    UV = str(BOOTSTRAP / "bin" / "uv")
    subprocess.check_call([UV, "python", "install", "3.12"])
    if not ENV.exists():
        subprocess.check_call([UV, "venv", "--python", "3.12", "--seed", str(ENV)])
    subprocess.check_call([str(ENV / "bin" / "python"), "-m", "pip", "install", "-r", str(ROOT / "requirements-ci.txt")])


def main() -> None:
    """Run requested Make targets with a fresh snapshot and retain reports."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bootstrap", action="store_true")
    parser.add_argument("--environment", type=Path, default=Path.home() / ".venvs" / "qtop-py312")
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--reports", type=Path, default=Path.home() / ".local" / "state" / "qtop-test-reports")
    parser.add_argument("targets", nargs="*", default=["pre-pr", "schema-ci"])
    args = parser.parse_args()
    if not sys.platform.startswith("linux"):
        parser.error("the full test runner requires Linux; use tools/test-local.ps1 through WSL on Windows")
    for command in ("git", "make"):
        if shutil.which(command) is None:
            parser.error("install git, make and python3-venv before bootstrapping (Go is needed for schema-ci)")
    if args.workers < 1:
        parser.error("workers must be positive")
    if any(target.startswith("-") or "=" in target for target in args.targets):
        parser.error("targets must be Make target names")
    ENV = args.environment.expanduser().resolve()
    if args.bootstrap:
        bootstrap(ENV)
    PYTHON = ENV / "bin" / "python"
    if not PYTHON.exists():
        parser.error("test environment is missing; first run with --bootstrap")
    from tools.private_output import private_output_dir

    reports = private_output_dir(args.reports.expanduser())
    with tempfile.TemporaryDirectory(prefix="qtop-local-tests-") as temporary:
        workspace = Path(temporary) / "checkout"
        manifest = snapshot_checkout(ROOT, workspace)
        env = os.environ.copy()
        env["PATH"] = str(ENV / "bin") + os.pathsep + env.get("PATH", "")
        env["PYTEST_XDIST_AUTO_NUM_WORKERS"] = str(args.workers)
        BASE_REF = env.get("FORTIFY_BASE_REF", "origin/develop")
        env["FORTIFY_BASE_REF"] = subprocess.check_output(["git", "rev-parse", BASE_REF], cwd=str(ROOT), universal_newlines=True).strip()
        manifest.update(targets=args.targets, base_ref=env["FORTIFY_BASE_REF"], workers=args.workers, status="running")
        try:
            # Sequential targets keep reports and corpus state deterministic.
            for target in args.targets:
                subprocess.check_call(["make", target, "PYTHON=" + str(PYTHON)], cwd=str(workspace), env=env)
            manifest["status"] = "passed"
        except BaseException as error:
            manifest.update(status="failed", error_type=type(error).__name__)
            raise
        finally:
            # A unique run directory prevents stale success reports from being reused.
            run_reports = Path(tempfile.mkdtemp(prefix="run-", dir=str(reports)))
            if (workspace / "artifacts").exists():
                shutil.copytree(str(workspace / "artifacts"), str(run_reports / "artifacts"))
            (run_reports / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
            print("Local test evidence: " + str(run_reports), flush=True)


if __name__ == "__main__":
    main()
