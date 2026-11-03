## Copyright (c) 2026 qtop contributors
## SPDX-License-Identifier: MIT
"""Validate source or prebuilt release distributions in isolated installations."""

import argparse
import hashlib
import json
import os
import shutil
import subprocess
import sys
import tempfile
import zipfile
from configparser import ConfigParser
from email.parser import Parser
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def checked(command, cwd, env=None):
    result = subprocess.run(command, cwd=str(cwd), env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, universal_newlines=True, timeout=120)
    if result.returncode:
        raise RuntimeError("%s failed:\n%s" % (command, result.stdout))
    return result.stdout


def digest(path):
    result = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(65536), b""):
            result.update(chunk)
    return result.hexdigest()


def exercise_wheel(wheel, expected_version, root, work):
    with zipfile.ZipFile(str(wheel)) as archive:
        metadata_path = next(name for name in archive.namelist() if name.endswith(".dist-info/METADATA"))
        metadata = Parser().parsestr(archive.read(metadata_path).decode("utf-8"))
        entry_points = archive.read(metadata_path.rsplit("/", 1)[0] + "/entry_points.txt").decode("utf-8")
        entry_config = ConfigParser()
        entry_config.read_string(entry_points)
        if entry_config.get("console_scripts", "qtop") != "qtop_py.qtop:cli_main":
            raise RuntimeError("Installed console script must use the CLI error wrapper")
        if metadata["Name"] != "qtop" or metadata["Version"] != expected_version or metadata["Requires-Python"] != ">=3.6" or metadata.get_all("Requires-Dist"):
            raise RuntimeError("Incorrect package identity, Python requirement or unexpected runtime dependencies")
        for filename in ("qtopconf.yaml", "helpfile.txt"):
            if archive.read("qtop_py/" + filename) != (root / filename).read_bytes():
                raise RuntimeError("Missing or altered installed defaults: %s" % filename)
    environment = work / "installed"
    checked([sys.executable, "-m", "venv", str(environment)], work)
    bindir = environment / ("Scripts" if os.name == "nt" else "bin")
    python = bindir / ("python.exe" if os.name == "nt" else "python")
    cli = bindir / ("qtop.exe" if os.name == "nt" else "qtop")
    env = dict(os.environ, HOME=str(work))
    for name in ("PYTHONPATH", "PYTHONHOME", "QTOP_SCHEDULER"):
        env.pop(name, None)
    checked([str(python), "-m", "pip", "install", "--no-deps", "--no-index", str(wheel)], work, env)
    installed_path = checked([str(python), "-I", "-c", "import qtop_py; print(qtop_py.__file__)"], work, env).strip()
    if environment.resolve() not in Path(installed_path).resolve().parents:
        raise RuntimeError("Package was not imported from the clean environment")
    version = checked([str(cli), "--version"], work, env)
    if metadata["Version"] not in version:
        raise RuntimeError("Installed CLI version does not match distribution metadata")
    checked([str(cli), "--help"], work, env)
    for backend, source in (
        ("demo", None),
        ("pbs", root / "qtop_py/contrib"),
        ("oar", root / "qtop_py/contrib"),
        ("sge", root / "qtop_py/contrib"),
        ("slurm", root / "tests/plugins/slurm_samples/basic"),
    ):
        output = work / (backend + "-results")
        command = [str(cli), "-b", backend, "-E", "-O", "-o", "savepath=" + str(output)]
        if source is not None:
            command.extend(["-s", str(source)])
        checked(command, work, env)
        (export,) = output.glob("qtop_json_*.json")
        state = json.loads(export.read_text())
        if not all(isinstance(state.get(key), list) for key in ("jobs", "nodes", "queues")):
            raise RuntimeError("Installed CLI produced an invalid %s export" % backend)
    print("install-check: %s defaults, version, help and all backends passed on Python %s" % (wheel.name, sys.version.split()[0]))


def validate_installation(root=ROOT, artifacts=None):
    with tempfile.TemporaryDirectory(prefix="qtop-install-check-") as temporary:
        work = Path(temporary)
        expected_version = checked([sys.executable, "-c", "import qtop_py; print(qtop_py.__version__)"], root).strip()
        supplied = []
        if artifacts is None:
            source = work / "source"
            source.mkdir()
            for filename in ("pyproject.toml", "setup.py", "README.md", "LICENSE", "qtopconf.yaml", "helpfile.txt", "MANIFEST.in"):
                shutil.copyfile(root / filename, source / filename)
            shutil.copytree(str(root / "qtop_py"), str(source / "qtop_py"), ignore=shutil.ignore_patterns("__pycache__", "*.pyc"))
            (source / "tools").mkdir()
            shutil.copyfile(root / "tools" / "build_py.py", source / "tools" / "build_py.py")
            checked([sys.executable, "-c", "from setuptools.build_meta import build_sdist; build_sdist('dist')"], source)
            (sdist,) = (source / "dist").glob("qtop-*.tar.gz")
        else:
            artifacts = Path(artifacts).resolve()
            files = sorted(artifacts.iterdir())
            sdists = list(artifacts.glob("qtop-*.tar.gz"))
            wheels_to_publish = list(artifacts.glob("qtop-*.whl"))
            if len(sdists) != 1 or len(wheels_to_publish) != 1 or len(files) != 2 or any(not file.is_file() or file.is_symlink() for file in files):
                raise RuntimeError("Release directory must contain exactly one qtop sdist and one wheel")
            (sdist,), (published_wheel,) = sdists, wheels_to_publish
            supplied = [published_wheel]
        hashes = {path: digest(path) for path in [sdist] + supplied}
        wheels = work / "wheels"
        checked([sys.executable, "-m", "pip", "wheel", "--no-deps", "--no-build-isolation", "--no-index", "--wheel-dir", str(wheels), str(sdist)], work)
        (wheel,) = wheels.glob("qtop-*.whl")
        for index, candidate in enumerate(supplied + [wheel]):
            case = work / ("case-%s" % index)
            case.mkdir()
            exercise_wheel(candidate, expected_version, root, case)
        for path, before in hashes.items():
            if digest(path) != before:
                raise RuntimeError("Validation modified a release input: %s" % path)
            print("validated sha256 %s %s" % (before, path.name))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--artifacts-dir", type=Path, help="Validate the exact prebuilt sdist/wheel pair in this directory")
    validate_installation(artifacts=parser.parse_args().artifacts_dir)
