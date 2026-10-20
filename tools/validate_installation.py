# SPDX-License-Identifier: MIT
"""Build an sdist, build its wheel, and exercise a clean installed CLI."""

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


def validate_installation(root=ROOT):
    with tempfile.TemporaryDirectory(prefix="qtop-install-check-") as temporary:
        work = Path(temporary)
        source = work / "source"
        source.mkdir()
        for filename in ("pyproject.toml", "setup.py", "README.md", "LICENSE", "qtopconf.yaml", "helpfile.txt", "MANIFEST.in"):
            shutil.copyfile(root / filename, source / filename)
        shutil.copytree(str(root / "qtop_py"), str(source / "qtop_py"), ignore=shutil.ignore_patterns("__pycache__", "*.pyc"))
        (source / "tools").mkdir()
        shutil.copyfile(root / "tools" / "build_py.py", source / "tools" / "build_py.py")
        checked([sys.executable, "-c", "from setuptools.build_meta import build_sdist; build_sdist('dist')"], source)
        (sdist,) = (source / "dist").glob("qtop-*.tar.gz")
        expected_version = checked([sys.executable, "-c", "import qtop_py; print(qtop_py.__version__)"], source).strip()
        wheels = work / "wheels"
        checked([sys.executable, "-m", "pip", "wheel", "--no-deps", "--no-build-isolation", "--wheel-dir", str(wheels), str(sdist)], work)
        (wheel,) = wheels.glob("qtop-*.whl")
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
        version = checked([str(cli), "--version"], work, env)
        if metadata["Version"] not in version:
            raise RuntimeError("Installed CLI version does not match distribution metadata")
        checked([str(cli), "--help"], work, env)
        checked([str(cli), "-b", "demo", "-E", "-O", "-o", "savepath=" + str(work / "results")], work, env)
        print("install-check: sdist/wheel, defaults, version, help and demo passed on Python %s" % sys.version.split()[0])


if __name__ == "__main__":
    validate_installation()
