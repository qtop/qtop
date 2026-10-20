import shutil
import subprocess
import sys
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_wheel_contains_runtime_defaults(tmp_path):
    source = tmp_path / "source"
    source.mkdir()
    for filename in ("pyproject.toml", "README.md", "LICENSE", "qtopconf.yaml", "helpfile.txt"):
        shutil.copyfile(ROOT / filename, source / filename)
    shutil.copytree(ROOT / "qtop_py", source / "qtop_py", ignore=shutil.ignore_patterns("__pycache__", "*.pyc"))
    (source / "tools").mkdir()
    shutil.copyfile(ROOT / "tools" / "build_py.py", source / "tools" / "build_py.py")
    wheels = tmp_path / "wheels"

    result = subprocess.run(
        [sys.executable, "-m", "build", "--wheel", "--no-isolation", "--outdir", str(wheels), str(source)],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        universal_newlines=True,
    )

    assert result.returncode == 0, result.stdout
    wheel_files = list(wheels.glob("*.whl"))
    assert len(wheel_files) == 1
    with zipfile.ZipFile(wheel_files[0]) as wheel:
        for filename in ("qtopconf.yaml", "helpfile.txt"):
            assert wheel.read("qtop_py/" + filename) == (ROOT / filename).read_bytes()
