## Copyright (c) 2026 qtop contributors
## SPDX-License-Identifier: MIT
import shutil
import sys
import zipfile

import pytest

from qtop_py import __version__
from tools.validate_installation import ROOT, checked, digest, exercise_wheel, validate_installation


def test_source_distribution_installs_with_defaults_and_cli():
    validate_installation()


def build_release_artifacts(directory):
    source = directory / "source"
    shutil.copytree(str(ROOT), str(source), ignore=shutil.ignore_patterns(".git", "artifacts", "build", "dist", "*.egg-info", "__pycache__", ".*cache", ".venv"))
    output = directory / "dist"
    checked([sys.executable, "-m", "build", "--no-isolation", "--outdir", str(output)], source)
    return output


@pytest.fixture(scope="module")
def release_artifacts(tmp_path_factory):
    return build_release_artifacts(tmp_path_factory.mktemp("release-artifacts"))


def test_packaging_workers_use_distinct_source_trees(tmp_path, monkeypatch):
    sources = []
    monkeypatch.setitem(globals(), "checked", lambda command, cwd: sources.append(cwd))
    for name in ("worker-a", "worker-b"):
        build_release_artifacts(tmp_path / name)
    assert sources[0] != sources[1] and ROOT not in sources
    assert all((source / "qtop_py" / "__init__.py").is_file() for source in sources)


def test_exact_release_artifacts_install_without_being_replaced(release_artifacts, capsys):
    before = {path.name: digest(path) for path in release_artifacts.iterdir()}
    validate_installation(artifacts=release_artifacts)
    assert {path.name: digest(path) for path in release_artifacts.iterdir()} == before
    assert capsys.readouterr().out.count("all backends passed") == 2


@pytest.mark.parametrize("layout", ["missing", "stale", "foreign", "symlink"])
def test_release_directory_rejects_ambiguous_inputs(tmp_path, layout):
    (tmp_path / "qtop-1.tar.gz").write_bytes(b"sdist")
    if layout != "missing":
        wheel = tmp_path / "qtop-1-py3-none-any.whl"
        if layout == "symlink":
            wheel.symlink_to(tmp_path / "qtop-1.tar.gz")
        else:
            wheel.write_bytes(b"wheel")
    if layout == "stale":
        (tmp_path / "qtop-0-py3-none-any.whl").write_bytes(b"old wheel")
    if layout == "foreign":
        (tmp_path / "other-package.tar.gz").write_bytes(b"other")
    with pytest.raises(RuntimeError, match="exactly one qtop sdist and one wheel"):
        validate_installation(artifacts=tmp_path)


@pytest.mark.parametrize("tampering", ["version", "defaults", "entrypoint"])
def test_release_metadata_and_assets_are_checked_before_installation(release_artifacts, tmp_path, tampering):
    (original,) = release_artifacts.glob("*.whl")
    wheel = tmp_path / original.name
    with zipfile.ZipFile(str(original)) as source, zipfile.ZipFile(str(wheel), "w") as target:
        for member in source.infolist():
            data = source.read(member)
            if tampering == "version" and member.filename.endswith(".dist-info/METADATA"):
                data = data.replace(("Version: " + __version__).encode(), b"Version: 0.0.0")
            elif tampering == "defaults" and member.filename == "qtop_py/qtopconf.yaml":
                data = b"tampered"
            elif tampering == "entrypoint" and member.filename.endswith("entry_points.txt"):
                data = data.replace(b"qtop_py.qtop:cli_main", b"qtop_py.qtop:main")
            target.writestr(member, data)
    with pytest.raises(RuntimeError, match="package identity|installed defaults|console script"):
        exercise_wheel(wheel, __version__, ROOT, tmp_path)
