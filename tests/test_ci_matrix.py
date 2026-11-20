# Copyright (c) 2026 qtop contributors
# SPDX-License-Identifier: MIT
from collections import Counter
from itertools import product
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from typing import Final

import yaml

ROOT: "Final" = Path(__file__).resolve().parents[1]
FIELDS: "Final" = ("image", "family", "pkgs", "pybin", "target", "experimental")
DEFAULTS: "Final" = {"pkgs": "-", "pybin": "python3", "target": "ci", "experimental": False}


def lane_counts(github, gitlab):
    """Return lane multisets, retaining duplicate lanes and failure policy."""
    gh = Counter(tuple(dict(DEFAULTS, **lane)[field] for field in FIELDS) for lane in github["jobs"]["lane"]["strategy"]["matrix"]["include"])
    gl = Counter()
    for name in ("nightly-matrix", "nightly-matrix-experimental"):
        job = gitlab[name]
        for row in job["parallel"]["matrix"]:
            values = dict(DEFAULTS, **{key.lower(): value for key, value in row.items()})
            values["experimental"] = job.get("allow_failure", False)
            axes = [value if isinstance(value, list) else [value] for value in (values[field] for field in FIELDS)]
            gl.update(product(*axes))
    return gh, gl


def read_matrices():
    """Read provider documents with the developer-only safe YAML loader."""
    return tuple(yaml.safe_load((ROOT / path).read_text()) for path in (".github/workflows/nightly-matrix.yml", "ci/nightly-matrix.gitlab-ci.yml"))


def test_committed_nightly_matrices_match():
    github, gitlab = lane_counts(*read_matrices())
    assert github and github == gitlab


def make_prerequisites(target):
    """Return the prerequisite targets of a double-colon Makefile rule."""
    for line in (ROOT / "Makefile").read_text().splitlines():
        if line.startswith(target + "::"):
            return line.split("::", 1)[1].split("##", 1)[0].split()
    raise AssertionError("Makefile has no %s:: rule" % target)


def test_github_split_runs_every_pre_pr_check_once():
    def workflow(name):
        return yaml.safe_load((ROOT / ".github/workflows" / name).read_text())

    lanes = workflow("build.yml")["jobs"]["modern-python"]["strategy"]["matrix"]["include"]
    build_checks = next(lane["checks"].split() for lane in lanes if lane["python-version"] == "3.12.3")
    coverage_runs = [step["run"] for step in workflow("coverage.yml")["jobs"]["coverage"]["steps"] if "run" in step]
    assert "make coverage-xml PYTHON=python" in coverage_runs
    assert "coverage" in make_prerequisites("coverage-xml")
    assert Counter(build_checks + ["coverage"]) == Counter(make_prerequisites("pre-pr"))


@pytest.mark.parametrize("change", ("missing", "duplicate", "failure-policy", "target"))
def test_nightly_matrix_drift_is_detected(change):
    github, gitlab = read_matrices()
    lanes = github["jobs"]["lane"]["strategy"]["matrix"]["include"]
    if change == "missing":
        lanes.pop()
    elif change == "duplicate":
        lanes.append(lanes[0].copy())
    elif change == "failure-policy":
        lanes[0]["experimental"] = True
    else:
        lanes[0]["target"] = "compat-py36"
    gh, gl = lane_counts(github, gitlab)
    assert gh != gl
