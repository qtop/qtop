## Copyright (c) 2026 qtop contributors
## SPDX-License-Identifier: MIT
from collections import Counter
from itertools import product
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from typing import Final

import yaml

ROOT: "Final" = Path(__file__).resolve().parents[1]
FIELDS: "Final" = ("image", "family", "pkgs", "pybin", "target", "tier")
DEFAULTS: "Final" = {"pkgs": "-", "pybin": "python3", "target": "ci"}
TIERS: "Final" = ("core", "wide")  # GitHub: nightly-<tier>.yml; GitLab: job nightly-<tier>


def lane_counts(github, gitlab):
    """Return lane multisets per tier, retaining duplicate lanes."""
    gh = Counter(tuple(dict(DEFAULTS, tier=tier, **lane)[field] for field in FIELDS) for tier in TIERS for lane in github[tier]["jobs"]["lane"]["strategy"]["matrix"]["include"])
    gl = Counter()
    for tier in TIERS:
        for row in gitlab["nightly-" + tier]["parallel"]["matrix"]:
            values = dict(DEFAULTS, tier=tier, **{key.lower(): value for key, value in row.items()})
            axes = [value if isinstance(value, list) else [value] for value in (values[field] for field in FIELDS)]
            gl.update(product(*axes))
    return gh, gl


def read_matrices():
    """Read provider documents with the developer-only safe YAML loader."""

    def load(path):
        return yaml.safe_load((ROOT / path).read_text())

    return {tier: load(".github/workflows/nightly-%s.yml" % tier) for tier in TIERS}, load("ci/nightly-matrix.gitlab-ci.yml")


def test_committed_nightly_matrices_match():
    github, gitlab = lane_counts(*read_matrices())
    assert github and github == gitlab
    assert sum(1 for lane in github.elements() if lane[-1] == "core") == 8


def test_only_wide_lanes_may_fail_without_failing_their_run():
    github, gitlab = read_matrices()
    assert all("continue-on-error" not in job for workflow in github.values() for job in workflow["jobs"].values())
    assert gitlab["nightly-wide"]["allow_failure"] is True and "allow_failure" not in gitlab["nightly-core"]


def make_prerequisites(target):
    """Return the prerequisite targets of a double-colon Makefile rule."""
    for line in (ROOT / "Makefile").read_text().splitlines():
        if line.startswith(target + "::"):
            return line.split("::", 1)[1].split("##", 1)[0].split()
    raise AssertionError("Makefile has no %s:: rule" % target)


def make_recipe(target):
    """Return the words of a double-colon Makefile rule's recipe lines."""
    lines = (ROOT / "Makefile").read_text().splitlines()
    start = next(index for index, line in enumerate(lines) if line.startswith(target + "::"))
    recipe = []
    for line in lines[start + 1 :]:
        if not line.startswith("\t"):
            break
        recipe += line.split()
    return recipe


def test_scheduled_nightly_runs_test_develop():
    """GitHub schedules main, which only moves at releases; scheduled runs check out develop instead."""
    github, _ = read_matrices()
    checkouts = [step for workflow in github.values() for job in workflow["jobs"].values() for step in job["steps"] if step.get("uses", "").startswith("actions/checkout")]
    assert checkouts and all(step["with"]["ref"] == "${{ github.event_name == 'schedule' && 'develop' || '' }}" for step in checkouts)


def test_pull_requests_run_the_core_debian_lane_on_both_forges():
    """Most nightly failures came from lane conditions (root, distro toolchain); PRs now run one such lane."""
    github, _ = read_matrices()
    lane = next(lane for lane in github["core"]["jobs"]["lane"]["strategy"]["matrix"]["include"] if lane["image"] == "debian:bookworm")
    command = "bash ci/scripts/matrix_lane.sh %s %s %s %s" % tuple(dict(DEFAULTS, **lane)[field] for field in ("family", "pkgs", "pybin", "target"))
    build = yaml.safe_load((ROOT / ".github/workflows/build.yml").read_text())["jobs"]["nightly-lane"]
    assert build["container"] == "debian:bookworm" and command in [step.get("run") for step in build["steps"]]
    gitlab = yaml.safe_load((ROOT / "ci/test.gitlab-ci.yml").read_text())["nightly-lane"]
    assert gitlab["image"] == "debian:bookworm" and gitlab["script"] == [command]


def test_github_split_runs_every_pre_pr_check_once():
    def workflow(name):
        return yaml.safe_load((ROOT / ".github/workflows" / name).read_text())

    lanes = workflow("build.yml")["jobs"]["modern-python"]["strategy"]["matrix"]["include"]
    build_checks = next(lane["checks"].split() for lane in lanes if lane["python-version"] == "3.12.3")
    coverage_runs = [step["run"] for step in workflow("coverage.yml")["jobs"]["coverage"]["steps"] if "run" in step]
    assert "make coverage-xml PYTHON=python" in coverage_runs
    assert "coverage" in make_prerequisites("coverage-xml")
    assert Counter(build_checks + ["coverage"]) == Counter(make_prerequisites("pre-pr"))


@pytest.mark.parametrize("change", ("missing", "duplicate", "tier", "target"))
def test_nightly_matrix_drift_is_detected(change):
    github, gitlab = read_matrices()
    lanes = github["core"]["jobs"]["lane"]["strategy"]["matrix"]["include"]
    if change == "missing":
        lanes.pop()
    elif change == "duplicate":
        lanes.append(lanes[0].copy())
    elif change == "tier":
        github["wide"]["jobs"]["lane"]["strategy"]["matrix"]["include"].append(lanes.pop())
    else:
        lanes[0]["target"] = "compat-py36"
    gh, gl = lane_counts(github, gitlab)
    assert gh != gl


def test_attested_sbom_keeps_the_serial_number_attest_sbom_requires():
    """actions/attest-sbom rejects CycloneDX without serialNumber, which --output-reproducible omits."""
    assert "--output-reproducible" not in make_recipe("sbom")
    assert "--output-format JSON" in " ".join(make_recipe("sbom"))


def test_release_attests_the_published_distributions_and_their_sbom():
    workflow = yaml.safe_load((ROOT / ".github/workflows/release.yml").read_text())
    build = workflow["jobs"]["build"]
    steps = build["steps"]
    positions = {step.get("id") or step.get("run", "").split(" PYTHON")[0]: index for index, step in enumerate(steps)}
    attest = {step["uses"].split("@")[0]: step for step in steps if step.get("uses", "").startswith("actions/attest")}
    # github-build writes the SBOMs (validated-build runs sbom sbom-ci); verification follows both attestations.
    assert {"release-check", "sbom", "sbom-ci"} <= set(make_recipe("validated-build"))
    assert positions["make ci-deps github-ci github-build"] < positions["provenance"] < positions["sbom-attestation"]
    assert positions["sbom-attestation"] < positions["make verify-attestations"]
    assert set(attest) == {"actions/attest-build-provenance", "actions/attest-sbom"}
    assert all(step["with"]["subject-path"] == "dist/*" for step in attest.values())
    assert attest["actions/attest-sbom"]["with"]["sbom-path"] == "artifacts/sbom/qtop.cdx.json"
    assert "SBOM_DIR ?= artifacts/sbom" in (ROOT / "Makefile").read_text()
    assert build["permissions"] == {"contents": "read", "id-token": "write", "attestations": "write"}
    assert workflow["jobs"]["release-assets"]["permissions"] == {"contents": "write"}
    assert workflow["jobs"]["release-assets"]["needs"] == "build"
    # The offline recipe reads the bundles under the names the release attaches them as.
    assets = next(step["run"] for step in steps if "artifacts/release-assets" in step.get("run", ""))
    offline = " ".join(make_recipe("verify-attestations-offline"))
    for name in ("qtop-provenance.intoto.jsonl", "qtop-sbom.intoto.jsonl"):
        assert name in assets and name in offline
    assert "qtop-release-evidence.json" in assets and "qtop.cdx.json" in assets and "ci.cdx.json" in assets
    for job in workflow["jobs"].values():
        for step in job["steps"]:
            if "uses" in step:
                assert len(step["uses"].split("@")[1]) == 40, "pin %s to a commit SHA" % step["uses"]
