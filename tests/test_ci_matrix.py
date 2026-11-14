# Copyright (c) 2026 qtop contributors
# SPDX-License-Identifier: MIT
import pytest

from tools.check_ci_matrix import lane_counts, read_matrices


def test_committed_nightly_matrices_match():
    github, gitlab = lane_counts(*read_matrices())
    assert github and github == gitlab


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
