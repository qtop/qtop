# Copyright (c) 2026 qtop contributors
# SPDX-License-Identifier: MIT
import pytest

from tools.check_coverage import percentages


def test_runtime_floors_are_independent_of_tooling_and_branch_totals():
    report = {
        "files": {
            "qtop_py/a.py": {"summary": {"num_statements": 100, "covered_lines": 94, "num_branches": 20, "covered_branches": 16}},
            "tools/a.py": {"summary": {"num_statements": 1000, "covered_lines": 1000, "num_branches": 1000, "covered_branches": 1000}},
        }
    }
    assert percentages(report) == (94, 80)


@pytest.mark.parametrize("files", ({}, {"qtop_py/a.py": {"summary": {"num_statements": 1, "covered_lines": 1, "num_branches": 0}}}))
def test_missing_measurements_fail_closed(files):
    with pytest.raises(ValueError):
        percentages({"files": files})
