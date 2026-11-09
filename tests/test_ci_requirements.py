## Copyright (c) 2026 qtop contributors
## SPDX-License-Identifier: MIT
import re
from pathlib import Path

import pytest
from packaging.markers import default_environment
from packaging.requirements import Requirement


@pytest.mark.parametrize("implementation, expected", (("cpython", True), ("pypy", False)))
def test_atheris_is_selected_only_for_the_supported_interpreter(implementation, expected):
    requirements = (Path(__file__).resolve().parents[1] / "requirements-ci.txt").read_text()
    requirement = Requirement(next(line for line in requirements.splitlines() if line.startswith("atheris==")))
    environment = default_environment()
    environment.update(python_version="3.12", sys_platform="linux", platform_machine="x86_64", implementation_name=implementation)
    assert requirement.marker.evaluate(environment) is expected


def test_pre_commit_ruff_matches_the_ci_pin():
    root = Path(__file__).resolve().parents[1]
    hooks = (root / ".pre-commit-config.yaml").read_text()
    requirements = (root / "requirements-ci.txt").read_text()
    hook_rev = re.search(r"ruff-pre-commit\n\s+rev: v(\S+)", hooks)
    ci_pin = re.search(r"^ruff==(\S+)$", requirements, re.M)
    assert hook_rev and ci_pin and hook_rev.group(1) == ci_pin.group(1)
