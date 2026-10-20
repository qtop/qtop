# SPDX-License-Identifier: MIT
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
