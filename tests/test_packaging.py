# SPDX-License-Identifier: MIT
from tools.validate_installation import validate_installation


def test_source_distribution_installs_with_defaults_and_cli():
    validate_installation()
