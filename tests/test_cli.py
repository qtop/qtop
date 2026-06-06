##
## qtop is a tool to monitor queuing systems - https://github.com/qtop/qtop
##
## SPDX-License-Identifier: MIT
##

import os
import subprocess
import sys

import pytest


@pytest.mark.parametrize(
    "arguments, expected_message",
    (
        (("-bb",), "The selected scheduler is not supported"),
        (("-b", "auto"), "No scheduler could be auto-detected"),
    ),
)
@pytest.mark.parametrize("module", ("qtop_py.cli", "qtop_py.qtop"))
def test_known_cli_errors_do_not_show_tracebacks(module, arguments, expected_message):
    environment = os.environ.copy()
    environment.pop("QTOP_SCHEDULER", None)

    result = subprocess.run(
        [sys.executable, "-m", module] + list(arguments),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        universal_newlines=True,
        env=environment,
    )

    assert result.returncode == 1
    assert expected_message in result.stderr
    assert "Traceback" not in result.stderr
