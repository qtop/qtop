import os
import subprocess
import sys
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]


def run_cli(tmp_path, *args):
    env = os.environ.copy()
    env["HOME"] = str(tmp_path)
    env.pop("QTOP_SCHEDULER", None)
    env["PYTHONPATH"] = str(ROOT) + os.pathsep + env.get("PYTHONPATH", "")

    return subprocess.run(
        [sys.executable, "-m", "qtop_py.cli", *args],
        cwd=str(ROOT),
        env=env,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=10,
    )


@pytest.mark.parametrize(
    "args, expected",
    (
        (("-d",), "No scheduler could be auto-detected"),
        (("-bb",), "Selected scheduler system not supported"),
        (("-b", "not-a-scheduler"), "Selected scheduler system not supported"),
    ),
)
def test_expected_cli_scheduler_errors_are_concise(tmp_path, args, expected):
    result = run_cli(tmp_path, *args)
    output = result.stdout + result.stderr

    assert result.returncode != 0
    assert expected in output
    assert "Traceback" not in output
