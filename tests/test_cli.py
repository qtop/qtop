import os
import subprocess
import sys
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]


def run_cli(tmp_path, module, *args, extra_env=None):
    env = os.environ.copy()
    env["HOME"] = str(tmp_path)
    env.pop("QTOP_SCHEDULER", None)
    env.update(extra_env or {})
    env["PYTHONPATH"] = str(ROOT) + os.pathsep + env.get("PYTHONPATH", "")

    return subprocess.run(
        [sys.executable, "-m", module, *args],
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
@pytest.mark.parametrize("module", ("qtop_py.cli", "qtop_py.qtop"))
def test_expected_cli_scheduler_errors_are_concise(tmp_path, module, args, expected):
    result = run_cli(tmp_path, module, *args)
    output = result.stdout + result.stderr

    assert result.returncode != 0
    assert expected in output
    assert "Traceback" not in output
    assert "SchedulerNotSpecified" not in output
    assert "InvalidScheduler" not in output
    assert "SyntaxWarning" not in output


def test_packaging_entry_points_use_cli_wrapper():
    packaging_config = (ROOT / "pyproject.toml").read_text()

    assert "[project.scripts]" in packaging_config
    assert 'qtop = "qtop_py.qtop:cli_main"' in packaging_config
    assert not (ROOT / "setup.py").exists()


@pytest.mark.parametrize("module", ("qtop_py.cli", "qtop_py.qtop"))
def test_no_scheduler_message_is_not_duplicated(tmp_path, module):
    result = run_cli(tmp_path, module, extra_env={"QTOP_SCHEDULER": "nonsense"})
    output = result.stdout + result.stderr

    assert result.returncode == 1
    assert output.count("No suitable scheduler was found") == 1
    assert "Traceback" not in output
