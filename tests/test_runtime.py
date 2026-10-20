## SPDX-License-Identifier: MIT
"""Exercise collection, rendering and cleanup together inside the test process."""

import itertools
import json
import logging
import re
import shutil
import sys
import tarfile
from pathlib import Path

import pytest

from qtop_py import qtop, utils
from qtop_py.sample import SNAPSHOT_NAME

ROOT = Path(__file__).resolve().parents[1]
RUNTIME_GLOBALS = (
    "CURPATH",
    "QTOPPATH",
    "HELP_FP",
    "SAMPLE_FILENAME",
    "scheduler",
    "config",
    "dynamic_config",
    "cluster",
    "viewport",
    "user_to_color",
    "args",
    "transposed_matrices",
    "h_counter",
    "change_mapping",
    "help_main_switch",
    "web",
    "document",
)


@pytest.fixture
def runtime(tmp_path, monkeypatch):
    for name in RUNTIME_GLOBALS:
        monkeypatch.setattr(qtop, name, getattr(qtop, name, None), raising=False)
    home = tmp_path / "home"
    home.mkdir()
    monkeypatch.setenv("HOME", str(home))
    monkeypatch.setenv("USER", "runtime-test")
    monkeypatch.delenv("QTOP_SCHEDULER", raising=False)
    monkeypatch.delenv("QTOP_COLOR", raising=False)
    monkeypatch.setattr(qtop, "USERPATH", str(home / ".local/qtop"))
    monkeypatch.setattr(qtop, "SYSTEMCONFDIR", str(tmp_path / "etc"))
    log = str(tmp_path / "logs/qtop.log")
    monkeypatch.setattr(qtop, "QTOP_LOGFILE", log)
    monkeypatch.setattr(utils, "QTOP_LOGFILE", log)
    sequence = itertools.count()

    def run(*arguments, save_only=True):
        output = tmp_path / ("output-%s" % next(sequence))
        output.mkdir()
        monkeypatch.chdir(ROOT)
        monkeypatch.setattr(sys, "argv", ["qtop", "-c", "OFF", "-E"] + (["-O"] if save_only else []) + list(arguments) + ["-o", "savepath=" + str(output)])
        logger = logging.getLogger()
        handlers = logger.handlers[:]
        level, disabled = logger.level, logger.disabled
        try:
            return qtop.cli_main(), output
        finally:
            for handler in logger.handlers[:]:
                if handler not in handlers:
                    logger.removeHandler(handler)
                    handler.close()
            logger.setLevel(level)
            logger.disabled = disabled

    return run


@pytest.fixture
def scheduler_inputs(tmp_path):
    source = tmp_path / "inputs"
    shutil.copytree(str(ROOT / "qtop_py/contrib"), str(source))
    for name in ("squeue.txt", "sinfo.txt"):
        shutil.copyfile(str(ROOT / "tests/plugins/slurm_samples/basic" / name), str(source / name))
    return source


@pytest.mark.parametrize("backend, nodes, running", (("slurm", 3, 2), ("pbs", 829, 7590), ("sge", 17, 61), ("oar", 183, 0)))
@pytest.mark.parametrize("mode", ("default", "classic", "anonymous"))
def test_runtime_exports_and_renders_each_scheduler(runtime, scheduler_inputs, backend, nodes, running, mode):
    flags = {
        "default": [],
        "classic": ["-C", "-T", "-F", "-a", "-rr", "-4", "--expand-queues", "-c", "ON"],
        "anonymous": ["-LL", "-A", "-e", "-F"],
    }[mode]
    result, output = runtime("-b", backend, "-s", str(scheduler_inputs), *flags)
    assert result == 0
    state = json.loads(next(output.glob("qtop_json_*.json")).read_text())
    assert len(state["nodes"]) == nodes
    assert sum(queue["running"] for queue in state["queues"]) == running
    screen = next(output.glob("qtop_fullview_*.out")).read_text()
    colored = screen
    screen = re.sub(r"\x1b\[[0-9;]*m", "", screen)
    assert "Job accounting summary" in screen
    assert "Worker Nodes occupancy" in screen
    assert "User accounts and pool mappings" in screen
    assert "Total:%s" % nodes in screen
    assert ("\x1b[1;" in colored) == (mode == "classic")
    assert len(qtop.cluster.workernode_dict) >= nodes
    if mode == "anonymous":
        archive = next(output.glob("*.tar"))
        with tarfile.open(str(archive)) as saved:
            assert json.load(saved.extractfile(SNAPSHOT_NAME))["state"] == state
            assert "qtop.log" not in saved.getnames()
        result, replayed = runtime("--load-sample", str(archive))
        assert result == 0
        assert json.loads(next(replayed.glob("qtop_json_*.json")).read_text()) == state


def test_runtime_renders_large_cluster_in_multiple_matrices(runtime):
    source = ROOT / "tests/plugins/slurm_samples/large_cluster"
    result, output = runtime("-b", "slurm", "-s", str(source), "-T", "-o", "term_size=[30,20]")
    assert result == 0
    state = json.loads(next(output.glob("qtop_json_*.json")).read_text())
    assert len(state["nodes"]) == 18
    assert "User accounts and pool mappings" in next(output.glob("qtop_fullview_*.out")).read_text()
    assert qtop.viewport.max_width == 20
    assert qtop.cluster.highest_wn > qtop.viewport.h_term_size - 11


@pytest.mark.parametrize("failure, expected", (("missing", "Scheduler input file"), ("config", "Invalid configuration"), ("xml", "Cannot parse sge scheduler input")))
def test_runtime_failures_restore_stdout_and_close_logs(runtime, tmp_path, capsys, failure, expected):
    stdout = sys.stdout
    if failure == "missing":
        flags = ["-b", "slurm", "-s", str(tmp_path)]
    elif failure == "config":
        config = tmp_path / "bad.yaml"
        config.write_text("sorting: wrong-type\n")
        flags = ["-b", "slurm", "-f", str(config)]
    else:
        (tmp_path / "qstat.F.xml.stdout").write_text("<broken>")
        flags = ["-b", "sge", "-s", str(tmp_path)]
    original_handlers = logging.getLogger().handlers[:]
    result, _output = runtime(*flags)
    assert result == 1
    assert sys.stdout is stdout
    assert logging.getLogger().handlers == original_handlers
    assert expected in capsys.readouterr().err


def test_watch_refreshes_after_transpose_and_quits_cleanly(runtime, scheduler_inputs, monkeypatch):
    keys = iter(("t", "q"))
    monkeypatch.setattr(qtop, "wait_for_keypress_or_autorefresh", lambda *_args: next(keys))
    calls = []
    monkeypatch.setattr(qtop.subprocess, "call", lambda command, **_kwargs: calls.append(command) or 0)
    with pytest.raises(SystemExit) as stopped:
        runtime("-b", "slurm", "-s", str(scheduler_inputs), "-w", "1", "-T", save_only=False)
    assert stopped.value.code == 0
    assert qtop.dynamic_config["transpose_wn_matrices"] is True
    assert len(calls) == 4
    assert calls[0] == calls[2] == ["/usr/bin/clear"]
    assert all(command[1].endswith(".out") for command in (calls[1], calls[3]))
