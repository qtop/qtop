## Copyright (c) 2026 qtop contributors
## SPDX-License-Identifier: MIT
"""Exercise collection, rendering and cleanup together inside the test process."""

import json
import logging
import re
import shutil
import sys
import tarfile
from pathlib import Path

import pytest

from qtop_py import core_process as process_io
from qtop_py import qtop
from qtop_py.core_sample import SNAPSHOT_NAME
from qtop_py.snapshot import Snapshot
from qtop_py.ui import terminal as terminal_io

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture
def scheduler_inputs(tmp_path):
    source = tmp_path / "inputs"
    shutil.copytree(str(ROOT / "qtop_py/contrib"), str(source))
    for name in ("squeue.txt", "sinfo.txt"):
        shutil.copyfile(str(ROOT / "tests/plugins/slurm_samples/basic" / name), str(source / name))
    for path in (ROOT / "tests/plugins/slurm_json_samples/v0.0.43").glob("*.json"):
        shutil.copyfile(str(path), str(source / path.name))
    return source


@pytest.mark.parametrize("backend, nodes, running", (("slurm", 3, 2), ("slurm_json", 3, 2), ("pbs", 829, 7590), ("sge", 17, 62), ("oar", 183, 190)))
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
    if backend == "oar":
        assert sum(queue["queued"] for queue in state["queues"]) == 69
    screen = next(output.glob("qtop_fullview_*.out")).read_text()
    colored = screen
    screen = re.sub(r"\x1b\[[0-9;]*m", "", screen)
    assert "Job accounting summary" in screen
    assert "Worker Nodes occupancy" in screen
    assert "User accounts and pool mappings" in screen
    assert "Total:%s" % nodes in screen
    assert ("\x1b[1;" in colored) == (mode == "classic")
    assert len(runtime.views[-1].cluster.workernode_dict) >= nodes
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
    viewport = runtime.views[-1].model.viewport
    assert viewport.max_width == 20
    assert runtime.views[-1].cluster.highest_wn > viewport.h_term_size - 11


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
    monkeypatch.setattr(terminal_io, "wait_for_keypress_or_autorefresh", lambda *_args: next(keys))
    calls = []
    monkeypatch.setattr(qtop.subprocess, "call", lambda command, **_kwargs: calls.append(command) or 0)
    with pytest.raises(SystemExit) as stopped:
        runtime("-b", "slurm", "-s", str(scheduler_inputs), "-w", "1", "-T", save_only=False)
    assert stopped.value.code == 0
    assert runtime.views[-1].model.display_overrides["transpose_wn_matrices"] is True  # the frame after "t"
    assert len(calls) == 4
    assert calls[0] == calls[2] == ["/usr/bin/clear"]
    assert all(command[1].endswith(".out") for command in (calls[1], calls[3]))


@pytest.mark.parametrize(
    "override,key",
    [("transpose_wn_matrices='maybe'", "transpose_wn_matrices"), ("term_size=[0,80]", "term_size"), ("sorting={'user_sort': [] , 'reverse': 'maybe'}", "sorting.reverse")],
)
def test_invalid_cli_overrides_fail_with_key_before_collection(runtime, monkeypatch, capsys, override, key):
    monkeypatch.setattr(process_io, "execute_shell_batch_commands", lambda *args, **kwargs: pytest.fail("Invalid config must not execute scheduler commands"))
    code, _ = runtime("-b", "demo", "-o", override)
    assert code != 0
    assert key in capsys.readouterr().err


@pytest.mark.parametrize("anonymous", [False, True])
def test_watch_capture_closes_each_archive_before_key_handling(runtime, scheduler_inputs, monkeypatch, anonymous):
    keys = iter(("t", "q"))
    archives = []
    original = qtop.fileutils.init_sample_file

    def capture(*args, **kwargs):
        archive = original(*args, **kwargs)
        archives.append(archive)
        return archive

    def key(*args):
        assert archives and all(archive.closed for archive in archives)
        return next(keys)

    monkeypatch.setattr(qtop.fileutils, "init_sample_file", capture)
    monkeypatch.setattr(terminal_io, "wait_for_keypress_or_autorefresh", key)
    monkeypatch.setattr(qtop.subprocess, "call", lambda *args, **kwargs: 0)
    flags = ["-A", "-e"] if anonymous else []
    with pytest.raises(SystemExit) as stopped:
        runtime("-b", "slurm", "-s", str(scheduler_inputs), "-w", "1", "-L", *flags, save_only=False)
    assert stopped.value.code == 0
    assert len(archives) == 2 and all(archive.closed for archive in archives)


def test_replay_collection_uses_snapshot_without_constructing_a_scheduler(tmp_path):
    from types import SimpleNamespace

    state = {"jobs": [], "nodes": [], "queues": []}

    class OfflineBackend:
        @staticmethod
        def get_mnemonic():
            return "offline"

        def __init__(self, *args):
            pytest.fail("Replay must not construct a live backend")

    options = SimpleNamespace(load_sample="snapshot.tar", sample_snapshot={"state": state})
    config = {"savepath": str(tmp_path)}
    snapshot, prepared = qtop.collect_snapshot(options, config, OfflineBackend, {}, None)
    assert snapshot.to_cluster_state() == state
    assert prepared is config


def test_failed_export_removes_partial_file_and_does_not_notify_web(tmp_path):
    from types import SimpleNamespace
    from unittest.mock import Mock

    class FailedSnapshot:
        def save(self, path):
            Path(path).write_text("partial JSON")
            raise OSError("export failed")

    web = Mock()
    with pytest.raises(OSError, match="export failed"):
        qtop.export_snapshot(FailedSnapshot(), SimpleNamespace(EXPORT=True, WEB=True), str(tmp_path), "test", web)
    assert not list(tmp_path.iterdir())
    web.set_filename.assert_not_called()


def test_interrupted_export_never_shows_a_partial_snapshot(tmp_path):
    from types import SimpleNamespace
    from unittest.mock import Mock

    class InterruptedSnapshot:
        def save(self, path):
            Path(path).write_text("{")
            assert not list(tmp_path.glob("qtop_json_*.json")), "readers would see a partial snapshot"
            raise KeyboardInterrupt

    with pytest.raises(KeyboardInterrupt):
        qtop.export_snapshot(InterruptedSnapshot(), SimpleNamespace(EXPORT=True, WEB=False), str(tmp_path), "test", Mock())
    assert not list(tmp_path.iterdir())


@pytest.mark.parametrize("export, serve", [(False, False), (True, False), (False, True), (True, True)])
def test_export_switches_write_valid_json_and_only_notify_requested_web(tmp_path, export, serve):
    from types import SimpleNamespace
    from unittest.mock import Mock

    state = {"jobs": [], "nodes": [], "queues": []}
    web = Mock()
    qtop.export_snapshot(Snapshot.from_cluster_state(state), SimpleNamespace(EXPORT=export, WEB=serve), str(tmp_path), "test", web)
    paths = list(tmp_path.glob("qtop_json_*.json"))
    assert len(paths) == int(export or serve)
    if paths:
        assert json.loads(paths[0].read_text()) == state
    assert len(list(tmp_path.iterdir())) == len(paths)  # no .part left behind
    if serve:
        web.set_filename.assert_called_once_with(str(paths[0]))
    else:
        web.set_filename.assert_not_called()


def test_watch_wraps_before_paging_and_reflows_after_resize(runtime, scheduler_inputs, monkeypatch):
    sizes = iter(((8, 20), (8, 40)))
    keys = iter(("", "q"))
    monkeypatch.setattr(terminal_io, "calculate_term_size", lambda *_args: next(sizes))
    monkeypatch.setattr(terminal_io, "wait_for_keypress_or_autorefresh", lambda *_args: next(keys))
    pages = []

    def show(command, **_kwargs):
        if command[0] == "/bin/cat":
            pages.append(Path(command[1]).read_text())
        return 0

    monkeypatch.setattr(qtop.subprocess, "call", show)
    with pytest.raises(SystemExit) as stopped:
        runtime("-b", "slurm", "-s", str(scheduler_inputs), "-w", "1", "-c", "ON", save_only=False)
    assert stopped.value.code == 0 and len(pages) == 2
    for page, columns in zip(pages, (20, 40)):
        lines = re.sub(r"\x1b(?:\[[0-9;]*m|c)", "", page).splitlines()
        assert next(line for line in lines if line.strip()).startswith("./qtop.py")
        assert len(lines) == 7 and all(len(line) <= columns for line in lines)
    assert pages[0] != pages[1]


def test_legacy_five_field_snapshots_convert_to_a_cluster_state():
    """tools/validate_trace_export.py still builds Snapshot(...) from old five-field exports."""
    from collections import OrderedDict, namedtuple

    from qtop_py.core_cluster_state import validate_cluster_state
    from qtop_py.snapshot import JobRow

    QueueRow = namedtuple("QueueRow", ["lm", "queued", "run", "state"])
    nodes = [{"domainname": "wn1", "state": "-", "np": 2, "core_job_map": {0: "7"}, "qname": ["batch"]}]
    jobs = OrderedDict([("7", JobRow("alice", "R", "batch")), ("8", JobRow("bob", "Q", "batch"))])
    snapshot = Snapshot(nodes, jobs, OrderedDict([("batch", QueueRow("--", "1", "1", "Q"))]), 1, 1)
    state = snapshot.to_cluster_state()
    assert state["jobs"] == [{"id": "7", "user": "alice", "state": "R", "queue": "batch"}, {"id": "8", "user": "bob", "state": "Q", "queue": "batch"}]
    assert state["queues"] == [{"name": "batch", "limit": "--", "queued": 1, "running": 1, "state": "Q"}]
    assert state["nodes"] == nodes and state["nodes"] is not nodes
    assert validate_cluster_state(state) == state
