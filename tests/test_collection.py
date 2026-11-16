## Copyright (c) 2026 qtop contributors
## SPDX-License-Identifier: MIT
import os
import subprocess
import sys
import threading
from pathlib import Path
from types import SimpleNamespace

import pytest

from qtop_py import qtop, utils, yaml_parser
from qtop_py.errors import OperationalError
from qtop_py.plugins.slurm import SlurmBatchSystem
from qtop_py.plugins.slurm_json import SlurmJSONBatchSystem
from qtop_py.process import communicate_with_timeout
from qtop_py.runtime import RunResources
from qtop_py.schedulers.collection import CollectionPoller

ROOT = Path(__file__).resolve().parents[1]
SAMPLES = ROOT / "tests/plugins/slurm_json_samples/v0.0.43"


def test_twenty_second_cadence_failure_retention_and_recovery(tmp_path):
    now, calls = [100.0], []

    def collect(directory, cancel):
        calls.append(now[0])
        if len(calls) == 2:
            raise ValueError("malformed next response")
        path = Path(directory) / "frame.json"
        path.write_text(str(len(calls)))
        return {"generation": len(calls)}, None, None, {"jobs": str(path)}

    poller = CollectionPoller(collect, 20, clock=lambda: now[0])
    first, _ = poller.read()
    for second in (100, 101, 110, 119.999):
        now[0] = second
        assert not poller.refresh()
        assert poller.read() == (first, "")
    now[0] = 120
    assert poller.refresh()
    observed, error = poller.read()
    assert observed is first and "malformed" in error
    assert observed.traces["jobs"] == ("frame.json", b"1")
    now[0] = 140
    assert poller.refresh()
    observed, error = poller.read()
    assert observed.state == {"generation": 3} and not error
    assert calls == [100, 120, 140]
    poller.close()
    now[0] = 200
    assert not poller.refresh()


def test_slow_collection_skips_missed_slots_without_burst():
    now = [0.0]

    def collect(directory, cancel):
        now[0] += 45
        return {}, None, None, {}

    poller = CollectionPoller(collect, 20, clock=lambda: now[0])
    assert poller.refresh()
    assert poller.deadline == 100
    assert not poller.refresh()
    poller.close()


def test_initial_failure_never_publishes_an_observation():
    def collect(directory, cancel):
        raise OperationalError("no first snapshot")

    with pytest.raises(OperationalError, match="no first"):
        CollectionPoller(collect, 20)


def test_background_refresh_does_not_need_any_ui_reads():
    ready = threading.Event()
    count = []

    def collect(directory, cancel):
        count.append(1)
        if len(count) > 1:
            ready.set()
        return {}, None, None, {}

    poller = CollectionPoller(collect, 0.01)
    try:
        poller.start()
        poller.start()
        assert ready.wait(5)
    finally:
        poller.close()
    assert not poller.thread.is_alive()


def test_concurrent_refreshes_do_not_overlap_or_publish_partial_state():
    now, entered, release = [0.0], threading.Event(), threading.Event()
    calls = []

    def collect(directory, cancel):
        calls.append(1)
        if len(calls) > 1:
            entered.set()
            assert release.wait(5)
        return {"complete": len(calls)}, None, None, {}

    poller = CollectionPoller(collect, 20, clock=lambda: now[0])
    now[0] = 20
    first = threading.Thread(target=poller.refresh)
    second = threading.Thread(target=poller.refresh)
    first.start()
    assert entered.wait(5)
    second.start()
    assert poller.read()[0].state == {"complete": 1}
    release.set()
    first.join(5)
    second.join(5)
    poller.close()
    assert calls == [1, 1]
    assert poller.read()[0].state == {"complete": 2}


@pytest.mark.parametrize("child_exits", [False, True], ids=["running-client", "inherited-pipes"])
@pytest.mark.skipif(os.name != "posix", reason="process-group termination is POSIX-specific")
def test_cancellation_reaps_clients_and_inherited_pipe_holders(child_exits):
    code = "import time; time.sleep(60)"
    if child_exits:
        code = "import os,time; pid=os.fork(); time.sleep(60) if pid==0 else None"
    process = subprocess.Popen([sys.executable, "-c", code], stdout=subprocess.PIPE, stderr=subprocess.PIPE, start_new_session=True)
    cancel = threading.Event()
    timer = threading.Timer(0.1, cancel.set)
    timer.start()
    try:
        with pytest.raises(OperationalError, match="cancelled"):
            communicate_with_timeout(process, 10, cancel=cancel)
    finally:
        cancel.set()
        timer.join()
        if process.poll() is None:
            process.kill()
        process.wait()
    assert process.stdout.closed and process.stderr.closed


@pytest.fixture
def frame_context(tmp_path, monkeypatch):
    monkeypatch.setattr(sys, "argv", ["qtop", "-b", "slurm_json"])
    args = utils.parse_qtop_cmdline_args()
    args.workdir = str(tmp_path)
    config = yaml_parser.parse(str(ROOT / "qtopconf.yaml"))
    config["savepath"] = str(tmp_path)
    resources = RunResources()
    monkeypatch.setattr(resources, "start_capture", lambda *args: None)
    paths = SimpleNamespace(qtoppath=str(ROOT))

    def frame():
        return qtop._collect_frame(resources, args, config, {}, qtop.discover_qtop_batch_systems(), str(tmp_path), paths, "sample%(datetime)s.tar")

    yield frame, args, config, resources
    resources.stop_collection()


def test_json_collection_is_ordered_and_cached_between_redraws(frame_context, monkeypatch):
    frame, _args, _config, resources = frame_context
    calls = []

    def execute(commands, filenames, key, savepath, **kwargs):
        calls.append(key)
        assert kwargs["timeout"] == 120
        assert "cancel" in kwargs
        assert Path(kwargs["cwd"]) == Path(_config["savepath"])
        assert Path(filenames[key]).parent == Path(savepath)
        Path(filenames[key]).write_bytes((SAMPLES / ("slurm_" + key[:-5] + ".json")).read_bytes())
        return filenames[key]

    monkeypatch.setattr(qtop, "execute_shell_batch_commands", execute)
    first = frame()[0]
    for _ in range(5):
        assert frame()[0].to_cluster_state() == first.to_cluster_state()
    assert calls == ["partitions_json", "nodes_json", "jobs_json"]
    assert first._collection_age >= 0
    resources.poller.deadline = 0
    resources.poller.refresh()
    assert calls == ["partitions_json", "nodes_json", "jobs_json"] * 2


def test_empty_jobs_falls_back_once_and_later_failure_retains_legacy_snapshot(frame_context, monkeypatch, caplog):
    frame, _args, _config, resources = frame_context
    calls = []

    def fetch(options, configuration, backend):
        calls.append(backend.get_mnemonic())
        if len(calls) > 2:
            raise OperationalError("next RPC failed")
        if backend is SlurmJSONBatchSystem:
            paths = {key + "_json": str(SAMPLES / ("slurm_" + key + ".json")) for key in ("partitions", "nodes", "jobs")}
            blank = Path(options.workdir) / "blank.json"
            blank.write_text("")
            paths["jobs_json"] = str(blank)
            return paths
        base = ROOT / "tests/plugins/slurm_samples/basic"
        return {"squeue_file": str(base / "squeue.txt"), "sinfo_file": str(base / "sinfo.txt")}

    monkeypatch.setattr(qtop, "fetch_scheduler_files", fetch)
    snapshot, _, selected, _, _ = frame()
    assert selected is SlurmBatchSystem
    assert "falling back to slurm" in caplog.text
    resources.poller.deadline = 0
    resources.poller.refresh()
    assert frame()[0].to_cluster_state() == snapshot.to_cluster_state()
    assert calls == ["slurm_json", "slurm", "slurm"]
    assert "next RPC failed" in frame()[0]._collection_error


def test_autodetection_prefers_json_but_explicit_legacy_is_preserved(monkeypatch):
    config = yaml_parser.parse(str(ROOT / "qtopconf.yaml"))
    systems = qtop.discover_qtop_batch_systems()
    monkeypatch.setattr(qtop.shutil, "which", lambda command: "/bin/" + command)
    assert qtop.auto_get_avail_batch_system(config, systems) == "slurm_json"
    assert qtop.decide_batch_system("slurm", None, "auto", config["schedulers"], systems, config) == "slurm"


def test_live_capture_copies_the_published_trace_set_without_reparsing(frame_context, monkeypatch):
    frame, args, _config, resources = frame_context
    args.SAMPLE = 1
    calls = []

    def execute(commands, filenames, key, savepath, **kwargs):
        calls.append(key)
        Path(filenames[key]).write_bytes((SAMPLES / ("slurm_" + key[:-5] + ".json")).read_bytes())
        return filenames[key]

    captured = []
    monkeypatch.setattr(qtop, "execute_shell_batch_commands", execute)
    monkeypatch.setattr(resources, "start_capture", lambda savepath, filename, traces, *rest: captured.append({key: Path(path).read_bytes() for key, path in traces.items()}))
    frame()
    frame()
    assert len(calls) == 3 and captured[0] == captured[1]
    assert set(captured[0]) == {"partitions_json", "nodes_json", "jobs_json"}


def test_collection_process_has_an_explicit_working_directory(tmp_path):
    import shlex

    working = tmp_path / "working"
    working.mkdir()
    (working / "marker").write_text("correct RPC directory")
    output = tmp_path / "traces"
    output.mkdir()
    target = output / "trace"
    command = shlex.quote(sys.executable) + " -c " + shlex.quote("from pathlib import Path; print(Path('marker').read_text())")
    qtop.execute_shell_batch_commands({"trace": command}, {"trace": str(target)}, "trace", str(output), cancel=threading.Event(), cwd=str(working))
    assert target.read_text().strip() == "correct RPC directory"


def test_stale_messages_show_age_and_escape_the_failure(capsys):
    from qtop_py.ui.mvu_view import TextDisplay

    display = SimpleNamespace(
        MVU_model=SimpleNamespace(backend=SlurmJSONBatchSystem),
        cluster=SimpleNamespace(worker_nodes=[]),
        config={},
        snapshot=SimpleNamespace(_collection_age=25, _collection_span=2, _collection_error="RPC failed\x1b[2J"),
    )
    TextDisplay.display_backend_messages(display)
    output = capsys.readouterr().out
    assert "25.0s" in output and "collection span 2.0s" in output and "not globally atomic" in output
    assert "stale data" in output and "\x1b" not in output


def test_close_cancels_a_worker_without_reporting_shutdown_as_failure():
    entered = threading.Event()
    count = []

    def collect(directory, cancel):
        count.append(1)
        if len(count) > 1:
            entered.set()
            assert cancel.wait(5)
            raise OperationalError("Scheduler collection cancelled")
        return {}, None, None, {}

    poller = CollectionPoller(collect, 20)
    poller.deadline = 0
    poller.start()
    try:
        assert entered.wait(5)
    finally:
        poller.close()
    assert not poller.thread.is_alive() and poller.read()[1] == ""
