## Copyright (c) 2026 qtop contributors
## SPDX-License-Identifier: MIT
import contextlib
import io
import os
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

from qtop_py import configuration, fileutils, utils
from qtop_py import core_inputs as inputs
from qtop_py import core_process as process_io
from qtop_py import core_yaml_parser as yaml_parser
from qtop_py.core_errors import OperationalError
from qtop_py.core_process import communicate_with_timeout
from qtop_py.plugins.slurm import SlurmBatchSystem
from qtop_py.plugins.slurm_json import SlurmJSONBatchSystem
from qtop_py.runtime import RunResources
from qtop_py.schedulers import collector, registry
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


def _dead(pid):
    """True once pid is gone or a zombie; a container's PID 1 may never reap it, and root never gets EPERM."""
    try:
        os.kill(pid, 0)
    except (ProcessLookupError, PermissionError):
        return True
    try:
        with open("/proc/%d/stat" % pid) as stat_file:
            return stat_file.read().rsplit(")", 1)[1].split()[0] == "Z"
    except OSError:
        state = subprocess.run(["ps", "-o", "stat=", "-p", str(pid)], stdout=subprocess.PIPE, check=False).stdout
        return not state.strip() or state.strip().startswith(b"Z")


@pytest.mark.skipif(os.name != "posix", reason="process groups are POSIX-specific")
def test_group_stragglers_that_escaped_the_first_kill_are_killed():
    """A child forked while killpg() is delivered can miss it; once the leader is reaped, kill the rest."""
    code = "import os,time\nif os.fork()==0:\n    print(os.getpid(),flush=True); time.sleep(60)"
    leader = subprocess.Popen([sys.executable, "-c", code], start_new_session=True, stdout=subprocess.PIPE)
    child = int(leader.stdout.readline())
    leader.wait(10)  # the leader exits at once; its child stays in the group, as if it escaped the kill
    try:
        assert not _dead(child)
        process_io._kill_group_stragglers(leader)
        deadline = time.monotonic() + 5
        while not _dead(child) and time.monotonic() < deadline:
            time.sleep(0.01)
        assert _dead(child)
    finally:
        leader.stdout.close()
        with contextlib.suppress(ProcessLookupError, PermissionError):
            os.killpg(leader.pid, signal.SIGKILL)


@pytest.fixture
def frame_context(tmp_path, monkeypatch):
    monkeypatch.setattr(sys, "argv", ["qtop", "-b", "slurm_json"])
    args = utils.parse_qtop_cmdline_args()
    args.workdir = str(tmp_path)
    config = configuration.with_backend_defaults(yaml_parser.parse(str(ROOT / "qtopconf.yaml")))
    config["savepath"] = str(tmp_path)
    resources = RunResources()
    monkeypatch.setattr(resources, "start_capture", lambda *args: None)
    paths = SimpleNamespace(qtoppath=str(ROOT))

    def frame():
        return collector._collect_frame(resources, args, config, {}, registry.discover_qtop_batch_systems(), str(tmp_path), paths, "sample%(datetime)s.tar")

    yield frame, args, config, resources
    resources.stop_collection()


def test_json_collection_is_ordered_and_cached_between_redraws(frame_context, monkeypatch):
    frame, _args, _config, resources = frame_context
    calls = []

    def execute(commands, filenames, key, savepath, **kwargs):
        calls.append(key)
        assert kwargs["timeout"] == 180
        assert "cancel" in kwargs
        assert Path(kwargs["cwd"]) == Path(_config["savepath"])
        assert Path(filenames[key]).parent == Path(savepath)
        assert Path(savepath).parent == Path(_config["savepath"])
        Path(filenames[key]).write_bytes((SAMPLES / ("slurm_" + key[:-5] + ".json")).read_bytes())
        return filenames[key]

    monkeypatch.setattr(process_io, "execute_shell_batch_commands", execute)
    first = frame()[0]
    for _ in range(5):
        assert frame()[0].to_cluster_state() == first.to_cluster_state()
    assert sorted(calls) == ["jobs_json", "nodes_json", "partitions_json"]
    assert first._collection_age >= 0
    resources.poller.deadline = 0
    resources.poller.refresh()
    assert sorted(calls) == sorted(["jobs_json", "nodes_json", "partitions_json"] * 2)


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

    monkeypatch.setattr(collector, "fetch_scheduler_files", fetch)
    snapshot, _, selected, _, _ = frame()
    assert selected is SlurmBatchSystem
    assert "falling back to slurm" in caplog.text
    resources.poller.deadline = 0
    resources.poller.refresh()
    assert frame()[0].to_cluster_state() == snapshot.to_cluster_state()
    assert calls == ["slurm_json", "slurm", "slurm"]
    assert "next RPC failed" in frame()[0]._collection_error


def test_autodetection_prefers_legacy_but_explicit_json_is_preserved(monkeypatch):
    config = configuration.with_backend_defaults(yaml_parser.parse(str(ROOT / "qtopconf.yaml")))
    systems = registry.discover_qtop_batch_systems()
    monkeypatch.setattr(registry.shutil, "which", lambda command: "/bin/" + command)
    assert registry.auto_get_avail_batch_system(config, systems) == "slurm"
    assert registry.decide_batch_system("slurm", None, "auto", config["schedulers"], systems, config) == "slurm"
    assert registry.decide_batch_system("slurm_json", None, "auto", config["schedulers"], systems, config) == "slurm_json"


def test_live_capture_copies_the_published_trace_set_without_reparsing(frame_context, monkeypatch):
    frame, args, _config, resources = frame_context
    args.SAMPLE = 1
    calls = []

    def execute(commands, filenames, key, savepath, **kwargs):
        calls.append(key)
        Path(filenames[key]).write_bytes((SAMPLES / ("slurm_" + key[:-5] + ".json")).read_bytes())
        return filenames[key]

    captured = []
    monkeypatch.setattr(process_io, "execute_shell_batch_commands", execute)
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
    process_io.execute_shell_batch_commands({"trace": command}, {"trace": str(target)}, "trace", str(output), cancel=threading.Event(), cwd=str(working), env=os.environ)
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


@pytest.mark.parametrize("all_partitions", (False, True), ids=("visible", "all"))
def test_slurm_summary_recollects_each_cycle_without_a_poller(frame_context, monkeypatch, all_partitions):
    frame, args, _config, resources = frame_context
    args.BATCH_SYSTEM, args.SLURM_SUMMARY, args.ALL_PARTITIONS, args.WATCH = "slurm", True, all_partitions, 1
    calls = []
    source = ROOT / "tests/plugins/slurm_samples/cpu_allocations"

    def execute(commands, filenames, key, savepath, **kwargs):
        assert key in ("squeue_file", "sinfo_file")
        assert (" --all" in commands[key]) == all_partitions
        calls.append(key)
        text = (source / (key[:-5] + ".txt")).read_text()
        if len(calls) > 2:
            text = text.replace("alice", "changed")
        Path(filenames[key]).write_text(text)
        return filenames[key]

    monkeypatch.setattr(process_io, "execute_shell_batch_commands", execute)
    states = [frame()[0].to_cluster_state() for _ in range(3)]
    assert sorted(calls) == sorted(["squeue_file", "sinfo_file"] * 3)
    assert resources.poller is None
    assert states[0]["jobs"][0]["user"] == "alice"
    assert states[1]["jobs"][0]["user"] == states[2]["jobs"][0]["user"] == "changed"
    assert states[0]["nodes"][0]["cpu_allocation_source"] == "estimated"
    assert "slurm_gpu_bindings" not in states[0]["nodes"][0]


@pytest.mark.parametrize("fails", [False, True], ids=["success", "command-failure"])
def test_startup_progress_observes_live_stream_before_waiting(tmp_path, monkeypatch, fails):
    output, processes = io.StringIO(), []
    options = SimpleNamespace(_input_progress=fileutils.StartupProgress(output))
    destination = tmp_path / "jobs.txt"

    def communicate(process, *args, **kwargs):
        assert output.getvalue() == "."  # visible while the RPC is in flight
        assert not process.stdout.closed
        processes.append(process)
        return communicate_with_timeout(process, *args, **kwargs)

    monkeypatch.setattr(process_io, "communicate_with_timeout", communicate)
    import shlex

    command = shlex.quote(sys.executable) + " -c " + shlex.quote("print('jobs'); raise SystemExit(%s)" % int(fails))
    try:
        process_io.execute_shell_batch_commands({"jobs": command}, {"jobs": str(destination)}, "jobs", str(tmp_path), input_options=options, env=os.environ)
    except OperationalError:
        assert fails
    else:
        assert not fails
        with inputs.open_input(str(destination), options=options) as source:
            assert source.read() == "jobs\n"
    assert output.getvalue() == ".."  # parsing the saved response adds no dots
    assert processes[0].stdout.closed and processes[0].stderr.closed


@pytest.mark.parametrize("fails", [False, True], ids=["success", "failure"])
def test_collection_workspace_uses_savepath_without_fallback(tmp_path, monkeypatch, fails):
    parent = tmp_path / "configured"
    parent.mkdir()
    elsewhere = tmp_path / "system-temp"
    elsewhere.mkdir()
    monkeypatch.setattr("tempfile.tempdir", str(elsewhere))
    now, paths = [0.0], []

    def collect(directory, cancel):
        path = Path(directory)
        paths.append(path)
        assert path.parent == parent
        assert path.stat().st_mode & 0o777 == 0o700
        (path / "partial").write_text("data")
        if fails and len(paths) > 1:
            raise OSError("workspace full")
        return {"generation": len(paths)}, None, None, {}

    poller = CollectionPoller(collect, 20, clock=lambda: now[0], SAVEPATH=str(parent))
    original = poller.read()[0]
    now[0] = 20
    assert poller.refresh()
    assert poller.read()[0].state == (original.state if fails else {"generation": 2})
    assert all(not path.exists() for path in paths)
    assert list(parent.iterdir()) == list(elsewhere.iterdir()) == []
    poller.close()
    parent.rmdir()
    with pytest.raises(FileNotFoundError):
        CollectionPoller(collect, 20, SAVEPATH=str(parent))
    assert list(elsewhere.iterdir()) == []


def _timed_commands(tmp_path, scripts):
    import shlex
    import time

    commands = {}
    for key, script in scripts.items():
        commands[key] = (str(tmp_path / (key + ".txt")), " ".join(shlex.quote(part) for part in (sys.executable, "-c", script)))
    args = SimpleNamespace(SOURCEDIR=None)
    start = time.monotonic()
    try:
        return collector.get_input_filenames(commands, {"savepath": str(tmp_path), "scheduler_command_timeout": 30}, args, SlurmBatchSystem), time.monotonic() - start
    except OperationalError as error:
        return error, time.monotonic() - start


def test_scheduler_commands_run_concurrently_and_keep_config_order(tmp_path):
    """Each command waits until all three have started: run one after another, the first gives up and fails."""
    keys = ("squeue_file", "sinfo_file", "scontrol_file")
    barrier = tmp_path / "barrier"
    barrier.mkdir()
    rendezvous = (
        "import pathlib, sys, time\n"
        "barrier = pathlib.Path(%r)\n"
        "(barrier / '%s').touch()\n"
        "deadline = time.monotonic() + 20\n"
        "while len(list(barrier.iterdir())) < 3:\n"
        "    if time.monotonic() > deadline:\n"
        "        sys.exit('ran alone')\n"
        "    time.sleep(0.01)\n"
        "print('%s')\n"
    )
    files, _ = _timed_commands(tmp_path, {key: rendezvous % (str(barrier), key, key) for key in keys})
    assert not isinstance(files, OperationalError), files
    assert list(files) == list(keys)
    assert all(Path(path).read_text() == key + "\n" for key, path in files.items())


def test_the_first_required_failure_in_config_order_is_reported(tmp_path):
    late_first = "import sys, time; time.sleep(0.4); sys.exit(3)"
    error, _ = _timed_commands(tmp_path, {"squeue_file": late_first, "sinfo_file": "import sys; sys.exit(4)"})
    assert isinstance(error, OperationalError)
    assert "exit 3" in str(error)  # squeue comes first in config order, although sinfo failed sooner


def test_a_failed_optional_command_is_dropped_while_the_rest_complete(tmp_path):
    files, _ = _timed_commands(tmp_path, {"squeue_file": "print('q')", "sinfo_file": "print('n')", "scontrol_file": "import sys; sys.exit(1)"})
    assert sorted(files) == ["sinfo_file", "squeue_file"]


@pytest.mark.parametrize("demanding", (False, True), ids=("default", "demanding"))
def test_live_state_is_validated_once_unless_demanding(frame_context, monkeypatch, demanding):
    from qtop_py import core_cluster_state as cluster_state
    from qtop_py import core_collect, snapshot

    frame, args, _config, _resources = frame_context
    args.DEMANDING = demanding

    def execute(commands, filenames, key, savepath, **kwargs):
        Path(filenames[key]).write_bytes((SAMPLES / ("slurm_" + key[:-5] + ".json")).read_bytes())
        return filenames[key]

    calls = []
    real_validate = cluster_state.validate_cluster_state

    def counting_validate(state):
        calls.append(1)
        return real_validate(state)

    monkeypatch.setattr(process_io, "execute_shell_batch_commands", execute)
    monkeypatch.setattr(cluster_state, "validate_cluster_state", counting_validate)
    monkeypatch.setattr(core_collect, "validate_cluster_state", counting_validate)  # core_api.collect()
    monkeypatch.setattr(snapshot, "validate_cluster_state", counting_validate)
    frame()
    assert len(calls) == (2 if demanding else 1)
