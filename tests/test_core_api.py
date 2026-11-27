## Copyright (c) 2026 qtop contributors
## SPDX-License-Identifier: MIT
"""core_api is the stable surface for scheduler backends; these tests pin it."""

import ast
import json
import os
from pathlib import Path
from types import SimpleNamespace

from qtop_py import core_api
from qtop_py.schedulers import collector

ROOT = Path(__file__).resolve().parents[1]

PUBLIC = [
    "BackendOptions",
    "Collection",
    "FileEmptyError",
    "GenericBatchSystem",
    "OperationalError",
    "SchedulerInputError",
    "StatExtractor",
    "TraceMasker",
    "build_cluster_state",
    "check_empty_file",
    "collect",
    "collect_state",
    "command_env",
    "command_lines",
    "open_input",
    "resolve_job_id",
    "run_commands",
    "trace_files",
    "validate_cluster_state",
]


def test_public_surface_is_pinned():
    """Changing the surface is an API change: update PUBLIC, docs/architecture.md and the CHANGELOG."""
    assert sorted(core_api.__all__) == PUBLIC
    assert all(hasattr(core_api, name) for name in PUBLIC)


def test_builtin_backends_take_the_shared_surface_from_core_api():
    """Backends may import their own helpers, but the shared surface only through core_api."""
    shared = {"qtop_py.core_inputs", "qtop_py.core_serialiser", "qtop_py.core_errors", "qtop_py.core_source_trace"}  # types: core_state_types
    for path in sorted((ROOT / "qtop_py" / "plugins").glob("*.py")):
        for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
            if isinstance(node, ast.ImportFrom) and node.module:
                assert node.module not in shared, "%s: import %s from qtop_py.core_api" % (path.name, node.module)
                if node.module == "qtop_py":
                    assert not {alias.name for alias in node.names} & {name.rsplit(".", 1)[1] for name in shared}, path.name


def test_qtop_collects_through_core_api_like_any_library_caller():
    """qtop is the reference library caller: its collector takes what core_api offers from core_api."""
    offered = {"qtop_py.core_collect", "qtop_py.core_cluster_state", "qtop_py.core_serialiser", "qtop_py.core_errors", "qtop_py.core_inputs"}
    tree = ast.parse((ROOT / "qtop_py" / "schedulers" / "collector.py").read_text(encoding="utf-8"))
    imported = {node.module for node in ast.walk(tree) if isinstance(node, ast.ImportFrom) and node.module}
    imported |= {alias.name for node in ast.walk(tree) if isinstance(node, ast.Import) for alias in node.names}
    assert not imported & offered, "collector.py: import these from qtop_py.core_api"
    calls = {node.func.attr for node in ast.walk(tree) if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and getattr(node.func.value, "id", "") == "core_api"}
    assert {"collect", "command_lines", "run_commands", "trace_files"} <= calls


def test_collect_returns_the_backend_object_and_its_traces(tmp_path):
    (tmp_path / "jobs.txt").write_text("7 alice R batch node1\n")
    built = core_api.collect(_TableBackend, str(tmp_path), core_api.BackendOptions(SOURCEDIR=str(tmp_path)))
    assert isinstance(built.system, _TableBackend) and built.files == {"jobs": str(tmp_path / "jobs.txt")}
    again = core_api.collect(_TableBackend, str(tmp_path), core_api.BackendOptions(), files=built.files)  # traces in hand: no commands, no env
    assert again.state == built.state


def test_a_backend_written_against_core_api_alone_yields_a_valid_state(tmp_path):
    trace = tmp_path / "jobs.txt"
    trace.write_text("7 alice R batch node1\n8 bob Q batch -\n", encoding="utf-8")

    class TinyBackend(core_api.GenericBatchSystem):
        @staticmethod
        def get_mnemonic():
            return "tiny"

        def __init__(self, files, config, options):
            self.options = options
            core_api.check_empty_file(files["jobs"])
            with core_api.open_input(files["jobs"], options=options) as stream:
                self.rows = [line.split() for line in stream if line.strip()]

        def get_jobs_info(self):
            return [r[0] for r in self.rows], [r[1] for r in self.rows], [r[2] for r in self.rows], [r[3] for r in self.rows]

        def get_queues_info(self):
            return 1, 1, [{"queue_name": "batch", "run": "1", "queued": "1", "lm": "--", "state": "Q"}]

        def get_worker_nodes(self, job_ids, job_queues, options):
            return [{"domainname": "node1", "state": "-", "np": 2, "core_job_map": {0: "7"}, "qname": ["batch"]}]

    state = TinyBackend({"jobs": str(trace)}, {}, core_api.BackendOptions()).get_cluster_state(core_api.BackendOptions())
    assert core_api.validate_cluster_state(state) == state
    assert [job["user"] for job in state["jobs"]] == ["alice", "bob"]


def test_one_shot_anonymize_masks_the_built_state_once():
    """qtop -A without -w or -L masks in the collector, as watch mode does; backends see raw names."""
    raw = {
        "jobs": [{"id": "7", "user": "alice", "state": "R", "queue": "batch"}],
        "nodes": [{"domainname": "node1", "state": "-", "np": 1, "core_job_map": {"0": "7"}, "qname": ["batch"]}],
        "queues": [{"name": "batch", "limit": "--", "running": 1, "queued": 0, "state": "Q"}],
    }

    class RawBackend:
        @staticmethod
        def get_mnemonic():
            return "raw"

        def __init__(self, files, config, options):
            assert not hasattr(options, "ANONYMIZE") or options.ANONYMIZE  # it may be set; backends ignore it

        def get_cluster_state(self, options):
            return json.loads(json.dumps(raw))

    args = SimpleNamespace(load_sample=None, SAMPLE=0, ANONYMIZE=True, workdir="")
    snapshot, _config = collector.MVU_collect_snapshot(args, {"savepath": ""}, RawBackend, {}, None)
    text = json.dumps(snapshot.to_cluster_state())
    assert not any(name in text for name in ("alice", "node1", "batch"))


def test_collect_state_runs_a_backends_own_commands_without_the_cli(tmp_path):
    """The library entry point: no command line, configuration file or terminal."""
    from qtop_py.plugins.demo import DemoBatchSystem

    state = core_api.collect_state(DemoBatchSystem, str(tmp_path), env=os.environ)
    assert core_api.validate_cluster_state(state) == state and state["nodes"] and state["jobs"]


class _TableBackend(core_api.GenericBatchSystem):
    """One trace, 'id user state queue node' per line; the command is never needed with SOURCEDIR."""

    default_commands = {"jobs": "%(savepath)s/jobs%(pid)s.txt, false"}

    @staticmethod
    def get_mnemonic():
        return "table"

    def __init__(self, files, config, options):
        with core_api.open_input(files["jobs"], options=options) as stream:
            self.rows = [line.split() for line in stream if line.strip()]

    def get_jobs_info(self):
        return [r[0] for r in self.rows], [r[1] for r in self.rows], [r[2] for r in self.rows], [r[3] for r in self.rows]

    def get_queues_info(self):
        return 1, 0, [{"queue_name": "batch", "run": "1", "queued": "0", "lm": "--", "state": "Q"}]

    def get_worker_nodes(self, job_ids, job_queues, options):
        return [{"domainname": "node1", "state": "-", "np": 1, "core_job_map": {0: "7"}, "qname": ["batch"]}]


def test_collect_state_reads_a_source_directory_without_running_commands(tmp_path):
    (tmp_path / "jobs.txt").write_text("7 alice R batch node1\n")
    state = core_api.collect_state(_TableBackend, str(tmp_path), core_api.BackendOptions(SOURCEDIR=str(tmp_path)))
    assert [(job["id"], job["user"]) for job in state["jobs"]] == [("7", "alice")]


def test_collect_state_reads_source_traces_outside_the_workdir(tmp_path):
    source, workdir = tmp_path / "traces", tmp_path / "work"
    source.mkdir()
    workdir.mkdir()
    (source / "jobs.txt").write_text("7 alice R batch node1\n")
    state = core_api.collect_state(_TableBackend, str(workdir), core_api.BackendOptions(SOURCEDIR=str(source)))
    assert [job["user"] for job in state["jobs"]] == ["alice"]


def test_collect_state_reports_a_failing_required_command(tmp_path):
    import pytest

    with pytest.raises(core_api.OperationalError, match="failed"):
        core_api.collect_state(_TableBackend, str(tmp_path), env=os.environ)  # runs "false"
    with pytest.raises(ValueError, match="env="):
        core_api.collect_state(_TableBackend, str(tmp_path))
