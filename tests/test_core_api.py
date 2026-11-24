## Copyright (c) 2026 qtop contributors
## SPDX-License-Identifier: MIT
"""core_api is the stable surface for scheduler backends; these tests pin it."""

import ast
from pathlib import Path
from types import SimpleNamespace

from qtop_py import core_api

ROOT = Path(__file__).resolve().parents[1]

PUBLIC = [
    "FileEmptyError",
    "GenericBatchSystem",
    "OperationalError",
    "StatExtractor",
    "TraceMasker",
    "build_cluster_state",
    "check_empty_file",
    "open_input",
    "resolve_job_id",
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

    options = SimpleNamespace(SOURCEDIR=None)
    state = TinyBackend({"jobs": str(trace)}, {}, options).get_cluster_state(options)
    assert core_api.validate_cluster_state(state) == state
    assert [job["user"] for job in state["jobs"]] == ["alice", "bob"]
