##
## qtop is a tool to monitor queuing systems - https://github.com/qtop/qtop
##
## Copyright (c) 2016 Fotis Georgatos
## Copyright (c) 2016 Sotiris Fragkiskos
## Copyright (c) 2023 Hewlett Packard Enterprise Development LP
##
## SPDX-License-Identifier: MIT
##
"""Shared scheduler parsing: the StatExtractor base and the GenericBatchSystem interface."""

import re
from typing import TYPE_CHECKING, Any, Collection, Dict, Mapping, Optional, Sequence, Tuple, cast

if TYPE_CHECKING:
    from typing import List

    from qtop_py.core_source_trace import TraceMasker
    from qtop_py.core_state_types import ClusterState, Config, JobsInfo, Node, Options, QueuesInfo

from qtop_py.core_cluster_state import build_cluster_state
from qtop_py.schedulers.extensions import ExtensionSpec
from qtop_py.schedulers.metadata import node_is_idle


def resolve_job_id(job_id: str, known_ids: Collection[str]) -> str:
    """Prefer exact array tasks; use a reported parent only when necessary."""
    if job_id in known_ids:
        return job_id
    parent = re.sub(r"\[[0-9]*\]$", "", job_id)
    for candidate in (parent, parent + "[]"):
        if candidate in known_ids:
            return candidate
    return job_id


class StatExtractor(object):
    """Extract statistics using scheduler-specific input parsers."""

    def __init__(self, config: "Config", options: "Options") -> None:
        self.config = config
        self.options = options


class GenericBatchSystem(object):
    """The interface each scheduler backend implements, with defaults for the optional hooks."""

    optional_trace_keys: Tuple[str, ...] = ()
    optional_trace_aliases: Dict[str, str] = {}
    optional_trace_warning = "Optional scheduler details unavailable."
    detection_priority = 0  # -b auto tries lower values first; ties keep the configuration's order
    is_simulated = False
    # Each backend declares its own defaults; schedulers.registry reads only what a class declares
    # itself, and the configuration layers override them per key (qtopconf.yaml schedulers: etc.).
    signature_command: Optional[str] = None  # a command whose presence means this scheduler (-b auto)
    default_commands: Mapping[str, str] = {}  # trace key -> "savepath file template, command"
    state_abbreviations: Mapping[str, str] = {}  # scheduler job state -> qtop state name
    # A backend whose records carry extra fields declares their ExtensionSpec here; metadata collects
    # the specs of all registered backends, and node_job_fields comes from the spec, not a second copy.
    extensions: Optional[ExtensionSpec] = None
    node_job_fields: Tuple[str, ...] = ()
    blocked_queue_note = ""
    native_queue_membership = False
    node_is_idle = staticmethod(node_is_idle)
    classic_report_name = "qtop"
    classic_job_count_source = ""
    classic_account_note = "  ('all' includes every reported job state)"

    @staticmethod
    def collection_command(key: str, command: str, options: "Options") -> Optional[str]:
        """Return the command collecting trace key, or None to omit that input."""
        return command

    @staticmethod
    def queue_is_blocked(state: object) -> bool:
        """Return True if the queue state means disabled or stopped."""
        state = str(state)
        return state.startswith("D") or state.endswith("S")

    @staticmethod
    def node_state_marker(node: "Node") -> str:
        """Return an extra node-state symbol for the matrix; none by default."""
        return ""

    @staticmethod
    def display_messages(section: str, snapshot: object, nodes: "List[Node]", config: "Config") -> "List[str]":
        """Return severity-prefixed display lines for the scheduler messages block."""
        return []

    def mask_source_trace(self, key: str, text: str, mask: "TraceMasker") -> "Tuple[str, str]":
        """Return the archive name and masked text of a trace; raise ValueError if unsupported."""
        raise ValueError("This backend has no source trace masker")

    def get_queues_info(self) -> "QueuesInfo":
        """Return the running and queued job totals and the queue records."""
        raise NotImplementedError

    def get_worker_nodes(self, job_ids: "List[str]", job_queues: "List[str]", options: "Options") -> "List[Node]":
        """Return the worker nodes, given the job IDs and their queues."""
        raise NotImplementedError

    def get_jobs_info(self) -> "JobsInfo":
        """Return the job IDs, users, states and queues, as parallel lists."""
        raise NotImplementedError

    @staticmethod
    def get_mnemonic() -> str:
        """Return the scheduler name used in the configuration and on the command line."""
        raise NotImplementedError

    def get_cluster_state(self, options: "Options") -> "ClusterState":
        """Collect scheduler data behind one stable, plain-data boundary."""
        job_ids, users, job_states, job_queues = self.get_jobs_info()
        _running, _queued, queues = self.get_queues_info()
        nodes = self.get_worker_nodes(job_ids, job_queues, options)
        return build_cluster_state(job_ids, users, job_states, job_queues, nodes, queues)

    @classmethod
    def ensure_worker_nodes_have_qnames(cls, _worker_nodes: "Sequence[Mapping[str, Any]]", job_ids: "List[str]", job_queues: "List[str]") -> "List[Node]":
        """Associate node allocations with their reported job queues, sorted by name (unknown last) for stable output."""
        job_ids_queues = dict(zip(job_ids, job_queues))
        # The copies keep every Node field and add qname, so they are Node-shaped.
        return cast(
            "List[Node]",
            [
                dict(
                    worker_node,
                    qname=sorted(
                        set(
                            job_ids_queues.get(resolve_job_id(job_id, job_ids_queues))
                            for job_id in list(worker_node["core_job_map"].values()) + [job for field in cls.node_job_fields for job in worker_node.get(field, [])]
                        ),
                        key=lambda queue: (queue is None, queue or ""),
                    ),
                )
                for worker_node in _worker_nodes
            ],
        )
