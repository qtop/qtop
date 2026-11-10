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
from itertools import count
from typing import TYPE_CHECKING, Any, Callable, Collection, Dict, Mapping, Sequence, Tuple, cast

if TYPE_CHECKING:
    from typing import List

    from qtop_py.source_trace import TraceMasker
    from qtop_py.state_types import ClusterState, Config, JobsInfo, Node, Options, QueuesInfo

from qtop_py.cluster_state import build_cluster_state
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
        self.anonymize = self.anonymize_func() if self.options.ANONYMIZE else self.eponymize_func()

    def anonymize_func(self) -> Callable[[str, str], str]:
        """Return a function mapping each name to a stable anonymous one, for -A output that can be shared.

        d4-p4-04 --> d_anon_wn_0, d4-p4-05 --> d_anon_wn_1, biomed017 --> b_anon_user_0, alice --> a_anon_q_0
        """
        counters = {}
        stored_dict: Dict[str, Tuple[str, str]] = {}
        for key in ["users", "wns", "qs", "jobnums", "jobnames", "jobtimes"]:
            counters[key] = count()

        maps = {"users": "_anon_user_", "wns": "_anon_wn_", "qs": "_anon_q_", "jobnums": "_anon_jn_", "jobnames": "_anon_nm_", "jobtimes": "never"}

        def _anonymize_func(s: str, a_type: str) -> str:
            """Return the anonymous name of s for the kind a_type (users, wns, qs, ...)."""
            dup_counter = counters[a_type]

            s_type = maps[a_type]
            cnt = "0"
            new_name_parts = [s[0], s_type, cnt]
            if s not in stored_dict:
                cnt = str(next(dup_counter))
                new_name_parts.pop()
                new_name_parts.append(cnt)
            stored_dict.setdefault(s, ("".join(new_name_parts), s_type))
            return stored_dict[s][0]

        return _anonymize_func

    def eponymize_func(self) -> Callable[[str, str], str]:
        """Return a function that leaves names unchanged, used when not anonymising."""
        return lambda s, a_type: s


class GenericBatchSystem(object):
    """The interface each scheduler backend implements, with defaults for the optional hooks."""

    optional_trace_keys: Tuple[str, ...] = ()
    optional_trace_aliases: Dict[str, str] = {}
    optional_trace_warning = "Optional scheduler details unavailable."
    detection_priority = 0
    is_simulated = False
    node_job_fields: Tuple[str, ...] = ()
    blocked_queue_note = ""
    native_queue_membership = False
    node_is_idle = staticmethod(node_is_idle)
    classic_report_name = "qtop"
    classic_job_count_source = ""
    classic_account_note = "  ('all' includes every reported job state)"

    @staticmethod
    def collection_command(key: str, command: str, options: "Options") -> str:
        """Return the shell command collecting trace key, adjusted for the options."""
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
