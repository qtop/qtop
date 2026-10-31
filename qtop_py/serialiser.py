##
## qtop is a tool to monitor queuing systems - https://github.com/qtop/qtop
##
## Copyright (c) 2016 Fotis Georgatos
## Copyright (c) 2016 Sotiris Fragkiskos
## Copyright (c) 2023 Hewlett Packard Enterprise Development LP
##
## SPDX-License-Identifier: MIT
##

import logging
import re
from itertools import count
from typing import TYPE_CHECKING, Callable, Collection, Dict, Sequence, Tuple

if TYPE_CHECKING:
    from typing import List

    from qtop_py.contracts import ClusterState, Config, JobsInfo, Node, Options, QueuesInfo

from qtop_py.cluster_state import build_cluster_state
from qtop_py.plugins.metadata import CLASSIC_REPORT_NAME, node_is_idle


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

    def _process_qstat_line(self, re_search: str, line: str, re_match_positions: Sequence[str]) -> Dict[str, str]:
        """
        extracts data from a tabular qstat-like file
        returns a list
        """
        m = re.search(re_search, line.strip())

        try:
            if m is None:
                raise AttributeError("qstat row did not match")
            job_id, user, job_state, queue = [m.group(x) for x in re_match_positions]
        except AttributeError:
            logging.warning("Line: %s not properly parsed by regex expression. Assuming alternative qstat format." % line.strip())
            raise
        job_id = job_id.split(".")[0]
        user = self.anonymize(user, "users")
        return {"JobId": job_id, "UnixAccount": user, "S": job_state, "Queue": queue}

    def anonymize_func(self) -> Callable[[str, str], str]:
        """
        creates and returns an _anonymize_func object (closure)
        Anonymisation can be used by the user for providing feedback to the developers, without leaking cluster's private data.
        i.e. the logs and the output should no longer contain sensitive information about the clusters ran by the user.
        """
        counters = {}
        stored_dict: Dict[str, Tuple[str, str]] = {}
        for key in ["users", "wns", "qs", "jobnums", "jobnames", "jobtimes"]:
            counters[key] = count()

        maps = {"users": "_anon_user_", "wns": "_anon_wn_", "qs": "_anon_q_", "jobnums": "_anon_jn_", "jobnames": "_anon_nm_", "jobtimes": "never"}

        def _anonymize_func(s: str, a_type: str) -> str:
            """
            d4-p4-04 --> d_anon_wn_0
            d4-p4-05 --> d_anon_wn_1
            biomed017--> b_anon_user_0
            alice    --> a_anon_q_0
            """
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
        return lambda s, a_type: s

    def anonymize_queue_list_nametag(self, queue_list_nametag):
        name, nodename = queue_list_nametag.text.split("@")
        name = self.anonymize(name, "qs")
        nodename = self.anonymize(nodename, "wns")
        return name + "@" + nodename


class GenericBatchSystem(object):
    optional_trace_keys: Tuple[str, ...] = ()
    optional_trace_aliases: Dict[str, str] = {}
    optional_trace_warning = "Optional scheduler details unavailable."
    detection_priority = 0
    is_simulated = False
    node_job_fields: Tuple[str, ...] = ()
    blocked_queue_note = ""
    native_queue_membership = False
    node_is_idle = staticmethod(node_is_idle)
    classic_report_name = CLASSIC_REPORT_NAME

    @staticmethod
    def collection_command(key, command, options):
        return command

    @staticmethod
    def queue_is_blocked(state):
        state = str(state)
        return state.startswith("D") or state.endswith("S")

    @staticmethod
    def node_state_marker(node):
        return ""

    @staticmethod
    def display_messages(section, document, nodes, config):
        return []

    def mask_source_trace(self, key, text, mask):
        raise ValueError("This backend has no source trace masker")

    def get_queues_info(self) -> "QueuesInfo":
        raise NotImplementedError

    def get_worker_nodes(self, job_ids: "List[str]", job_queues: "List[str]", options: "Options") -> "List[Node]":
        raise NotImplementedError

    def get_jobs_info(self) -> "JobsInfo":
        raise NotImplementedError

    @staticmethod
    def get_mnemonic() -> str:
        raise NotImplementedError

    def get_cluster_state(self, options: "Options") -> "ClusterState":
        """Collect scheduler data behind one stable, plain-data boundary."""
        job_ids, users, job_states, job_queues = self.get_jobs_info()
        _running, _queued, queues = self.get_queues_info()
        nodes = self.get_worker_nodes(job_ids, job_queues, options)
        return build_cluster_state(job_ids, users, job_states, job_queues, nodes, queues)

    @classmethod
    def ensure_worker_nodes_have_qnames(cls, _worker_nodes, job_ids, job_queues):
        """Associate node allocations with their reported job queues."""
        job_ids_queues = dict(zip(job_ids, job_queues))
        return [
            dict(
                worker_node,
                qname=list(
                    set(
                        job_ids_queues.get(resolve_job_id(job_id, job_ids_queues))
                        for job_id in list(worker_node["core_job_map"].values()) + [job for field in cls.node_job_fields for job in worker_node.get(field, [])]
                    )
                ),
            )
            for worker_node in _worker_nodes
        ]
