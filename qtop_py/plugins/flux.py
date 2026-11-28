## Copyright (c) 2026 qtop contributors
## SPDX-License-Identifier: MIT
"""Flux backend: flux jobs --json, flux resource R, list and status, and flux queue list.

Every trace is readable by any user of a system instance. A job's exact cores (its R) are
not: only the job's owner and the instance owner may read them. So the cores in use come
from `flux resource list` (exact), and a job takes its share of them on each of its nodes:
exact where a node runs one job, estimated where several share it (flux_diagnostics).
"""

import json
import logging
import re
from collections import OrderedDict
from typing import TYPE_CHECKING, Any, Dict, List, Set, Tuple

from qtop_py.core_api import GenericBatchSystem, SchedulerInputError, open_input
from qtop_py.core_hostlist import HostlistParser
from qtop_py.core_safety import MAX_CORES_PER_NODE
from qtop_py.schedulers.extensions import ExtensionSpec

if TYPE_CHECKING:
    from typing import Final

    from qtop_py.core_state_types import ClusterState, Config, JobsInfo, Node, Options, QueuesInfo

logger = logging.getLogger(__name__)

## Active job states, as the ST letters flux jobs prints; inactive jobs are not listed without -a.
JOB_STATES: "Final" = {"DEPEND": "D", "PRIORITY": "P", "SCHED": "S", "RUN": "R", "CLEANUP": "C"}
## flux resource status states; a trailing * (offline broker) makes any of them down ('d').
NODE_STATES: "Final" = {"avail": "-", "allocated": "-", "housekeeping": "b", "torpid": "s", "draining": "o", "drained": "o", "exclude": "o"}
## Jobs of an instance without named queues; flux queue list prints its anonymous queue with no name.
DEFAULT_QUEUE: "Final" = "default"
MAX_RANKS: "Final" = HostlistParser.MAX_EXPANDED_NODES
_RLIST_ITEM = re.compile(r"rank([0-9]+|\[[0-9,\-]+\])/(\S+)")
_RLIST_CHILD = re.compile(r"([a-z]+)([0-9]+|\[[0-9,\-]+\])(?:,|$)")

FLUX_EXTENSION: "Final" = ExtensionSpec("Flux", diagnostics=("flux_diagnostics", ("shared_nodes", "unowned_cores", "unplaced_cores")))


def idset(text: str, limit: int) -> List[int]:
    """Expand a Flux idset (0-5, 0,3-4 or [3-4]) in order; raise SchedulerInputError if malformed or over limit."""
    body = text[1:-1] if text.startswith("[") and text.endswith("]") else text
    ids: List[int] = []
    for part in body.split(","):
        bounds = re.fullmatch(r"([0-9]+)(?:-([0-9]+))?", part)
        if not bounds:
            raise SchedulerInputError("Invalid Flux idset %r" % text)
        first, last = int(bounds.group(1)), int(bounds.group(2) or bounds.group(1))
        if last < first or len(ids) + last - first >= limit:
            raise SchedulerInputError("Invalid or oversized Flux idset %r" % text)
        ids.extend(range(first, last + 1))
    return ids


class FluxBatchSystem(GenericBatchSystem):
    """Flux backend; nodes are broker ranks, queues are Flux queues (or one anonymous queue)."""

    detection_priority = 5  # -b auto: Slurm -2, Slurm JSON -1, PBS 0, OAR 1, SGE 2, LSF 3, HTCondor 4, Flux 5
    signature_command = "flux"
    default_commands = {
        "jobs_json": "%(savepath)s/flux_jobs%(pid)s.json, flux jobs -A --json",
        "resource_json": "%(savepath)s/flux_resource_R%(pid)s.json, flux resource R",
        "rlist_file": "%(savepath)s/flux_resource_list%(pid)s.txt, flux resource list -no '{state} {rlist}'",
        "status_file": "%(savepath)s/flux_resource_status%(pid)s.txt, flux resource status -no '{state} {nodelist}'",
        "queues_file": "%(savepath)s/flux_queue_list%(pid)s.txt, flux queue list -no '{queue} {enabled} {started}'",
    }
    state_abbreviations = {
        "D": "waiting_of_user",
        "P": "queued_of_user",
        "S": "queued_of_user",
        "R": "running_of_user",
        "C": "exiting_of_user",
    }
    extensions = FLUX_EXTENSION

    @staticmethod
    def get_mnemonic() -> str:
        """Return the scheduler name, flux."""
        return "flux"

    @staticmethod
    def queue_is_blocked(state: object) -> bool:
        """Return True for a stopped queue, which schedules no jobs (flux queue stop)."""
        return str(state).endswith(":Stopped")

    @staticmethod
    def display_messages(section: str, snapshot: object, nodes: "List[Node]", config: "Config") -> List[str]:
        """Say where core ownership is estimated or unknown, under the snapshot."""
        notes = getattr(snapshot, "_diagnostics", {}).get("flux_diagnostics", {})
        if section == "snapshot" and any(notes.values()):
            return [
                "NOTE: Flux core ownership: %s nodes run several jobs, so which job holds which allocated core is estimated; "
                "%s allocated cores have no listed job (?); %s job cores exceed the allocation."
                % tuple(notes.get(key, 0) for key in ("shared_nodes", "unowned_cores", "unplaced_cores"))
            ]
        return []

    def __init__(self, scheduler_output_filenames: Dict[str, str], config: "Config", options: "Options") -> None:
        self.files = scheduler_output_filenames
        self.config = config
        self.options = options
        self.diagnostics: Dict[str, int] = {}

    def get_cluster_state(self, options: "Options") -> "ClusterState":
        """Return the cluster state, with flux_diagnostics when ownership is estimated or unknown."""
        state = super().get_cluster_state(options)
        if self.diagnostics:
            state["flux_diagnostics"] = self.diagnostics.copy()
        return state

    def get_jobs_info(self) -> "JobsInfo":
        """Return f58 job IDs, user names, flux jobs state letters and queues."""
        jobs = self._jobs()
        return [job["id"] for job in jobs], [job["user"] for job in jobs], [job["state"] for job in jobs], [job["queue"] for job in jobs]

    def get_queues_info(self) -> "QueuesInfo":
        """Return each queue's running and queued (S or P) jobs; state is Enabled|Disabled:Started|Stopped."""
        queues: Dict[str, Any] = OrderedDict()
        for name, state in self._queues():
            queues[name] = {"queue_name": name, "run": 0, "queued": 0, "lm": "--", "state": state}
        for job in self._jobs():
            queue = queues.setdefault(job["queue"], {"queue_name": job["queue"], "run": 0, "queued": 0, "lm": "--", "state": "?"})
            queue["run"] += job["state"] == "R"
            queue["queued"] += job["state"] in ("S", "P")
        return sum(queue["run"] for queue in queues.values()), sum(queue["queued"] for queue in queues.values()), list(queues.values())

    def get_worker_nodes(self, job_ids: List[str], job_queues: List[str], options: "Options") -> "List[Node]":
        """Return one node per broker rank, its allocated cores owned by the jobs running there."""
        hosts, cores = self._inventory()
        states = self._node_states()
        nodes: Dict[str, Dict[str, Any]] = OrderedDict()
        for rank, host in enumerate(hosts):
            nodes[host] = OrderedDict([("domainname", host), ("np", len(cores.get(rank, []))), ("core_job_map", {}), ("state", states.get(host, "?"))])
        allocated = self._allocated(len(hosts))
        shares: Dict[str, List[Tuple[str, int]]] = OrderedDict()
        for job in self._jobs():
            for host, count in job["shares"]:
                shares.setdefault(host, []).append((job["id"], count))
        diagnostics = {"shared_nodes": 0, "unowned_cores": 0, "unplaced_cores": 0}
        for rank, host in enumerate(hosts):
            position = {core: index for index, core in enumerate(cores.get(rank, []))}
            free = [position[core] for core in sorted(allocated.get(rank, ())) if core in position]
            owners = shares.get(host, [])
            diagnostics["shared_nodes"] += len(owners) > 1
            for job_id, count in owners:
                taken, free = free[:count], free[count:]
                nodes[host]["core_job_map"].update((core, job_id) for core in taken)
                diagnostics["unplaced_cores"] += count - len(taken)
            nodes[host]["core_job_map"].update((core, "?") for core in free)
            diagnostics["unowned_cores"] += len(free)
        self.diagnostics = {key: count for key, count in diagnostics.items() if count}
        logger.info("worker_nodes contains %s entries", len(nodes))
        return self.ensure_worker_nodes_have_qnames(list(nodes.values()), job_ids, job_queues)

    def _inventory(self) -> Tuple[List[str], Dict[int, List[int]]]:
        """Return the host of each rank and each rank's core IDs, from flux resource R."""
        data = self._json("resource_json")
        execution = data.get("execution") if isinstance(data, dict) else None
        if not isinstance(execution, dict) or not isinstance(execution.get("R_lite"), list) or not isinstance(execution.get("nodelist"), list):
            raise SchedulerInputError("flux resource R lacks execution.R_lite or execution.nodelist")
        hosts: List[str] = []
        for group in execution["nodelist"]:
            expanded = HostlistParser.expand_nodelist(group) if isinstance(group, str) else []
            if not expanded:
                raise SchedulerInputError("Invalid Flux nodelist %r" % (group,))
            hosts.extend(expanded)
        if len(set(hosts)) != len(hosts):
            raise SchedulerInputError("flux resource R names a host twice")
        cores: Dict[int, List[int]] = {}
        for entry in execution["R_lite"]:
            children = entry.get("children") if isinstance(entry, dict) else None
            if not isinstance(entry.get("rank"), str) or not isinstance(children, dict) or not isinstance(children.get("core", ""), str):
                raise SchedulerInputError("Invalid Flux R_lite entry %r" % (entry,))
            for rank in idset(entry["rank"], MAX_RANKS):
                if rank >= len(hosts) or rank in cores:
                    raise SchedulerInputError("Flux R_lite rank %d is unnamed or repeated" % rank)
                cores[rank] = idset(children["core"], MAX_CORES_PER_NODE + 1) if children.get("core") else []
                if len(cores[rank]) > MAX_CORES_PER_NODE or len(set(cores[rank])) != len(cores[rank]):
                    raise SchedulerInputError("Flux rank %d core IDs are repeated or exceed %d" % (rank, MAX_CORES_PER_NODE))
        return hosts, cores

    def _allocated(self, ranks: int) -> Dict[int, Set[int]]:
        """Return the allocated core IDs of each rank, from the allocated line of flux resource list."""
        allocated: Dict[int, Set[int]] = {}
        for line in self._lines("rlist_file"):
            state, _, items = line.partition(" ")
            if state != "allocated":
                continue
            for item in items.split():
                match = _RLIST_ITEM.fullmatch(item)
                children = _RLIST_CHILD.findall(match.group(2)) if match else []
                if not match or "".join(name + ids + "," for name, ids in children).rstrip(",") != match.group(2):
                    raise SchedulerInputError("Invalid flux resource list entry %r" % item)
                for rank in idset(match.group(1), ranks):
                    for name, ids in children:
                        if name == "core":
                            allocated.setdefault(rank, set()).update(idset(ids, MAX_CORES_PER_NODE))
        return allocated

    def _node_states(self) -> Dict[str, str]:
        """Return each host's state marker from flux resource status; a trailing * (offline) is down."""
        states: Dict[str, str] = {}
        for line in self._lines("status_file"):
            fields = line.split()
            if len(fields) == 1:
                continue  # a state with no nodes
            if len(fields) != 2 or fields[0].rstrip("*") not in NODE_STATES:
                raise SchedulerInputError("Unsupported flux resource status line %r" % line)
            marker = "d" if fields[0].endswith("*") else NODE_STATES[fields[0]]
            for host in HostlistParser.expand_nodelist(fields[1]):
                states[host] = marker
        return states

    def _queues(self) -> List[Tuple[str, str]]:
        """Return (name, Enabled|Disabled:Started|Stopped) per queue; the anonymous queue is named default."""
        queues = []
        marks = {"✔": True, "✗": False}
        for line in self._lines("queues_file"):
            fields = line.split()
            if len(fields) == 2:
                fields.insert(0, DEFAULT_QUEUE)
            if len(fields) != 3 or fields[1] not in marks or fields[2] not in marks:
                raise SchedulerInputError("Unsupported flux queue list line %r" % line)
            queues.append((fields[0], "%s:%s" % ("Enabled" if marks[fields[1]] else "Disabled", "Started" if marks[fields[2]] else "Stopped")))
        return queues

    def _jobs(self) -> List[Dict[str, Any]]:
        if not hasattr(self, "_parsed_jobs"):
            data = self._json("jobs_json")
            if not isinstance(data, dict) or not isinstance(data.get("jobs"), list):
                raise SchedulerInputError("flux jobs --json output lacks a jobs list")
            self._parsed_jobs = [job for job in (self._job(record) for record in data["jobs"]) if job]
        return self._parsed_jobs

    @staticmethod
    def _job(record: Any) -> Dict[str, Any]:
        """Return one active job with its cores per host, or {} for an inactive one."""
        if not isinstance(record, dict):
            raise SchedulerInputError("flux jobs --json lists a non-object job")
        job_id, user, state, queue = record.get("jobid"), record.get("username"), record.get("state"), record.get("queue")
        if state == "INACTIVE":
            return {}
        if not isinstance(job_id, str) or not job_id or not isinstance(user, str) or not user or state not in JOB_STATES or not (queue is None or isinstance(queue, str)):
            raise SchedulerInputError("Invalid flux jobs record: jobid=%r username=%r state=%r queue=%r" % (job_id, user, state, queue))
        shares: List[Tuple[str, int]] = []
        if isinstance(record.get("nodelist"), str) and record["nodelist"]:
            hosts = HostlistParser.expand_nodelist(record["nodelist"])
            ncores = record.get("ncores")
            if not hosts or not isinstance(ncores, int) or isinstance(ncores, bool) or not 0 <= ncores <= MAX_CORES_PER_NODE * len(hosts):
                raise SchedulerInputError("Invalid flux job %s nodelist %r or ncores %r" % (job_id, record["nodelist"], ncores))
            each, extra = divmod(ncores, len(hosts))
            shares = [(host, each + (index < extra)) for index, host in enumerate(hosts)]
        return {"id": job_id, "user": user, "state": JOB_STATES[state], "queue": queue or DEFAULT_QUEUE, "shares": shares}

    def _json(self, key: str) -> Any:
        with open_input(self.files.get(key, ""), "r", encoding="utf-8", errors="replace", options=self.options) as stream:
            return json.loads(stream.read())

    def _lines(self, key: str) -> List[str]:
        with open_input(self.files.get(key, ""), "r", encoding="utf-8", errors="replace", options=self.options) as stream:
            return [line.strip() for line in stream if line.strip()]
