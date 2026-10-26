##
## qtop is a tool to monitor queuing systems - https://github.com/qtop/qtop
##
## SPDX-License-Identifier: MIT
##

"""Small, scheduler-neutral data transformations for cluster snapshots."""

import copy
import re
from typing import TYPE_CHECKING, Dict, List, Sequence, cast

if TYPE_CHECKING:
    from qtop_py.contracts import ClusterState, Job, Node, Queue, RawQueue

CLUSTER_STATE_KEYS = ("jobs", "nodes", "queues")


def build_cluster_state(
    job_ids: Sequence[str],
    users: Sequence[str],
    job_states: Sequence[str],
    job_queues: Sequence[str],
    nodes: "Sequence[Node]",
    queues: "Sequence[RawQueue]",
) -> "ClusterState":
    """Return the deliberately boring public representation of a cluster."""
    if len({len(values) for values in (job_ids, users, job_states, job_queues)}) != 1:
        raise ValueError("job IDs, users, states and queues must have equal lengths")
    jobs: "List[Job]" = [
        {
            "id": re.sub(r"\[\]$", "", job_id),
            "user": user,
            "state": state,
            "queue": queue,
        }
        for job_id, user, state, queue in zip(job_ids, users, job_states, job_queues)
    ]
    normalized_queues: "List[Queue]" = [
        {
            "name": queue["queue_name"],
            "limit": str(queue["lm"]),
            "queued": int(queue["queued"]),
            "running": int(queue["run"]),
            "state": queue["state"],
        }
        for queue in queues
    ]
    return {
        "jobs": jobs,
        "nodes": copy.deepcopy(list(nodes)),
        "queues": normalized_queues,
    }


def _record(value: object, label: str) -> Dict[str, object]:
    if not isinstance(value, dict) or not all(isinstance(key, str) for key in cast(Dict[object, object], value)):
        raise TypeError("%s must be an object with string keys" % label)
    return cast(Dict[str, object], value)


def validate_cluster_state(cluster_state: object) -> "ClusterState":
    """Validate imported records before claiming the static snapshot contract."""
    data = _record(cluster_state, "cluster state")
    if set(data) != set(CLUSTER_STATE_KEYS):
        raise ValueError("cluster state keys must be jobs, nodes, queues")
    for key, fields in (("jobs", ("id", "user", "state", "queue")), ("nodes", ("domainname", "state")), ("queues", ("name", "limit", "state"))):
        entries = data[key]
        if not isinstance(entries, list):
            raise TypeError("cluster state %s must be a list" % key)
        for entry in cast(List[object], entries):
            record = _record(entry, key)
            if any(not isinstance(record.get(field), str) for field in fields):
                raise TypeError("%s record requires text fields: %s" % (key, ", ".join(fields)))
            if key == "nodes":
                if "cpu_allocation_source" in record and record["cpu_allocation_source"] not in ("slurm", "estimated", "none"):
                    raise ValueError("node cpu_allocation_source must be slurm, estimated or none")
                np = record.get("np")
                if isinstance(np, bool) or not isinstance(np, (str, int)):
                    raise ValueError("node np must be a nonnegative integer or integer string")
                if int(np) < 0:
                    raise ValueError("node np must be nonnegative")
                qnames, cores = record.get("qname"), record.get("core_job_map")
                if not isinstance(qnames, list) or any(queue is not None and not isinstance(queue, str) for queue in cast(List[object], qnames)):
                    raise TypeError("node qname must be a list of strings or nulls")
                if not isinstance(cores, dict) or any(not isinstance(core, (str, int)) or not isinstance(job, str) for core, job in cast(Dict[object, object], cores).items()):
                    raise TypeError("node core_job_map must map integer/string cores to string job IDs")
            elif key == "queues":
                for field in ("running", "queued"):
                    count = record.get(field)
                    if type(count) is not int or count < 0:
                        raise ValueError("queue %s must be a nonnegative integer" % field)
    return cast("ClusterState", data)


def cluster_state_totals(cluster_state: object) -> Dict[str, int]:
    """Derive summary values instead of storing duplicated mutable counters."""
    state = validate_cluster_state(cluster_state)
    return {
        "running_jobs": sum(queue["running"] for queue in state["queues"]),
        "queued_jobs": sum(queue["queued"] for queue in state["queues"]),
    }
