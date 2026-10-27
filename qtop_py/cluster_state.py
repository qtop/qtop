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

from qtop_py.gpu import validate_gpu_resources

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
            for field in ("slurm_partitions",) if key == "jobs" else ("slurm_job_ids",) if key == "nodes" else ():
                if field in record:
                    values = record[field]
                    if (
                        not isinstance(values, list)
                        or (field == "slurm_partitions" and not values)
                        or any(not isinstance(value, str) or not value for value in cast(List[object], values))
                    ):
                        raise ValueError("%s must be a list of nonempty strings" % field)
                    if field == "slurm_partitions" and record["queue"] not in values:
                        raise ValueError("job queue must belong to slurm_partitions")
            if key == "jobs":
                if "accounting_state" in record and record["accounting_state"] not in ("running", "queued", "other"):
                    raise ValueError("job accounting_state must be running, queued or other")
                if "slurm_array_job_id" in record and "slurm_het_job_id" in record:
                    raise ValueError("job cannot have both Slurm array and heterogeneous identities")
                for parent, child, separator in (("slurm_array_job_id", "slurm_array_task_id", "_"), ("slurm_het_job_id", "slurm_het_job_offset", "+")):
                    if parent in record or child in record:
                        base, index = record.get(parent), record.get(child)
                        if not isinstance(base, str) or not isinstance(index, str) or not re.fullmatch(r"[0-9]+", base) or not re.fullmatch(r"[0-9]+", index):
                            raise ValueError("job Slurm identity requires a numeric parent and task/component ID")
                        if record["id"] != base + separator + index:
                            raise ValueError("job Slurm identity conflicts with canonical ID")
            if key == "nodes":
                if "slurm_node_state" in record and not isinstance(record["slurm_node_state"], str):
                    raise ValueError("slurm_node_state must be text")
                for gpu_field in ("slurm_gpu_configured", "slurm_gpu_used", "slurm_gpu_bindings"):
                    if gpu_field in record:
                        validate_gpu_resources(record[gpu_field], binding=gpu_field == "slurm_gpu_bindings")
                if "slurm_unmapped_cpus" in record and (type(record["slurm_unmapped_cpus"]) is not int or record["slurm_unmapped_cpus"] < 0):
                    raise ValueError("slurm_unmapped_cpus must be a nonnegative integer")
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
    jobs = cast(List[Dict[str, object]], data["jobs"])
    if any("accounting_state" in job for job in jobs) and not all("accounting_state" in job for job in jobs):
        raise ValueError("job accounting_state must be present for all jobs or none")
    return cast("ClusterState", data)


def cluster_state_totals(cluster_state: object) -> Dict[str, int]:
    """Derive summary values instead of storing duplicated mutable counters."""
    state = validate_cluster_state(cluster_state)
    if state["jobs"] and all("accounting_state" in job for job in state["jobs"]):
        return {
            "running_jobs": len({job["id"] for job in state["jobs"] if job.get("accounting_state") == "running"}),
            "queued_jobs": len({job["id"] for job in state["jobs"] if job.get("accounting_state") == "queued"}),
        }
    return {
        "running_jobs": sum(queue["running"] for queue in state["queues"]),
        "queued_jobs": sum(queue["queued"] for queue in state["queues"]),
    }
