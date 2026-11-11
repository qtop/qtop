## Copyright (c) 2026 qtop contributors
## SPDX-License-Identifier: MIT
##
## qtop is a tool to monitor queuing systems - https://github.com/qtop/qtop
##

"""Small, scheduler-neutral data transformations for cluster snapshots."""

import re
from typing import TYPE_CHECKING, Dict, List, Optional, Sequence, Set, cast

if TYPE_CHECKING:
    from qtop_py.state_types import ClusterState, Job, Node, Queue, RawQueue

from qtop_py.safety import MAX_CORES_PER_NODE
from qtop_py.schedulers.metadata import DIAGNOSTICS, validate_diagnostics, validate_record_extensions

CLUSTER_STATE_KEYS = ("jobs", "nodes", "queues")
# Schema 1 is the bare jobs/nodes/queues state, with no envelope. Schema 2 adds the
# observation envelope and optional job/node qid and job kind (docs/cluster-state-model.md).
ENVELOPE_KEYS = ("schema_version", "source")
JOB_KINDS = ("job", "pod", "vm", "task")


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
    jobs: List[Job] = [
        {
            "id": re.sub(r"\[\]$", "", job_id),
            "user": user,
            "state": state,
            "queue": queue,
        }
        for job_id, user, state, queue in zip(job_ids, users, job_states, job_queues)
    ]
    normalized_queues: List[Queue] = [
        {
            "name": queue["queue_name"],
            "limit": str(queue["lm"]),
            "queued": int(queue["queued"]),
            "running": int(queue["run"]),
            "state": queue["state"],
        }
        for queue in queues
    ]
    # Nodes are handed over, not copied: every consumer copies at its own boundary
    # (Snapshot.from_cluster_state, sample.anonymize_state), so a copy here was a second one.
    return {
        "jobs": jobs,
        "nodes": list(nodes),
        "queues": normalized_queues,
    }


def _record(value: object, label: str) -> Dict[str, object]:
    if not isinstance(value, dict) or not all(isinstance(key, str) for key in cast(Dict[object, object], value)):
        raise TypeError("%s must be an object with string keys" % label)
    return cast(Dict[str, object], value)


_CORE_KEY_TYPES = frozenset((int, str))
_TEXT_TYPE = frozenset((str,))


def _core_job_map_types_valid(cores: Dict[object, object]) -> bool:
    """Check every core is an int or str and every job a str."""
    # Fast path: exact int/str types are a subset of what the isinstance() check
    # accepts; anything else (bool, str subclasses, wrong types) takes that check.
    if _CORE_KEY_TYPES.issuperset(map(type, cores)) and _TEXT_TYPE.issuperset(map(type, cores.values())):
        return True
    return all(isinstance(core, (str, int)) and isinstance(job, str) for core, job in cores.items())


def _qid_prefix(data: Dict[str, object]) -> Optional[str]:
    """Check the schema version and source; return the qid namespace "backend:cluster_id:", or None for schema 1."""
    version = data.get("schema_version", 1)
    if type(version) is not int or version not in (1, 2):
        raise ValueError("unsupported cluster state schema_version %r" % (version,))
    if version == 1:
        if "source" in data:
            raise ValueError("cluster state source requires schema_version 2")
        return None
    source = _record(data.get("source"), "cluster state source")
    if set(source) != {"backend", "cluster_id"} or not all(isinstance(value, str) and value and ":" not in value for value in source.values()):
        raise ValueError("cluster state source requires nonempty backend and cluster_id text without ':'")
    return "%s:%s:" % (source["backend"], source["cluster_id"])


def _validate_identity(key: str, record: Dict[str, object], qid_prefix: Optional[str], qids: Set[str]) -> None:
    if qid_prefix is None:
        raise ValueError("%s qid and kind require schema_version 2" % key)
    if "qid" in record:
        qid = record["qid"]
        if not isinstance(qid, str) or not qid.startswith(qid_prefix) or len(qid) == len(qid_prefix):
            raise ValueError("%s qid must be text in its source namespace %s<id>" % (key, qid_prefix))
        if qid in qids:
            raise ValueError("%s qid %r is not unique in its source namespace" % (key, qid))
        qids.add(qid)
    if "kind" in record and (key != "jobs" or record["kind"] not in JOB_KINDS):
        raise ValueError("job kind must be one of %s; nodes have no kind" % ", ".join(JOB_KINDS))


def validate_cluster_state(cluster_state: object) -> "ClusterState":
    """Validate imported records before claiming the static snapshot contract."""
    data = _record(cluster_state, "cluster state")
    if set(data) - DIAGNOSTICS.keys() - set(ENVELOPE_KEYS) != set(CLUSTER_STATE_KEYS):
        raise ValueError("cluster state keys must be jobs, nodes, queues with optional scheduler diagnostics")
    validate_diagnostics(data)
    qid_prefix = _qid_prefix(data)
    qids: Set[str] = set()
    for key, fields in (("jobs", ("id", "user", "state", "queue")), ("nodes", ("domainname", "state")), ("queues", ("name", "limit", "state"))):
        entries = data[key]
        if not isinstance(entries, list):
            raise TypeError("cluster state %s must be a list" % key)
        for entry in cast(List[object], entries):
            record = _record(entry, key)
            if any(not isinstance(record.get(field), str) for field in fields):
                raise TypeError("%s record requires text fields: %s" % (key, ", ".join(fields)))
            validate_record_extensions(key, record)
            if key != "queues" and ("qid" in record or "kind" in record):
                _validate_identity(key, record, qid_prefix, qids)
            if key == "jobs" and "accounting_state" in record and record["accounting_state"] not in ("running", "queued", "other"):
                raise ValueError("job accounting_state must be running, queued or other")
            if key == "nodes":
                np = record.get("np")
                if isinstance(np, bool) or not isinstance(np, (str, int)):
                    raise ValueError("node np must be a nonnegative integer or integer string")
                if int(np) < 0:
                    raise ValueError("node np must be nonnegative")
                if int(np) > MAX_CORES_PER_NODE:
                    raise ValueError("node np must not exceed %d cores" % MAX_CORES_PER_NODE)
                qnames, cores = record.get("qname"), record.get("core_job_map")
                if not isinstance(qnames, list) or any(queue is not None and not isinstance(queue, str) for queue in cast(List[object], qnames)):
                    raise TypeError("node qname must be a list of strings or nulls")
                if not isinstance(cores, dict) or not _core_job_map_types_valid(cast(Dict[object, object], cores)):
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
    return validated_cluster_state_totals(validate_cluster_state(cluster_state))


def validated_cluster_state_totals(state: "ClusterState") -> Dict[str, int]:
    """Totals for a state the caller has already passed through validate_cluster_state()."""
    if state["jobs"] and all("accounting_state" in job for job in state["jobs"]):
        return {
            "running_jobs": len({job["id"] for job in state["jobs"] if job.get("accounting_state") == "running"}),
            "queued_jobs": len({job["id"] for job in state["jobs"] if job.get("accounting_state") == "queued"}),
        }
    return {
        "running_jobs": sum(queue["running"] for queue in state["queues"]),
        "queued_jobs": sum(queue["queued"] for queue in state["queues"]),
    }
