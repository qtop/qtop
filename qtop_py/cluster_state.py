##
## qtop is a tool to monitor queuing systems - https://github.com/qtop/qtop
##
## SPDX-License-Identifier: MIT
##

"""Small, scheduler-neutral data transformations for cluster snapshots."""

import copy
import re


CLUSTER_STATE_KEYS = ("jobs", "nodes", "queues")


def build_cluster_state(job_ids, users, job_states, job_queues, nodes, queues):
    """Return the deliberately boring public representation of a cluster."""
    jobs = [
        {
            "id": re.sub(r"\[\]$", "", job_id),
            "user": user,
            "state": state,
            "queue": queue,
        }
        for job_id, user, state, queue in zip(job_ids, users, job_states, job_queues)
    ]
    normalized_queues = [
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
        "nodes": copy.deepcopy(nodes),
        "queues": normalized_queues,
    }


def validate_cluster_state(cluster_state):
    """Validate the stable top-level contract without imposing scheduler fields."""
    if not isinstance(cluster_state, dict):
        raise TypeError("cluster state must be a dictionary")
    if set(cluster_state) != set(CLUSTER_STATE_KEYS):
        raise ValueError("cluster state keys must be jobs, nodes, queues")
    for key in CLUSTER_STATE_KEYS:
        if not isinstance(cluster_state[key], list):
            raise TypeError("cluster state %s must be a list" % key)
    return cluster_state


def cluster_state_totals(cluster_state):
    """Derive summary values instead of storing duplicated mutable counters."""
    validate_cluster_state(cluster_state)
    return {
        "running_jobs": sum(queue["running"] for queue in cluster_state["queues"]),
        "queued_jobs": sum(queue["queued"] for queue in cluster_state["queues"]),
    }
