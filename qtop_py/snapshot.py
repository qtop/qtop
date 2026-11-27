## Copyright (c) 2026 qtop contributors
## SPDX-License-Identifier: MIT
"""The read-only, display-safe Snapshot of one scheduler poll, plus its five-field view for the renderers."""

import copy
import json
from collections import OrderedDict, namedtuple
from typing import TYPE_CHECKING, Callable, Optional, Tuple, cast

from qtop_py.core_cluster_state import validate_cluster_state, validated_cluster_state_totals
from qtop_py.core_safety import raw_and_visible

if TYPE_CHECKING:
    from typing import Dict

    from qtop_py import core_state_types as state_types


JobRow = namedtuple("JobRow", ["user_name", "job_state", "job_queue"])
QueueRow = namedtuple("QueueRow", ["lm", "queued", "run", "state"])


class Snapshot(namedtuple("Snapshot", ["worker_nodes", "jobs_dict", "queues_dict", "total_running_jobs", "total_queued_jobs"])):
    """One frame's worker nodes, jobs, queues and job totals, built from a validated cluster state."""

    if TYPE_CHECKING:
        _state: state_types.ClusterState
        _collection_age: float
        _collection_span: float
        _collection_error: str
        _job_metadata: Dict[str, state_types.Job]

    @classmethod
    def from_cluster_state(
        cls,
        cluster_state: "state_types.ClusterState",
        validated: bool = False,
        display_state: Optional[Callable[[str], str]] = None,
        collection: Optional[Tuple[float, float, str]] = None,
    ) -> "Snapshot":
        """Validate a cluster state and build the complete snapshot, with display-safe text.

        validated=True skips validation for a state the caller has just passed through
        validate_cluster_state() itself (live collection does, in its worker thread).
        display_state maps each job state for display (the exported state keeps the original).
        collection is (age, span, error) of a background poll. Nothing is set after this returns.
        """
        # The one copy boundary (JSON-shaped): one traversal yields the raw copy kept
        # for export and the display-safe copy, sharing no dict or list.
        original, cluster_state = raw_and_visible(cluster_state if validated else validate_cluster_state(cluster_state))
        totals = validated_cluster_state_totals(cluster_state)
        shown_state = display_state or (lambda state: state)
        jobs_dict = OrderedDict((job["id"], JobRow(job["user"], shown_state(job["state"]), job["queue"])) for job in cluster_state["jobs"])
        queues_dict = OrderedDict(
            (
                queue["name"],
                QueueRow(str(queue["limit"]), str(queue["queued"]), str(queue["running"]), queue["state"]),
            )
            for queue in cluster_state["queues"]
        )
        snapshot = cls(
            cluster_state["nodes"],
            jobs_dict,
            queues_dict,
            totals["running_jobs"],
            totals["queued_jobs"],
        )
        snapshot._state = original
        snapshot._job_metadata = {job["id"]: job for job in cluster_state["jobs"]}
        if collection is not None:
            snapshot._collection_age, snapshot._collection_span, snapshot._collection_error = collection
        return snapshot

    @property
    def _diagnostics(self) -> "Dict[str, Dict[str, int]]":
        return {key: cast("Dict[str, int]", value) for key, value in self._state.items() if key.endswith("_diagnostics")} if hasattr(self, "_state") else {}

    def to_cluster_state(self) -> "state_types.ClusterState":
        """Return a copy of the cluster state behind this snapshot."""
        if hasattr(self, "_state"):
            return copy.deepcopy(self._state)
        # Five-field legacy snapshots are adapted only at this boundary.
        metadata: "Dict[str, state_types.JobResources]" = getattr(self, "_job_metadata", {})
        state: "state_types.ClusterState" = {
            "jobs": [
                {
                    **metadata.get(job_id, {}),
                    "id": job_id,
                    "user": job.user_name,
                    "state": job.job_state,
                    "queue": job.job_queue,
                }
                for job_id, job in self.jobs_dict.items()
            ],
            "nodes": copy.deepcopy(self.worker_nodes),
            "queues": [
                {
                    "name": queue_name,
                    "limit": queue.lm,
                    "queued": int(queue.queued),
                    "running": int(queue.run),
                    "state": queue.state,
                }
                for queue_name, queue in self.queues_dict.items()
            ],
        }
        cast("Dict[str, object]", state).update(copy.deepcopy(getattr(self, "_diagnostics", {})))
        return state

    def save(self, filename: str) -> None:
        """Write the cluster state to filename as JSON."""
        with open(filename, "w", encoding="utf-8") as outfile:
            # One dumps() call: json.dump() streams through the pure-Python encoder, ~6x slower on 1 MB exports.
            outfile.write(json.dumps(self._state if hasattr(self, "_state") else self.to_cluster_state()))
