## Copyright (c) 2026 qtop contributors
## SPDX-License-Identifier: MIT
"""The read-only, display-safe Snapshot of one scheduler poll, plus its five-field view for the renderers."""

import copy
import json
from collections import OrderedDict, namedtuple
from typing import TYPE_CHECKING, cast

from qtop_py.cluster_state import validate_cluster_state, validated_cluster_state_totals
from qtop_py.safety import rebuild, visible_data

if TYPE_CHECKING:
    from typing import Dict

    from qtop_py import state_types


JobRow = namedtuple("JobRow", ["user_name", "job_state", "job_queue"])
QueueRow = namedtuple("QueueRow", ["lm", "queued", "run", "state"])


class Snapshot(namedtuple("Snapshot", ["worker_nodes", "jobs_dict", "queues_dict", "total_running_jobs", "total_queued_jobs"])):
    if TYPE_CHECKING:
        _state: state_types.ClusterState
        _job_metadata: Dict[str, state_types.Job]

    @classmethod
    def from_cluster_state(cls, cluster_state: "state_types.ClusterState") -> "Snapshot":
        cluster_state = rebuild(validate_cluster_state(cluster_state))  # the one copy boundary (JSON-shaped)
        original = cluster_state
        # visible_data() rebuilds every dict and list, so its result shares no
        # mutable container with `original`; no second deep copy is needed below.
        cluster_state = visible_data(cluster_state)
        totals = validated_cluster_state_totals(cluster_state)
        jobs_dict = OrderedDict((job["id"], JobRow(job["user"], job["state"], job["queue"])) for job in cluster_state["jobs"])
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
        return snapshot

    @property
    def _diagnostics(self) -> "Dict[str, Dict[str, int]]":
        return {key: cast("Dict[str, int]", value) for key, value in self._state.items() if key.endswith("_diagnostics")} if hasattr(self, "_state") else {}

    def to_cluster_state(self) -> "state_types.ClusterState":
        if hasattr(self, "_state"):
            return copy.deepcopy(self._state)
        # Five-field legacy documents are adapted only at this boundary.
        state: "state_types.ClusterState" = {
            "jobs": [
                {
                    **getattr(self, "_job_metadata", {}).get(job_id, {}),
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
        with open(filename, "w", encoding="utf-8") as outfile:
            json.dump(self._state if hasattr(self, "_state") else self.to_cluster_state(), outfile)
