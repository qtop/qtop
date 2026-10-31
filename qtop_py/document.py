## SPDX-License-Identifier: MIT
"""Normalized state with a cached five-field adapter for legacy renderers."""

import copy
import json
from collections import OrderedDict, namedtuple
from typing import TYPE_CHECKING, cast

from qtop_py.cluster_state import cluster_state_totals, validate_cluster_state

if TYPE_CHECKING:
    from typing import Dict

    from qtop_py import contracts


JobDoc = namedtuple("JobDoc", ["user_name", "job_state", "job_queue"])
QDoc = namedtuple("QDoc", ["lm", "queued", "run", "state"])


class Document(namedtuple("Document", ["worker_nodes", "jobs_dict", "queues_dict", "total_running_jobs", "total_queued_jobs"])):
    if TYPE_CHECKING:
        _state: contracts.ClusterState
        _job_metadata: Dict[str, contracts.Job]

    @classmethod
    def from_cluster_state(cls, cluster_state: "contracts.ClusterState") -> "Document":
        cluster_state = copy.deepcopy(validate_cluster_state(cluster_state))
        totals = cluster_state_totals(cluster_state)
        jobs_dict = OrderedDict((job["id"], JobDoc(job["user"], job["state"], job["queue"])) for job in cluster_state["jobs"])
        queues_dict = OrderedDict(
            (
                queue["name"],
                QDoc(str(queue["limit"]), str(queue["queued"]), str(queue["running"]), queue["state"]),
            )
            for queue in cluster_state["queues"]
        )
        document = cls(
            copy.deepcopy(cluster_state["nodes"]),
            jobs_dict,
            queues_dict,
            totals["running_jobs"],
            totals["queued_jobs"],
        )
        document._state = cluster_state
        document._job_metadata = {job["id"]: job for job in cluster_state["jobs"]}
        return document

    @property
    def _diagnostics(self) -> "Dict[str, Dict[str, int]]":
        return {key: cast("Dict[str, int]", value) for key, value in self._state.items() if key.endswith("_diagnostics")} if hasattr(self, "_state") else {}

    def to_cluster_state(self) -> "contracts.ClusterState":
        if hasattr(self, "_state"):
            return copy.deepcopy(self._state)
        # Five-field legacy documents are adapted only at this boundary.
        state: "contracts.ClusterState" = {
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
        with open(filename, "w") as outfile:
            json.dump(self._state if hasattr(self, "_state") else self.to_cluster_state(), outfile)
