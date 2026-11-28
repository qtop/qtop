## Copyright (c) 2026 qtop contributors
## SPDX-License-Identifier: MIT
"""Shared state/configuration types and backend protocol; no typing_extensions runtime dependency."""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from abc import abstractmethod
    from typing import Any, Dict, List, Optional, Tuple, Union

    from typing_extensions import Protocol, TypedDict

    Config = Dict[str, Any]  # Legacy configuration remains deliberately extensible.
    IdentifierMaps = Dict[str, Dict[str, str]]

    class JobResources(TypedDict, total=False):
        """Optional neutral job fields; a backend types its own next to its ExtensionSpec."""

        accounting_state: str
        qid: str  # schema 2 qualified ID "<backend>:<cluster_id>:<id>", unique in the observation
        kind: str  # schema 2: job, pod, vm or task
        parent_qid: str  # schema 3 beta1: same-source workload containment, not controller ownership

    class Job(JobResources):
        """A job: ID, user, state and queue, plus optional scheduler-specific fields."""

        id: str
        user: str
        state: str
        queue: str

    class _AcceleratorKind(TypedDict, total=False):
        kind: str  # "tpu" or "npu"; absent for a GPU

    class GPUResource(_AcceleratorKind):
        """An accelerator count of one type (a GPU unless kind says TPU or NPU), with device indices and MIG details when known."""

        type: Optional[str]
        count: int
        device_indices: Optional[List[int]]
        mig_profile: Optional[str]
        parent_gpu_uuid: Optional[str]

    class GPUBinding(GPUResource):
        """A GPU allocation bound to a job."""

        job_id: str

    class NodeResources(TypedDict, total=False):
        """Optional neutral node fields: accelerators and CPU-placement source (schedulers/resources.py)."""

        gpu_configured: List[GPUResource]
        gpu_used: List[GPUResource]
        gpu_bindings: List[GPUBinding]
        cpu_allocation_source: str  # scheduler, estimated or none
        qid: str  # schema 2 qualified ID "<backend>:<cluster_id>:<id>", unique in the observation

    class Node(NodeResources):
        """A worker node: name, state, core count, queues and core-to-job map."""

        domainname: str
        state: str
        np: Union[int, str]
        qname: List[Optional[str]]
        core_job_map: Dict[Union[int, str], str]

    class QueueIdentity(TypedDict, total=False):
        """The optional schema 2 qualified ID of a scheduling domain."""

        qid: str  # schema 2 qualified ID "<backend>:<cluster_id>:<name>", unique among queues

    class Queue(QueueIdentity):
        """A scheduling domain (queue, partition, ...): name, limit, job counts and state."""

        name: str
        limit: str
        queued: int
        running: int
        state: str

    class RawQueue(TypedDict):
        """A queue record as a backend parses it, before validation."""

        queue_name: str
        lm: Union[int, str]
        queued: Union[int, str]
        run: Union[int, str]
        state: str

    class Source(TypedDict):
        """Where an observation comes from: the backend and a cluster ID chosen by the operator."""

        backend: str
        cluster_id: str

    class Observation(TypedDict, total=False):
        """The schema 2 envelope; a state without it is schema 1."""

        schema_version: int
        schema_revision: str  # required beta1 marker for experimental schema 3
        source: Source

    class ClusterState(Observation):
        """The plain-data boundary between backends and qtop: jobs, nodes and queues (scheduling domains).

        A backend types its own fields (e.g. slurm_diagnostics, slurm_node_state) next to its
        ExtensionSpec, not here; GenericBatchSystem stores diagnostics under the spec's key.
        """

        jobs: List[Job]
        nodes: List[Node]
        queues: List[Queue]

    class SampleSnapshot(TypedDict):
        """The replayable snapshot stored in a support archive."""

        format_version: int
        scheduler: str
        state: ClusterState
        config: Config
        options: Dict[str, object]  # Known display flags are validated; extra metadata may be retained.

    class Options(Protocol):
        """The run options a backend may read: the CLI's namespace, or core_api.BackendOptions.

        A backend's own switches (add_cli_options) are optional and read with getattr.
        """

        SOURCEDIR: Optional[str]

    JobsInfo = Tuple[List[str], List[str], List[str], List[str]]
    QueuesInfo = Tuple[int, int, List[RawQueue]]

    class Scheduler(Protocol):
        """What qtop needs from a scheduler backend."""

        optional_trace_keys: Tuple[str, ...]

        @abstractmethod
        def mask_source_trace(self, key: str, text: str, mask: object) -> Tuple[str, str]:
            """Return the archive name and masked text of a trace."""

        @abstractmethod
        def get_cluster_state(self, options: Options) -> ClusterState:
            """Collect the scheduler's jobs, nodes and queues as a ClusterState."""
