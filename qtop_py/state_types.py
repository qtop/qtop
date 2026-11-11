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
        """Optional scheduler-specific job fields (Slurm partitions, arrays, TRES)."""

        slurm_partitions: List[str]
        accounting_state: str
        slurm_array_job_id: str
        slurm_array_task_id: str
        slurm_het_job_id: str
        slurm_het_job_offset: str
        slurm_tres_alloc: str
        slurm_tres_per_node: str
        slurm_tres_per_task: str
        qid: str  # schema 2 qualified ID "<backend>:<cluster_id>:<id>", unique in the observation
        kind: str  # schema 2: job, pod, vm or task

    class Job(JobResources):
        """A job: ID, user, state and queue, plus optional scheduler-specific fields."""

        id: str
        user: str
        state: str
        queue: str

    class GPUResource(TypedDict):
        """A GPU count of one type, with device indices and MIG details when known."""

        type: Optional[str]
        count: int
        device_indices: Optional[List[int]]
        mig_profile: Optional[str]
        parent_gpu_uuid: Optional[str]

    class GPUBinding(GPUResource):
        """A GPU allocation bound to a job."""

        job_id: str

    class NodeResources(TypedDict, total=False):
        """Optional scheduler-specific node fields (Slurm state and GPUs, OAR jobs, CPU placement)."""

        slurm_node_state: str
        slurm_gpu_configured: List[GPUResource]
        slurm_gpu_used: List[GPUResource]
        slurm_gpu_bindings: List[GPUBinding]
        slurm_job_ids: List[str]
        oar_job_ids: List[str]
        slurm_unmapped_cpus: int
        cpu_allocation_source: str
        slurm_gres: str
        slurm_gres_used: str
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

    class SnapshotDiagnostics(TypedDict, total=False):
        """Per-scheduler counts of parser notes, e.g. rejected rows."""

        slurm_diagnostics: Dict[str, int]
        oar_diagnostics: Dict[str, int]

    class Source(TypedDict):
        """Where an observation comes from: the backend and a cluster ID chosen by the operator."""

        backend: str
        cluster_id: str

    class Observation(TypedDict, total=False):
        """The schema 2 envelope; a state without it is schema 1."""

        schema_version: int
        source: Source

    class ClusterState(SnapshotDiagnostics, Observation):
        """The plain-data boundary between backends and qtop: jobs, nodes and queues (scheduling domains)."""

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
        """The command-line options a backend may read."""

        ANONYMIZE: bool

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
