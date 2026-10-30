## SPDX-License-Identifier: MIT
"""Static contracts only: importing qtop never requires typing_extensions."""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from abc import abstractmethod
    from typing import Any, Dict, List, Optional, Tuple, Union

    from typing_extensions import Protocol, TypedDict

    Config = Dict[str, Any]  # Legacy configuration remains deliberately extensible.
    IdentifierMaps = Dict[str, Dict[str, str]]

    class JobResources(TypedDict, total=False):
        slurm_partitions: List[str]
        accounting_state: str
        slurm_array_job_id: str
        slurm_array_task_id: str
        slurm_het_job_id: str
        slurm_het_job_offset: str
        slurm_tres_alloc: str
        slurm_tres_per_node: str
        slurm_tres_per_task: str

    class Job(JobResources):
        id: str
        user: str
        state: str
        queue: str

    class GPUResource(TypedDict):
        type: Optional[str]
        count: int
        device_indices: Optional[List[int]]
        mig_profile: Optional[str]
        parent_gpu_uuid: Optional[str]

    class GPUBinding(GPUResource):
        job_id: str

    class NodeResources(TypedDict, total=False):
        slurm_node_state: str
        slurm_gpu_configured: List[GPUResource]
        slurm_gpu_used: List[GPUResource]
        slurm_gpu_bindings: List[GPUBinding]
        slurm_job_ids: List[str]
        slurm_unmapped_cpus: int
        cpu_allocation_source: str
        slurm_gres: str
        slurm_gres_used: str

    class Node(NodeResources):
        domainname: str
        state: str
        np: Union[int, str]
        qname: List[Optional[str]]
        core_job_map: Dict[Union[int, str], str]

    class Queue(TypedDict):
        name: str
        limit: str
        queued: int
        running: int
        state: str

    class RawQueue(TypedDict):
        queue_name: str
        lm: Union[int, str]
        queued: Union[int, str]
        run: Union[int, str]
        state: str

    class SnapshotDiagnostics(TypedDict, total=False):
        slurm_diagnostics: Dict[str, int]

    class ClusterState(SnapshotDiagnostics):
        jobs: List[Job]
        nodes: List[Node]
        queues: List[Queue]

    class Snapshot(TypedDict):
        format_version: int
        scheduler: str
        state: ClusterState
        config: Config
        options: Dict[str, object]  # Known display flags are validated; extra metadata may be retained.

    class Options(Protocol):
        ANONYMIZE: bool

    JobsInfo = Tuple[List[str], List[str], List[str], List[str]]
    QueuesInfo = Tuple[int, int, List[RawQueue]]

    class Scheduler(Protocol):
        @abstractmethod
        def get_cluster_state(self, options: Options) -> ClusterState:
            pass
