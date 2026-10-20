## SPDX-License-Identifier: MIT
"""Static contracts only: importing qtop never requires typing_extensions."""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Any, Dict, List, Optional, Tuple, Union

    from typing_extensions import Protocol, TypedDict

    Config = Dict[str, Any]  # Legacy configuration remains deliberately extensible.
    IdentifierMaps = Dict[str, Dict[str, str]]

    class Job(TypedDict):
        id: str
        user: str
        state: str
        queue: str

    class Node(TypedDict):
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

    class ClusterState(TypedDict):
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
        def get_cluster_state(self, options: Options) -> ClusterState: ...
