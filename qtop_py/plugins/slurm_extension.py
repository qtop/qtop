## Copyright (c) 2026 qtop contributors
## SPDX-License-Identifier: MIT
"""Slurm's extension fields (slurm_*) in a cluster state: owned by the slurm and slurm_json plugins."""

from typing import TYPE_CHECKING, Any, Dict, List, Mapping, cast

from qtop_py.core_api import SchedulerInputError
from qtop_py.schedulers.extensions import ExtensionSpec, Mask, MaskJob, nonempty_texts

if TYPE_CHECKING:  # typing.Final is Python 3.8+; the quoted annotations keep 3.6 working
    from typing import Final

    from qtop_py.core_state_types import Job, Node

    class SlurmJob(Job, total=False):
        """A job with the slurm_* fields this extension validates and masks."""

        slurm_partitions: List[str]
        slurm_array_job_id: str
        slurm_array_task_id: str
        slurm_het_job_id: str
        slurm_het_job_offset: str
        slurm_tres_alloc: str
        slurm_tres_per_node: str
        slurm_tres_per_task: str

    class SlurmNode(Node, total=False):
        """A node with Slurm's state, topology block, extra job list, unmapped CPUs and GRES text."""

        slurm_node_state: str
        slurm_block: str
        slurm_job_ids: List[str]
        slurm_unmapped_cpus: int
        slurm_gres: str
        slurm_gres_used: str


NODE_JOB_FIELDS: "Final" = ("slurm_job_ids",)
JOB_IDENTITIES: "Final" = (("slurm_array_job_id", "slurm_array_task_id", "_"), ("slurm_het_job_id", "slurm_het_job_offset", "+"))


def validate_job(record: Dict[str, object]) -> None:
    """Raise ValueError if slurm_partitions is malformed or omits the job's queue."""
    if "slurm_partitions" in record:
        values = record["slurm_partitions"]
        if not isinstance(values, list) or not values or not nonempty_texts(cast(List[object], values)):
            raise SchedulerInputError("slurm_partitions must be a list of nonempty strings")
        if record["queue"] not in values:
            raise SchedulerInputError("job queue must belong to slurm_partitions")


def validate_node(record: Dict[str, object]) -> None:
    """Raise ValueError if a node's Slurm state, block or unmapped-CPU count is malformed."""
    if "slurm_node_state" in record and not isinstance(record["slurm_node_state"], str):
        raise SchedulerInputError("slurm_node_state must be text")
    if "slurm_block" in record and not nonempty_texts([record["slurm_block"]]):
        raise SchedulerInputError("slurm_block must be nonempty text")
    if "slurm_unmapped_cpus" in record and (type(record["slurm_unmapped_cpus"]) is not int or record["slurm_unmapped_cpus"] < 0):
        raise SchedulerInputError("slurm_unmapped_cpus must be a nonnegative integer")


def mask_job(original: Mapping[str, Any], job: Dict[str, object], mask: Mask) -> None:
    """Copy partitions (masked as queues) and array/het identities (parent masked as a job)."""
    if "slurm_partitions" in original:
        job["slurm_partitions"] = [mask(name, "queue") for name in original["slurm_partitions"]]
    for parent, child, _ in JOB_IDENTITIES:
        if parent in original:
            job[parent], job[child] = mask(original[parent], "job"), original.get(child, "")


def mask_node(original: Mapping[str, Any], node: Dict[str, Any], mask_job_id: MaskJob) -> None:
    """Copy node state and CPU fields and job IDs (masked); accelerators are neutral (schedulers/resources.py)."""
    node.update({field: original[field] for field in ("slurm_node_state", "slurm_unmapped_cpus") if field in original})
    node.update({field: [mask_job_id(value) for value in original[field]] for field in NODE_JOB_FIELDS if field in original})


def mask_node_names(original: Mapping[str, Any], node: Dict[str, Any], mask: Mask) -> None:
    """Copy the node's topology block (its NVLink domain), masked as a block name."""
    if "slurm_block" in original:
        node["slurm_block"] = mask(original["slurm_block"], "block")


def node_busy(node: Mapping[str, Any]) -> bool:
    """True if Slurm reports the node not idle (accelerators in use are a neutral test)."""
    return str(node.get("slurm_node_state", "idle")).lower() != "idle"


SLURM_EXTENSION: "Final" = ExtensionSpec(
    "Slurm",
    # all_partitions: 1 when the commands ran with --all; jobs_from_scontrol: running jobs squeue missed (listed from
    # scontrol); the other counters make a snapshot incomplete
    diagnostics=(
        "slurm_diagnostics",
        ("rejected_squeue_rows", "rejected_sinfo_rows", "jobs_with_missing_nodes", "jobs_with_invalid_nodelists", "all_partitions", "jobs_from_scontrol"),
    ),
    node_job_fields=NODE_JOB_FIELDS,
    job_identities=JOB_IDENTITIES,
    validate_job=validate_job,
    validate_node=validate_node,
    mask_job=mask_job,
    mask_node=mask_node,
    node_busy=node_busy,
    mask_node_names=mask_node_names,
)
