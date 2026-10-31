## SPDX-License-Identifier: MIT
"""Backend-owned snapshot metadata rules; no runtime/plugin registry imports."""

import copy
import re
from typing import TYPE_CHECKING, Any, Callable, Dict, Mapping, cast

if TYPE_CHECKING:
    from qtop_py.contracts import Job, Node

from qtop_py.plugins.gpu import validate_gpu_resources

DIAGNOSTICS = {
    "slurm_diagnostics": ("Slurm", ("rejected_squeue_rows", "rejected_sinfo_rows", "jobs_with_missing_nodes", "jobs_with_invalid_nodelists")),
    "oar_diagnostics": ("OAR", ("missing_resources", "extra_resources", "unlisted_jobs", "shared_resources")),
}
JOB_IDENTITIES = (("slurm_array_job_id", "slurm_array_task_id", "_"), ("slurm_het_job_id", "slurm_het_job_offset", "+"))
NODE_JOB_FIELDS = ("slurm_job_ids", "oar_job_ids")
CLASSIC_REPORT_NAME = "PBS"


def validate_diagnostics(data: Dict[str, object]) -> None:
    for name, (label, keys) in DIAGNOSTICS.items():
        if name not in data:
            continue
        diagnostics = data[name]
        if not isinstance(diagnostics, dict) or not all(isinstance(key, str) for key in diagnostics):
            raise TypeError("%s diagnostics must be an object with string keys" % label)
        if any(key not in keys or type(count) is not int or count < 0 for key, count in diagnostics.items()):
            raise ValueError(label + " diagnostics require known counters with nonnegative integer values")


def validate_record_extensions(kind: str, record: Dict[str, object]) -> None:
    for field in ("slurm_partitions",) if kind == "jobs" else NODE_JOB_FIELDS if kind == "nodes" else ():
        if field in record:
            values = record[field]
            if not isinstance(values, list) or (field == "slurm_partitions" and not values) or not all(isinstance(value, str) and value for value in values):
                raise ValueError("%s must be a list of nonempty strings" % field)
            if field == "slurm_partitions" and record["queue"] not in values:
                raise ValueError("job queue must belong to slurm_partitions")
    if kind == "jobs":
        if all(parent in record for parent, _, _ in JOB_IDENTITIES):
            raise ValueError("job cannot have both Slurm array and heterogeneous identities")
        for parent, child, separator in JOB_IDENTITIES:
            if parent in record or child in record:
                base, index = record.get(parent), record.get(child)
                if not isinstance(base, str) or not isinstance(index, str) or not re.fullmatch(r"[0-9]+", base) or not re.fullmatch(r"[0-9]+", index):
                    raise ValueError("job Slurm identity requires a numeric parent and task/component ID")
                if record["id"] != base + separator + index:
                    raise ValueError("job Slurm identity conflicts with canonical ID")
    if kind == "nodes":
        if "slurm_node_state" in record and not isinstance(record["slurm_node_state"], str):
            raise ValueError("slurm_node_state must be text")
        for field in ("slurm_gpu_configured", "slurm_gpu_used", "slurm_gpu_bindings"):
            if field in record:
                validate_gpu_resources(record[field], binding=field == "slurm_gpu_bindings")
        if "slurm_unmapped_cpus" in record and (type(record["slurm_unmapped_cpus"]) is not int or record["slurm_unmapped_cpus"] < 0):
            raise ValueError("slurm_unmapped_cpus must be a nonnegative integer")
        if "cpu_allocation_source" in record and record["cpu_allocation_source"] not in ("slurm", "estimated", "none"):
            raise ValueError("node cpu_allocation_source must be slurm, estimated or none")


def mask_job_extensions(original: Mapping[str, Any], target: "Job", mask: Callable[[str, str], str]) -> None:
    job = cast(Dict[str, object], target)
    if "slurm_partitions" in original:
        job["slurm_partitions"] = [mask(name, "queue") for name in original["slurm_partitions"]]
    for parent, child, _ in JOB_IDENTITIES:
        if parent in original:
            job[parent], job[child] = mask(original[parent], "job"), original.get(child, "")


def mask_node_extensions(original: Mapping[str, Any], target: "Node", mask_job: Callable[[str], str]) -> None:
    node = cast(Dict[str, Any], target)
    node.update({field: original[field] for field in ("slurm_node_state", "slurm_unmapped_cpus", "cpu_allocation_source") if field in original})
    node.update({field: [mask_job(value) for value in original[field]] for field in NODE_JOB_FIELDS if field in original})
    for field in ("slurm_gpu_configured", "slurm_gpu_used", "slurm_gpu_bindings"):
        if field in original:
            node[field] = copy.deepcopy(original[field])
            for resource in node[field]:
                resource["type"] = resource["parent_gpu_uuid"] = None
                if field == "slurm_gpu_bindings":
                    resource["job_id"] = mask_job(resource["job_id"])


def node_is_idle(node: Mapping[str, Any]) -> bool:
    available = node["state"] == "-" and not node["core_job_map"] and not any(node.get(field) for field in NODE_JOB_FIELDS)
    return available and node.get("slurm_node_state", "idle").lower() == "idle" and not any(resource["count"] for resource in node.get("slurm_gpu_used", []))
