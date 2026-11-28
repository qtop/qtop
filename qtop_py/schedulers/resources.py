## Copyright (c) 2026 qtop contributors
## SPDX-License-Identifier: MIT
"""Scheduler-neutral node resources: accelerators (gpu_*) and CPU-placement provenance.

Any backend may set these node fields; qtop validates, masks (-A), counts and displays them the same
way for all. States written before they were neutral used Slurm names (slurm_gpu_*, and the
cpu_allocation_source value slurm); upgrade_node() renames those on the way in.
"""

import copy
from typing import Any, Callable, Dict, List, Mapping, Sequence

from qtop_py.core_errors import SchedulerInputError
from qtop_py.schedulers.gpu import ACCELERATOR_KINDS, accelerator_map_lines, validate_gpu_resources

GPU_FIELDS = ("gpu_configured", "gpu_used", "gpu_bindings")
LEGACY_GPU_FIELDS = {"slurm_" + field: field for field in GPU_FIELDS}
## exact: the scheduler names each job's CPU IDs; estimated: qtop placed the jobs on the node's cores;
## none: no CPU placement (jobs are listed without cores).
CPU_ALLOCATION_SOURCES = ("scheduler", "estimated", "none")
LEGACY_CPU_ALLOCATION_SOURCES = {"slurm": "scheduler"}


def upgrade_node(record: Dict[str, object]) -> None:
    """Rename a node's pre-neutral fields in place: slurm_gpu_* to gpu_*, cpu_allocation_source slurm to scheduler."""
    for legacy, field in LEGACY_GPU_FIELDS.items():
        if legacy in record:
            if field in record:
                raise SchedulerInputError("node has both %s and %s" % (legacy, field))
            record[field] = record.pop(legacy)
    source = record.get("cpu_allocation_source")
    if isinstance(source, str) and source in LEGACY_CPU_ALLOCATION_SOURCES:
        record["cpu_allocation_source"] = LEGACY_CPU_ALLOCATION_SOURCES[source]


def validate_node(record: Mapping[str, object]) -> None:
    """Raise SchedulerInputError if a node's accelerator lists or CPU-placement source are malformed."""
    for field in GPU_FIELDS:
        if field in record:
            validate_gpu_resources(record[field], binding=field == "gpu_bindings")
    if "cpu_allocation_source" in record and record["cpu_allocation_source"] not in CPU_ALLOCATION_SOURCES:
        raise SchedulerInputError("node cpu_allocation_source must be one of %s" % ", ".join(CPU_ALLOCATION_SOURCES))


def mask_node(original: Mapping[str, Any], node: Dict[str, Any], mask_job_id: Callable[[str], str]) -> None:
    """Copy the placement source and accelerators to node, with device types, UUIDs and bound jobs masked."""
    if "cpu_allocation_source" in original:
        node["cpu_allocation_source"] = original["cpu_allocation_source"]
    for field in GPU_FIELDS:
        if field in original:
            node[field] = copy.deepcopy(original[field])
            for resource in node[field]:
                resource["type"] = resource["parent_gpu_uuid"] = None
                if field == "gpu_bindings":
                    resource["job_id"] = mask_job_id(resource["job_id"])


def node_busy(node: Mapping[str, Any]) -> bool:
    """True if any of the node's accelerators is in use."""
    return any(resource["count"] for resource in node.get("gpu_used", []))


def messages(section: str, nodes: Sequence[Mapping[str, Any]], config: Mapping[str, Any]) -> List[str]:
    """Notes every backend gets from these fields: CPU placement (placement), accelerators by node (accelerators)."""
    if section == "placement":
        sources = {node.get("cpu_allocation_source") for node in nodes}
        lines = []
        if "estimated" in sources:
            lines.append("WARNING: CPU placement: estimated where allocation details are unavailable.")
        if "scheduler" in sources:
            lines.append("INFO: CPU placement: scheduler CPU IDs; Linux task affinity is not shown.")
        return lines
    if section == "accelerators":
        lines = accelerator_map_lines(nodes, config.get("matrix_accelerators", ACCELERATOR_KINDS))
        return ["INFO: Accelerators by node (job IDs, ? = unknown):"] + ["INFO: " + line for line in lines] if lines else []
    return []
