# SPDX-License-Identifier: MIT
"""Slurm GPU metadata; resource indices are not CUDA or physical GPU IDs."""

import re
from typing import TYPE_CHECKING, Any, Dict, List

if TYPE_CHECKING:
    from qtop_py.contracts import GPUResource

MAX_GPU_INDICES = 100000


def configured_pair_lines(nodes: List[Dict[str, Any]], mapping: object) -> List[str]:
    """Experimental display of administrator-configured pairs, not discovered links."""
    if not mapping:
        return []
    # Reuse the bounded hostlist parser without importing the scheduler when disabled.
    from qtop_py.plugins.slurm import SlurmBatchSystem

    if not isinstance(mapping, dict):
        raise ValueError("experimental_gpu_pairs must be a node-to-pairs mapping")
    inventory = {}
    for pattern, pairs in mapping.items():
        hosts = SlurmBatchSystem.expand_nodelist(pattern) if isinstance(pattern, str) else []
        if not hosts or not isinstance(pairs, list) or not pairs:
            raise ValueError("GPU pairs require a valid node range and nonempty pair list")
        indices = []
        for pair in pairs:
            if not isinstance(pair, list) or len(pair) != 2 or any(type(index) is not int or index < 0 for index in pair):
                raise ValueError("Each GPU pair must contain two nonnegative integer indices")
            indices.extend(pair)
        if len(set(indices)) != len(indices):
            raise ValueError("Configured GPU pairs must not overlap")
        for host in hosts:
            if host in inventory:
                raise ValueError("GPU pair node ranges must not overlap")
            inventory[host] = pairs
        if len(inventory) > SlurmBatchSystem.MAX_EXPANDED_NODES:
            raise ValueError("Too many GPU pair nodes")
    lines = []
    for node in nodes:
        pairs = inventory.get(node["domainname"], [])
        if not pairs:
            continue
        owners = {}  # type: Dict[int, List[str]]
        for binding in node.get("slurm_gpu_bindings", []):
            if binding.get("mig_profile") is not None:
                continue
            for index in binding.get("device_indices") or []:
                owners.setdefault(index, []).append(binding["job_id"])
        used = node.get("slurm_gpu_used")
        known_usage = bool(used) and all(resource["count"] == 0 or (resource["device_indices"] is not None and resource["mig_profile"] is None) for resource in used or [])
        busy = {index for resource in used or [] for index in resource["device_indices"] or []}
        labels = []
        for number, pair in enumerate(pairs, 1):
            devices = []
            for index in pair:
                jobs = sorted(set(owners.get(index, [])))
                status = "/".join(jobs) if jobs else "idle" if known_usage and index not in busy else "?"
                devices.append("gpu%s=%s" % (index, status))
            labels.append("pair%s[%s]" % (number, ", ".join(devices)))
        lines.append("%s: %s" % (node["domainname"], " ".join(labels)))
    return lines


def parse_gpu_gres(value: str) -> "List[GPUResource]":
    """Keep unknown indices and MIG parent identities explicit."""
    resources: "List[GPUResource]" = []
    for entry in re.split(r",(?![^()]*\))", value):
        if not entry.startswith("gpu:"):
            continue
        match = re.fullmatch(r"gpu:(?:([^:(),\s]+):)?([0-9]{1,10})(?:\(([^()]*)\))?", entry)
        if match is None:
            raise ValueError("Invalid GPU GRES entry")
        gres_type, count_text, detail = match.groups()
        count = int(count_text)
        indices = None
        if detail and detail.startswith("IDX:") and detail != "IDX:N/A":
            indices = []
            for token in detail[4:].split(","):
                span = re.fullmatch(r"([0-9]{1,10})(?:-([0-9]{1,10}))?", token)
                if span is None:
                    raise ValueError("Invalid GPU index range")
                first = int(span.group(1))
                last = int(span.group(2)) if span.group(2) is not None else first
                if last < first or len(indices) + last - first + 1 > MAX_GPU_INDICES:
                    raise ValueError("Invalid or oversized GPU index range")
                indices.extend(range(first, last + 1))
            if len(indices) != count or len(set(indices)) != len(indices):
                raise ValueError("GPU count and indices disagree")
        elif detail and not (detail == "IDX:N/A" or re.fullmatch(r"S:[0-9,-]+", detail)):
            raise ValueError("Unrecognized GPU GRES detail")
        profile = re.search(r"(?:^|[_-])([1-9][0-9]*g\.[1-9][0-9]*gb)(?:$|[_-])", gres_type or "", re.IGNORECASE)
        resources.append(
            {
                "type": gres_type,
                "count": count,
                "device_indices": indices,
                "mig_profile": profile.group(1).lower() if profile else None,
                "parent_gpu_uuid": None,
            }
        )
    return resources


def validate_gpu_resources(value: object, binding: bool = False) -> None:
    """Validate structured GPU metadata read from portable snapshots."""
    if not isinstance(value, list):
        raise ValueError("GPU resources must be a list")
    for resource in value:
        if not isinstance(resource, dict):
            raise ValueError("GPU resource must be an object")
        if type(resource.get("count")) is not int or resource["count"] < 0:
            raise ValueError("GPU count must be a nonnegative integer")
        for field in ("type", "mig_profile", "parent_gpu_uuid"):
            if field not in resource or (resource[field] is not None and not isinstance(resource[field], str)):
                raise ValueError("GPU %s must be text or null" % field)
        profile = resource["mig_profile"]
        if profile is not None and not re.fullmatch(r"[1-9][0-9]*g\.[1-9][0-9]*gb", profile):
            raise ValueError("Invalid MIG profile")
        if "device_indices" not in resource:
            raise ValueError("GPU device_indices is required")
        indices = resource["device_indices"]
        if indices is not None:
            if not isinstance(indices, list) or len(indices) > MAX_GPU_INDICES or any(type(index) is not int or index < 0 for index in indices):
                raise ValueError("GPU device_indices must be nonnegative integers or null")
            if len(indices) != resource["count"] or len(set(indices)) != len(indices):
                raise ValueError("GPU count and indices disagree")
        if binding and (not isinstance(resource.get("job_id"), str) or not resource["job_id"]):
            raise ValueError("GPU binding requires a job_id")
