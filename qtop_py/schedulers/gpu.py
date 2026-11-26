## Copyright (c) 2026 qtop contributors
## SPDX-License-Identifier: MIT
"""Slurm accelerator metadata (GPU, TPU, NPU); resource indices are not CUDA or physical device IDs.

A resource's optional "kind" names a TPU or NPU; without it the resource is a GPU, so snapshots
from before TPU/NPU support keep their meaning.
"""

import re
from typing import TYPE_CHECKING, Any, Dict, List, Mapping, Optional, Sequence, Tuple

from qtop_py.core_hostlist import HostlistParser

if TYPE_CHECKING:
    from typing import Final

    from qtop_py.core_state_types import GPUResource

MAX_GPU_INDICES: "Final" = 100000
ACCELERATOR_KINDS: "Final" = ("gpu", "tpu", "npu")  # Slurm GRES names, in display order


def resource_kind(resource: "Mapping[str, Any]") -> str:
    """Return the accelerator kind of a resource: its "kind", or "gpu" when it has none."""
    return str(resource.get("kind", "gpu"))


def _pair_inventory(mapping: object) -> Dict[str, List[List[int]]]:
    """Validate experimental_gpu_pairs and expand it to {host: pairs}."""
    if not isinstance(mapping, dict):
        raise ValueError("experimental_gpu_pairs must be a node-to-pairs mapping")
    inventory = {}
    for pattern, pairs in mapping.items():
        hosts = HostlistParser.expand_nodelist(pattern) if isinstance(pattern, str) else []
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
        if len(inventory) > HostlistParser.MAX_EXPANDED_NODES:
            raise ValueError("Too many GPU pair nodes")
    return inventory


def configured_pair_lines(nodes: Sequence[Mapping[str, Any]], mapping: object) -> List[str]:
    """Experimental display of administrator-configured pairs, not discovered links."""
    if not mapping:
        return []
    inventory = _pair_inventory(mapping)
    lines = []
    for node in nodes:
        pairs = inventory.get(node["domainname"], [])
        if not pairs:
            continue
        labels = []
        for number, pair in enumerate(pairs, 1):
            devices = ["gpu%s=%s" % (index, device_status(node, "gpu", index)) for index in pair]
            labels.append("pair%s[%s]" % (number, ", ".join(devices)))
        lines.append("%s: %s" % (node["domainname"], " ".join(labels)))
    return lines


def _gres_indices(detail: Optional[str], count: int, kind: str) -> Optional[List[int]]:
    """Return the device indices of an IDX: detail, or None when Slurm does not say (IDX:N/A, S:..., none)."""
    if not detail or detail == "IDX:N/A" or re.fullmatch(r"S:[0-9,-]+", detail):
        return None
    if not detail.startswith("IDX:"):
        raise ValueError("Unrecognized %s GRES detail" % kind.upper())
    indices: List[int] = []
    for token in detail[4:].split(","):
        span = re.fullmatch(r"([0-9]{1,10})(?:-([0-9]{1,10}))?", token)
        if span is None:
            raise ValueError("Invalid %s index range" % kind.upper())
        first = int(span.group(1))
        last = int(span.group(2)) if span.group(2) is not None else first
        if last < first or len(indices) + last - first + 1 > MAX_GPU_INDICES:
            raise ValueError("Invalid or oversized %s index range" % kind.upper())
        indices.extend(range(first, last + 1))
    if len(indices) != count or len(set(indices)) != len(indices):
        raise ValueError("%s count and indices disagree" % kind.upper())
    return indices


def _gres_resource(entry: str, kind: str) -> "GPUResource":
    """Parse one kind:[type:]count[(detail)] GRES entry."""
    match = re.fullmatch(r"[a-z]{3}:(?:([^:(),\s]+):)?([0-9]{1,10})(?:\(([^()]*)\))?", entry)
    if match is None:
        raise ValueError("Invalid %s GRES entry" % kind.upper())
    gres_type, count_text, detail = match.groups()
    count = int(count_text)
    profile = re.search(r"(?:^|[_-])([1-9][0-9]*g\.[1-9][0-9]*gb)(?:$|[_-])", gres_type or "", re.IGNORECASE) if kind == "gpu" else None
    resource: "GPUResource" = {
        "type": gres_type,
        "count": count,
        "device_indices": _gres_indices(detail, count, kind),
        "mig_profile": profile.group(1).lower() if profile else None,
        "parent_gpu_uuid": None,
    }
    if kind != "gpu":
        resource["kind"] = kind
    return resource


def parse_accelerator_gres(value: str) -> "List[GPUResource]":
    """Parse the gpu, tpu and npu entries of a Slurm GRES string; keep unknown indices and MIG parent identities explicit."""
    resources: "List[GPUResource]" = []
    for entry in re.split(r",(?![^()]*\))", value):
        kind = entry.split(":", 1)[0]
        if kind in ACCELERATOR_KINDS and ":" in entry:
            resources.append(_gres_resource(entry, kind))
    return resources


parse_gpu_gres = parse_accelerator_gres  # the name from before TPU and NPU support


def node_devices(node: "Mapping[str, Any]", kind: str) -> List[int]:
    """Return the whole-device indices of a kind configured on a node (MIG slices excluded).

    Configured GRES without indices, e.g. gpu:a100:4(S:0-1), number their devices from 0, as Slurm does.
    """
    devices: List[int] = []
    for resource in node.get("slurm_gpu_configured") or []:
        if resource_kind(resource) != kind or resource["mig_profile"] is not None:
            continue
        indices = resource["device_indices"]
        if indices is None:
            start = max(devices) + 1 if devices else 0
            indices = list(range(start, start + min(resource["count"], MAX_GPU_INDICES)))
        devices.extend(index for index in indices if index not in devices)
    return sorted(devices)


def node_slices(node: "Mapping[str, Any]", kind: str) -> List[int]:
    """Return the MIG slice indices of a kind configured on a node; slices without indices follow its whole devices."""
    taken = node_devices(node, kind)
    slices: List[int] = []
    for resource in node.get("slurm_gpu_configured") or []:
        if resource_kind(resource) != kind or resource["mig_profile"] is None:
            continue
        indices = resource["device_indices"]
        if indices is None:
            start = max(taken + slices) + 1 if taken or slices else 0
            indices = list(range(start, start + min(resource["count"], MAX_GPU_INDICES)))
        slices.extend(index for index in indices if index not in slices and index not in taken)
    return sorted(slices)


def slice_jobs(node: "Mapping[str, Any]", kind: str, index: int) -> List[str]:
    """The IDs of the jobs whose GRES indices on the node include the MIG slice."""
    bindings = node.get("slurm_gpu_bindings", [])
    return sorted({b["job_id"] for b in bindings if resource_kind(b) == kind and b.get("mig_profile") is not None and index in (b.get("device_indices") or [])})


def _device_jobs(node: "Mapping[str, Any]", kind: str, index: int) -> List[str]:
    """The IDs of the jobs whose GRES indices on the node include the whole device."""
    bindings = node.get("slurm_gpu_bindings", [])
    return sorted({b["job_id"] for b in bindings if resource_kind(b) == kind and b.get("mig_profile") is None and index in (b.get("device_indices") or [])})


def _whole_device_usage(node: "Mapping[str, Any]", kind: str) -> "Tuple[bool, List[int]]":
    """Whether Slurm's usage of a kind on the node names its whole devices, and the indices it lists as in use.

    Only whole-device entries say which whole devices are idle: MIG slices in use do not, since they may sit on any
    of them, but beside a whole-device entry they say nothing of the whole devices it covers.
    """
    used = [resource for resource in node.get("slurm_gpu_used") or [] if resource_kind(resource) == kind]
    whole = [resource for resource in used if resource["mig_profile"] is None]
    known = bool(whole) and all(resource["count"] == 0 or resource["device_indices"] is not None for resource in whole)
    return known, [index for resource in whole for index in resource["device_indices"] or []]


def device_status(node: "Mapping[str, Any]", kind: str, index: int) -> str:
    """Return who holds a device: its job IDs joined by /, "idle", or "?" when Slurm's usage does not say."""
    jobs = _device_jobs(node, kind, index)
    if jobs:
        return "/".join(jobs)
    known, in_use = _whole_device_usage(node, kind)
    return "idle" if known and index not in in_use else "?"


def held_by_unseen_job(node: "Mapping[str, Any]", kind: str, index: int) -> bool:
    """True for a "?" device that Slurm lists in use, but that no job in the squeue and scontrol details holds."""
    return not _device_jobs(node, kind, index) and index in _whole_device_usage(node, kind)[1]


def _runs(devices: Sequence[int], statuses: Sequence[str]) -> List[str]:
    """Join consecutive devices of equal status: [0, 1, 2], [a, a, b] -> ["0-1=a", "2=b"]."""
    labels: List[str] = []
    start = 0
    for position in range(1, len(devices) + 1):
        if position < len(devices) and statuses[position] == statuses[start] and devices[position] == devices[position - 1] + 1:
            continue
        first, last = devices[start], devices[position - 1]
        labels.append(("%s-%s" % (first, last) if last != first else str(first)) + "=" + statuses[start])
        start = position
    return labels


def accelerator_map_lines(nodes: Sequence[Mapping[str, Any]], kinds: Sequence[str] = ACCELERATOR_KINDS) -> List[str]:
    """One line per node with accelerators: each GPU, TPU and NPU with its jobs, e.g. "n1: gpu0-1=101 gpu2-7=idle".

    Any number of devices; consecutive devices of the same status share one label. kinds limits the
    kinds shown (matrix_accelerators).
    """
    lines = []
    for node in nodes:
        labels: List[str] = []
        for kind in (kind for kind in ACCELERATOR_KINDS if kind in kinds):
            devices = node_devices(node, kind)
            labels.extend(kind + label for label in _runs(devices, [device_status(node, kind, index) for index in devices]))
        if labels:
            lines.append("%s: %s" % (node["domainname"], " ".join(labels)))
    return lines


def _validate_text_fields(resource: Mapping[str, Any]) -> None:
    """type, mig_profile and parent_gpu_uuid are text or null; a MIG profile looks like 1g.10gb."""
    for field in ("type", "mig_profile", "parent_gpu_uuid"):
        if field not in resource or (resource[field] is not None and not isinstance(resource[field], str)):
            raise ValueError("GPU %s must be text or null" % field)
    profile = resource["mig_profile"]
    if profile is not None and not re.fullmatch(r"[1-9][0-9]*g\.[1-9][0-9]*gb", profile):
        raise ValueError("Invalid MIG profile")


def _validate_indices(resource: Mapping[str, Any]) -> None:
    """device_indices is null, or count distinct nonnegative integers."""
    if "device_indices" not in resource:
        raise ValueError("GPU device_indices is required")
    indices = resource["device_indices"]
    if indices is None:
        return
    if not isinstance(indices, list) or len(indices) > MAX_GPU_INDICES or any(type(index) is not int or index < 0 for index in indices):
        raise ValueError("GPU device_indices must be nonnegative integers or null")
    if len(indices) != resource["count"] or len(set(indices)) != len(indices):
        raise ValueError("GPU count and indices disagree")


def _validate_kind(resource: Mapping[str, Any]) -> None:
    """An optional kind is tpu, npu or gpu; only GPUs have MIG profiles."""
    if "kind" in resource and resource["kind"] not in ACCELERATOR_KINDS:
        raise ValueError("Accelerator kind must be one of %s" % ", ".join(ACCELERATOR_KINDS))
    if resource["mig_profile"] is not None and resource_kind(resource) != "gpu":
        raise ValueError("Only GPUs have MIG profiles")


def validate_gpu_resources(value: object, binding: bool = False) -> None:
    """Validate structured accelerator metadata read from portable snapshots; bindings also need a job_id."""
    if not isinstance(value, list):
        raise ValueError("GPU resources must be a list")
    for resource in value:
        if not isinstance(resource, dict):
            raise ValueError("GPU resource must be an object")
        if type(resource.get("count")) is not int or resource["count"] < 0:
            raise ValueError("GPU count must be a nonnegative integer")
        _validate_text_fields(resource)
        _validate_indices(resource)
        _validate_kind(resource)
        if binding and (not isinstance(resource.get("job_id"), str) or not resource["job_id"]):
            raise ValueError("GPU binding requires a job_id")
