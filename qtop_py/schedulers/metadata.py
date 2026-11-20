## Copyright (c) 2026 qtop contributors
## SPDX-License-Identifier: MIT
"""Scheduler extension rules for cluster states, combined from each backend's ExtensionSpec.

Validation, -A masking and the idle test loop over EXTENSIONS; nothing here names a scheduler.
A new backend with extension fields adds its spec module and one entry below; the plugin
conformance test fails if a registered plugin's spec is missing. No runtime/plugin imports.
"""

from typing import TYPE_CHECKING, Any, Callable, Dict, Mapping, Tuple, cast

if TYPE_CHECKING:
    from typing import Final

    from qtop_py.state_types import Job, Node

from qtop_py.schedulers.extensions import ExtensionSpec, validate_identities, validate_text_lists
from qtop_py.schedulers.oar_extension import OAR_EXTENSION
from qtop_py.schedulers.slurm_extension import SLURM_EXTENSION

EXTENSIONS: "Final[Tuple[ExtensionSpec, ...]]" = (SLURM_EXTENSION, OAR_EXTENSION)

DIAGNOSTICS: "Final" = {spec.diagnostics[0]: (spec.label, spec.diagnostics[1]) for spec in EXTENSIONS if spec.diagnostics is not None}
JOB_IDENTITIES: "Final" = tuple(identity for spec in EXTENSIONS for identity in spec.job_identities)
NODE_JOB_FIELDS: "Final" = tuple(field for spec in EXTENSIONS for field in spec.node_job_fields)


def validate_diagnostics(data: Dict[str, object]) -> None:
    """Raise TypeError or ValueError unless each scheduler's diagnostics are counts under the known keys."""
    for name, (label, keys) in DIAGNOSTICS.items():
        if name not in data:
            continue
        diagnostics = data[name]
        if not isinstance(diagnostics, dict) or not all(isinstance(key, str) for key in diagnostics):
            raise TypeError("%s diagnostics must be an object with string keys" % label)
        if any(key not in keys or type(count) is not int or count < 0 for key, count in diagnostics.items()):
            raise ValueError(label + " diagnostics require known counters with nonnegative integer values")


def validate_record_extensions(kind: str, record: Dict[str, object]) -> None:
    """Raise ValueError if a job's or node's scheduler-specific fields are malformed."""
    for spec in EXTENSIONS:
        if kind == "jobs":
            spec.validate_job(record)
            validate_identities(record, spec.job_identities, spec.label)
        elif kind == "nodes":
            validate_text_lists(record, spec.node_job_fields)
            spec.validate_node(record)


def mask_job_extensions(original: Mapping[str, Any], target: "Job", mask: Callable[[str, str], str]) -> None:
    """Copy a job's scheduler-specific fields to target, masking partition and job names."""
    for spec in EXTENSIONS:
        spec.mask_job(original, cast(Dict[str, object], target), mask)


def mask_node_extensions(original: Mapping[str, Any], target: "Node", mask_job: Callable[[str], str]) -> None:
    """Copy a node's scheduler-specific fields to target, masking job IDs."""
    for spec in EXTENSIONS:
        spec.mask_node(original, cast(Dict[str, Any], target), mask_job)


def node_is_idle(node: Mapping[str, Any]) -> bool:
    """Return True if the node is free, runs no jobs and no extension field shows it in use."""
    available = node["state"] == "-" and not node["core_job_map"] and not any(node.get(field) for field in NODE_JOB_FIELDS)
    return available and not any(spec.node_busy(node) for spec in EXTENSIONS)
