## Copyright (c) 2026 qtop contributors
## SPDX-License-Identifier: MIT
"""Scheduler extension rules for cluster states, combined from each backend's ExtensionSpec.

Validation, -A masking and the idle test loop over the specs the registered backends declare
(their extensions attribute); nothing here names a scheduler. Plugins import this module, so the
registry is read on first use, not at import. A replayed or --load-sample state is checked
against every registered backend's spec, whichever backend produced it.
"""

import importlib
from functools import lru_cache
from typing import TYPE_CHECKING, Any, Callable, Dict, Mapping, NamedTuple, Optional, Tuple, cast

if TYPE_CHECKING:
    from qtop_py.core_state_types import Job, Node

from qtop_py.core_errors import SchedulerInputError
from qtop_py.schedulers.extensions import ExtensionSpec, validate_identities, validate_text_lists

Extensions = NamedTuple(
    "Extensions",
    [
        ("specs", Tuple[ExtensionSpec, ...]),
        ("diagnostics", Dict[str, Tuple[str, Tuple[str, ...]]]),
        ("job_identities", Tuple[Tuple[str, str, str], ...]),
        ("node_job_fields", Tuple[str, ...]),
    ],
)


@lru_cache(maxsize=None)
def extensions() -> Extensions:
    """Return the registered backends' ExtensionSpecs, each once, and what they declare together."""
    # Deliberate, resolved at first use: the registry loads the plugins, and plugins import this module.
    registry = importlib.import_module("qtop_py.schedulers.registry")

    specs: Tuple[ExtensionSpec, ...] = ()
    for backend in registry.PLUGIN_BATCH_SYSTEMS:
        if backend.extensions is not None and backend.extensions not in specs:
            specs += (backend.extensions,)
    return Extensions(
        specs,
        {spec.diagnostics[0]: (spec.label, spec.diagnostics[1]) for spec in specs if spec.diagnostics is not None},
        tuple(identity for spec in specs for identity in spec.job_identities),
        tuple(field for spec in specs for field in spec.node_job_fields),
    )


def job_identity(job: Mapping[str, Any]) -> Optional[Tuple[str, str, str]]:
    """Return the (parent field, child field, separator) of a job's structured ID, or None."""
    return next((fields for fields in extensions().job_identities if fields[0] in job), None)


def validate_diagnostics(data: Dict[str, object]) -> None:
    """Raise TypeError or ValueError unless each scheduler's diagnostics are counts under the known keys."""
    for name, (label, keys) in extensions().diagnostics.items():
        if name not in data:
            continue
        diagnostics = data[name]
        if not isinstance(diagnostics, dict) or not all(isinstance(key, str) for key in diagnostics):
            raise TypeError("%s diagnostics must be an object with string keys" % label)
        if any(key not in keys or type(count) is not int or count < 0 for key, count in diagnostics.items()):
            raise SchedulerInputError(label + " diagnostics require known counters with nonnegative integer values")


def validate_record_extensions(kind: str, record: Dict[str, object]) -> None:
    """Raise ValueError if a job's or node's scheduler-specific fields are malformed."""
    for spec in extensions().specs:
        if kind == "jobs":
            spec.validate_job(record)
            validate_identities(record, spec.job_identities, spec.label)
        elif kind == "nodes":
            validate_text_lists(record, spec.node_job_fields)
            spec.validate_node(record)


def mask_job_extensions(original: Mapping[str, Any], target: "Job", mask: Callable[[str, str], str]) -> None:
    """Copy a job's scheduler-specific fields to target, masking partition and job names."""
    for spec in extensions().specs:
        spec.mask_job(original, cast(Dict[str, object], target), mask)


def mask_node_extensions(original: Mapping[str, Any], target: "Node", mask_job: Callable[[str], str], mask: Optional[Callable[[str, str], str]] = None) -> None:
    """Copy a node's scheduler-specific fields to target, masking job IDs and, given mask, other names."""
    for spec in extensions().specs:
        spec.mask_node(original, cast(Dict[str, Any], target), mask_job)
        if mask is not None:
            spec.mask_node_names(original, cast(Dict[str, Any], target), mask)


def node_is_idle(node: Mapping[str, Any]) -> bool:
    """Return True if the node is free, runs no jobs and no extension field shows it in use."""
    known = extensions()
    available = node["state"] == "-" and not node["core_job_map"] and not any(node.get(field) for field in known.node_job_fields)
    return available and not any(spec.node_busy(node) for spec in known.specs)
