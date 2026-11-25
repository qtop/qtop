## Copyright (c) 2026 qtop contributors
## SPDX-License-Identifier: MIT
"""The shape of one scheduler's extension fields in a cluster state.

A backend whose jobs or nodes carry fields beyond the neutral schema (slurm_*, oar_*, ...)
describes them in one ExtensionSpec, in or next to its plugin module, and declares it as the
plugin's ``extensions`` attribute. schedulers.metadata collects the registered backends' specs,
so validation, masking (-A) and the idle test cover those fields for every state, whichever
backend produced it, including replayed and --load-sample states.
"""

import re
from typing import Any, Callable, Dict, List, Mapping, Optional, Tuple, cast

_TEXT_TYPE = frozenset((str,))

Mask = Callable[[str, str], str]
MaskJob = Callable[[str], str]


def nonempty_texts(values: List[object]) -> bool:
    """Check every value is a nonempty str."""
    if _TEXT_TYPE.issuperset(map(type, values)):  # exact str only, so nonempty is truthiness
        return all(values)
    return all(isinstance(value, str) and value for value in values)


def validate_text_lists(record: Mapping[str, object], fields: Tuple[str, ...]) -> None:
    """Raise ValueError unless each present field is a list of nonempty strings."""
    for field in fields:
        if field in record:
            values = record[field]
            if not isinstance(values, list) or not nonempty_texts(cast(List[object], values)):
                raise ValueError("%s must be a list of nonempty strings" % field)


def validate_identities(record: Mapping[str, object], identities: Tuple[Tuple[str, str, str], ...], label: str) -> None:
    """Raise ValueError unless each (parent, child, separator) identity is numeric and matches the job ID."""
    if len(identities) > 1 and all(parent in record for parent, _, _ in identities):
        raise ValueError("job cannot have both %s array and heterogeneous identities" % label)
    for parent, child, separator in identities:
        if parent in record or child in record:
            base, index = record.get(parent), record.get(child)
            if not isinstance(base, str) or not isinstance(index, str) or not re.fullmatch(r"[0-9]+", base) or not re.fullmatch(r"[0-9]+", index):
                raise ValueError("job %s identity requires a numeric parent and task/component ID" % label)
            if record["id"] != base + separator + index:
                raise ValueError("job %s identity conflicts with canonical ID" % label)


def _no_check(record: Dict[str, object]) -> None:
    return None


def _no_mask_job(original: Mapping[str, Any], job: Dict[str, object], mask: Mask) -> None:
    return None


def _mask_node_job_fields(fields: Tuple[str, ...]) -> Callable[[Mapping[str, Any], Dict[str, Any], MaskJob], None]:
    def mask_node(original: Mapping[str, Any], node: Dict[str, Any], mask_job: MaskJob) -> None:
        node.update({field: [mask_job(value) for value in original[field]] for field in fields if field in original})

    return mask_node


def _never_busy(node: Mapping[str, Any]) -> bool:
    return False


class ExtensionSpec(object):
    """One scheduler's extension fields: validators, -A maskers, diagnostics and idle test.

    label: name in messages ("Slurm").
    diagnostics: (top-level key, allowed counter names), or None.
    node_job_fields: node fields listing job IDs beyond core_job_map; validated, masked and
        counted as occupancy without any code in the spec.
    job_identities: (parent field, child field, separator) of structured job IDs, masked by sample.
    validate_job / validate_node: raise ValueError on a malformed present field.
    mask_job(original, job, mask) / mask_node(original, node, mask_job): copy the fields into
        the anonymized record, masking names. The default mask_node copies node_job_fields with
        each job ID masked; a custom one must do that itself (it chooses the key order).
    mask_node_names(original, node, mask): copy node fields that hold other names (e.g. a Slurm
        block), masked as mask(name, kind); the kind must be one of core_sample's mapping kinds.
    node_busy(node): True if the extension fields alone show the node is in use.
    """

    def __init__(
        self,
        label: str,
        diagnostics: Optional[Tuple[str, Tuple[str, ...]]] = None,
        node_job_fields: Tuple[str, ...] = (),
        job_identities: Tuple[Tuple[str, str, str], ...] = (),
        validate_job: Callable[[Dict[str, object]], None] = _no_check,
        validate_node: Callable[[Dict[str, object]], None] = _no_check,
        mask_job: Callable[[Mapping[str, Any], Dict[str, object], Mask], None] = _no_mask_job,
        mask_node: Optional[Callable[[Mapping[str, Any], Dict[str, Any], MaskJob], None]] = None,
        node_busy: Callable[[Mapping[str, Any]], bool] = _never_busy,
        mask_node_names: Callable[[Mapping[str, Any], Dict[str, Any], Mask], None] = _no_mask_job,
    ) -> None:
        self.label = label
        self.diagnostics = diagnostics
        self.node_job_fields = node_job_fields
        self.job_identities = job_identities
        self.validate_job = validate_job
        self.validate_node = validate_node
        self.mask_job = mask_job
        self.mask_node = mask_node or _mask_node_job_fields(node_job_fields)
        self.node_busy = node_busy
        self.mask_node_names = mask_node_names

    def __repr__(self) -> str:
        return "ExtensionSpec(%r)" % self.label
