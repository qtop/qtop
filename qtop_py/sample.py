## SPDX-License-Identifier: MIT
"""Portable support snapshots; never extract or execute archive contents."""

import copy
import datetime
import io
import json
import platform
import re
import tarfile
from typing import TYPE_CHECKING, Dict, List, Optional, Tuple, cast, overload

if TYPE_CHECKING:
    from qtop_py.contracts import ClusterState, Config, IdentifierMaps, Options, Snapshot

from qtop_py.cluster_state import validate_cluster_state

SNAPSHOT_NAME = "qtop-snapshot.json"
DISPLAY_OPTIONS = (
    "ANONYMIZE",
    "BLINDREMAP",
    "CLASSIC",
    "COLOR",
    "FORCE_NAMES",
    "GET_GECOS",
    "SHOW_GROUPS",
    "EXPAND_QUEUES",
    "LESS",
    "NOMASKING",
    "STRICTCHECK",
    "sect_1_off",
    "sect_2_off",
    "sect_3_off",
    "SHOW_ACCOUNT_TOTALS",
)


def anonymize_state(state: object) -> "Tuple[ClusterState, IdentifierMaps]":
    """Mask public identifiers consistently, including job/core references."""
    state = copy.deepcopy(validate_cluster_state(state))
    mappings: "IdentifierMaps" = {kind: {} for kind in ("user", "node", "queue", "job")}

    @overload
    def mask(value: str, kind: str) -> str:
        pass

    @overload
    def mask(value: None, kind: str) -> None:
        pass

    def mask(value: Optional[str], kind: str) -> Optional[str]:
        if value is None:
            return None
        value = str(value)
        mapping = mappings[kind]
        if value not in mapping:
            index = len(mapping)
            mapping[value] = str(100000 + index) if kind == "job" else "%s_anon_%s_%s" % (kind[0], {"node": "wn", "queue": "q"}.get(kind, kind), index)
        return mapping[value]

    structured_jobs = {job["id"]: job for job in state["jobs"] if "slurm_array_job_id" in job or "slurm_het_job_id" in job}

    def job_id(value: str) -> str:
        identity = structured_jobs.get(str(value))
        if identity is not None:
            array_parent = identity.get("slurm_array_job_id")
            if array_parent is not None:
                masked = mask(array_parent, "job") + "_" + identity.get("slurm_array_task_id", "")
            else:
                masked = mask(identity.get("slurm_het_job_id", ""), "job") + "+" + identity.get("slurm_het_job_offset", "")
            mappings["job"][str(value)] = masked
            return masked
        # PBS array tasks share the base ID in the jobs collection.
        match = re.match(r"^(.*?)(\[.*\])?$", str(value))
        if match is None:
            raise ValueError("Invalid job ID")
        return mask(match.group(1), "job") + (match.group(2) or "")

    original_jobs = state["jobs"]
    state["jobs"] = [{"id": job_id(job["id"]), "user": mask(job["user"], "user"), "state": job["state"], "queue": mask(job["queue"], "queue")} for job in original_jobs]
    for original_job, job in zip(original_jobs, state["jobs"]):
        if "accounting_state" in original_job:
            job["accounting_state"] = original_job["accounting_state"]
        if "slurm_partitions" in original_job:
            job["slurm_partitions"] = [mask(name, "queue") for name in original_job["slurm_partitions"]]
        if "slurm_array_job_id" in original_job:
            job["slurm_array_job_id"] = mask(original_job["slurm_array_job_id"], "job")
            job["slurm_array_task_id"] = original_job.get("slurm_array_task_id", "")
        if "slurm_het_job_id" in original_job:
            job["slurm_het_job_id"] = mask(original_job["slurm_het_job_id"], "job")
            job["slurm_het_job_offset"] = original_job.get("slurm_het_job_offset", "")
    for queue in state["queues"]:
        queue["name"] = mask(queue["name"], "queue")
    original_nodes = state["nodes"]
    state["nodes"] = [
        {
            "domainname": mask(node["domainname"], "node"),
            "state": node["state"],
            "np": node["np"],
            "qname": [mask(queue, "queue") for queue in node["qname"]],
            "core_job_map": {str(core): job_id(job) for core, job in node["core_job_map"].items()},
        }
        for node in state["nodes"]
    ]
    for original, node in zip(original_nodes, state["nodes"]):
        if "slurm_job_ids" in original:
            node["slurm_job_ids"] = [job_id(value) for value in original["slurm_job_ids"]]
        if "slurm_unmapped_cpus" in original:
            node["slurm_unmapped_cpus"] = original["slurm_unmapped_cpus"]
        for field, resources in (("slurm_gpu_configured", original.get("slurm_gpu_configured")), ("slurm_gpu_used", original.get("slurm_gpu_used"))):
            if resources is not None:
                masked_resources = copy.deepcopy(resources)
                for resource in masked_resources:
                    resource["type"] = None
                    resource["parent_gpu_uuid"] = None
                if field == "slurm_gpu_configured":
                    node["slurm_gpu_configured"] = masked_resources
                else:
                    node["slurm_gpu_used"] = masked_resources
        if "slurm_gpu_bindings" in original:
            node["slurm_gpu_bindings"] = copy.deepcopy(original["slurm_gpu_bindings"])
            for binding in node["slurm_gpu_bindings"]:
                binding["job_id"] = job_id(binding["job_id"])
                binding["type"] = None
                binding["parent_gpu_uuid"] = None
        source = original.get("cpu_allocation_source")
        if source is not None and source in ("slurm", "estimated", "none"):
            node["cpu_allocation_source"] = source
    return state, mappings


def sanitize_config(config: "Config", mappings: "IdentifierMaps") -> "Tuple[Config, List[str]]":
    """Remove local commands/paths and translate identifier-bearing rules."""
    config = copy.deepcopy(config)
    omitted = ("savepath", "schedulers", "signature_commands", "extract_info", "remapping")
    config.update(savepath="/tmp/qtop_results_$USER", schedulers={}, signature_commands={}, extract_info=None, remapping=[])
    # Control strings (state codes, sort names, colors) must not be rewritten
    # merely because they happen to equal a user/queue name.
    original = copy.deepcopy(config)
    # Resolve regexes against the actual names, rather than exposing their patterns.
    for key, kind in (("user_color_mappings", "user"), ("queue_color_mappings", "queue")):
        config[key] = [
            {"^%s$" % re.escape(alias): color}
            for rule in cast(List[Dict[str, str]], original.get(key) or [])
            for pattern, color in rule.items()
            for name, alias in mappings[kind].items()
            if re.search(pattern, name)
        ]
    for key in ("filtering", "highlight"):
        rules: List[Dict[str, List[str]]] = []
        for rule in cast(List[Dict[str, List[str]]], original.get(key) or []):
            translated: Dict[str, List[str]] = {}
            for action, patterns in rule.items():
                rule_kind: Optional[str] = "node" if "name_patterns" in action else "user" if "user_pat" in action else "queue" if "queue" in action else None
                if rule_kind:
                    matches = [alias for name, alias in mappings[rule_kind].items() if any(re.search(pattern, name) for pattern in patterns)]
                    translated[action] = matches if "queue" in action else ["^%s$" % re.escape(alias) for alias in matches]
                else:
                    translated[action] = patterns
            rules.append(translated)
        config[key] = rules
    return config, list(omitted)


def add_json(archive: tarfile.TarFile, name: str, value: object) -> None:
    data = (json.dumps(value, indent=2) + "\n").encode("utf-8")
    member = tarfile.TarInfo(name)
    member.size = len(data)
    member.mode = 0o600
    archive.addfile(member, io.BytesIO(data))


def save_snapshot(
    archive: tarfile.TarFile,
    state: "ClusterState",
    config: "Config",
    options: "Options",
    scheduler: str,
    version: str,
    omitted: List[str],
) -> None:
    add_json(
        archive,
        SNAPSHOT_NAME,
        {
            "format_version": 1,
            "captured_at_utc": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "python_version": platform.python_version(),
            "qtop_version": version,
            "scheduler": scheduler,
            "state": state,
            "config": config,
            "options": {key: getattr(options, key) if key == "COLOR" else bool(getattr(options, key)) for key in DISPLAY_OPTIONS if hasattr(options, key)},
            "omitted_config": omitted,
            "reproduce": "qtop --load-sample ARCHIVE.tar",
            "scope": "Parsed state and presentation; raw scheduler parsing is not replayed.",
        },
    )
    # Original logs can contain arbitrary malformed input or old session data.
    add_json(archive, "capture-log.json", {"status": "snapshot collected", "raw_log_omitted": True})


def load_snapshot(path: str) -> "Snapshot":
    with tarfile.open(path, "r:*") as archive:
        member = archive.getmember(SNAPSHOT_NAME)
        if not member.isfile() or member.size > 64 * 1024 * 1024:
            raise ValueError("snapshot must be a regular file smaller than 64 MiB")
        stream = archive.extractfile(member)
        if stream is None:
            raise ValueError("snapshot data is missing")
        with stream:
            payload = json.load(stream)
    if not isinstance(payload, dict):
        raise TypeError("snapshot must be an object")
    snapshot = cast("Config", payload)
    if type(snapshot.get("format_version")) is not int or snapshot["format_version"] != 1:
        raise ValueError("unsupported snapshot format version")
    if not isinstance(snapshot.get("scheduler"), str):
        raise TypeError("snapshot scheduler must be a string")
    snapshot["state"] = validate_cluster_state(snapshot["state"])
    if not isinstance(snapshot["config"], dict) or not isinstance(snapshot["options"], dict):
        raise ValueError("invalid snapshot configuration/options")
    for name, value in cast(Dict[str, object], snapshot["options"]).items():
        if name == "COLOR":
            if value not in ("ON", "OFF", "AUTO"):
                raise ValueError("snapshot COLOR must be ON, OFF or AUTO")
        elif name in DISPLAY_OPTIONS:
            if value is not None and not isinstance(value, bool):
                raise TypeError("snapshot option %s must be boolean" % name)
            snapshot["options"][name] = bool(value)
    snapshot["config"]["extract_info"] = None
    snapshot["options"]["GET_GECOS"] = False
    snapshot["options"]["SHOW_GROUPS"] = False
    return cast("Snapshot", snapshot)
