## Copyright (c) 2026 qtop contributors
## SPDX-License-Identifier: MIT
"""Versioned Slurm JSON input using the existing Slurm accounting and GRES rules."""

import os
import shlex
from collections import Counter
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple

from qtop_py import core_safety as safety
from qtop_py.core_api import open_input
from qtop_py.plugins.slurm import SlurmBatchSystem, SlurmStatExtractor, slurm_accounting_state
from qtop_py.schedulers.gpu import parse_gpu_gres

if TYPE_CHECKING:
    from typing import Final

    from qtop_py.core_api import TraceMasker
    from qtop_py.core_state_types import ClusterState, Config, Options

JOB_FLAG_PRIORITY: "Final" = (
    "COMPLETING",
    "STAGE_OUT",
    "CONFIGURING",
    "RESIZING",
    "REQUEUED",
    "REQUEUE_FED",
    "REQUEUE_HOLD",
    "SPECIAL_EXIT",
    "STOPPED",
    "REVOKED",
    "RESV_DEL_HOLD",
    "SIGNALING",
)
SUPPORTED_PARSERS: "Final" = ("v0.0.40", "v0.0.41", "v0.0.42", "v0.0.43", "v0.0.44", "v0.0.45", "v0.0.46")


class UnsupportedSlurmJSON(ValueError):
    """The Slurm client cannot provide a supported JSON response."""


def integer(value: Any, default: Optional[int] = None) -> int:
    """Read Slurm integer wrappers without treating unset values as allocations."""
    if isinstance(value, dict):
        value = value.get("number") if value.get("set") is True and not value.get("infinite") else None
    if value is None or (default is not None and value in (4294967294, 4294967295)):
        if default is not None:
            return default
        raise ValueError("missing Slurm integer")
    if type(value) is not int or value < 0:
        raise ValueError("invalid Slurm integer")
    return value


def flags(value: Any) -> str:
    """Normalize the scalar and array forms used for Slurm state flags."""
    if isinstance(value, list) and all(isinstance(item, str) and item for item in value):
        value = "+".join(value)
    if not isinstance(value, str) or not value:
        raise ValueError("missing Slurm state")
    return value


def unique_fields(pairs: List[Tuple[str, Any]]) -> Dict[str, Any]:
    """Reject duplicate JSON keys rather than silently replacing allocation data."""
    fields: Dict[str, Any] = {}
    for key, value in pairs:
        if key in fields:
            raise ValueError("duplicate Slurm JSON field %r" % key)
        fields[key] = value
    return fields


def read_response(path: str, key: str, options: Any = None) -> Tuple[str, List[Dict[str, Any]]]:
    """Reject unknown schemas, error responses and malformed record lists."""
    with open_input(path, encoding="utf-8", options=options) as stream:
        data = safety.load_json(stream, object_pairs_hook=unique_fields)
    if not isinstance(data, dict):
        raise ValueError("Slurm JSON response must be an object")
    meta = data.get("meta")
    plugin = meta.get("plugin") if isinstance(meta, dict) else None
    version = plugin.get("data_parser") if isinstance(plugin, dict) else None
    if not isinstance(version, str):
        raise ValueError("missing Slurm JSON parser metadata")
    if version.startswith("data_parser/"):
        version = version[len("data_parser/") :]
    if version not in SUPPORTED_PARSERS:
        raise UnsupportedSlurmJSON("unsupported Slurm JSON data_parser %r" % version)
    if data.get("errors"):
        raise ValueError("Slurm JSON reports scheduler errors")
    rows = data.get(key)
    if not isinstance(rows, list) or any(not isinstance(row, dict) for row in rows):
        raise ValueError("Slurm JSON requires a %s list" % key)
    return version, rows


class SlurmJSONBatchSystem(SlurmBatchSystem):
    """Slurm JSON backend; publish only completely validated collections."""

    signature_command = "scontrol"
    default_commands = {
        # Sequential queries: small partition inventory first, jobs last.
        "partitions_json": "%(savepath)s/slurm_partitions%(pid)s.json, scontrol --json show partitions",
        "nodes_json": "%(savepath)s/slurm_nodes%(pid)s.json, scontrol --json show nodes",
        "jobs_json": "%(savepath)s/slurm_jobs%(pid)s.json, scontrol --json show jobs",
    }

    detection_priority = -1
    display_scheduler = "slurm"
    collection_interval = 20.0
    fallback_backend = SlurmBatchSystem
    fallback_note = "Try --slurm-json-squeue for empty job JSON."
    optional_trace_keys: Tuple[str, ...] = ()
    optional_trace_aliases: Dict[str, str] = {}

    @staticmethod
    def get_mnemonic() -> str:
        """Return the explicit backend name."""
        return "slurm_json"

    @staticmethod
    def display_job_state(state: str) -> str:
        """Apply Slurm's compact-state flag precedence without discarding raw flags."""
        states = state.upper().split("+")
        primary = next((flag for flag in JOB_FLAG_PRIORITY if flag in states), states[0])
        return SlurmStatExtractor._map_job_state(primary)

    @staticmethod
    def collection_command(key: str, command: str, options: Any) -> str:
        """Respect visibility and optionally collect jobs through squeue JSON."""
        if key == "jobs_json" and getattr(options, "SLURM_JSON_SQUEUE", False):
            json_option = next((word for word in shlex.split(command) if word == "--json" or word.startswith("--json=")), "--json")
            command = "squeue %s --states=all" % shlex.quote(json_option)
        if getattr(options, "ALL_PARTITIONS", False) and not options.SOURCEDIR:
            command += " --all"
        return command

    @staticmethod
    def capability_failure(error: Exception) -> bool:
        """Fall back once for unsupported JSON, never for malformed allocations."""
        return isinstance(error, UnsupportedSlurmJSON) or any(
            text in str(error).lower() for text in ("unrecognized option", "unknown option", "json support", "data_parser plugin")
        )

    def __init__(self, scheduler_output_filenames: Dict[str, str], config: "Config", options: "Options") -> None:
        super().__init__({}, config, options)
        self._topology: Dict[str, Dict[str, Any]] = {}
        self._gpu_inventory: Dict[str, Dict[str, Any]] = {}
        self._allocations: Dict[str, Dict[str, Tuple[List[int], int, bool]]] = {}
        self._bindings: Dict[str, Dict[str, Any]] = {}
        try:
            self._load(scheduler_output_filenames)
        except (KeyError, TypeError, AttributeError, IndexError) as error:
            raise ValueError("malformed Slurm JSON field: %s" % error) from error

    def _load(self, scheduler_output_filenames: Dict[str, str]) -> None:
        versions = []
        records = {}
        for key in ("partitions", "nodes", "jobs"):
            path = scheduler_output_filenames[key + "_json"]
            if not os.path.getsize(path):
                raise UnsupportedSlurmJSON("empty Slurm JSON %s response; try --slurm-json-squeue for jobs" % key)
            version, records[key] = read_response(path, key, self.options)
            versions.append(version)
        if len(set(versions)) != 1:
            raise ValueError("Slurm JSON responses use different schema versions")
        self._version = versions[0]
        self._partitions: Dict[str, str] = {}
        for row in records["partitions"]:
            name = row["name"]
            if not isinstance(name, str) or not name or name in self._partitions:
                raise ValueError("invalid or duplicate Slurm partition")
            self._partitions[name] = flags(row["partition"]["state"] if isinstance(row.get("partition"), dict) else row["state"])
        self._nodes = self._parse_nodes(records["nodes"])
        self._jobs = self._parse_jobs(records["jobs"])

    def _parse_nodes(self, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        nodes = []
        for row in rows:
            name = row["name"]
            if not isinstance(name, str) or not name or name in self._topology:
                raise ValueError("invalid or duplicate Slurm node")
            self._topology[name] = row
            capacity = integer(row["cpus"])
            if safety.cores_per_node(capacity) is None:
                raise ValueError("Slurm node exceeds CPU budget")
            partitions = row.get("partitions")
            partitions = [] if partitions is None else partitions
            if isinstance(partitions, str):
                partitions = partitions.split(",") if partitions else []
            if not isinstance(partitions, list) or any(not isinstance(partition, str) or not partition for partition in partitions):
                raise ValueError("invalid Slurm node partitions")
            state = flags(row["state"])
            marker = (
                "d"
                if any(flag in state.upper() for flag in ("DOWN", "DRAIN", "FAIL", "NO_RESPOND", "NOT_RESPOND", "INVALID_REG", "POWERED_DOWN"))
                else SlurmStatExtractor._map_node_state(state.split("+")[0])
            )
            self._gpu_inventory[name] = {}
            for source, target in (("gres", "slurm_gpu_configured"), ("gres_used", "slurm_gpu_used")):
                gres = row.get(source)
                if gres is not None:
                    if not isinstance(gres, str):
                        raise ValueError("invalid Slurm node GRES")
                    self._gpu_inventory[name][target] = parse_gpu_gres(gres)
            for partition in partitions or [""]:
                nodes.append(
                    {
                        "raw_domainname": name,
                        "domainname": name,
                        "np": str(capacity),
                        "state": marker,
                        "slurm_node_state": state,
                        "qname": partition,
                        "partition_state": self._partitions.get(partition, "?"),
                    }
                )
        return nodes

    def _parse_jobs(self, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        jobs = []
        seen = set()
        expanded_count = 0
        for row in rows:
            job_id = str(integer(row["job_id"]))
            task_ids = []
            array = integer(row.get("array_job_id"), 0)
            task = integer(row.get("array_task_id"), 4294967294)
            het = integer(row.get("het_job_id"), 0)
            if array and task != 4294967294:
                job_id = "%s_%s" % (array, task)
            elif array and row.get("array_task_string"):
                task_ids = SlurmStatExtractor.expand_job_id("%s_[%s]" % (array, row["array_task_string"]))
            elif array:
                raise ValueError("missing Slurm array task identity")
            if het:
                if array:
                    raise ValueError("conflicting array and heterogeneous identity")
                job_id = "%s+%s" % (het, integer(row.get("het_job_offset")))
            state = flags(row["job_state"])
            primary = self.display_job_state(state)
            partitions = list(dict.fromkeys(row["partition"].split(",")))
            user = row.get("user_name") or str(integer(row.get("user_id")))
            if not isinstance(user, str) or not user or any(not partition for partition in partitions):
                raise ValueError("invalid Slurm job user or partition")
            node_list = row.get("nodes")
            node_list = "" if node_list is None else node_list
            if not isinstance(node_list, str):
                raise ValueError("invalid Slurm JSON node-list type")
            hosts = self.expand_nodelist(node_list)
            if len(set(hosts)) != len(hosts):
                raise ValueError("duplicate Slurm JSON allocated hosts")
            if row.get("nodes") and not hosts:
                raise ValueError("invalid Slurm JSON node list")
            job = {
                "JobId": job_id,
                "UnixAccount": user,
                "S": primary,
                "raw_state": state,
                "Queue": partitions[0],
                "slurm_partitions": partitions,
                "accounting_state": slurm_accounting_state(primary),
                "CPUs": integer(row.get("cpus"), 0),
                "Nodes": node_list,
            }
            for source, target in (("tres_alloc_str", "slurm_tres_alloc"), ("tres_per_node", "slurm_tres_per_node"), ("tres_per_task", "slurm_tres_per_task")):
                job[target] = "" if row.get(source) is None else row[source]
                if not isinstance(job[target], str):
                    raise ValueError("invalid Slurm job TRES")
            if len(task_ids) > 1 and primary in self.ACTIVE_STATES:
                raise ValueError("grouped active array allocations are ambiguous")
            expanded_count += len(task_ids) if task_ids else 0
            if expanded_count > SlurmStatExtractor.MAX_EXPANDED_TASKS:
                raise ValueError("too many grouped Slurm JSON array tasks")
            for identity in task_ids or [job_id]:
                if identity in seen:
                    raise ValueError("duplicate Slurm JSON job identity")
                seen.add(identity)
                task_job = dict(job, JobId=identity)
                task_job.update(SlurmStatExtractor.identity_fields(identity))
                if primary in self.ACTIVE_STATES:
                    self._parse_allocation(task_job, row, hosts)
                jobs.append(task_job)
        return jobs

    def _parse_allocation(self, job: Dict[str, Any], row: Dict[str, Any], hosts: List[str]) -> None:
        if any(host not in self._topology for host in hosts):
            raise ValueError("Slurm allocation references missing node; try --all")
        gres = row.get("gres_detail")
        gres = [] if gres is None else gres
        if not isinstance(gres, list) or gres and len(gres) != len(hosts):
            raise ValueError("Slurm GRES detail does not match allocated hosts")
        self._bindings[job["JobId"]] = {host: parse_gpu_gres(value) for host, value in zip(hosts, gres) if value is not None}
        resources = row.get("job_resources")
        if resources is None:
            return  # unavailable detail is explicitly estimated by the legacy policy
        if not isinstance(resources, dict):
            raise ValueError("invalid Slurm job_resources")
        allocations = resources.get("allocated_nodes") if self._version == "v0.0.40" else resources.get("nodes", {}).get("allocation")
        if not isinstance(allocations, list):
            raise ValueError("missing Slurm per-node allocation list")
        total = integer(resources.get("cpus")) if self._version != "v0.0.40" else max(integer(resources.get("allocated_cpus"), 0), integer(resources.get("allocated_cores"), 0))
        if total:
            job["CPUs"] = integer(total)
        details = {}
        for allocation in allocations:
            host = allocation["nodename" if self._version == "v0.0.40" else "name"]
            if host in details or host not in hosts:
                raise ValueError("conflicting Slurm allocation node")
            node = self._topology.get(host)
            if node is None:
                raise ValueError("Slurm allocation references missing node; try --all")
            cpus = self._cpu_ids(allocation, node)
            count = len(cpus) if self._version == "v0.0.40" else integer(allocation["cpus"]["count"])
            if count > len(cpus):
                raise ValueError("Slurm CPU count exceeds the reported allocation bitmap")
            if count > integer(node["cpus"]):
                raise ValueError("Slurm per-node allocation exceeds capacity")
            details[host] = (cpus, count, bool(cpus) and count == len(cpus))
        if self._version == "v0.0.40" and len(details) == 1 and job["CPUs"] < sum(detail[1] for detail in details.values()):
            host = next(iter(details))
            cpus = details[host][0]
            details[host] = (cpus, job["CPUs"], False)
        if set(details) != set(hosts) or sum(detail[1] for detail in details.values()) != job["CPUs"]:
            raise ValueError("Slurm per-node CPU allocation totals disagree")
        self._allocations[job["JobId"]] = details

    def _cpu_ids(self, allocation: Dict[str, Any], node: Dict[str, Any]) -> List[int]:
        cpus: List[int] = []
        sockets = allocation["sockets"]
        cores_per_socket, threads = integer(node["cores"]), integer(node["threads"])
        if not cores_per_socket or not threads or not integer(node["sockets"]) or cores_per_socket * threads * integer(node["sockets"]) > safety.MAX_CORES_PER_NODE:
            raise ValueError("invalid Slurm socket/core/SMT topology")
        socket_rows = sockets.items() if isinstance(sockets, dict) else ((row["index"], row) for row in sockets)
        for socket, details in socket_rows:
            socket_id = integer(int(socket) if isinstance(socket, str) and socket.isdecimal() else socket)
            cores = details["cores"]
            core_rows = cores.items() if isinstance(cores, dict) else ((row["index"], row["status"]) for row in cores)
            for core, status in core_rows:
                state = flags(status).upper()
                if "UNALLOCATED" in state:
                    continue
                if "ALLOCATED" not in state:
                    raise ValueError("unknown Slurm CPU allocation status")
                core_id = integer(int(core) if isinstance(core, str) and core.isdecimal() else core)
                if socket_id < 0 or socket_id >= integer(node["sockets"]) or core_id < 0 or core_id >= cores_per_socket:
                    raise ValueError("Slurm allocation outside CPU topology")
                cpus.extend(range((socket_id * cores_per_socket + core_id) * threads, (socket_id * cores_per_socket + core_id + 1) * threads))
        if len(set(cpus)) != len(cpus) or cpus and max(cpus) >= integer(node["cpus"]):
            raise ValueError("duplicate or out-of-capacity Slurm CPU IDs")
        return cpus

    def _apply_cpu_allocations(self, worker_nodes: Dict[str, Dict[str, Any]], job_hosts: Any = None) -> None:
        estimates = {host: Counter(node["core_job_map"].values()) for host, node in worker_nodes.items()}
        for node in worker_nodes.values():
            node["core_job_map"] = {}
            node["cpu_allocation_source"] = "none"
        assignments = []
        for job in self._jobs:
            if job["S"] not in self.ACTIVE_STATES:
                continue
            hosts = job_hosts(job) if job_hosts else self.expand_nodelist(job["Nodes"])
            details = self._allocations.get(job["JobId"])
            for host in hosts:
                if host not in worker_nodes:
                    continue
                count = estimates[host].get(job["JobId"], 0) if job["CPUs"] else 0
                cpus, count, exact = details[host] if details else ([], count, False)
                assignments.append((cpus, count, exact, host, job["JobId"]))
        # Unknown placement must never consume a CPU reported exactly for another job.
        for cpus, count, exact, host, job_id in sorted(assignments, key=lambda item: not item[2]):
            node = worker_nodes[host]
            mapping = node["core_job_map"]
            if not exact:
                cpus = [cpu for cpu in (cpus or range(int(node["np"]))) if cpu not in mapping][:count]
            if len(cpus) != count or any(cpu in mapping for cpu in cpus):
                raise ValueError("Slurm CPU allocations overlap or exceed representable capacity")
            mapping.update((cpu, job_id) for cpu in cpus)
            node["cpu_allocation_source"] = "estimated" if not exact or node["cpu_allocation_source"] == "estimated" else "slurm"

    def _apply_gpu_allocations(self, worker_nodes: Dict[str, Dict[str, Any]], job_hosts: Any = None) -> None:
        for host, node in worker_nodes.items():
            node.update(self._gpu_inventory[host])
        for job_id, hosts in self._bindings.items():
            for host, resources in hosts.items():
                worker_nodes[host].setdefault("slurm_gpu_bindings", []).extend(dict(resource, job_id=job_id) for resource in resources)

    def get_cluster_state(self, options: "Options") -> "ClusterState":
        """Reuse Slurm accounting and preserve otherwise empty partitions."""
        state = super().get_cluster_state(options)
        for job, raw in zip(state["jobs"], self._jobs):
            if "+" in raw["raw_state"]:
                job["state"] = raw["raw_state"]
        queues = {queue["name"]: queue for queue in state["queues"]}
        queues.pop("", None)
        for name, status in self._partitions.items():
            if name not in queues:
                queues[name] = {"name": name, "state": status, "limit": "--", "running": 0, "queued": 0}
            queues[name]["state"] = status
        state["queues"] = list(queues.values())
        for node in state["nodes"]:
            node["qname"] = [name for name in node["qname"] if name]
        return state

    def mask_source_trace(self, key: str, text: str, mask: "TraceMasker") -> Tuple[str, str]:
        """Require portable masked-state replay rather than leaking unknown JSON fields."""
        raise ValueError("JSON source fields are not allowlisted; use the anonymized portable snapshot")
