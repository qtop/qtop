##
## qtop is a tool to monitor queuing systems - https://github.com/qtop/qtop
##
## Copyright (c) 2016 Fotis Georgatos
## Copyright (c) 2016 Sotiris Fragkiskos
## Copyright (c) 2026 Nicola Trozzi
##
## SPDX-License-Identifier: MIT
##
"""Slurm backend: parses squeue, sinfo and, optionally, scontrol output."""

import logging
import re
from collections import OrderedDict
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Mapping, Match, Optional, Sequence, Set, Tuple, cast

from qtop_py.core_api import FileEmptyError, GenericBatchSystem, StatExtractor, check_empty_file, open_input
from qtop_py.core_hostlist import HostlistParser
from qtop_py.plugins.slurm_extension import SLURM_EXTENSION
from qtop_py.schedulers.gpu import ACCELERATOR_KINDS, parse_accelerator_gres

if TYPE_CHECKING:
    from typing import Final

    from qtop_py.core_api import TraceMasker
    from qtop_py.core_state_types import ClusterState, Config, JobsInfo, Node, Options, QueuesInfo

logger = logging.getLogger(__name__)

SLURM_JOB_STATES: "Final" = {
    "BOOT_FAIL": "BF",
    "CANCELLED": "CA",
    "COMPLETED": "CD",
    "CONFIGURING": "CF",
    "COMPLETING": "CG",
    "DEADLINE": "DL",
    "FAILED": "F",
    "NODE_FAIL": "NF",
    "OUT_OF_MEMORY": "OOM",
    "PENDING": "PD",
    "PREEMPTED": "PR",
    "RUNNING": "R",
    "RESV_DEL_HOLD": "RD",
    "REQUEUE_FED": "RF",
    "REQUEUE_HOLD": "RH",
    "REQUEUED": "RQ",
    "RESIZING": "RS",
    "REVOKED": "RV",
    "SIGNALING": "SI",
    "SPECIAL_EXIT": "SE",
    "STAGE_OUT": "SO",
    "STOPPED": "ST",
    "SUSPENDED": "S",
    "TIMEOUT": "TO",
    # Flags without a public compact squeue code map to the closest display
    # state while raw_state remains available in ClusterState.
    "EXPEDITING": "PD",
    "LAUNCH_FAILED": "F",
    "POWER_UP_NODE": "CF",
    "RECONFIG_FAIL": "F",
    "UPDATE_DB": "CG",
}
SLURM_COMPACT_JOB_STATES: "Final" = set(SLURM_JOB_STATES.values())
# scontrol show topology, topology/block: "BlockName=b1 BlockIndex=0 Nodes=n[01-18] BlockSize=18" (Slurm's
# topology_block.c). Aggregated blocks print AggregatedBlock= and are not domains; topology/tree prints SwitchName=.
TOPOLOGY_BLOCK_RE: "Final" = re.compile(r"(BlockName|AggregatedBlock)=(\S+) BlockIndex=([0-9]+)(?: Nodes=(\S+))? BlockSize=([0-9]+)")
SLURM_QUEUED_STATES: "Final" = {"PD", "RD", "RF", "RH", "RQ", "SE"}


def slurm_accounting_state(state: str) -> str:
    """Group compact states as running, waiting (including held), or other."""
    return "running" if state == "R" else "queued" if state in SLURM_QUEUED_STATES else "other"


class SlurmArrayIdError(ValueError):
    """An array task range that cannot be expanded: malformed, truncated or too large."""


class SlurmStatExtractor(StatExtractor):
    """Parse squeue, sinfo and scontrol output."""

    MAX_EXPANDED_TASKS = 100000

    @classmethod
    def expand_job_id(cls, job_id: str) -> List[str]:
        """Expand complete legacy array groups into bounded, scalar task IDs."""
        if "_[" not in job_id:
            return [job_id]
        match = re.fullmatch(r"([0-9]+)_\[([0-9,:-]+)(?:%[1-9][0-9]*)?\]", job_id)
        error = "Invalid, truncated or oversized Slurm array ID %r; collect squeue with -r/--array and SLURM_BITSTR_LEN=0." % job_id
        if match is None:
            raise SlurmArrayIdError(error)
        tasks: Set[int] = set()
        expanded_count = 0
        for token in match.group(2).split(","):
            span = re.fullmatch(r"([0-9]{1,10})(?:-([0-9]{1,10})(?::([0-9]{1,10}))?)?", token)
            if span is None:
                raise SlurmArrayIdError(error)
            first = int(span.group(1))
            last = int(span.group(2)) if span.group(2) is not None else first
            step = int(span.group(3)) if span.group(3) is not None else 1
            if last < first or step < 1:
                raise SlurmArrayIdError(error)
            expanded_count += (last - first) // step + 1
            if expanded_count > cls.MAX_EXPANDED_TASKS:
                raise SlurmArrayIdError(error)
            tasks.update(range(first, last + 1, step))
        return ["%s_%s" % (match.group(1), task) for task in sorted(tasks)]

    @classmethod
    def task_ids(cls, job_id: str, array_job_id: str, array_task_id: str) -> List[str]:
        """Prefer explicit array fields; retain formatted-ID support for old traces."""
        if array_task_id.upper() in ("", "N/A", "(NULL)"):
            if array_task_id and re.match(r"[0-9]+_", job_id):
                raise ValueError("Conflicting Slurm array identity for job %s" % job_id)
            return cls.expand_job_id(job_id)
        if not re.fullmatch(r"[0-9]{1,10}", array_job_id) or "+" in job_id:
            raise ValueError("Invalid or conflicting Slurm array identity for job %s" % job_id)
        ids = cls.expand_job_id("%s_[%s]" % (array_job_id, array_task_id))
        scalar = re.fullmatch(r"[0-9]+_[0-9]+", job_id)
        group = re.match(r"([0-9]+)_\[", job_id)
        if (scalar and ids != [job_id]) or (group and group.group(1) != array_job_id):
            raise ValueError("Conflicting Slurm array identity for job %s" % job_id)
        return ids

    @staticmethod
    def identity_fields(job_id: str) -> Dict[str, str]:
        """Return the array or heterogeneous job parent and child IDs encoded in a job ID."""
        match = re.fullmatch(r"([0-9]+)([_+])([0-9]+)", job_id)
        if match is None:
            return {}
        if match.group(2) == "_":
            return {"slurm_array_job_id": match.group(1), "slurm_array_task_id": match.group(3)}
        return {"slurm_het_job_id": match.group(1), "slurm_het_job_offset": match.group(3)}

    def _row_task_ids(self, line: str, job_id: str, fields: List[str]) -> Optional[List[str]]:
        """Return the task IDs a squeue row stands for, or None for an unusable array range.

        One such row, typically a task range cut by SLURM_BITSTR_LEN in a saved trace, is counted
        as rejected rather than failing the whole snapshot (qtop/qtop#606); conflicts still raise.
        """
        try:
            if len(fields) == 11 and not (fields[10] in ("", "N/A") and "_" not in job_id):
                return self.task_ids(job_id, fields[9], fields[10])
            return self.expand_job_id(job_id) if "_[" in job_id else [job_id]
        except SlurmArrayIdError as error:
            self.rejected_squeue_rows += 1
            logger.warning("Line: %s rejected: %s", line, error)
            return None

    def extract_squeue(self, orig_file: str) -> List[Dict[str, Any]]:
        """Parse squeue output; live collection also passes --states=all.

        squeue -r -h -O 'JobID:0|,UserName:0|,StateCompact:0|,Partition:0|,NumCPUs:0|,NodeList:0|,tres-alloc:0|,tres-per-node:0|,tres-per-task:0|,ArrayJobID:0|,ArrayTaskID:0'
        """
        self.rejected_squeue_rows = 0
        try:
            check_empty_file(orig_file)
        except FileEmptyError:
            logger.error("File %s seems to be empty.", orig_file)
            return []

        jobs: Dict[str, Any] = OrderedDict()
        expanded_count = 0
        # Per-trace caches: a large squeue repeats few distinct users, states, partitions and CPU counts.
        states: Dict[str, Tuple[str, str]] = {}
        partition_lists: Dict[str, Optional[Tuple[str, ...]]] = {}
        cpu_counts: Dict[str, int] = {}
        with open_input(orig_file, "r", encoding="utf-8", errors="replace", options=self.options) as fin:
            for line in fin:
                line = line.strip()
                if not line:
                    continue
                fields = [field.strip() for field in line.split("|")]
                partitions_seen = None
                if len(fields) in (6, 9, 11):
                    job_id, user, state, partition, cpus, nodes = fields[:6]
                    if job_id and user and state:
                        if partition not in partition_lists:
                            names = [name.strip().rstrip("*") for name in partition.split(",")]
                            partition_lists[partition] = tuple(OrderedDict.fromkeys(names)) if all(names) else None
                        partitions_seen = partition_lists[partition]
                if partitions_seen is None:
                    self.rejected_squeue_rows += 1
                    logger.warning("Line: %s not properly parsed as Slurm squeue output.", line)
                    continue

                task_ids = self._row_task_ids(line, job_id, fields)
                if task_ids is None:
                    continue
                if "_[" in job_id or len(task_ids) > 1:
                    expanded_count += len(task_ids)
                    if expanded_count > self.MAX_EXPANDED_TASKS:
                        raise ValueError("Too many grouped Slurm array tasks; collect squeue with -r/--array.")
                if state not in states:
                    mapped = self._map_job_state(state)
                    states[state] = (mapped, slurm_accounting_state(mapped))
                mapped_state, accounting = states[state]
                if cpus not in cpu_counts:
                    cpu_counts[cpus] = max(1, self._safe_int(cpus, default=1))
                for task_id in task_ids:
                    job = {
                        "JobId": task_id,
                        "UnixAccount": user,
                        "S": mapped_state,
                        "raw_state": state,
                        "Queue": partitions_seen[0],
                        "slurm_partitions": list(partitions_seen),
                        "accounting_state": accounting,
                        "CPUs": cpu_counts[cpus],
                        "Nodes": nodes,
                    }
                    if "_" in task_id or "+" in task_id:
                        job.update(self.identity_fields(task_id))
                    if len(fields) >= 9:
                        job["slurm_tres_alloc"], job["slurm_tres_per_node"], job["slurm_tres_per_task"] = fields[6:9]
                    if task_id in jobs and jobs[task_id] != job:
                        previous = jobs[task_id]
                        ignored = {"Queue", "slurm_partitions"}
                        comparable = {key: value for key, value in job.items() if key not in ignored}
                        if job["S"] in SLURM_QUEUED_STATES and {key: value for key, value in previous.items() if key not in ignored} == comparable:
                            previous["slurm_partitions"] = list(OrderedDict.fromkeys(previous["slurm_partitions"] + list(partitions_seen)))
                            continue
                        raise ValueError("Conflicting Slurm records for job %s; collect a fresh squeue trace with -r/--array." % task_id)
                    jobs[task_id] = job
        return list(jobs.values())

    def extract_sinfo(self, orig_file: str) -> List[Dict[str, Any]]:
        """Parse sinfo output.

        sinfo -N -h -O 'NodeList:0|,Partition:0|,StateCompact:0|,CPUs:0|,Gres:0|,GresUsed:0|,Available:0'
        """
        self.rejected_sinfo_rows = 0
        try:
            check_empty_file(orig_file)
        except FileEmptyError:
            logger.error("File %s seems to be empty.", orig_file)
            return []

        nodes = []
        with open_input(orig_file, "r", encoding="utf-8", errors="replace", options=self.options) as fin:
            for line in fin:
                line = line.strip()
                if not line:
                    continue
                try:
                    fields = [field.strip() for field in line.split("|")]
                    if len(fields) not in (4, 5, 6, 7):
                        raise ValueError("expected 4/6 node fields with optional partition availability")
                    node_name, partition, state, cpus = fields[:4]
                    if not all((node_name, partition.rstrip("*"))):
                        raise ValueError("missing required node field")
                except ValueError:
                    self.rejected_sinfo_rows += 1
                    logger.warning("Line: %s not properly parsed as Slurm sinfo output.", line)
                    continue

                nodes.append(
                    {
                        "domainname": node_name,
                        "raw_domainname": node_name,
                        "qname": partition.rstrip("*"),
                        "state": self._map_node_state(state),
                        "slurm_node_state": state,
                        "partition_state": (fields[-1].lower() or "?") if len(fields) in (5, 7) else "?",
                        "np": str(max(0, self._safe_int(cpus, default=0))),
                    }
                )
                if len(fields) >= 6:
                    nodes[-1].update(zip(("slurm_gres", "slurm_gres_used"), fields[4:6]))
        return nodes

    @staticmethod
    def _safe_int(value: Any, default: int) -> int:
        try:
            return int(value)
        except (TypeError, ValueError):
            return default

    @staticmethod
    def _map_job_state(state: str) -> str:
        normalized = state.upper().strip()
        if normalized in SLURM_COMPACT_JOB_STATES:
            return normalized
        return SLURM_JOB_STATES.get(normalized, normalized or "?")

    @staticmethod
    def _map_node_state(state: str) -> str:
        normalized = state.lower().strip()
        normalized = normalized.rstrip("*+#~$@%!^-")
        state_map = {
            "idle": "-",
            "alloc": "b",
            "allocated": "b",
            "allocated+": "b",
            "mix": "b",
            "mixed": "b",
            "block": "d",
            "blocked": "d",
            "comp": "c",
            "completing": "c",
            "down": "d",
            "drain": "d",
            "drained": "d",
            "drng": "d",
            "draining": "d",
            "fail": "d",
            "failg": "d",
            "failing": "d",
            "maint": "d",
            "maintenance": "d",
            "resv": "r",
            "reserved": "r",
            "planned": "-",
            "plnd": "-",
            "future": "?",
            "futr": "?",
            "power_down": "d",
            "pow_dn": "d",
            "powered_down": "d",
            "power_up": "c",
            "pow_up": "c",
            "powering_up": "c",
            "unknown": "?",
            "unk": "?",
        }
        return state_map.get(normalized, "?")


class SlurmBatchSystem(GenericBatchSystem, HostlistParser):
    """Slurm backend."""

    signature_command = "sinfo"
    default_commands = {
        # Copy the command after ', ' directly into a shell for debugging.
        # Include all retained job states, including suspended jobs; --all opts into hidden partitions.
        "squeue_file": (
            "%(savepath)s/squeue%(pid)s.txt, squeue --states=all -r -h -O "
            "'JobID:0|,UserName:0|,StateCompact:0|,Partition:0|,NumCPUs:0|,NodeList:0|,tres-alloc:0|,tres-per-node:0|,tres-per-task:0|,ArrayJobID:0|,ArrayTaskID:0'"
        ),
        "sinfo_file": "%(savepath)s/sinfo%(pid)s.txt, sinfo -N -h -O 'NodeList:0|,Partition:0|,StateCompact:0|,CPUs:0|,Gres:0|,GresUsed:0|,Available:0'",
        # Collect Slurm CPU IDs by default; missing/failed details use estimates.
        "scontrol_file": "%(savepath)s/scontrol%(pid)s.txt, scontrol --details show jobs",
        # Slurm topology/block (e.g. NVL72 NVLink domains); other topologies list no blocks.
        "topology_file": "%(savepath)s/topology%(pid)s.txt, scontrol show topology",
    }
    state_abbreviations = {
        "BF": "failed_boot_of_user",
        "R": "running_of_user",
        "RH": "hold_of_user",
        "PD": "queued_of_user",
        "CA": "cancelled_of_user",
        "CD": "finished_of_user",
        "CG": "exiting_of_user",
        "CF": "configuring_of_user",
        "DL": "deadline_of_user",
        "F": "failed_of_user",
        "NF": "failed_node_of_user",
        "OOM": "out_of_memory_of_user",
        "TO": "timeout_of_user",
        "PR": "preempted_of_user",
        "RD": "hold_of_user",
        "RF": "queued_of_user",
        "RQ": "queued_of_user",
        "RS": "resizing_of_user",
        "RV": "revoked_of_user",
        "SI": "signaling_of_user",
        "SE": "special_exit_of_user",
        "SO": "stage_out_of_user",
        "S": "suspended_of_user",
        "ST": "stopped_of_user",
    }

    # squeue cuts array task ranges (453906_[2,6-14,...]) at SLURM_BITSTR_LEN characters unless it is 0
    # (qtop/qtop#606); this overrides a user's own setting, which suits interactive squeue but breaks parsing.
    command_env = {"SLURM_BITSTR_LEN": "0"}

    @staticmethod
    def add_cli_options(parser: Any) -> None:
        """--slurm-summary, --slurm-json-squeue and --all, for the slurm and slurm_json backends."""
        slurm_mode = parser.add_mutually_exclusive_group()
        slurm_mode.add_argument(
            "--slurm-summary",
            action="store_true",
            dest="SLURM_SUMMARY",
            help="Use only Slurm sinfo/squeue on each display cycle; CPU placement is estimated and per-job GPU indices are unavailable",
        )
        slurm_mode.add_argument(
            "--slurm-json-squeue", action="store_true", dest="SLURM_JSON_SQUEUE", help="Use squeue --json for slurm_json jobs when scontrol returns empty job JSON"
        )
        parser.add_argument(
            "--all",
            action="store_true",
            dest="ALL_PARTITIONS",
            help="Include hidden and group-unavailable partitions in live Slurm queries (subject to permissions)",
        )

    @staticmethod
    def check_cli_options(parser: Any, args: Any) -> None:
        """--slurm-summary selects the legacy slurm backend; it accepts -b slurm or auto."""
        if getattr(args, "SLURM_SUMMARY", False):
            if args.BATCH_SYSTEM and args.BATCH_SYSTEM.lower() not in ("auto", "slurm"):
                parser.error("--slurm-summary requires -b slurm or auto")
            args.BATCH_SYSTEM = "slurm"

    native_queue_membership = True
    optional_trace_keys: Tuple[str, ...] = ("scontrol_file", "topology_file")
    optional_trace_aliases = {"scontrol_file": "scontrol.txt", "topology_file": "topology.txt"}
    optional_trace_warning = "Using estimated Slurm CPU placement."
    optional_trace_warnings = {"topology_file": "Slurm topology blocks are not shown."}
    detection_priority = -2
    extensions = SLURM_EXTENSION
    node_job_fields = SLURM_EXTENSION.node_job_fields
    blocked_queue_note = " (partition closed to submissions or scheduling)"

    @staticmethod
    def collection_command(key: str, command: str, options: Any) -> Optional[str]:
        """Skip details in summary mode and respect --all for live collection."""
        if key in ("scontrol_file", "topology_file") and getattr(options, "SLURM_SUMMARY", False):
            return None
        if getattr(options, "ALL_PARTITIONS", False) and not options.SOURCEDIR and key in ("squeue_file", "sinfo_file", "scontrol_file"):
            command += " --all"
        return command

    @staticmethod
    def queue_is_blocked(state: object) -> bool:
        """Return True if the partition is down, drained or inactive."""
        return str(state).lower() in ("down", "drain", "inact", "inactive") or GenericBatchSystem.queue_is_blocked(state)

    @staticmethod
    def node_state_marker(node: "Node") -> str:
        """Return the Slurm node-state suffix (*, +, #, ~, ...) shown in the matrix."""
        state = node.get("slurm_node_state", "")
        return state[-1] if state and state[-1] in "*+#~$@%!^-" else ""

    @staticmethod
    def display_messages(section: str, snapshot: object, nodes: "List[Node]", config: "Config") -> List[str]:
        """Return the Slurm notes for a section: rejected rows, accounting rules, CPU placement caveats and accelerators."""
        if section == "snapshot":
            return SlurmBatchSystem._snapshot_messages(snapshot)
        if section == "accounting":
            return SlurmBatchSystem._accounting_messages(snapshot)
        if section == "placement":
            return SlurmBatchSystem._placement_messages(nodes)
        if section == "accelerators":
            return SlurmBatchSystem._accelerator_messages(nodes, config.get("matrix_accelerators", ACCELERATOR_KINDS))
        if section == "resources":
            return SlurmBatchSystem._block_messages(nodes, config) + (SlurmBatchSystem._gpu_pair_messages(nodes, config) if config.get("experimental_gpu_pairs") else [])
        return []

    @staticmethod
    def _accelerator_messages(nodes: "List[Node]", kinds: Sequence[str] = ACCELERATOR_KINDS) -> List[str]:
        """List each node's GPUs, TPUs and NPUs with their jobs; the view places them (accelerator_map)."""
        from qtop_py.schedulers.gpu import accelerator_map_lines

        lines = accelerator_map_lines(nodes, kinds)
        return ["INFO: Accelerators by node (job IDs, ? = unknown):"] + ["INFO: " + line for line in lines] if lines else []

    @staticmethod
    def _block_messages(nodes: "List[Node]", config: "Config") -> List[str]:
        """Experimental: one line per topology block (NVLink domain) with free/busy nodes, idle accelerators and jobs across blocks.

        Shown only with experimental_nvlink_blocks: true (the configuration normalizes YAML text to a bool).
        """
        if config.get("experimental_nvlink_blocks") is not True:
            return []
        from qtop_py.schedulers.gpu import device_status, node_devices

        blocks: Dict[str, List[Mapping[str, Any]]] = OrderedDict()
        job_blocks: Dict[str, Set[str]] = {}
        for node in nodes:
            if "slurm_block" not in node:
                continue
            blocks.setdefault(node["slurm_block"], []).append(node)
            for job_id in set(node["core_job_map"].values()) | set(node.get("slurm_job_ids", ())):
                job_blocks.setdefault(job_id, set()).add(node["slurm_block"])
        lines = []
        for name, members in blocks.items():
            jobs = [set(member["core_job_map"].values()) | set(member.get("slurm_job_ids", ())) for member in members]
            free = sum(1 for member, held in zip(members, jobs) if str(member["state"][0]) == "-" and not held and not SLURM_EXTENSION.node_busy(member))
            busy = sum(1 for held in jobs if held)
            parts = ["%s nodes (%s free, %s busy)" % (len(members), free, busy)]
            for kind in (kind for kind in ACCELERATOR_KINDS if kind in config.get("matrix_accelerators", ACCELERATOR_KINDS)):
                statuses = [device_status(member, kind, index) for member in members for index in node_devices(member, kind)]
                if statuses:
                    parts.append("%s %s/%s idle" % (kind, statuses.count("idle"), len(statuses)))
            spanning = sorted({job_id for held in jobs for job_id in held if len(job_blocks[job_id]) > 1}, key=lambda job_id: (len(job_id), job_id))
            if spanning:
                parts.append("jobs across blocks: " + " ".join(spanning))
            lines.append("INFO: %s: %s" % (name, ", ".join(parts)))
        return ["INFO: Topology blocks (Slurm topology/block, e.g. NVLink domains):"] + lines if lines else []

    @staticmethod
    def _snapshot_messages(snapshot: object) -> List[str]:
        """Warn about rejected rows and jobs on missing or invalid node lists, with what to check."""
        lines = []
        diagnostics = getattr(snapshot, "_diagnostics", {}).get("slurm_diagnostics", {})
        if any(diagnostics.values()):
            lines.append(
                "WARNING: Slurm snapshot incomplete: %s rejected job rows; %s rejected node rows; "
                "%s active jobs reference missing nodes; %s active jobs have invalid node lists."
                % tuple(diagnostics.get(key, 0) for key in ("rejected_squeue_rows", "rejected_sinfo_rows", "jobs_with_missing_nodes", "jobs_with_invalid_nodelists"))
            )
        if diagnostics.get("jobs_with_missing_nodes"):
            lines.append("INFO: Jobs on missing nodes: those nodes may be in hidden or group-unavailable partitions; try qtop --all, if not already used.")
        if diagnostics.get("jobs_with_invalid_nodelists"):
            lines.append("INFO: Invalid node lists: a starting or completing job may have no nodes yet; otherwise check that squeue does not cut the NodeList field.")
        if diagnostics.get("rejected_squeue_rows") or diagnostics.get("rejected_sinfo_rows"):
            lines.append(
                "INFO: Rejected rows: check the squeue/sinfo formats in qtopconf.yaml and that SLURM_BITSTR_LEN=0 "
                "when the traces were saved (truncated array ranges); qtop logs each rejected line."
            )
        return lines

    @staticmethod
    def _accounting_messages(snapshot: object) -> List[str]:
        """Explain how array tasks, heterogeneous components and multi-partition jobs are counted."""
        lines = []
        lines.append("INFO: Slurm job counts include individual array tasks and heterogeneous components.")
        if any(len(job.get("slurm_partitions", [])) > 1 for job in getattr(snapshot, "_job_metadata", {}).values()):
            lines.append("INFO: Slurm partition counts include each eligible partition; global job counts are unique.")
        return lines

    @staticmethod
    def _placement_messages(nodes: "List[Node]") -> List[str]:
        """Say where CPU placement is estimated or comes from Slurm, and what the matrix omits."""
        lines = []
        sources = {node.get("cpu_allocation_source") for node in nodes}
        if "estimated" in sources:
            lines.append("WARNING: Slurm CPU placement: estimated where allocation details are unavailable.")
        if "slurm" in sources:
            lines.append("INFO: Slurm CPU placement: scheduler CPU IDs; Linux task affinity is not shown.")
        omitted = sum(node.get("slurm_unmapped_cpus", 0) for node in nodes)
        if omitted:
            lines.append("WARNING: Slurm CPU matrix omits %s shared/excess CPU assignments; job/node membership is retained." % omitted)
        return lines

    @staticmethod
    def _gpu_pair_messages(nodes: "List[Node]", config: "Config") -> List[str]:
        """List the configured GPU pairs (experimental_gpu_pairs) and their jobs."""
        lines = []
        from qtop_py.schedulers.gpu import configured_pair_lines

        try:
            pairs = configured_pair_lines(nodes, config["experimental_gpu_pairs"])
        except ValueError as error:
            lines.append("WARNING: Configured GPU pairs unavailable: %s" % error)
        else:
            if pairs:
                lines.append("INFO: Configured GPU pairs (experimental; job IDs, ? = unknown):")
                lines.extend("INFO: " + line for line in pairs)
        return lines

    def _mask_topology(self, text: str, mask: "TraceMasker") -> str:
        """Mask block and node names of scontrol show topology block records; raise ValueError on any other line."""
        rows = []
        for line in text.splitlines():
            match = TOPOLOGY_BLOCK_RE.fullmatch(line.strip())
            if match is None:
                raise ValueError("unsupported topology line")
            label, block, index, hosts, size = match.groups()
            expanded = self.expand_nodelist(hosts)
            if hosts and not expanded:
                raise ValueError("invalid topology node list")
            nodes = " Nodes=" + ",".join(mask(host, "node") for host in expanded) if hosts else ""
            rows.append("%s=%s BlockIndex=%s%s BlockSize=%s" % (label, mask(block, "block"), index, nodes, size))
        return "\n".join(rows) + "\n"

    def mask_source_trace(self, key: str, text: str, mask: "TraceMasker") -> Tuple[str, str]:
        """Return the masked name and text of an squeue, sinfo, scontrol or topology trace."""
        if key == "topology_file":
            return "topology.txt", self._mask_topology(text, mask)
        if key == "scontrol_file":

            def field(match: Match[str]) -> str:
                name, value = match.groups()
                if name in ("JobId", "ArrayJobId", "HetJobId"):
                    value = mask(value, "job")
                elif name in ("Nodes", "NodeList"):
                    value = ",".join(mask(host, "node") for host in self.expand_nodelist(value))
                elif name in ("CPU_IDs", "NumCPUs", "CPUs/Task", "Mem"):
                    value = mask.public(value, r"[0-9,\-()]*")
                else:
                    name, value = mask(name, "field"), mask(value)
                return name + "=" + value

            if re.sub(r"(?<!\S)\S[^\s=]*=\S*", "", text).strip():
                raise ValueError("unsupported detail syntax")
            return "scontrol.txt", re.sub(r"(?<!\S)(\S[^\s=]*)=(\S*)", field, text)
        if key not in ("squeue_file", "sinfo_file"):
            raise ValueError("unsupported trace")
        rows = []
        for line in text.splitlines():
            if not line.strip():
                rows.append(line)
                continue
            fields = [part.strip() for part in line.split("|")]
            if key == "squeue_file":
                if len(fields) not in (6, 9, 11):
                    raise ValueError("unsupported job row")
                fields[0], fields[1] = mask(fields[0], "job"), mask(fields[1], "user")
                fields[2] = mask.public(fields[2], "(?:" + "|".join(SLURM_COMPACT_JOB_STATES) + ")")
                fields[3] = ",".join(mask(part.rstrip("*"), "queue") + ("*" if part.endswith("*") else "") for part in fields[3].split(","))
                fields[4] = mask.public(fields[4], r"[0-9]+")
                fields[5] = ",".join(mask(host, "node") for host in self.expand_nodelist(fields[5])) if fields[5] not in ("", "(null)", "None", "N/A") else fields[5]
                for index in range(6, min(9, len(fields))):
                    fields[index] = mask(fields[index])
                if len(fields) == 11:
                    fields[9] = mask(fields[9], "job") if fields[9] not in ("", "N/A", "0") else fields[9]
                    fields[10] = mask.public(fields[10], r"(?:[0-9]+|N/A)?")
            else:
                if len(fields) not in (4, 5, 6, 7):
                    raise ValueError("unsupported node row")
                fields[0] = mask(fields[0], "node")
                fields[1] = mask(fields[1].rstrip("*"), "queue") + ("*" if fields[1].endswith("*") else "")
                fields[2] = mask.public(
                    fields[2],
                    r"(?:idle|alloc|allocated|mix|mixed|down|drain|drained|drng|comp|completing|fail|failg|maint|resv|reserved|"
                    r"planned|plnd|future|futr|unknown|unk)(?:[*+#~$@%!^\-])*",
                )
                fields[3] = mask.public(fields[3], r"[0-9]+")
                if len(fields) >= 6:
                    fields[4:6] = [mask(value) for value in fields[4:6]]
                if len(fields) in (5, 7):
                    fields[-1] = mask.public(fields[-1], r"(?:up|down|inactive|drain|unknown|\?)?")
            rows.append("|".join(fields))
        return ("squeue.txt" if key == "squeue_file" else "sinfo.txt"), "\n".join(rows) + "\n"

    ACTIVE_STATES = set(["R", "CG", "CF", "S", "ST", "RS", "SI", "SO"])
    QUEUED_STATES = SLURM_QUEUED_STATES
    STATE_PRIORITY = {"d": 5, "b": 4, "c": 2, "r": 1, "-": 0, "?": -1}
    PARTITION_STATE_PRIORITY = {"?": -1, "up": 0, "drain": 1, "down": 2, "inact": 3, "inactive": 3}

    @staticmethod
    def get_mnemonic() -> str:
        """Return the scheduler name, slurm."""
        return "slurm"

    def __init__(self, scheduler_output_filenames: Dict[str, str], config: "Config", options: "Options") -> None:
        # "" (falsy, like the former None) when a trace is not configured; reading it then fails as an OSError.
        self.squeue_file = scheduler_output_filenames.get("squeue_file", "")
        self.sinfo_file = scheduler_output_filenames.get("sinfo_file", "")
        self.scontrol_file = "" if getattr(options, "SLURM_SUMMARY", False) else scheduler_output_filenames.get("scontrol_file", "")
        self.topology_file = "" if getattr(options, "SLURM_SUMMARY", False) else scheduler_output_filenames.get("topology_file", "")
        self.config = config
        self.options = options
        self.slurm_stat_maker = SlurmStatExtractor(self.config, self.options)

    def get_cluster_state(self, options: "Options") -> "ClusterState":
        """Return the cluster state with Slurm partitions, accounting states and array IDs."""
        state = super().get_cluster_state(options)
        for job, raw in zip(state["jobs"], self._get_jobs()):
            job["slurm_partitions"] = list(raw["slurm_partitions"])
            job["accounting_state"] = raw["accounting_state"]
            if "slurm_array_job_id" in raw:
                job["slurm_array_job_id"] = raw["slurm_array_job_id"]
                job["slurm_array_task_id"] = raw["slurm_array_task_id"]
            if "slurm_het_job_id" in raw:
                job["slurm_het_job_id"] = raw["slurm_het_job_id"]
                job["slurm_het_job_offset"] = raw["slurm_het_job_offset"]
            if "slurm_tres_alloc" in raw:
                job["slurm_tres_alloc"] = raw["slurm_tres_alloc"]
                job["slurm_tres_per_node"] = raw["slurm_tres_per_node"]
                job["slurm_tres_per_task"] = raw["slurm_tres_per_task"]
        diagnostics = dict(self.allocation_diagnostics)
        for source in ("squeue", "sinfo"):
            count = getattr(self.slurm_stat_maker, "rejected_%s_rows" % source, 0)
            if count:
                diagnostics["rejected_%s_rows" % source] = count
        if diagnostics:
            state["slurm_diagnostics"] = diagnostics
        return state

    def get_jobs_info(self) -> "JobsInfo":
        """Return job IDs, users, states and partitions from squeue."""
        jobs = self._get_jobs()
        job_ids = [job["JobId"] for job in jobs]
        usernames = [job["UnixAccount"] for job in jobs]
        job_states = [job["S"] for job in jobs]
        queue_names = [job["Queue"] for job in jobs]

        logger.debug(
            "job_ids, usernames, job_states, queue_names lengths: %(job_ids)s, %(usernames)s, %(job_states)s, %(queue_names)s",
            {"job_ids": len(job_ids), "usernames": len(usernames), "job_states": len(job_states), "queue_names": len(queue_names)},
        )
        return job_ids, usernames, job_states, queue_names

    def get_queues_info(self) -> "QueuesInfo":
        """Return the running and queued totals and per-partition records."""
        queue_counts: Dict[str, Any] = OrderedDict()
        total_running_jobs = 0
        total_queued_jobs = 0

        for node in self._get_nodes():
            queue = queue_counts.setdefault(node["qname"], {"run": 0, "queued": 0, "lm": "--", "state": "?"})
            state = node.get("partition_state", "?")
            if queue["state"] == "?" or self.PARTITION_STATE_PRIORITY.get(state, -1) > self.PARTITION_STATE_PRIORITY.get(queue["state"], -1):
                queue["state"] = state

        for job in self._get_jobs():
            if job["accounting_state"] == "running":
                total_running_jobs += 1
            elif job["accounting_state"] == "queued":
                total_queued_jobs += 1
            for name in job["slurm_partitions"]:
                queue = queue_counts.setdefault(name, {"run": 0, "queued": 0, "lm": "--", "state": "?"})
                if job["accounting_state"] == "running":
                    queue["run"] += 1
                elif job["accounting_state"] == "queued":
                    queue["queued"] += 1

        # Built in the return so QueuesInfo types each record as a queue record.
        return (
            total_running_jobs,
            total_queued_jobs,
            [
                {
                    "queue_name": queue_name,
                    "run": str(values["run"]),
                    "queued": str(values["queued"]),
                    "lm": values["lm"],
                    "state": values["state"],
                }
                for queue_name, values in queue_counts.items()
            ],
        )

    def get_worker_nodes(self, job_ids: List[str], job_queues: List[str], options: "Options") -> "List[Node]":
        """Return the nodes from sinfo, merged across partitions, with their jobs, CPUs and GPUs."""
        worker_nodes_by_name: Dict[str, Dict[str, Any]] = OrderedDict()

        for node in self._get_nodes():
            raw_name = node["raw_domainname"]
            worker_node = worker_nodes_by_name.setdefault(
                raw_name,
                {
                    "domainname": node["domainname"],
                    "np": node["np"],
                    "state": node["state"],
                    "qname": [],
                    "core_job_map": {},
                },
            )
            if node["qname"] not in worker_node["qname"]:
                worker_node["qname"].append(node["qname"])
            merged_state = self._merge_node_state(worker_node["state"], node["state"])
            raw_state = node.get("slurm_node_state", "")
            previous_raw = worker_node.get("slurm_node_state", "")
            if raw_state and (
                "slurm_node_state" not in worker_node
                or (
                    merged_state == node["state"]
                    and (merged_state != worker_node["state"] or (raw_state.endswith(tuple("*+#~$@%!^-")) and not previous_raw.endswith(tuple("*+#~$@%!^-"))))
                )
            ):
                worker_node["slurm_node_state"] = raw_state
            worker_node["state"] = merged_state
            worker_node["np"] = str(max(int(worker_node["np"]), int(node["np"])))
            for key in ("slurm_gres", "slurm_gres_used"):
                if key in node:
                    worker_node[key] = node[key]

        expanded: Dict[Any, List[str]] = {}  # this collection's job node lists, expanded once per distinct text

        def job_hosts(job: Mapping[str, Any]) -> List[str]:
            if job["Nodes"] not in expanded:
                expanded[job["Nodes"]] = self.expand_nodelist(job["Nodes"])
            return expanded[job["Nodes"]]

        self.allocation_diagnostics: Dict[str, Any] = {}
        node_jobs = self._map_jobs_to_nodes(self._get_jobs(), worker_nodes_by_name, self.allocation_diagnostics, job_hosts)
        for raw_name, worker_node in worker_nodes_by_name.items():
            worker_node["core_job_map"] = dict(enumerate(node_jobs.get(raw_name, ())))

        self._apply_cpu_allocations(worker_nodes_by_name, job_hosts)
        self._apply_gpu_allocations(worker_nodes_by_name, job_hosts)
        self._apply_blocks(worker_nodes_by_name)
        for node in worker_nodes_by_name.values():
            node.pop("slurm_unmapped_cpus", None)
        mapped: Dict[str, Any] = {}
        for node in worker_nodes_by_name.values():
            for job_id in node["core_job_map"].values():
                mapped[job_id] = mapped.get(job_id, 0) + 1
        for job in self._get_jobs():
            if job["S"] not in self.ACTIVE_STATES:
                continue
            hosts = [host for host in job_hosts(job) if host in worker_nodes_by_name]
            missing = job["CPUs"] - mapped.get(job["JobId"], 0)
            if hosts and missing > 0:
                node = worker_nodes_by_name[hosts[0]]
                node["slurm_unmapped_cpus"] = node.get("slurm_unmapped_cpus", 0) + missing
        worker_nodes = list(worker_nodes_by_name.values())
        logger.info("worker_nodes contains %s entries", len(worker_nodes))
        return cast("List[Node]", worker_nodes)  # built field by field as Node records

    def _read_blocks(self) -> Dict[str, str]:
        """Return {node: block} from the optional topology trace; empty if it is missing, unreadable or not topology/block."""
        if not self.topology_file:
            return {}
        try:
            with open_input(self.topology_file, "r", encoding="utf-8", errors="replace", options=self.options) as stream:
                text = stream.read()
        except OSError as error:
            logger.info("Slurm topology unavailable: %s", error)
            return {}
        blocks: Dict[str, str] = {}
        for line in text.splitlines():
            match = TOPOLOGY_BLOCK_RE.fullmatch(line.strip())
            if match is None or match.group(1) != "BlockName" or match.group(4) is None:
                continue  # aggregated blocks, tree switches, messages and empty blocks
            hosts = self.expand_nodelist(match.group(4))
            if not hosts:
                logger.warning("Ignoring Slurm topology block with an invalid node list: %s", match.group(2))
            for host in hosts:
                blocks.setdefault(host, match.group(2))
        return blocks

    def _apply_blocks(self, worker_nodes: Dict[str, Dict[str, Any]]) -> None:
        """Set slurm_block on each node in a topology block."""
        blocks = self._read_blocks()
        for raw_name, node in worker_nodes.items():
            if raw_name in blocks:
                node["slurm_block"] = blocks[raw_name]

    @staticmethod
    def _allocation_job_id(header: str) -> Optional[str]:
        # Job names are arbitrary text and must not be interpreted as identity fields.
        # Sonar regex finding: linear here, since the lookbehind only lets a match start at the beginning of a whitespace run; see docs/sonar-review.md
        header = re.split(r"(?<!\s)\s+JobName=", header, maxsplit=1)[0]
        pairs = re.findall(r"(?:^|\s)(JobId|ArrayJobId|ArrayTaskId|HetJobId|HetJobOffset)=(\S+)", header)
        fields: Dict[str, str] = dict(pairs)
        if len(fields) != len(pairs):
            return None
        array = "ArrayJobId" in fields or "ArrayTaskId" in fields
        het = "HetJobId" in fields or "HetJobOffset" in fields
        if array and het:
            return None
        if array or het:
            base, index = ("ArrayJobId", "ArrayTaskId") if array else ("HetJobId", "HetJobOffset")
            if not all(re.fullmatch(r"[0-9]+", fields.get(key, "")) for key in (base, index)):
                return None
            return fields[base] + ("_" if array else "+") + fields[index]
        job_id = fields.get("JobId", "")
        return job_id if re.fullmatch(r"[0-9]+(?:_[0-9]+|\+[0-9]+)?", job_id) else None

    def _apply_gpu_allocations(self, worker_nodes: Dict[str, Dict[str, Any]], job_hosts: Optional[Callable[[Mapping[str, Any]], List[str]]] = None) -> None:
        for node in worker_nodes.values():
            for raw, parsed in (("slurm_gres", "slurm_gpu_configured"), ("slurm_gres_used", "slurm_gpu_used")):
                if raw in node:
                    try:
                        node[parsed] = parse_accelerator_gres(node[raw])
                    except ValueError as error:
                        logger.warning("Ignoring invalid Slurm GPU metadata: %s", error)
        if not self.scontrol_file:
            return
        allocations, invalid = self._read_gpu_allocations()
        for job in self._get_jobs():
            job_id = job["JobId"]
            if job["S"] not in self.ACTIVE_STATES or job_id in invalid:
                continue
            allocated = set(job_hosts(job) if job_hosts else self.expand_nodelist(job["Nodes"]))
            allocation = allocations.get(job_id, {})
            if not set(allocation).issubset(allocated):
                continue
            for host, resources in allocation.items():
                if host in worker_nodes:
                    bindings = worker_nodes[host].setdefault("slurm_gpu_bindings", [])
                    bindings.extend(dict(resource, job_id=job_id) for resource in resources)

    def _read_gpu_allocations(self) -> Tuple[Dict[str, Any], Set[str]]:
        """Return GPU allocations by job and host from the scontrol details, and the jobs whose details are invalid.

        An unreadable file yields none, so no GPU is bound to a job.
        """
        allocations: Dict[str, Any] = {}
        invalid: Set[str] = set()
        try:
            with open_input(self.scontrol_file, "r", encoding="utf-8", errors="replace", options=self.options) as stream:
                job_id = None
                for line in stream:
                    if re.match(r"\s*JobId=", line):
                        job_id = self._allocation_job_id(line)
                        continue
                    if job_id is None or not re.match(r"\s*Nodes=", line):
                        continue
                    fields = dict(re.findall(r"(?:^|\s)(Nodes|GRES)=(\S*)", line))
                    if "GRES" not in fields:
                        continue
                    hosts = self.expand_nodelist(fields["Nodes"])
                    try:
                        resources = parse_accelerator_gres(fields["GRES"])
                    except ValueError:
                        invalid.add(job_id)
                        continue
                    if not hosts or len(hosts) * max(1, sum(resource["count"] for resource in resources)) > self.MAX_EXPANDED_NODES:
                        invalid.add(job_id)
                        continue
                    allocation = allocations.setdefault(job_id, {})
                    for host in hosts:
                        if host in allocation:
                            invalid.add(job_id)
                        allocation[host] = resources
        except (OSError, UnicodeError) as error:
            logger.warning("Cannot read optional Slurm GPU details: %s", error)
            return {}, set()
        return allocations, invalid

    def _read_cpu_allocations(self) -> Dict[str, Any]:
        allocations: Dict[str, Any] = {}
        if not self.scontrol_file:
            return allocations
        try:
            with open_input(self.scontrol_file, "r", encoding="utf-8", errors="replace", options=self.options) as stream:
                job_id = None
                parsed: Dict[str, Tuple[int, ...]] = {}  # CPU_IDs text -> its distinct CPU numbers, for this collection
                for line in stream:
                    header = re.match(r"\s*JobId=(\S+)(.*)", line)
                    if header:
                        job_id = self._allocation_job_id(header.group(0))
                        if job_id is not None:
                            allocations.setdefault(job_id, {})
                        continue
                    if not re.match(r"\s*(?:Nodes|CPU_IDs)=", line):
                        continue
                    detail = dict(re.findall(r"(?:^|\s)(Nodes|CPU_IDs)=(\S+)", line))
                    if "Nodes" in detail and "CPU_IDs" in detail and job_id is not None:
                        hosts = self.expand_nodelist(detail["Nodes"])
                        cpus: Any = parsed.get(detail["CPU_IDs"])
                        if cpus is None:  # invalid lists are reparsed, so they warn and fail exactly as before
                            cpus = self.expand_nodelist("[" + detail["CPU_IDs"] + "]")
                            if cpus and len(parsed) < 4096 and all(cpu.isdecimal() for cpu in cpus) and len(set(map(int, cpus))) == len(cpus):
                                cpus = parsed[detail["CPU_IDs"]] = tuple(map(int, cpus))
                        if not hosts or not cpus or len(hosts) * len(cpus) > self.MAX_EXPANDED_NODES or (type(cpus) is not tuple and len(set(map(int, cpus))) != len(cpus)):
                            allocations[job_id] = None
                        else:
                            allocation = allocations[job_id]
                            if allocation is not None:
                                for host in hosts:
                                    if host in allocation:
                                        allocations[job_id] = None
                                        break
                                    allocation[host] = cpus if type(cpus) is tuple else tuple(map(int, cpus))
        except (OSError, UnicodeError, ValueError) as error:
            logger.warning("Cannot read optional Slurm CPU details: %s; using estimates.", error)
            return {}
        return allocations

    def _apply_cpu_allocations(self, worker_nodes: Dict[str, Dict[str, Any]], job_hosts: Optional[Callable[[Mapping[str, Any]], List[str]]] = None) -> None:
        details = self._read_cpu_allocations()
        exact: Dict[str, Dict[str, Any]] = {name: {} for name in worker_nodes}
        estimated = set()
        for job in self._get_jobs():
            if job["S"] not in self.ACTIVE_STATES:
                continue
            hosts = job_hosts(job) if job_hosts else self.expand_nodelist(job["Nodes"])
            allocation = details.get(job["JobId"]) or {}
            valid = allocation and set(allocation) == set(hosts) and sum(len(cpus) for cpus in allocation.values()) == job["CPUs"]
            if valid:
                valid = all(host in worker_nodes and max(allocation[host]) < int(worker_nodes[host]["np"]) for host in hosts)  # CPU lists are nonempty
            if not valid:
                estimated.update(hosts)
                if job["JobId"] in details:
                    logger.warning("Incomplete or inconsistent Slurm CPU details for job %s; using estimates.", job["JobId"])
                continue
            for host, cpus in allocation.items():
                for cpu in cpus:
                    if cpu in exact[host] and exact[host][cpu] != job["JobId"]:
                        estimated.add(host)
                        logger.warning("Overlapping Slurm CPU allocations on %s; using estimates.", host)
                    exact[host][cpu] = job["JobId"]
        for host, node in worker_nodes.items():
            if exact[host] and host not in estimated:
                node["core_job_map"] = exact[host]
                node["cpu_allocation_source"] = "slurm"
            else:
                node["cpu_allocation_source"] = "estimated" if host in estimated else "none"

    def _get_jobs(self) -> List[Dict[str, Any]]:
        if not hasattr(self, "_jobs"):
            self._jobs = self.slurm_stat_maker.extract_squeue(self.squeue_file)
        return self._jobs

    def _get_nodes(self) -> List[Dict[str, Any]]:
        if not hasattr(self, "_nodes"):
            self._nodes = self.slurm_stat_maker.extract_sinfo(self.sinfo_file)
        return self._nodes

    @classmethod
    def _merge_node_state(cls, previous: str, current: str) -> str:
        previous_priority = cls.STATE_PRIORITY.get(previous, 0)
        current_priority = cls.STATE_PRIORITY.get(current, 0)
        return current if current_priority > previous_priority else previous

    @classmethod
    def _map_jobs_to_nodes(
        cls,
        jobs: List[Dict[str, Any]],
        worker_nodes_by_name: Mapping[str, Any],
        diagnostics: Optional[Dict[str, Any]] = None,
        job_hosts: Optional[Callable[[Mapping[str, Any]], List[str]]] = None,
    ) -> Dict[str, List[str]]:
        node_jobs: Dict[str, List[str]] = OrderedDict((node_name, []) for node_name in worker_nodes_by_name)
        for node in worker_nodes_by_name.values():
            node["slurm_job_ids"] = []
        for job in jobs:
            if job["S"] not in cls.ACTIVE_STATES:
                continue
            allocated_nodes = job_hosts(job) if job_hosts else cls.expand_nodelist(job["Nodes"])
            if diagnostics is not None:
                key = "jobs_with_invalid_nodelists" if not allocated_nodes else "jobs_with_missing_nodes"
                if not allocated_nodes or any(node not in worker_nodes_by_name for node in allocated_nodes):
                    diagnostics[key] = diagnostics.get(key, 0) + 1
            nodes = [node for node in allocated_nodes if node in worker_nodes_by_name]
            if not nodes:
                continue

            for node in nodes:
                worker_nodes_by_name[node]["slurm_job_ids"].append(job["JobId"])
            remaining_cpus = max(1, job["CPUs"])
            for idx, node in enumerate(nodes):
                capacity = int(worker_nodes_by_name[node]["np"])
                available = max(0, capacity - len(node_jobs[node]))
                if not available:
                    continue
                nodes_left = len(nodes) - idx
                cpus_for_node = max(1, remaining_cpus // nodes_left)
                cpus_for_node = min(available, cpus_for_node, remaining_cpus)
                node_jobs[node].extend([job["JobId"]] * cpus_for_node)
                remaining_cpus -= cpus_for_node
                if remaining_cpus <= 0:
                    break
            # Revisit earlier nodes when a smaller node could not take its share.
            for node in nodes:
                available = max(0, int(worker_nodes_by_name[node]["np"]) - len(node_jobs[node]))
                count = min(available, remaining_cpus)
                node_jobs[node].extend([job["JobId"]] * count)
                remaining_cpus -= count
        return node_jobs
