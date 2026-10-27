##
## qtop is a tool to monitor queuing systems - https://github.com/qtop/qtop
##
## Copyright (c) 2016 Fotis Georgatos
## Copyright (c) 2016 Sotiris Fragkiskos
## Copyright (c) 2026 Nicola Trozzi
##
## SPDX-License-Identifier: MIT
##

import logging
import re
from collections import OrderedDict
from typing import List, Optional

import qtop_py.fileutils as fileutils
from qtop_py.gpu import parse_gpu_gres
from qtop_py.serialiser import GenericBatchSystem, StatExtractor

SLURM_JOB_STATES = {
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
SLURM_COMPACT_JOB_STATES = set(SLURM_JOB_STATES.values())
SLURM_QUEUED_STATES = {"PD", "CF", "RD", "RF", "RH", "RQ", "SE"}


class SlurmStatExtractor(StatExtractor):
    MAX_EXPANDED_TASKS = 100000

    @classmethod
    def expand_job_id(cls, job_id: str) -> List[str]:
        """Expand complete legacy array groups into bounded, scalar task IDs."""
        if "_[" not in job_id:
            return [job_id]
        match = re.fullmatch(r"([0-9]+)_\[([0-9,:-]+)(?:%[1-9][0-9]*)?\]", job_id)
        error = "Invalid, truncated or oversized Slurm array ID %r; collect squeue with -r/--array." % job_id
        if match is None:
            raise ValueError(error)
        tasks = set()
        expanded_count = 0
        for token in match.group(2).split(","):
            span = re.fullmatch(r"([0-9]{1,10})(?:-([0-9]{1,10})(?::([0-9]{1,10}))?)?", token)
            if span is None:
                raise ValueError(error)
            first = int(span.group(1))
            last = int(span.group(2)) if span.group(2) is not None else first
            step = int(span.group(3)) if span.group(3) is not None else 1
            if last < first or step < 1:
                raise ValueError(error)
            expanded_count += (last - first) // step + 1
            if expanded_count > cls.MAX_EXPANDED_TASKS:
                raise ValueError(error)
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
    def identity_fields(job_id: str):
        match = re.fullmatch(r"([0-9]+)([_+])([0-9]+)", job_id)
        if match is None:
            return {}
        if match.group(2) == "_":
            return {"slurm_array_job_id": match.group(1), "slurm_array_task_id": match.group(3)}
        return {"slurm_het_job_id": match.group(1), "slurm_het_job_offset": match.group(3)}

    def extract_squeue(self, orig_file):
        """
        Parse output from:
        squeue -r -h -O 'JobID:0|,UserName:0|,StateCompact:0|,Partition:0|,NumCPUs:0|,NodeList:0|,tres-alloc:0|,tres-per-node:0|,tres-per-task:0|,ArrayJobID:0|,ArrayTaskID:0'
        """
        try:
            fileutils.check_empty_file(orig_file)
        except fileutils.FileEmptyError:
            logging.error("File %s seems to be empty." % orig_file)
            return []

        jobs = OrderedDict()
        expanded_count = 0
        with open(orig_file, "r") as fin:
            for line in fin:
                line = line.strip()
                if not line:
                    continue
                try:
                    fields = [field.strip() for field in line.split("|")]
                    if len(fields) not in (6, 9, 11):
                        raise ValueError("expected 6 legacy, 9 resource or 11 identity fields")
                    job_id, user, state, partition, cpus, nodes = fields[:6]
                    if not all((job_id, user, state)) or not all(name.strip().rstrip("*") for name in partition.split(",")):
                        raise ValueError("missing required job field")
                except ValueError:
                    logging.warning("Line: %s not properly parsed as Slurm squeue output." % line)
                    continue

                task_ids = self.task_ids(job_id, fields[9], fields[10]) if len(fields) == 11 else self.expand_job_id(job_id)
                if "_[" in job_id or len(task_ids) > 1:
                    expanded_count += len(task_ids)
                    if expanded_count > self.MAX_EXPANDED_TASKS:
                        raise ValueError("Too many grouped Slurm array tasks; collect squeue with -r/--array.")
                partitions = list(OrderedDict.fromkeys(self.anonymize(name.strip().rstrip("*"), "qs") for name in partition.split(",") if name.strip().rstrip("*")))
                for task_id in task_ids:
                    job = {
                        "JobId": task_id,
                        "UnixAccount": self.anonymize(user, "users"),
                        "S": self._map_job_state(state),
                        "raw_state": state,
                        "Queue": partitions[0],
                        "slurm_partitions": partitions,
                        "accounting_state": "running" if self._map_job_state(state) == "R" else "queued" if self._map_job_state(state) in SLURM_QUEUED_STATES else "other",
                        "CPUs": max(1, self._safe_int(cpus, default=1)),
                        "Nodes": nodes,
                    }
                    job.update(self.identity_fields(task_id))
                    if len(fields) >= 9:
                        job.update(zip(("slurm_tres_alloc", "slurm_tres_per_node", "slurm_tres_per_task"), fields[6:9]))
                    if task_id in jobs and jobs[task_id] != job:
                        previous = jobs[task_id]
                        ignored = {"Queue", "slurm_partitions"}
                        comparable = {key: value for key, value in job.items() if key not in ignored}
                        if job["S"] in SLURM_QUEUED_STATES and {key: value for key, value in previous.items() if key not in ignored} == comparable:
                            previous["slurm_partitions"] = list(OrderedDict.fromkeys(previous["slurm_partitions"] + partitions))
                            continue
                        raise ValueError("Conflicting Slurm records for job %s; collect a fresh squeue trace with -r/--array." % task_id)
                    jobs[task_id] = job
        return list(jobs.values())

    def extract_sinfo(self, orig_file):
        """
        Parse output from:
        sinfo -N -h -O 'NodeList:0|,Partition:0|,StateCompact:0|,CPUs:0|,Gres:0|,GresUsed:0|,Available:0'
        """
        try:
            fileutils.check_empty_file(orig_file)
        except fileutils.FileEmptyError:
            logging.error("File %s seems to be empty." % orig_file)
            return []

        nodes = []
        with open(orig_file, "r") as fin:
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
                    logging.warning("Line: %s not properly parsed as Slurm sinfo output." % line)
                    continue

                nodes.append(
                    {
                        "domainname": self.anonymize(node_name, "wns"),
                        "raw_domainname": node_name,
                        "qname": self.anonymize(partition.rstrip("*"), "qs"),
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
    def _safe_int(value, default):
        try:
            return int(value)
        except (TypeError, ValueError):
            return default

    @staticmethod
    def _map_job_state(state):
        normalized = state.upper().strip()
        if normalized in SLURM_COMPACT_JOB_STATES:
            return normalized
        return SLURM_JOB_STATES.get(normalized, normalized or "?")

    @staticmethod
    def _map_node_state(state):
        normalized = state.lower().strip()
        normalized = re.sub(r"[*+#~$@%!^\-]+$", "", normalized)
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


class SlurmBatchSystem(GenericBatchSystem):
    # Bound both numeric ranges and Cartesian products from untrusted traces.
    MAX_EXPANDED_NODES = 100000
    ACTIVE_STATES = set(["R", "CG", "CF", "S", "ST", "RS", "SI", "SO"])
    QUEUED_STATES = SLURM_QUEUED_STATES
    STATE_PRIORITY = {"d": 5, "b": 4, "c": 2, "r": 1, "-": 0, "?": -1}
    PARTITION_STATE_PRIORITY = {"?": -1, "up": 0, "drain": 1, "down": 2, "inact": 3, "inactive": 3}

    @staticmethod
    def get_mnemonic():
        return "slurm"

    def __init__(self, scheduler_output_filenames, config, options):
        self.squeue_file = scheduler_output_filenames.get("squeue_file")
        self.sinfo_file = scheduler_output_filenames.get("sinfo_file")
        self.scontrol_file = scheduler_output_filenames.get("scontrol_file")
        self.config = config
        self.options = options
        self.slurm_stat_maker = SlurmStatExtractor(self.config, self.options)

    def get_cluster_state(self, options):
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
        return state

    def get_jobs_info(self):
        jobs = self._get_jobs()
        job_ids = [job["JobId"] for job in jobs]
        usernames = [job["UnixAccount"] for job in jobs]
        job_states = [job["S"] for job in jobs]
        queue_names = [job["Queue"] for job in jobs]

        logging.debug(
            "job_ids, usernames, job_states, queue_names lengths: "
            "%(job_ids)s, %(usernames)s, %(job_states)s, %(queue_names)s"
            % {"job_ids": len(job_ids), "usernames": len(usernames), "job_states": len(job_states), "queue_names": len(queue_names)}
        )
        return job_ids, usernames, job_states, queue_names

    def get_queues_info(self):
        queue_counts = OrderedDict()
        total_running_jobs = 0
        total_queued_jobs = 0

        for node in self._get_nodes():
            queue = queue_counts.setdefault(node["qname"], {"run": 0, "queued": 0, "lm": "--", "state": "?"})
            state = node.get("partition_state", "?")
            if queue["state"] == "?" or self.PARTITION_STATE_PRIORITY.get(state, -1) > self.PARTITION_STATE_PRIORITY.get(queue["state"], -1):
                queue["state"] = state

        for job in self._get_jobs():
            if job["S"] == "R":
                total_running_jobs += 1
            elif job["S"] in self.QUEUED_STATES:
                total_queued_jobs += 1
            for name in job["slurm_partitions"]:
                queue = queue_counts.setdefault(name, {"run": 0, "queued": 0, "lm": "--", "state": "?"})
                if job["S"] == "R":
                    queue["run"] += 1
                elif job["S"] in self.QUEUED_STATES:
                    queue["queued"] += 1

        qstatq_lod = [
            {
                "queue_name": queue_name,
                "run": str(values["run"]),
                "queued": str(values["queued"]),
                "lm": values["lm"],
                "state": values["state"],
            }
            for queue_name, values in queue_counts.items()
        ]

        return total_running_jobs, total_queued_jobs, qstatq_lod

    def get_worker_nodes(self, job_ids, job_queues, options):
        worker_nodes_by_name = OrderedDict()

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

        node_jobs = self._map_jobs_to_nodes(self._get_jobs(), worker_nodes_by_name)
        for raw_name, worker_node in worker_nodes_by_name.items():
            worker_node["core_job_map"] = dict((idx, job_id) for idx, job_id in enumerate(node_jobs.get(raw_name, [])))

        self._apply_cpu_allocations(worker_nodes_by_name)
        self._apply_gpu_allocations(worker_nodes_by_name)
        for node in worker_nodes_by_name.values():
            node.pop("slurm_unmapped_cpus", None)
        mapped = {}
        for node in worker_nodes_by_name.values():
            for job_id in node["core_job_map"].values():
                mapped[job_id] = mapped.get(job_id, 0) + 1
        for job in self._get_jobs():
            hosts = [host for host in self.expand_nodelist(job["Nodes"]) if host in worker_nodes_by_name]
            missing = job["CPUs"] - mapped.get(job["JobId"], 0)
            if job["S"] in self.ACTIVE_STATES and hosts and missing > 0:
                node = worker_nodes_by_name[hosts[0]]
                node["slurm_unmapped_cpus"] = node.get("slurm_unmapped_cpus", 0) + missing
        worker_nodes = list(worker_nodes_by_name.values())
        logging.info("worker_nodes contains %s entries" % len(worker_nodes))
        return worker_nodes

    @staticmethod
    def _allocation_job_id(header: str) -> Optional[str]:
        # Job names are arbitrary text and must not be interpreted as identity fields.
        header = re.split(r"\s+JobName=", header, maxsplit=1)[0]
        pairs = re.findall(r"(?:^|\s)(JobId|ArrayJobId|ArrayTaskId|HetJobId|HetJobOffset)=(\S+)", header)
        fields = dict(pairs)
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

    def _apply_gpu_allocations(self, worker_nodes):
        for node in worker_nodes.values():
            for raw, parsed in (("slurm_gres", "slurm_gpu_configured"), ("slurm_gres_used", "slurm_gpu_used")):
                if raw in node:
                    try:
                        node[parsed] = parse_gpu_gres(node[raw])
                    except ValueError as error:
                        logging.warning("Ignoring invalid Slurm GPU metadata: %s", error)
        if not self.scontrol_file:
            return
        allocations = {}
        invalid = set()
        try:
            with open(self.scontrol_file, "r") as stream:
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
                        resources = parse_gpu_gres(fields["GRES"])
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
            logging.warning("Cannot read optional Slurm GPU details: %s", error)
            return
        for job in self._get_jobs():
            job_id = job["JobId"]
            if job["S"] not in self.ACTIVE_STATES or job_id in invalid:
                continue
            hosts = set(self.expand_nodelist(job["Nodes"]))
            allocation = allocations.get(job_id, {})
            if not set(allocation).issubset(hosts):
                continue
            for host, resources in allocation.items():
                if host in worker_nodes:
                    bindings = worker_nodes[host].setdefault("slurm_gpu_bindings", [])
                    bindings.extend(dict(resource, job_id=job_id) for resource in resources)

    def _read_cpu_allocations(self):
        allocations = {}
        if not self.scontrol_file:
            return allocations
        try:
            with open(self.scontrol_file, "r") as stream:
                job_id = None
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
                        cpus = self.expand_nodelist("[" + detail["CPU_IDs"] + "]")
                        if not hosts or not cpus or len(hosts) * len(cpus) > self.MAX_EXPANDED_NODES or len(set(map(int, cpus))) != len(cpus):
                            allocations[job_id] = None
                        else:
                            allocation = allocations[job_id]
                            if allocation is not None:
                                for host in hosts:
                                    if host in allocation:
                                        allocations[job_id] = None
                                        break
                                    allocation[host] = [int(cpu) for cpu in cpus]
        except (OSError, UnicodeError, ValueError) as error:
            logging.warning("Cannot read optional Slurm CPU details: %s; using estimates.", error)
            return {}
        return allocations

    def _apply_cpu_allocations(self, worker_nodes):
        details = self._read_cpu_allocations()
        exact = {name: {} for name in worker_nodes}
        estimated = set()
        for job in self._get_jobs():
            if job["S"] not in self.ACTIVE_STATES:
                continue
            hosts = self.expand_nodelist(job["Nodes"])
            allocation = details.get(job["JobId"])
            valid = allocation and set(allocation) == set(hosts) and sum(len(cpus) for cpus in allocation.values()) == job["CPUs"]
            if valid:
                valid = all(host in worker_nodes and all(cpu < int(worker_nodes[host]["np"]) for cpu in allocation[host]) for host in hosts)
            if not valid:
                estimated.update(hosts)
                if job["JobId"] in details:
                    logging.warning("Incomplete or inconsistent Slurm CPU details for job %s; using estimates.", job["JobId"])
                continue
            for host, cpus in allocation.items():
                for cpu in cpus:
                    if cpu in exact[host] and exact[host][cpu] != job["JobId"]:
                        estimated.add(host)
                        logging.warning("Overlapping Slurm CPU allocations on %s; using estimates.", host)
                    exact[host][cpu] = job["JobId"]
        for host, node in worker_nodes.items():
            if exact[host] and host not in estimated:
                node["core_job_map"] = exact[host]
                node["cpu_allocation_source"] = "slurm"
            else:
                node["cpu_allocation_source"] = "estimated" if host in estimated else "none"

    def _get_jobs(self):
        if not hasattr(self, "_jobs"):
            self._jobs = self.slurm_stat_maker.extract_squeue(self.squeue_file)
        return self._jobs

    def _get_nodes(self):
        if not hasattr(self, "_nodes"):
            self._nodes = self.slurm_stat_maker.extract_sinfo(self.sinfo_file)
        return self._nodes

    @classmethod
    def _merge_node_state(cls, previous, current):
        previous_priority = cls.STATE_PRIORITY.get(previous, 0)
        current_priority = cls.STATE_PRIORITY.get(current, 0)
        return current if current_priority > previous_priority else previous

    @classmethod
    def _map_jobs_to_nodes(cls, jobs, worker_nodes_by_name):
        node_jobs = OrderedDict((node_name, []) for node_name in worker_nodes_by_name)
        for node in worker_nodes_by_name.values():
            node["slurm_job_ids"] = []
        for job in jobs:
            if job["S"] not in cls.ACTIVE_STATES:
                continue
            nodes = [node for node in cls.expand_nodelist(job["Nodes"]) if node in worker_nodes_by_name]
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

    @classmethod
    def expand_nodelist(cls, nodelist: Optional[str]) -> List[str]:
        """Expand comma-separated host groups; reject malformed lists as a unit.

        Commas inside brackets separate numeric choices. Unbalanced brackets,
        empty groups, descending ranges and oversized expansions are unusable;
        return no hosts rather than assigning a job to an invented hostname.
        """
        nodelist = nodelist.strip() if nodelist else ""
        if not nodelist or nodelist.startswith("("):
            return []

        try:
            groups = []
            depth = 0
            start = 0
            for index, char in enumerate(nodelist):
                if char == "[":
                    if depth:
                        raise ValueError("nested brackets")
                    depth = 1
                elif char == "]":
                    if not depth:
                        raise ValueError("unmatched closing bracket")
                    depth = 0
                elif char == "," and not depth:
                    groups.append(nodelist[start:index])
                    start = index + 1
            if depth:
                raise ValueError("unmatched opening bracket")
            groups.append(nodelist[start:])

            expanded = []
            for group in groups:
                if not group or re.search(r"\s", group):
                    raise ValueError("empty or whitespace-containing host group")
                pending = [group]
                while any("[" in item for item in pending):
                    next_pending = []
                    for item in pending:
                        next_pending.extend(cls._expand_first_range(item))
                        if len(expanded) + len(next_pending) > cls.MAX_EXPANDED_NODES:
                            raise ValueError("too many expanded hosts")
                    pending = next_pending
                expanded.extend(pending)
                if len(expanded) > cls.MAX_EXPANDED_NODES:
                    raise ValueError("too many expanded hosts")
            return list(OrderedDict.fromkeys(expanded))
        except ValueError as error:
            logging.warning("Ignoring invalid Slurm nodelist %r: %s", nodelist, error)
            return []

    @classmethod
    def _expand_first_range(cls, value: str) -> List[str]:
        match = re.search(r"\[([^\]]+)\]", value)
        if not match:
            if "[" in value:
                raise ValueError("empty host range")
            return [value]

        prefix = value[: match.start()]
        suffix = value[match.end() :]
        choices = []
        for token in match.group(1).split(","):
            if not re.fullmatch(r"[0-9]+(?:-[0-9]+)?", token):
                raise ValueError("invalid numeric host range")
            if "-" not in token:
                choices.append(token)
                continue
            start, end = token.split("-", 1)
            first, last = int(start), int(end)
            if last < first:
                raise ValueError("descending host range")
            if len(choices) + last - first + 1 > cls.MAX_EXPANDED_NODES:
                raise ValueError("too many expanded hosts")
            width = len(start)
            choices.extend(str(number).zfill(width) for number in range(first, last + 1))
        return [prefix + choice + suffix for choice in choices]
