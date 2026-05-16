##
## qtop is a tool to monitor queuing systems - https://github.com/qtop/qtop
##
## Copyright (c) 2016 Fotis Georgatos
## Copyright (c) 2016 Sotiris Fragkiskos
## Copyright (c) 2026 qtop contributors
##
## SPDX-License-Identifier: MIT
##

import logging
import math
import re
from collections import OrderedDict

import qtop_py.fileutils as fileutils
from qtop_py.serialiser import GenericBatchSystem, StatExtractor

JOB_STATE_ALIASES = {
    "RUNNING": "R",
    "R": "R",
    "PENDING": "PD",
    "PD": "PD",
    "COMPLETING": "CG",
    "CG": "CG",
    "COMPLETED": "CD",
    "CD": "CD",
    "CONFIGURING": "CF",
    "CF": "CF",
    "CANCELLED": "CA",
    "CA": "CA",
    "FAILED": "F",
    "F": "F",
    "NODE_FAIL": "NF",
    "NF": "NF",
    "TIMEOUT": "TO",
    "TO": "TO",
    "PREEMPTED": "PR",
    "PR": "PR",
    "SUSPENDED": "S",
    "S": "S",
    "STOPPED": "ST",
    "ST": "ST",
    "BOOT_FAIL": "BF",
    "BF": "BF",
    "SPECIAL_EXIT": "SE",
    "SE": "SE",
    "STAGE_OUT": "SO",
    "SO": "SO",
    "SIGNALING": "SI",
    "SI": "SI",
    "REVOKED": "RV",
    "RV": "RV",
    "REQUEUE_HOLD": "RH",
    "RH": "RH",
}

RUNNING_LIKE_JOB_STATES = set(["R", "CG", "CF"])
QUEUED_JOB_STATES = set(["PD", "RH"])
ALLOCATED_JOB_STATES = RUNNING_LIKE_JOB_STATES.union(set(["S", "ST"]))

NODE_STATE_PRIORITY = {
    "?": 0,
    "-": 1,
    "r": 2,
    "c": 3,
    "%": 4,
    "a": 5,
    "d": 6,
}


def normalize_job_state(state):
    state = state.strip().upper()
    return JOB_STATE_ALIASES.get(state, state)


def normalize_node_state(state):
    state = state.lower().strip("*+#~!$")
    state = state.split("+", 1)[0]
    if state in ("idle", "planned", "plnd"):
        return "-"
    if state in ("alloc", "allocated"):
        return "a"
    if state in ("mix", "mixed"):
        return "%"
    if state in ("down", "drain", "drng", "fail", "failing", "maint", "maintenance", "unk", "unknown"):
        return "d"
    if state in ("comp", "completing"):
        return "c"
    if state in ("resv", "reserved"):
        return "r"
    return state[:1] or "?"


def split_top_level_commas(value):
    parts = []
    current = []
    depth = 0
    for char in value:
        if char == "[":
            depth += 1
        elif char == "]" and depth:
            depth -= 1

        if char == "," and depth == 0:
            parts.append("".join(current))
            current = []
            continue
        current.append(char)

    if current:
        parts.append("".join(current))
    return parts


def expand_slurm_nodelist(nodelist):
    if not nodelist or nodelist.startswith("("):
        return []

    nodes = []
    for item in split_top_level_commas(nodelist):
        nodes.extend(_expand_one_nodelist_item(item))
    return nodes


def _expand_one_nodelist_item(value):
    match = re.search(r"\[([^\]]+)\]", value)
    if not match:
        return [value]

    prefix = value[: match.start()]
    suffix = value[match.end() :]
    expanded = []
    for choice in _expand_bracket_choices(match.group(1)):
        expanded.extend(_expand_one_nodelist_item(prefix + choice + suffix))
    return expanded


def _expand_bracket_choices(value):
    choices = []
    for part in value.split(","):
        if "-" not in part:
            choices.append(part)
            continue

        start, end = part.split("-", 1)
        if not (start.isdigit() and end.isdigit()):
            choices.append(part)
            continue

        width = len(start)
        choices.extend(str(number).zfill(width) for number in range(int(start), int(end) + 1))
    return choices


def safe_int(value, fallback=0):
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return fallback


class SlurmStatExtractor(StatExtractor):
    def extract_squeue(self, orig_file):
        """
        Parse output from:
        squeue -h -o %i|%P|%u|%T|%C|%R
        """
        try:
            fileutils.check_empty_file(orig_file)
        except fileutils.FileEmptyError:
            logging.error("File %s seems to be empty." % orig_file)
            return []

        jobs = []
        with open(orig_file, "r") as fin:
            for line in fin:
                job = self._parse_squeue_line(line)
                if job:
                    jobs.append(job)
        return jobs

    @staticmethod
    def _parse_squeue_line(line):
        line = line.strip()
        if not line or line.startswith("#"):
            return None

        if "|" in line:
            parts = [part.strip() for part in line.split("|", 5)]
        else:
            parts = line.split(None, 5)

        if len(parts) != 6 or parts[0].upper() == "JOBID":
            logging.warning("Line: %s not properly parsed as Slurm squeue output." % line)
            return None

        job_id, partition, user, state, cpus, nodes = parts
        return {
            "JobId": job_id.split(".")[0],
            "Queue": partition.rstrip("*"),
            "UnixAccount": user,
            "S": normalize_job_state(state),
            "CPUs": safe_int(cpus, 1),
            "Nodes": nodes,
        }

    def extract_sinfo(self, orig_file):
        """
        Parse output from:
        sinfo -N -h -o %N|%P|%t|%c
        """
        try:
            fileutils.check_empty_file(orig_file)
        except fileutils.FileEmptyError:
            logging.error("File %s seems to be empty." % orig_file)
            return []

        nodes = []
        with open(orig_file, "r") as fin:
            for line in fin:
                node_info = self._parse_sinfo_line(line)
                if not node_info:
                    continue
                for node_name in expand_slurm_nodelist(node_info["NodeName"]):
                    node = node_info.copy()
                    node["NodeName"] = node_name
                    nodes.append(node)
        return nodes

    @staticmethod
    def _parse_sinfo_line(line):
        line = line.strip()
        if not line or line.startswith("#"):
            return None

        if "|" in line:
            parts = [part.strip() for part in line.split("|", 3)]
        else:
            parts = line.split(None, 3)

        if len(parts) != 4 or parts[0].upper() in ("NODELIST", "NODE"):
            logging.warning("Line: %s not properly parsed as Slurm sinfo output." % line)
            return None

        node_name, partition, state, cpus = parts
        return {
            "NodeName": node_name,
            "Queue": partition.rstrip("*"),
            "state": normalize_node_state(state),
            "np": safe_int(cpus),
        }


class SlurmBatchSystem(GenericBatchSystem):
    @staticmethod
    def get_mnemonic():
        return "slurm"

    def __init__(self, scheduler_output_filenames, config, options):
        self.squeue_file = scheduler_output_filenames.get("squeue_file")
        self.sinfo_file = scheduler_output_filenames.get("sinfo_file")
        self.config = config
        self.options = options
        self.slurm_stat_maker = SlurmStatExtractor(self.config, self.options)
        self.anonymize = self.slurm_stat_maker.anonymize

    def get_jobs_info(self):
        job_ids, usernames, job_states, queue_names = [], [], [], []
        for job in self._get_jobs():
            job_ids.append(job["JobId"])
            usernames.append(self.anonymize(job["UnixAccount"], "users"))
            job_states.append(job["S"])
            queue_names.append(self.anonymize(job["Queue"], "qs"))

        logging.debug(
            "job_ids, usernames, job_states, queue_names lengths: "
            "%(job_ids)s, %(usernames)s, %(job_states)s, %(queue_names)s"
            % {"job_ids": len(job_ids), "usernames": len(usernames), "job_states": len(job_states), "queue_names": len(queue_names)}
        )
        return job_ids, usernames, job_states, queue_names

    def get_queues_info(self):
        queue_counts = OrderedDict()

        for node in self._get_nodes():
            queue_counts.setdefault(node["Queue"], {"run": 0, "queued": 0, "lm": "--", "state": "E"})

        for job in self._get_jobs():
            queue_counts.setdefault(job["Queue"], {"run": 0, "queued": 0, "lm": "--", "state": "E"})
            if job["S"] in RUNNING_LIKE_JOB_STATES:
                queue_counts[job["Queue"]]["run"] += 1
            elif job["S"] in QUEUED_JOB_STATES:
                queue_counts[job["Queue"]]["queued"] += 1

        qstatq_lod = []
        total_running_jobs = 0
        total_queued_jobs = 0
        for queue_name, values in queue_counts.items():
            total_running_jobs += values["run"]
            total_queued_jobs += values["queued"]
            qstatq_lod.append(
                {
                    "queue_name": self.anonymize(queue_name, "qs"),
                    "run": str(values["run"]),
                    "queued": str(values["queued"]),
                    "lm": values["lm"],
                    "state": values["state"],
                }
            )

        return total_running_jobs, total_queued_jobs, qstatq_lod

    def get_worker_nodes(self, job_ids, job_queues, options):
        worker_nodes_by_name = OrderedDict()

        for node in self._get_nodes():
            worker_node = worker_nodes_by_name.setdefault(
                node["NodeName"],
                {
                    "domainname": self.anonymize(node["NodeName"], "wns"),
                    "np": str(node["np"]),
                    "state": node["state"],
                    "qname": set(),
                    "core_job_map": {},
                },
            )
            worker_node["np"] = str(max(safe_int(worker_node["np"]), node["np"]))
            worker_node["state"] = self._prefer_node_state(worker_node["state"], node["state"])
            worker_node["qname"].add(self.anonymize(node["Queue"], "qs"))

        for job in self._get_jobs():
            if job["S"] not in ALLOCATED_JOB_STATES:
                continue
            nodes = expand_slurm_nodelist(job["Nodes"])
            if not nodes:
                continue

            cores_per_node = self._cores_per_node(job["CPUs"], len(nodes))
            queue_name = self.anonymize(job["Queue"], "qs")
            for node_name in nodes:
                worker_node = worker_nodes_by_name.setdefault(
                    node_name,
                    {
                        "domainname": self.anonymize(node_name, "wns"),
                        "np": str(cores_per_node),
                        "state": "a",
                        "qname": set(),
                        "core_job_map": {},
                    },
                )
                worker_node["qname"].add(queue_name)
                self._add_job_to_worker_node(worker_node, job["JobId"], cores_per_node)

        worker_nodes = []
        for worker_node in worker_nodes_by_name.values():
            worker_node["qname"] = sorted(worker_node["qname"])
            worker_nodes.append(worker_node)

        logging.info("worker_nodes contains %s entries" % len(worker_nodes))
        return worker_nodes

    def _get_jobs(self):
        if not hasattr(self, "_jobs"):
            self._jobs = self.slurm_stat_maker.extract_squeue(self.squeue_file)
        return self._jobs

    def _get_nodes(self):
        if not hasattr(self, "_nodes"):
            self._nodes = self.slurm_stat_maker.extract_sinfo(self.sinfo_file)
        return self._nodes

    @staticmethod
    def _cores_per_node(cpus, node_count):
        if not node_count:
            return 0
        return max(1, int(math.ceil(float(cpus or 1) / node_count)))

    @staticmethod
    def _add_job_to_worker_node(worker_node, job_id, cores_per_node):
        core_job_map = worker_node["core_job_map"]
        start_core = len(core_job_map)
        requested_end = start_core + cores_per_node
        worker_node["np"] = str(max(safe_int(worker_node["np"]), requested_end))
        for core in range(start_core, requested_end):
            core_job_map[core] = job_id

    @staticmethod
    def _prefer_node_state(current_state, candidate_state):
        current_priority = NODE_STATE_PRIORITY.get(current_state, 0)
        candidate_priority = NODE_STATE_PRIORITY.get(candidate_state, 0)
        return candidate_state if candidate_priority > current_priority else current_state

    @staticmethod
    def _expand_nodelist(nodelist):
        return expand_slurm_nodelist(nodelist)
