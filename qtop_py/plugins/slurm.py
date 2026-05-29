##
## qtop is a tool to monitor queuing systems - https://github.com/qtop/qtop
##
## Copyright (c) 2016 Fotis Georgatos
## Copyright (c) 2016 Sotiris Fragkiskos
## Copyright (c) 2023 Hewlett Packard Enterprise Development LP
##
## SPDX-License-Identifier: MIT
##

import logging
import re

import qtop_py.fileutils as fileutils
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
    "RESIZING": "RS",
    "REQUEUED": "RQ",
    "REVOKED": "RV",
    "SIGNALING": "SI",
    "SPECIAL_EXIT": "SE",
    "STAGE_OUT": "SO",
    "STOPPED": "ST",
    "SUSPENDED": "S",
    "TIMEOUT": "TO",
}

SLURM_NODE_STATES = {
    "ALLOCATED": "a",
    "COMPLETING": "c",
    "DOWN": "d",
    "DRAIN": "D",
    "DRAINED": "D",
    "DRAINING": "D",
    "FAIL": "f",
    "FAILING": "f",
    "FUTURE": "F",
    "IDLE": "-",
    "MAINT": "M",
    "MIXED": "m",
    "POWER_DOWN": "p",
    "POWERED_DOWN": "p",
    "POWERING_DOWN": "p",
    "RESERVED": "r",
    "UNKNOWN": "?",
}


class SlurmStatExtractor(StatExtractor):
    def extract_squeue(self, orig_file):
        try:
            fileutils.check_empty_file(orig_file)
        except fileutils.FileEmptyError:
            logging.error("File %s seems to be empty." % orig_file)
            return []

        all_squeue_values = []
        with open(orig_file, "r") as fin:
            for line in fin:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                fields = line.split("|")
                if len(fields) < 6:
                    logging.warning("Line: %s not properly parsed as SLURM squeue output." % line)
                    continue
                job_id, user, state, queue, nodes, cpus = fields[:6]
                user = self.anonymize(user, "users")
                all_squeue_values.append(
                    {
                        "JobId": job_id,
                        "UnixAccount": user,
                        "S": compact_slurm_job_state(state),
                        "Queue": queue,
                        "Nodes": nodes,
                        "CPUs": int(cpus or 0),
                    }
                )
        return all_squeue_values

    def extract_sinfo(self, orig_file):
        try:
            fileutils.check_empty_file(orig_file)
        except fileutils.FileEmptyError:
            logging.error("File %s seems to be empty." % orig_file)
            return []

        all_sinfo_values = []
        with open(orig_file, "r") as fin:
            for line in fin:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                fields = line.split("|")
                if len(fields) < 4:
                    logging.warning("Line: %s not properly parsed as SLURM sinfo output." % line)
                    continue
                queue, state, nodes, cpus = fields[:4]
                all_sinfo_values.append({"queue_name": queue.rstrip("*"), "state": state, "nodes": int(nodes or 0), "cpus": cpus})
        return all_sinfo_values


class SlurmBatchSystem(GenericBatchSystem):
    @staticmethod
    def get_mnemonic():
        return "slurm"

    def __init__(self, scheduler_output_filenames, config, options):
        self.scontrol_file = scheduler_output_filenames.get("scontrol_file")
        self.squeue_file = scheduler_output_filenames.get("squeue_file")
        self.sinfo_file = scheduler_output_filenames.get("sinfo_file")

        self.config = config
        self.options = options
        self.stat_maker = SlurmStatExtractor(self.config, self.options)

    def get_worker_nodes(self, job_ids, job_queues, options):
        try:
            fileutils.check_empty_file(self.scontrol_file)
        except fileutils.FileEmptyError:
            return []

        worker_nodes = []
        anonymize = self.stat_maker.anonymize_func()
        for block in self._read_all_nodes(self.scontrol_file):
            domainname = block["NodeName"] if not self.options.ANONYMIZE else anonymize(block["NodeName"], "wns")
            worker_nodes.append(
                {
                    "domainname": domainname,
                    "state": compact_slurm_node_state(block.get("State", "UNKNOWN")),
                    "np": int(block.get("CPUTot", 0) or 0),
                    "core_job_map": {},
                }
            )

        worker_node_by_name = dict((worker_node["domainname"], worker_node) for worker_node in worker_nodes)
        self._assign_jobs_to_cores(worker_node_by_name)
        return self.ensure_worker_nodes_have_qnames(worker_nodes, job_ids, job_queues)

    def get_jobs_info(self):
        job_ids, usernames, job_states, queue_names = [], [], [], []

        for qstat in self.stat_maker.extract_squeue(self.squeue_file):
            job_ids.append(qstat["JobId"])
            usernames.append(qstat["UnixAccount"])
            job_states.append(qstat["S"])
            queue_names.append(qstat["Queue"])

        logging.debug(
            "job_ids, usernames, job_states, queue_names lengths: "
            "%(job_ids)s, %(usernames)s, %(job_states)s, %(queue_names)s"
            % {"job_ids": len(job_ids), "usernames": len(usernames), "job_states": len(job_states), "queue_names": len(queue_names)}
        )
        return job_ids, usernames, job_states, queue_names

    def get_queues_info(self):
        queue_totals = {}
        for queue in self.stat_maker.extract_sinfo(self.sinfo_file):
            queue_totals.setdefault(queue["queue_name"], {"queue_name": queue["queue_name"], "run": 0, "queued": 0, "lm": "--", "state": queue["state"]})

        for job in self.stat_maker.extract_squeue(self.squeue_file):
            queue = queue_totals.setdefault(job["Queue"], {"queue_name": job["Queue"], "run": 0, "queued": 0, "lm": "--", "state": "unknown"})
            if job["S"] == "R":
                queue["run"] += 1
            elif job["S"] == "PD":
                queue["queued"] += 1

        qstatq_list = []
        total_running_jobs, total_queued_jobs = 0, 0
        for queue_name in sorted(queue_totals):
            queue = queue_totals[queue_name]
            total_running_jobs += queue["run"]
            total_queued_jobs += queue["queued"]
            qstatq_list.append(
                {
                    "queue_name": queue["queue_name"],
                    "run": str(queue["run"]),
                    "queued": str(queue["queued"]),
                    "lm": queue["lm"],
                    "state": queue["state"],
                }
            )
        return total_running_jobs, total_queued_jobs, qstatq_list

    def _assign_jobs_to_cores(self, worker_node_by_name):
        for job in self.stat_maker.extract_squeue(self.squeue_file):
            if job["S"] != "R":
                continue
            node_names = expand_slurm_nodelist(job["Nodes"])
            if not node_names:
                continue
            cpus_left = job["CPUs"]
            while cpus_left > 0:
                assigned_this_pass = 0
                for node_name in node_names:
                    worker_node = worker_node_by_name.get(node_name)
                    if worker_node is None:
                        continue
                    assigned = assign_job_to_next_free_core(worker_node, job["JobId"])
                    if assigned:
                        cpus_left -= 1
                        assigned_this_pass += 1
                        if cpus_left == 0:
                            break
                if assigned_this_pass == 0:
                    logging.warning("Could not assign all requested CPUs for SLURM job %s." % job["JobId"])
                    break

    @staticmethod
    def _read_all_nodes(orig_file):
        nodes = []
        with open(orig_file, mode="r") as fin:
            for line in fin:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                node = parse_scontrol_line(line)
                if "NodeName" in node:
                    nodes.append(node)
        return nodes


def parse_scontrol_line(line):
    values = {}
    for field in line.split():
        if "=" not in field:
            continue
        key, value = field.split("=", 1)
        values[key] = value
    return values


def compact_slurm_job_state(state):
    return SLURM_JOB_STATES.get(state.upper(), state)


def compact_slurm_node_state(state):
    state = state.split("+", 1)[0].upper()
    return SLURM_NODE_STATES.get(state, state[:1].lower())


def assign_job_to_next_free_core(worker_node, job_id):
    for core in range(int(worker_node["np"])):
        core = str(core)
        if core not in worker_node["core_job_map"]:
            worker_node["core_job_map"][core] = job_id
            return True
    return False


def expand_slurm_nodelist(nodelist):
    if not nodelist or nodelist == "(null)":
        return []

    expanded = []
    for part in split_hostlist(nodelist):
        match = re.match(r"^(?P<prefix>[^\[]+)\[(?P<ranges>[^\]]+)\]$", part)
        if not match:
            expanded.append(part)
            continue
        prefix = match.group("prefix")
        for item in match.group("ranges").split(","):
            if "-" in item:
                start, end = item.split("-", 1)
                width = len(start)
                for number in range(int(start), int(end) + 1):
                    expanded.append(prefix + str(number).zfill(width))
            else:
                expanded.append(prefix + item)
    return expanded


def split_hostlist(hostlist):
    parts = []
    current = []
    bracket_depth = 0
    for char in hostlist:
        if char == "[":
            bracket_depth += 1
        elif char == "]":
            bracket_depth -= 1
        elif char == "," and bracket_depth == 0:
            parts.append("".join(current))
            current = []
            continue
        current.append(char)
    if current:
        parts.append("".join(current))
    return parts
