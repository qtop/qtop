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

from qtop_py.serialiser import GenericBatchSystem, StatExtractor


class SlurmStatExtractor(StatExtractor):
    def extract_squeue(self, orig_file):
        """
        Reads squeue output emitted by:
        squeue -h -o "%i|%u|%T|%P|%C|%N"
        """
        all_squeue_values = []
        with open(orig_file, "r") as fin:
            for line in fin:
                line = line.strip()
                if not line:
                    continue
                fields = line.split("|", 5)
                if len(fields) != 6:
                    logging.warning("Skipping malformed squeue line: %s" % line)
                    continue
                job_id, user, state, partition, cpus, nodelist = fields
                all_squeue_values.append(
                    {
                        "JobId": re.sub(r"_[0-9]+$", "", job_id),
                        "UnixAccount": self.anonymize(user, "users"),
                        "S": state,
                        "Queue": partition,
                        "Cpus": int(cpus or 0),
                        "NodeList": nodelist,
                    }
                )
        return all_squeue_values


class SlurmBatchSystem(GenericBatchSystem):
    @staticmethod
    def get_mnemonic():
        return "slurm"

    def __init__(self, scheduler_output_filenames, config, options):
        self.scontrol_nodes_file = scheduler_output_filenames.get("scontrol_nodes_file")
        self.squeue_file = scheduler_output_filenames.get("squeue_file")

        self.config = config
        self.options = options
        self.slurm_stat_maker = SlurmStatExtractor(self.config, self.options)

    def get_worker_nodes(self, job_ids, job_queues, options):
        jobs = self.slurm_stat_maker.extract_squeue(self.squeue_file)
        jobs_by_node = self._jobs_by_node(jobs)
        nodes = self._read_scontrol_nodes(self.scontrol_nodes_file)

        worker_nodes = []
        for node in nodes:
            node_name = node["NodeName"]
            worker_node = {
                "domainname": node_name,
                "state": self._qtop_state(node.get("State", "")),
                "np": node.get("CPUTot", 0),
                "core_job_map": self._core_job_map(jobs_by_node.get(node_name, []), node.get("CPUTot", 0)),
            }
            worker_nodes.append(worker_node)

        return self.ensure_worker_nodes_have_qnames(worker_nodes, job_ids, job_queues)

    def get_jobs_info(self):
        job_ids, usernames, job_states, queue_names = [], [], [], []
        for job in self.slurm_stat_maker.extract_squeue(self.squeue_file):
            job_ids.append(str(job["JobId"]))
            usernames.append(job["UnixAccount"])
            job_states.append(self._slurm_job_state(job["S"]))
            queue_names.append(job["Queue"])

        return job_ids, usernames, job_states, queue_names

    def get_queues_info(self):
        jobs = self.slurm_stat_maker.extract_squeue(self.squeue_file)
        qstatq = {}
        for job in jobs:
            queue_name = job["Queue"]
            qstatq.setdefault(queue_name, {"queue_name": queue_name, "run": 0, "queued": 0, "lm": "--", "state": "E"})
            state = self._slurm_job_state(job["S"])
            if state == "R":
                qstatq[queue_name]["run"] += 1
            elif state == "Q":
                qstatq[queue_name]["queued"] += 1

        total_running_jobs = sum(queue["run"] for queue in qstatq.values())
        total_queued_jobs = sum(queue["queued"] for queue in qstatq.values())
        return total_running_jobs, total_queued_jobs, list(qstatq.values())

    @staticmethod
    def _read_scontrol_nodes(orig_file):
        nodes = []
        with open(orig_file, "r") as fin:
            for line in fin:
                values = SlurmBatchSystem._parse_key_values(line)
                if values:
                    values["CPUTot"] = int(values.get("CPUTot", values.get("CPUs", 0)) or 0)
                    nodes.append(values)
        return nodes

    @staticmethod
    def _parse_key_values(line):
        values = {}
        for key, value in re.findall(r"(\w+)=([^\s]+)", line):
            values[key] = value
        return values

    @staticmethod
    def _jobs_by_node(jobs):
        jobs_by_node = {}
        for job in jobs:
            if job["S"] in ("PENDING", "PD"):
                continue
            for node in SlurmBatchSystem._expand_nodelist(job["NodeList"]):
                jobs_by_node.setdefault(node, []).append(job)
        return jobs_by_node

    @staticmethod
    def _core_job_map(jobs, total_cores):
        core_job_map = {}
        next_core = 0
        for job in jobs:
            for _ in range(max(job["Cpus"], 1)):
                if next_core >= total_cores:
                    return core_job_map
                core_job_map[str(next_core)] = job["JobId"]
                next_core += 1
        return core_job_map

    @staticmethod
    def _expand_nodelist(nodelist):
        if not nodelist or nodelist == "(None assigned)":
            return []

        result = []
        for part in SlurmBatchSystem._split_nodelist(nodelist):
            match = re.match(r"^(?P<prefix>[^\[]+)\[(?P<ranges>[^\]]+)\]$", part)
            if not match:
                result.append(part)
                continue
            prefix = match.group("prefix")
            for item in match.group("ranges").split(","):
                if "-" not in item:
                    result.append(prefix + item)
                    continue
                start, end = item.split("-", 1)
                width = len(start)
                for node_number in range(int(start), int(end) + 1):
                    result.append(prefix + str(node_number).zfill(width))
        return result

    @staticmethod
    def _split_nodelist(nodelist):
        parts = []
        start = 0
        depth = 0
        for idx, char in enumerate(nodelist):
            if char == "[":
                depth += 1
            elif char == "]":
                depth -= 1
            elif char == "," and depth == 0:
                parts.append(nodelist[start:idx])
                start = idx + 1
        parts.append(nodelist[start:])
        return parts

    @staticmethod
    def _qtop_state(slurm_state):
        state = slurm_state.split("+")[0].split("*")[0].lower()
        if state in ("idle", "alloc", "allocated", "mix", "mixed", "comp", "completing"):
            return "-"
        if state in ("down", "drain", "drained", "fail", "failing", "maint"):
            return "d"
        return state[:1] or "-"

    @staticmethod
    def _slurm_job_state(slurm_state):
        return {
            "RUNNING": "R",
            "R": "R",
            "PENDING": "Q",
            "PD": "Q",
            "COMPLETED": "C",
            "CD": "C",
            "COMPLETING": "R",
            "CG": "R",
            "CANCELLED": "C",
            "CA": "C",
            "FAILED": "E",
            "F": "E",
            "TIMEOUT": "E",
            "TO": "E",
            "SUSPENDED": "S",
            "S": "S",
        }.get(slurm_state, slurm_state[:1] or "?")
