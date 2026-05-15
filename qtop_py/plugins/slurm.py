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
import re
from collections import OrderedDict

from qtop_py.serialiser import GenericBatchSystem, StatExtractor
import qtop_py.fileutils as fileutils


class SlurmStatExtractor(StatExtractor):
    def extract_squeue(self, orig_file):
        """
        Parse output from:
        squeue -h -o %i|%u|%t|%P|%N
        """
        try:
            fileutils.check_empty_file(orig_file)
        except fileutils.FileEmptyError:
            logging.error("File %s seems to be empty." % orig_file)
            return []

        jobs = []
        with open(orig_file, "r") as fin:
            for line in fin:
                line = line.strip()
                if not line:
                    continue
                try:
                    job_id, user, state, partition, nodes = line.split("|", 4)
                except ValueError:
                    logging.warning("Line: %s not properly parsed as Slurm squeue output." % line)
                    continue

                jobs.append(
                    {
                        "JobId": job_id,
                        "UnixAccount": self.anonymize(user, "users"),
                        "S": state,
                        "Queue": self.anonymize(partition, "qs"),
                        "Nodes": nodes,
                    }
                )
        return jobs

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
                line = line.strip()
                if not line:
                    continue
                try:
                    node_name, partition, state, cpus = line.split("|", 3)
                except ValueError:
                    logging.warning("Line: %s not properly parsed as Slurm sinfo output." % line)
                    continue

                nodes.append(
                    {
                        "domainname": self.anonymize(node_name, "wns"),
                        "raw_domainname": node_name,
                        "qname": [self.anonymize(partition.rstrip("*"), "qs")],
                        "state": self._map_node_state(state),
                        "np": cpus,
                    }
                )
        return nodes

    @staticmethod
    def _map_node_state(state):
        state = state.lower().strip("*+#~")
        if state in ("idle", "planned"):
            return "-"
        if state in ("alloc", "allocated"):
            return "a"
        if state == "mix":
            return "%"
        if state in ("down", "drain", "drng", "fail", "failing", "maint"):
            return "d"
        if state in ("comp", "completing"):
            return "c"
        if state in ("resv", "reserved"):
            return "r"
        return state[:1] or "?"


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

    def get_jobs_info(self):
        job_ids, usernames, job_states, queue_names = [], [], [], []
        for job in self._get_jobs():
            job_ids.append(job["JobId"])
            usernames.append(job["UnixAccount"])
            job_states.append(job["S"])
            queue_names.append(job["Queue"])

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

        for node in self.slurm_stat_maker.extract_sinfo(self.sinfo_file):
            queue_counts.setdefault(node["qname"][0], {"run": 0, "queued": 0, "lm": "--", "state": "E"})

        for job in self._get_jobs():
            queue = job["Queue"]
            queue_counts.setdefault(queue, {"run": 0, "queued": 0, "lm": "--", "state": "E"})
            if job["S"] == "R":
                queue_counts[queue]["run"] += 1
                total_running_jobs += 1
            elif job["S"] == "PD":
                queue_counts[queue]["queued"] += 1
                total_queued_jobs += 1

        qstatq_lod = []
        for queue_name, values in queue_counts.items():
            qstatq_lod.append(
                {
                    "queue_name": queue_name,
                    "run": str(values["run"]),
                    "queued": str(values["queued"]),
                    "lm": values["lm"],
                    "state": values["state"],
                }
            )

        return total_running_jobs, total_queued_jobs, qstatq_lod

    def get_worker_nodes(self, job_ids, job_queues, options):
        jobs = self._get_jobs()
        node_jobs = self._map_jobs_to_nodes(jobs)
        worker_nodes_by_name = OrderedDict()

        for node in self.slurm_stat_maker.extract_sinfo(self.sinfo_file):
            raw_name = node.pop("raw_domainname")
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
            if node["qname"][0] not in worker_node["qname"]:
                worker_node["qname"].append(node["qname"][0])
            if worker_node["state"] == "-" and node["state"] != "-":
                worker_node["state"] = node["state"]

        for raw_name, worker_node in worker_nodes_by_name.items():
            worker_node["core_job_map"] = dict((idx, job_id) for idx, job_id in enumerate(node_jobs.get(raw_name, [])))
            worker_node["np"] = str(max(int(worker_node["np"]), len(worker_node["core_job_map"])))

        worker_nodes = list(worker_nodes_by_name.values())
        logging.info("worker_nodes contains %s entries" % len(worker_nodes))
        return worker_nodes

    def _get_jobs(self):
        if not hasattr(self, "_jobs"):
            self._jobs = self.slurm_stat_maker.extract_squeue(self.squeue_file)
        return self._jobs

    @classmethod
    def _map_jobs_to_nodes(cls, jobs):
        node_jobs = {}
        for job in jobs:
            if job["S"] != "R":
                continue
            for node in cls._expand_nodelist(job["Nodes"]):
                node_jobs.setdefault(node, []).append(job["JobId"])
        return node_jobs

    @classmethod
    def _expand_nodelist(cls, nodelist):
        if not nodelist or nodelist.startswith("("):
            return []
        expanded = [nodelist]
        while True:
            bracketed = None
            for item in expanded:
                if "[" in item:
                    bracketed = item
                    break
            if bracketed is None:
                return expanded

            expanded.remove(bracketed)
            expanded.extend(cls._expand_first_range(bracketed))

    @staticmethod
    def _expand_first_range(value):
        match = re.search(r"(\[[^\]]+\])", value)
        if not match:
            return [value]
        prefix = value[: match.start()]
        suffix = value[match.end() :]
        choices = []
        for part in match.group(1).strip("[]").split(","):
            if "-" not in part:
                choices.append(part)
                continue
            start, end = part.split("-", 1)
            width = len(start)
            choices.extend(str(i).zfill(width) for i in range(int(start), int(end) + 1))
        return [prefix + choice + suffix for choice in choices]
