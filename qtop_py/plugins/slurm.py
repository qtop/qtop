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

import qtop_py.fileutils as fileutils
from qtop_py.serialiser import GenericBatchSystem, StatExtractor


class SlurmStatExtractor(StatExtractor):
    def extract_squeue(self, orig_file):
        """
        Parse output from:
        squeue -h -o %i|%u|%t|%P|%C|%N
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
                    job_id, user, state, partition, cpus, nodes = line.split("|", 5)
                except ValueError:
                    logging.warning("Line: %s not properly parsed as Slurm squeue output." % line)
                    continue

                jobs.append(
                    {
                        "JobId": job_id,
                        "UnixAccount": self.anonymize(user, "users"),
                        "S": state,
                        "Queue": self.anonymize(partition.rstrip("*"), "qs"),
                        "CPUs": self._safe_int(cpus, default=1),
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
                        "qname": self.anonymize(partition.rstrip("*"), "qs"),
                        "state": self._map_node_state(state),
                        "np": str(self._safe_int(cpus, default=0)),
                    }
                )
        return nodes

    @staticmethod
    def _safe_int(value, default):
        try:
            return int(value)
        except (TypeError, ValueError):
            return default

    @staticmethod
    def _map_node_state(state):
        normalized = state.lower().strip()
        normalized = re.sub(r"[*+#~$@%!]+$", "", normalized)
        state_map = {
            "idle": "-",
            "alloc": "b",
            "allocated": "b",
            "mix": "b",
            "mixed": "b",
            "comp": "c",
            "completing": "c",
            "down": "d",
            "drain": "d",
            "drained": "d",
            "drng": "d",
            "fail": "d",
            "failing": "d",
            "maint": "d",
            "resv": "r",
            "reserved": "r",
            "planned": "-",
            "plnd": "-",
            "future": "?",
        }
        return state_map.get(normalized, normalized[:1] or "?")


class SlurmBatchSystem(GenericBatchSystem):
    ACTIVE_STATES = set(["R", "CG", "CF", "S", "ST"])
    QUEUED_STATES = set(["PD", "CF"])
    STATE_PRIORITY = {"d": 5, "b": 4, "c": 2, "r": 1, "-": 0, "?": -1}

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

        for node in self._get_nodes():
            queue_counts.setdefault(node["qname"], {"run": 0, "queued": 0, "lm": "--", "state": "E"})

        for job in self._get_jobs():
            queue = job["Queue"]
            queue_counts.setdefault(queue, {"run": 0, "queued": 0, "lm": "--", "state": "E"})
            if job["S"] == "R":
                queue_counts[queue]["run"] += 1
                total_running_jobs += 1
            elif job["S"] in self.QUEUED_STATES:
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
            worker_node["state"] = self._merge_node_state(worker_node["state"], node["state"])
            worker_node["np"] = str(max(int(worker_node["np"]), int(node["np"])))

        node_jobs = self._map_jobs_to_nodes(self._get_jobs(), worker_nodes_by_name)
        for raw_name, worker_node in worker_nodes_by_name.items():
            worker_node["core_job_map"] = dict((idx, job_id) for idx, job_id in enumerate(node_jobs.get(raw_name, [])))

        worker_nodes = list(worker_nodes_by_name.values())
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

    @classmethod
    def _merge_node_state(cls, previous, current):
        previous_priority = cls.STATE_PRIORITY.get(previous, 0)
        current_priority = cls.STATE_PRIORITY.get(current, 0)
        return current if current_priority > previous_priority else previous

    @classmethod
    def _map_jobs_to_nodes(cls, jobs, worker_nodes_by_name):
        node_jobs = OrderedDict((node_name, []) for node_name in worker_nodes_by_name)
        for job in jobs:
            if job["S"] not in cls.ACTIVE_STATES:
                continue
            nodes = [node for node in cls.expand_nodelist(job["Nodes"]) if node in worker_nodes_by_name]
            if not nodes:
                continue

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
        return node_jobs

    @classmethod
    def expand_nodelist(cls, nodelist):
        if not nodelist or nodelist.startswith("("):
            return []

        expanded = [nodelist]
        while any("[" in item for item in expanded):
            next_expanded = []
            for item in expanded:
                next_expanded.extend(cls._expand_first_range(item))
            expanded = next_expanded
        return expanded

    @staticmethod
    def _expand_first_range(value):
        match = re.search(r"\[([^\]]+)\]", value)
        if not match:
            return [value]

        prefix = value[: match.start()]
        suffix = value[match.end() :]
        choices = []
        for token in match.group(1).split(","):
            if "-" not in token:
                choices.append(token)
                continue
            start, end = token.split("-", 1)
            width = len(start)
            choices.extend(str(number).zfill(width) for number in range(int(start), int(end) + 1))
        return [prefix + choice + suffix for choice in choices]
