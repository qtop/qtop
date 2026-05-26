##
## qtop is a tool to monitor queuing systems - https://github.com/qtop/qtop
##
## SPDX-License-Identifier: MIT
##

import logging
from collections import defaultdict

import qtop_py.fileutils as fileutils
from qtop_py.serialiser import GenericBatchSystem, StatExtractor


class SlurmStatExtractor(StatExtractor):
    def __init__(self, config, options):
        StatExtractor.__init__(self, config, options)

    @staticmethod
    def parse_key_values(line):
        values = {}
        for token in line.strip().split():
            if "=" not in token:
                continue
            key, value = token.split("=", 1)
            values[key] = value
        return values

    @staticmethod
    def normalize_node_state(state):
        base_state = state.split("+", 1)[0].split("*", 1)[0].upper()
        if base_state in ("IDLE", "COMPLETING"):
            return "-"
        if base_state in ("ALLOCATED", "MIXED"):
            return "b"
        if base_state in ("DOWN", "DRAIN", "DRAINED", "FAIL", "FAILING", "UNKNOWN", "NO_RESPOND"):
            return "d"
        return base_state[:1].lower() or "?"

    @staticmethod
    def expand_nodelist(nodelist):
        if not nodelist or nodelist.startswith("("):
            return []
        if "[" not in nodelist:
            return [nodelist]

        prefix, rest = nodelist.split("[", 1)
        ranges = rest.split("]", 1)[0]
        nodes = []
        for item in ranges.split(","):
            if "-" in item:
                start, end = item.split("-", 1)
                width = len(start)
                for idx in range(int(start), int(end) + 1):
                    nodes.append("%s%s" % (prefix, str(idx).zfill(width)))
            else:
                nodes.append("%s%s" % (prefix, item))
        return nodes

    def extract_jobs(self, slurm_jobs_file):
        jobs = []
        try:
            fileutils.check_empty_file(slurm_jobs_file)
        except fileutils.FileEmptyError:
            logging.error("File %s seems to be empty." % slurm_jobs_file)
            return jobs

        with open(slurm_jobs_file, "r") as fin:
            for line in fin:
                parts = line.rstrip("\n").split("|")
                if len(parts) < 5:
                    continue
                job_id, user, state, partition, nodelist = parts[:5]
                jobs.append(
                    {
                        "JobId": job_id,
                        "UnixAccount": self.anonymize(user, "users"),
                        "S": state,
                        "Queue": self.anonymize(partition, "qs"),
                        "Nodes": self.expand_nodelist(nodelist),
                    }
                )
        return jobs

    def extract_nodes(self, slurm_nodes_file):
        nodes = []
        try:
            fileutils.check_empty_file(slurm_nodes_file)
        except fileutils.FileEmptyError:
            logging.error("File %s seems to be empty." % slurm_nodes_file)
            return nodes

        with open(slurm_nodes_file, "r") as fin:
            for line in fin:
                node = self.parse_key_values(line)
                if not node.get("NodeName"):
                    continue
                nodes.append(node)
        return nodes


class SlurmBatchSystem(GenericBatchSystem):
    @staticmethod
    def get_mnemonic():
        return "slurm"

    def __init__(self, scheduler_output_filenames, config, options):
        self.slurm_nodes_file = scheduler_output_filenames.get("slurm_nodes_file")
        self.slurm_jobs_file = scheduler_output_filenames.get("slurm_jobs_file")
        self.config = config
        self.options = options
        self.slurm_stat_maker = SlurmStatExtractor(self.config, self.options)

    def get_worker_nodes(self, job_ids, job_queues, options):
        nodes = self.slurm_stat_maker.extract_nodes(self.slurm_nodes_file)
        jobs = self.slurm_stat_maker.extract_jobs(self.slurm_jobs_file)
        node_jobs = defaultdict(list)

        for job in jobs:
            if job["S"] != "R":
                continue
            for node_name in job["Nodes"]:
                node_jobs[node_name].append(job["JobId"])

        worker_nodes = []
        anonymize = self.slurm_stat_maker.anonymize_func() if self.options.ANONYMIZE else self.slurm_stat_maker.eponymize_func()
        for node in nodes:
            node_name = node["NodeName"]
            core_job_map = dict((idx, job_id) for idx, job_id in enumerate(node_jobs[node_name]))
            worker_nodes.append(
                {
                    "domainname": anonymize(node_name, "wns"),
                    "state": self.slurm_stat_maker.normalize_node_state(node.get("State", "?")),
                    "np": node.get("CPUTot", node.get("CPUs", 0)),
                    "core_job_map": core_job_map,
                }
            )

        return self.ensure_worker_nodes_have_qnames(worker_nodes, job_ids, job_queues)

    def get_jobs_info(self):
        jobs = self.slurm_stat_maker.extract_jobs(self.slurm_jobs_file)
        job_ids, usernames, job_states, queue_names = [], [], [], []

        for job in jobs:
            job_ids.append(job["JobId"])
            usernames.append(job["UnixAccount"])
            job_states.append(job["S"])
            queue_names.append(job["Queue"])

        return job_ids, usernames, job_states, queue_names

    def get_queues_info(self):
        jobs = self.slurm_stat_maker.extract_jobs(self.slurm_jobs_file)
        queues = defaultdict(lambda: {"run": 0, "queued": 0, "state": "?", "lm": 0})

        for job in jobs:
            queue = queues[job["Queue"]]
            if job["S"] == "R":
                queue["run"] += 1
                queue["state"] = "R"
            else:
                queue["queued"] += 1
                if queue["state"] == "?":
                    queue["state"] = job["S"]

        qstatq_list = []
        for queue_name, values in queues.items():
            qstatq_list.append(
                {
                    "queue_name": queue_name,
                    "run": str(values["run"]),
                    "queued": str(values["queued"]),
                    "state": values["state"],
                    "lm": values["lm"],
                }
            )

        total_running_jobs = sum(int(q["run"]) for q in qstatq_list)
        total_queued_jobs = sum(int(q["queued"]) for q in qstatq_list)
        return total_running_jobs, total_queued_jobs, qstatq_list
