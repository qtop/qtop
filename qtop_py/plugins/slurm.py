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

import qtop_py.fileutils as fileutils
from qtop_py.serialiser import GenericBatchSystem, StatExtractor


def expand_slurm_nodelist(nodelist):
    """
    Expand Slurm's compact node-list syntax.

    Examples:
    node[001-003,007] -> node001, node002, node003, node007
    rack[1-2]n[01-02] -> rack1n01, rack1n02, rack2n01, rack2n02
    """
    if not nodelist:
        return []

    expanded = []
    for part in _split_top_level_commas(nodelist):
        expanded.extend(_expand_slurm_part(part))
    return expanded


def _split_top_level_commas(value):
    parts = []
    depth = 0
    start = 0
    for idx, char in enumerate(value):
        if char == "[":
            depth += 1
        elif char == "]":
            depth -= 1
        elif char == "," and depth == 0:
            parts.append(value[start:idx])
            start = idx + 1
    parts.append(value[start:])
    return [part for part in parts if part]


def _expand_slurm_part(part):
    match = re.search(r"\[([^\]]+)\]", part)
    if not match:
        return [part]

    prefix = part[: match.start()]
    suffix = part[match.end() :]
    values = []
    for item in match.group(1).split(","):
        if "-" in item:
            start, stop = item.split("-", 1)
            width = len(start)
            for number in range(int(start), int(stop) + 1):
                values.append(str(number).zfill(width))
        else:
            values.append(item)

    expanded = []
    for value in values:
        for suffix_value in _expand_slurm_part(suffix):
            expanded.append(prefix + value + suffix_value)
    return expanded


class SlurmStatExtractor(StatExtractor):
    def __init__(self, config, options):
        StatExtractor.__init__(self, config, options)

    def extract_squeue(self, orig_file):
        """
        Parse:
        squeue -h -o "%i|%P|%u|%t|%C|%R"
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
                if not line or line.startswith("#"):
                    continue
                parts = line.split("|")
                if len(parts) != 6:
                    logging.warning("Skipping malformed squeue line: %s" % line)
                    continue
                job_id, partition, user, state, cpus, nodelist = parts
                jobs.append(
                    {
                        "JobId": job_id.split(".")[0],
                        "Queue": partition,
                        "UnixAccount": self.anonymize(user, "users"),
                        "S": state,
                        "CPUs": int(cpus or 0),
                        "NodeList": nodelist,
                    }
                )
        return jobs

    def extract_sinfo(self, orig_file):
        """
        Parse:
        sinfo -h -N -o "%N|%P|%t|%c"
        """
        try:
            fileutils.check_empty_file(orig_file)
        except fileutils.FileEmptyError:
            logging.error("File %s seems to be empty." % orig_file)
            return []

        nodes = []
        node_lookup = {}
        with open(orig_file, "r") as fin:
            for line in fin:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                parts = line.split("|")
                if len(parts) != 4:
                    logging.warning("Skipping malformed sinfo line: %s" % line)
                    continue
                nodelist, partition, state, cpus = parts
                partition = partition.rstrip("*")
                for node_name in expand_slurm_nodelist(nodelist):
                    visible_node_name = self.anonymize(node_name, "wns")
                    queue_name = self.anonymize(partition, "qs")
                    mapped_state = _map_slurm_node_state(state)
                    existing_node = node_lookup.get(visible_node_name)
                    if existing_node is not None:
                        if queue_name not in existing_node["qname"]:
                            existing_node["qname"].append(queue_name)
                        existing_node["state"] = _merge_node_state(existing_node["state"], mapped_state)
                        existing_node["np"] = max(existing_node["np"], int(cpus or 0))
                        continue
                    node = {
                        "domainname": visible_node_name,
                        "qname": [queue_name],
                        "state": mapped_state,
                        "np": int(cpus or 0),
                        "core_job_map": {},
                    }
                    node_lookup[visible_node_name] = node
                    nodes.append(node)
        return nodes


class SlurmBatchSystem(GenericBatchSystem):
    @staticmethod
    def get_mnemonic():
        return "slurm"

    def __init__(self, scheduler_output_filenames, config, options):
        self.sinfo_file = scheduler_output_filenames.get("sinfo_file")
        self.squeue_file = scheduler_output_filenames.get("squeue_file")
        self.config = config
        self.options = options
        self.slurm_stat_maker = SlurmStatExtractor(self.config, self.options)

    def get_jobs_info(self):
        job_ids, usernames, job_states, queue_names = [], [], [], []
        for job in self.slurm_stat_maker.extract_squeue(self.squeue_file):
            job_ids.append(job["JobId"])
            usernames.append(job["UnixAccount"])
            job_states.append(job["S"])
            queue_names.append(job["Queue"])
        return job_ids, usernames, job_states, queue_names

    def get_queues_info(self):
        queues = {}
        for job in self.slurm_stat_maker.extract_squeue(self.squeue_file):
            queue_name = job["Queue"]
            queue = queues.setdefault(queue_name, {"queue_name": queue_name, "run": 0, "queued": 0, "lm": "--", "state": "E"})
            if job["S"] in ("R", "CG"):
                queue["run"] += 1
            elif job["S"] in ("PD", "CF", "RF", "RH", "RQ", "RS", "RV"):
                queue["queued"] += 1

        qstatq_list = []
        for queue in queues.values():
            qstatq_list.append(
                {
                    "queue_name": queue["queue_name"],
                    "run": str(queue["run"]),
                    "queued": str(queue["queued"]),
                    "lm": queue["lm"],
                    "state": queue["state"],
                }
            )
        total_running_jobs = sum(int(queue["run"]) for queue in qstatq_list)
        total_queued_jobs = sum(int(queue["queued"]) for queue in qstatq_list)
        return total_running_jobs, total_queued_jobs, qstatq_list

    def get_worker_nodes(self, job_ids, job_queues, options):
        nodes = self.slurm_stat_maker.extract_sinfo(self.sinfo_file)
        node_lookup = dict((node["domainname"], node) for node in nodes)

        for job in self.slurm_stat_maker.extract_squeue(self.squeue_file):
            allocated_nodes = _get_allocated_nodes(job["NodeList"])
            if not allocated_nodes:
                continue
            cores_per_node = max(1, int((job["CPUs"] + len(allocated_nodes) - 1) / len(allocated_nodes)))
            for node_name in allocated_nodes:
                visible_node_name = self.slurm_stat_maker.anonymize(node_name, "wns")
                node = node_lookup.get(visible_node_name)
                if node is None:
                    node = {"domainname": visible_node_name, "qname": [], "state": "%", "np": cores_per_node, "core_job_map": {}}
                    node_lookup[visible_node_name] = node
                    nodes.append(node)
                start_core = len(node["core_job_map"])
                for core in range(start_core, start_core + cores_per_node):
                    if core >= int(node["np"]):
                        node["np"] = core + 1
                    node["core_job_map"][str(core)] = job["JobId"]

        return self.ensure_worker_nodes_have_qnames(nodes, job_ids, job_queues)


def _get_allocated_nodes(nodelist_or_reason):
    if not nodelist_or_reason or nodelist_or_reason.startswith("("):
        return []
    if nodelist_or_reason in ("None", "N/A"):
        return []
    return expand_slurm_nodelist(nodelist_or_reason)


def _map_slurm_node_state(state):
    state = state.lower().replace("*", "").replace("~", "").replace("$", "")
    if state in ("idle", "idle+cloud", "idle+power"):
        return "-"
    if state in ("alloc", "allocated"):
        return "b"
    if state in ("mix", "mixed"):
        return "%"
    if state.startswith("down"):
        return "d"
    if state.startswith("drain") or state.startswith("drng"):
        return "d"
    if state.startswith("maint"):
        return "m"
    if state.startswith("comp"):
        return "c"
    if state.startswith("resv"):
        return "r"
    if state.startswith("fail"):
        return "f"
    if state.startswith("unk"):
        return "?"
    return state[:1] or "?"


def _merge_node_state(first, second):
    priority = {"d": 90, "f": 80, "m": 70, "%": 60, "b": 50, "c": 40, "r": 30, "-": 10, "?": 0}
    if priority.get(second, 20) > priority.get(first, 20):
        return second
    return first
