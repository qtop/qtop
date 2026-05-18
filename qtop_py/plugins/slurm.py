import logging
import re
from collections import defaultdict

from qtop_py.serialiser import GenericBatchSystem, StatExtractor


RUNNING_STATES = {"R", "CG", "CF", "RUNNING", "COMPLETING", "CONFIGURING"}
QUEUED_STATES = {"PD", "PENDING"}
SLURM_STATE_ALIASES = {
    "RUNNING": "R",
    "PENDING": "PD",
    "COMPLETING": "CG",
    "CONFIGURING": "CF",
    "FAILED": "F",
    "CANCELLED": "CA",
    "CANCELED": "CA",
    "SUSPENDED": "S",
    "TIMEOUT": "TO",
}


class SlurmStatExtractor(StatExtractor):
    def extract_squeue(self, orig_file):
        jobs = []
        with open(orig_file, "r") as fin:
            for line in fin:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                try:
                    job_id, queue, user, state, cpus, nodes = line.split("|", 5)
                except ValueError:
                    logging.warning("Skipping unparsable Slurm squeue line: %s" % line)
                    continue

                user = self.anonymize(user, "users")
                state = self.normalize_job_state(state)
                jobs.append(
                    {
                        "JobId": job_id,
                        "Queue": queue.rstrip("*"),
                        "UnixAccount": user,
                        "S": state,
                        "CPUs": int(cpus or 0),
                        "Nodes": nodes,
                    }
                )
        return jobs

    @staticmethod
    def normalize_job_state(state):
        state = state.strip().upper()
        return SLURM_STATE_ALIASES.get(state, state)


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
        for qstat in self.slurm_stat_maker.extract_squeue(self.squeue_file):
            job_ids.append(qstat["JobId"])
            usernames.append(qstat["UnixAccount"])
            job_states.append(qstat["S"])
            queue_names.append(qstat["Queue"])
        return job_ids, usernames, job_states, queue_names

    def get_queues_info(self):
        queue_stats = defaultdict(lambda: {"queue_name": "", "run": 0, "queued": 0, "lm": "--", "state": "E R"})
        for job in self.slurm_stat_maker.extract_squeue(self.squeue_file):
            queue = job["Queue"]
            queue_stats[queue]["queue_name"] = queue
            if job["S"] in RUNNING_STATES:
                queue_stats[queue]["run"] += 1
            elif job["S"] in QUEUED_STATES:
                queue_stats[queue]["queued"] += 1

        qstatq_list = list(queue_stats.values())
        total_running_jobs = sum(queue["run"] for queue in qstatq_list)
        total_queued_jobs = sum(queue["queued"] for queue in qstatq_list)
        return total_running_jobs, total_queued_jobs, qstatq_list

    def get_worker_nodes(self, job_ids, job_queues, options):
        worker_nodes = self._read_sinfo_nodes(self.sinfo_file)
        nodes_by_name = {node["domainname"]: node for node in worker_nodes}

        for job in self.slurm_stat_maker.extract_squeue(self.squeue_file):
            if job["S"] not in RUNNING_STATES:
                continue
            target_nodes = expand_slurm_nodelist(job["Nodes"])
            if not target_nodes:
                continue

            remaining_cpus = max(job["CPUs"], 1)
            target_count = len(target_nodes)
            while remaining_cpus:
                made_progress = False
                for node_name in target_nodes:
                    node = nodes_by_name.setdefault(node_name, self._empty_node(node_name, job["Queue"]))
                    self._append_job_to_next_core(node, job["JobId"])
                    remaining_cpus -= 1
                    made_progress = True
                    if not remaining_cpus:
                        break
                if not made_progress or target_count == 0:
                    break

        worker_nodes = list(nodes_by_name.values())
        return self.ensure_worker_nodes_have_qnames(worker_nodes, job_ids, job_queues)

    def _read_sinfo_nodes(self, sinfo_file):
        nodes = []
        anonymize = self.slurm_stat_maker.anonymize_func()
        with open(sinfo_file, "r") as fin:
            for line in fin:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                try:
                    node_name, state, queue, cpus = line.split("|", 3)
                except ValueError:
                    logging.warning("Skipping unparsable Slurm sinfo line: %s" % line)
                    continue
                domainname = node_name if not self.options.ANONYMIZE else anonymize(node_name, "wns")
                nodes.append(
                    {
                        "domainname": domainname,
                        "state": map_slurm_node_state(state),
                        "np": parse_slurm_cpu_total(cpus),
                        "core_job_map": {},
                        "qname": [queue.rstrip("*")],
                    }
                )
        return nodes

    @staticmethod
    def _empty_node(node_name, queue):
        return {"domainname": node_name, "state": "b", "np": 1, "core_job_map": {}, "qname": [queue]}

    @staticmethod
    def _append_job_to_next_core(node, job_id):
        core_job_map = node["core_job_map"]
        next_core = len(core_job_map)
        if next_core >= int(node["np"]):
            node["np"] = next_core + 1
        core_job_map[next_core] = job_id


def parse_slurm_cpu_total(cpu_field):
    try:
        return int(cpu_field.split("/")[-1])
    except (IndexError, ValueError):
        return 0


def map_slurm_node_state(state):
    state = state.lower().split("+", 1)[0].rstrip("*~#")
    if state in {"idle"}:
        return "-"
    if state in {"alloc", "allocated"}:
        return "b"
    if state in {"mix", "mixed"}:
        return "%"
    if state.startswith("drain"):
        return "d"
    if state.startswith("down") or state in {"fail", "failing"}:
        return "d"
    if state in {"comp", "completing"}:
        return "c"
    return state[:1] or "-"


def expand_slurm_nodelist(nodelist):
    if not nodelist or nodelist.startswith("("):
        return []

    expanded = []
    for part in _split_top_level_commas(nodelist):
        match = re.match(r"^(?P<prefix>[^\[]+)\[(?P<inner>[^\]]+)\]$", part)
        if not match:
            expanded.append(part)
            continue

        prefix = match.group("prefix")
        for item in match.group("inner").split(","):
            if "-" in item:
                start, end = item.split("-", 1)
                width = max(len(start), len(end))
                for value in range(int(start), int(end) + 1):
                    expanded.append("%s%s" % (prefix, str(value).zfill(width)))
            else:
                expanded.append("%s%s" % (prefix, item))
    return expanded


def _split_top_level_commas(value):
    parts = []
    buf = []
    depth = 0
    for char in value:
        if char == "[":
            depth += 1
        elif char == "]":
            depth -= 1
        elif char == "," and depth == 0:
            parts.append("".join(buf))
            buf = []
            continue
        buf.append(char)
    if buf:
        parts.append("".join(buf))
    return parts
