##
## qtop is a tool to monitor queuing systems - https://github.com/qtop/qtop
##
## Copyright (c) 2016 Fotis Georgatos
## Copyright (c) 2016 Sotiris Fragkiskos
## Copyright (c) 2023 Hewlett Packard Enterprise Development LP
##
## SPDX-License-Identifier: MIT
##

import re
import logging
from qtop_py.serialiser import StatExtractor, GenericBatchSystem
import qtop_py.fileutils as fileutils


def expand_slurm_nodelist(nodelist):
    """
    Expand Slurm compact node list notation to individual node names.

    Examples:
      'wn001'              -> ['wn001']
      'wn[001-003]'        -> ['wn001', 'wn002', 'wn003']
      'wn[001-003,005]'    -> ['wn001', 'wn002', 'wn003', 'wn005']
      'wn[001,003,005-007]'-> ['wn001', 'wn003', 'wn005', 'wn006', 'wn007']
      '(Priority)'         -> []
      '(Resources)'        -> []
    """
    if not nodelist or nodelist.startswith("("):
        return []

    m = re.match(r"^([\w.-]+)\[(.+)\]$", nodelist)
    if not m:
        # Single node or comma-separated list without bracket ranges
        return [n.strip() for n in nodelist.split(",") if n.strip()]

    prefix = m.group(1)
    range_str = m.group(2)

    nodes = []
    for part in range_str.split(","):
        part = part.strip()
        if "-" in part:
            start_str, end_str = part.split("-", 1)
            width = len(start_str)
            start, end = int(start_str), int(end_str)
            for i in range(start, end + 1):
                nodes.append("%s%s" % (prefix, str(i).zfill(width)))
        else:
            nodes.append("%s%s" % (prefix, part))

    return nodes


class SlurmStatExtractor(StatExtractor):
    def __init__(self, config, options):
        StatExtractor.__init__(self, config, options)
        # squeue format: squeue -h -o "%i %P %u %T %C %R"
        # Fields: JOBID PARTITION USER STATE CPUS NODELIST(REASON)
        self.squeue_search = (
            r"^\s*(?P<job_id>\d+)\s+"
            r"(?P<partition>[\w,.-]+)\s+"
            r"(?P<user>[\w.-]+)\s+"
            r"(?P<state>[A-Z_]+)\s+"
            r"(?P<cpus>\d+)\s+"
            r"(?P<nodelist>\S+)"
        )

    def extract_squeue(self, orig_file):
        """
        Parse squeue output file.

        Expected format (squeue -h -o "%i %P %u %T %C %R"):
          JOBID PARTITION USER STATE CPUS NODELIST(REASON)

        Returns a list of dicts with keys:
          JobId, UnixAccount, S, Queue, Cpus, NodeList
        """
        try:
            fileutils.check_empty_file(orig_file)
        except fileutils.FileEmptyError:
            logging.error("File %s seems to be empty." % orig_file)
            return []

        all_values = []
        with open(orig_file, "r") as fin:
            for line in fin:
                line = line.strip()
                if not line:
                    continue
                # Skip header lines
                if line.upper().startswith("JOBID") or line.upper().startswith("JOB_ID"):
                    continue

                m = re.match(self.squeue_search, line)
                if not m:
                    logging.warning("Could not parse squeue line: %s" % line)
                    continue

                user = self.anonymize(m.group("user"), "users")
                all_values.append(
                    {
                        "JobId": m.group("job_id"),
                        "UnixAccount": user,
                        "S": m.group("state"),
                        "Queue": m.group("partition"),
                        "Cpus": int(m.group("cpus")),
                        "NodeList": m.group("nodelist"),
                    }
                )

        return all_values


class SlurmBatchSystem(GenericBatchSystem):
    @staticmethod
    def get_mnemonic():
        return "slurm"

    # Maps sinfo node state abbreviations to single-character qtop state codes
    _SINFO_STATE_MAP = {
        "alloc": "b",
        "allocated": "b",
        "mix": "%",
        "mixed": "%",
        "idle": "-",
        "down": "d",
        "drain": "x",
        "draining": "x",
        "drained": "x",
        "comp": "c",
        "completing": "c",
        "fail": "f",
        "failing": "f",
        "maint": "m",
        "reserved": "r",
        "future": "u",
        "unknown": "?",
        "power_up": "p",
        "power_down": "p",
    }

    # sinfo format: sinfo -N -h -o "%N %t %c %P"
    _SINFO_SEARCH = r"^(?P<nodename>[\w.-]+)\s+(?P<state>[\w*+~#$@^]+)\s+(?P<cpus>\d+)\s+(?P<partition>[\w,*]+)"

    def __init__(self, scheduler_output_filenames, config, options):
        self.sinfo_file = scheduler_output_filenames.get("sinfo_file")
        self.squeue_file = scheduler_output_filenames.get("squeue_file")
        self.config = config
        self.options = options
        self.slurm_stat_maker = SlurmStatExtractor(self.config, self.options)
        self._squeue_data = None

    def _get_squeue_data(self):
        if self._squeue_data is None:
            self._squeue_data = self.slurm_stat_maker.extract_squeue(self.squeue_file)
        return self._squeue_data

    def get_jobs_info(self):
        """
        Returns four parallel lists: job_ids, usernames, job_states, queue_names.
        Reads from the squeue output file.
        """
        job_ids, usernames, job_states, queue_names = [], [], [], []
        for entry in self._get_squeue_data():
            job_ids.append(entry["JobId"])
            usernames.append(entry["UnixAccount"])
            job_states.append(entry["S"])
            queue_names.append(entry["Queue"])

        logging.debug(
            "job_ids, usernames, job_states, queue_names lengths: "
            "%(job_ids)s, %(usernames)s, %(job_states)s, %(queue_names)s"
            % {
                "job_ids": len(job_ids),
                "usernames": len(usernames),
                "job_states": len(job_states),
                "queue_names": len(queue_names),
            }
        )
        return job_ids, usernames, job_states, queue_names

    def get_worker_nodes(self, job_ids, job_queues, options):
        """
        Parse sinfo output to build worker node information.
        Running job-to-core assignments are inferred from squeue node lists.

        sinfo format expected: sinfo -N -h -o "%N %t %c %P"
        """
        try:
            fileutils.check_empty_file(self.sinfo_file)
        except fileutils.FileEmptyError:
            return []

        # Build node -> [(job_id, cpus_on_this_node), ...] from running jobs
        node_to_jobs = {}
        for entry in self._get_squeue_data():
            if entry["S"] != "RUNNING":
                continue
            nodes = expand_slurm_nodelist(entry["NodeList"])
            if not nodes:
                continue
            cpus_per_node = max(1, entry["Cpus"] // len(nodes))
            for node in nodes:
                node_to_jobs.setdefault(node, []).append((entry["JobId"], cpus_per_node))

        anonymize = self.slurm_stat_maker.anonymize_func()
        worker_nodes = []

        with open(self.sinfo_file, "r") as fin:
            for line in fin:
                line = line.strip()
                if not line:
                    continue
                # Skip header lines
                if line.upper().startswith("NODELIST") or line.upper().startswith("NODE"):
                    if not re.match(r"^\w+\d", line):
                        continue

                m = re.match(self._SINFO_SEARCH, line)
                if not m:
                    logging.warning("Could not parse sinfo line: %s" % line)
                    continue

                nodename = m.group("nodename")
                # Strip trailing state modifiers: * (not responding), ~ (powered down), etc.
                raw_state = re.sub(r"[*+~#$@^]", "", m.group("state"))
                ncpus = int(m.group("cpus"))

                state = self._SINFO_STATE_MAP.get(raw_state, "?")
                dn = nodename if not self.options.ANONYMIZE else anonymize(nodename, "wns")

                # Assign consecutive core indices to each job running on this node
                core_job_map = {}
                core_idx = 0
                for job_id, cpus in node_to_jobs.get(nodename, []):
                    for _ in range(cpus):
                        if core_idx < ncpus:
                            core_job_map[core_idx] = job_id
                            core_idx += 1

                worker_nodes.append(
                    {
                        "domainname": dn,
                        "state": state,
                        "np": ncpus,
                        "core_job_map": core_job_map,
                    }
                )

        worker_nodes = self.ensure_worker_nodes_have_qnames(worker_nodes, job_ids, job_queues)
        logging.info("worker_nodes contains %s entries" % len(worker_nodes))
        return worker_nodes

    def get_queues_info(self):
        """
        Derive partition (queue) statistics from squeue data.
        Returns (total_running, total_queued, qstatq_list).
        """
        partition_running = {}
        partition_queued = {}

        for entry in self._get_squeue_data():
            partition = entry["Queue"]
            if entry["S"] == "RUNNING":
                partition_running[partition] = partition_running.get(partition, 0) + 1
            else:
                partition_queued[partition] = partition_queued.get(partition, 0) + 1

        qstatq_list = []
        for partition in sorted(set(list(partition_running.keys()) + list(partition_queued.keys()))):
            qstatq_list.append(
                {
                    "queue_name": partition,
                    "run": str(partition_running.get(partition, 0)),
                    "queued": str(partition_queued.get(partition, 0)),
                    "state": "E",
                    "lm": "--",
                }
            )

        total_running = sum(partition_running.values())
        total_queued = sum(partition_queued.values())
        return total_running, total_queued, qstatq_list
