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
from qtop_py.serialiser import StatExtractor, GenericBatchSystem
import qtop_py.fileutils as fileutils


class SlurmStatExtractor(StatExtractor):
    def __init__(self, config, options):
        StatExtractor.__init__(self, config, options)

    def extract_squeue(self, orig_file):
        """
        reads squeue output file and parses job information.
        Standard squeue output columns:
        JOBID PARTITION NAME USER STATE TIME TIME_LIMIT NODES NODELIST(REASON)
        Returns a list of dicts with keys: JobId, UnixAccount, S, Queue, JobName
        """
        try:
            fileutils.check_empty_file(orig_file)
        except fileutils.FileEmptyError:
            logging.error("File %s seems to be empty." % orig_file)
            return []

        all_values = []
        with open(orig_file, "r") as fin:
            header = fin.readline().strip()
            # Skip comment lines and empty lines
            for line in fin:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                # Use regex to parse the line - squeue output has variable whitespace
                # Format: JOBID PARTITION NAME USER STATE TIME TIME_LIMIT NODES NODELIST
                parts = re.split(r'\s+', line)
                if len(parts) >= 5:
                    qstat_values = dict()
                    qstat_values["JobId"] = parts[0]                # JOBID
                    qstat_values["Queue"] = self.anonymize(parts[1], "qs")   # PARTITION
                    qstat_values["JobName"] = parts[2]               # NAME
                    qstat_values["UnixAccount"] = self.anonymize(parts[3], "users")  # USER
                    qstat_values["S"] = self._map_state(parts[4])   # STATE
                    all_values.append(qstat_values)
        return all_values

    @staticmethod
    def _map_state(state):
        """Map Slurm state codes to qtop state codes."""
        state_map = {
            "PD": "Q",  # Pending
            "R": "R",   # Running
            "CG": "E",  # Completing
            "CD": "E",  # Completed
            "F": "F",   # Failed
            "NF": "F",  # Node Fail
            "TO": "E",  # Timeout
            "PR": "S",  # Preempted
            "S": "S",   # Suspended
            "DL": "E",  # Deadline
            "SE": "E",  # Special Exit
            "RV": "E",  # Revoked
            "WC": "E",  # Waiting for Container
            "BF": "Q",  # Boot Fail
            "CA": "C",  # Cancelled
            "CC": "C",  # Completed (exit code)
        }
        return state_map.get(state.upper(), "?")


class SlurmBatchSystem(GenericBatchSystem):
    @staticmethod
    def get_mnemonic():
        return "slurm"

    def __init__(self, scheduler_output_filenames, config, options):
        self.squeue_file = scheduler_output_filenames.get("squeue_file")
        self.scontrol_file = scheduler_output_filenames.get("scontrol_file")
        self.sinfo_file = scheduler_output_filenames.get("sinfo_file")

        self.config = config
        self.options = options
        self.slurm_stat_maker = SlurmStatExtractor(self.config, self.options)

    def get_worker_nodes(self, job_ids, job_queues, options):
        """
        Reads scontrol show node output to get worker node information.
        Format per node:
        NodeName=<name> State=<state> ... (may have Jobs= section)
        """
        try:
            fileutils.check_empty_file(self.scontrol_file)
        except fileutils.FileEmptyError:
            logging.error("File %s seems to be empty." % self.scontrol_file)
            return []

        all_slurm_values = []
        anonymize = self.slurm_stat_maker.anonymize_func()

        with open(self.scontrol_file, "r") as fin:
            current_block = None

            for line in fin:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue

                if line.startswith("NodeName="):
                    # Save previous node before starting new one
                    if current_block is not None:
                        # Ensure core_job_map is set before saving
                        if "core_job_map" not in current_block:
                            current_block["core_job_map"] = dict()
                        all_slurm_values.append(current_block)

                    # Start new node block
                    current_block = self._parse_node_line(line, anonymize, options)

                elif line.lstrip().startswith("Jobs=") and current_block is not None:
                    if "core_job_map" not in current_block:
                        jobs = self._parse_jobs_line(line)
                        if jobs:
                            current_block["core_job_map"] = dict((idx, job) for idx, job in enumerate(jobs))
                        else:
                            current_block["core_job_map"] = dict()

            # Don't forget the last node
            if current_block is not None:
                if "core_job_map" not in current_block:
                    current_block["core_job_map"] = dict()
                all_slurm_values.append(current_block)

        all_slurm_values = self.ensure_worker_nodes_have_qnames(all_slurm_values, job_ids, job_queues)
        return all_slurm_values

    def _parse_node_line(self, line, anonymize, options):
        """
        Parse a NodeName= line to extract node info.
        """
        block = {}

        # Extract NodeName
        match = re.search(r"NodeName=(\S+)", line)
        if match:
            node_name = match.group(1)
            block["domainname"] = node_name if not options.ANONYMIZE else anonymize(node_name, "wns")

        # Extract State
        match = re.search(r"State=(\S+)", line)
        if match:
            state_raw = match.group(1).lower()
            # Map Slurm state to qtop single-char state
            state_map = {
                "idle": "-",
                "mixed": "b",
                "allocated": "b",
                "down": "d",
                "drain": "d",
                "drained": "d",
                "fail": "d",
                "failed": "d",
                "maint": "d",
                "reserved": "d",
                "reboot": "d",
                "powering_down": "d",
                "powering_up": "-",
            }
            block["state"] = state_map.get(state_raw, "-")

        # Extract Cores (CPUs)
        match = re.search(r"CPUs=(\d+)", line)
        if match:
            block["np"] = int(match.group(1))

        # Extract Jobs if present on the same line as NodeName
        # Format: NodeName=xxx ... Jobs=(12345,12346)
        match = re.search(r"Jobs=\s*\(([^)]*)\)", line)
        if match:
            jobs_str = match.group(1).strip()
            if jobs_str:
                job_ids_list = [j.split("_")[0] for j in jobs_str.split(",")]
                block["core_job_map"] = dict((idx, jid) for idx, jid in enumerate(job_ids_list))

        # If core_job_map was not set here, it will be set by the Jobs= line handler

        return block

    def _parse_jobs_line(self, line):
        """
        Parse a Jobs= line to extract job IDs.
        Format: Jobs=(12345) or Jobs=(12345,12346)
        """
        match = re.search(r"Jobs=\s*\(([^)]*)\)", line)
        if not match:
            return []
        jobs_str = match.group(1).strip()
        if not jobs_str:
            return []
        job_ids = []
        for part in jobs_str.split(","):
            part = part.strip()
            if part:
                # Format may be "123456_123" or just "123456"
                job_id = part.split("_")[0]
                job_ids.append(job_id)
        return job_ids

    def get_jobs_info(self):
        """
        Reads squeue output file and returns job info as 4 parallel lists.
        """
        job_ids, usernames, job_states, queue_names = [], [], [], []

        all_values = self.slurm_stat_maker.extract_squeue(self.squeue_file)
        for qstat in all_values:
            job_ids.append(str(qstat["JobId"]))
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
        """
        Parses sinfo output to extract queue information.
        Returns total_running, total_queued, and qstatq_list.
        """
        try:
            fileutils.check_empty_file(self.sinfo_file)
        except fileutils.FileEmptyError:
            logging.error("File %s seems to be empty." % self.sinfo_file)
            return 0, 0, []

        qstatq_list = []
        total_running_jobs = 0
        total_queued_jobs = 0

        with open(self.sinfo_file, "r") as fin:
            fin.readline()  # header
            for line in fin:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue

                parts = line.split()
                if len(parts) >= 3:
                    queue_name = parts[0]
                    if self.options.ANONYMIZE:
                        queue_name = self.slurm_stat_maker.anonymize(queue_name, "qs")

                    # Parse state (e.g. "up", "down", "drain")
                    state = parts[1].lower()

                    # Parse number of nodes in this partition
                    try:
                        nodes_count = int(parts[2])
                    except ValueError:
                        nodes_count = 0

                    # Try to extract jobs info
                    # Format: PARTITION NAME STATE NODES...
                    # We estimate running from jobs, queued from pending
                    qstatq_values = {
                        "queue_name": queue_name,
                        "run": 0,
                        "queued": 0,
                        "state": state[:1].upper(),
                        "lm": "0",
                    }
                    qstatq_list.append(qstatq_values)

        # Calculate totals from squeue data
        all_values = self.slurm_stat_maker.extract_squeue(self.squeue_file)
        for qstat in all_values:
            if qstat["S"] == "R":
                total_running_jobs += 1
            elif qstat["S"] == "Q":
                total_queued_jobs += 1

        return total_running_jobs, total_queued_jobs, qstatq_list
