##
## qtop is a tool to monitor queuing systems - https://github.com/qtop/qtop
##
## Copyright (c) 2016 Fotis Georgatos
## Copyright (c) 2016 Sotiris Fragkiskos
##
## SPDX-License-Identifier: MIT
##

import logging
import re
from qtop_py.serialiser import StatExtractor, GenericBatchSystem
import qtop_py.fileutils as fileutils


class SlurmStatExtractor(StatExtractor):
    """Parses Slurm squeue / sinfo output files."""

    def extract_squeue(self, orig_file):
        """
        Reads squeue.txt (produced by: squeue --noheader --format='%i|%u|%T|%P|%j')
        Returns list of dicts:
          [{"JobId": "123", "UnixAccount": "alice", "S": "R", "Queue": "normal", "JobName": "myjob"}, ...]

        Slurm states are mapped to single-letter codes used by qtop:
          RUNNING  -> R
          PENDING  -> Q
          SUSPENDED-> S
          COMPLETING->C
          others   -> H  (held / unknown)
        """
        state_map = {
            "RUNNING": "R",
            "PENDING": "Q",
            "SUSPENDED": "S",
            "COMPLETING": "C",
            "COMPLETED": "C",
            "FAILED": "F",
            "CANCELLED": "X",
            "TIMEOUT": "X",
        }

        all_values = []
        try:
            fileutils.check_empty_file(orig_file)
        except fileutils.FileEmptyError:
            logging.error("File %s is empty." % orig_file)
            return all_values

        anonymize = self.anonymize_func()
        with open(orig_file, "r") as fin:
            for line in fin:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                parts = line.split("|")
                if len(parts) < 5:
                    logging.warning("Skipping malformed squeue line: %r", line)
                    continue
                job_id, user, state_raw, partition, job_name = parts[:5]
                user = user if not self.options.ANONYMIZE else anonymize(user, "users")
                state = state_map.get(state_raw.upper(), "H")
                all_values.append({
                    "JobId": job_id.strip(),
                    "UnixAccount": user.strip(),
                    "S": state,
                    "Queue": partition.strip(),
                    "JobName": job_name.strip(),
                })
        return all_values

    def extract_squeue_queues(self, orig_file):
        """
        Parses the same squeue file to build queue summary info
        (mirrors extract_qstatq).  Returns:
          [..., {"queue_name": "normal", "run": 10, "queued": 5, "lm": "--", "state": "E R"},
                {"Total_running": N, "Total_queued": M}]
        """
        raw = self.extract_squeue(orig_file)
        from collections import defaultdict
        queues = defaultdict(lambda: {"run": 0, "queued": 0})
        for job in raw:
            q = job["Queue"]
            if job["S"] == "R":
                queues[q]["run"] += 1
            elif job["S"] == "Q":
                queues[q]["queued"] += 1

        result = []
        total_run = total_queued = 0
        for qname, counts in sorted(queues.items()):
            result.append({
                "queue_name": qname,
                "run": counts["run"],
                "queued": counts["queued"],
                "lm": "--",
                "state": "E R",
            })
            total_run += counts["run"]
            total_queued += counts["queued"]
        result.append({"Total_running": total_run, "Total_queued": total_queued})
        return result


class SlurmBatchSystem(GenericBatchSystem):
    """Slurm batch system plugin for qtop."""

    @staticmethod
    def get_mnemonic():
        return "slurm"

    def __init__(self, scheduler_output_filenames, config, options):
        self.squeue_file = scheduler_output_filenames.get("squeue_file")
        self.sinfo_file = scheduler_output_filenames.get("sinfo_file")
        self.config = config
        self.options = options
        self.stat_maker = SlurmStatExtractor(config, options)

    # ------------------------------------------------------------------
    # Required GenericBatchSystem interface
    # ------------------------------------------------------------------

    def get_jobs_info(self):
        """Returns (job_ids, usernames, job_states, queue_names)."""
        job_ids, usernames, job_states, queue_names = [], [], [], []
        jobs = self.stat_maker.extract_squeue(self.squeue_file)
        for job in jobs:
            job_ids.append(job["JobId"])
            usernames.append(job["UnixAccount"])
            job_states.append(job["S"])
            queue_names.append(job["Queue"])
        logging.debug(
            "Slurm jobs parsed: %d running+queued", len(job_ids)
        )
        return job_ids, usernames, job_states, queue_names

    def get_queues_info(self):
        """Returns (total_running, total_queued, queue_list)."""
        qstatqs = self.stat_maker.extract_squeue_queues(self.squeue_file)
        queue_list = []
        total_running = total_queued = 0
        for item in qstatqs:
            if "Total_running" in item:
                total_running = item["Total_running"]
                total_queued = item["Total_queued"]
            else:
                queue_list.append(item)
        return int(total_running), int(total_queued), queue_list

    def get_worker_nodes(self, job_ids, job_queues, options):
        """
        Reads sinfo.txt (produced by:
          sinfo --noheader --format='%n|%t|%c|%G|%O' --Node)
        where %n=nodename, %t=state, %c=cpus, %G=gres, %O=cpu_load.

        Also reads squeue to build the core->job mapping per node
        (squeue --noheader --format='%i|%u|%T|%P|%j|%R' where %R=nodelist).
        """
        all_wn_values = []
        try:
            fileutils.check_empty_file(self.sinfo_file)
        except fileutils.FileEmptyError:
            logging.error("sinfo file %s is empty." % self.sinfo_file)
            return all_wn_values

        # Build node->job list from squeue nodelist column
        node_job_map = self._build_node_job_map()

        anonymize = self.stat_maker.anonymize_func()
        state_map = {
            "alloc": "R",
            "allocated": "R",
            "idle": "-",
            "mixed": "R",
            "down": "d",
            "drain": "d",
            "drained": "d",
            "draining": "d",
            "fail": "o",
            "offline": "o",
            "unknown": "?",
            "maint": "d",
        }

        with open(self.sinfo_file, "r") as fin:
            for line in fin:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                parts = line.split("|")
                if len(parts) < 3:
                    logging.warning("Skipping malformed sinfo line: %r", line)
                    continue
                nodename = parts[0].strip()
                state_raw = parts[1].strip().rstrip("*")  # strip drain marker
                ncpus_str = parts[2].strip()

                domainname = nodename if not options.ANONYMIZE else anonymize(nodename, "wns")
                state = state_map.get(state_raw.lower(), "?")

                try:
                    np = int(ncpus_str)
                except ValueError:
                    np = 0

                jobs_on_node = node_job_map.get(nodename, [])
                core_job_map = {}
                for core_idx, job_id in enumerate(jobs_on_node):
                    core_job_map[str(core_idx)] = job_id

                wn = {
                    "domainname": domainname,
                    "state": state,
                    "np": str(np),
                    "core_job_map": core_job_map,
                }
                all_wn_values.append(wn)

        all_wn_values = self.ensure_worker_nodes_have_qnames(
            all_wn_values, job_ids, job_queues
        )
        return all_wn_values

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _build_node_job_map(self):
        """
        Parse squeue to map nodename -> [job_id, ...].
        Handles simple nodenames and compact Slurm nodelist expressions
        like node[01-03] -> node01, node02, node03.
        """
        node_job_map = {}
        try:
            fileutils.check_empty_file(self.squeue_file)
        except fileutils.FileEmptyError:
            return node_job_map

        with open(self.squeue_file, "r") as fin:
            for line in fin:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                parts = line.split("|")
                if len(parts) < 6:
                    continue
                job_id = parts[0].strip()
                state_raw = parts[2].strip().upper()
                nodelist = parts[5].strip() if len(parts) > 5 else ""
                if state_raw != "RUNNING" or not nodelist or nodelist in ("(None)", "N/A", ""):
                    continue
                for node in self._expand_nodelist(nodelist):
                    node_job_map.setdefault(node, []).append(job_id)
        return node_job_map

    @staticmethod
    def _expand_nodelist(nodelist):
        """
        Expand compact Slurm nodelist expressions.
        e.g. 'node[01-03,05]' -> ['node01', 'node02', 'node03', 'node05']
        Plain names are returned as-is.
        """
        # Plain nodename (no brackets)
        if "[" not in nodelist:
            return [nodelist]

        nodes = []
        # Handle comma-separated groups: node[01-02],gpu[01]
        # Split on '],' boundary
        segments = re.split(r"\],?", nodelist)
        for segment in segments:
            segment = segment.strip().rstrip(",")
            if not segment:
                continue
            if "[" in segment:
                prefix, range_str = segment.split("[", 1)
                for part in range_str.split(","):
                    part = part.strip()
                    if "-" in part:
                        start, end = part.split("-")
                        width = len(start)
                        for i in range(int(start), int(end) + 1):
                            nodes.append("%s%s" % (prefix, str(i).zfill(width)))
                    else:
                        nodes.append("%s%s" % (prefix, part))
            else:
                nodes.append(segment)
        return nodes
