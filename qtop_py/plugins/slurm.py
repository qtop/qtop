##
## qtop is a tool to monitor queuing systems - https://github.com/qtop/qtop
##
## SPDX-License-Identifier: MIT
##

import re
from qtop_py.serialiser import StatExtractor, GenericBatchSystem
import qtop_py.fileutils as fileutils


class SlurmStatExtractor(StatExtractor):
    def __init__(self, config, options):
        StatExtractor.__init__(self, config, options)

    def _read_nonempty_lines(self, path):
        fileutils.check_empty_file(path)
        with open(path, "r") as handle:
            return [line.strip() for line in handle if line.strip()]

    def extract_squeue(self, path):
        lines = self._read_nonempty_lines(path)
        jobs = []
        for line in lines:
            # Expected test fixture format:
            # JOBID|USER|STATE|PARTITION
            parts = line.split("|")
            if len(parts) != 4:
                continue
            job_id, user, state, partition = parts
            jobs.append(
                {
                    "JobId": self.anonymize(job_id, "jobnums"),
                    "UnixAccount": self.anonymize(user, "users"),
                    "S": state,
                    "Queue": self.anonymize(partition, "qs"),
                }
            )
        return jobs

    def extract_sinfo(self, path):
        lines = self._read_nonempty_lines(path)
        queue_rows = []
        for line in lines:
            # Expected test fixture format:
            # PARTITION|RUNNING|PENDING|STATE
            parts = line.split("|")
            if len(parts) != 4:
                continue
            partition, running, pending, state = parts
            queue_rows.append(
                {
                    "queue_name": self.anonymize(partition, "qs"),
                    "run": running,
                    "queued": pending,
                    "state": state,
                    "lm": "0",
                }
            )
        return queue_rows

    def extract_scontrol_nodes(self, path):
        lines = self._read_nonempty_lines(path)
        workers = []
        node_re = re.compile(r"NodeName=(?P<node>[^|]+)\|State=(?P<state>[^|]+)\|CPUTot=(?P<cpus>\d+)")
        for line in lines:
            match = node_re.search(line)
            if not match:
                continue
            workers.append(
                {
                    "domainname": self.anonymize(match.group("node"), "wns"),
                    "state": match.group("state")[0].lower(),
                    "np": match.group("cpus"),
                    "core_job_map": {},
                    "qname": [],
                }
            )
        return workers


class SlurmBatchSystem(GenericBatchSystem):
    @staticmethod
    def get_mnemonic():
        return "slurm"

    def __init__(self, scheduler_output_filenames, config, options):
        self.squeue_file = scheduler_output_filenames.get("squeue_file")
        self.sinfo_file = scheduler_output_filenames.get("sinfo_file")
        self.scontrol_file = scheduler_output_filenames.get("scontrol_file")
        self.stat = SlurmStatExtractor(config, options)

    def get_jobs_info(self):
        values = self.stat.extract_squeue(self.squeue_file)
        return [v["JobId"] for v in values], [v["UnixAccount"] for v in values], [v["S"] for v in values], [v["Queue"] for v in values]

    def get_queues_info(self):
        values = self.stat.extract_sinfo(self.sinfo_file)
        total_running = sum(int(v["run"]) for v in values)
        total_queued = sum(int(v["queued"]) for v in values)
        return total_running, total_queued, values

    def get_worker_nodes(self, job_ids, job_queues, options):
        return self.stat.extract_scontrol_nodes(self.scontrol_file)
