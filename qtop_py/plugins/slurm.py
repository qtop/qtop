##
## qtop is a tool to monitor queuing systems - https://github.com/qtop/qtop
##
## SPDX-License-Identifier: MIT
##

try:
    import ujson as json
except ImportError:
    import json
import logging
import re
from qtop_py.serialiser import StatExtractor, GenericBatchSystem
import qtop_py.fileutils as fileutils
import itertools


class SLURMStatExtractor(StatExtractor):
    def __init__(self, config, options):
        StatExtractor.__init__(self, config, options)
        self.user_q_search = (
            r"^(?P<job_id>\d+)\s+"
            r"(?P<partition>[\w-]+)\s+"
            r"(?P<name>[\w%.=+/{}*-]+)\s+"
            r"(?P<user>[A-Za-z0-9.*]+)\s+"
            r"(?P<state>PD|R|CA|CF|CG|CD|F|NF|TO)\s+"
            r"(?P<time>\d+-\d+:\d+:\d+|\d+:\d+:\d+)\s+"
            r"(?P<nodes>\d+)"
        )

    def extract_squeue(self, orig_file):
        """
        reads squeue.txt and parses the output file
        returns data in format:
        [
            {
                "JobId": "1234",
                "JobName": "My Job",
                "Partition": "compute",
                "User": "user1",
                "State": "R"
            }
        ]
        """
        try:
            fileutils.check_empty_file(orig_file)
        except fileutils.FileEmptyError:
            logging.error("File %s seems to be empty." % orig_file)
            all_squeue_values = []
        else:
            try:
                with open(orig_file) as f:
                    _ = json.load(f)
            except json.JSONDecodeError:
                logging.info("Extracting squeue output using regex")
                all_squeue_values = self._extract_squeue_regex(orig_file)
            else:
                logging.info("Extracting squeue output using json")
                all_squeue_values = self._extract_squeue_json(orig_file)

        return all_squeue_values

    def _extract_squeue_regex(self, squeue_file):
        all_squeue_values = []
        with open(squeue_file, "r") as fin:
            fin.readline()  # header
            for line in fin:
                squeue_values = self._process_squeue_line(self.user_q_search, line)
                all_squeue_values.append(squeue_values)
        return all_squeue_values

    def _extract_squeue_json(self, squeue_file):
        all_squeue_values = []
        with open(squeue_file, "r") as fin:
            data = json.load(fin)
            jobs = data["jobs"]
            for job in jobs:
                squeue_values = {
                    "JobId": job["job_id"],
                    "Partition": job["partition"],
                    "JobName": job["name"],
                    "User": job["user"],
                    "State": job["state"]
                }
                all_squeue_values.append(squeue_values)
        return all_squeue_values

    def extract_sinfo(self, orig_file):
        """
        reads sinfo.txt and parses the data
        returns data in format:
        [
            {
                "partition": "compute",
                "state": "up",
                "nodes": 10
            }
        ]
        """
        try:
            fileutils.check_empty_file(orig_file)
        except fileutils.FileEmptyError:
            logging.error("File %s seems to be empty." % orig_file)
            all_sinfo_values = []
        else:
            try:
                with open(orig_file) as f:
                    _ = json.load(f)
            except json.JSONDecodeError:
                logging.info("Extracting sinfo output using regex")
                all_sinfo_values = self._extract_sinfo_regex(orig_file)
            else:
                logging.info("Extracting sinfo output using json")
                all_sinfo_values = self._extract_sinfo_json(orig_file)

        return all_sinfo_values

    def _extract_sinfo_regex(self, sinfo_file):
        partition_search = r"^(?P<partition>[\w-]+)\s+(?P<state>\w+)\s+(?P<nodes>\d+)"
        all_sinfo_values = []
        with open(sinfo_file, "r") as fin:
            for line in fin:
                match = re.search(partition_search, line)
                if match:
                    sinfo_values = {
                        "partition": match.group("partition"),
                        "state": match.group("state"),
                        "nodes": match.group("nodes")
                    }
                    all_sinfo_values.append(sinfo_values)
        return all_sinfo_values

    def _extract_sinfo_json(self, sinfo_file):
        all_sinfo_values = []
        with open(sinfo_file, "r") as fin:
            data = json.load(fin)
            partitions = data["partitions"]
            for partition in partitions:
                sinfo_values = {
                    "partition": partition["partition"],
                    "state": partition["state"],
                    "nodes": partition["nodes"]
                }
                all_sinfo_values.append(sinfo_values)
        return all_sinfo_values


class SLURMBatchSystem(GenericBatchSystem):
    @staticmethod
    def get_mnemonic():
        return "slurm"

    def __init__(self, scheduler_output_filenames, config, options):
        self.scontrol_file = scheduler_output_filenames.get("scontrol_file")
        self.squeue_file = scheduler_output_filenames.get("squeue_file")
        self.sinfo_file = scheduler_output_filenames.get("sinfo_file")

        self.config = config
        self.options = options
        self.squeue_maker = SLURMStatExtractor(self.config, self.options)

    def get_worker_nodes(self, job_ids, job_queues, options):
        try:
            fileutils.check_empty_file(self.scontrol_file)
        except fileutils.FileEmptyError:
            all_slurm_values = []
            return all_slurm_values

        raw_blocks = self._read_all_blocks(self.scontrol_file)
        all_slurm_values = []
        anonymize = self.squeue_maker.anonymize_func()
        for block in raw_blocks:
            slurm_values = {"domainname": block["NodeName"]}
            slurm_values["state"] = block["State"]

            slurm_values["cpus"] = block.get("CPUs", 0)
            slurm_values["gpus"] = block.get("Gres", "0")

            jobs = block.get("Jobs", "")
            job_core_map = dict(self._get_jobs_cores(jobs))
            slurm_values["core_job_map"] = job_core_map

            all_slurm_values.append(slurm_values)

        return all_slurm_values

    def get_jobs_info(self):
        job_ids, usernames, job_states, partition_names = [], [], [], []
        squeue_data = self.squeue_maker.extract_squeue(self.squeue_file)
        for job in squeue_data:
            job_ids.append(job["JobId"])
            usernames.append(job["User"])
            job_states.append(job["State"])
            partition_names.append(job["Partition"])

        return job_ids, usernames, job_states, partition_names

    def get_partitions_info(self):
        sinfo_data = self.squeue_maker.extract_sinfo(self.sinfo_file)
        return sinfo_data

    @staticmethod
    def _get_jobs_cores(jobs):
        for job in jobs.split(","):
            job_id, core = job.split("/")
            yield core, job_id

    def _read_all_blocks(self, orig_file):
        with open(orig_file, mode="r") as fin:
            result = []
            reading = True
            while reading:
                block = self._read_block(fin)
                if block:
                    result.append(block)
                else:
                    reading = False
        return result

    @staticmethod
    def _read_block(fin):
        line = fin.readline().strip()
        if not line:
            return None

        block = {"NodeName": line}
        while (line := fin.readline()) != "\n":
            if line.strip():
                key, value = line.split("=", 1)
                block[key.strip()] = value.strip()
        return block

