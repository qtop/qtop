##
## qtop is a tool to monitor queuing systems - https://github.com/qtop/qtop
##
## Copyright (c) 2016 Fotis Georgatos
## Copyright (c) 2016 Sotiris Fragkiskos
## Copyright (c) 2023 Hewlett Packard Enterprise Development LP
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


class PBSStatExtractor(StatExtractor):
    def __init__(self, config, options):
        StatExtractor.__init__(self, config, options)
        self.user_q_search = (
            r"^(?P<host_name>(?P<job_id>[0-9\[\]-]+)\.(?P<domain>[\w*-]+))\s+"
            r"(?P<name>[\w%.=+/{}*-]+)\s+"
            r"(?P<user>[A-Za-z0-9.*]+)\s+"
            r"(?P<time>\d+:\d*:?\d*\*?|0)\s+"
            r"(?P<state>[BCEFHMQRSTUWX])\s+"
            r"(?P<queue_name>\w+)"
        )

        self.user_q_search_prior = (
            r"\s{0,2}"
            r"(?P<job_id>\d+)\s+"
            r"(?:[0-9]\.[0-9]+)\s+"
            r"(?:[\w.-]+)\s+"
            r"(?P<user>[\w.-]+)\s+"
            r"(?P<state>[a-z])\s+"
            r"(?:\d{2}/\d{2}/\d{2}|0)\s+"
            r"(?:\d+:\d+:\d*|0)\s+"
            r"(?P<queue_name>\w+@[\w.-]+)\s+"
            r"(?:\d+)\s+"
            r"(?:\w*)"
        )

    def extract_qstat(self, orig_file):
        """
        reads qstat.txt and parses the output file
        the data is returned in the following format:
        [
            {
                "JobId": "1234",
                "JobName: "My Job",
                "Queue": "workq",
                "UnixAccount": "user1",
                "S": "Q"
            }
        ]
        """
        try:
            fileutils.check_empty_file(orig_file)
        except fileutils.FileEmptyError:
            logging.error("File %s seems to be empty." % orig_file)
            all_qstat_values = []
        else:
            try:
                with open(orig_file) as f:
                    _ = json.load(f)
            except json.JSONDecodeError:
                logging.info("Extracting qstat output using regex")
                all_qstat_values = self._extract_qstat_regex(orig_file)
            else:
                logging.info("Extracting qstat output using json")
                all_qstat_values = self._extract_qstat_json(orig_file)

        return all_qstat_values

    def _extract_qstat_regex(self, qstat_file):
        all_qstat_values = list()
        with open(qstat_file, "r") as fin:
            _ = fin.readline()  # header
            fin.readline()  # horizontal row
            line = fin.readline()  # first line
            re_match_positions = ("job_id", "user", "state", "queue_name")  # was: (1, 5, 7, 8), (1, 4, 5, 8)
            try:  # first qstat line determines which format qstat follows.
                re_search = self.user_q_search
                qstat_values = self._process_qstat_line(re_search, line, re_match_positions)
                # unused: _job_nr, _ce_name, _name, _time_use = m.group(2), m.group(3), m.group(4), m.group(6)
            except AttributeError:  # this means 'prior' exists in qstat, it's another format
                re_search = self.user_q_search_prior
                qstat_values = self._process_qstat_line(re_search, line, re_match_positions)
                # unused:  _prior, _name, _submit, _start_at, _queue_domain, _slots, _ja_taskID =
                # m.group(2), m.group(3), m.group(6), m.group(7), m.group(9), m.group(10), m.group(11)
            finally:
                all_qstat_values.append(qstat_values)

            # hence the rest of the lines should follow either try's or except's same format
            for line in fin:
                qstat_values = self._process_qstat_line(re_search, line, re_match_positions)
                all_qstat_values.append(qstat_values)

        return all_qstat_values

    def _extract_qstat_json(self, qstat_file):
        all_qstat_values = list()
        with open(qstat_file, "r") as fin:
            data = json.load(fin)
            jobs = data["Jobs"]
            for job_id, job in jobs.items():
                qstat_values = dict()
                user = job["Job_Owner"].split("@")[0]
                user = self.anonymize(user, "users")
                qstat_values["JobId"] = job_id.split(".")[0]
                qstat_values["Queue"] = job["queue"]
                qstat_values["JobName"] = job["Job_Name"]
                qstat_values["UnixAccount"] = user
                qstat_values["S"] = job["job_state"]
                all_qstat_values.append(qstat_values)
        return all_qstat_values

    def extract_qstatq(self, orig_file):
        """
        reads qstat_q.txt and parses the data
        returns the data in the following format:
        [
            {
                "queue_name": "worq",
                "run": "63",
                "queued": "4",
                "lm": "",
                "state": "E"
            }
        ]
        """
        try:
            fileutils.check_empty_file(orig_file)
        except fileutils.FileEmptyError:
            logging.error("File %s seems to be empty." % orig_file)
            all_qstatq_values = []
        else:
            try:
                with open(orig_file) as f:
                    _ = json.load(f)
            except json.JSONDecodeError:
                logging.info("Extracting qstat_q output using regex")
                all_qstatq_values = self._extract_qstatq_regex(orig_file)
            else:
                logging.info("Extracting qstat_q output using json")
                all_qstatq_values = self._extract_qstatq_json(orig_file)

        return all_qstatq_values

    def _extract_qstatq_regex(self, qstatq_file):
        anonymize = self.anonymize_func()
        queue_search = (
            r"^(?P<queue_name>[\w.-]+)\s+"
            r"(?:--|[0-9]+[mgtkp]b[a-z]*)\s+"
            r"(?:--|\d+:\d+:?\d*:?)\s+"
            r"(?:--|\d+:\d+:?\d+:?)\s+(--)\s+"
            r"(?P<run>\d+)\s+"
            r"(?P<queued>\d+)\s+"
            r"(?P<lm>--|\d+)\s+"
            r"(?P<state>[DE] [RS])"
        )
        run_qd_search = r"^\s*(?P<tot_run>\d+)\s+(?P<tot_queued>\d+)"  # this picks up the last line contents

        all_qstatq_values = list()
        with open(qstatq_file, "r") as fin:
            fin.readline()
            fin.readline()
            # server_name = fin.next().split(': ')[1].strip()
            fin.readline()
            fin.readline()  # .strip()  # the headers line should later define the keys in temp_dict, should they be different
            fin.readline()
            for line in fin:
                line = line.strip()
                m = re.search(queue_search, line)
                n = re.search(run_qd_search, line)
                temp_dict = {}
                try:
                    queue_name = m.group("queue_name") if not self.options.ANONYMIZE else anonymize(m.group("queue_name"), "qs")
                    run, queued, lm, state = m.group("run"), m.group("queued"), m.group("lm"), m.group("state")
                except AttributeError:
                    try:
                        total_running_jobs, total_queued_jobs = n.group("tot_run"), n.group("tot_queued")
                    except AttributeError:
                        continue
                else:
                    for key, value in [("queue_name", queue_name), ("run", run), ("queued", queued), ("lm", lm), ("state", state)]:
                        temp_dict[key] = value
                    all_qstatq_values.append(temp_dict)
            all_qstatq_values.append({"Total_running": total_running_jobs, "Total_queued": total_queued_jobs})

        return all_qstatq_values

    def _extract_qstatq_json(self, qstatq_file):
        anonymize = self.anonymize_func()
        all_qstatq_values = list()
        with open(qstatq_file, "r") as fin:
            data = json.load(fin)
            queues = data["Queue"]
            for queue_name, queue in queues.items():
                qstatq_values = dict()
                queue_name = queue_name if not self.options.ANONYMIZE else anonymize(queue_name)
                qstatq_values["queue_name"] = queue_name
                qstatq_values["run"] = queue["state_count"].split(" ")[4].split(":")[1]
                qstatq_values["queued"] = queue["state_count"].split(" ")[1].split(":")[1]
                qstatq_values["lm"] = "--"  # TODO: find value in json
                qstatq_values["state"] = "E" if queue["enabled"] == "True" else "D"
                all_qstatq_values.append(qstatq_values)
            total_running_jobs = sum([int(item["run"]) for item in all_qstatq_values])
            total_queued_jobs = sum([int(item["queued"]) for item in all_qstatq_values])
            all_qstatq_values.append({"Total_running": total_running_jobs, "Total_queued": total_queued_jobs})
        return all_qstatq_values


class PBSBatchSystem(GenericBatchSystem):
    @staticmethod
    def get_mnemonic():
        return "pbs"

    def __init__(self, scheduler_output_filenames, config, options):
        self.pbsnodes_file = scheduler_output_filenames.get("pbsnodes_file")
        self.qstat_file = scheduler_output_filenames.get("qstat_file")
        self.qstatq_file = scheduler_output_filenames.get("qstatq_file")

        self.config = config
        self.options = options
        self.qstat_maker = PBSStatExtractor(self.config, self.options)

    def get_worker_nodes(self, job_ids, job_queues, options):
        try:
            fileutils.check_empty_file(self.pbsnodes_file)
        except fileutils.FileEmptyError:
            all_pbs_values = []
            return all_pbs_values

        raw_blocks = self._read_all_blocks(self.pbsnodes_file)
        all_pbs_values = []
        anonymize = self.qstat_maker.anonymize_func()
        for block in raw_blocks:
            pbs_values = dict()
            pbs_values["domainname"] = block["domainname"] if not self.options.ANONYMIZE else anonymize(block["domainname"], "wns")

            nextchar = block["state"][0]
            state = (nextchar == "f") and "-" or nextchar

            pbs_values["state"] = state

            # find attribute for number of cores, default is 0
            if block.get("np"):
                pbs_values["np"] = block["np"]
            elif block.get("pcpus"):
                pbs_values["np"] = block["pcpus"]
            else:
                pbs_values["np"] = block.get("resources_available.ncpus", 0)

            if block.get("gpus", 0) > 0:  # this should be rare.
                pbs_values["gpus"] = block["gpus"]

            try:  # this should turn up more often, hence the try/except.
                _ = block["jobs"]
            except KeyError:
                pbs_values["core_job_map"] = dict()  # change of behaviour: all entries should contain the key even if no value
            else:
                # jobs = re.split(r'(?<=[A-Za-z0-9]),\s?', block['jobs'])
                jobs = re.findall(r"[0-9][0-9a-zA-Z\[\],.-]*\/[^,]+", block["jobs"])
                pbs_values["core_job_map"] = dict((core, job) for job, core in self._get_jobs_cores(jobs))
            finally:
                all_pbs_values.append(pbs_values)

        all_pbs_values = self.ensure_worker_nodes_have_qnames(all_pbs_values, job_ids, job_queues)
        return all_pbs_values

    def get_jobs_info(self):
        """
        reads qstat YAML/json file and populates four lists. Returns the lists
        ex read_qstat_yaml
        Common for PBS, OAR, SGE
        """
        job_ids, usernames, job_states, queue_names = [], [], [], []

        qstats = self.qstat_maker.extract_qstat(self.qstat_file)
        for qstat in qstats:
            job_ids.append(re.sub(r"\[\]$", "", str(qstat["JobId"])))
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
        Parses the generated qstatq yaml/json file and extracts
        the information necessary for building the
        user accounts and pool mappings table.
        """
        qstatq_list = []
        qstatqs_total = self.qstat_maker.extract_qstatq(self.qstatq_file)

        for qstatq in qstatqs_total[:-1]:
            qstatq_list.append(qstatq)
        for _total in qstatqs_total[-1:]:  # this is at most one item
            total_running_jobs, total_queued_jobs = _total["Total_running"], _total["Total_queued"]
            break
        else:
            total_running_jobs, total_queued_jobs = 0, 0

        return int(eval(str(total_running_jobs))), int(eval(str(total_queued_jobs))), qstatq_list

    @staticmethod
    def _get_jobs_cores(jobs):  # block['jobs']
        """
        Generator that takes job ids in this format (for Torque):
        '0/10102182.f-batch01.grid.sinica.edu.tw, 1/10102106.f-batch01.grid.sinica.edu.tw, 2/10102339.f-batch01.grid.sinica.edu.tw, 3/10104007.f-batch01.grid.sinica.edu.tw'
        or this format (for PBS Pro):
        '2257887.cluster-pbs5/0, 2257887.cluster-pbs5/1, 2257887.cluster-pbs5/2, 2257887.cluster-pbs5/3, 2257887.cluster-pbs5/4'
        and spits tuples of the format (0, 10102182)    (job,core)
        """
        for core_job in jobs:
            part1, part2 = core_job.strip().split("/")
            if re.search(r"^\d+[,-]?[\d,-]*$", part1):  # Torque job id
                core, job = part1, part2
            elif re.match(r"[\w.-]+", part1):  # PBS Pro job id
                job, core = part1, part2

            if ("," in core) or ("-" in core):  # job id with subjobs
                for subcore, subjob in PBSBatchSystem.get_corejob_from_range(core, job):
                    subjob = subjob.strip().split("/")[0].split(".")[0]
                    yield subjob, subcore  # TODO: int or no int?
            else:  # job id without subjobs
                job = job.strip().split("/")[0].split(".")[0]
                job = re.sub(r"\[\d*\]$", "", job)
                yield job, core

    def _read_all_blocks(self, orig_file):
        """
        reads pbsnodes txt file block by block
        """
        with open(orig_file, mode="r") as fin:
            result = []
            reading = True
            while reading:
                wn_block = self._read_block(fin)
                if wn_block:
                    result.append(wn_block)
                else:
                    reading = False
        return result

    @staticmethod
    def _read_block(fin):
        domain_name = fin.readline().strip()
        if not domain_name:
            return None

        block = {"domainname": domain_name}
        reading = True
        while reading:
            line = fin.readline()
            if line == "\n":
                reading = False
            else:
                try:
                    key, value = line.split(" = ")
                except ValueError:  # e.g. if line is 'jobs =' with no jobs
                    pass
                else:
                    block[key.strip()] = value.strip()
        return block

    @staticmethod
    def get_corejob_from_range(core_selections, job):
        _cores = list()
        subselections = core_selections.split(",")
        for subselection in subselections:
            if "-" in subselection:
                range_ = list(map(int, subselection.split("-")))
                range_[-1] += 1
                _cores.extend([map(str, range(*range_))])
            else:
                _cores.append([subselection])
        all_cores = list(itertools.chain.from_iterable(_cores))
        for core in all_cores:
            yield core, job
