##
## qtop is a tool to monitor queuing systems - https://github.com/qtop/qtop
##
## Copyright (c) 2016 Fotis Georgatos
## Copyright (c) 2016 Sotiris Fragkiskos
## Copyright (c) 2023 Hewlett Packard Enterprise Development LP
## Copyright (c) 2026 Jacob Hatchett
##
## SPDX-License-Identifier: MIT
##
"""PBS/Torque and PBS Pro backend: parses pbsnodes, qstat and qstat -q output."""

try:
    import ujson as json
except ImportError:
    import json
import itertools
import logging
import re
from typing import TYPE_CHECKING, Any, Dict, Iterable, Iterator, List, Optional, TextIO, Tuple, Union, cast

import qtop_py.fileutils as fileutils
from qtop_py.safety import load_json
from qtop_py.schedulers.tabular import TabularStatExtractor
from qtop_py.serialiser import GenericBatchSystem, StatExtractor, resolve_job_id

if TYPE_CHECKING:
    from qtop_py.source_trace import TraceMasker
    from qtop_py.state_types import Config, JobsInfo, Node, Options, QueuesInfo, RawQueue

# NQS request states mapped onto PBS single-letter job states.
NQS_REQUEST_STATES = {
    "EXITING": "E",
    "HELD": "H",
    "QUEUED": "Q",
    "RUNNING": "R",
    "SUSPENDED": "S",
    "TRANSITING": "T",
    "WAITING": "W",
}


class PBSStatExtractor(TabularStatExtractor):
    """Parse qstat and qstat -q output, tabular or JSON."""

    def __init__(self, config: "Config", options: "Options") -> None:
        StatExtractor.__init__(self, config, options)
        self.user_q_search = (
            r"^(?P<host_name>(?P<job_id>[0-9\[\]-]+)\.(?P<domain>[\w.*-]+))\s+"
            r"(?P<name>.+?)\s+"
            r"(?P<user>[A-Za-z0-9._*-]+)\s+"
            r"(?P<time>\d+:\d*:?\d*\*?|0)\s+"
            r"(?P<state>[A-Za-z])\s+"
            r"(?P<queue_name>[\w.-]+)(?=\s|$)"
        )

        self.user_q_search_prior = (
            r"\s{0,2}"
            r"(?P<job_id>\d+)\s+"
            r"(?:[0-9]\.[0-9]+)\s+"
            r"(?:[\w.-]+)\s+"
            r"(?P<user>[\w.-]+)\s+"
            r"(?P<state>[A-Za-z]+)\s+"
            r"(?:\d{2}/\d{2}/\d{2,4}|0)\s+"
            r"(?:\d+:\d+:\d*|0)\s+"
            r"(?:(?P<queue_name>[\w.-]+@[\w.-]+)\s+)?"  # blank for pending (qw) jobs
            r"(?:\d+)(?=\s|$)"
        )

        # Legacy NQS-style qstat: "<queue>;  type=BATCH; ..." headers precede their request rows.
        self.nqs_queue_header = r"^(?P<queue_name>[\w.-]+);\s+type=\w+;"
        self.nqs_request = r"^\d+:\s+(?P<name>\S+)\s+(?P<job_id>\d+)\s+(?P<user>[\w.-]+)\s+(?P<state>[A-Z]+)$"

    def extract_qstat(self, orig_file: str) -> List[Dict[str, Any]]:
        """Parse qstat output into job records of this form.

        [{"JobId": "1234", "JobName": "My Job", "Queue": "workq", "UnixAccount": "user1", "S": "Q"}]
        """
        try:
            fileutils.check_empty_file(orig_file)
        except fileutils.FileEmptyError:
            logging.error("File %s seems to be empty." % orig_file)
            all_qstat_values = []
        else:
            try:
                with open(orig_file, encoding="utf-8", errors="replace") as f:
                    _ = load_json(f)
            except json.JSONDecodeError:
                logging.info("Extracting qstat output using regex")
                all_qstat_values = self._extract_qstat_regex(orig_file)
            else:
                logging.info("Extracting qstat output using json")
                all_qstat_values = self._extract_qstat_json(orig_file)

        return all_qstat_values

    def _extract_qstat_regex(self, qstat_file: str) -> List[Dict[str, Any]]:
        all_qstat_values = list()
        with open(qstat_file, "r", encoding="utf-8", errors="replace") as fin:
            re_match_positions = ("job_id", "user", "state", "queue_name")  # was: (1, 5, 7, 8), (1, 4, 5, 8)

            pattern = None
            nqs_queue = None
            for line in fin:
                if not line.strip():
                    continue

                if pattern in (None, self.nqs_request):
                    header = re.match(self.nqs_queue_header, line.strip())
                    if header:
                        pattern, nqs_queue = self.nqs_request, header.group("queue_name")
                        continue

                if pattern == self.nqs_request:
                    qstat_values = self._process_nqs_request_line(line, nqs_queue)
                    if qstat_values:
                        all_qstat_values.append(qstat_values)
                    continue

                if pattern is None:
                    for candidate in (self.user_q_search, self.user_q_search_prior):
                        try:
                            qstat_values = self._process_qstat_line(candidate, line, re_match_positions)
                        except AttributeError:
                            continue
                        else:
                            pattern = candidate
                            all_qstat_values.append(self._default_pending_queue(qstat_values))
                            break
                    continue

                try:
                    qstat_values = self._process_qstat_line(pattern, line, re_match_positions)
                except AttributeError:
                    continue
                all_qstat_values.append(self._default_pending_queue(qstat_values))

        return all_qstat_values

    @staticmethod
    def _default_pending_queue(qstat_values: Dict[str, str]) -> Dict[str, str]:
        """SGE-style -prior output leaves the queue blank for pending jobs; label them as the SGE plugin does."""
        if not qstat_values["Queue"]:
            qstat_values["Queue"] = "Pending"
        return qstat_values

    def _process_nqs_request_line(self, line: str, queue_name: Optional[str]) -> Optional[Dict[str, Any]]:
        """Parse one NQS request row; summary ("1 run; 0 wait;") and column-header lines yield None."""
        m = re.match(self.nqs_request, line.strip())
        if m is None:
            if re.match(r"^\d+:", line.strip()):
                logging.warning("Line: %s not properly parsed as an NQS request. Skipping." % line.strip())
            return None
        state = NQS_REQUEST_STATES.get(m.group("state"))
        if state is None:
            logging.warning("Unknown NQS request state %s in line: %s. Skipping." % (m.group("state"), line.strip()))
            return None
        return {"JobId": m.group("job_id"), "UnixAccount": self.anonymize(m.group("user"), "users"), "S": state, "Queue": queue_name}

    def _extract_qstat_json(self, qstat_file: str) -> List[Dict[str, Any]]:
        all_qstat_values = list()
        with open(qstat_file, "r", encoding="utf-8", errors="replace") as fin:
            data = load_json(fin)
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

    def extract_qstatq(self, orig_file: str) -> List[Dict[str, Any]]:
        """Parse qstat -q output into queue records of this form.

        [{"queue_name": "workq", "run": "63", "queued": "4", "lm": "", "state": "E"}]
        """
        try:
            fileutils.check_empty_file(orig_file)
        except fileutils.FileEmptyError:
            logging.error("File %s seems to be empty." % orig_file)
            all_qstatq_values = []
        else:
            try:
                with open(orig_file, encoding="utf-8", errors="replace") as f:
                    _ = load_json(f)
            except json.JSONDecodeError:
                logging.info("Extracting qstat_q output using regex")
                all_qstatq_values = self._extract_qstatq_regex(orig_file)
            else:
                logging.info("Extracting qstat_q output using json")
                all_qstatq_values = self._extract_qstatq_json(orig_file)

        return all_qstatq_values

    def _extract_qstatq_regex(self, qstatq_file: str) -> List[Dict[str, Any]]:
        anonymize = self.anonymize_func()
        queue_search = (
            r"^(?P<queue_name>[\w.-]+)\s+"
            r"(?:--|[0-9]+[mgtkp]b[a-z]*)\s+"
            r"(?:--|\d+:\d+:?\d*:?)\s+"
            r"(?:--|\d+:\d+:?\d+:?)\s+(?:--|\d+)\s+"
            r"(?P<run>\d+)\s+"
            r"(?P<queued>\d+)\s+"
            r"(?P<lm>--|\d+)\s+"
            r"(?P<state>[DE]\s+[RS])"
        )
        run_qd_search = r"^\s*(?P<tot_run>\d+)\s+(?P<tot_queued>\d+)"  # this picks up the last line contents

        all_qstatq_values = list()
        total_running_jobs: Union[int, str] = 0  # str once read from the totals line
        total_queued_jobs: Union[int, str] = 0
        with open(qstatq_file, "r", encoding="utf-8", errors="replace") as fin:
            fin.readline()
            fin.readline()
            fin.readline()
            fin.readline()  # .strip()  # the headers line should later define the keys in temp_dict, should they be different
            fin.readline()
            for line in fin:
                line = line.strip()
                m = re.search(queue_search, line)
                n = re.search(run_qd_search, line)
                if m is not None:
                    queue_name = m.group("queue_name") if not self.options.ANONYMIZE else anonymize(m.group("queue_name"), "qs")
                    run, queued, lm, state = m.group("run"), m.group("queued"), m.group("lm"), m.group("state")
                    all_qstatq_values.append({"queue_name": queue_name, "run": run, "queued": queued, "lm": lm, "state": state})
                elif n is not None:
                    total_running_jobs, total_queued_jobs = n.group("tot_run"), n.group("tot_queued")
            all_qstatq_values.append({"Total_running": total_running_jobs, "Total_queued": total_queued_jobs})

        return all_qstatq_values

    def _extract_qstatq_json(self, qstatq_file: str) -> List[Dict[str, Any]]:
        anonymize = self.anonymize_func()
        all_qstatq_values = list()
        with open(qstatq_file, "r", encoding="utf-8", errors="replace") as fin:
            data = load_json(fin)
            queues = data["Queue"]
            for queue_name, queue in queues.items():
                qstatq_values = dict()
                queue_name = queue_name if not self.options.ANONYMIZE else anonymize(queue_name, "qs")
                qstatq_values["queue_name"] = queue_name
                try:
                    pairs = [token.split(":", 1) for token in queue["state_count"].split()]
                    counts = dict(pairs)
                    run, queued = counts["Running"], counts["Queued"]
                    if len(counts) != len(pairs) or not all(re.fullmatch(r"[0-9]+", count) for count in (run, queued)):
                        raise ValueError("invalid or duplicate job counts")
                except (KeyError, ValueError, TypeError, AttributeError) as error:
                    raise ValueError("Invalid PBS state_count for queue %s" % queue_name) from error
                qstatq_values["run"], qstatq_values["queued"] = run, queued
                qstatq_values["lm"] = str(queue.get("max_run", queue.get("max_running", "--")))
                qstatq_values["state"] = "E" if str(queue["enabled"]).lower() == "true" else "D"
                if "started" in queue:
                    qstatq_values["state"] += " R" if str(queue["started"]).lower() == "true" else " S"
                all_qstatq_values.append(qstatq_values)
            total_running_jobs = sum(int(item["run"]) for item in all_qstatq_values)
            total_queued_jobs = sum(int(item["queued"]) for item in all_qstatq_values)
            all_qstatq_values.append({"Total_running": total_running_jobs, "Total_queued": total_queued_jobs})
        return all_qstatq_values


class PBSBatchSystem(GenericBatchSystem):
    """PBS/Torque and PBS Pro backend."""

    classic_report_name = "PBS"
    classic_job_count_source = "reported by qstat - q"
    classic_account_note = "  ('all' also includes those in C and W states, as reported by qstat)"

    @staticmethod
    def _mask_json_trace(key: str, text: str, mask: "TraceMasker") -> Tuple[str, str]:
        """Mask JSON job or queue traces, keeping only known fields."""
        data = json.loads(text)
        container = "Jobs" if key == "qstat_file" else "Queue"
        records = {}
        labels = {"Job_Owner", "Job_Name", "job_state", "queue", "state_count", "enabled", "started", "max_run", "max_running"}
        for name, record in data[container].items():
            result = mask.structured(record, labels)
            if container == "Jobs":
                result["Job_Owner"] = mask(record["Job_Owner"].split("@")[0], "user") + "@" + mask(record["Job_Owner"].split("@")[-1], "node")
                result["queue"] = mask(record["queue"], "queue")
                result["job_state"] = mask.public(record["job_state"], "[A-Z]")
                name = mask(name.split(".")[0], "job") + ".server"
            else:
                for field in ("state_count", "enabled", "started", "max_run", "max_running"):
                    if field in record:
                        value = record[field]
                        if isinstance(value, str):
                            value = mask.public(value, r"(?:(?:Transit|Queued|Held|Waiting|Running|Exiting|Begun):[0-9]+\s*|[0-9]+|[Tt]rue|[Ff]alse|--)+")
                        elif not isinstance(value, (bool, int, float)):
                            raise ValueError("unsupported queue scalar")
                        result[field] = value
                name = mask(name, "queue")
            records[name] = result
        return ("qstat.txt" if container == "Jobs" else "qstat_q.txt"), json.dumps({container: records}, indent=2) + "\n"

    def mask_source_trace(self, key: str, text: str, mask: "TraceMasker") -> Tuple[str, str]:
        """Return the masked name and text of a pbsnodes, qstat or qstat -q trace."""
        if key in ("qstat_file", "qstatq_file") and text.lstrip().startswith("{"):
            return self._mask_json_trace(key, text, mask)
        rows = []
        for index, line in enumerate(text.splitlines()):
            if not line.strip():
                rows.append(line)
                continue
            if key == "qstat_file":
                if index < 2:
                    rows.append("Job id Name User Time Use S Queue" if index == 0 else "---")
                    continue
                match = re.fullmatch(self.qstat_maker.user_q_search + r"\s*", line)
                if not match:
                    raise ValueError("unsupported job row")
                rows.append(
                    "%s.server %s %s %s %s %s"
                    % (
                        mask(match["job_id"], "job"),
                        mask(match["name"]),
                        mask(match["user"], "user"),
                        mask.public(match["time"], r"[0-9:*]+"),
                        mask.public(match["state"], "[A-Z]"),
                        mask(match["queue_name"], "queue"),
                    )
                )
            elif key == "qstatq_file":
                if index < 5:
                    rows.append("server: server" if index == 1 else "")
                    continue
                fields = line.split()
                if len(fields) == 10:
                    fields[0] = mask(fields[0], "queue")
                    for i in range(1, 8):
                        fields[i] = mask.public(fields[i], r"(?:--|[0-9:]+|[0-9]+[mgtkp]b[a-z]*)")
                    fields[8] = mask.public(fields[8], "D|E")
                    fields[9] = mask.public(fields[9], "R|S")
                    rows.append(" ".join(fields))
                elif re.fullmatch(r"\s*[0-9]+\s+[0-9]+\s*", line):
                    rows.append(line)
                else:
                    rows.append(mask.words(line))
            elif key == "pbsnodes_file":
                if not line[0].isspace():
                    rows.append(mask(line.strip(), "node"))
                elif "=" in line:
                    name, value = [item.strip() for item in line.split("=", 1)]
                    if name in ("np", "pcpus", "resources_available.ncpus", "gpus"):
                        value = mask.public(value, r"[0-9]+")
                    elif name == "state":
                        value = mask.public(
                            value,
                            r"(?:free|busy|down|offline|unknown|job-busy|job-exclusive|reserve|time-shared)"
                            r"(?:,(?:free|busy|down|offline|unknown|job-busy|job-exclusive|reserve|time-shared))*",
                        )
                    elif name == "jobs":
                        value = re.sub(r"([0-9,\-]+)/([0-9\[\]-]+)(?:\.[\w.*-]+)?", lambda match: match[1] + "/" + mask(match[2], "job") + ".server", value)
                        value = mask.public(value, r"(?:[0-9,\-]+/[0-9\[\]\-]+\.server(?:,?\s*)?)*")
                    else:
                        name, value = mask(name, "field"), mask(value)
                    rows.append("     " + name + " = " + value)
                else:
                    raise ValueError("unsupported continuation")
            else:
                raise ValueError("unsupported trace")
        return {"qstat_file": "qstat.txt", "qstatq_file": "qstat_q.txt", "pbsnodes_file": "pbsnodes_a.txt"}[key], "\n".join(rows) + "\n"

    @staticmethod
    def get_mnemonic() -> str:
        """Return the scheduler name, pbs."""
        return "pbs"

    def __init__(self, scheduler_output_filenames: Dict[str, str], config: "Config", options: "Options") -> None:
        # "" (falsy, like the former None) when a trace is not configured; reading it then fails as an OSError.
        self.pbsnodes_file = scheduler_output_filenames.get("pbsnodes_file", "")
        self.qstat_file = scheduler_output_filenames.get("qstat_file", "")
        self.qstatq_file = scheduler_output_filenames.get("qstatq_file", "")

        self.config = config
        self.options = options
        self.qstat_maker = PBSStatExtractor(self.config, self.options)

    def get_worker_nodes(self, job_ids: List[str], job_queues: List[str], options: "Options") -> "List[Node]":
        """Return the nodes from pbsnodes; an empty file yields none."""
        try:
            fileutils.check_empty_file(self.pbsnodes_file)
        except fileutils.FileEmptyError:
            return []

        raw_blocks = self._read_all_blocks(self.pbsnodes_file)
        known_ids = set(job_ids)
        all_pbs_values = []
        anonymize = self.qstat_maker.anonymize_func()
        for block in raw_blocks:
            pbs_values = dict()
            pbs_values["domainname"] = block["domainname"] if not self.options.ANONYMIZE else anonymize(block["domainname"], "wns")

            nextchar = block["state"][0]
            state = "-" if nextchar == "f" else nextchar

            pbs_values["state"] = state

            # find attribute for number of cores, default is 0
            if block.get("np"):
                pbs_values["np"] = block["np"]
            elif block.get("pcpus"):
                pbs_values["np"] = block["pcpus"]
            else:
                pbs_values["np"] = block.get("resources_available.ncpus", 0)

            raw_gpus = block.get("gpus", 0)
            try:
                gpus = int(raw_gpus)
                if gpus < 0:
                    raise ValueError("abnormal GPU count")
            except (TypeError, ValueError):
                logging.warning("Ignoring invalid PBS GPU count %r on node %s", raw_gpus, pbs_values["domainname"])
            else:
                if gpus > 0:
                    pbs_values["gpus"] = raw_gpus

            try:  # this should turn up more often, hence the try/except.
                _ = block["jobs"]
            except KeyError:
                pbs_values["core_job_map"] = dict()  # change of behaviour: all entries should contain the key even if no value
            else:
                jobs = re.findall(r"[0-9][0-9a-zA-Z\[\],.-]*\/[^,]+", block["jobs"])
                pbs_values["core_job_map"] = dict((core, resolve_job_id(job, known_ids)) for job, core in self._get_jobs_cores(jobs))
            finally:
                all_pbs_values.append(pbs_values)

        return self.ensure_worker_nodes_have_qnames(all_pbs_values, job_ids, job_queues)

    def get_jobs_info(self) -> "JobsInfo":
        """Return job IDs, users, states and queues from qstat (shared with OAR and SGE)."""
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

    def get_queues_info(self) -> "QueuesInfo":
        """Return the running and queued totals and per-queue records from qstat -q."""
        qstatq_list = []
        qstatqs_total = self.qstat_maker.extract_qstatq(self.qstatq_file)

        for qstatq in qstatqs_total[:-1]:
            qstatq_list.append(qstatq)
        for _total in qstatqs_total[-1:]:  # this is at most one item
            total_running_jobs, total_queued_jobs = _total["Total_running"], _total["Total_queued"]
            break
        else:
            total_running_jobs, total_queued_jobs = 0, 0

        queues: "List[RawQueue]" = cast("List[RawQueue]", qstatq_list)  # every row but the totals line is a queue record
        return int(total_running_jobs), int(total_queued_jobs), queues

    @staticmethod
    def _get_jobs_cores(jobs: List[str]) -> Iterator[Tuple[str, str]]:  # block['jobs']
        """Yield (core, job) pairs from a node's job list, in Torque or PBS Pro form.

        Torque:  '0/10102182.f-batch01.grid.sinica.edu.tw, 1/10102106.f-batch01.grid.sinica.edu.tw'
        PBS Pro: '2257887.cluster-pbs5/0, 2257887.cluster-pbs5/1, 2257887.cluster-pbs5/2'
        """
        for core_job in jobs:
            part1, part2 = core_job.strip().split("/")
            if re.search(r"^\d+[,-]?[\d,-]*$", part1):  # Torque job id
                core, job = part1, part2
            elif re.match(r"[\w.-]+", part1):  # PBS Pro job id
                job, core = part1, part2

            job = re.sub(r"\[\]$", "", job.strip().split("/")[0].split(".")[0])
            if ("," in core) or ("-" in core):  # job id with subjobs
                for subcore, subjob in PBSBatchSystem.get_corejob_from_range(core, job):
                    # Core identifiers stay as strings, matching the non-range branch.
                    yield subjob, subcore
            else:  # job id without subjobs
                yield job, core

    def _read_all_blocks(self, orig_file: str) -> List[Dict[str, Any]]:
        """Reads pbsnodes txt file block by block."""
        with open(orig_file, mode="r", encoding="utf-8", errors="replace") as fin:
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
    def _read_block(fin: TextIO) -> Optional[Dict[str, Any]]:
        for line in fin:
            domain_name = line.strip()
            if domain_name:
                break
        else:
            return None

        block = {"domainname": domain_name}
        previous_key = None
        reading = True
        while reading:
            line = fin.readline()
            if not line.strip():
                reading = False
            else:
                key_value = line.split(" = ", 1)
                if len(key_value) == 2:
                    key, value = key_value
                    block[key.strip()] = value.strip()
                    previous_key = key.strip()
                elif previous_key == "jobs" and line[0].isspace():
                    block["jobs"] += " " + line.strip()
        return block

    @staticmethod
    def get_corejob_from_range(core_selections: str, job: str) -> Iterator[Tuple[str, str]]:
        """Yield (core, job) pairs for a core selection such as '0-3,8'."""
        _cores: List[Iterable[str]] = list()
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
