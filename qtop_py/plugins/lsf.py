## Copyright (c) 2026 qtop contributors
## SPDX-License-Identifier: MIT
"""LSF backend: parses bhosts -w, bqueues -w and delimited bjobs -o output."""

import logging
import re
from collections import OrderedDict
from typing import TYPE_CHECKING, Any, Dict, List, Tuple

from qtop_py.core_api import GenericBatchSystem, open_input

if TYPE_CHECKING:
    from typing import Final

    from qtop_py.core_api import TraceMasker
    from qtop_py.core_state_types import Config, JobsInfo, Node, Options, QueuesInfo

logger = logging.getLogger(__name__)

## bhosts STATUS to qtop node symbols: '-' usable, 'j' full, 'b' busy, 'o' closed by an
## administrator or policy (counted off), 'd' down or unreachable, '?' anything else.
HOST_STATES: "Final" = {
    "ok": "-",
    "closed_full": "j",
    "closed_excl": "j",
    "closed_cu_excl": "j",
    "closed_busy": "b",
    "closed_adm": "o",
    "closed_lock": "o",
    "closed_wind": "o",
    "closed_ego": "o",
    "closed_rc": "o",
    "closed_lim": "d",
    "unavail": "d",
    "unreach": "d",
    "unlicensed": "d",
}
BJOBS_COLUMNS: "Final" = ("JOBID", "STAT", "USER", "QUEUE", "EXEC_HOST")
NO_JOBS: "Final" = ("No unfinished job found", "No job found")
EXEC_SLOT = re.compile(r"(?:([0-9]+)\*)?([^\s:*]+)")
TRACE_NAMES: "Final" = {"bhosts_file": "bhosts.txt", "bqueues_file": "bqueues.txt", "bjobs_file": "bjobs.txt"}


def parse_exec_host(value: str) -> "OrderedDict[str, int]":
    """Return slots per host from an EXEC_HOST value: '4*hostA:2*hostB' and 'hostA:hostA' both count."""
    slots: "OrderedDict[str, int]" = OrderedDict()
    value = value.strip()
    if value in ("", "-"):
        return slots
    for token in value.split(":"):
        match = EXEC_SLOT.fullmatch(token)
        if not match:
            raise ValueError("Cannot parse LSF EXEC_HOST: %s" % value)
        slots[match.group(2)] = slots.get(match.group(2), 0) + int(match.group(1) or 1)
    return slots


class LSFBatchSystem(GenericBatchSystem):
    """LSF backend."""

    blocked_queue_note = " (queue Closed or Inact)"

    @staticmethod
    def get_mnemonic() -> str:
        """Return the scheduler name, lsf."""
        return "lsf"

    @staticmethod
    def queue_is_blocked(state: object) -> bool:
        """Return True for a Closed or inactive queue (bqueues STATUS Closed:* or *:Inact)."""
        status = str(state).lower()
        return status.startswith("closed") or status.endswith(":inact")

    def __init__(self, scheduler_output_filenames: Dict[str, str], config: "Config", options: "Options") -> None:
        self.bhosts_file = scheduler_output_filenames.get("bhosts_file", "")
        self.bqueues_file = scheduler_output_filenames.get("bqueues_file", "")
        self.bjobs_file = scheduler_output_filenames.get("bjobs_file", "")
        self.config = config
        self.options = options

    def get_jobs_info(self) -> "JobsInfo":
        """Return job IDs (array elements as id[index]), users, LSF states and queues from bjobs."""
        jobs = self._jobs()
        return [job["id"] for job in jobs], [job["user"] for job in jobs], [job["state"] for job in jobs], [job["queue"] for job in jobs]

    def get_queues_info(self) -> "QueuesInfo":
        """Return per-queue running and pending job counts from bjobs, with status and MAX from bqueues.

        bqueues NJOBS/PEND/RUN count job slots, not jobs, so they are not used as job counts.
        """
        queues: Dict[str, Any] = OrderedDict()
        for row in self._table(self.bqueues_file, "QUEUE_NAME"):
            limit, name = row.get("MAX", "-"), row["QUEUE_NAME"]
            queues[name] = {"queue_name": name, "run": 0, "queued": 0, "lm": limit if limit != "-" else "--", "state": row["STATUS"]}
        for job in self._jobs():
            queue = queues.setdefault(job["queue"], {"queue_name": job["queue"], "run": 0, "queued": 0, "lm": "--", "state": "?"})
            queue["run"] += job["state"] == "RUN"
            queue["queued"] += job["state"] == "PEND"
        return sum(queue["run"] for queue in queues.values()), sum(queue["queued"] for queue in queues.values()), list(queues.values())

    def get_worker_nodes(self, job_ids: List[str], job_queues: List[str], options: "Options") -> "List[Node]":
        """Return hosts from bhosts, filling each one's slots with the jobs bjobs places there, in bjobs order."""
        nodes: Dict[str, Dict[str, Any]] = OrderedDict()
        for row in self._table(self.bhosts_file, "HOST_NAME"):
            maximum, name = row.get("MAX", "-"), row["HOST_NAME"]
            node: Dict[str, Any] = OrderedDict()
            node["domainname"] = name
            node["np"] = int(maximum) if maximum.isdigit() else 0
            node["core_job_map"] = {}
            node["state"] = HOST_STATES.get(row["STATUS"].lower(), "?")
            nodes[name] = node
        for job in self._jobs():
            for host, count in job["slots"].items():
                node = nodes.setdefault(host, OrderedDict([("domainname", host), ("np", 0), ("core_job_map", {}), ("state", "?")]))
                first = len(node["core_job_map"])
                node["core_job_map"].update((first + slot, job["id"]) for slot in range(count))
        for node in nodes.values():
            node["np"] = max(node["np"], len(node["core_job_map"]))  # MAX '-' or an oversubscribed host
        logger.info("worker_nodes contains %s entries", len(nodes))
        return self.ensure_worker_nodes_have_qnames(list(nodes.values()), job_ids, job_queues)

    def mask_source_trace(self, key: str, text: str, mask: "TraceMasker") -> Tuple[str, str]:
        """Return the masked name and text of a bhosts, bqueues or bjobs trace."""
        if key not in TRACE_NAMES:
            raise ValueError("unsupported trace")
        lines = text.splitlines()
        rows = lines[:1]
        for line in lines[1:]:
            if not line.strip() or line.strip().startswith(NO_JOBS):
                rows.append(line)
                continue
            if key == "bjobs_file":
                fields = line.split("|")
                if len(fields) != len(lines[0].split("|")):
                    raise ValueError("unsupported job row")
                columns = dict(zip(lines[0].strip().split("|"), range(len(fields))))
                for column, kind in (("JOBID", "job"), ("USER", "user"), ("QUEUE", "queue")):
                    fields[columns[column]] = mask(fields[columns[column]], kind)
                if "JOBINDEX" in columns:
                    fields[columns["JOBINDEX"]] = mask.public(fields[columns["JOBINDEX"]], "[0-9]+")
                fields[columns["STAT"]] = mask.public(fields[columns["STAT"]], "[A-Z]+")
                slots = parse_exec_host(fields[columns["EXEC_HOST"]])
                fields[columns["EXEC_HOST"]] = ":".join("%s*%s" % (count, mask(host, "node")) for host, count in slots.items()) or "-"
                rows.append("|".join(fields))
            else:
                fields = line.split()
                if len(fields) != len(lines[0].split()):
                    raise ValueError("unsupported %s row" % ("host" if key == "bhosts_file" else "queue"))
                fields[0] = mask(fields[0], "node" if key == "bhosts_file" else "queue")
                status = 1 if key == "bhosts_file" else 2
                fields[status] = mask.public(fields[status], r"ok|closed_\w+|unavail|unreach|unlicensed" if status == 1 else r"(?:Open|Closed):(?:Active|Inact)")
                fields[1:] = [field if index + 1 == status else mask.public(field, r"[0-9]+|-") for index, field in enumerate(fields[1:])]
                rows.append(" ".join(fields))
        return TRACE_NAMES[key], "\n".join(rows) + "\n"

    def _jobs(self) -> List[Dict[str, Any]]:
        if not hasattr(self, "_parsed_jobs"):
            self._parsed_jobs = self._read_bjobs(self.bjobs_file)
        return self._parsed_jobs

    def _read_bjobs(self, path: str) -> List[Dict[str, Any]]:
        """Parse bjobs -o "... delimiter='|'" output, whose header names the columns."""
        jobs: List[Dict[str, Any]] = []
        with open_input(path, "r", encoding="utf-8", errors="replace", options=self.options) as stream:
            lines = [line.rstrip("\n") for line in stream if line.strip()]
        if not lines or lines[0].strip().startswith(NO_JOBS):
            return jobs
        header = lines[0].strip().split("|")
        missing = [column for column in BJOBS_COLUMNS if column not in header]
        if missing:
            raise ValueError("bjobs output lacks columns %s; use the -o format in qtopconf.yaml" % ", ".join(missing))
        for line in lines[1:]:
            fields = line.strip().split("|")
            if len(fields) != len(header):
                raise ValueError("Cannot parse LSF job row: %s" % line.strip())
            row = dict(zip(header, fields))
            index = row.get("JOBINDEX", "0")
            if not re.fullmatch(r"[0-9]+", row["JOBID"]) or not re.fullmatch(r"[0-9]+", index):
                raise ValueError("Invalid LSF job ID: %s" % line.strip())
            slots = parse_exec_host(row["EXEC_HOST"])
            jobs.append(
                {
                    "id": row["JOBID"] if index == "0" else "%s[%s]" % (row["JOBID"], index),
                    "user": row["USER"],
                    "state": row["STAT"],
                    "queue": row["QUEUE"],
                    "slots": OrderedDict((host, count) for host, count in slots.items()),
                }
            )
        return jobs

    def _table(self, path: str, first_column: str) -> List[Dict[str, str]]:
        """Parse a whitespace-aligned bhosts/bqueues table by its header names."""
        with open_input(path, "r", encoding="utf-8", errors="replace", options=self.options) as stream:
            lines = [line.split() for line in stream if line.strip()]
        if not lines or lines[0][0] != first_column:
            raise ValueError("LSF table %s does not start with a %s header" % (path, first_column))
        rows = []
        for fields in lines[1:]:
            if len(fields) != len(lines[0]):
                raise ValueError("Cannot parse LSF row in %s: %s" % (path, " ".join(fields)))
            rows.append(dict(zip(lines[0], fields)))
        return rows
