##
## qtop is a tool to monitor queuing systems - https://github.com/qtop/qtop
##
## Copyright (c) 2016 Fotis Georgatos
## Copyright (c) 2016 Sotiris Fragkiskos
##
## SPDX-License-Identifier: MIT
##
"""
Slurm support for qtop.

Reads two scheduler-output files captured by the qtop runner:

  squeue_file -- output of ``squeue -h -o '%i|%P|%u|%T|%C|%R'``
  sinfo_file  -- output of ``sinfo -h -N -o '%N|%T|%P|%C'``

The pipe separator avoids whitespace-in-fields ambiguity (job names, reasons,
node-state qualifiers like ``mix*`` etc.) without locking us into a specific
table-formatting width.

Implementation notes
--------------------

This plugin is intentionally minimal: it follows the same StatExtractor /
GenericBatchSystem pattern used by ``oar.py``, parses text-mode Slurm output
(no JSON dependency on the Slurm side, since ``--json`` is optional on
older clusters), and expands Slurm's compact node-list notation
(``wn[001-003,005]``) into individual node names so the rest of qtop can
treat each worker node uniformly.
"""

import logging
import re

from qtop_py.serialiser import GenericBatchSystem, StatExtractor

#: Slurm job-state letters as they appear in ``squeue -h -t all``. We keep
#: the full long form to avoid colliding with qtop's per-scheduler state map.
SLURM_RUNNING_STATES = frozenset({"RUNNING", "COMPLETING"})
SLURM_QUEUED_STATES = frozenset({"PENDING", "CONFIGURING"})

#: Used when squeue puts a reason in the NODELIST column for pending jobs.
_REASON_PREFIX = "("


def expand_nodelist(nodelist):
    """Expand Slurm's compact host-range notation into a flat list of names.

    Examples::

        >>> expand_nodelist("wn001")
        ['wn001']
        >>> expand_nodelist("wn[001-003]")
        ['wn001', 'wn002', 'wn003']
        >>> expand_nodelist("wn[001,003-004]")
        ['wn001', 'wn003', 'wn004']
        >>> expand_nodelist("(Priority)")
        []
        >>> expand_nodelist("")
        []

    Width-preserving: ``001`` stays zero-padded to 3 digits.
    """
    if not nodelist or nodelist.startswith(_REASON_PREFIX):
        return []

    m = re.match(r"^([A-Za-z][\w.-]*)\[([^\]]+)\]$", nodelist)
    if not m:
        # No bracket form. Either a single node or a comma-separated bare list.
        return [n.strip() for n in nodelist.split(",") if n.strip()]

    prefix = m.group(1)
    body = m.group(2)
    nodes = []
    for chunk in body.split(","):
        chunk = chunk.strip()
        if "-" in chunk:
            lo_s, hi_s = chunk.split("-", 1)
            width = len(lo_s)
            for n in range(int(lo_s), int(hi_s) + 1):
                nodes.append("%s%s" % (prefix, str(n).zfill(width)))
        else:
            nodes.append("%s%s" % (prefix, chunk))
    return nodes


# ---------------------------------------------------------------------------
# Parsers
# ---------------------------------------------------------------------------


class SlurmStatExtractor(StatExtractor):
    """Parse the squeue and sinfo files written by the qtop runner."""

    def __init__(self, config, options):
        StatExtractor.__init__(self, config, options)
        # squeue with -o '%i|%P|%u|%T|%C|%R'
        self.squeue_re = re.compile(
            r"^\s*"
            r"(?P<job_id>\d+(?:_\d+)?)\|"
            r"(?P<queue>[\w,.-]+)\|"
            r"(?P<user>[\w.-]+)\|"
            r"(?P<state>[A-Z_]+)\|"
            r"(?P<cpus>\d+)\|"
            r"(?P<nodelist>.+?)\s*$"
        )
        # sinfo with -N -o '%N|%T|%P|%C'
        self.sinfo_re = re.compile(
            r"^\s*"
            r"(?P<node>[\w.-]+)\|"
            r"(?P<state>[A-Za-z*~+@]+)\|"
            r"(?P<partition>[\w,.*-]+)\|"
            r"(?P<cpus>[\d/]+)\s*$"
        )

    def extract_squeue(self, path):
        """Return a list of dicts, one per job in the squeue file."""
        jobs = []
        with open(path, "r") as f:
            for raw_line in f:
                line = raw_line.rstrip("\n")
                if not line.strip():
                    continue
                m = self.squeue_re.match(line)
                if not m:
                    logging.debug(
                        "slurm: skipping unparseable squeue line: %r", line
                    )
                    continue
                jobs.append(m.groupdict())
        return jobs

    def extract_sinfo(self, path):
        """Return a list of dicts, one per worker-node row in sinfo (-N)."""
        nodes = []
        with open(path, "r") as f:
            for raw_line in f:
                line = raw_line.rstrip("\n")
                if not line.strip():
                    continue
                m = self.sinfo_re.match(line)
                if not m:
                    logging.debug(
                        "slurm: skipping unparseable sinfo line: %r", line
                    )
                    continue
                nodes.append(m.groupdict())
        return nodes


# ---------------------------------------------------------------------------
# Public batch-system interface
# ---------------------------------------------------------------------------


class SLURMBatchSystem(GenericBatchSystem):
    @staticmethod
    def get_mnemonic():
        return "slurm"

    def __init__(self, scheduler_output_filenames, config, options):
        GenericBatchSystem.__init__(self)
        self.config = config
        self.options = options
        self.squeue_path = scheduler_output_filenames["squeue_file"]
        self.sinfo_path = scheduler_output_filenames["sinfo_file"]
        self._extractor = SlurmStatExtractor(config, options)
        self._squeue_jobs = self._extractor.extract_squeue(self.squeue_path)
        self._sinfo_rows = self._extractor.extract_sinfo(self.sinfo_path)

    # ---- worker nodes ---------------------------------------------------

    def get_worker_nodes(self, job_ids, job_queues, options):
        """Return the list of {domainname, state, qname, np} dicts qtop expects."""
        # squeue tells us which nodes each job runs on; collect job -> nodelist
        # so we can attach running jobs to their nodes later.
        node_to_jobs = {}
        for j in self._squeue_jobs:
            if j["state"] not in SLURM_RUNNING_STATES:
                continue
            for node in expand_nodelist(j["nodelist"]):
                node_to_jobs.setdefault(node, []).append(j["job_id"])

        nodes_out = []
        seen = set()
        for row in self._sinfo_rows:
            name = row["node"]
            if name in seen:
                continue
            seen.add(name)
            # CPUS column from sinfo -N is "alloc/idle/other/total"
            total = row["cpus"].split("/")[-1] if "/" in row["cpus"] else row["cpus"]
            np = int(total) if total.isdigit() else 0
            # strip trailing * (loaded) and ~ (powered down) qualifiers
            state = row["state"].rstrip("*~+@").lower()
            nodes_out.append(
                {
                    "domainname": name,
                    "state": state,
                    "qname": [row["partition"]],
                    "np": np,
                    "core_job_map": {},
                    "existing_busy_nodes": int(bool(node_to_jobs.get(name))),
                    "jobs": node_to_jobs.get(name, []),
                }
            )
        return nodes_out

    # ---- jobs -----------------------------------------------------------

    def get_jobs_info(self):
        job_ids, user_names, job_states, job_queues = [], [], [], []
        for j in self._squeue_jobs:
            job_ids.append(j["job_id"])
            user_names.append(j["user"])
            job_states.append(j["state"])
            job_queues.append(j["queue"])
        return job_ids, user_names, job_states, job_queues

    # ---- queues ---------------------------------------------------------

    def get_queues_info(self):
        """Aggregate per-partition counts from squeue."""
        per_partition = {}
        for j in self._squeue_jobs:
            slot = per_partition.setdefault(
                j["queue"], {"queued": 0, "run": 0, "lm": 0, "state": "?"}
            )
            if j["state"] in SLURM_RUNNING_STATES:
                slot["run"] += 1
            elif j["state"] in SLURM_QUEUED_STATES:
                slot["queued"] += 1
            slot["state"] = "E" if (slot["run"] + slot["queued"]) > 0 else "?"
        queues_info = []
        for name, counts in per_partition.items():
            queues_info.append(
                {
                    "queue_name": name,
                    "state": counts["state"],
                    "lm": counts["lm"],
                    "run": counts["run"],
                    "queued": counts["queued"],
                }
            )
        total_run = sum(c["run"] for c in per_partition.values())
        total_queued = sum(c["queued"] for c in per_partition.values())
        return queues_info, total_run, total_queued
