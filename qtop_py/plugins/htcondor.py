## Copyright (c) 2026 qtop contributors
## SPDX-License-Identifier: MIT
"""HTCondor backend: parses condor_status -json and condor_q -json ClassAds."""

import json
import logging
from collections import OrderedDict
from typing import TYPE_CHECKING, Any, Dict, List, Mapping

from qtop_py import core_inputs as inputs
from qtop_py.core_serialiser import GenericBatchSystem, StatExtractor

if TYPE_CHECKING:
    from typing import Final

    from qtop_py.core_state_types import Config, JobsInfo, Node, Options, QueuesInfo

## condor_q JobStatus codes, as the letters condor_q prints (6, transferring output, is shown as E).
JOB_STATES: "Final" = {1: "I", 2: "R", 3: "X", 4: "C", 5: "H", 6: "E", 7: "S"}
## A machine is off ('o') when every slot is Drained or in the Owner state (not offered to the pool).
OFF_STATES: "Final" = ("Drained", "Owner")
DEFAULT_GROUP: "Final" = "default"


def slot_type(ad: Mapping[str, Any]) -> str:
    """Return Partitionable, Dynamic or Static, also for pools that only set the boolean attributes."""
    if ad.get("SlotType"):
        return str(ad["SlotType"])
    return "Partitionable" if ad.get("PartitionableSlot") else "Dynamic" if ad.get("DynamicSlot") else "Static"


def _count(value: Any, default: int = 0) -> int:
    """Return a nonnegative ClassAd integer, or default when it is missing or not an integer."""
    return value if isinstance(value, int) and not isinstance(value, bool) and value >= 0 else default


class HTCondorBatchSystem(GenericBatchSystem):
    """HTCondor backend; accounting groups stand in for queues."""

    classic_account_note = "  (queues are accounting groups; 'default' has none)"

    @staticmethod
    def get_mnemonic() -> str:
        """Return the scheduler name, htcondor."""
        return "htcondor"

    def __init__(self, scheduler_output_filenames: Dict[str, str], config: "Config", options: "Options") -> None:
        self.status_file = scheduler_output_filenames.get("status_json", "")
        self.q_file = scheduler_output_filenames.get("q_json", "")
        self.config = config
        self.options = options
        # With -A, one stable mapping for owners, groups and machines across both traces.
        self.anonymize = StatExtractor(config, options).anonymize

    def get_jobs_info(self) -> "JobsInfo":
        """Return ClusterId.ProcId job IDs, owners, condor_q state letters and accounting groups."""
        jobs = self._jobs()
        return [job["id"] for job in jobs], [job["user"] for job in jobs], [job["state"] for job in jobs], [job["queue"] for job in jobs]

    def get_queues_info(self) -> "QueuesInfo":
        """Return running and idle job counts per accounting group; HTCondor has no queue limits or states."""
        queues: Dict[str, Any] = OrderedDict()
        for job in self._jobs():
            queue = queues.setdefault(job["queue"], {"queue_name": job["queue"], "run": 0, "queued": 0, "lm": "--", "state": "-"})
            queue["run"] += job["state"] == "R"
            queue["queued"] += job["state"] == "I"
        return sum(queue["run"] for queue in queues.values()), sum(queue["queued"] for queue in queues.values()), list(queues.values())

    def get_worker_nodes(self, job_ids: List[str], job_queues: List[str], options: "Options") -> "List[Node]":
        """Return machines with their cores, each job filling its slot's cores in condor_q order."""
        machines: Dict[str, Dict[str, Any]] = OrderedDict()
        slot_cpus: Dict[str, int] = {}
        for ad in self._ads(self.status_file):
            if not isinstance(ad.get("Machine"), str) or not ad["Machine"] or not isinstance(ad.get("Name"), str):
                raise ValueError("condor_status ad lacks Name or Machine: %r" % ad.get("Name"))
            name = self.anonymize(ad["Machine"], "wns")
            machine = machines.setdefault(name, {"cores": 0, "states": set()})
            kind = slot_type(ad)
            if kind == "Partitionable":
                machine["cores"] += _count(ad.get("TotalSlotCpus"), _count(ad.get("Cpus")))
            elif kind == "Static":
                machine["cores"] += _count(ad.get("Cpus"), 1)
            if kind != "Dynamic":
                machine["states"].add(str(ad.get("State", "")))
            slot_cpus[self._slot(ad["Name"])] = _count(ad.get("Cpus"), 1)
        nodes: Dict[str, Dict[str, Any]] = OrderedDict()
        for name, machine in machines.items():
            off = machine["states"] and machine["states"] <= set(OFF_STATES)
            nodes[name] = OrderedDict([("domainname", name), ("np", machine["cores"]), ("core_job_map", {}), ("state", "o" if off else "-")])
        for job in self._jobs():
            if not job["slot"]:
                continue
            host = job["slot"].split("@", 1)[1]
            node = nodes.setdefault(host, OrderedDict([("domainname", host), ("np", 0), ("core_job_map", {}), ("state", "?")]))
            first = len(node["core_job_map"])
            cores: int = slot_cpus[job["slot"]] if job["slot"] in slot_cpus else job["cpus"]
            node["core_job_map"].update((first + core, job["id"]) for core in range(cores))
        for node in nodes.values():
            node["np"] = max(node["np"], len(node["core_job_map"]))
        logging.info("worker_nodes contains %s entries", len(nodes))
        return self.ensure_worker_nodes_have_qnames(list(nodes.values()), job_ids, job_queues)

    def _slot(self, name: str) -> str:
        """Return slot@machine with the machine anonymized like condor_status Machine."""
        slot, separator, machine = name.partition("@")
        if not separator or not slot or not machine:
            raise ValueError("Invalid HTCondor slot name: %r" % name)
        return "%s@%s" % (slot, self.anonymize(machine, "wns"))

    def _jobs(self) -> List[Dict[str, Any]]:
        if not hasattr(self, "_parsed_jobs"):
            self._parsed_jobs = [self._job(ad) for ad in self._ads(self.q_file)]
        return self._parsed_jobs

    def _job(self, ad: Mapping[str, Any]) -> Dict[str, Any]:
        cluster, proc, status = ad.get("ClusterId"), ad.get("ProcId"), ad.get("JobStatus")
        if _count(cluster, -1) < 0 or _count(proc, -1) < 0 or status not in JOB_STATES or not isinstance(ad.get("Owner"), str):
            raise ValueError("Invalid condor_q job ad: ClusterId=%r ProcId=%r JobStatus=%r" % (cluster, proc, status))
        group = ad.get("AcctGroup")
        remote = ad.get("RemoteHost")
        return {
            "id": "%s.%s" % (cluster, proc),
            "user": self.anonymize(ad["Owner"], "users"),
            "state": JOB_STATES[status],
            "queue": self.anonymize(group, "qs") if isinstance(group, str) and group else DEFAULT_GROUP,
            "slot": self._slot(remote) if isinstance(remote, str) and remote else "",
            "cpus": _count(ad.get("RequestCpus"), 1) or 1,
        }

    def _ads(self, path: str) -> List[Mapping[str, Any]]:
        """Read a JSON ClassAd list; empty output (condor_q with no jobs) is an empty list."""
        with inputs.open_input(path, "r", encoding="utf-8", errors="replace", options=self.options) as stream:
            text = stream.read()
        if not text.strip():
            return []
        ads = json.loads(text)
        if not isinstance(ads, list) or not all(isinstance(ad, dict) for ad in ads):
            raise ValueError("HTCondor JSON in %s is not a list of ClassAds" % path)
        return ads
