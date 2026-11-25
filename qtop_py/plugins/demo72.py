## Copyright (c) 2026 qtop contributors
## SPDX-License-Identifier: MIT
"""A simulated GB200 NVL72 rack for trying qtop's accelerator view without a scheduler: qtop -b demo72.

One NVLink domain: 18 compute trays, each with 4 GPUs and 2 Grace CPUs (144 cores, 36 per GPU).
A large training job spans several whole trays, smaller jobs take 1, 2 or 4 GPUs of one tray,
the odd tray is down, and one job's GPU details are missing, so its GPUs show as "?". The jobs
change every PERIOD seconds, so watch mode (-w) moves. Nodes carry the Slurm GPU fields the
slurm backends report (slurm_gpu_configured, slurm_gpu_used, slurm_gpu_bindings), so the
accelerator map, -A, exports and replay treat them as they treat live Slurm data.
"""

import random
import time
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple

from qtop_py.core_api import GenericBatchSystem
from qtop_py.plugins.slurm_extension import SLURM_EXTENSION
from qtop_py.schedulers.gpu import accelerator_map_lines

if TYPE_CHECKING:
    from typing import Final

    from qtop_py.core_state_types import Config, GPUResource, JobsInfo, Node, Options, QueuesInfo

TRAYS: "Final" = 18
GPUS_PER_TRAY: "Final" = 4
CORES_PER_GPU: "Final" = 36  # 2 Grace CPUs x 72 Neoverse V2 cores, split evenly over the tray's 4 GPUs
GPU_TYPE: "Final" = "gb200"
PERIOD: "Final" = 20  # seconds between simulated scheduling rounds
QUEUES: "Final" = ("train", "infer", "debug")
USERS: "Final" = ("ada", "grace", "turing", "hopper", "lovelace", "shannon", "noether", "dijkstra")
FIRST_JOB_ID: "Final" = 4100000
GPU_FILL: "Final" = 0.85  # small jobs stop once this share of the up GPUs is allocated


def tray_name(tray: int) -> str:
    """Return the hostname of tray 0..17: tray01..tray18 (qtop numbers nodes by their digits)."""
    return "tray%02d" % (tray + 1)


class NVL72Simulator(object):
    """One scheduling round of an NVL72 rack, reproducible from its seed."""

    def __init__(self, seed: int) -> None:
        rng = random.Random(seed)  # nosec B311 - simulation only
        self.down = {tray for tray in range(TRAYS) if rng.random() < 0.06}
        self.gpu_owner: Dict[Tuple[int, int], str] = {}  # (tray, gpu) -> job ID
        self.jobs: List[Dict[str, Any]] = []  # id, user, state, queue, trays
        self.hidden_job: Optional[str] = None  # its GPU details are "missing"
        up = [tray for tray in range(TRAYS) if tray not in self.down]
        next_id = FIRST_JOB_ID + (seed % 1000) * 100

        # A training job on a run of whole up trays: what an NVL72 rack is for.
        span = rng.randint(6, max(6, len(up) - 6))
        start = rng.randint(0, len(up) - span)
        big = str(next_id)
        next_id += 1
        self._run(big, rng.choice(USERS), "train", [(tray, gpu) for tray in up[start : start + span] for gpu in range(GPUS_PER_TRAY)])

        # Inference and debug jobs of 1, 2 or 4 GPUs on one tray, until the rack is mostly full.
        target = int(GPU_FILL * len(up) * GPUS_PER_TRAY)
        while len(self.gpu_owner) < target:
            size = rng.choice((1, 1, 2, 2, 4))
            trays = [tray for tray in up if sum((tray, gpu) not in self.gpu_owner for gpu in range(GPUS_PER_TRAY)) >= size]
            if not trays:
                break
            tray = rng.choice(trays)
            gpus = [(tray, gpu) for gpu in range(GPUS_PER_TRAY) if (tray, gpu) not in self.gpu_owner][:size]
            self._run(str(next_id), rng.choice(USERS), "debug" if size == 1 and rng.random() < 0.3 else "infer", gpus)
            next_id += 1
        small = [job["id"] for job in self.jobs[1:]]
        self.hidden_job = rng.choice(small) if small else None

        # Waiting: a full-rack job that cannot start while anything else runs, and a few small ones.
        self.jobs.append({"id": str(next_id), "user": rng.choice(USERS), "state": "PD", "queue": "train", "gpus": []})
        for offset in range(1, rng.randint(2, 6)):
            self.jobs.append({"id": str(next_id + offset), "user": rng.choice(USERS), "state": "PD", "queue": rng.choice(QUEUES[1:]), "gpus": []})

    def _run(self, job_id: str, user: str, queue: str, gpus: List[Tuple[int, int]]) -> None:
        for device in gpus:
            self.gpu_owner[device] = job_id
        self.jobs.append({"id": job_id, "user": user, "state": "R", "queue": queue, "gpus": gpus})


def _gpu_resource(indices: Optional[List[int]], count: int) -> "GPUResource":
    return {"type": GPU_TYPE, "count": count, "device_indices": indices, "mig_profile": None, "parent_gpu_uuid": None}


class Demo72BatchSystem(GenericBatchSystem):
    """Simulated NVL72 rack (-b demo72): Slurm-shaped GPU records, no scheduler needed."""

    signature_command = "echo"
    default_commands = {
        "demo72_file": "%(savepath)s/demo72%(pid)s.txt, echo 'NVL72 demo here'",
    }
    state_abbreviations = {
        "R": "running_of_user",
        "PD": "queued_of_user",
    }
    extensions = SLURM_EXTENSION
    node_job_fields = SLURM_EXTENSION.node_job_fields
    is_simulated = True

    @staticmethod
    def get_mnemonic() -> str:
        """Return the scheduler name, demo72."""
        return "demo72"

    def __init__(self, scheduler_output_filenames: Dict[str, str], config: "Config", options: "Options") -> None:
        """Keep the trace names and configuration; simulate this PERIOD's scheduling round."""
        self.scheduler_output_filenames = scheduler_output_filenames
        self.config = config
        self.sim = NVL72Simulator(int(time.time()) // PERIOD)

    def get_jobs_info(self) -> "JobsInfo":
        """Return the running and waiting jobs as four parallel lists."""
        jobs = self.sim.jobs
        return [job["id"] for job in jobs], [job["user"] for job in jobs], [job["state"] for job in jobs], [job["queue"] for job in jobs]

    def get_queues_info(self) -> "QueuesInfo":
        """Return the running and queued totals and a record per partition."""
        running = [job for job in self.sim.jobs if job["state"] == "R"]
        waiting = [job for job in self.sim.jobs if job["state"] == "PD"]
        return (
            len(running),
            len(waiting),
            [
                {
                    "queue_name": queue,
                    "run": str(sum(job["queue"] == queue for job in running)),
                    "queued": str(sum(job["queue"] == queue for job in waiting)),
                    "state": "Q",
                    "lm": "--",
                }
                for queue in QUEUES
            ],
        )

    def get_worker_nodes(self, job_ids: List[str], job_queues: List[str], options: "Options") -> "List[Node]":
        """Return the 18 trays with their CPU map (36 cores per GPU) and Slurm GPU records."""
        nodes = []
        for tray in range(TRAYS):
            owners = {gpu: self.sim.gpu_owner[(tray, gpu)] for gpu in range(GPUS_PER_TRAY) if (tray, gpu) in self.sim.gpu_owner}
            down = tray in self.sim.down
            node: Dict[str, Any] = {
                "domainname": tray_name(tray),
                "state": "d" if down else "b" if owners else "-",
                "np": GPUS_PER_TRAY * CORES_PER_GPU,
                "core_job_map": {gpu * CORES_PER_GPU + core: job for gpu, job in owners.items() for core in range(CORES_PER_GPU)},
                "slurm_node_state": "down" if down else "allocated" if len(owners) == GPUS_PER_TRAY else "mixed" if owners else "idle",
                "cpu_allocation_source": "slurm",
                "slurm_gpu_configured": [_gpu_resource(list(range(GPUS_PER_TRAY)), GPUS_PER_TRAY)],
            }
            if not down:  # a down tray reports no usage, so its GPUs show as unknown, not idle
                node["slurm_gpu_used"] = [_gpu_resource(sorted(owners), len(owners))]
            bindings = [dict(_gpu_resource([gpu], 1), job_id=job) for gpu, job in sorted(owners.items()) if job != self.sim.hidden_job]
            if bindings:
                node["slurm_gpu_bindings"] = bindings
            nodes.append(node)
        return self.ensure_worker_nodes_have_qnames(nodes, job_ids, job_queues)

    @staticmethod
    def display_messages(section: str, snapshot: object, nodes: "List[Node]", config: "Config") -> List[str]:
        """Return the accelerator map, and what the simulation stands for."""
        if section == "accelerators":
            lines = accelerator_map_lines(nodes)
            return ["INFO: Accelerators by node (job IDs, ? = unknown):"] + ["INFO: " + line for line in lines] if lines else []
        if section == "placement":
            return ["INFO: demo72 simulates one GB200 NVL72 rack (one NVLink domain): %s trays x %s GPUs, %s Grace cores per GPU." % (TRAYS, GPUS_PER_TRAY, CORES_PER_GPU)]
        return []
