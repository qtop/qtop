# SPDX-License-Identifier: MIT
"""Experimental queue membership and observed idle-node counts."""

from collections import OrderedDict
from typing import Any, Dict, List, Optional, Set, Tuple


def queue_node_counts(nodes: List[Dict[str, Any]], queue_names: List[str], scheduler: str, mapping: object = None) -> List[Tuple[str, Optional[int], Optional[int]]]:
    """Use authoritative membership, never infer eligible pools from running jobs."""
    by_name = {node["domainname"]: node for node in nodes}
    membership: Dict[str, Set[str]] = {}
    if scheduler in ("slurm", "sge"):
        for node in nodes:
            for queue in node["qname"]:
                if queue:
                    membership.setdefault(queue, set()).add(node["domainname"])
    if mapping is not None:
        from qtop_py.plugins.slurm import SlurmBatchSystem

        if not isinstance(mapping, dict):
            raise ValueError("experimental_queue_nodes must map queue names to node ranges")
        for queue, pattern in mapping.items():
            hosts = SlurmBatchSystem.expand_nodelist(pattern) if isinstance(pattern, str) else []
            if not isinstance(queue, str) or not queue.strip() or not hosts:
                raise ValueError("Queue-node mappings require a queue name and valid node range")
            membership[queue] = set(hosts)
    idle = {
        name
        for name, node in by_name.items()
        if node["state"] == "-"
        and not node["core_job_map"]
        and not node.get("slurm_job_ids")
        and not node.get("oar_job_ids")
        and node.get("slurm_node_state", "idle").lower() == "idle"
        and not any(resource["count"] for resource in node.get("slurm_gpu_used", []))
    }
    counts = []
    for queue in OrderedDict.fromkeys(queue_names + list(membership)):
        hosts = membership.get(queue)
        total = len(hosts) if hosts is not None else None
        free = len(hosts & idle) if hosts is not None and hosts.issubset(by_name) else None
        counts.append((queue, free, total))
    return counts
