## SPDX-License-Identifier: MIT

from types import SimpleNamespace

import pytest

from qtop_py import qtop
from qtop_py.cluster_state import cluster_state_totals, validate_cluster_state
from qtop_py.plugins.slurm import SlurmBatchSystem
from qtop_py.sample import anonymize_state


@pytest.fixture
def trace(tmp_path):
    def create(jobs, nodes, details=None):
        files = {}
        for key, text in (("squeue", jobs), ("sinfo", nodes), ("scontrol", details)):
            if text is not None:
                path = tmp_path / (key + ".txt")
                path.write_text(text)
                files[key + "_file"] = str(path)
        return SlurmBatchSystem(files, {}, SimpleNamespace(ANONYMIZE=False))

    return create


@pytest.mark.parametrize("capacities, expected", [((8, 2), (6, 2)), ((2, 8), (2, 6)), ((4, 4), (4, 4))])
def test_fallback_uses_all_available_capacity(trace, capacities, expected):
    slurm = trace("1|alice|R|batch|8|node[1-2]\n", "node1|batch|mix|%s\nnode2|batch|mix|%s\n" % capacities)
    state = slurm.get_cluster_state(slurm.options)
    assert tuple(len(node["core_job_map"]) for node in state["nodes"]) == expected
    assert all(node["slurm_job_ids"] == ["1"] for node in state["nodes"])
    assert not any(node.get("slurm_unmapped_cpus") for node in state["nodes"])


@pytest.mark.parametrize("details", [None, "JobId=1\n Nodes=node1 CPU_IDs=0-3\nJobId=2\n Nodes=node1 CPU_IDs=0-3\n"])
def test_shared_cpus_retain_both_users_node_membership(trace, details):
    slurm = trace("1|alice|R|batch|4|node1\n2|bob|R|batch|4|node1\n", "node1|batch|mix|4\n", details)
    state = slurm.get_cluster_state(slurm.options)
    node = state["nodes"][0]
    assert len(node["core_job_map"]) == 4
    assert node["slurm_job_ids"] == ["1", "2"]
    assert node["slurm_unmapped_cpus"] == 4
    cluster = SimpleNamespace(workernode_dict={1: node})
    qtop.Cluster._calculate_jobs_per_node(None, cluster.workernode_dict)
    users = qtop.WNOccupancy.calculate_user_node_use(None, cluster, {"1": ("alice", "batch"), "2": ("bob", "batch")})
    assert users == {"alice": 1, "bob": 1}
    masked, mappings = anonymize_state(state)
    assert masked["nodes"][0]["slurm_job_ids"] == [mappings["job"][job] for job in ("1", "2")]
    assert masked["nodes"][0]["slurm_unmapped_cpus"] == 4
    assert validate_cluster_state(masked) == masked


@pytest.mark.parametrize("jobs", ["1|alice|PD|batch,gpu,batch|1|(Priority)\n", "1|alice|PD|batch|1|(Priority)\n1|alice|PD|gpu|1|(Priority)\n"])
def test_pending_partition_memberships_do_not_inflate_global_totals(trace, jobs):
    slurm = trace(jobs, "node1|batch|idle|4\nnode1|gpu|idle|4\n")
    state = slurm.get_cluster_state(slurm.options)
    assert len(state["jobs"]) == 1
    assert state["jobs"][0]["queue"] == "batch"
    assert state["jobs"][0]["slurm_partitions"] == ["batch", "gpu"]
    assert [(queue["name"], queue["queued"]) for queue in state["queues"]] == [("batch", 1), ("gpu", 1)]
    assert slurm.get_queues_info()[:2] == (0, 1)
    assert cluster_state_totals(state) == {"running_jobs": 0, "queued_jobs": 1}
    document = qtop.Document.from_cluster_state(state)
    assert document.total_queued_jobs == 1
    assert document.to_cluster_state() == state
    masked, mappings = anonymize_state(state)
    assert masked["jobs"][0]["slurm_partitions"] == [mappings["queue"][name] for name in ("batch", "gpu")]
    assert cluster_state_totals(masked) == cluster_state_totals(state)
