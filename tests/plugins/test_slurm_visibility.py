## SPDX-License-Identifier: MIT

from types import SimpleNamespace

import pytest

from qtop_py import qtop
from qtop_py.cluster_state import cluster_state_totals, validate_cluster_state
from qtop_py.plugins.slurm import SlurmBatchSystem
from qtop_py.sample import anonymize_state


@pytest.mark.parametrize("hosts, missing, invalid", [("node2", 1, 0), ("node[1-2]", 1, 0), ("node[2-1]", 0, 1)])
def test_unrepresented_allocations_are_flagged_and_jobs_survive_replay(trace, hosts, missing, invalid):
    slurm = trace("1|alice|R|batch|4|%s\n" % hosts, "node1|batch|mix|8\n")
    state = slurm.get_cluster_state(slurm.options)
    diagnostics = state["slurm_diagnostics"]
    assert diagnostics.get("jobs_with_missing_nodes", 0) == missing
    assert diagnostics.get("jobs_with_invalid_nodelists", 0) == invalid
    assert state["jobs"][0]["id"] == "1"
    assert state["queues"][0]["running"] == 1
    assert qtop.Document.from_cluster_state(state).to_cluster_state() == state
    masked, _ = anonymize_state(state)
    assert masked["slurm_diagnostics"] == diagnostics
    assert validate_cluster_state(masked) == masked


def test_rejected_rows_are_counted_without_discarding_valid_data(trace):
    slurm = trace("bad-job\n1|alice|R|batch|2|node1\n", "bad-node\nnode1|batch|mix|8\n")
    state = slurm.get_cluster_state(slurm.options)
    assert state["slurm_diagnostics"] == {"rejected_squeue_rows": 1, "rejected_sinfo_rows": 1}
    assert len(state["jobs"]) == len(state["nodes"]) == 1
    assert slurm.get_cluster_state(slurm.options) == state  # Cached reads do not accumulate counters.


def test_clean_frames_have_no_diagnostics(trace):
    slurm = trace("1|alice|R|batch|2|node1\n", "node1|batch|mix|8\n")
    assert "slurm_diagnostics" not in slurm.get_cluster_state(slurm.options)


@pytest.mark.parametrize("raw_state,normalized", [("idle", "-"), ("down", "d"), ("alloc", "b")])
@pytest.mark.parametrize("suffix", list("*~#!%$@^-+") + ["", "~*"])
def test_slurm_state_suffix_reaches_matrix_and_snapshot(trace, raw_state, normalized, suffix, monkeypatch):
    monkeypatch.setattr(qtop, "scheduler", "slurm", raising=False)
    slurm = trace("", "node1|batch|%s%s|8\n" % (raw_state, suffix))
    state = slurm.get_cluster_state(slurm.options)
    assert state["nodes"][0]["state"] == normalized
    assert state["nodes"][0]["slurm_node_state"] == raw_state + suffix
    masked, _ = anonymize_state(state)
    assert validate_cluster_state(masked)["nodes"][0]["slurm_node_state"] == raw_state + suffix
    replayed = qtop.Document.from_cluster_state(masked).to_cluster_state()
    colored = qtop.colorize_nodestate(replayed["nodes"], {normalized: "Green"}, qtop.colorize)
    occupancy = SimpleNamespace(cluster=SimpleNamespace(workernode_dict={1: colored[0]}))
    config = {"workernodes_matrix": [{"node state": {"max_len": 1}}]}
    lines = qtop.WNOccupancy.calc_general_mult_attr_line(occupancy, "node state", "state", config)
    assert str(lines["attr1line"][0]) == (suffix[-1] if suffix else normalized)
    assert lines["attr1line"][0].color == "Green"
    assert str(colored[0]["state"][0]) == normalized


@pytest.mark.parametrize("states", [("idle", "idle~"), ("idle~", "idle"), ("idle~", "down*"), ("down*", "idle~")])
def test_duplicate_partition_rows_preserve_flag_on_selected_state(trace, states):
    slurm = trace("", "node1|batch|%s|8\nnode1|gpu|%s|8\n" % states)
    node = slurm.get_cluster_state(slurm.options)["nodes"][0]
    assert node["slurm_node_state"] == ("down*" if "down*" in states else "idle~")


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
