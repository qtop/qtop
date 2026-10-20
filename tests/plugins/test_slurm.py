##
## qtop is a tool to monitor queuing systems - https://github.com/qtop/qtop
##
## Copyright (c) 2026 Nicola Trozzi
##
## SPDX-License-Identifier: MIT
##

import os

import pytest

from qtop_py.plugins.slurm import SLURM_JOB_STATES, SlurmBatchSystem, SlurmStatExtractor


class Options(object):
    ANONYMIZE = False


def scheduler_files(sample_name):
    sample_dir = os.path.join(os.path.dirname(__file__), "slurm_samples", sample_name)
    return {
        "squeue_file": os.path.join(sample_dir, "squeue.txt"),
        "sinfo_file": os.path.join(sample_dir, "sinfo.txt"),
    }


@pytest.mark.parametrize(
    "sample_name, expected_jobs, expected_queues, expected_nodes",
    (
        (
            "basic",
            (["101", "102", "103"], ["alice", "bob", "carol"], ["R", "R", "PD"], ["compute", "compute", "long"]),
            (2, 1, {"compute": ("2", "0"), "long": ("0", "1")}),
            {
                "node001": ("-", ["compute"], {0: "101", 1: "101"}),
                "node002": ("b", ["compute"], {0: "102", 1: "102"}),
                "node003": ("-", ["long"], {}),
            },
        ),
        (
            "mixed",
            (["201", "202", "203", "204"], ["alice", "dave", "erin", "frank"], ["R", "R", "CG", "CA"], ["gpu", "gpu", "gpu", "gpu"]),
            (2, 0, {"gpu": ("2", "0")}),
            {
                "gpu001": ("b", ["gpu"], {0: "201", 1: "201", 2: "202", 3: "203"}),
                "gpu002": ("d", ["gpu"], {}),
            },
        ),
        (
            "multi_partition",
            (["301", "302", "303"], ["frank", "grace", "heidi"], ["R", "PD", "R"], ["debug", "compute", "compute"]),
            (2, 1, {"compute": ("1", "1"), "debug": ("1", "0")}),
            {
                "shared001": ("b", ["compute", "debug"], {0: "301", 1: "301", 2: "303"}),
                "shared002": ("-", ["compute"], {0: "303"}),
            },
        ),
    ),
)
def test_slurm_command_traces(sample_name, expected_jobs, expected_queues, expected_nodes):
    slurm = SlurmBatchSystem(scheduler_files(sample_name), {}, Options())

    assert slurm.get_jobs_info() == expected_jobs

    total_running, total_queued, queues = slurm.get_queues_info()
    assert (total_running, total_queued) == expected_queues[:2]
    if sample_name == "multi_partition":
        assert [queue["queue_name"] for queue in queues] == ["compute", "debug"]
    queue_counts = dict((queue["queue_name"], (queue["run"], queue["queued"])) for queue in queues)
    assert queue_counts == expected_queues[2]

    worker_nodes = dict((node["domainname"], node) for node in slurm.get_worker_nodes(expected_jobs[0], expected_jobs[3], Options()))
    assert set(worker_nodes) == set(expected_nodes)
    for node_name, expected in expected_nodes.items():
        expected_state, expected_qnames, expected_core_map = expected
        assert worker_nodes[node_name]["state"] == expected_state
        assert worker_nodes[node_name]["qname"] == expected_qnames
        assert worker_nodes[node_name]["core_job_map"] == expected_core_map


def test_slurm_exposes_plain_cluster_state():
    state = SlurmBatchSystem(scheduler_files("basic"), {}, Options()).get_cluster_state(Options())

    assert list(state) == ["jobs", "nodes", "queues"]
    assert state["jobs"][0] == {"id": "101", "user": "alice", "state": "R", "queue": "compute"}
    assert state["nodes"][0]["domainname"] == "node001"
    assert state["queues"][0]["name"] == "compute"


@pytest.mark.parametrize(
    "sample_name, expected_nodes, expected_cores",
    (
        ("large_cluster", 18, 288),
        ("large_mixed", 20, 320),
        ("large_multi_partition", 18, 288),
    ),
)
def test_large_slurm_cluster_samples_exceed_256_cores(sample_name, expected_nodes, expected_cores):
    slurm = SlurmBatchSystem(scheduler_files(sample_name), {}, Options())

    total_running, total_queued, queues = slurm.get_queues_info()
    assert total_running >= 3
    assert total_queued == 1
    assert queues

    job_ids, _usernames, _job_states, job_queues = slurm.get_jobs_info()
    worker_nodes = dict((node["domainname"], node) for node in slurm.get_worker_nodes(job_ids, job_queues, Options()))
    assert len(worker_nodes) == expected_nodes
    assert sum(int(node["np"]) for node in worker_nodes.values()) == expected_cores
    assert expected_cores > 256


@pytest.mark.parametrize(
    "nodelist, expected",
    (
        ("node001", ["node001"]),
        ("node[001-003]", ["node001", "node002", "node003"]),
        ("gpu[01-02,04]", ["gpu01", "gpu02", "gpu04"]),
        ("rack[01-02]node[001-002]", ["rack01node001", "rack01node002", "rack02node001", "rack02node002"]),
        ("(Priority)", []),
        ("", []),
    ),
)
def test_expand_nodelist(nodelist, expected):
    assert SlurmBatchSystem.expand_nodelist(nodelist) == expected


@pytest.mark.parametrize(
    "raw_state, mapped",
    (
        ("idle", "-"),
        ("alloc*", "b"),
        ("mix+", "b"),
        ("drain", "d"),
        ("down#", "d"),
        ("resv", "r"),
        ("future", "?"),
    ),
)
def test_map_node_state(raw_state, mapped):
    assert SlurmStatExtractor._map_node_state(raw_state) == mapped


@pytest.mark.parametrize("long_state, compact_state", sorted(SLURM_JOB_STATES.items()))
def test_map_all_documented_slurm_job_states(long_state, compact_state):
    assert SlurmStatExtractor._map_job_state(long_state) == compact_state
    assert SlurmStatExtractor._map_job_state(compact_state) == compact_state


@pytest.mark.parametrize(
    "raw_state, mapped",
    (
        ("allocated", "b"),
        ("blocked", "d"),
        ("completing", "c"),
        ("down*", "d"),
        ("drained", "d"),
        ("draining@", "d"),
        ("fail", "d"),
        ("failing^", "d"),
        ("future", "?"),
        ("idle", "-"),
        ("maint$", "d"),
        ("mixed", "b"),
        ("planned-", "-"),
        ("power_down~", "d"),
        ("power_up#", "c"),
        ("reserved", "r"),
        ("unknown", "?"),
    ),
)
def test_map_all_documented_slurm_node_states(raw_state, mapped):
    assert SlurmStatExtractor._map_node_state(raw_state) == mapped


@pytest.mark.parametrize(
    "nodelist, expected",
    (
        ("node001,node003", ["node001", "node003"]),
        ("node[001-002],gpu[01,03-04],login", ["node001", "node002", "gpu01", "gpu03", "gpu04", "login"]),
        ("rack[1-2]node[01-02],other", ["rack1node01", "rack1node02", "rack2node01", "rack2node02", "other"]),
        ("node[01-02],node02,node[02-03]", ["node01", "node02", "node03"]),
        ("  node[01-02]  ", ["node01", "node02"]),
        (None, []),
    ),
)
def test_expand_host_groups(nodelist, expected):
    assert SlurmBatchSystem.expand_nodelist(nodelist) == expected


def test_malformed_host_lists_terminate_without_partial_results():
    # Run in a child so reintroducing the unmatched-bracket loop fails promptly.
    import json
    import subprocess
    import sys

    invalid = [
        "node[",
        "node]",
        "node[]",
        "node[[01]]",
        "node[01-02",
        "node[01]]",
        "node[a-b]",
        "node[1-x]",
        "node[-1]",
        "node[2-1]",
        "node[1-2-3]",
        "node[1,,2]",
        "node[,1]",
        "node[1,]",
        ",node01",
        "node01,",
        "node01,,node02",
        "node 01",
        "node[0-1000000000000]",
        "rack[000-999]node[000-999]",
        "node01,node[]",
    ]
    script = (
        "import json, sys; from qtop_py.plugins.slurm import SlurmBatchSystem; print(json.dumps([SlurmBatchSystem.expand_nodelist(value) for value in json.loads(sys.argv[1])]))"
    )
    completed = subprocess.run(
        [sys.executable, "-c", script, json.dumps(invalid)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        universal_newlines=True,
        timeout=10,
        check=True,
    )
    assert json.loads(completed.stdout) == [[] for _ in invalid]


def test_host_expansion_limit(monkeypatch, caplog):
    monkeypatch.setattr(SlurmBatchSystem, "MAX_EXPANDED_NODES", 3)
    assert SlurmBatchSystem.expand_nodelist("node[1-3]") == ["node1", "node2", "node3"]
    for value in ("node[1-4]", "rack[1-2]node[1-2]", "node[1-2],other[1-2]"):
        assert SlurmBatchSystem.expand_nodelist(value) == []
    assert "too many expanded hosts" in caplog.text


@pytest.mark.parametrize("anonymize", (False, True))
def test_partial_and_malformed_synthetic_snapshot(anonymize, caplog):
    options = Options()
    options.ANONYMIZE = anonymize
    slurm = SlurmBatchSystem(scheduler_files("partial_and_malformed"), {}, options)
    ids, users, states, queues = slurm.get_jobs_info()
    assert ids == ["701", "702", "703", "704", "705", "706"]
    assert states == ["R", "PD", "NEW_STATE", "R", "R", "R"]
    assert len(set(users)) == 6
    assert slurm._get_jobs()[2]["raw_state"] == "NEW_STATE"
    assert slurm.get_queues_info()[:2] == (4, 1)
    nodes = slurm.get_worker_nodes(ids, queues, options)
    assert len(nodes) == 3
    assert sum(int(node["np"]) for node in nodes) == 12
    assert [node["core_job_map"] for node in nodes] == [{0: "701", 1: "701"}, {0: "701", 1: "701"}, {0: "706"}]
    assert nodes[2]["state"] == "?"
    assert "not properly parsed" in caplog.text
    assert "Ignoring invalid Slurm nodelist" in caplog.text


@pytest.mark.parametrize(
    "squeue, sinfo, expected_jobs, expected_nodes",
    (
        ("", "", 0, 0),
        ("", "node01|compute|idle|4\n", 0, 1),
        ("1|alice|R|compute|4|node01\n", "", 1, 0),
    ),
)
def test_empty_or_nonconcurrent_snapshots(tmp_path, squeue, sinfo, expected_jobs, expected_nodes):
    files = {}
    for key, contents in (("squeue_file", squeue), ("sinfo_file", sinfo)):
        path = tmp_path / key
        path.write_text(contents, encoding="utf-8")
        files[key] = str(path)
    slurm = SlurmBatchSystem(files, {}, Options())
    state = slurm.get_cluster_state(Options())
    assert len(state["jobs"]) == expected_jobs
    assert len(state["nodes"]) == expected_nodes
    assert all(not node["core_job_map"] for node in state["nodes"])


@pytest.mark.parametrize("cpus, expected_job_cpus, expected_node_cpus", (("", 1, "0"), ("bad", 1, "0"), ("-2", 1, "0"), ("0", 1, "0")))
def test_unusable_cpu_counts(tmp_path, cpus, expected_job_cpus, expected_node_cpus):
    queue = tmp_path / "squeue.txt"
    info = tmp_path / "sinfo.txt"
    queue.write_text("1|alice|R|compute|%s|node01\n" % cpus, encoding="utf-8")
    info.write_text("node01|compute||%s\n" % cpus, encoding="utf-8")
    extractor = SlurmStatExtractor({}, Options())
    assert extractor.extract_squeue(str(queue))[0]["CPUs"] == expected_job_cpus
    node = extractor.extract_sinfo(str(info))[0]
    assert node["np"] == expected_node_cpus
    assert node["state"] == "?"
