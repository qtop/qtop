##
## qtop is a tool to monitor queuing systems - https://github.com/qtop/qtop
##
## Copyright (c) 2026 Nicola Trozzi
##
## SPDX-License-Identifier: MIT
##

import os
from pathlib import Path

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
    assert state["jobs"][0] == {"id": "101", "user": "alice", "state": "R", "queue": "compute", "slurm_partitions": ["compute"], "accounting_state": "running"}
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


@pytest.fixture
def detailed_slurm(tmp_path):
    source = Path(__file__).parent / "slurm_samples/cpu_allocations"
    files = {}
    for command in ("squeue", "sinfo", "scontrol"):
        path = tmp_path / (command + ".txt")
        path.write_bytes((source / path.name).read_bytes())
        files[command + "_file"] = str(path)
    return SlurmBatchSystem(files, {}, Options()), tmp_path / "scontrol.txt"


def test_detailed_allocations_keep_sparse_cpu_ids_and_uneven_distribution(detailed_slurm):
    slurm, _ = detailed_slurm
    state = slurm.get_cluster_state(Options())
    nodes = {node["domainname"]: node for node in state["nodes"]}
    assert nodes["node001"]["core_job_map"] == {4: "101", 0: "102", 2: "102"}
    assert nodes["node002"]["core_job_map"] == {1: "101", 3: "101", 5: "101"}
    assert [node["cpu_allocation_source"] for node in state["nodes"]] == ["slurm", "slurm", "none"]
    assert nodes["node001"]["slurm_gres"] == "gpu:a100:4(S:0-1)"
    assert nodes["node002"]["slurm_gres_used"] == "gpu:a100:1(IDX:2)"
    assert state["jobs"][0]["slurm_tres_alloc"] == "cpu=4,gres/gpu=2"
    assert state["jobs"][0]["slurm_tres_per_node"] == "gres/gpu:a100:1"
    assert state["jobs"][2]["slurm_tres_alloc"] == "cpu=1,gres/gpu=1"


@pytest.mark.parametrize(
    "details",
    [
        None,
        "",
        "malformed",
        "JobId=101\n   Nodes=node001 CPU_IDs=4\n",
        "JobId=101\n   Nodes=node001 CPU_IDs=99\n   Nodes=node002 CPU_IDs=1,3,5\n",
        "JobId=101\n   Nodes=node001 CPU_IDs=0-1000000000\n",
        "JobId=101\n   Nodes=node001 CPU_IDs=1,01\n",
        "JobId=101\n   Nodes=node001 CPU_IDs=garbage\n",
    ],
)
def test_unusable_cpu_details_fall_back_without_losing_jobs(detailed_slurm, details):
    slurm, path = detailed_slurm
    if details is None:
        path.unlink()
    else:
        path.write_text(details)
    state = slurm.get_cluster_state(Options())
    nodes = state["nodes"]
    assert len(state["jobs"]) == 3
    assert sum(len(node["core_job_map"]) for node in nodes) == 6
    assert [node["cpu_allocation_source"] for node in nodes] == ["estimated", "estimated", "none"]


def test_partial_details_do_not_mix_estimated_positions_with_exact_ids(detailed_slurm):
    slurm, path = detailed_slurm
    path.write_text(path.read_text().split("JobId=102")[0])
    nodes = slurm.get_cluster_state(Options())["nodes"]
    assert nodes[0]["cpu_allocation_source"] == "estimated"
    assert nodes[1]["cpu_allocation_source"] == "slurm"
    assert nodes[1]["core_job_map"] == {1: "101", 3: "101", 5: "101"}


def test_overlapping_cpu_allocations_use_estimates(detailed_slurm):
    slurm, path = detailed_slurm
    path.write_text(path.read_text().replace("CPU_IDs=0,2", "CPU_IDs=4,6"))
    nodes = slurm.get_cluster_state(Options())["nodes"]
    assert nodes[0]["cpu_allocation_source"] == "estimated"
    assert nodes[1]["cpu_allocation_source"] == "slurm"


@pytest.mark.parametrize("header, job_id", [("JobId=501 ArrayJobId=101 ArrayTaskId=3", "101_3"), ("JobId=501 HetJobId=101 HetJobOffset=1", "101+1")])
def test_detailed_allocations_match_array_and_heterogeneous_ids(detailed_slurm, header, job_id):
    slurm, path = detailed_slurm
    jobs = slurm._get_jobs()
    jobs[0]["JobId"] = job_id
    path.write_text(path.read_text().replace("JobId=101 JobName=parallel", header))
    nodes = slurm.get_cluster_state(Options())["nodes"]
    assert nodes[0]["core_job_map"][4] == job_id
    assert nodes[1]["core_job_map"][5] == job_id


def test_anonymized_details_preserve_provenance_and_mask_resource_text(detailed_slurm):
    from qtop_py.sample import anonymize_state

    slurm, _ = detailed_slurm
    state, mappings = anonymize_state(slurm.get_cluster_state(Options()))
    assert state["nodes"][0]["cpu_allocation_source"] == "slurm"
    assert "slurm_gres" not in state["nodes"][0]
    assert "slurm_tres_alloc" not in state["jobs"][0]
    assert state["nodes"][0]["core_job_map"]["4"] == mappings["job"]["101"]


@pytest.mark.parametrize(
    "job_id, expected",
    [
        ("101_2", ["101_2"]),
        ("201+1", ["201+1"]),
        ("101_[3]", ["101_3"]),
        ("101_[1-5:2,7,9%2]", ["101_1", "101_3", "101_5", "101_7", "101_9"]),
        ("101_[1-3,2-4]", ["101_1", "101_2", "101_3", "101_4"]),
    ],
)
def test_slurm_expands_complete_grouped_array_ids(job_id, expected):
    assert SlurmStatExtractor.expand_job_id(job_id) == expected


@pytest.mark.parametrize(
    "job_id",
    [
        "101_[1-...]",
        "101_[1-3",
        "101_[]",
        "101_[3-1]",
        "101_[1-3:0]",
        "101_[1-3%0]",
        "101_[1,,3]",
        "101_[1-100001]",
        "101_[12345678901]",
    ],
)
def test_slurm_rejects_unusable_array_groups(job_id):
    with pytest.raises(ValueError, match="collect squeue with -r/--array"):
        SlurmStatExtractor.expand_job_id(job_id)


def test_slurm_bounds_total_legacy_array_expansion(tmp_path, monkeypatch):
    monkeypatch.setattr(SlurmStatExtractor, "MAX_EXPANDED_TASKS", 3)
    queue = tmp_path / "squeue.txt"
    queue.write_text("101_[0-1]|alice|PD|compute|1|(Priority)\n102_[0-1]|bob|PD|compute|1|(Priority)\n")
    with pytest.raises(ValueError, match="Too many grouped Slurm array tasks"):
        SlurmStatExtractor({}, Options()).extract_squeue(str(queue))


@pytest.mark.parametrize("conflict", [False, True])
def test_duplicate_array_records_do_not_inflate_or_shift_accounting(tmp_path, conflict):
    queue = tmp_path / "squeue.txt"
    queue.write_text("101_[1-3]|alice|PD|compute|1|(Priority)\n101_2|%s|PD|compute|1|(Priority)\n" % ("bob" if conflict else "alice"))
    extractor = SlurmStatExtractor({}, Options())
    if conflict:
        with pytest.raises(ValueError, match="Conflicting Slurm records for job 101_2"):
            extractor.extract_squeue(str(queue))
    else:
        assert [job["JobId"] for job in extractor.extract_squeue(str(queue))] == ["101_1", "101_2", "101_3"]


@pytest.mark.parametrize(
    "header, expected",
    [
        ("JobId=501 ArrayTaskId=3  ArrayJobId=101 JobName=job", "101_3"),
        ("JobId=501 HetJobOffset=0\tHetJobId=201 JobName=job", "201+0"),
        ("JobId=501 ArrayJobId=101 ArrayTaskId=3-7 JobName=job", None),
        ("JobId=501 ArrayJobId=101 ArrayTaskId=3%2 JobName=job", None),
        ("JobId=501 ArrayJobId=101 ArrayTaskId=3junk JobName=job", None),
        ("JobId=501 HetJobId=201 HetJobOffset=1-2 JobName=job", None),
        ("JobId=501 ArrayJobId=101 JobName=job", None),
        ("JobId=501 HetJobOffset=1 JobName=job", None),
        ("JobId=501 ArrayJobId=101 ArrayTaskId=3 HetJobId=201 HetJobOffset=1", None),
        ("JobId=501 JobName=ArrayJobId=101 ArrayTaskId=3", "501"),
        ("JobId=501 ArrayJobId=101 ArrayTaskId=3 ArrayTaskId=4", None),
    ],
)
def test_slurm_requires_complete_unambiguous_allocation_identity(header, expected):
    assert SlurmBatchSystem._allocation_job_id(header) == expected


def test_grouped_scontrol_identity_cannot_supply_a_single_tasks_cpu_map(detailed_slurm):
    slurm, path = detailed_slurm
    slurm._get_jobs()[0]["JobId"] = "101_3"
    path.write_text(path.read_text().replace("JobId=101 JobName=parallel", "JobId=501 ArrayJobId=101 ArrayTaskId=3-7 JobName=group"))
    assert "101_3" not in slurm._read_cpu_allocations()
    nodes = slurm.get_cluster_state(Options())["nodes"]
    assert nodes[0]["cpu_allocation_source"] == "estimated"
    assert nodes[1]["cpu_allocation_source"] == "estimated"


def test_slurm_array_task_and_component_counts_match_cpu_ownership():
    files = scheduler_files("arrays_and_het")
    files["scontrol_file"] = str(Path(files["squeue_file"]).with_name("scontrol.txt"))
    slurm = SlurmBatchSystem(files, {}, Options())
    state = slurm.get_cluster_state(Options())
    assert slurm.get_queues_info()[:2] == (4, 4)
    assert [job["id"] for job in state["jobs"]] == ["101_1", "101_2", "101_3", "101_5", "101_7", "101_9", "201+0", "201+1"]
    assert state["nodes"][0]["core_job_map"] == {2: "101_1", 4: "101_1", 0: "201+0"}
    assert state["nodes"][1]["core_job_map"] == {1: "101_2", 5: "101_2", 3: "201+1", 7: "201+1"}
    assert all(node["cpu_allocation_source"] == "slurm" for node in state["nodes"])


def test_slurm_free_text_cannot_supply_cpu_allocation_fields(detailed_slurm):
    slurm, path = detailed_slurm
    path.write_text("JobId=101 JobName=not_allocations Nodes=node001 CPU_IDs=4\n Command=other Nodes=node002 CPU_IDs=1,3,5\n")
    assert slurm._read_cpu_allocations() == {"101": {}}


@pytest.mark.parametrize(
    "display, parent, task, expected",
    [
        ("501", "501", "N/A", ["501"]),
        ("501", "101", "3", ["101_3"]),
        ("101_3", "101", "3", ["101_3"]),
        ("101_[1-...]", "101", "3", ["101_3"]),
        ("101_[3-9:2%2]", "101", "3-9:2%2", ["101_3", "101_5", "101_7", "101_9"]),
        ("201+1", "202", "N/A", ["201+1"]),
    ],
)
def test_explicit_array_columns_determine_canonical_identity(tmp_path, display, parent, task, expected):
    path = tmp_path / "squeue.txt"
    path.write_text("%s|alice|PD|compute|2|(Priority)|cpu=2|N/A|N/A|%s|%s\n" % (display, parent, task))
    jobs = SlurmStatExtractor({}, Options()).extract_squeue(str(path))
    assert [job["JobId"] for job in jobs] == expected
    for job in jobs:
        if "_" in job["JobId"]:
            assert job["slurm_array_job_id"] == parent
            assert job["slurm_array_task_id"] == job["JobId"].split("_", 1)[1]
        else:
            assert "slurm_array_job_id" not in job
    if display == "201+1":
        assert jobs[0]["slurm_het_job_id"] == "201"
        assert jobs[0]["slurm_het_job_offset"] == "1"


@pytest.mark.parametrize(
    "display, parent, task",
    [
        ("101_4", "101", "3"),
        ("101_3", "101", "N/A"),
        ("101_[1-...]", "102", "3"),
        ("201+0", "101", "3"),
        ("501", "", "3"),
        ("501", "101", "junk"),
        ("501", "101", "1-..."),
    ],
)
def test_explicit_array_identity_conflicts_are_actionable(tmp_path, display, parent, task):
    path = tmp_path / "squeue.txt"
    path.write_text("%s|alice|PD|compute|2|(Priority)|cpu=2|N/A|N/A|%s|%s\n" % (display, parent, task))
    with pytest.raises(ValueError, match="Slurm array"):
        SlurmStatExtractor({}, Options()).extract_squeue(str(path))


def test_explicit_array_fields_match_scontrol_internal_job_ids(detailed_slurm):
    slurm, path = detailed_slurm
    queue = Path(slurm.squeue_file)
    rows = queue.read_text().splitlines()
    queue.write_text(rows[0].replace("101|", "501|", 1) + "|101|3\n" + "\n".join(row + "|" + row.split("|", 1)[0] + "|N/A" for row in rows[1:]) + "\n")
    path.write_text(path.read_text().replace("JobId=101 JobName=parallel", "JobId=501 ArrayJobId=101 ArrayTaskId=3 JobName=parallel"))
    state = slurm.get_cluster_state(Options())
    assert state["jobs"][0]["id"] == "101_3"
    assert state["jobs"][0]["slurm_array_job_id"] == "101"
    assert state["jobs"][0]["slurm_array_task_id"] == "3"
    assert state["nodes"][1]["core_job_map"] == {1: "101_3", 3: "101_3", 5: "101_3"}
    assert state["nodes"][1]["cpu_allocation_source"] == "slurm"


@pytest.mark.parametrize("resources", [False, True])
@pytest.mark.parametrize("availability, blocked", [("up", False), ("down", True), ("drain", True), ("inact", True), ("INACTIVE", True), ("", False), ("?", False)])
def test_slurm_partition_availability_is_separate_from_node_state(tmp_path, resources, availability, blocked):
    from qtop_py.qtop import queue_is_blocked

    info = tmp_path / "sinfo.txt"
    queue = tmp_path / "squeue.txt"
    fields = ["node001", "compute*", "down", "8"]
    if resources:
        fields.extend(["gpu:a100:4(S:0-1)", "gpu:a100:1(IDX:2)"])
    info.write_text("|".join(fields + [availability]) + "\n")
    queue.write_text("101|alice|PD|compute|1|(Priority)\n")
    system = SlurmBatchSystem({"sinfo_file": str(info), "squeue_file": str(queue)}, {}, Options())
    nodes = system._get_nodes()
    assert nodes[0]["state"] == "d"
    assert nodes[0]["partition_state"] == (availability.lower() or "?")
    if resources:
        assert nodes[0]["slurm_gres_used"] == "gpu:a100:1(IDX:2)"
    running, queued, queues = system.get_queues_info()
    assert (running, queued) == (0, 1)
    assert queues[0]["state"] == (availability.lower() or "?")
    assert queue_is_blocked(queues[0]["state"], "slurm") is blocked


@pytest.mark.parametrize("resources", [False, True])
def test_legacy_slurm_traces_keep_unknown_partition_availability(tmp_path, resources):
    info = tmp_path / "sinfo.txt"
    info.write_text("node001|compute*|down|8" + ("|(null)|(null)" if resources else "") + "\n")
    system = SlurmBatchSystem({"sinfo_file": str(info)}, {}, Options())
    system._jobs = []
    assert system.get_queues_info()[2][0]["state"] == "?"


@pytest.mark.parametrize("states, expected", [(("up", "down"), "down"), (("up", "drain"), "drain"), (("down", "inact"), "inact"), (("?", "down"), "down")])
@pytest.mark.parametrize("reverse", [False, True])
def test_partition_availability_keeps_blocking_states_across_node_rows(tmp_path, states, expected, reverse):
    info = tmp_path / "sinfo.txt"
    info.write_text("".join("node00%s|compute|idle|8|%s\n" % (index, state) for index, state in enumerate(reversed(states) if reverse else states, 1)))
    system = SlurmBatchSystem({"sinfo_file": str(info)}, {}, Options())
    system._jobs = []
    assert system.get_queues_info()[2][0]["state"] == expected
