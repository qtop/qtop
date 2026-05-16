##
## qtop is a tool to monitor queuing systems - https://github.com/qtop/qtop
##
## Copyright (c) 2026 qtop contributors
##
## SPDX-License-Identifier: MIT
##

import os

import pytest

from qtop_py.plugins.slurm import SlurmBatchSystem, normalize_job_state, normalize_node_state


class Options(object):
    ANONYMIZE = False


class AnonymizeOptions(object):
    ANONYMIZE = True


def scheduler_files(sample_name):
    sample_dir = os.path.join(os.path.dirname(__file__), "slurm_samples", sample_name)
    return {
        "squeue_file": os.path.join(sample_dir, "squeue.txt"),
        "sinfo_file": os.path.join(sample_dir, "sinfo.txt"),
    }


def build_slurm(sample_name, options=None):
    return SlurmBatchSystem(scheduler_files(sample_name), {}, options or Options())


def repeated_core_map(*job_counts):
    core_job_map = {}
    for job_id, count in job_counts:
        start_core = len(core_job_map)
        for core in range(start_core, start_core + count):
            core_job_map[core] = job_id
    return core_job_map


@pytest.mark.parametrize(
    "sample_name, expected_jobs, expected_queues, expected_nodes",
    (
        (
            "basic",
            (["1001", "1002", "1003"], ["alice", "bob", "carol"], ["R", "R", "PD"], ["compute", "compute", "debug"]),
            (2, 1, {"compute": ("2", "0"), "debug": ("0", "1")}),
            {
                "node001": ("%", ["compute"], repeated_core_map(("1001", 8), ("1002", 6))),
                "node002": ("a", ["compute"], repeated_core_map(("1002", 6))),
                "node003": ("-", ["debug"], {}),
                "node004": ("d", ["debug"], {}),
            },
        ),
        (
            "multi_partition",
            (["2001", "2002", "2003", "2004"], ["dana", "erin", "frank", "grace"], ["R", "CG", "PD", "R"], ["gpu", "long", "gpu", "interactive"]),
            (3, 1, {"gpu": ("1", "1"), "long": ("1", "0"), "interactive": ("1", "0")}),
            {
                "gpu01": ("%", ["gpu", "long"], 80),
                "gpu02": ("-", ["gpu"], 0),
                "gpu03": ("d", ["gpu"], 0),
                "login01": ("a", ["interactive"], 2),
            },
        ),
        (
            "edge_cases",
            (["3001_7", "3002", "3003"], ["heidi", "ivan", "judy"], ["R", "S", "RH"], ["batch", "debug", "batch"]),
            (1, 1, {"batch": ("1", "1"), "debug": ("0", "0")}),
            {
                "compute001": ("a", ["batch"], 12),
                "compute002": ("%", ["batch"], 12),
                "compute003": ("%", ["batch"], 12),
                "compute007": ("%", ["batch"], 12),
                "shared01": ("r", ["debug"], 2),
            },
        ),
    ),
)
def test_slurm_command_traces_cover_qtop_contract(sample_name, expected_jobs, expected_queues, expected_nodes):
    slurm = build_slurm(sample_name)

    assert slurm.get_jobs_info() == expected_jobs

    total_running, total_queued, queues = slurm.get_queues_info()
    assert (total_running, total_queued) == expected_queues[:2]
    queue_counts = dict((queue["queue_name"], (queue["run"], queue["queued"])) for queue in queues)
    assert queue_counts == expected_queues[2]

    worker_nodes = dict((node["domainname"], node) for node in slurm.get_worker_nodes(expected_jobs[0], expected_jobs[3], Options()))
    assert set(worker_nodes) == set(expected_nodes)
    for node_name, expected in expected_nodes.items():
        expected_state, expected_qnames, expected_core_map = expected
        assert worker_nodes[node_name]["state"] == expected_state
        assert worker_nodes[node_name]["qname"] == expected_qnames
        if isinstance(expected_core_map, int):
            assert len(worker_nodes[node_name]["core_job_map"]) == expected_core_map
        else:
            assert worker_nodes[node_name]["core_job_map"] == expected_core_map


@pytest.mark.parametrize(
    "nodelist, expected",
    (
        ("node001", ["node001"]),
        ("node[001-003]", ["node001", "node002", "node003"]),
        ("gpu[01-02,04]", ["gpu01", "gpu02", "gpu04"]),
        ("rack[01-02]node[001-002]", ["rack01node001", "rack01node002", "rack02node001", "rack02node002"]),
        ("node[001-002],gpu[01-02,04]", ["node001", "node002", "gpu01", "gpu02", "gpu04"]),
        ("(Priority)", []),
    ),
)
def test_expand_nodelist(nodelist, expected):
    assert SlurmBatchSystem._expand_nodelist(nodelist) == expected


@pytest.mark.parametrize(
    "state, expected",
    (
        ("RUNNING", "R"),
        ("PENDING", "PD"),
        ("COMPLETING", "CG"),
        ("REQUEUE_HOLD", "RH"),
        ("S", "S"),
    ),
)
def test_normalize_job_state(state, expected):
    assert normalize_job_state(state) == expected


@pytest.mark.parametrize(
    "state, expected",
    (
        ("idle", "-"),
        ("alloc", "a"),
        ("mixed", "%"),
        ("drain*", "d"),
        ("reserved", "r"),
    ),
)
def test_normalize_node_state(state, expected):
    assert normalize_node_state(state) == expected


def test_slurm_anonymization_uses_qtop_anonymizer():
    slurm = build_slurm("basic", AnonymizeOptions())

    job_ids, users, job_states, queues = slurm.get_jobs_info()
    assert job_ids == ["1001", "1002", "1003"]
    assert job_states == ["R", "R", "PD"]
    assert users == ["a_anon_user_0", "b_anon_user_1", "c_anon_user_2"]
    assert queues == ["c_anon_q_0", "c_anon_q_0", "d_anon_q_1"]

    worker_nodes = slurm.get_worker_nodes(job_ids, queues, AnonymizeOptions())
    assert worker_nodes[0]["domainname"] == "n_anon_wn_0"
    assert worker_nodes[0]["qname"] == ["c_anon_q_0"]
