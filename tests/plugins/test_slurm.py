##
## qtop is a tool to monitor queuing systems - https://github.com/qtop/qtop
##
## Copyright (c) 2026 Nicola Trozzi
##
## SPDX-License-Identifier: MIT
##

import os

import pytest

from qtop_py.plugins.slurm import SlurmBatchSystem, SlurmStatExtractor


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
    queue_counts = dict((queue["queue_name"], (queue["run"], queue["queued"])) for queue in queues)
    assert queue_counts == expected_queues[2]

    worker_nodes = dict((node["domainname"], node) for node in slurm.get_worker_nodes(expected_jobs[0], expected_jobs[3], Options()))
    assert set(worker_nodes) == set(expected_nodes)
    for node_name, expected in expected_nodes.items():
        expected_state, expected_qnames, expected_core_map = expected
        assert worker_nodes[node_name]["state"] == expected_state
        assert worker_nodes[node_name]["qname"] == expected_qnames
        assert worker_nodes[node_name]["core_job_map"] == expected_core_map


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
