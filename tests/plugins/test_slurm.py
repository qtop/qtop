##
## qtop is a tool to monitor queuing systems - https://github.com/qtop/qtop
##
## SPDX-License-Identifier: MIT
##

import os

import pytest

from qtop_py.plugins.slurm import SlurmBatchSystem


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
                "node001": ("-", ["compute"], {0: "101"}),
                "node002": ("a", ["compute"], {0: "102"}),
                "node003": ("-", ["long"], {}),
            },
        ),
        (
            "mixed",
            (["201", "202", "203"], ["alice", "dave", "erin"], ["R", "R", "CG"], ["gpu", "gpu", "gpu"]),
            (2, 0, {"gpu": ("2", "0")}),
            {
                "gpu001": ("%", ["gpu"], {0: "201", 1: "202"}),
                "gpu002": ("d", ["gpu"], {}),
            },
        ),
        (
            "multi_partition",
            (["301", "302"], ["frank", "grace"], ["R", "PD"], ["debug", "compute"]),
            (1, 1, {"debug": ("1", "0"), "compute": ("0", "1")}),
            {
                "shared001": ("a", ["compute", "debug"], {0: "301"}),
                "shared002": ("-", ["compute"], {}),
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
    "nodelist, expected",
    (
        ("node001", ["node001"]),
        ("node[001-003]", ["node001", "node002", "node003"]),
        ("gpu[01-02,04]", ["gpu01", "gpu02", "gpu04"]),
        ("rack[01-02]node[001-002]", ["rack01node001", "rack01node002", "rack02node001", "rack02node002"]),
        ("(Priority)", []),
    ),
)
def test_expand_nodelist(nodelist, expected):
    assert SlurmBatchSystem._expand_nodelist(nodelist) == expected
