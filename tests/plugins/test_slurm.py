##
## qtop is a tool to monitor queuing systems - https://github.com/qtop/qtop
##
## Copyright (c) 2016 Fotis Georgatos
## Copyright (c) 2016 Sotiris Fragkiskos
## Copyright (c) 2023 Hewlett Packard Enterprise Development LP
##
## SPDX-License-Identifier: MIT
##

import os

import pytest

from qtop_py.plugins import slurm


class Options(object):
    ANONYMIZE = False


FIXTURES_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "fixtures", "slurm")


@pytest.mark.parametrize(
    "nodelist, expanded",
    (
        ("cpu[001-003]", ["cpu001", "cpu002", "cpu003"]),
        ("rack-a[01-02],rack-b03", ["rack-a01", "rack-a02", "rack-b03"]),
        ("gpu004", ["gpu004"]),
        ("(null)", []),
    ),
)
def test_expand_slurm_nodelist(nodelist, expanded):
    assert slurm.expand_slurm_nodelist(nodelist) == expanded


@pytest.mark.parametrize(
    "state, compact",
    (
        ("IDLE", "-"),
        ("ALLOCATED", "a"),
        ("MIXED+DRAIN", "m"),
        ("DOWN", "d"),
    ),
)
def test_compact_slurm_node_state(state, compact):
    assert slurm.compact_slurm_node_state(state) == compact


@pytest.mark.parametrize(
    "fixture_name, total_cores, running_jobs, queued_jobs",
    (
        ("cluster_alpha", 512, 4, 1),
        ("cluster_beta", 320, 5, 1),
        ("cluster_gamma", 384, 4, 1),
    ),
)
def test_slurm_trace_clusters_over_256_cores(fixture_name, total_cores, running_jobs, queued_jobs):
    fixture_dir = os.path.join(FIXTURES_DIR, fixture_name)
    batch = slurm.SlurmBatchSystem(
        {
            "scontrol_file": os.path.join(fixture_dir, "scontrol_show_nodes.txt"),
            "squeue_file": os.path.join(fixture_dir, "squeue.txt"),
            "sinfo_file": os.path.join(fixture_dir, "sinfo.txt"),
        },
        {},
        Options(),
    )

    job_ids, usernames, job_states, job_queues = batch.get_jobs_info()
    worker_nodes = batch.get_worker_nodes(job_ids, job_queues, Options())
    total_running_jobs, total_queued_jobs, queues = batch.get_queues_info()

    assert sum(int(worker_node["np"]) for worker_node in worker_nodes) == total_cores
    assert total_cores > 256
    assert total_running_jobs == running_jobs
    assert total_queued_jobs == queued_jobs
    assert len(job_ids) == len(usernames) == len(job_states) == len(job_queues)
    assert queues
    assert any(worker_node["core_job_map"] for worker_node in worker_nodes)
    assert all("qname" in worker_node for worker_node in worker_nodes)
