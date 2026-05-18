from pathlib import Path

import pytest

from qtop_py.plugins.slurm import (
    SlurmBatchSystem,
    SlurmStatExtractor,
    expand_slurm_nodelist,
    map_slurm_node_state,
    parse_slurm_cpu_total,
)


INPUTS = Path(__file__).parents[1] / "inputs"


class Options:
    ANONYMIZE = False


@pytest.mark.parametrize(
    "nodelist, expected",
    (
        ("node001", ["node001"]),
        ("node[001-003,005]", ["node001", "node002", "node003", "node005"]),
        ("rack-a[7-8],rack-b01", ["rack-a7", "rack-a8", "rack-b01"]),
        ("(Resources)", []),
    ),
)
def test_expand_slurm_nodelist(nodelist, expected):
    assert expand_slurm_nodelist(nodelist) == expected


@pytest.mark.parametrize(
    "cpu_field, expected",
    (
        ("2/2/0/4", 4),
        ("8", 8),
        ("", 0),
    ),
)
def test_parse_slurm_cpu_total(cpu_field, expected):
    assert parse_slurm_cpu_total(cpu_field) == expected


@pytest.mark.parametrize(
    "state, expected",
    (
        ("idle", "-"),
        ("allocated", "b"),
        ("mix*", "%"),
        ("drain", "d"),
        ("down", "d"),
        ("completing", "c"),
    ),
)
def test_map_slurm_node_state(state, expected):
    assert map_slurm_node_state(state) == expected


def test_extract_squeue_normalizes_states_and_fields():
    extractor = SlurmStatExtractor({}, Options())

    jobs = extractor.extract_squeue(str(INPUTS / "slurm_squeue_1.txt"))

    assert jobs == [
        {"JobId": "101", "Queue": "batch", "UnixAccount": "alice", "S": "R", "CPUs": 4, "Nodes": "node[001-002]"},
        {"JobId": "102", "Queue": "batch", "UnixAccount": "bob", "S": "PD", "CPUs": 2, "Nodes": "(Resources)"},
        {"JobId": "103", "Queue": "debug", "UnixAccount": "carol", "S": "CG", "CPUs": 1, "Nodes": "node003"},
    ]


@pytest.mark.parametrize("case_id", (1, 2, 3))
def test_slurm_batch_system_reads_trace_cases(case_id):
    batch = SlurmBatchSystem(
        {
            "squeue_file": str(INPUTS / "slurm_squeue_%s.txt" % case_id),
            "sinfo_file": str(INPUTS / "slurm_sinfo_%s.txt" % case_id),
        },
        {},
        Options(),
    )

    job_ids, usernames, job_states, queue_names = batch.get_jobs_info()
    worker_nodes = batch.get_worker_nodes(job_ids, queue_names, Options())
    total_running, total_queued, queue_info = batch.get_queues_info()

    assert len(job_ids) == 3
    assert len(usernames) == 3
    assert len(job_states) == 3
    assert len(queue_names) == 3
    assert worker_nodes
    assert all("domainname" in node for node in worker_nodes)
    assert all("core_job_map" in node for node in worker_nodes)
    assert total_running >= 1
    assert total_queued >= 0
    assert queue_info


def test_running_slurm_jobs_are_mapped_to_nodes():
    batch = SlurmBatchSystem(
        {
            "squeue_file": str(INPUTS / "slurm_squeue_1.txt"),
            "sinfo_file": str(INPUTS / "slurm_sinfo_1.txt"),
        },
        {},
        Options(),
    )

    job_ids, _, _, queues = batch.get_jobs_info()
    nodes = {node["domainname"]: node for node in batch.get_worker_nodes(job_ids, queues, Options())}

    assert set(nodes["node001"]["core_job_map"].values()) == {"101"}
    assert set(nodes["node002"]["core_job_map"].values()) == {"101"}
    assert set(nodes["node003"]["core_job_map"].values()) == {"103"}
    assert nodes["node004"]["core_job_map"] == {}
