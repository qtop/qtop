## SPDX-License-Identifier: MIT

import json

import pytest

from qtop_py.cluster_state import build_cluster_state, cluster_state_totals, validate_cluster_state
from qtop_py.qtop import Document


def sample_state():
    return build_cluster_state(
        ["42[]"],
        ["alice"],
        ["R"],
        ["debug"],
        [{"domainname": "node001", "state": "-", "qname": ["debug"], "np": "2", "core_job_map": {}}],
        [{"queue_name": "debug", "lm": "2", "queued": "0", "run": "1", "state": "E"}],
    )


@pytest.mark.parametrize("diagnostics", [{"rejected_squeue_rows": -1}, {"unknown": 1}, {"jobs_with_missing_nodes": True}, {"rejected_sinfo_rows": "1"}])
def test_invalid_slurm_diagnostics_are_rejected(diagnostics):
    state = sample_state()
    state["slurm_diagnostics"] = diagnostics
    with pytest.raises(ValueError, match="Slurm diagnostics"):
        validate_cluster_state(state)


def test_cluster_state_rejects_nontext_slurm_node_state():
    state = sample_state()
    state["nodes"][0]["slurm_node_state"] = ["idle*"]
    with pytest.raises(ValueError, match="slurm_node_state"):
        validate_cluster_state(state)


def test_cluster_state_is_plain_and_has_only_the_public_keys():
    state = sample_state()

    assert list(state) == ["jobs", "nodes", "queues"]
    assert state["jobs"] == [{"id": "42", "user": "alice", "state": "R", "queue": "debug"}]
    assert state["queues"][0]["running"] == 1
    assert cluster_state_totals(state) == {"running_jobs": 1, "queued_jobs": 0}


def test_cluster_state_copies_nodes_instead_of_sharing_mutable_input():
    nodes = [{"domainname": "node001", "qname": ["debug"]}]
    state = build_cluster_state([], [], [], [], nodes, [])

    state["nodes"][0]["qname"].append("batch")

    assert nodes == [{"domainname": "node001", "qname": ["debug"]}]


def test_document_round_trip_and_save_use_cluster_state(tmp_path):
    state = sample_state()
    document = Document.from_cluster_state(state)
    output = tmp_path / "cluster-state.json"

    document.save(str(output))

    assert document.to_cluster_state() == state
    assert json.loads(output.read_text()) == state


@pytest.mark.parametrize(
    "invalid",
    (
        [],
        {"jobs": [], "nodes": []},
        {"jobs": (), "nodes": [], "queues": []},
    ),
)
def test_cluster_state_rejects_invalid_shapes(invalid):
    with pytest.raises((TypeError, ValueError)):
        validate_cluster_state(invalid)


@pytest.mark.parametrize("source", [[], {}, None, "physical"])
def test_cluster_state_rejects_invalid_cpu_allocation_source(source):
    state = sample_state()
    state["nodes"][0]["cpu_allocation_source"] = source
    with pytest.raises(ValueError, match="cpu_allocation_source"):
        validate_cluster_state(state)


@pytest.mark.parametrize(
    "fields",
    [
        {"slurm_array_job_id": "42"},
        {"slurm_array_task_id": "3"},
        {"slurm_array_job_id": "42", "slurm_array_task_id": "3-7"},
        {"slurm_array_job_id": "42", "slurm_array_task_id": 3},
        {"slurm_array_job_id": "42", "slurm_array_task_id": "4"},
        {"slurm_array_job_id": "42", "slurm_array_task_id": "3", "slurm_het_job_id": "42", "slurm_het_job_offset": "3"},
    ],
)
def test_cluster_state_rejects_inconsistent_structured_job_identity(fields):
    state = sample_state()
    state["jobs"][0]["id"] = "42_3"
    state["jobs"][0].update(fields)
    with pytest.raises(ValueError, match="Slurm identity|both Slurm"):
        validate_cluster_state(state)


@pytest.mark.parametrize(
    "field,value",
    [("slurm_partitions", []), ("slurm_partitions", "debug"), ("slurm_partitions", ["other"]), ("slurm_partitions", [1]), ("slurm_partitions", [""]), ("accounting_state", "R")],
)
def test_invalid_job_membership_metadata_is_rejected(field, value):
    state = sample_state()
    state["jobs"][0][field] = value
    with pytest.raises(ValueError):
        validate_cluster_state(state)


@pytest.mark.parametrize(
    "field,value",
    [("slurm_job_ids", "42"), ("slurm_job_ids", [1]), ("slurm_job_ids", [""]), ("slurm_unmapped_cpus", -1), ("slurm_unmapped_cpus", True), ("slurm_unmapped_cpus", "1")],
)
def test_invalid_node_membership_metadata_is_rejected(field, value):
    state = sample_state()
    state["nodes"][0][field] = value
    with pytest.raises(ValueError):
        validate_cluster_state(state)


def test_mixed_accounting_metadata_is_rejected():
    state = sample_state()
    state["jobs"].append(dict(state["jobs"][0], id="43", accounting_state="running"))
    with pytest.raises(ValueError, match="all jobs or none"):
        validate_cluster_state(state)
