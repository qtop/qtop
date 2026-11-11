## Copyright (c) 2026 qtop contributors
## SPDX-License-Identifier: MIT

import json

import pytest

from qtop_py.cluster_state import build_cluster_state, cluster_state_totals, validate_cluster_state
from qtop_py.qtop import Snapshot


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


def test_cluster_state_hands_nodes_over_and_snapshot_isolates_them():
    """Ownership: the builder passes nodes through uncopied; Snapshot is the one copy boundary."""
    nodes = [{"domainname": "node001", "np": 2, "state": "j", "qname": ["debug"], "core_job_map": {"0": "1"}}]
    state = build_cluster_state(["1"], ["alice"], ["R"], ["debug"], nodes, [])
    assert state["nodes"][0] is nodes[0]

    snapshot = Snapshot.from_cluster_state(state)
    nodes[0]["qname"].append("batch")
    nodes[0]["core_job_map"]["1"] = "1"

    assert snapshot.worker_nodes[0]["qname"] == ["debug"]
    assert snapshot.worker_nodes[0]["core_job_map"] == {"0": "1"}
    assert snapshot.to_cluster_state()["nodes"][0]["qname"] == ["debug"]


def test_snapshot_round_trip_and_save_use_cluster_state(tmp_path):
    state = sample_state()
    snapshot = Snapshot.from_cluster_state(state)
    output = tmp_path / "cluster-state.json"

    snapshot.save(str(output))

    assert snapshot.to_cluster_state() == state
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


def test_snapshot_owns_lossless_state_independently_of_input_and_rendering_views(tmp_path):
    import copy

    state = sample_state()
    state["jobs"][0]["slurm_tres_alloc"] = "cpu=2,gres/gpu=1"
    state["queues"][0]["site_description"] = "retained metadata"
    state["slurm_diagnostics"] = {"jobs_with_missing_nodes": 1}
    expected = copy.deepcopy(state)
    snapshot = Snapshot.from_cluster_state(state)
    state["jobs"][0]["user"] = "changed input"
    snapshot.worker_nodes[0]["qname"].append("render-only queue")
    snapshot.jobs_dict.clear()
    snapshot.queues_dict.clear()
    exported = snapshot.to_cluster_state()
    assert exported == expected
    exported["nodes"][0]["qname"].append("changed export")
    assert snapshot.to_cluster_state() == expected
    path = tmp_path / "state.json"
    snapshot.save(str(path))
    assert json.loads(path.read_text()) == expected


@pytest.mark.parametrize("np, valid", [(65536, True), ("65536", True), (65537, False), ("100000000", False)])
def test_node_core_counts_are_bounded_for_every_backend_and_replay(np, valid):
    state = sample_state()
    state["nodes"][0]["np"] = np
    if valid:
        validate_cluster_state(state)
    else:
        with pytest.raises(ValueError, match="must not exceed 65536 cores"):
            validate_cluster_state(state)


def observation(**source):
    state = sample_state()
    state["schema_version"] = 2
    state["source"] = dict({"backend": "slurm", "cluster_id": "cluster-a"}, **source)
    state["jobs"][0].update(qid="slurm:cluster-a:42", kind="job")
    state["nodes"][0]["qid"] = "slurm:cluster-a:node001"
    state["queues"][0]["qid"] = "slurm:cluster-a:" + state["queues"][0]["name"]
    return state


def test_schema_1_states_stay_valid_and_cannot_carry_schema_2_fields():
    assert validate_cluster_state(sample_state()) == sample_state()
    assert validate_cluster_state(dict(sample_state(), schema_version=1))["schema_version"] == 1
    for key, value in (("qid", "slurm:cluster-a:42"), ("kind", "job")):
        state = sample_state()
        state["jobs"][0][key] = value
        with pytest.raises(ValueError, match="schema_version 2"):
            validate_cluster_state(state)
    with pytest.raises(ValueError, match="source requires schema_version 2"):
        validate_cluster_state(dict(sample_state(), source={"backend": "slurm", "cluster_id": "a"}))


@pytest.mark.parametrize("version", [0, 3, "2", True, None])
def test_unknown_schema_versions_are_rejected(version):
    with pytest.raises(ValueError, match="schema_version"):
        validate_cluster_state(dict(observation(), schema_version=version))


@pytest.mark.parametrize(
    "change",
    [
        lambda state: state.pop("source"),
        lambda state: state["source"].update(cluster_id=""),
        lambda state: state["source"].update(cluster_id="a:b"),  # ":" would make qid namespaces ambiguous
        lambda state: state["source"].update(region="eu"),
        lambda state: state["jobs"][0].update(qid="slurm:cluster-b:42"),  # another cluster's namespace
        lambda state: state["jobs"][0].update(qid="slurm:cluster-a:"),
        lambda state: state["jobs"][0].update(qid=42),
        lambda state: state["nodes"][0].update(qid="slurm:cluster-a:42"),  # not the node's domainname
        lambda state: state["jobs"][0].update(qid="slurm:cluster-a:43"),  # not the job's id
        lambda state: state["jobs"][0].update(qid="slurm:cluster-a:42:x"),
        lambda state: state["jobs"][0].update(qid="SLURM:cluster-a:42"),  # another backend's namespace
        lambda state: state["jobs"].append(dict(state["jobs"][0])),  # a duplicate job qid
        lambda state: state["queues"][0].update(qid="slurm:cluster-a:other"),  # not the queue's name
        lambda state: state["queues"].append(dict(state["queues"][0])),  # a duplicate queue qid
        lambda state: state["jobs"][0].update(kind="container"),
        lambda state: state["nodes"][0].update(kind="vm"),
    ],
)
def test_schema_2_envelope_and_identities_are_unambiguous(change):
    state = observation()
    change(state)
    with pytest.raises((TypeError, ValueError)):
        validate_cluster_state(state)


@pytest.mark.parametrize("version", [1, 2])
def test_top_level_is_closed_so_unknown_collections_are_rejected_not_dropped(version):
    # Issue #593: a new collection needs a new schema version; until then an unknown key is an error, by name.
    state = observation() if version == 2 else sample_state()
    state["allocations"] = []
    with pytest.raises(ValueError, match="unknown in schema_version %d: allocations" % version):
        validate_cluster_state(state)


def test_qids_are_unique_per_relation_so_a_job_and_node_may_share_a_native_id():
    state = observation()
    state["nodes"][0].update(domainname="42", qid="slurm:cluster-a:42")
    assert validate_cluster_state(state) is state


def test_schema_2_metadata_survives_snapshot_json_round_trip(tmp_path):
    state = observation()
    assert validate_cluster_state(state) is state
    snapshot = Snapshot.from_cluster_state(state)
    assert snapshot.to_cluster_state() == state
    path = tmp_path / "state.json"
    snapshot.save(str(path))
    assert Snapshot.from_cluster_state(json.loads(path.read_text())).to_cluster_state() == state
    assert len(snapshot) == 5 and snapshot.worker_nodes[0]["qid"] == "slurm:cluster-a:node001"
