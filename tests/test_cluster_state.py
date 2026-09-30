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
