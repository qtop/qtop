## Copyright (c) 2026 qtop contributors
## SPDX-License-Identifier: MIT
"""Shared queue policy contracts; parser and configuration regressions stay focused."""

import pytest

from qtop_py.qtop import queue_is_blocked, scheduler_backend
from qtop_py.queue_nodes import queue_node_counts
from qtop_py.sample import sanitize_config
from qtop_py.yaml_parser import safe_load


def node(name, queues, **extra):
    return dict({"domainname": name, "qname": queues, "state": "-", "core_job_map": {}}, **extra)


@pytest.mark.parametrize(
    "scheduler,nodes,queues,expected",
    [
        pytest.param(
            "slurm",
            [
                node("node001", ["batch"]),
                node("node001", ["batch"]),
                node("node002", ["batch"], core_job_map={0: "101"}),
                node("node003", ["batch", "gpu"], slurm_node_state="idle~"),
                node("node004", ["gpu"], slurm_job_ids=["102"]),
            ],
            ["batch", "gpu", "missing"],
            [("batch", 1, 3), ("gpu", 0, 2), ("missing", None, None)],
            id="slurm-native-membership-deduplicates-and-excludes-unavailable",
        ),
        *[
            pytest.param(scheduler, [node("node001", ["batch"])], ["batch"], [("batch", None, None)], id=scheduler + "-observed-jobs-do-not-prove-eligibility")
            for scheduler in ("pbs", "oar")
        ],
        pytest.param(
            "slurm",
            [node("node001", ["batch"], state="d"), node("node002", ["batch"], slurm_gpu_used=[{"count": 1}])],
            ["batch"],
            [("batch", 0, 2)],
            id="slurm-disabled-and-gpu-allocated-nodes-are-busy",
        ),
    ],
)
def test_queue_membership_contract(scheduler, nodes, queues, expected):
    assert queue_node_counts(nodes, queues, scheduler_backend(scheduler)) == expected


def test_yaml_mapping_handles_padded_ranges_overlap_and_missing_nodes(tmp_path):
    path = tmp_path / "config.yaml"
    path.write_text("experimental_queue_nodes:\n  old: node[001-002]\n  shared: node001,node001\n  absent: node003\n")
    mapping = safe_load(str(path))["experimental_queue_nodes"]
    nodes = [node("node001", []), node("node002", [], oar_job_ids=["101"])]
    assert queue_node_counts(nodes, [], scheduler_backend("oar"), mapping) == [("old", 1, 2), ("shared", 1, 1), ("absent", None, 1)]
    masked, omitted = sanitize_config({"experimental_queue_nodes": mapping}, {"user": {}, "node": {}, "queue": {}})
    assert "experimental_queue_nodes" not in masked
    assert "experimental_queue_nodes" in omitted


@pytest.mark.parametrize(
    "mapping",
    [True, [], {"": "node001"}, {"batch": "node[003-001]"}, {"batch": ["node001"]}],
    ids=["boolean", "list", "empty-queue-name", "descending-host-range", "nontext-host-pattern"],
)
def test_invalid_membership_mappings_are_rejected(mapping):
    with pytest.raises(ValueError):
        queue_node_counts([], [], scheduler_backend("pbs"), mapping)


@pytest.mark.parametrize(
    "scheduler,state,blocked",
    [
        pytest.param(scheduler, state, blocked, id="%s-%s" % (scheduler or "default", state))
        for scheduler in (None, "pbs", "slurm", "oar")
        for state, blocked in (("D", True), ("DS", True), ("E", False), ("Q", False))
    ]
    + [
        pytest.param("slurm", state, blocked, id="slurm-partition-" + state)
        for state, blocked in (("up", False), ("UP", False), ("down", True), ("drain", True), ("inact", True), ("INACTIVE", True), ("?", False))
    ],
)
def test_queue_blocking_contract(scheduler, state, blocked):
    assert queue_is_blocked(state, scheduler) is blocked


@pytest.mark.parametrize(
    "state,blocked",
    [pytest.param(state, True, id="blocked-" + state) for state in ("a", "A", "c", "C", "d", "D", "E", "o", "s", "S", "u", "Sdu")]
    + [pytest.param(state, False, id="neutral-" + (state or "empty")) for state in ("", "-", "?", "r")],
)
def test_sge_queue_blocking_flags(state, blocked):
    assert bool(queue_is_blocked(state, "sge")) is blocked
