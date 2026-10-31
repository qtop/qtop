# SPDX-License-Identifier: MIT
import pytest

from qtop_py.qtop import scheduler_backend
from qtop_py.queue_nodes import queue_node_counts
from qtop_py.sample import sanitize_config
from qtop_py.yaml_parser import safe_load


def node(name, queues, **extra):
    return dict({"domainname": name, "qname": queues, "state": "-", "core_job_map": {}}, **extra)


def test_native_membership_counts_idle_nodes_once_and_rejects_unavailable_nodes():
    nodes = [
        node("node001", ["batch"]),
        node("node001", ["batch"]),
        node("node002", ["batch"], core_job_map={0: "101"}),
        node("node003", ["batch", "gpu"], slurm_node_state="idle~"),
        node("node004", ["gpu"], slurm_job_ids=["102"]),
    ]
    assert queue_node_counts(nodes, ["batch", "gpu", "missing"], scheduler_backend("slurm")) == [("batch", 1, 3), ("gpu", 0, 2), ("missing", None, None)]


@pytest.mark.parametrize("scheduler", ["pbs", "oar"])
def test_observed_job_queues_do_not_establish_eligibility(scheduler):
    assert queue_node_counts([node("node001", ["batch"])], ["batch"], scheduler_backend(scheduler)) == [("batch", None, None)]


def test_yaml_mapping_handles_padded_ranges_overlap_and_missing_nodes(tmp_path):
    path = tmp_path / "config.yaml"
    path.write_text("experimental_queue_nodes:\n  old: node[001-002]\n  shared: node001,node001\n  absent: node003\n")
    mapping = safe_load(str(path))["experimental_queue_nodes"]
    nodes = [node("node001", []), node("node002", [], oar_job_ids=["101"])]
    assert queue_node_counts(nodes, [], scheduler_backend("oar"), mapping) == [("old", 1, 2), ("shared", 1, 1), ("absent", None, 1)]
    masked, omitted = sanitize_config({"experimental_queue_nodes": mapping}, {"user": {}, "node": {}, "queue": {}})
    assert "experimental_queue_nodes" not in masked
    assert "experimental_queue_nodes" in omitted


@pytest.mark.parametrize("mapping", [True, [], {"": "node001"}, {"batch": "node[003-001]"}, {"batch": ["node001"]}])
def test_invalid_membership_mappings_are_rejected(mapping):
    with pytest.raises(ValueError):
        queue_node_counts([], [], scheduler_backend("pbs"), mapping)


def test_disabled_and_gpu_only_allocations_are_not_idle():
    nodes = [node("node001", ["batch"], state="d"), node("node002", ["batch"], slurm_gpu_used=[{"count": 1}])]
    assert queue_node_counts(nodes, ["batch"], scheduler_backend("slurm")) == [("batch", 0, 2)]
