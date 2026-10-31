# SPDX-License-Identifier: MIT
import json
from types import SimpleNamespace

import pytest

from qtop_py.cluster_state import validate_cluster_state
from qtop_py.plugins.gpu import parse_gpu_gres
from qtop_py.plugins.slurm import SlurmBatchSystem
from qtop_py.qtop import Document
from qtop_py.sample import anonymize_state


def test_h100_nvl_and_mig_profiles_keep_separate_resource_indices():
    resources = parse_gpu_gres("gpu:h100_nvl:8(S:0-1),gpu:h100_1g.12gb:2(IDX:8,10),shard:8(-/5,5/5)")
    assert resources[0]["count"] == 8
    assert resources[0]["device_indices"] is None
    assert resources[0]["mig_profile"] is None
    assert resources[1]["device_indices"] == [8, 10]
    assert resources[1]["mig_profile"] == "1g.12gb"
    assert all(resource["parent_gpu_uuid"] is None for resource in resources)


@pytest.mark.parametrize("value", ["gpu:2(IDX:0,1)", "gpu:2(IDX:0-1)", "gpu:h100:2(IDX:0-1)"])
def test_gpu_indices_lists_and_ranges(value):
    assert parse_gpu_gres(value)[0]["device_indices"] == [0, 1]


@pytest.mark.parametrize("value", ["gpu:2", "gpu:h100:2(IDX:N/A)", "gpu:h100:2(S:0-1)"])
def test_unknown_indices_are_not_invented(value):
    assert parse_gpu_gres(value)[0]["device_indices"] is None


@pytest.mark.parametrize(
    "value",
    ["gpu:2(IDX:1)", "gpu:2(IDX:1,1)", "gpu:2(IDX:2-1)", "gpu:1(IDX:0-9999999999)", "gpu:1(IDX:-1)", "gpu:x:bad", "gpu:1(IDX:0", "gpu:1(foo)"],
)
def test_malformed_gpu_records_are_rejected(value):
    with pytest.raises(ValueError):
        parse_gpu_gres(value)


def backend(tmp_path, details):
    paths = {}
    traces = {
        "squeue": "101|alice|R|gpu|2|node1\n102|bob|R|gpu|1|node1\n103|eve|PD|gpu|1|(Priority)\n",
        "sinfo": "node1|gpu|mix|8|gpu:h100_nvl:8(S:0-1)|gpu:h100_nvl:2(IDX:0,7)\n",
        "scontrol": details,
    }
    for name, text in traces.items():
        path = tmp_path / (name + ".txt")
        path.write_text(text)
        paths[name + "_file"] = str(path)
    return SlurmBatchSystem(paths, {}, SimpleNamespace(ANONYMIZE=False))


def test_bindings_survive_export_replay_and_anonymization(tmp_path):
    slurm = backend(tmp_path, "JobId=101\n Nodes=node1 CPU_IDs=0-1 GRES=gpu:h100_nvl:1(IDX:7)\nJobId=102\n Nodes=node1 CPU_IDs=2 GRES=gpu:h100_1g.12gb:1(IDX:8)\n")
    state = slurm.get_cluster_state(slurm.options)
    node = state["nodes"][0]
    bindings = node["slurm_gpu_bindings"]
    assert [(binding["job_id"], binding["device_indices"]) for binding in bindings] == [("101", [7]), ("102", [8])]
    assert bindings[1]["mig_profile"] == "1g.12gb"
    assert node["slurm_gpu_configured"][0]["count"] == 8
    assert node["slurm_gpu_used"][0]["device_indices"] == [0, 7]
    decoded = json.loads(json.dumps(state))
    assert validate_cluster_state(decoded) == decoded
    assert Document.from_cluster_state(decoded).to_cluster_state() == decoded
    masked, mappings = anonymize_state(state)
    masked_bindings = masked["nodes"][0]["slurm_gpu_bindings"]
    assert [binding["job_id"] for binding in masked_bindings] == [mappings["job"][job] for job in ("101", "102")]
    assert masked_bindings[1]["mig_profile"] == "1g.12gb"
    assert all(binding["type"] is None and binding["parent_gpu_uuid"] is None for binding in masked_bindings)
    assert validate_cluster_state(masked) == masked


@pytest.mark.parametrize(
    "details",
    [
        "JobId=101\n Nodes=node1 GRES=gpu:2(IDX:0)\n",
        "JobId=101\n Nodes=node2 GRES=gpu:1(IDX:0)\n",
        "JobId=999\n Nodes=node1 GRES=gpu:1(IDX:0)\n",
        "JobId=103\n Nodes=node1 GRES=gpu:1(IDX:0)\n",
        "JobId=101\n Nodes=node1 GRES=gpu:1(IDX:0)\n Nodes=node1 GRES=gpu:1(IDX:1)\n",
        "JobId=101 JobName=Nodes=node1 GRES=gpu:1(IDX:0)\n",
    ],
)
def test_invalid_or_stale_allocations_never_create_bindings(tmp_path, details):
    slurm = backend(tmp_path, details)
    state = slurm.get_cluster_state(slurm.options)
    assert "slurm_gpu_bindings" not in state["nodes"][0]


@pytest.mark.parametrize("field,value", [("count", -1), ("count", True), ("device_indices", [True]), ("device_indices", [1, 1]), ("mig_profile", "arbitrary"), ("job_id", None)])
def test_imported_gpu_bindings_are_validated(tmp_path, field, value):
    slurm = backend(tmp_path, "JobId=101\n Nodes=node1 GRES=gpu:1(IDX:0)\n")
    state = slurm.get_cluster_state(slurm.options)
    state["nodes"][0]["slurm_gpu_bindings"][0][field] = value
    with pytest.raises(ValueError):
        validate_cluster_state(state)


def test_eight_h100_nvl_nodes_keep_bindings_local(tmp_path):
    slurm = backend(tmp_path, "JobId=101\n Nodes=node[1-8] CPU_IDs=0 GRES=gpu:h100_nvl:1(IDX:7)\n")
    slurm._jobs = [{"JobId": "101", "UnixAccount": "alice", "S": "R", "Queue": "gpu", "slurm_partitions": ["gpu"], "accounting_state": "running", "CPUs": 8, "Nodes": "node[1-8]"}]
    slurm._nodes = [
        {"domainname": "node%s" % index, "raw_domainname": "node%s" % index, "qname": "gpu", "np": "8", "state": "b", "slurm_gres": "gpu:h100_nvl:8(S:0-1)"}
        for index in range(1, 9)
    ]
    state = slurm.get_cluster_state(slurm.options)
    assert len(state["nodes"]) == 8
    for node in state["nodes"]:
        assert node["slurm_gpu_configured"][0]["count"] == 8
        assert node["slurm_gpu_bindings"][0]["device_indices"] == [7]
        assert node["slurm_gpu_bindings"][0]["job_id"] == "101"


def test_shared_device_retains_each_job_binding(tmp_path):
    slurm = backend(tmp_path, "JobId=101\n Nodes=node1 GRES=gpu:1(IDX:7)\nJobId=102\n Nodes=node1 GRES=gpu:1(IDX:7)\n")
    bindings = slurm.get_cluster_state(slurm.options)["nodes"][0]["slurm_gpu_bindings"]
    assert [(binding["job_id"], binding["device_indices"]) for binding in bindings] == [("101", [7]), ("102", [7])]


def test_untyped_mig_identity_is_unknown_not_whole_gpu():
    resource = parse_gpu_gres("gpu:1(IDX:8)")[0]
    assert resource["mig_profile"] is None
    assert resource["parent_gpu_uuid"] is None
