# SPDX-License-Identifier: MIT
import pytest

from qtop_py.gpu import configured_pair_lines, parse_gpu_gres
from qtop_py.sample import sanitize_config
from qtop_py.yaml_parser import safe_load
from tests.plugins.test_slurm_gpu import backend


def test_yaml_ranges_and_split_pair_ownership(tmp_path):
    path = tmp_path / "qtopconf.yaml"
    path.write_text("experimental_gpu_pairs:\n  gpu[001-009]: [[0, 1], [2, 3]]\n")
    config = safe_load(str(path))
    node = {
        "domainname": "gpu009",
        "slurm_gpu_used": parse_gpu_gres("gpu:h100_nvl:3(IDX:0-2)"),
        "slurm_gpu_bindings": [
            dict(parse_gpu_gres("gpu:h100_nvl:2(IDX:0-1)")[0], job_id="101"),
            dict(parse_gpu_gres("gpu:h100_nvl:1(IDX:2)")[0], job_id="102"),
        ],
    }
    assert configured_pair_lines([node], config["experimental_gpu_pairs"]) == ["gpu009: pair1[gpu0=101, gpu1=101] pair2[gpu2=102, gpu3=idle]"]
    assert configured_pair_lines([dict(node, domainname="gpu010")], config["experimental_gpu_pairs"]) == []


def test_missing_details_and_mig_are_not_invented():
    node = {
        "domainname": "gpu001",
        "slurm_gpu_used": parse_gpu_gres("gpu:2"),
        "slurm_gpu_bindings": [dict(parse_gpu_gres("gpu:h100_1g.12gb:1(IDX:0)")[0], job_id="101")],
    }
    assert configured_pair_lines([node], {"gpu001": [[0, 1]]}) == ["gpu001: pair1[gpu0=?, gpu1=?]"]
    assert configured_pair_lines([node], None) == []


@pytest.mark.parametrize("usage", ["N/A", "(null)", "", "shard:1(IDX:0)", "gpu:h100_1g.12gb:1(IDX:0)"])
def test_unavailable_or_mig_usage_cannot_establish_idle_physical_gpus(usage):
    node = {"domainname": "gpu001", "slurm_gpu_used": parse_gpu_gres(usage)}
    assert configured_pair_lines([node], {"gpu001": [[0, 1]]}) == ["gpu001: pair1[gpu0=?, gpu1=?]"]


def test_explicit_zero_usage_establishes_idle_without_inventing_job_ownership():
    node = {"domainname": "gpu001", "slurm_gpu_used": parse_gpu_gres("gpu:h100_nvl:0(IDX:N/A)")}
    assert configured_pair_lines([node], {"gpu001": [[0, 1]]}) == ["gpu001: pair1[gpu0=idle, gpu1=idle]"]


def test_collected_slurm_bindings_support_split_and_shared_ownership(tmp_path):
    slurm = backend(tmp_path, "JobId=101\n Nodes=node1 GRES=gpu:h100_nvl:2(IDX:0,7)\nJobId=102\n Nodes=node1 GRES=gpu:h100_nvl:1(IDX:7)\n")
    nodes = slurm.get_cluster_state(slurm.options)["nodes"]
    assert configured_pair_lines(nodes, {"node1": [[0, 7]]}) == ["node1: pair1[gpu0=101, gpu7=101/102]"]


def test_anonymized_samples_omit_identifying_node_patterns():
    config = {"experimental_gpu_pairs": {"private[001-009]": [[0, 1]]}}
    masked, omitted = sanitize_config(config, {"user": {}, "queue": {}, "node": {}})
    assert "experimental_gpu_pairs" not in masked
    assert "experimental_gpu_pairs" in omitted
    assert "experimental_gpu_pairs" in config


@pytest.mark.parametrize(
    "mapping",
    [
        "bad",
        {"gpu[009-001]": [[0, 1]]},
        {"gpu001": [[0, 0]]},
        {"gpu001": [[0, 1], [1, 2]]},
        {"gpu001": [[False, 1]]},
        {"gpu001": [[-1, 1]]},
        {"gpu001": [[0]]},
        {"gpu001": []},
        {"gpu[001-009]": [[0, 1]], "gpu009": [[2, 3]]},
    ],
)
def test_invalid_inventory_is_rejected(mapping):
    with pytest.raises(ValueError):
        configured_pair_lines([], mapping)
