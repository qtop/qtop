## Copyright (c) 2026 qtop contributors
## SPDX-License-Identifier: MIT
"""GPUs, TPUs and NPUs of any count: Slurm GRES parsing, the per-device map, replay and -A."""

import json

import pytest

from qtop_py.core_api import BackendOptions
from qtop_py.core_cluster_state import validate_cluster_state
from qtop_py.core_sample import anonymize_state
from qtop_py.plugins.slurm import SlurmBatchSystem
from qtop_py.schedulers.gpu import accelerator_map_lines, parse_accelerator_gres, parse_gpu_gres
from qtop_py.snapshot import Snapshot


def slurm_state(tmp_path, sinfo, details):
    paths = {}
    traces = {"squeue": "101|alice|R|acc|2|node1\n102|bob|R|acc|1|node1\n", "sinfo": sinfo, "scontrol": details}
    for name, text in traces.items():
        (tmp_path / (name + ".txt")).write_text(text)
        paths[name + "_file"] = str(tmp_path / (name + ".txt"))
    slurm = SlurmBatchSystem(paths, {}, BackendOptions())
    return slurm.get_cluster_state(slurm.options)


@pytest.mark.parametrize("kind", ("gpu", "tpu", "npu"))
@pytest.mark.parametrize("count", (1, 4, 8, 16))
def test_any_kind_and_count_parses_with_indices(kind, count):
    (resource,) = parse_accelerator_gres("%s:model:%s(IDX:0-%s)" % (kind, count, count - 1))
    assert resource["count"] == count and resource["device_indices"] == list(range(count))
    assert resource.get("kind", "gpu") == kind
    assert ("kind" in resource) is (kind != "gpu")  # GPU records keep their pre-TPU/NPU shape


def test_mixed_gres_keeps_each_kind_and_skips_other_resources():
    resources = parse_accelerator_gres("gpu:a100:2(IDX:0-1),tpu:v4:4(IDX:0-3),npu:910b:8(S:0-1),shard:8,mps:100")
    assert [(r.get("kind", "gpu"), r["count"]) for r in resources] == [("gpu", 2), ("tpu", 4), ("npu", 8)]
    assert parse_gpu_gres is parse_accelerator_gres


def test_only_gpus_have_mig_profiles():
    assert parse_accelerator_gres("npu:x_1g.10gb:1")[0]["mig_profile"] is None
    assert parse_accelerator_gres("gpu:a100_1g.10gb:1")[0]["mig_profile"] == "1g.10gb"


@pytest.mark.parametrize("value", ("tpu:v4:2(IDX:0)", "npu:x:1(bogus)", "tpu:"))
def test_malformed_tpu_and_npu_records_are_rejected(value):
    with pytest.raises(ValueError):
        parse_accelerator_gres(value)


def test_eight_gpus_show_their_jobs_device_by_device(tmp_path):
    state = slurm_state(
        tmp_path,
        "node1|acc|mix|8|gpu:h100:8(S:0-1)|gpu:h100:3(IDX:0-1,7)\n",
        "JobId=101\n Nodes=node1 GRES=gpu:h100:2(IDX:0-1)\nJobId=102\n Nodes=node1 GRES=gpu:h100:1(IDX:7)\n",
    )
    assert accelerator_map_lines(state["nodes"]) == ["node1: gpu0-1=101 gpu2-6=idle gpu7=102"]


def test_tpus_and_npus_share_the_map_with_gpus(tmp_path):
    state = slurm_state(
        tmp_path,
        "node1|acc|mix|8|gpu:2(IDX:0-1),tpu:v5e:4(IDX:0-3),npu:910b:8(IDX:0-7)|gpu:0(IDX:N/A),tpu:v5e:2(IDX:0-1),npu:910b:8(IDX:0-7)\n",
        "JobId=101\n Nodes=node1 GRES=tpu:v5e:2(IDX:0-1)\nJobId=102\n Nodes=node1 GRES=npu:910b:8(IDX:0-7)\n",
    )
    assert accelerator_map_lines(state["nodes"]) == ["node1: gpu0-1=idle tpu0-1=101 tpu2-3=idle npu0-7=102"]
    decoded = json.loads(json.dumps(state))
    assert validate_cluster_state(decoded) == decoded
    assert Snapshot.from_cluster_state(decoded).to_cluster_state() == decoded
    masked, mappings = anonymize_state(state)
    assert validate_cluster_state(masked) == masked
    (line,) = accelerator_map_lines(masked["nodes"])
    assert not line.startswith("node1") and line.endswith(": gpu0-1=idle tpu0-1=%s tpu2-3=idle npu0-7=%s" % (mappings["job"]["101"], mappings["job"]["102"]))


def test_unknown_usage_is_a_question_mark_not_idle(tmp_path):
    state = slurm_state(tmp_path, "node1|acc|mix|8|npu:4|npu:2(IDX:N/A)\n", "")
    assert accelerator_map_lines(state["nodes"]) == ["node1: npu0-3=?"]


@pytest.mark.parametrize("field,value", [("kind", "fpga"), ("kind", 1), ("mig_profile", "1g.10gb")])
def test_imported_tpu_records_are_validated(tmp_path, field, value):
    state = slurm_state(tmp_path, "node1|acc|mix|8|tpu:4(IDX:0-3)|tpu:0(IDX:N/A)\n", "")
    state["nodes"][0]["slurm_gpu_configured"][0][field] = value
    with pytest.raises(ValueError):
        validate_cluster_state(state)


def test_the_map_is_a_slurm_resources_note_and_can_be_switched_off(tmp_path):
    state = slurm_state(tmp_path, "node1|acc|mix|8|gpu:8(IDX:0-7)|gpu:0(IDX:N/A)\n", "")
    shown = SlurmBatchSystem.display_messages("resources", None, state["nodes"], {})
    assert shown == ["INFO: Accelerators by node (job IDs, ? = unknown):", "INFO: node1: gpu0-7=idle"]
    assert SlurmBatchSystem.display_messages("resources", None, state["nodes"], {"accelerator_map": False}) == []
    assert SlurmBatchSystem.display_messages("resources", None, [{"domainname": "cpu1"}], {}) == []
