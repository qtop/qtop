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


def test_the_map_is_its_own_slurm_section(tmp_path):
    state = slurm_state(tmp_path, "node1|acc|mix|8|gpu:8(IDX:0-7)|gpu:0(IDX:N/A)\n", "")
    shown = SlurmBatchSystem.display_messages("accelerators", None, state["nodes"], {})
    assert shown == ["INFO: Accelerators by node (job IDs, ? = unknown):", "INFO: node1: gpu0-7=idle"]
    assert SlurmBatchSystem.display_messages("resources", None, state["nodes"], {}) == []
    assert SlurmBatchSystem.display_messages("accelerators", None, [{"domainname": "cpu1"}], {}) == []


@pytest.mark.parametrize(
    "value,placement",
    [("before", "before"), ("After", "after"), ("false", "off"), ("False", "off"), (False, "off"), ("off", "off"), ("true", "before"), (True, "before")],
)
def test_accelerator_map_takes_yaml_text_and_python_values(value, placement):
    from qtop_py.configuration import accelerator_map_placement

    assert accelerator_map_placement(value) == placement


def test_an_unknown_accelerator_map_value_is_a_config_error():
    from qtop_py.configuration import accelerator_map_placement

    with pytest.raises(ValueError, match="before, after or false"):
        accelerator_map_placement("below")


def _view(placement, nodes):
    from types import SimpleNamespace

    from qtop_py.ui.mvu_view import TextDisplay

    display = object.__new__(TextDisplay)
    display.config = {"transpose_wn_matrices": True} if placement is None else {"transpose_wn_matrices": True, "accelerator_map": placement}
    display.MVU_model = SimpleNamespace(backend=SlurmBatchSystem, display_overrides={})
    display.cluster = SimpleNamespace(worker_nodes=nodes)
    display.snapshot = SimpleNamespace()
    display.wns_occupancy = SimpleNamespace(node_column_start=0, node_column_stop=1)
    display.display_basic_legend = lambda: print("LEGEND")
    display.display_matrix = lambda *args: print("MATRIX")
    return display


@pytest.mark.parametrize(
    "placement,expected", [(None, ["LEGEND", "MATRIX"]), ("before", ["LEGEND", "ACC", "MATRIX"]), ("after", ["LEGEND", "MATRIX", "ACC"]), ("off", ["LEGEND", "MATRIX"])]
)
def test_the_map_goes_before_or_after_the_matrix(tmp_path, capsys, placement, expected):
    state = slurm_state(tmp_path, "node1|acc|mix|8|gpu:8(IDX:0-7)|gpu:0(IDX:N/A)\n", "")
    display = _view(placement, state["nodes"])
    display._accelerators_shown = False
    display.display_wns_occupancy(None, display.cluster)
    lines = capsys.readouterr().out.splitlines()
    assert ["ACC" if line.startswith("INFO: ") else line for line in lines if line != "INFO: node1: gpu0-7=idle"] == expected
    display.display_backend_messages()
    assert "Accelerators" not in capsys.readouterr().out  # shown once, by the matrix


def test_with_the_matrix_hidden_the_map_joins_the_scheduler_messages(tmp_path, capsys):
    state = slurm_state(tmp_path, "node1|acc|mix|8|gpu:8(IDX:0-7)|gpu:0(IDX:N/A)\n", "")
    display = _view("before", state["nodes"])
    display._accelerators_shown = False  # -2: display_wns_occupancy never ran
    display.display_backend_messages()
    assert "INFO: node1: gpu0-7=idle" in capsys.readouterr().out.splitlines()


def _qtop(tmp_path, *flags):
    import subprocess
    import sys
    from pathlib import Path

    root = Path(__file__).resolve().parents[2]
    command = [sys.executable, "-m", "qtop_py.qtop", "-b", "slurm", "-s", str(root / "tests/plugins/slurm_samples/arrays_and_het"), "-c", "OFF"]
    result = subprocess.run(
        command + list(flags) + ["-o", "savepath=%s" % tmp_path], cwd=str(root), stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True, check=True
    )
    return result.stdout.splitlines()


def test_gpus_are_matrix_cells_with_user_ids_like_cores(tmp_path):
    lines = _qtop(tmp_path)
    assert "(wnid_lines/gpu_map/core_map/Node state/queue name)" in "\n".join(lines)
    rows = [line for line in lines if line[:2] in ("1|", "2|")]
    # GPUs (idle _, in use by an unnamed job ?) | cores | state | queue
    assert rows == ["1|_?__|1_0_0___|b|c|", "2|__?_|_0_1_0_1|b|c|"]
    assert not any("Accelerators by node" in line for line in lines)  # job-ID lines are opt-in


def test_untransposed_matrices_list_gpu_lines_above_the_core_lines(tmp_path):
    labels = [line.rsplit("=", 1)[1] for line in _qtop(tmp_path, "-T") if "=" in line and line.rsplit("=", 1)[1][:3] in ("Cor", "Gpu")]
    assert labels == ["Gpu%s" % gpu for gpu in range(4)] + ["Core%s" % core for core in range(8)]


def test_job_id_lines_can_still_join_the_matrix(tmp_path):
    lines = _qtop(tmp_path, "-o", "accelerator_map=before")
    assert lines[lines.index("INFO: Accelerators by node (job IDs, ? = unknown):") - 1].startswith("===> 2. Worker Nodes occupancy")


@pytest.mark.parametrize(
    "option,key,expected",
    [
        ("accelerator_map=false", "accelerator_map", "off"),
        ("accelerator_map=After", "accelerator_map", "after"),
        ("experimental_nvlink_blocks=true", "experimental_nvlink_blocks", True),
        ("experimental_nvlink_blocks=no", "experimental_nvlink_blocks", False),
    ],
)
def test_command_line_options_read_these_settings_like_the_yaml(option, key, expected):
    from types import SimpleNamespace

    from qtop_py.configuration import update_config_with_cmdline_vars

    assert update_config_with_cmdline_vars(SimpleNamespace(OPTION=[option]), {})[key] == expected


def test_a_bad_command_line_setting_is_an_operational_error():
    from types import SimpleNamespace

    from qtop_py.configuration import update_config_with_cmdline_vars
    from qtop_py.core_errors import OperationalError

    with pytest.raises(OperationalError, match="accelerator_map"):
        update_config_with_cmdline_vars(SimpleNamespace(OPTION=["accelerator_map=below"]), {})
