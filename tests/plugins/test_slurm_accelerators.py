## Copyright (c) 2026 qtop contributors
## SPDX-License-Identifier: MIT
"""GPUs, TPUs and NPUs of any count: Slurm GRES parsing, the per-device map, replay and -A."""

import json
from types import SimpleNamespace

import pytest

from qtop_py.core_api import BackendOptions
from qtop_py.core_cluster_state import validate_cluster_state
from qtop_py.core_sample import anonymize_state
from qtop_py.plugins.slurm import SlurmBatchSystem
from qtop_py.schedulers import resources
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
    state["nodes"][0]["gpu_configured"][0][field] = value
    with pytest.raises(ValueError):
        validate_cluster_state(state)


def test_the_map_is_a_shared_section_slurm_does_not_repeat(tmp_path):
    state = slurm_state(tmp_path, "node1|acc|mix|8|gpu:8(IDX:0-7)|gpu:0(IDX:N/A)\n", "")
    shown = resources.messages("accelerators", state["nodes"], {})
    assert shown == ["INFO: Accelerators by node (job IDs, ? = unknown):", "INFO: node1: gpu0-7=idle"]
    assert SlurmBatchSystem.display_messages("resources", None, state["nodes"], {}) == []
    assert resources.messages("accelerators", [{"domainname": "cpu1"}], {}) == []


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
    labels = [line.rsplit("=", 1)[1] for line in _qtop(tmp_path, "-T") if "=" in line and line.rsplit("=", 1)[1][:3] in ("Cpu", "Gpu")]
    assert labels == ["Gpu%s" % gpu for gpu in range(4)] + ["Cpu%s" % core for core in range(8)]


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


@pytest.mark.parametrize(
    "value,kinds",
    [
        ("all", ("gpu", "tpu", "npu")),
        (True, ("gpu", "tpu", "npu")),
        ("npu, gpu", ("gpu", "npu")),
        (["gpu, npu"], ("gpu", "npu")),
        (["tpu"], ("tpu",)),
        ("none", ()),
        ("false", ()),
        (False, ()),
    ],
)
def test_matrix_accelerators_takes_yaml_text_lists_and_python_values(value, kinds):
    from qtop_py.configuration import matrix_accelerators

    assert matrix_accelerators(value) == kinds


def test_unknown_accelerator_kinds_are_a_config_error():
    from qtop_py.configuration import matrix_accelerators

    with pytest.raises(ValueError, match="fpga"):
        matrix_accelerators("gpu, fpga")


def test_hidden_kinds_leave_the_matrix_and_the_job_id_lines(tmp_path):
    state = slurm_state(tmp_path, "node1|acc|mix|8|gpu:2(IDX:0-1),npu:4(IDX:0-3)|gpu:0(IDX:N/A),npu:0(IDX:N/A)\n", "")
    assert accelerator_map_lines(state["nodes"], ("npu",)) == ["node1: npu0-3=idle"]
    assert resources.messages("accelerators", state["nodes"], {"matrix_accelerators": ()}) == []
    rows = [line for line in _qtop(tmp_path, "-o", "matrix_accelerators=none") if line[:2] in ("1|", "2|")]
    assert rows == ["1|1_0_0___|b|c|", "2|_0_1_0_1|b|c|"]  # CPUs only, as before GPUs were shown
    assert "gpu_map" not in "\n".join(_qtop(tmp_path, "-o", "matrix_accelerators=none"))


def test_untransposed_gpu_lines_sit_between_dashed_rules(tmp_path):
    lines = _qtop(tmp_path, "-T")
    start = lines.index("--")
    assert lines[start - 1] == "cc=queue name"
    assert [line.rsplit("=", 1)[1] for line in lines[start + 1 : start + 5]] == ["Gpu0", "Gpu1", "Gpu2", "Gpu3"]
    assert lines[start + 5] == "--" and lines[start + 6].endswith("=Cpu0")
    assert "--" not in _qtop(tmp_path, "-T", "-o", "matrix_accelerators=none")  # no devices shown, no rules


def test_a_mig_slice_in_use_leaves_idle_whole_gpus_idle(tmp_path):
    state = slurm_state(
        tmp_path,
        "node1|acc|mix|8|gpu:a100:4(S:0),gpu:a100_3g.20gb:2(S:0)|gpu:a100:3(IDX:0-2),gpu:a100_3g.20gb:1(IDX:4)\n",
        "JobId=101\n Nodes=node1 GRES=gpu:a100:2(IDX:0-1)\nJobId=102\n Nodes=node1 GRES=gpu:a100:1(IDX:2)\n",
    )
    assert accelerator_map_lines(state["nodes"]) == ["node1: gpu0-1=101 gpu2=102 gpu3=idle"]


def _placement(state, config=None, all_partitions=False):
    snapshot = SimpleNamespace(_diagnostics={"slurm_diagnostics": {"all_partitions": 1}} if all_partitions else {})
    return [line for line in SlurmBatchSystem.display_messages("placement", snapshot, state["nodes"], config or {}) if "accelerators" in line]


def test_gpus_held_by_unseen_jobs_ask_for_all_unless_it_was_used(tmp_path):
    sinfo = "node1|acc|mix|8|gpu:a100:4(S:0)|gpu:a100:4(IDX:0-3)\n"
    details = "JobId=101\n Nodes=node1 CPU_IDs=0-1 GRES=gpu:a100:2(IDX:0-1)\nJobId=102\n Nodes=node1 CPU_IDs=2 GRES=gpu:a100:1(IDX:2)\n"
    state = slurm_state(tmp_path, sinfo, details)
    assert accelerator_map_lines(state["nodes"]) == ["node1: gpu0-1=101 gpu2=102 gpu3=?"]
    warning, info = _placement(state)
    assert warning == "WARNING: Slurm accelerators: 1 GPU in use by jobs qtop does not see show ?: run qtop --all for hidden partitions."
    assert info.startswith("INFO: Slurm accelerators: if --all does not name their jobs, PrivateData=jobs")
    (line,) = _placement(state, all_partitions=True)
    assert line.startswith("INFO: Slurm accelerators: 1 GPU in use by jobs qtop does not see show ?, even with --all: PrivateData=jobs")
    assert _placement(state, {"matrix_accelerators": ()}) == []  # hidden kinds, no note
    seen = slurm_state(tmp_path, sinfo.replace("4(IDX:0-3)", "3(IDX:0-2)"), details)
    assert _placement(seen) == []  # every GPU in use has its job: nothing to explain


@pytest.mark.parametrize(("all_partitions", "source", "recorded"), ((True, None, {"all_partitions": 1}), (False, None, None), (True, "/traces", None)))
def test_diagnostics_record_live_collection_with_all(tmp_path, all_partitions, source, recorded):
    paths = {}
    for name, text in (("squeue", "101|alice|R|acc|1|node1\n"), ("sinfo", "node1|acc|mix|8\n")):
        (tmp_path / (name + ".txt")).write_text(text)
        paths[name + "_file"] = str(tmp_path / (name + ".txt"))
    slurm = SlurmBatchSystem(paths, {}, BackendOptions(ALL_PARTITIONS=all_partitions, SOURCEDIR=source))
    state = slurm.get_cluster_state(slurm.options)
    assert state.get("slurm_diagnostics") == recorded  # saved traces may come from either, so replay records nothing
    snapshot = SimpleNamespace(_diagnostics={"slurm_diagnostics": {"all_partitions": 1}})
    assert SlurmBatchSystem.display_messages("snapshot", snapshot, [], {}) == []  # --all alone is no incomplete snapshot


def test_gpus_in_use_without_allocation_details_are_counted(tmp_path):
    state = slurm_state(tmp_path, "node1|acc|mix|8|gpu:a100:4(S:0)|gpu:a100:3(IDX:0-2)\n", "")
    assert accelerator_map_lines(state["nodes"]) == ["node1: gpu0-2=? gpu3=idle"]
    assert _placement(state) == ["WARNING: Slurm accelerators: 3 GPUs in use show ? where allocation details are unavailable."]


def test_mig_slices_follow_the_whole_gpus_with_their_users_or_percent(tmp_path):
    from qtop_py.schedulers.gpu import node_devices, node_slices, slice_jobs

    state = slurm_state(
        tmp_path,
        "node1|acc|mix|8|gpu:a100:2(S:0),gpu:a100_3g.20gb:4(S:0)|gpu:a100:1(IDX:0),gpu:a100_3g.20gb:2(IDX:2-3)\n",
        "JobId=101\n Nodes=node1 GRES=gpu:a100:1(IDX:0),gpu:a100_3g.20gb:1(IDX:2)\nJobId=102\n Nodes=node1 GRES=gpu:a100_3g.20gb:1(IDX:3)\n",
    )
    (node,) = state["nodes"]
    assert (node_devices(node, "gpu"), node_slices(node, "gpu")) == ([0, 1], [2, 3, 4, 5])
    assert [slice_jobs(node, "gpu", index) for index in (2, 3, 4)] == [["101"], ["102"], []]
    assert node_slices({"gpu_configured": parse_accelerator_gres("gpu:a100_3g.20gb:2(IDX:6-7)")}, "gpu") == [6, 7]
    assert accelerator_map_lines(state["nodes"]) == ["node1: gpu0=101 gpu1=idle mig2=101 mig3=102"]  # held slices only


def test_matrix_shows_mig_slices_after_whole_gpus(tmp_path):
    import subprocess
    import sys
    from pathlib import Path

    traces = {
        "squeue.txt": "101|alice|R|acc|2|node[1-2]\n",
        "sinfo.txt": "node1|acc|mix|8|gpu:a100:2(S:0),gpu:a100_3g.20gb:2(S:0)|gpu:a100:1(IDX:0),gpu:a100_3g.20gb:1(IDX:2)\n"
        "node2|acc|mix|8|gpu:a100_3g.20gb:3(S:0)|gpu:a100_3g.20gb:1(IDX:0)\n",
        "scontrol.txt": "JobId=101\n Nodes=node1 CPU_IDs=0 GRES=gpu:a100:1(IDX:0),gpu:a100_3g.20gb:1(IDX:2)\n Nodes=node2 CPU_IDs=0 GRES=gpu:a100_3g.20gb:1(IDX:0)\n",
    }
    (tmp_path / "in").mkdir()
    for name, text in traces.items():
        (tmp_path / "in" / name).write_text(text)
    root = Path(__file__).resolve().parents[2]
    command = [sys.executable, "-m", "qtop_py.qtop", "-b", "slurm", "-s", str(tmp_path / "in"), "-c", "OFF", "-T", "-o", "savepath=%s" % tmp_path]
    lines = subprocess.run(command, cwd=str(root), stdout=subprocess.PIPE, universal_newlines=True, check=True).stdout.splitlines()
    assert [line for line in lines if "=Gpu" in line] == ["00=Gpu0", "_%=Gpu1", "0%=Gpu2", "%#=Gpu3"]


def test_unreadable_gres_used_names_a_node_and_the_next_step(tmp_path):
    state = slurm_state(tmp_path, "node1|acc|mix|8|gpu:a100:4(S:0)|gpu:a100:4(IDX:0-2)\n", "JobId=101\n Nodes=node1 GRES=gpu:a100:2(IDX:0-1)\n")
    assert accelerator_map_lines(state["nodes"]) == ["node1: gpu0-1=101 gpu2-3=?"]
    (line,) = [line for line in _placement(state) if "unreadable" in line]
    assert line.startswith("WARNING: Slurm accelerators: GresUsed of 1 node unreadable, e.g. node1: gpu:a100:4(IDX:0-2);")
    assert "sinfo -N -n node1 -O NodeList,Gres,GresUsed" in line and "qtop issue" in line


SCONTROL_ONLY = (
    "JobId=101\n Nodes=node1 CPU_IDs=0-1 GRES=gpu:a100:2(IDX:0-1)\n"
    "JobId=102\n Nodes=node1 CPU_IDs=2 GRES=gpu:a100:1(IDX:2)\n"
    "JobId=103 JobName=late\n   UserId=carol(1003) GroupId=carol(1003)\n   JobState=RUNNING Reason=None\n"
    "   Partition=acc AllocNode:Sid=login:1\n   ReqNodeList=(null) ExcNodeList=(null)\n   NodeList=node1\n"
    "   NumNodes=1 NumCPUs=1 NumTasks=1\n     Nodes=node1 CPU_IDs=3 Mem=1024 GRES=gpu:a100:1(IDX:3)\n"
    "JobId=104 JobName=cpu_only\n   UserId=dave(1004)\n   JobState=RUNNING\n   Partition=acc\n   NodeList=node1\n   NumCPUs=1\n"
    "     Nodes=node1 CPU_IDs=4 Mem=1024 GRES=\n"
)


def test_running_gpu_jobs_missing_from_squeue_are_listed_from_scontrol(tmp_path):
    state = slurm_state(tmp_path, "node1|acc|mix|8|gpu:a100:4(S:0)|gpu:a100:4(IDX:0-3)\n", SCONTROL_ONLY)
    assert accelerator_map_lines(state["nodes"]) == ["node1: gpu0-1=101 gpu2=102 gpu3=103"]
    late = {job["id"]: job for job in state["jobs"]}["103"]
    assert (late["user"], late["state"], late["queue"]) == ("carol", "R", "acc")
    assert "104" not in {job["id"] for job in state["jobs"]}  # no accelerators: squeue's view stands
    assert state["slurm_diagnostics"] == {"jobs_from_scontrol": 1}
    snapshot = SimpleNamespace(_diagnostics={"slurm_diagnostics": state["slurm_diagnostics"]})
    (note,) = SlurmBatchSystem.display_messages("snapshot", snapshot, [], {})
    assert note.startswith("INFO: Slurm: 1 running job holding accelerators missing from squeue")
    assert _placement(state) == []  # every GPU in use now has its job


def test_incomplete_scontrol_blocks_and_finished_jobs_are_not_listed(tmp_path):
    details = SCONTROL_ONLY.replace("JobState=RUNNING Reason", "JobState=COMPLETED Reason")
    state = slurm_state(tmp_path, "node1|acc|mix|8|gpu:a100:4(S:0)|gpu:a100:4(IDX:0-3)\n", details)
    assert accelerator_map_lines(state["nodes"]) == ["node1: gpu0-1=101 gpu2=102 gpu3=?"]
    assert "slurm_diagnostics" not in state
