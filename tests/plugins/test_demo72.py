## Copyright (c) 2026 qtop contributors
## SPDX-License-Identifier: MIT
"""qtop -b demo72: a simulated GB200 NVL72 rack whose state is a valid, Slurm-shaped cluster state."""

import json

import pytest

from qtop_py.core_api import BackendOptions
from qtop_py.core_cluster_state import validate_cluster_state
from qtop_py.core_sample import anonymize_state
from qtop_py.plugins import demo72
from qtop_py.schedulers import resources
from qtop_py.schedulers.gpu import accelerator_map_lines


def state_for(seed, monkeypatch):
    monkeypatch.setattr(demo72.time, "time", lambda: seed * demo72.PERIOD)
    backend = demo72.Demo72BatchSystem({}, {}, BackendOptions())
    return backend, backend.get_cluster_state(BackendOptions())


@pytest.mark.parametrize("seed", range(0, 400, 7))
def test_every_round_is_a_valid_rack(seed, monkeypatch):
    backend, state = state_for(seed, monkeypatch)
    assert validate_cluster_state(json.loads(json.dumps(state))) is not None
    nodes = state["nodes"]
    assert [node["domainname"] for node in nodes] == ["tray%02d" % tray for tray in range(1, 19)]
    for node in nodes:
        assert node["np"] == 144 and node["gpu_configured"][0]["count"] == 4
        if node["state"] == "d":
            assert not node["core_job_map"] and "gpu_used" not in node
            assert accelerator_map_lines([node]) == ["%s: gpu0-3=?" % node["domainname"]]
            continue
        used = node["gpu_used"][0]
        bound = {index for binding in node.get("gpu_bindings", []) for index in binding["device_indices"]}
        assert bound <= set(used["device_indices"])  # a binding always sits on a used GPU
        cores = {core // demo72.CORES_PER_GPU for core in node["core_job_map"]}
        assert cores == set(used["device_indices"])  # 36 cores per allocated GPU
    running = [job for job in backend.sim.jobs if job["state"] == "R"]
    big = running[0]
    assert big["queue"] == "train" and len(big["gpus"]) >= 6 * 4 and len(big["gpus"]) % 4 == 0  # whole trays
    assert any(job["state"] == "PD" for job in backend.sim.jobs)


def test_the_map_shows_whole_tray_jobs_and_an_unknown_gpu(monkeypatch):
    backend, state = state_for(3, monkeypatch)
    lines = accelerator_map_lines(state["nodes"])
    assert len(lines) == 18
    big = backend.sim.jobs[0]["id"]
    assert sum(line.endswith(": gpu0-3=" + big) for line in lines) >= 6
    if backend.sim.hidden_job:
        assert any("=?" in line for line in lines)


def test_rounds_are_reproducible_and_change_over_time(monkeypatch):
    _, first = state_for(11, monkeypatch)
    _, again = state_for(11, monkeypatch)
    _, later = state_for(12, monkeypatch)
    assert first == again and first != later


def test_anonymized_rounds_mask_jobs_and_keep_the_gpus(monkeypatch):
    _, state = state_for(5, monkeypatch)
    masked, _ = anonymize_state(state)
    assert validate_cluster_state(masked) == masked
    text = json.dumps(masked)
    assert not any(user in text for user in demo72.USERS)
    assert [len(accelerator_map_lines([node])) for node in masked["nodes"]] == [1] * 18


def test_notes_name_the_rack_and_list_the_accelerators(monkeypatch):
    _, state = state_for(9, monkeypatch)
    assert "GB200 NVL72" in demo72.Demo72BatchSystem.display_messages("placement", None, state["nodes"], {})[0]
    lines = resources.messages("accelerators", state["nodes"], {})
    assert lines[0] == "INFO: Accelerators by node (job IDs, ? = unknown):" and len(lines) == 19
