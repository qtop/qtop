## Copyright (c) 2026 qtop contributors
## SPDX-License-Identifier: MIT
"""Tests for the Flux backend, over traces captured from flux-core 0.91 and edits of them."""

import json
from pathlib import Path

import pytest
from support import nodes_by_name, sample_backend

from qtop_py import configuration
from qtop_py.core_api import BackendOptions, SchedulerInputError
from qtop_py.core_sample import anonymize_state
from qtop_py.plugins.flux import JOB_STATES, FluxBatchSystem, idset

SAMPLE = Path(__file__).resolve().parent / "flux_samples" / "basic"
TRACES = {
    "jobs_json": "flux_jobs.json",
    "resource_json": "flux_resource_R.json",
    "rlist_file": "flux_resource_list.txt",
    "status_file": "flux_resource_status.txt",
    "queues_file": "flux_queue_list.txt",
}
R_TWO = {"version": 1, "execution": {"R_lite": [{"rank": "0-1", "children": {"core": "0-3"}}], "nodelist": ["n[1-2]"]}}


def backend(tmp_path=None, **replaced):
    """Return a Flux backend over the sample traces, with any trace replaced (JSON-encoded unless str)."""
    return sample_backend(FluxBatchSystem, SAMPLE, TRACES, tmp_path, **replaced)


def job(job_id, state="RUN", nodelist=None, ncores=1, queue="batch", user="alice"):
    return {"jobid": job_id, "username": user, "state": state, "queue": queue, "nodelist": nodelist, "ncores": ncores}


def test_sample_jobs_use_f58_ids_flux_state_letters_and_queues():
    job_ids, users, states, queues = backend().get_jobs_info()
    assert len(job_ids) == 11 and set(users) == {"fluxuser"}
    assert job_ids[-1] == "\u0192HcexB9" and states[-1] == "R" and queues[-1] == "batch"
    assert sorted(states) == ["D"] + ["R"] * 5 + ["S"] * 5  # the held job is S, as flux jobs prints it


def test_queues_count_running_and_queued_jobs_and_mark_stopped_ones():
    running, queued, queues = backend().get_queues_info()
    assert (running, queued) == (5, 5)
    by_name = {queue["queue_name"]: queue for queue in queues}
    assert by_name["long"]["state"] == "Enabled:Stopped" and FluxBatchSystem.queue_is_blocked(by_name["long"]["state"])
    assert (by_name["batch"]["run"], by_name["batch"]["queued"]) == (5, 3)  # the dependent job is neither
    assert not FluxBatchSystem.queue_is_blocked(by_name["gpu"]["state"])


def test_nodes_are_broker_ranks_with_their_allocated_cores_and_states():
    nodes = nodes_by_name(backend())
    assert list(nodes) == ["node%02d" % rank for rank in range(1, 9)]
    assert {name: node["state"] for name, node in nodes.items()} == dict(
        [("node%02d" % rank, "-") for rank in (1, 2, 3, 4, 6)] + [("node05", "o"), ("node07", "d"), ("node08", "d")]
    )
    assert nodes["node01"]["np"] == 16 and nodes["node01"]["core_job_map"] == {14: "\u0192TcL7My", 15: "\u0192TcL7My"}
    assert nodes["node05"]["core_job_map"] == {core: "\u0192L6TkWb" for core in range(8, 16)}  # draining, still running mpi
    assert nodes["node06"]["core_job_map"] == {core: "\u0192HcexB9" for core in range(16)}
    assert not nodes["node07"]["core_job_map"]


def test_jobs_sharing_a_node_split_its_allocated_cores_and_say_so(tmp_path):
    jobs = {"jobs": [job("fA", nodelist="n1", ncores=2), job("fB", nodelist="n[1-2]", ncores=3)]}
    flux = backend(tmp_path, jobs_json=jobs, resource_json=R_TWO, rlist_file="allocated rank0/core[0-3] rank1/core3\n", status_file="avail n[1-2]\n")
    nodes = nodes_by_name(flux)
    assert nodes["n1"]["core_job_map"] == {0: "fA", 1: "fA", 2: "fB", 3: "fB"}  # fB: 2 cores on n1, 1 on n2
    assert nodes["n2"]["core_job_map"] == {3: "fB"}
    state = flux.get_cluster_state(BackendOptions())
    assert state["flux_diagnostics"] == {"shared_nodes": 1}
    notes = FluxBatchSystem.display_messages("snapshot", type("S", (), {"_diagnostics": state})(), [], {})
    assert notes and "1 nodes run several jobs" in notes[0]


def test_allocated_cores_without_a_listed_job_are_unknown_and_excess_job_cores_are_counted(tmp_path):
    jobs = {"jobs": [job("fA", nodelist="n2", ncores=3)]}
    flux = backend(tmp_path, jobs_json=jobs, resource_json=R_TWO, rlist_file="allocated rank0/core[2-3] rank1/core[0-1]\n", status_file="avail n[1-2]\n")
    nodes = nodes_by_name(flux)
    assert nodes["n1"]["core_job_map"] == {2: "?", 3: "?"}
    assert nodes["n2"]["core_job_map"] == {0: "fA", 1: "fA"}
    assert flux.diagnostics == {"unowned_cores": 2, "unplaced_cores": 1}


def test_core_ids_with_gaps_map_to_positions(tmp_path):
    resource = {"version": 1, "execution": {"R_lite": [{"rank": "0", "children": {"core": "0-1,8-9"}}], "nodelist": ["n1"]}}
    jobs = {"jobs": [job("fA", nodelist="n1", ncores=2)]}
    nodes = nodes_by_name(backend(tmp_path, jobs_json=jobs, resource_json=resource, rlist_file="allocated rank0/core[8-9]\n", status_file="avail n1\n"))
    assert nodes["n1"]["np"] == 4 and nodes["n1"]["core_job_map"] == {2: "fA", 3: "fA"}


def test_allocated_gpus_and_empty_status_states_are_ignored(tmp_path):
    rlist = "free rank1/core[0-3]\nallocated rank0/core[0-1],gpu[0-1]\n"
    flux = backend(tmp_path, jobs_json={"jobs": [job("fA", nodelist="n1", ncores=2)]}, resource_json=R_TWO, rlist_file=rlist, status_file="avail n[1-2]\ndrained\n")
    nodes = nodes_by_name(flux)
    assert nodes["n1"]["core_job_map"] == {0: "fA", 1: "fA"} and nodes["n2"]["state"] == "-"


def test_an_instance_without_named_queues_uses_default(tmp_path):
    flux = backend(tmp_path, jobs_json={"jobs": [job("fA", state="SCHED", queue=None)]}, queues_file=" y y\n")
    assert flux.get_jobs_info()[3] == ["default"]
    assert flux.get_queues_info()[2] == [{"queue_name": "default", "run": 0, "queued": 1, "lm": "--", "state": "Enabled:Started"}]


def test_a_paused_queue_is_blocked_and_the_queue_trace_is_optional(tmp_path):
    states = {queue["queue_name"]: queue["state"] for queue in backend(tmp_path, queues_file="sub y p\n").get_queues_info()[2]}
    assert states["sub"] == "Enabled:Paused" and FluxBatchSystem.queue_is_blocked(states["sub"])
    files = {key: str(SAMPLE / name) for key, name in TRACES.items() if key != "queues_file"}
    names = [queue["queue_name"] for queue in FluxBatchSystem(files, {}, BackendOptions()).get_queues_info()[2]]
    assert sorted(names) == ["batch", "gpu", "long"]


def test_inactive_jobs_and_an_empty_instance_list_nothing(tmp_path):
    assert backend(tmp_path, jobs_json={"jobs": [job("fA", state="INACTIVE")]}).get_jobs_info() == ([], [], [], [])
    assert backend(tmp_path, jobs_json={"jobs": []}).get_queues_info()[:2] == (0, 0)


@pytest.mark.parametrize(
    "text, expected",
    [("0", [0]), ("0-3", [0, 1, 2, 3]), ("[3-4]", [3, 4]), ("0,2-3,7", [0, 2, 3, 7])],
)
def test_idsets_expand_in_order(text, expected):
    assert idset(text, 100) == expected


@pytest.mark.parametrize("text", ["", "x", "3-1", "1,,2", "[0-200]", "0-100"])
def test_malformed_or_oversized_idsets_are_input_errors(text):
    with pytest.raises(SchedulerInputError, match="idset"):
        idset(text, 100)


@pytest.mark.parametrize(
    "replaced, message",
    [
        ({"jobs_json": {"jobs": [job("fA", state="BOGUS")]}}, "Invalid flux jobs record"),
        ({"jobs_json": {"jobs": [job("fA", nodelist="n[1-")]}}, "Invalid flux job fA nodelist"),
        ({"jobs_json": {"queue": []}}, "lacks a jobs list"),
        ({"resource_json": {"version": 1}}, "lacks execution"),
        ({"resource_json": {"version": 1, "execution": {"R_lite": [{"rank": "2", "children": {"core": "0"}}], "nodelist": ["n1"]}}}, "unnamed or repeated"),
        ({"jobs_json": {"jobs": ["fA"]}}, "non-object job"),
        ({"resource_json": {"version": 1, "execution": {"R_lite": [], "nodelist": ["n[1-"]}}}, "Invalid Flux nodelist"),
        ({"resource_json": {"version": 1, "execution": {"R_lite": [], "nodelist": ["n1", "n1"]}}}, "names a host twice"),
        ({"resource_json": {"version": 1, "execution": {"R_lite": [{"rank": 0}], "nodelist": ["n1"]}}}, "Invalid Flux R_lite entry"),
        ({"resource_json": {"version": 1, "execution": {"R_lite": [{"rank": "0", "children": {"core": "0,0"}}], "nodelist": ["n1"]}}}, "core IDs are repeated"),
        ({"rlist_file": "allocated rank0/core[0-1]junk\n"}, "Invalid flux resource list entry"),
        ({"status_file": "sleeping node01\n"}, "Unsupported flux resource status line"),
        ({"queues_file": "batch yes no\n"}, "Unsupported flux queue list line"),
        ({"queues_file": "batch \u2714 \u2714\n"}, "Unsupported flux queue list line"),
    ],
)
def test_unreadable_traces_name_what_is_wrong(tmp_path, replaced, message):
    with pytest.raises(SchedulerInputError, match=message):
        backend(tmp_path, **replaced).get_cluster_state(BackendOptions())


def test_anonymize_masks_users_hosts_and_queues():
    state = anonymize_state(backend().get_cluster_state(BackendOptions()))[0]
    text = json.dumps(state)
    assert "fluxuser" not in text and "node06" not in text and '"batch"' not in text
    assert len(state["nodes"]) == 8


def test_every_job_state_letter_is_configured():
    from qtop_py import core_yaml_parser as yaml_parser

    config = configuration.with_backend_defaults(yaml_parser.parse(str(Path(__file__).resolve().parents[2] / "qtopconf.yaml")))
    assert set(JOB_STATES.values()) == set(config["state_abbreviations"]["flux"])
