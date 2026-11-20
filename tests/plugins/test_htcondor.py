## Copyright (c) 2026 qtop contributors
## SPDX-License-Identifier: MIT
"""Tests for the HTCondor backend's condor_status and condor_q JSON parsing."""

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from qtop_py.plugins.htcondor import JOB_STATES, HTCondorBatchSystem, slot_type

SAMPLE = Path(__file__).resolve().parent / "htcondor_samples" / "basic"


def backend(tmp_path=None, status=None, jobs=None, anonymize=False):
    """Return an HTCondor backend over the sample traces, with either trace replaced (JSON-encoded unless str)."""
    paths = {"status_json": str(SAMPLE / "condor_status.json"), "q_json": str(SAMPLE / "condor_q.json")}
    for key, value in (("status_json", status), ("q_json", jobs)):
        if value is not None:
            path = tmp_path / key
            path.write_text(value if isinstance(value, str) else json.dumps(value))
            paths[key] = str(path)
    return HTCondorBatchSystem(paths, {}, SimpleNamespace(ANONYMIZE=anonymize))


def nodes_by_name(condor):
    job_ids, _users, _states, queues = condor.get_jobs_info()
    return {node["domainname"]: node for node in condor.get_worker_nodes(job_ids, queues, None)}


def test_sample_jobs_use_cluster_proc_ids_state_letters_and_groups():
    job_ids, users, states, queues = backend().get_jobs_info()
    assert job_ids == ["100.0", "100.1", "101.0", "102.0", "102.1", "103.0", "104.0", "105.0"]
    assert states == ["R", "R", "R", "I", "H", "R", "S", "I"]
    assert queues[:3] == ["group_physics", "group_physics", "default"] and users[2] == "bob"


def test_queues_are_accounting_groups_counting_running_and_idle_jobs():
    running, queued, queues = backend().get_queues_info()
    assert (running, queued) == (4, 2)
    assert [(queue["queue_name"], queue["run"], queue["queued"]) for queue in queues] == [("group_physics", 2, 0), ("default", 2, 1), ("group_bio", 0, 1)]


def test_machines_count_partitionable_and_static_cores_and_fill_slots():
    nodes = nodes_by_name(backend())
    node01 = nodes["node01.example.org"]
    assert node01["np"] == 32  # TotalSlotCpus, not the 8 unclaimed Cpus
    assert [node01["core_job_map"][core] for core in range(24)] == ["100.0"] * 8 + ["100.1"] * 16
    assert nodes["node04.example.org"]["np"] == 4 and nodes["node04.example.org"]["core_job_map"] == {0: "103.0", 1: "104.0"}
    assert [nodes["node0%s.example.org" % index]["state"] for index in range(1, 6)] == ["-", "-", "o", "-", "o"]


def test_boolean_slot_attributes_stand_in_for_slot_type():
    assert [slot_type(ad) for ad in ({"PartitionableSlot": True}, {"DynamicSlot": True}, {}, {"SlotType": "Static", "DynamicSlot": True})] == [
        "Partitionable",
        "Dynamic",
        "Static",
        "Static",
    ]


def test_unknown_machines_and_missing_dynamic_slots_use_request_cpus(tmp_path):
    jobs = [{"ClusterId": 1, "ProcId": 0, "Owner": "u", "JobStatus": 2, "RemoteHost": "slot1_9@ghost.example.org", "RequestCpus": 3}]
    nodes = nodes_by_name(backend(tmp_path, jobs=jobs))
    assert (nodes["ghost.example.org"]["np"], nodes["ghost.example.org"]["state"]) == (3, "?")
    assert sorted(nodes["ghost.example.org"]["core_job_map"]) == [0, 1, 2]


@pytest.mark.parametrize("text", ("", "  \n", "[]"))
def test_condor_q_with_no_jobs_is_an_empty_pool(tmp_path, text):
    condor = backend(tmp_path, jobs=text)
    assert condor.get_jobs_info() == ([], [], [], [])
    assert not any(node["core_job_map"] for node in nodes_by_name(condor).values())


@pytest.mark.parametrize(
    "jobs, message",
    (
        ('{"ClusterId": 1}', "not a list of ClassAds"),
        ([{"ClusterId": 1, "ProcId": 0, "Owner": "u", "JobStatus": 9}], "Invalid condor_q job ad"),
        ([{"ClusterId": "1", "ProcId": 0, "Owner": "u", "JobStatus": 1}], "Invalid condor_q job ad"),
        ([{"ClusterId": 1, "ProcId": 0, "Owner": "u", "JobStatus": 2, "RemoteHost": "nohost"}], "Invalid HTCondor slot name"),
    ),
)
def test_malformed_job_ads_are_rejected(tmp_path, jobs, message):
    with pytest.raises(ValueError, match=message):
        nodes_by_name(backend(tmp_path, jobs=jobs))


def test_status_ads_need_name_and_machine(tmp_path):
    with pytest.raises(ValueError, match="lacks Name or Machine"):
        nodes_by_name(backend(tmp_path, status=[{"Name": "slot1@a", "SlotType": "Static", "Cpus": 1}]))


def test_anonymize_maps_machines_in_status_and_remote_hosts_alike():
    condor = backend(anonymize=True)
    _job_ids, users, _states, queues = condor.get_jobs_info()
    nodes = nodes_by_name(condor)
    assert "alice" not in users and "group_physics" not in queues and "default" in queues
    machines = {ad["Machine"] for ad in json.loads((SAMPLE / "condor_status.json").read_text())}
    assert len(machines) == 5 and not machines & set(nodes)
    assert len(nodes) == 5  # remote hosts resolved to the same anonymized machines
    assert any(node["np"] == 32 and node["core_job_map"].get(0) == "100.0" for node in nodes.values())


def test_every_job_status_letter_is_configured():
    from qtop_py import yaml_parser

    config = yaml_parser.parse(str(Path(__file__).resolve().parents[2] / "qtopconf.yaml"))
    assert set(JOB_STATES.values()) == set(config["state_abbreviations"]["htcondor"])
