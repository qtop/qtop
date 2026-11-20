## Copyright (c) 2026 qtop contributors
## SPDX-License-Identifier: MIT
"""Tests for the LSF backend's bhosts, bqueues and bjobs parsing."""

from pathlib import Path
from types import SimpleNamespace

import pytest

from qtop_py.plugins.lsf import HOST_STATES, LSFBatchSystem, parse_exec_host

SAMPLE = Path(__file__).resolve().parent / "lsf_samples" / "basic"
BHOSTS = "HOST_NAME STATUS JL/U MAX NJOBS RUN SSUSP USUSP RSV\n"
BQUEUES = "QUEUE_NAME PRIO STATUS MAX JL/U JL/P JL/H NJOBS PEND RUN SUSP\n"
BJOBS = "JOBID|JOBINDEX|STAT|USER|QUEUE|EXEC_HOST\n"


def backend(tmp_path=None, bhosts=None, bqueues=None, bjobs=None, anonymize=False):
    """Return an LSF backend over the sample traces, with any given trace replaced."""
    paths = {key: str(SAMPLE / name) for key, name in (("bhosts_file", "bhosts.txt"), ("bqueues_file", "bqueues.txt"), ("bjobs_file", "bjobs.txt"))}
    for key, text in (("bhosts_file", bhosts), ("bqueues_file", bqueues), ("bjobs_file", bjobs)):
        if text is not None:
            path = tmp_path / key
            path.write_text(text)
            paths[key] = str(path)
    return LSFBatchSystem(paths, {}, SimpleNamespace(ANONYMIZE=anonymize))


def nodes_by_name(lsf):
    job_ids, _users, _states, queues = lsf.get_jobs_info()
    return {node["domainname"]: node for node in lsf.get_worker_nodes(job_ids, queues, None)}


@pytest.mark.parametrize(
    "value, expected",
    (
        ("4*hostA:2*hostB", {"hostA": 4, "hostB": 2}),
        ("hostA:hostA:hostB", {"hostA": 2, "hostB": 1}),
        ("2*hostA:hostA", {"hostA": 3}),
        ("-", {}),
        ("", {}),
    ),
)
def test_exec_host_accepts_compact_and_repeated_forms(value, expected):
    assert dict(parse_exec_host(value)) == expected


@pytest.mark.parametrize("value", ("4*", "*hostA", "hostA::hostB", "4*host A"))
def test_exec_host_rejects_malformed_values(value):
    with pytest.raises(ValueError, match="EXEC_HOST"):
        parse_exec_host(value)


def test_sample_jobs_keep_lsf_states_and_array_indices():
    job_ids, users, states, queues = backend().get_jobs_info()
    assert job_ids == ["1001", "1002", "1003", "1004", "1005", "1006[1]", "1006[2]", "1007", "1008"]
    assert states[5:9] == ["RUN", "PEND", "SSUSP", "PSUSP"]
    assert users[0] == "alice" and queues[1] == "normal"


def test_queue_counts_come_from_jobs_not_bqueues_slot_columns():
    running, queued, queues = backend().get_queues_info()
    by_name = {queue["queue_name"]: queue for queue in queues}
    assert (running, queued) == (4, 3)
    # bqueues says normal RUN 28 / PEND 16 slots; bjobs has two running and one pending job there.
    assert (by_name["normal"]["run"], by_name["normal"]["queued"], by_name["normal"]["lm"]) == (2, 1, "256")
    assert (by_name["idle"]["state"], by_name["idle"]["lm"]) == ("Closed:Inact", "--")


def test_hosts_fill_slots_in_job_order_including_suspended_jobs():
    nodes = nodes_by_name(backend())
    hosta01 = nodes["hosta01"]
    assert hosta01["np"] == 32
    assert [hosta01["core_job_map"][index] for index in range(16)] == ["1001"] * 8 + ["1002"] * 4 + ["1006[1]"] * 2 + ["1007"] * 2
    assert sorted(hosta01["qname"]) == ["normal", "short"]
    assert [nodes[name]["state"] for name in ("hosta01", "hosta02", "hostb01", "hostb02", "hostc01")] == ["-", "j", "o", "d", "-"]


def test_unknown_hosts_and_unlimited_or_oversubscribed_slots(tmp_path):
    lsf = backend(
        tmp_path,
        bhosts=BHOSTS + "small ok - 2 3 3 0 0 0\nopen ok - - 1 1 0 0 0\nodd closed_Mystery - 4 0 0 0 0 0\n",
        bjobs=BJOBS + "1|0|RUN|u|q|3*small:open\n2|0|RUN|u|q|2*elsewhere\n",
    )
    nodes = nodes_by_name(lsf)
    assert nodes["small"]["np"] == 3  # MAX 2, three slots used
    assert nodes["open"]["np"] == 1  # MAX '-'
    assert (nodes["elsewhere"]["np"], nodes["elsewhere"]["state"]) == (2, "?")
    assert nodes["odd"]["state"] == "?"


@pytest.mark.parametrize("text", ("", "No unfinished job found\n"))
def test_no_jobs_yields_empty_lists(tmp_path, text):
    lsf = backend(tmp_path, bjobs=text)
    assert lsf.get_jobs_info() == ([], [], [], [])
    assert all(not node["core_job_map"] for node in nodes_by_name(lsf).values())


@pytest.mark.parametrize(
    "bjobs, message",
    (
        ("JOBID STAT USER QUEUE EXEC_HOST\n1 RUN u q host\n", "lacks columns"),
        (BJOBS + "1|0|RUN|u|q\n", "Cannot parse LSF job row"),
        (BJOBS + "x1|0|RUN|u|q|-\n", "Invalid LSF job ID"),
    ),
)
def test_malformed_bjobs_is_rejected(tmp_path, bjobs, message):
    with pytest.raises(ValueError, match=message):
        backend(tmp_path, bjobs=bjobs).get_jobs_info()


def test_tables_need_their_header_and_full_rows(tmp_path):
    with pytest.raises(ValueError, match="HOST_NAME header"):
        nodes_by_name(backend(tmp_path, bhosts="hosta01 ok - 4 0 0 0 0 0\n"))
    with pytest.raises(ValueError, match="Cannot parse LSF row"):
        backend(tmp_path, bqueues=BQUEUES + "short 40 Open:Active\n").get_queues_info()


@pytest.mark.parametrize("state, blocked", (("Open:Active", False), ("Closed:Active", True), ("Open:Inact", True), ("Closed:Inact", True)))
def test_closed_or_inactive_queues_are_blocked(state, blocked):
    assert LSFBatchSystem.queue_is_blocked(state) is blocked


def test_anonymize_maps_hosts_users_and_queues_consistently():
    lsf = backend(anonymize=True)
    _job_ids, users, _states, queues = lsf.get_jobs_info()
    nodes = nodes_by_name(lsf)
    assert "alice" not in users and "normal" not in queues
    assert not any(name.startswith("host") for name in nodes)
    # The host bjobs places 1001 on is the same anonymized bhosts host.
    assert any(node["core_job_map"].get(0) == "1001" and node["np"] == 32 for node in nodes.values())


def test_every_mapped_state_is_a_known_symbol():
    assert set(HOST_STATES.values()) <= {"-", "j", "b", "o", "d"}
    assert all(key == key.lower() for key in HOST_STATES)
