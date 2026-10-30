##
## qtop is a tool to monitor queuing systems - https://github.com/qtop/qtop
##
## Copyright (c) 2016 Fotis Georgatos
## Copyright (c) 2016 Sotiris Fragkiskos
## Copyright (c) 2026 Utkarsh Sinha
##
## SPDX-License-Identifier: MIT
##

"""Tests for the OAR statistics extractor.

OAR previously had no empty-input guard, unlike the PBS, SGE and Slurm plugins
which all call ``fileutils.check_empty_file``. An empty ``oarstat`` file would
therefore be parsed as if it held data. ``extract_qstat`` now mirrors the PBS
behaviour: log the empty file and return no records.
"""

import logging
from types import SimpleNamespace

import pytest

from qtop_py.plugins.oar import DEFAULT_NODE_STATE_MAPPING, OARBatchSystem, OarStatExtractor
from qtop_py.qtop import Cluster, Document, WNOccupancy
from qtop_py.sample import anonymize_state


class _Opts:
    ANONYMIZE = False


def _make_extractor():
    return OarStatExtractor({}, _Opts())


@pytest.fixture
def oar_trace(tmp_path):
    def create(details, jobs=None):
        paths = {}
        traces = {
            "oarnodes_s": "---\nnode1:\n    1: Alive\n",
            "oarnodes_y": details,
            "oarstat": "Job id Name User Submission Date S Queue\n------ ---- ---- --------------- - -----\n"
            + ("101 job+name alice 2026-10-29 10:00:00 R default\n102 job bob 2026-10-29 10:00:00 R debug\n" if jobs is None else jobs),
        }
        for name, text in traces.items():
            path = tmp_path / (name + ".txt")
            path.write_text(text)
            paths[name + "_file"] = str(path)
        return OARBatchSystem(paths, {}, _Opts())

    return create


@pytest.mark.parametrize(
    "field, owners",
    [
        ("jobs: '101'", ["101"]),
        ('jobs: "101"', ["101"]),
        ("jobs: 101,102", ["101", "102"]),
        ("jobs: [101, 102]", ["101", "102"]),
        ("jobs:\n    - 101\n    - '102'", ["101", "102"]),
    ],
)
def test_resource_yaml_variants_preserve_all_job_memberships(oar_trace, field, owners):
    backend = oar_trace("---\n\n1:\n    state: Alive\n\n    " + field + "\n")
    state = backend.get_cluster_state(backend.options)
    node = state["nodes"][0]
    assert node["core_job_map"] == {0: "101"}
    assert node["oar_job_ids"] == owners
    assert set(node["qname"]) == ({"default", "debug"} if len(owners) > 1 else {"default"})
    assert WNOccupancy._count_jobs_strict(state["nodes"]) == len(owners)
    cluster_node = dict(node)
    Cluster._calculate_jobs_per_node(None, {1: cluster_node})
    cluster = SimpleNamespace(workernode_dict={1: cluster_node})
    usage = WNOccupancy.calculate_user_node_use(None, cluster, {"101": ("alice", "default"), "102": ("bob", "debug")})
    assert usage == ({"alice": 1, "bob": 1} if len(owners) > 1 else {"alice": 1})
    assert Document.from_cluster_state(state).to_cluster_state() == state
    masked, mappings = anonymize_state(state)
    assert masked["nodes"][0]["oar_job_ids"] == [mappings["job"][job] for job in owners]
    assert Document.from_cluster_state(masked).to_cluster_state() == masked


def test_unknown_owners_are_retained_with_repeatable_diagnostics(oar_trace):
    backend = oar_trace("---\n1:\n    jobs: 999\n", jobs="")
    state = backend.get_cluster_state(backend.options)
    assert state["nodes"][0]["core_job_map"] == {0: "999"}
    assert state["nodes"][0]["oar_job_ids"] == ["999"]
    assert state["oar_diagnostics"] == {"unlisted_jobs": 1}
    assert backend.get_cluster_state(backend.options) == state
    masked, _ = anonymize_state(state)
    assert masked["oar_diagnostics"] == state["oar_diagnostics"]


def test_mismatched_resource_snapshots_are_flagged_without_inventing_capacity(oar_trace):
    backend = oar_trace("---\n2:\n    jobs: 101\n")
    state = backend.get_cluster_state(backend.options)
    assert state["nodes"][0]["np"] == 1
    assert state["nodes"][0]["core_job_map"] == {0: "?"}
    assert state["oar_diagnostics"] == {"missing_resources": 1, "extra_resources": 1}


@pytest.mark.parametrize("details", ["---\n1:\n    jobs: 101,no\n", "---\n1:\n    jobs: 101\n1:\n    jobs: 102\n"])
def test_unusable_resource_ownership_fails_explicitly(oar_trace, details):
    backend = oar_trace(details)
    with pytest.raises(ValueError):
        backend.get_cluster_state(backend.options)


def test_extract_qstat_empty_file_returns_no_records(tmp_path, caplog):
    empty = tmp_path / "oarstat_empty.txt"
    empty.write_text("")

    with caplog.at_level(logging.ERROR):
        result = _make_extractor().extract_qstat(str(empty))

    assert result == []
    assert any("empty" in record.message.lower() for record in caplog.records)


def test_extract_qstat_reads_populated_file(tmp_path):
    # header + dashes + one job row matching the OAR user_q_search pattern
    content = (
        "Job id    Name       User       Submission Date       S Queue\n"
        "--------- ---------- ---------- --------------------- - --------\n"
        "101       jobname    alice      2026-06-17 10:00:00   R default\n"
    )
    populated = tmp_path / "oarstat.txt"
    populated.write_text(content)

    result = _make_extractor().extract_qstat(str(populated))

    assert len(result) == 1
    assert result[0]["JobId"] == "101"
    assert result[0]["UnixAccount"] == "alice"
    assert result[0]["S"] == "R"
    assert result[0]["Queue"] == "default"


def test_extract_qstat_missing_file_still_raises(tmp_path):
    # A genuinely missing path is a different error class than "empty" and must
    # not be silently swallowed by the new empty-file guard.
    missing = tmp_path / "does_not_exist.txt"
    with pytest.raises((OSError, IOError)):
        _make_extractor().extract_qstat(str(missing))


@pytest.mark.parametrize("anonymize", (False, True))
def test_queue_counts_reuse_job_snapshot_and_preserve_custom_queues(tmp_path, monkeypatch, anonymize):
    path = tmp_path / "oarstat.txt"
    path.write_text(
        "Job id Name User Submission Date S Queue\n"
        "------ ---- ---- --------------- - -----\n"
        "101 job alice 2026-10-29 10:00:00 R default\n"
        "102 job bob 2026-10-29 10:00:00 W default\n"
        "103 job alice 2026-10-29 10:00:00 R default-long\n"
        "104 job bob 2026-10-29 10:00:00 F default-long\n"
        "105 job bob 2026-10-29 10:00:00 H held\n"
    )
    backend = OARBatchSystem({"oarstat_file": str(path)}, {}, SimpleNamespace(ANONYMIZE=anonymize))
    original = backend.oar_stat_maker.extract_qstat
    reads = []

    def extract(filename):
        reads.append(filename)
        return original(filename)

    monkeypatch.setattr(backend.oar_stat_maker, "extract_qstat", extract)
    running, waiting, queues = backend.get_queues_info()
    ids, users, states, names = backend.get_jobs_info()
    assert running == 2 and waiting == 1
    assert ids == ["101", "102", "103", "104", "105"]
    assert states == ["R", "W", "R", "F", "H"]
    assert [(queue["run"], queue["queued"]) for queue in queues] == [(1, 1), (1, 0), (0, 0)]
    assert [queue["queue_name"] for queue in queues] == [names[0], names[2], names[4]]
    assert all(queue["lm"] == "--" and queue["state"] == "?" for queue in queues)
    if anonymize:
        assert not set(names) & {"default", "default-long", "held"}
        assert "alice" not in users
    else:
        assert names == ["default", "default", "default-long", "default-long", "held"]
    assert backend.get_queues_info() == (running, waiting, queues)
    assert reads == [str(path)]


@pytest.mark.parametrize("content", ["", "Job id Name User Submission Date S Queue\n------ ---- ---- --------------- - -----\n"])
def test_queue_counts_with_no_jobs(tmp_path, content):
    path = tmp_path / "oarstat.txt"
    path.write_text(content)
    backend = OARBatchSystem({"oarstat_file": str(path)}, {}, _Opts())
    assert backend.get_queues_info() == (0, 0, [])


@pytest.mark.parametrize(
    ("resources", "expected"),
    (
        ([], "?"),
        ([("1", "Alive"), ("2", "Alive")], "-"),
        ([("1", "Alive"), ("2", "Dead")], "%"),
        ([("1", "vendor-specific")], "?"),
    ),
)
def test_calculate_oar_state_handles_uniform_mixed_and_unknown_states(resources, expected):
    assert OARBatchSystem._calculate_oar_state(resources, DEFAULT_NODE_STATE_MAPPING) == expected
