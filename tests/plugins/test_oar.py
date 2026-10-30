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


class _Opts:
    ANONYMIZE = False


def _make_extractor():
    return OarStatExtractor({}, _Opts())


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
