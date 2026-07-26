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

import pytest

from qtop_py.plugins.oar import OarStatExtractor


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
