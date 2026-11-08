##
## qtop is a tool to monitor queuing systems - https://github.com/qtop/qtop
##
## Copyright (c) 2026 qtop contributors
##
## SPDX-License-Identifier: MIT
##
"""Regression tests for the PySCG 0016/0043/0045/0053 fixes."""

import os
import re
from pathlib import Path
from types import SimpleNamespace

import pytest

from qtop_py import qtop as qtop_module
from qtop_py.errors import OperationalError
from qtop_py.plugins import sge
from qtop_py.plugins.pbs import PBSStatExtractor

ROOT = Path(__file__).resolve().parents[1]


def test_unexpected_sge_xml_failures_are_reported_not_silently_exited(tmp_path, monkeypatch):
    path = tmp_path / "qstat.F.xml.stdout"
    path.write_text("<job_info/>")

    def broken_parser():
        raise RuntimeError("parser exploded")

    monkeypatch.setattr(sge, "_xml_parser", broken_parser)
    extractor = sge.SGEStatExtractor({}, SimpleNamespace(ANONYMIZE=False), {})
    with pytest.raises(OperationalError, match="Cannot read SGE XML .*parser exploded") as raised:
        extractor.get_xml_tree(str(path))
    assert isinstance(raised.value.__cause__, RuntimeError)


def test_scheduler_commands_get_the_c_locale_without_touching_our_environment(monkeypatch):
    monkeypatch.setenv("LC_ALL", "de_CH.UTF-8")
    env = qtop_module.scheduler_env()
    assert env["LC_ALL"] == "C" and os.environ["LC_ALL"] == "de_CH.UTF-8"


def test_user_detail_command_runs_in_the_c_locale(monkeypatch):
    seen = {}

    def fake_popen(*args, **kwargs):
        seen.update(kwargs)
        raise OSError("not executed")

    monkeypatch.setattr(qtop_module.subprocess, "Popen", fake_popen)
    config = {"extract_info": {"user_details_realtime": "getent passwd %s"}, "scheduler_command_timeout": 5}
    table = [["0", 1, 0, 1, "alice", 1]]
    assert qtop_module.get_detail_of_name(table, config, SimpleNamespace(GET_GECOS=True)) == {}
    assert seen["env"]["LC_ALL"] == "C"


def test_non_utf8_scheduler_output_is_parsed_not_fatal(tmp_path):
    path = tmp_path / "qstat.txt"
    path.write_bytes(
        b"Job id           Name             User             Time Use S Queue\n"
        b"---------------- ---------------- ---------------- -------- - -----\n"
        b"807140.delta     caf\xe9-run         alice            22:41:42 R gpu\n"
    )
    records = PBSStatExtractor({}, SimpleNamespace(ANONYMIZE=False)).extract_qstat(str(path))
    assert records == [{"JobId": "807140", "UnixAccount": "alice", "S": "R", "Queue": "gpu"}]


def test_web_page_loads_no_third_party_script():
    page = (ROOT / "qtop_py" / "web" / "index.html").read_text()
    assert not re.search(r"<script[^>]+src=", page, re.I) and "jquery" not in page.lower()
    assert 'fetch("api.json"' in page
