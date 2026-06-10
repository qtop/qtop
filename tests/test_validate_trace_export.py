import base64
import gzip
import json
import sys

import pytest

import tools.validate_trace_export as validate_trace_export
from tools.validate_trace_export import (
    account_symbols,
    load_exported_document,
    rendered_account_symbols,
    strip_ansi,
    validate_rendered_account_symbols,
    validate_symbol_contract,
)


def write_encoded_payload(tmp_path, payload):
    path = tmp_path / "trace.json.b64"
    path.write_bytes(base64.b64encode(gzip.compress(json.dumps(payload).encode("utf-8"))))
    return path


def test_load_exported_document_rehydrates_named_records(tmp_path):
    payload = [
        [{"domainname": "node001", "np": "1", "state": "-", "qname": ["debug"], "core_job_map": {"0": "1"}}],
        {"1": ["alice", "R", "debug"]},
        {"debug": ["1", 0, 1, "E"]},
        1,
        0,
    ]

    document = load_exported_document(write_encoded_payload(tmp_path, payload))

    assert document.jobs_dict["1"].user_name == "alice"
    assert document.jobs_dict["1"].job_state == "R"
    assert document.queues_dict["debug"].run == 1
    assert document.total_running_jobs == 1


def test_account_symbols_reads_qtop_symbol_column():
    class WNOccupancy:
        account_jobs_table = [["0", 1], ["*", 2]]

    assert account_symbols(WNOccupancy()) == ["0", "*"]


def test_validate_symbol_contract_accepts_long_tail_without_reserved_symbols():
    validate_symbol_contract(["0", "1", "*"])


@pytest.mark.parametrize("reserved", ["_", "#", "?"])
def test_validate_symbol_contract_rejects_reserved_account_symbols(reserved):
    with pytest.raises(ValueError, match="reserved symbols"):
        validate_symbol_contract(["0", reserved, "*"])


def test_rendered_account_symbols_strip_colour_sequences():
    rendered = "\x1b[31m[ *]\x1b[0m t_anon_user_104   |   2      2      0 |     2 |"

    assert strip_ansi(rendered) == "[ *] t_anon_user_104   |   2      2      0 |     2 |"
    assert rendered_account_symbols(rendered) == ["*"]


@pytest.mark.parametrize("reserved", ["_", "#", "?"])
def test_validate_rendered_account_symbols_rejects_reserved_symbols(reserved):
    rendered = "[ %s] t_anon_user_104   |   2      2      0 |     2 |" % reserved

    with pytest.raises(ValueError, match="reserved symbols"):
        validate_rendered_account_symbols(rendered)


def test_main_writes_colour_account_totals_artifacts(tmp_path, monkeypatch):
    trace_path = tmp_path / "trace.json.b64"
    trace_path.write_text("placeholder")
    artifact_dir = tmp_path / "artifacts"

    class Document(object):
        worker_nodes = [{}]
        jobs_dict = {"1": object()}
        queues_dict = {"debug": object()}
        total_running_jobs = 1
        total_queued_jobs = 0

    class WNOccupancy(object):
        account_jobs_table = [["*", 1]]

    def fake_render_document(document, show_account_totals=False, color="OFF"):
        rendered = "[ *] t_anon_user_104   |   1      1      0 |     1 |"
        if show_account_totals:
            rendered += "\n[ T] Totals            |   1      1      0 |     1 |"
        if color == "ON":
            rendered = "\x1b[31m%s\x1b[0m" % rendered
        return rendered, WNOccupancy()

    monkeypatch.setattr(validate_trace_export, "load_exported_document", lambda path: Document())
    monkeypatch.setattr(validate_trace_export, "render_document", fake_render_document)
    monkeypatch.setattr(sys, "argv", ["validate_trace_export.py", str(trace_path), "--artifact-dir", str(artifact_dir)])

    assert validate_trace_export.main() == 0

    rendered_colour_totals = (artifact_dir / "rendered-color-accounttotals.ans").read_text()
    rendered_plain_totals = (artifact_dir / "rendered-color-accounttotals.out").read_text()
    summary = json.loads((artifact_dir / "summary.json").read_text())

    assert "\x1b[" in rendered_colour_totals
    assert "[ T] Totals" in rendered_plain_totals
    assert summary["rendered_color_accounttotals_has_ansi"] is True
    assert summary["color_accounttotals_symbols"] == ["*"]
