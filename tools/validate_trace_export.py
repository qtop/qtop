#!/usr/bin/env python3
##
## qtop is a tool to monitor queuing systems - https://github.com/qtop/qtop
##
## Copyright (c) 2026 Nicola Trozzi
##
## SPDX-License-Identifier: MIT
##

"""Validate an exported qtop snapshot trace without committing trace data."""

import argparse
import base64
import copy
import gzip
import io
import json
import os
import re
import sys
import tempfile
from collections import OrderedDict, namedtuple
from contextlib import redirect_stdout
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import qtop_py.qtop as qtop  # noqa: E402

ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")
ACCOUNT_LINE_RE = re.compile(r"^\[\s*(.)\]\s+(.+?)\s+\|")


def decode_trace_payload(path):
    encoded = Path(path).read_bytes()
    decoded = base64.b64decode(encoded)
    return json.loads(gzip.decompress(decoded))


def load_exported_snapshot(path):
    payload = decode_trace_payload(path)
    if isinstance(payload, dict):
        return qtop.Snapshot.from_cluster_state(payload)
    if not isinstance(payload, list) or len(payload) != 5:
        raise ValueError("expected ClusterState or legacy qtop snapshot payload")

    worker_nodes, raw_jobs, raw_queues, total_running_jobs, total_queued_jobs = payload
    JobRow = namedtuple("JobRow", ["user_name", "job_state", "job_queue"])
    QueueRow = namedtuple("QueueRow", ["lm", "queued", "run", "state"])

    jobs_dict = OrderedDict((job_id, JobRow(*values)) for job_id, values in raw_jobs.items())
    queues_dict = OrderedDict((queue, QueueRow(*values)) for queue, values in raw_queues.items())
    return qtop.Snapshot(worker_nodes, jobs_dict, queues_dict, total_running_jobs, total_queued_jobs)


def qtop_args(show_account_totals=False, color="OFF"):
    return SimpleNamespace(
        COLOR=color,
        CLASSIC=False,
        WATCH=False,
        SAMPLE=False,
        STRICTCHECK=False,
        FORCE_NAMES=True,
        BLINDREMAP=False,
        REMAP=False,
        NOMASKING=False,
        SHOW_ACCOUNT_TOTALS=show_account_totals,
        TRANSPOSE=False,
        sect_1_off=False,
        sect_2_off=False,
        sect_3_off=False,
        ONLYSAVETOFILE=False,
        LESS=False,
        REPLAY=None,
        BATCH_SYSTEM="slurm",
        SOURCEDIR=None,
        WEB=False,
        EXPORT=False,
        ANONYMIZE=False,
        EXPERIMENTAL=False,
        REM_EMPTY_CORELINES=False,
        CONFFILE=None,
        OPTION=[],
        GET_GECOS=False,
        DEBUG=False,
        verbose=None,
        version=False,
    )


def render_snapshot(snapshot, show_account_totals=False, color="OFF", term_height=1000, term_columns=220):
    """Render a snapshot offline through the same model/query/view pipeline as qtop's main loop."""
    args = qtop_args(show_account_totals=show_account_totals, color=color)
    config, user_to_color, nodestate_to_color = qtop.configuration.load_yaml_config(args, str(ROOT), os.getcwd(), qtop.SYSTEMCONFDIR, qtop.USERPATH)
    config["extract_info"] = None
    viewport = qtop.Viewport()
    viewport.set_term_size(term_height, term_columns)
    trace_snapshot = qtop.Snapshot(copy.deepcopy(snapshot.worker_nodes), snapshot.jobs_dict, snapshot.queues_dict, snapshot.total_running_jobs, snapshot.total_queued_jobs)
    backend = qtop.discover_qtop_batch_systems()["slurm"]
    model = qtop.MVU_Model(config, args, viewport, display_overrides={"force_names": 1}, package_path=str(ROOT), help_file=str(ROOT / "helpfile.txt"))
    model, _ = qtop.MVU_update(model, "snapshot", (trace_snapshot, config, args, backend, user_to_color, nodestate_to_color))
    view_data = qtop.MVU_query(model)

    output = io.StringIO()
    with redirect_stdout(output):
        qtop.MVU_view(view_data, tempfile.gettempdir(), "qtop_trace_export_sample.tar.gz", os.path.join(tempfile.gettempdir(), "qtop_trace_export.log"))
    return output.getvalue(), view_data.occupancy


def account_symbols(wns_occupancy):
    return [str(line[0]) for line in wns_occupancy.account_jobs_table]


def validate_symbol_contract(symbols):
    forbidden = {"_", "#", "?"}
    used_forbidden = sorted(forbidden.intersection(symbols))
    if used_forbidden:
        raise ValueError("reserved symbols used as account IDs: %s" % ", ".join(used_forbidden))
    if "*" not in symbols:
        raise ValueError("long-tail account symbol '*' was not exercised by the trace")


def strip_ansi(text):
    return ANSI_RE.sub("", text)


def rendered_account_symbols(rendered):
    symbols = []
    for line in strip_ansi(rendered).splitlines():
        match = ACCOUNT_LINE_RE.match(line)
        if match:
            account_name = match.group(2).strip()
            if account_name == "Totals":
                continue
            symbols.append(match.group(1))
    return symbols


def validate_rendered_account_symbols(rendered):
    symbols = rendered_account_symbols(rendered)
    if not symbols:
        raise ValueError("rendered output did not include account-symbol mapping lines")
    validate_symbol_contract(symbols)


def extract_summary_line(text):
    for line in text.splitlines():
        if line.startswith("Summary:"):
            return line.strip()
    return ""


def validate_fullview_summary(rendered, fullview_path):
    expected = extract_summary_line(Path(fullview_path).read_text(errors="replace"))
    actual = extract_summary_line(rendered)
    if expected != actual:
        raise ValueError("summary mismatch: expected %r, got %r" % (expected, actual))


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("trace_json_b64", help="Path to a base64-encoded gzip qtop exported JSON document")
    parser.add_argument("--fullview", help="Optional qtop fullview output to compare stable summary totals against")
    parser.add_argument("--artifact-dir", default="artifacts/trace-export", help="Directory for generated validation artifacts")
    return parser.parse_args()


def main():
    args = parse_args()
    snapshot = load_exported_snapshot(args.trace_json_b64)
    rendered, wns_occupancy = render_snapshot(snapshot)
    rendered_with_totals, _ = render_snapshot(snapshot, show_account_totals=True)
    rendered_color, wns_occupancy_color = render_snapshot(snapshot, color="ON")
    rendered_color_with_totals, wns_occupancy_color_with_totals = render_snapshot(snapshot, show_account_totals=True, color="ON")

    symbols = account_symbols(wns_occupancy)
    validate_symbol_contract(symbols)
    validate_symbol_contract(account_symbols(wns_occupancy_color))
    validate_symbol_contract(account_symbols(wns_occupancy_color_with_totals))
    validate_rendered_account_symbols(rendered_color)
    validate_rendered_account_symbols(rendered_color_with_totals)
    if "[ T] Totals" not in rendered_with_totals:
        raise ValueError("-4/accounttotals rendering did not include the totals row")
    if "[ T] Totals" not in strip_ansi(rendered_color_with_totals):
        raise ValueError("-c ON -4 rendering did not include the totals row")
    if args.fullview:
        validate_fullview_summary(rendered, args.fullview)

    artifact_dir = Path(args.artifact_dir)
    artifact_dir.mkdir(parents=True, exist_ok=True)
    (artifact_dir / "rendered.out").write_text(rendered)
    (artifact_dir / "rendered-accounttotals.out").write_text(rendered_with_totals)
    (artifact_dir / "rendered-color.ans").write_text(rendered_color)
    (artifact_dir / "rendered-color.out").write_text(strip_ansi(rendered_color))
    (artifact_dir / "rendered-color-accounttotals.ans").write_text(rendered_color_with_totals)
    (artifact_dir / "rendered-color-accounttotals.out").write_text(strip_ansi(rendered_color_with_totals))
    (artifact_dir / "summary.json").write_text(
        json.dumps(
            {
                "worker_nodes": len(snapshot.worker_nodes),
                "jobs": len(snapshot.jobs_dict),
                "queues": len(snapshot.queues_dict),
                "total_running_jobs": snapshot.total_running_jobs,
                "total_queued_jobs": snapshot.total_queued_jobs,
                "account_symbols": sorted(set(symbols)),
                "color_account_symbols": sorted(set(rendered_account_symbols(rendered_color))),
                "color_accounttotals_symbols": sorted(set(rendered_account_symbols(rendered_color_with_totals))),
                "rendered_color_has_ansi": "\x1b[" in rendered_color,
                "rendered_color_accounttotals_has_ansi": "\x1b[" in rendered_color_with_totals,
            },
            indent=2,
            sort_keys=True,
        )
    )
    print("trace-export: ok workers=%s jobs=%s queues=%s" % (len(snapshot.worker_nodes), len(snapshot.jobs_dict), len(snapshot.queues_dict)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
