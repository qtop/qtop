#!/usr/bin/env python3
##
## qtop is a tool to monitor queuing systems - https://github.com/qtop/qtop
##
## Copyright (c) 2026 Nicola Trozzi
##
## SPDX-License-Identifier: MIT
##

"""Validate an exported qtop document trace without committing trace data."""

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


def load_exported_document(path):
    payload = decode_trace_payload(path)
    if not isinstance(payload, list) or len(payload) != 5:
        raise ValueError("expected exported qtop document payload with 5 top-level fields")

    worker_nodes, raw_jobs, raw_queues, total_running_jobs, total_queued_jobs = payload
    JobDoc = namedtuple("JobDoc", ["user_name", "job_state", "job_queue"])
    QDoc = namedtuple("QDoc", ["lm", "queued", "run", "state"])

    jobs_dict = OrderedDict((job_id, JobDoc(*values)) for job_id, values in raw_jobs.items())
    queues_dict = OrderedDict((queue, QDoc(*values)) for queue, values in raw_queues.items())
    return qtop.Document(worker_nodes, jobs_dict, queues_dict, total_running_jobs, total_queued_jobs)


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


def render_document(document, show_account_totals=False, color="OFF", term_height=1000, term_columns=220):
    args = qtop_args(show_account_totals=show_account_totals, color=color)
    qtop.args = args
    qtop.QTOPPATH = str(ROOT)
    qtop.QTOPCONF_YAML = "qtopconf.yaml"
    qtop.QTOP_LOGFILE = os.path.join(tempfile.gettempdir(), "qtop_trace_export.log")
    qtop.SAMPLE_FILENAME = "qtop_trace_export_sample.tar.gz"
    qtop.dynamic_config = {"force_names": 1}
    qtop.scheduler = "slurm"

    config, user_to_color, nodestate_to_color = qtop.load_yaml_config()
    config = qtop.update_config_with_cmdline_vars(args, config)
    config["extract_info"] = None
    qtop.config = config
    qtop.user_to_color = user_to_color
    qtop.web = SimpleNamespace(stop=lambda: None)
    qtop.viewport = qtop.Viewport()
    qtop.viewport.set_term_size(term_height, term_columns)
    qtop.transposed_matrices = []
    qtop.h_counter = iter([0, 1])
    qtop.help_main_switch = []

    trace_document = qtop.Document(copy.deepcopy(document.worker_nodes), document.jobs_dict, document.queues_dict, document.total_running_jobs, document.total_queued_jobs)
    processed_nodes = qtop.keep_queue_initials_only_and_colorize(trace_document.worker_nodes, qtop.queue_to_color)
    processed_nodes = qtop.colorize_nodestate(processed_nodes, nodestate_to_color, qtop.colorize)
    cluster = qtop.Cluster(trace_document, processed_nodes, qtop.WNFilter, config, args)
    qtop.cluster = cluster
    wns_occupancy = qtop.WNOccupancy(cluster, config, trace_document, user_to_color, list(trace_document.jobs_dict.keys()))
    display = qtop.TextDisplay(trace_document, config, qtop.viewport, wns_occupancy, cluster, args)

    output = io.StringIO()
    with redirect_stdout(output):
        display.display_selected_sections(tempfile.gettempdir(), qtop.SAMPLE_FILENAME, qtop.QTOP_LOGFILE)
    return output.getvalue(), wns_occupancy


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
    document = load_exported_document(args.trace_json_b64)
    rendered, wns_occupancy = render_document(document)
    rendered_with_totals, _ = render_document(document, show_account_totals=True)
    rendered_color, wns_occupancy_color = render_document(document, color="ON")
    rendered_color_with_totals, wns_occupancy_color_with_totals = render_document(document, show_account_totals=True, color="ON")

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
                "worker_nodes": len(document.worker_nodes),
                "jobs": len(document.jobs_dict),
                "queues": len(document.queues_dict),
                "total_running_jobs": document.total_running_jobs,
                "total_queued_jobs": document.total_queued_jobs,
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
    print("trace-export: ok workers=%s jobs=%s queues=%s" % (len(document.worker_nodes), len(document.jobs_dict), len(document.queues_dict)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
