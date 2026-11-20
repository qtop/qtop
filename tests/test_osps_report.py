## Copyright (c) 2026 qtop contributors
## SPDX-License-Identifier: MIT
"""The OSPS Baseline report keeps only control results, never Darnit's command evidence."""

import json
import re
from pathlib import Path

import yaml

from tools.osps_report import main, markdown, reduce_audit

ROOT = Path(__file__).resolve().parents[1]
AUDIT = {
    "framework": "openssf-baseline",
    "summary": {"total": 3, "pass": 1, "fail": 1, "warn": 1},
    "results": [
        {"id": "OSPS-DO-06.01", "level": 2, "status": "FAIL", "details": "Pattern | not found", "evidence": {"stdout": "secret"}},
        {"id": "OSPS-AC-01.01", "level": 1, "status": "WARN", "details": "manual", "evidence": {"stdout": '{"login": "someone"}'}},
        {"id": "OSPS-LE-01.01", "level": 1, "status": "PASS", "details": "ok", "sieve_phase": "file"},
    ],
}


def test_report_drops_evidence_and_counts_statuses_per_level():
    report = reduce_audit(AUDIT)
    assert [control["id"] for control in report["controls"]] == ["OSPS-AC-01.01", "OSPS-LE-01.01", "OSPS-DO-06.01"]
    assert all(set(control) == {"id", "level", "status", "details"} for control in report["controls"])
    assert report["by_level"] == {"1": {"WARN": 1, "PASS": 1}, "2": {"FAIL": 1}}
    assert "secret" not in json.dumps(report) and "someone" not in json.dumps(report)


def test_markdown_lists_only_controls_not_passing_with_escaped_details():
    text = markdown(reduce_audit(AUDIT))
    assert "| 1 | 1 | 0 | 1 | 0 | 0 |" in text
    assert "| OSPS-DO-06.01 | 2 | FAIL | Pattern \\| not found |" in text
    assert "OSPS-LE-01.01" not in text


def test_main_writes_json_and_markdown(tmp_path):
    raw = tmp_path / "raw.json"
    raw.write_text(json.dumps(AUDIT))
    assert main([str(raw), "--output-dir", str(tmp_path / "out")]) == 0
    assert json.loads((tmp_path / "out/osps-baseline.json").read_text())["framework"] == "openssf-baseline"
    assert (tmp_path / "out/osps-baseline.md").read_text().startswith("# OSPS Baseline audit")


def test_audit_is_report_only_read_only_and_pinned():
    workflow = yaml.safe_load((ROOT / ".github/workflows/osps-baseline.yml").read_text())
    assert workflow["permissions"] == {"contents": "read"} and "permissions" not in workflow["jobs"]["audit"]
    assert [step["run"] for step in workflow["jobs"]["audit"]["steps"] if "run" in step][0] == "make osps-audit PYTHON=python"
    makefile = (ROOT / "Makefile").read_text()
    assert re.search(r"^OSPS_TOOLS \?= darnit-mcp==\S+ zizmor==\S+$", makefile, re.M)
    recipe = makefile.split("osps-audit::", 1)[1].split("\n\n", 1)[0]
    assert "audit --no-fail" in recipe and "remediat" not in recipe.lower()
    assert "build/osps-darnit.json" in recipe and "$(OSPS_DIR)" in recipe.split("osps_report.py", 1)[1]
