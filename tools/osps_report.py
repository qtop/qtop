## Copyright (c) 2026 qtop contributors
## SPDX-License-Identifier: MIT
"""Reduce a Darnit OSPS Baseline audit (darnit audit -o json) to a shareable report.

The raw audit embeds command evidence, such as the authenticated user's GitHub profile,
so it stays in build/. This keeps only each control's ID, level, status and details,
and writes osps-baseline.json plus a Markdown summary for review. It never fails on
findings: the audit is report-only input to the OSPS Baseline self-assessment.
"""

import argparse
import json
import sys
from collections import Counter
from pathlib import Path

STATUSES = ("PASS", "FAIL", "WARN", "ERROR", "NA")
KEPT_FIELDS = ("id", "level", "status", "details")


def reduce_audit(audit):
    """Return the framework, per-level status counts and evidence-free control results."""
    controls = [{field: result.get(field) for field in KEPT_FIELDS} for result in audit["results"]]
    controls.sort(key=lambda control: (control["level"] or 0, control["id"]))
    levels = sorted({control["level"] for control in controls if control["level"] is not None})
    counts = {str(level): dict(Counter(control["status"] for control in controls if control["level"] == level)) for level in levels}
    return {"framework": audit.get("framework"), "summary": audit.get("summary", {}), "by_level": counts, "controls": controls}


def markdown(report):
    """Return the Markdown summary: counts per maturity level, then every control not passing."""
    lines = ["# OSPS Baseline audit (Darnit, report only)", "", "| Level | " + " | ".join(STATUSES) + " |", "|---|" + "---|" * len(STATUSES)]
    for level, counts in report["by_level"].items():
        lines.append("| %s | %s |" % (level, " | ".join(str(counts.get(status, 0)) for status in STATUSES)))
    lines += ["", "## Not passing", "", "| Control | Level | Status | Details |", "|---|---|---|---|"]
    for control in report["controls"]:
        if control["status"] != "PASS":
            details = str(control["details"] or "").replace("|", "\\|").replace("\n", " ")
            lines.append("| %s | %s | %s | %s |" % (control["id"], control["level"], control["status"], details))
    return "\n".join(lines) + "\n"


def main(argv=None):
    """Write osps-baseline.json and osps-baseline.md from a raw Darnit audit."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("audit", type=Path)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args(argv)
    report = reduce_audit(json.loads(args.audit.read_text()))
    args.output_dir.mkdir(parents=True, exist_ok=True)
    (args.output_dir / "osps-baseline.json").write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    (args.output_dir / "osps-baseline.md").write_text(markdown(report))
    print("wrote %s: %s" % (args.output_dir, json.dumps(report["summary"], sort_keys=True)))
    return 0


if __name__ == "__main__":
    sys.exit(main())
