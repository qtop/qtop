# Copyright (c) 2026 qtop contributors
# SPDX-License-Identifier: MIT
"""Enforce runtime statement and branch floors independently of tooling coverage."""

import argparse
import json
from pathlib import Path
from typing import Any, Dict, Tuple


def percentages(report: Dict[str, Any]) -> Tuple[float, float]:
    """Aggregate only production runtime files from coverage.py JSON."""
    summaries = [entry["summary"] for name, entry in report["files"].items() if name.replace("\\", "/").startswith("qtop_py/")]
    if not summaries:
        raise ValueError("coverage report contains no qtop_py files")
    statements = sum(item["num_statements"] for item in summaries)
    branches = sum(item["num_branches"] for item in summaries)
    if not statements or not branches:
        raise ValueError("coverage report must contain runtime statements and branch measurements")
    return (100 * sum(item["covered_lines"] for item in summaries) / statements, 100 * sum(item["covered_branches"] for item in summaries) / branches)


def main() -> None:
    """Read measured coverage and fail without weakening either configured floor."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("report", type=Path)
    parser.add_argument("--statements", type=float, required=True)
    parser.add_argument("--branches", type=float, required=True)
    args = parser.parse_args()
    statements, branches = percentages(json.loads(args.report.read_text()))
    print("Runtime statements: %.2f%%; branches: %.2f%%" % (statements, branches))
    if statements < args.statements or branches < args.branches:
        raise SystemExit("coverage below statement/branch floor %.2f/%.2f" % (args.statements, args.branches))


if __name__ == "__main__":
    main()
