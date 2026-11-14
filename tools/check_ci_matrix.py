# Copyright (c) 2026 qtop contributors
# SPDX-License-Identifier: MIT
"""Compare expanded nightly lanes across GitHub and GitLab."""

from collections import Counter
from itertools import product
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Final

import yaml

ROOT: "Final" = Path(__file__).resolve().parents[1]
FIELDS: "Final" = ("image", "family", "pkgs", "pybin", "target", "experimental")
DEFAULTS: "Final" = {"pkgs": "-", "pybin": "python3", "target": "ci", "experimental": False}


def lane_counts(github, gitlab):
    """Return lane multisets, retaining duplicate lanes and failure policy."""
    gh = Counter(tuple(dict(DEFAULTS, **lane)[field] for field in FIELDS) for lane in github["jobs"]["lane"]["strategy"]["matrix"]["include"])
    gl = Counter()
    for name in ("nightly-matrix", "nightly-matrix-experimental"):
        job = gitlab[name]
        for row in job["parallel"]["matrix"]:
            values = dict(DEFAULTS, **{key.lower(): value for key, value in row.items()})
            values["experimental"] = job.get("allow_failure", False)
            axes = [value if isinstance(value, list) else [value] for value in (values[field] for field in FIELDS)]
            gl.update(product(*axes))
    return gh, gl


def read_matrices():
    """Read provider documents with the developer-only safe YAML loader."""
    return tuple(yaml.safe_load((ROOT / path).read_text()) for path in (".github/workflows/nightly-matrix.yml", "ci/nightly-matrix.gitlab-ci.yml"))


def main():
    """Fail with missing and extra lanes when the provider matrices drift."""
    gh, gl = lane_counts(*read_matrices())
    if gh != gl:
        raise SystemExit("Nightly matrix drift: GitHub-only %s; GitLab-only %s" % (gh - gl, gl - gh))
    print("Nightly matrix parity: %d lanes" % sum(gh.values()))


if __name__ == "__main__":
    main()
