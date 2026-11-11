#!/usr/bin/env python3
## Copyright (c) 2026 qtop contributors
## SPDX-License-Identifier: MIT
"""Count non-blank, non-comment lines per author with git blame -w -M -C; names sharing an email are one author."""

import subprocess
import sys
from collections import Counter


def count_lines(paths):
    """Return {(name, email): lines} for the tracked Python files under paths."""
    files = subprocess.run(["git", "ls-files", "--", *("%s/*.py" % path.rstrip("/") for path in paths)], stdout=subprocess.PIPE, universal_newlines=True, check=True).stdout.split()
    counts = Counter()
    for name in files:
        blame = subprocess.run(["git", "blame", "-w", "-M", "-C", "--line-porcelain", "--", name], stdout=subprocess.PIPE, universal_newlines=True, check=True).stdout
        for line in blame.splitlines():
            if line.startswith("author "):
                author = line[7:]
            elif line.startswith("author-mail "):
                email = line[12:].strip("<>").lower()
            elif line.startswith("\t") and line[1:].strip() and not line[1:].lstrip().startswith("#"):
                counts[author, email] += 1
    return counts


def merge_by_email(counts):
    """Merge names linked by a shared email; label each author by their most-used name."""
    group = {}  # name or email -> group id
    for name, email in counts:
        ids = {group[key] for key in (name, email) if key in group} or {len(group)}
        target = min(ids)
        for key, value in list(group.items()):
            if value in ids:
                group[key] = target
        group[name] = group[email] = target
    totals, names = Counter(), {}
    for (name, email), lines in counts.items():
        totals[group[name]] += lines
        names.setdefault(group[name], Counter())[name] += lines
    return Counter({names[gid].most_common(1)[0][0]: lines for gid, lines in totals.items()})


def main(paths):
    """Print lines and share per author, highest first."""
    authors = merge_by_email(count_lines(paths or ["qtop_py"]))
    total = sum(authors.values())
    for author, lines in authors.most_common():
        print("%7d %5.1f%%  %s" % (lines, 100.0 * lines / total, author))
    print("%7d 100.0%%  total (%s)" % (total, " ".join(paths or ["qtop_py"])))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
