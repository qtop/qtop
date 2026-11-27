## Copyright (c) 2026 qtop contributors
## SPDX-License-Identifier: MIT
"""Report drift between CHANGELOG headings, release tags and the version inside each tag; read-only.

Non-blocking for now: prints findings and exits 0; --strict exits 1 when there are any.
It reads local tags, so run `git fetch --tags <qtop/qtop remote>` first (CI checkouts are tagless).
"""

import argparse
import re
import subprocess
import sys
from pathlib import Path
from typing import TYPE_CHECKING, Dict, List, Optional, Tuple

if TYPE_CHECKING:
    from typing import Final

ROOT: "Final" = Path(__file__).resolve().parents[1]
SINCE: "Final" = "0.9.20161130"  # the first tagged 0.9 release; older CHANGELOG history predates tags
RELEASE: "Final" = re.compile(r"v?[0-9]+(\.[0-9]+)+")
VERSION_LINE: "Final" = re.compile(r"__version__ *= *[\"']([^\"']+)[\"']")  # git grep output keeps a tag:path: prefix


def version_key(version: str) -> Tuple[int, ...]:
    """Order dotted versions numerically: 0.9.20261010 < 0.9.20261011, 0.8.10 > 0.8.9."""
    return tuple(int(part) for part in re.findall(r"[0-9]+", version))


def audit(headings: List[str], tags: Dict[str, Tuple[bool, Optional[str]]], current: str, since: str = SINCE) -> List[str]:
    """Return one finding per drift; tags maps name -> (annotated, __version__ inside the tag)."""
    floor = version_key(since)
    findings: List[str] = []
    for heading in headings:
        if heading == current or version_key(heading) < floor:
            continue  # the top section is tagged only after its PR merges
        if "v" + heading in tags:
            continue
        findings.append(heading + ": tagged only as bare " + heading + ", no v" + heading if heading in tags else heading + ": CHANGELOG section has no tag")
    for name in sorted(tags, key=version_key):
        annotated, inside = tags[name]
        base = name[1:] if name.startswith("v") else name
        if not RELEASE.fullmatch(name):
            findings.append(name + ": not a release tag name (v<version>)")
            continue
        if version_key(base) < floor:
            continue
        if base not in headings:
            findings.append(name + ": tag has no CHANGELOG section " + base)
        if not annotated:
            findings.append(name + ": lightweight tag (release tags are annotated: git tag -a)")
        if inside != base:
            findings.append(name + ": __version__ inside the tag is " + str(inside))
    return findings


def git(*args: str) -> str:
    """Return the text output of a git command in the checkout."""
    return subprocess.check_output(("git",) + args, cwd=str(ROOT), universal_newlines=True, stderr=subprocess.DEVNULL)


def tag_version(name: str) -> Optional[str]:
    """Return the __version__ string as committed in the tag, wherever the package kept it then."""
    for command in (("show", name + ":qtop_py/__init__.py"), ("grep", "-h", "-E", "^__version__ *=", name, "--", "*.py")):
        try:
            match = VERSION_LINE.search(git(*command))
        except subprocess.CalledProcessError:
            continue  # older layouts kept __version__ elsewhere, or the tag has none
        if match:
            return match.group(1)
    return None


def main() -> int:
    """Print the findings; exit 1 only with --strict and findings."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--since", default=SINCE, help="ignore versions older than this (default %(default)s)")
    parser.add_argument("--strict", action="store_true", help="exit 1 when there are findings")
    args = parser.parse_args()
    headings = re.findall(r"^## (.+)$", (ROOT / "CHANGELOG.md").read_text(encoding="utf-8"), re.M)
    package = VERSION_LINE.search((ROOT / "qtop_py/__init__.py").read_text(encoding="utf-8"))
    if package is None:
        parser.error("package version assignment was not found")
    current = package.group(1)
    tags: Dict[str, Tuple[bool, Optional[str]]] = {}
    for line in git("for-each-ref", "refs/tags", "--format=%(refname:short) %(objecttype)").splitlines():
        name, kind = line.rsplit(" ", 1)
        tags[name] = (kind == "tag", tag_version(name))
    findings = audit(headings, tags, current, args.since)
    for finding in findings:
        print(finding)
    print("release-audit: %d finding(s) across %d headings and %d tags%s" % (len(findings), len(headings), len(tags), "" if args.strict else " (non-blocking)"))
    return 1 if findings and args.strict else 0


if __name__ == "__main__":
    sys.exit(main())
