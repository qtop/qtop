# Copyright (c) 2026 qtop contributors
# SPDX-License-Identifier: MIT
"""Preview or write synchronized release files; never commit, tag or publish."""

import argparse
import datetime
import difflib
import re
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def release_plan(root, VERSION):
    """Validate a date version and return all file edits without changing files."""
    if not re.fullmatch(r"0\.9\.[0-9]{8}", VERSION):
        raise ValueError("expected 0.9.YYYYMMDD")
    date = datetime.datetime.strptime(VERSION[4:], "%Y%m%d").strftime("%Y-%m-%d")
    package = root / "qtop_py/__init__.py"
    old = re.search(r'__version__ = "([^"]+)"', package.read_text()).group(1)
    if VERSION <= old:
        raise ValueError("release version must advance")
    changelog = (root / "CHANGELOG.md").read_text()
    if "## " + VERSION in changelog:
        raise ValueError("release already exists in changelog")
    intro, pending = changelog.split("## Unreleased\n", 1)
    notes, history = pending.split("\n## ", 1)
    notes = notes.strip()
    changelog = intro + "## Unreleased\n\n## " + VERSION + "\n\n" + (notes + "\n" if notes else "")
    changelog += "- Release: update version metadata to " + VERSION + "\n\n## " + history
    old_recipe = "docs/qtop-%s-GCCcore-14.3.0.eb" % old
    new_recipe = old_recipe.replace(old, VERSION)
    if (root / new_recipe).exists():
        raise ValueError("new EasyBuild recipe already exists")
    manpage = (root / "docs/qtop.1").read_text()
    manpage = re.sub(r'(.TH QTOP 1 ")[^"]+(" "qtop )[^"]+', lambda m: m[1] + date + m[2] + VERSION, manpage, count=1)
    return {
        "qtop_py/__init__.py": package.read_text().replace(old, VERSION),
        "docs/qtop.1": manpage,
        "CHANGELOG.md": changelog,
        old_recipe: None,
        new_recipe: (root / old_recipe).read_text().replace(old, VERSION),
    }


def main():
    """Preview the branch-derived release, requiring --write to edit files."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--version")
    parser.add_argument("--write", action="store_true")
    args = parser.parse_args()
    branch = subprocess.check_output(["git", "branch", "--show-current"], cwd=str(ROOT), universal_newlines=True).strip()
    VERSION = args.version or "0.9." + branch[:8]
    try:
        edits = release_plan(ROOT, VERSION)
        for tag in (VERSION, "v" + VERSION):
            if subprocess.call(["git", "show-ref", "--verify", "--quiet", "refs/tags/" + tag], cwd=str(ROOT)) == 0:
                raise ValueError("release tag already exists: " + tag)
        dirty = subprocess.check_output(["git", "diff", "--name-only", "HEAD", "--"] + list(edits), cwd=str(ROOT), universal_newlines=True)
        if args.write and dirty:
            raise ValueError("release files have uncommitted edits: " + dirty.strip())
        for name, content in edits.items():
            path = ROOT / name
            before = path.read_text() if path.exists() else ""
            print("".join(difflib.unified_diff(before.splitlines(True), (content or "").splitlines(True), fromfile=name, tofile=name)), end="")
        if args.write:
            for name, content in edits.items():
                path = ROOT / name
                if content is None:
                    path.unlink()
                else:
                    path.write_text(content)
    except (ValueError, OSError) as error:
        parser.error(str(error))


if __name__ == "__main__":
    main()
