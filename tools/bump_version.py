#!/usr/bin/env python3
##
## qtop is a tool to monitor queuing systems - https://github.com/qtop/qtop
##
## Copyright (c) 2026 Jacob Hatchett
##
## SPDX-License-Identifier: MIT
##

"""Automated version bumping tool for qtop."""

import argparse
import datetime
import re
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def get_current_version():
    """Extract current version from qtop_py/__init__.py"""
    init_file = ROOT / "qtop_py" / "__init__.py"
    content = init_file.read_text()

    match = re.search(r'__version__ = ["\']([^"\']+)["\']', content)
    if not match:
        raise ValueError("Could not find __version__ in qtop_py/__init__.py")

    return match.group(1)


def update_version_file(new_version):
    """Update version in qtop_py/__init__.py"""
    init_file = ROOT / "qtop_py" / "__init__.py"
    content = init_file.read_text()

    updated_content = re.sub(r'__version__ = ["\']([^"\']+)["\']', f'__version__ = "{new_version}"', content)

    init_file.write_text(updated_content)
    print(f"Updated qtop_py/__init__.py: {new_version}")


def update_changelog(new_version):
    """Add new version entry to CHANGELOG.md"""
    changelog_file = ROOT / "CHANGELOG.md"
    content = changelog_file.read_text()

    # Add new entry at the top after the header
    today = datetime.date.today().strftime("%Y-%m-%d")
    new_entry = f"\n## {new_version}\n- Version bump on {today}\n\n"

    # Insert after the first line (# Changelog)
    lines = content.split("\n")
    lines.insert(1, new_entry.strip())

    changelog_file.write_text("\n".join(lines))
    print(f"Updated CHANGELOG.md: {new_version}")


def generate_version():
    """Generate new version in format: 0.9.YYYYMMDD"""
    today = datetime.date.today()
    return f"0.9.{today.strftime('%Y%m%d')}"


def main():
    parser = argparse.ArgumentParser(description="Bump qtop version")
    parser.add_argument("--version", help="Specific version to set (default: auto-generate based on date)")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be changed without making changes")

    args = parser.parse_args()

    current_version = get_current_version()
    new_version = args.version or generate_version()

    print(f"Current version: {current_version}")
    print(f"New version: {new_version}")

    if current_version == new_version:
        print("Version is already current.")
        return 0

    if args.dry_run:
        print("DRY RUN - no changes made")
        return 0

    try:
        update_version_file(new_version)
        update_changelog(new_version)
        print(f"Successfully bumped version to {new_version}")
        print("Don't forget to commit the changes!")

    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
