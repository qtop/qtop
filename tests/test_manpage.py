## Copyright (c) 2026 qtop contributors
## SPDX-License-Identifier: MIT
"""Keep docs/qtop.1 in step with the command line and the release."""

import re
import subprocess
import sys
from pathlib import Path

from qtop_py import __version__

ROOT = Path(__file__).resolve().parents[1]
MANPAGE = (ROOT / "docs" / "qtop.1").read_text(encoding="utf-8").replace("\\-", "-")


OPTION = r"(?<![\w-])(--?[A-Za-z0-9][\w-]*)"


def manpage_option_entries():
    """Return the options each OPTIONS entry defines: the line after every .TP."""
    lines = MANPAGE.split(".SH OPTIONS", 1)[1].split("\n.SH ", 1)[0].splitlines()
    return {option for previous, line in zip(lines, lines[1:]) if previous.strip() == ".TP" for option in re.findall(OPTION, line)}


def test_manpage_has_an_options_entry_for_every_command_line_option():
    usage = subprocess.run([sys.executable, "-m", "qtop_py.qtop", "--help"], cwd=str(ROOT), stdout=subprocess.PIPE, check=True, universal_newlines=True).stdout
    options = {match for line in usage.splitlines() if line.startswith("  -") for match in re.findall(OPTION, line.split("  ", 2)[1])}
    entries = manpage_option_entries()
    assert len(options) > 60, "--help parsing broke"
    assert sorted(options - entries) == [], "add an OPTIONS entry to docs/qtop.1"
    assert sorted(entries - options) == [], "docs/qtop.1 documents an option qtop no longer has"


def test_manpage_names_the_current_version():
    assert '"qtop %s"' % __version__ in MANPAGE
