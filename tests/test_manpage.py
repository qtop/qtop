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


def test_manpage_documents_every_command_line_option():
    usage = subprocess.run([sys.executable, "-m", "qtop_py.qtop", "--help"], cwd=str(ROOT), stdout=subprocess.PIPE, check=True, universal_newlines=True).stdout
    options = set(re.findall(r"(?<![\w-])(--?[A-Za-z0-9][\w-]*)", usage))
    missing = sorted(option for option in options if not re.search(r"(?<![\w-])%s(?![\w-])" % re.escape(option), MANPAGE))
    assert not missing


def test_manpage_names_the_current_version():
    assert '"qtop %s"' % __version__ in MANPAGE
