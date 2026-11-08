## Copyright (c) 2026 qtop contributors
## SPDX-License-Identifier: MIT
import os
from typing import TYPE_CHECKING

if TYPE_CHECKING:  # typing.Final is Python 3.8+; the quoted annotations keep 3.6 working
    from typing import Final

SYSTEMCONFDIR: "Final" = "/etc"
QTOPCONF_YAML: "Final" = "qtopconf.yaml"
# QTOP_LOGFILE = '$HOME/.local/qtop/logs/qtop_%s.log'  % os.getpid()
QTOP_LOGFILE: "Final" = os.path.expandvars("$HOME/.local/qtop/logs/qtop.log")
USERPATH: "Final" = os.path.expandvars("$HOME/.local/qtop")
MAX_UNIX_ACCOUNTS: "Final" = 87  # was : 62
KEYPRESS_TIMEOUT: "Final" = 2  # in sec, time to wait before autorefreshing display
FALLBACK_TERMSIZE: "Final" = [53, 176]
SYMBOL_LONG_TAIL_USER: "Final" = "*"
SYMBOL_UNKNOWN_NODE_STATE: "Final" = "?"
