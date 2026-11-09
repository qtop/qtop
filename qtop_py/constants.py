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
# user_id_ranking keys: job counts rank higher counts first; the final name key makes the order total.
USER_ID_RANKING_COUNT_KEYS: "Final" = ("R+Q", "R", "Q", "all")
USER_ID_RANKING_NAME_KEYS: "Final" = ("name_desc", "name_asc")
DEFAULT_USER_ID_RANKING: "Final" = ("all", "R", "name_desc")
# Note after the accounts title; {ranking} expands to the active user_id_ranking.
DEFAULT_USER_ACCOUNTS_HEADER: "Final" = "(ranked by {ranking})"
MAX_USER_ACCOUNTS_HEADER_LEN: "Final" = 200
SYMBOL_UNKNOWN_NODE_STATE: "Final" = "?"
