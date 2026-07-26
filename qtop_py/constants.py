import os

SYSTEMCONFDIR = "/etc"
QTOPCONF_YAML = "qtopconf.yaml"
QTOP_LOGFILE = "$HOME/.local/qtop/logs/qtop.log"
# QTOP_LOGFILE = '$HOME/.local/qtop/logs/qtop_%s.log'  % os.getpid()
QTOP_LOGFILE = os.path.expandvars(QTOP_LOGFILE)
USERPATH = os.path.expandvars("$HOME/.local/qtop")
MAX_UNIX_ACCOUNTS = 87  # was : 62
KEYPRESS_TIMEOUT = 2  # in sec, time to wait before autorefreshing display
FALLBACK_TERMSIZE = [53, 176]
SYMBOL_LONG_TAIL_USER = "*"
SYMBOL_UNKNOWN_NODE_STATE = "?"
