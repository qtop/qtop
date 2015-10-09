import os

QTOPCONF_YAML = 'qtopconf.yaml'
QTOP_LOGFILE = '$HOME/.local/qtop/qtop.log'
USERPATH = os.path.expandvars('$HOME/.local/qtop')
MAX_CORE_ALLOWED = 150000  # not used anywhere
MAX_UNIX_ACCOUNTS = 87  # was : 62