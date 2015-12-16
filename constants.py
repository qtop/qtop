import os

QTOPCONF_YAML = 'qtopconf.yaml'
QTOP_LOGFILE = '$HOME/.local/qtop/logs/qtop.log'
QTOP_TARFN = 'qtop_tar_$USER.tar.gz'
# QTOP_LOGFILE = '$HOME/.local/qtop/logs/qtop_%s.log'  % os.getpid()
QTOP_LOGFILE = os.path.expandvars(QTOP_LOGFILE)
QTOP_TARFN = os.path.expandvars(QTOP_TARFN)
USERPATH = os.path.expandvars('$HOME/.local/qtop')
SYSTEMCONFDIR = '/etc'
MAX_CORE_ALLOWED = 150000  # not used anywhere
MAX_UNIX_ACCOUNTS = 87  # was : 62
