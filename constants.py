import os

TMPDIR = '/tmp'
SYSTEMCONFDIR = '/etc'
QTOPCONF_YAML = 'qtopconf.yaml'
QTOP_LOGFILE = '$HOME/.local/qtop/logs/qtop.log'
QTOP_SAMPLE_FILENAME = 'qtop_sample_$USER_$DATE_$TIME.tar'
# QTOP_LOGFILE = '$HOME/.local/qtop/logs/qtop_%s.log'  % os.getpid()
QTOP_LOGFILE = os.path.expandvars(QTOP_LOGFILE)
QTOP_SAMPLE_FILENAME = os.path.expandvars(QTOP_SAMPLE_FILENAME)
savepath = os.path.expandvars(os.path.join(TMPDIR, 'qtop_results_$USER'))
USERPATH = os.path.expandvars('$HOME/.local/qtop')
MAX_CORE_ALLOWED = 150000  # not used anywhere
MAX_UNIX_ACCOUNTS = 87  # was : 62
