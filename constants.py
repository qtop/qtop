import os
import datetime

TMPDIR = '/tmp'
SYSTEMCONFDIR = '/etc'
QTOPCONF_YAML = 'qtopconf.yaml'
QTOP_LOGFILE = '$HOME/.local/qtop/logs/qtop.log'
# QTOP_LOGFILE = '$HOME/.local/qtop/logs/qtop_%s.log'  % os.getpid()
QTOP_LOGFILE = os.path.expandvars(QTOP_LOGFILE)
QTOP_SAMPLE_FILENAME = 'qtop_sample_${USER}_%(datetime)s.tar' % {'datetime': datetime.datetime.now().strftime("%Y%m%d-%H%M%S")}
QTOP_SAMPLE_FILENAME = os.path.expandvars(QTOP_SAMPLE_FILENAME)
savepath = os.path.expandvars(os.path.join(TMPDIR, 'qtop_results_$USER'))
USERPATH = os.path.expandvars('$HOME/.local/qtop')
MAX_CORE_ALLOWED = 150000  # not used anywhere
MAX_UNIX_ACCOUNTS = 87  # was : 62
KEYPRESS_TIMEOUT = 2  # in sec, time to wait before autorefreshing display
FALLBACK_TERMSIZE = [53, 176]
