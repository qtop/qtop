import logging
import sys
from sys import stdin, stdout
from os.path import expandvars
from constants import *
import utils


def handle_exception(exc_type, exc_value, exc_traceback):
    """
    This, when replacing sys.excepthook,
    will log uncaught exceptions to the logging module instead
    of printing them to stdout.
    """
    if issubclass(exc_type, KeyboardInterrupt):
        sys.__excepthook__(exc_type, exc_value, exc_traceback)
        return

    logging.error("Uncaught exception", exc_info=(exc_type, exc_value, exc_traceback))


class JobNotFound(Exception):
    def __init__(self, job_state):
        Exception.__init__(self, "Job state %s not found" % job_state)
        self.job_state = job_state


class NoSchedulerFound(Exception):
    def __init__(self):
        msg = 'No suitable scheduler was found. ' \
              'Please define one in a switch or env variable or in %s.\n' \
              'For more help, try ./qtop.py --help\nLog file created in %s' % (QTOPCONF_YAML, expandvars(QTOP_LOGFILE))
        Exception.__init__(self, msg)
        logging.critical(msg)


class SchedulerNotSpecified(Exception):
    pass


class InvalidScheduler(Exception):
    pass



