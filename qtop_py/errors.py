## Copyright (c) 2026 qtop contributors
## SPDX-License-Identifier: MIT
"""Exceptions shared across qtop's modules."""

import logging
import os

from qtop_py.constants import QTOP_LOGFILE, QTOPCONF_YAML
from qtop_py.fileutils import active_log_path


class OperationalError(Exception):
    """An expected failure that the CLI can explain without a traceback."""


class JobNotFound(Exception):
    """A job state that the scheduler mapping does not know."""

    def __init__(self, job_state: str) -> None:
        Exception.__init__(self, "Job state %s not found" % job_state)
        self.job_state = job_state


class NoSchedulerFound(Exception):
    """No scheduler was given and none could be detected."""

    def __init__(self) -> None:
        msg = "No suitable scheduler was found. Please define one in a switch or env variable or in %s.\nFor more help, try ./qtop.py --help\nLog file created in %s" % (
            QTOPCONF_YAML,
            active_log_path() or os.path.expandvars(QTOP_LOGFILE),
        )
        Exception.__init__(self, msg)
        logging.critical(msg)


class SchedulerNotSpecified(Exception):
    """Auto-detection found no scheduler."""


class InvalidScheduler(Exception):
    """The requested scheduler is not one qtop supports."""
