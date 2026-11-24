## Copyright (c) 2026 qtop contributors
## SPDX-License-Identifier: MIT
"""Exceptions shared across qtop's modules."""

from typing import Optional, Sequence


class OperationalError(Exception):
    """An expected failure that the CLI can explain without a traceback."""


class JobNotFound(Exception):
    """A job state that the scheduler mapping does not know."""

    def __init__(self, job_state: str) -> None:
        Exception.__init__(self, "Job state %s not found" % job_state)
        self.job_state = job_state


class NoSchedulerFound(Exception):
    """No scheduler was given, or the one given is not configured; the CLI explains how to pick one."""

    def __init__(self, requested: Optional[str] = None) -> None:
        Exception.__init__(self, "No suitable scheduler was found" + (": %s" % requested if requested else ""))
        self.requested = requested


class SchedulerNotSpecified(Exception):
    """Auto-detection found no scheduler."""


class InvalidScheduler(Exception):
    """The requested scheduler is not one qtop supports."""

    def __init__(self, requested: str = "", choices: Sequence[str] = ()) -> None:
        Exception.__init__(self, "Selected scheduler system not supported: %s" % requested)
        self.requested = requested
        self.choices = tuple(choices)
