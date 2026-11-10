## Copyright (c) 2026 qtop contributors
## SPDX-License-Identifier: MIT
"""Exceptions shared across qtop's modules."""


class OperationalError(Exception):
    """An expected failure that the CLI can explain without a traceback."""


class JobNotFound(Exception):
    """A job state that the scheduler mapping does not know."""

    def __init__(self, job_state: str) -> None:
        Exception.__init__(self, "Job state %s not found" % job_state)
        self.job_state = job_state
