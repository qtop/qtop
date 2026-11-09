## Copyright (c) 2026 qtop contributors
## SPDX-License-Identifier: MIT


class OperationalError(Exception):
    """An expected failure that the CLI can explain without a traceback."""


class JobNotFound(Exception):
    def __init__(self, job_state: str) -> None:
        Exception.__init__(self, "Job state %s not found" % job_state)
        self.job_state = job_state
