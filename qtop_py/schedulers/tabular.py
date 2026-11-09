## Copyright (c) 2026 qtop contributors
## SPDX-License-Identifier: MIT
"""Shared legacy tabular input for PBS and OAR backends."""

import logging
import re
from typing import Dict, Sequence

from qtop_py.serialiser import StatExtractor


class TabularStatExtractor(StatExtractor):
    """Parse whitespace-separated qstat-like tables."""

    def _process_qstat_line(self, pattern: str, line: str, re_match_positions: Sequence[str]) -> Dict[str, str]:
        """Return the named fields of a qstat-like line matched by pattern."""
        m = re.search(pattern, line.strip())

        try:
            if m is None:
                raise AttributeError("qstat row did not match")
            job_id, user, job_state, queue = [m.group(x) for x in re_match_positions]
        except AttributeError:
            logging.warning("Line: %s not properly parsed by regex expression. Assuming alternative qstat format." % line.strip())
            raise
        job_id = job_id.split(".")[0]
        user = self.anonymize(user, "users")
        return {"JobId": job_id, "UnixAccount": user, "S": job_state, "Queue": queue}
