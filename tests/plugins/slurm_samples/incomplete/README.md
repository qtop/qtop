# Synthetic incomplete Slurm snapshot

These invented names and jobs contain no production data. The pair exercises
comma-separated host groups, a malformed host list, nodes absent from sinfo,
unknown states, truncated rows, missing identities and invalid CPU counts.

Expected: six usable jobs (four running, one pending, one unknown), three
nodes, twelve available cores and five assigned cores. Jobs 704 and 705 remain
in job totals but have no usable node assignment. Unknown states remain in
job totals without being classified as running or queued.

SPDX-License-Identifier: MIT
