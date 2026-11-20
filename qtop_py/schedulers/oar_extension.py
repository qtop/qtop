## Copyright (c) 2026 qtop contributors
## SPDX-License-Identifier: MIT
"""OAR's extension fields (oar_*) in a cluster state: owned by the oar plugin."""

from qtop_py.schedulers.extensions import ExtensionSpec

OAR_EXTENSION = ExtensionSpec(
    "OAR",
    diagnostics=("oar_diagnostics", ("missing_resources", "extra_resources", "unlisted_jobs", "shared_resources")),
    node_job_fields=("oar_job_ids",),
)
