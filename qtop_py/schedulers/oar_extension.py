## Copyright (c) 2026 qtop contributors
## SPDX-License-Identifier: MIT
"""OAR's extension fields (oar_*) in a cluster state: owned by the oar plugin."""

from typing import TYPE_CHECKING

from qtop_py.schedulers.extensions import ExtensionSpec

if TYPE_CHECKING:  # typing.Final is Python 3.8+; the quoted annotations keep 3.6 working
    from typing import Final

OAR_EXTENSION: "Final" = ExtensionSpec(
    "OAR",
    diagnostics=("oar_diagnostics", ("missing_resources", "extra_resources", "unlisted_jobs", "shared_resources")),
    node_job_fields=("oar_job_ids",),
)
