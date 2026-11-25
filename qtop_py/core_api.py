## Copyright (c) 2026 qtop contributors
## SPDX-License-Identifier: MIT
"""The public interface for scheduler backends: what a backend may import from qtop.

A backend subclasses GenericBatchSystem (and usually StatExtractor), reads its traces with
open_input/check_empty_file, and returns a ClusterState from get_cluster_state(). It never sees
the CLI: options carry only what collection needs (BackendOptions shows the fields), and -A
masking happens once, after the state is built. collect_state() runs a backend end to end:
commands (or a source directory), parsing and validation. The core logs to qtop_py.* loggers and never
configures logging itself. Everything else in qtop_py is internal.

The types (ClusterState, Node, Options, ...) exist for type checkers only, as qtop supports
Python 3.6: import them from qtop_py.core_state_types under ``if TYPE_CHECKING:``.
tests/test_core_api.py pins this surface.
"""

from qtop_py.core_cluster_state import build_cluster_state, validate_cluster_state
from qtop_py.core_collect import BackendOptions, collect_state
from qtop_py.core_errors import OperationalError
from qtop_py.core_inputs import FileEmptyError, check_empty_file, open_input
from qtop_py.core_serialiser import GenericBatchSystem, StatExtractor, resolve_job_id
from qtop_py.core_source_trace import TraceMasker

__all__ = [
    "BackendOptions",
    "FileEmptyError",
    "GenericBatchSystem",
    "OperationalError",
    "StatExtractor",
    "TraceMasker",
    "build_cluster_state",
    "check_empty_file",
    "collect_state",
    "open_input",
    "resolve_job_id",
    "validate_cluster_state",
]
