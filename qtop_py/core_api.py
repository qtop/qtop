## Copyright (c) 2026 qtop contributors
## SPDX-License-Identifier: MIT
"""The public interface of qtop's scheduler core, for backends and for library callers.

A backend subclasses GenericBatchSystem (and usually StatExtractor), reads its traces with
open_input/check_empty_file, and returns a ClusterState from get_cluster_state(). It never sees
the CLI: options carry only what collection needs (BackendOptions shows the fields), and -A
masking happens once, after the state is built.

Library callers collect with collect_state() (just the state) or collect() (the state, the
backend object and the trace files): commands (or a source directory), parsing and validation,
with no command line, configuration file or terminal. The steps they compose are public too, for
callers that keep traces apart from parsing: command_lines, command_env, run_commands and
trace_files. qtop itself is such a caller: qtop_py/schedulers/collector.py takes everything listed
here from this module, and tests/test_core_api.py keeps it so.

The core logs to qtop_py.* loggers and never configures logging itself. Everything else in
qtop_py is internal.

The types (ClusterState, Node, Options, ...) exist for type checkers only, as qtop supports
Python 3.6: import them from qtop_py.core_state_types under ``if TYPE_CHECKING:``.
tests/test_core_api.py pins this surface.
"""

from qtop_py.core_cluster_state import build_cluster_state, validate_cluster_state
from qtop_py.core_collect import BackendOptions, Collection, collect, collect_state, command_env, command_lines, run_commands, trace_files
from qtop_py.core_errors import OperationalError
from qtop_py.core_inputs import FileEmptyError, check_empty_file, open_input
from qtop_py.core_serialiser import GenericBatchSystem, StatExtractor, resolve_job_id
from qtop_py.core_source_trace import TraceMasker

__all__ = [
    "BackendOptions",
    "Collection",
    "FileEmptyError",
    "GenericBatchSystem",
    "OperationalError",
    "StatExtractor",
    "TraceMasker",
    "build_cluster_state",
    "check_empty_file",
    "collect",
    "collect_state",
    "command_env",
    "command_lines",
    "open_input",
    "resolve_job_id",
    "run_commands",
    "trace_files",
    "validate_cluster_state",
]
