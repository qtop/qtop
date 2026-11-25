## Copyright (c) 2026 qtop contributors
## SPDX-License-Identifier: MIT
"""Collect one validated cluster state from a backend: the scheduler core's entry point.

collect_state() runs a backend's commands concurrently (or reads the traces of a source
directory), builds the backend and returns its validated ClusterState. It takes everything as
arguments; qtop's collector adapts its command line and configuration to these calls.
"""

import concurrent.futures
import logging
import os
import threading
from typing import TYPE_CHECKING, Any, Dict, Mapping, Optional, Tuple, Type

from qtop_py import core_process as process
from qtop_py.core_cluster_state import validate_cluster_state
from qtop_py.core_errors import OperationalError
from qtop_py.core_process import DEFAULT_SCHEDULER_COMMAND_TIMEOUT
from qtop_py.core_serialiser import GenericBatchSystem

if TYPE_CHECKING:
    from qtop_py.core_state_types import ClusterState

logger = logging.getLogger(__name__)

Commands = Dict[str, Tuple[str, str]]  # trace key -> (output file, command)


class BackendOptions(object):
    """The run options a backend may read, for callers without qtop's command line.

    SOURCEDIR: read traces from this directory instead of running commands. Any other keyword
    becomes an attribute: a backend's own switches, with the names its add_cli_options() gives
    them (e.g. Slurm's ALL_PARTITIONS, SLURM_SUMMARY, SLURM_JSON_SQUEUE). Backends read those
    with getattr and a default, so an option left out means the default.
    """

    def __init__(self, SOURCEDIR: Optional[str] = None, **backend_options: Any) -> None:
        self.SOURCEDIR = SOURCEDIR
        for name, value in backend_options.items():
            setattr(self, name, value)


def command_lines(backend: Type[GenericBatchSystem], entries: Mapping[str, str], workdir: str, options: Any) -> Commands:
    """Return {trace key: (output file, command)} from "output file, command" entries.

    Live collection adds the process ID to each file name; with options.SOURCEDIR the names are
    the plain trace names. The backend may adjust or drop a command (collection_command).
    """
    pid = "" if options.SOURCEDIR else "_" + str(os.getpid())
    lines = {}
    for key, entry in entries.items():
        path, command = entry.strip().split(", ", 1)
        path = path % {"savepath": workdir, "pid": pid}
        # Format codes stay copyable; accept legacy escaped percent signs.
        command = command.replace("%%", "%").replace("%(savepath)s", workdir)
        adjusted = backend.collection_command(key, command, options)
        if adjusted is not None:
            lines[key] = (path, adjusted)
    return lines


def command_env(backend: Type[GenericBatchSystem], base: Mapping[str, str]) -> Dict[str, str]:
    """Return the environment for a backend's commands: base (the caller's, e.g. os.environ) with what the backend requires."""
    return {**base, **backend.command_env}


def run_commands(
    commands: Commands,
    backend: Type[GenericBatchSystem],
    savepath: str,
    env: Mapping[str, str],
    timeout: float = DEFAULT_SCHEDULER_COMMAND_TIMEOUT,
    cancel: Optional[threading.Event] = None,
    config_name: str = "the scheduler command configuration",
    **runner: Any,
) -> Dict[str, Any]:
    """Start every command at once; return {key: output path or OperationalError}, in command order.

    savepath holds the temporary output; env is the caller's environment (e.g. os.environ). A
    collection then waits for the slowest command, not the sum of all of them. Each keeps its own
    timeout; the first required failure in command order is raised, as if they had run one after
    another. Setting cancel stops them all. runner passes cwd= and input_options= through.
    """
    savepath = os.path.realpath(savepath)
    filenames = {key: path for key, (path, _) in commands.items()}
    command_text = {key: command for key, (_, command) in commands.items()}
    stop = threading.Event()
    environment = command_env(backend, env)

    def run(key: str) -> str:
        return process.execute_shell_batch_commands(command_text, filenames, key, savepath, timeout=timeout, cancel=stop, env=environment, config_name=config_name, **runner)

    with concurrent.futures.ThreadPoolExecutor(max_workers=max(1, len(filenames)), thread_name_prefix="qtop-command") as pool:
        futures = {key: pool.submit(run, key) for key in filenames}
        pending = set(futures.values())
        try:
            while pending:
                _, pending = concurrent.futures.wait(pending, timeout=0.1)
                if cancel is not None and cancel.is_set():
                    stop.set()
        finally:
            if pending:  # interrupted (e.g. Ctrl-C): stop the commands still running
                stop.set()
    results: Dict[str, Any] = {}
    for key, future in futures.items():
        error = future.exception()
        if error is not None and (key not in backend.optional_trace_keys or not isinstance(error, OperationalError)):
            raise error
        results[key] = error if error is not None else future.result()
    return results


def trace_files(commands: Commands, backend: Type[GenericBatchSystem], options: Any, collected: Optional[Mapping[str, Any]] = None) -> Dict[str, str]:
    """Return the trace files by key: those run_commands() produced, or those of options.SOURCEDIR.

    A failed or missing optional trace is dropped with a warning; a missing required one raises.
    """
    files = {}
    for key, (path, _) in commands.items():
        if collected is not None:
            result = collected[key]
            if isinstance(result, OperationalError):
                logger.warning("%s %s", result, backend.optional_trace_warning)
                continue
            path = result
        if key in backend.optional_trace_keys and not os.path.isfile(path):
            continue
        if not os.path.isfile(path):
            raise OperationalError("Scheduler input file %s was not found. Check -s/--SetSourceDir and the selected scheduler." % path)
        files[key] = path
    if options.SOURCEDIR:
        for key, alias in backend.optional_trace_aliases.items():
            detail_path = os.path.join(options.SOURCEDIR, alias)
            if key not in files and os.path.isfile(detail_path):
                files[key] = detail_path
    return files


def collect_state(
    backend: Type[GenericBatchSystem],
    workdir: str,
    options: Optional[Any] = None,
    commands: Optional[Mapping[str, str]] = None,
    config: Optional[Mapping[str, Any]] = None,
    timeout: float = DEFAULT_SCHEDULER_COMMAND_TIMEOUT,
    cancel: Optional[threading.Event] = None,
    env: Optional[Mapping[str, str]] = None,
) -> "ClusterState":
    """Collect, build and validate one cluster state with backend.

    workdir receives the traces (it must exist). commands are "output file, command" entries by
    trace key, by default the backend's default_commands; with options.SOURCEDIR (see
    BackendOptions) the traces are read from that directory instead of running anything. To run
    commands, pass env, the environment they start from (e.g. os.environ). config is what the
    backend reads (state_abbreviations, its own keys); timeout limits each command.
    """
    options = options if options is not None else BackendOptions()
    lines = command_lines(backend, backend.default_commands if commands is None else commands, workdir, options)
    if not options.SOURCEDIR and env is None:
        raise ValueError("collect_state needs env= (e.g. os.environ) to run the scheduler commands")
    collected = None if options.SOURCEDIR else run_commands(lines, backend, workdir, env or {}, timeout=timeout, cancel=cancel)
    system = backend(trace_files(lines, backend, options, collected), dict(config or {}), options)
    return validate_cluster_state(system.get_cluster_state(options))
