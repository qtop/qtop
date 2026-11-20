## Copyright (c) 2026 qtop contributors
## SPDX-License-Identifier: MIT
"""Scheduler-neutral command collection, capture and snapshot orchestration."""

import concurrent.futures
import copy
import logging
import os
import tarfile
import tempfile
import threading
import time
from typing import TYPE_CHECKING, Any, Callable, Dict, Mapping, Optional, Tuple, Type, cast
from xml.etree.ElementTree import ParseError

from qtop_py import __version__, configuration, fileutils, process, sample
from qtop_py.colormap import user_to_color_default
from qtop_py.configuration import CONFIG_ERRORS
from qtop_py.constants import QTOPCONF_YAML
from qtop_py.errors import OperationalError
from qtop_py.runtime import RunPaths, RunResources
from qtop_py.schedulers.registry import decide_batch_system
from qtop_py.serialiser import GenericBatchSystem
from qtop_py.snapshot import Snapshot

if TYPE_CHECKING:
    from qtop_py import state_types


def finalize_filepaths_schedulercommands(args: Any, config: Dict[str, Any], backend: Type[GenericBatchSystem]) -> Dict[str, Tuple[str, str]]:
    """Return {input_key: (path, command)}, with PID suffixes for live collection."""
    d = dict()
    fn_append = "_" + str(os.getpid()) if not args.SOURCEDIR else ""
    for fn, path_command in config["schedulers"][backend.get_mnemonic()].items():
        path, command = path_command.strip().split(", ", 1)
        path = path % {"savepath": args.workdir, "pid": fn_append}
        # Format codes stay copyable; accept legacy escaped percent signs.
        command = command.replace("%%", "%").replace("%(savepath)s", args.workdir)
        adjusted = backend.collection_command(fn, command, args)
        if adjusted is not None:
            d[fn] = (path, adjusted)
    return d


def _run_commands_concurrently(
    INPUT_FNs_commands: Mapping[str, Tuple[str, str]], config: Dict[str, Any], args: Any, backend: Type[GenericBatchSystem]
) -> Dict[str, Any]:
    """Start every scheduler command at once; return {key: path or OperationalError} in config order.

    A frame then waits for the slowest command, not the sum of all of them. A failed required
    command, or the caller's own cancel event, stops the commands still running.
    """
    SETTINGS = configuration.collection_settings(config)
    _savepath = os.path.realpath(os.path.expandvars(SETTINGS.SAVEPATH))
    filenames = {key: path for key, (path, _) in INPUT_FNs_commands.items()}
    commands = {key: command for key, (_, command) in INPUT_FNs_commands.items()}
    controls = dict(getattr(args, "_collection_controls", {}))
    outer_cancel = controls.pop("cancel", None)
    stop = threading.Event()
    progress = {"input_options": args} if getattr(args, "_input_progress", None) is not None else {}

    def run(key: str) -> str:
        return process.execute_shell_batch_commands(commands, filenames, key, _savepath, timeout=SETTINGS.TIMEOUT, cancel=stop, **progress, **controls)

    first_failure: "Optional[concurrent.futures.Future[str]]" = None
    with concurrent.futures.ThreadPoolExecutor(max_workers=max(1, len(filenames)), thread_name_prefix="qtop-command") as pool:
        futures = {pool.submit(run, key): key for key in filenames}
        pending = set(futures)
        try:
            while pending:
                done, pending = concurrent.futures.wait(pending, timeout=0.1, return_when=concurrent.futures.FIRST_COMPLETED)
                if outer_cancel is not None and outer_cancel.is_set():
                    stop.set()
                for future in done:
                    error = future.exception()
                    required = futures[future] not in backend.optional_trace_keys
                    if error is not None and (required or not isinstance(error, OperationalError)):
                        first_failure = first_failure or future
                        stop.set()
        finally:
            if pending:
                stop.set()
    if first_failure is not None:
        raise cast(BaseException, first_failure.exception())
    results: Dict[str, Any] = {}
    for future, key in futures.items():
        results[key] = future.exception() or future.result()
    return results


def get_input_filenames(INPUT_FNs_commands: Mapping[str, Tuple[str, str]], config: Dict[str, Any], args: Any, backend: Type[GenericBatchSystem]) -> Dict[str, str]:
    """Return the trace files by key: found in the -s directory, or collected by running the scheduler commands."""
    filenames = dict()
    collected = {} if args.SOURCEDIR else _run_commands_concurrently(INPUT_FNs_commands, config, args, backend)
    for _file in INPUT_FNs_commands:
        filenames[_file] = INPUT_FNs_commands[_file][0]

        if not args.SOURCEDIR:
            result = collected[_file]
            if isinstance(result, OperationalError):
                logging.warning("%s %s", result, backend.optional_trace_warning)
                filenames.pop(_file)
                continue
            filenames[_file] = result

        if _file in backend.optional_trace_keys and not os.path.isfile(filenames[_file]):
            filenames.pop(_file)
            continue
        if not os.path.isfile(filenames[_file]):
            raise OperationalError("Scheduler input file %s was not found. Check -s/--SetSourceDir and the selected scheduler." % filenames[_file])
    if args.SOURCEDIR:
        for key, alias in backend.optional_trace_aliases.items():
            detail_path = os.path.join(args.SOURCEDIR, alias)
            if key not in filenames and os.path.isfile(detail_path):
                filenames[key] = detail_path
    return filenames


def fetch_scheduler_files(args: Any, config: Dict[str, Any], backend: Type[GenericBatchSystem]) -> Dict[str, str]:
    """Return the scheduler trace files, collected live or read from -s; raise OperationalError on bad commands."""
    scheduler_name = backend.get_mnemonic()
    try:
        INPUT_FNs_commands = finalize_filepaths_schedulercommands(args, config, backend)
    except (ValueError, TypeError, KeyError, AttributeError) as error:
        raise OperationalError("Invalid scheduler command configuration for %s in %s: %s" % (scheduler_name, QTOPCONF_YAML, error)) from error
    if getattr(args, "source_archive", None):
        from qtop_py.trace_archive import load_trace_archive

        try:
            if not hasattr(args, "archive_trace_paths"):
                args.archive_trace_paths = load_trace_archive(args.source_archive, args.workdir, config["schedulers"][scheduler_name], backend.optional_trace_keys)
            paths: Dict[str, str] = args.archive_trace_paths.copy()
            return paths
        except (OSError, tarfile.TarError, ValueError) as error:
            raise OperationalError("Cannot read scheduler trace archive %s: %s" % (args.source_archive, error)) from error
    return get_input_filenames(INPUT_FNs_commands, config, args, backend)


def MVU_collect_snapshot(args: Any, config: Dict[str, Any], backend: Any, trace_paths: Dict[str, str], archive: Any, collected: Any = None) -> Tuple[Snapshot, Dict[str, Any]]:
    """Collect one normalized snapshot, including replay and masking."""
    scheduler = backend.get_mnemonic()
    savepath = config["savepath"]
    scheduling_system: state_types.Scheduler
    try:
        if args.load_sample:
            cluster_state = args.sample_snapshot["state"]
        elif args.SAMPLE and args.ANONYMIZE:
            # Collect originals only in memory so all backends share one mapping.
            capture_args = copy.copy(args)
            capture_args.ANONYMIZE = False
            scheduling_system = collected.system if collected is not None else backend(trace_paths, config, capture_args)
            cluster_state = collected.state if collected is not None else scheduling_system.get_cluster_state(capture_args)
            cluster_state, mappings = sample.anonymize_state(cluster_state)
            config, omitted = sample.sanitize_config(config, mappings)
            snapshot_config = copy.deepcopy(config)
            config["savepath"] = savepath
            args.GET_GECOS = False
            args.SHOW_GROUPS = False
            if archive is None:
                raise OperationalError("Support archive was not initialized.")
            sample.save_snapshot(archive, cluster_state, snapshot_config, args, scheduler, __version__, omitted)
            if args.masked_traces:
                from qtop_py.source_trace import save_masked_traces

                save_masked_traces(archive, scheduling_system, trace_paths, mappings)
        else:
            scheduling_system = collected.system if collected is not None else backend(trace_paths, config, args)
            cluster_state = collected.state if collected is not None else scheduling_system.get_cluster_state(args)
    except (ValueError, UnicodeError, ParseError) as error:
        raise OperationalError("Cannot parse %s scheduler input in %s: %s" % (scheduler, args.workdir, error)) from error
    if collected is not None and args.ANONYMIZE and not args.SAMPLE:
        cluster_state = sample.anonymize_state(cluster_state)[0]
    snapshot = Snapshot.from_cluster_state(cluster_state)
    display_state = getattr(backend, "display_job_state", None)
    if display_state is not None:
        for job_id, job in snapshot.jobs_dict.items():
            snapshot.jobs_dict[job_id] = job._replace(job_state=display_state(job.job_state))
    return snapshot, config


def make_collector(args: Any, config: Dict[str, Any], backend: Any, scheduler: str) -> Callable[[str, threading.Event], Any]:
    """Build a collector with a single capability probe and fallback decision."""
    capture_args, capture_config = copy.copy(args), copy.deepcopy(config)
    capture_args.ANONYMIZE = False
    selected = backend
    probed = False

    def collect(directory: str, cancel: threading.Event) -> Any:
        nonlocal selected, probed
        collection_args = copy.copy(capture_args)
        collection_args.workdir = directory
        collection_args._collection_controls = {"cancel": cancel, "cwd": os.path.realpath(os.path.expandvars(capture_config["savepath"]))}
        collection_config = dict(capture_config, savepath=directory)
        try:
            files = fetch_scheduler_files(collection_args, collection_config, selected)
            system = selected(files, collection_config, collection_args)
            state = system.get_cluster_state(collection_args)
        except (OperationalError, ValueError) as error:
            fallback = getattr(selected, "fallback_backend", None)
            if probed or fallback is None or not selected.capability_failure(error):
                raise
            logging.warning("%s collection capability unavailable: %s; falling back to %s. %s", scheduler, error, fallback.get_mnemonic(), getattr(selected, "fallback_note", ""))
            selected = fallback
            files = fetch_scheduler_files(collection_args, collection_config, selected)
            system = selected(files, collection_config, collection_args)
            state = system.get_cluster_state(collection_args)
        finally:
            probed = True
        from qtop_py.cluster_state import validate_cluster_state

        state = validate_cluster_state(state)
        return state, system, selected, files if capture_args.SAMPLE else {}

    return collect


def _collect_frame(
    resources: RunResources,
    args: Any,
    config: Dict[str, Any],
    user_to_color: Dict[str, str],
    available_batch_systems: Mapping[str, Any],
    savepath: str,
    paths: RunPaths,
    sample_filename: str,
) -> Tuple[Snapshot, Dict[str, Any], Any, Dict[str, str], str]:
    """Pick the scheduler, start the frame's capture and collect one snapshot.

    Returns (snapshot, config, backend, user_to_color, sample filename).
    """
    scheduler = (
        args.sample_snapshot["scheduler"]
        if args.load_sample
        else decide_batch_system(args.BATCH_SYSTEM, os.environ.get("QTOP_SCHEDULER"), config["scheduler"], config["schedulers"], available_batch_systems, config)
    )
    backend = available_batch_systems[scheduler]
    collected = None
    collection_error = ""
    interval = getattr(backend, "collection_interval", 0)
    if interval and not args.SOURCEDIR and not args.load_sample:
        from qtop_py.schedulers.collection import CollectionPoller

        try:
            commands = finalize_filepaths_schedulercommands(args, config, backend)
        except CONFIG_ERRORS as error:
            raise OperationalError("Invalid scheduler collection configuration: %s" % error) from error
        key = (scheduler, repr(commands), config.get("scheduler_command_timeout"), savepath)
        if resources.poller is None or resources.poller_key != key:
            resources.stop_collection()
            resources.poller = CollectionPoller(make_collector(args, config, backend, scheduler), interval, SAVEPATH=savepath)
            resources.poller_key = key
            if args.WATCH:
                resources.poller.start()
        collected, collection_error = resources.poller.read()
        backend = collected.backend
        trace_paths = {}
        for trace_key, (name, content) in collected.traces.items():
            with tempfile.NamedTemporaryFile(dir=args.workdir, delete=False) as stream:
                stream.write(content)
                temporary = stream.name
            destination = os.path.join(args.workdir, name)
            # Sonar path finding: destination already contained (the trace name is a basename inside workdir); see docs/sonar-review.md
            os.replace(temporary, destination)
            trace_paths[trace_key] = destination
    else:
        resources.stop_collection()
        trace_paths = {} if args.load_sample else fetch_scheduler_files(args, config, backend)
    sample_filename = fileutils.get_sample_filename(sample_filename, config)
    resources.start_capture(savepath, sample_filename, trace_paths, config, paths.qtoppath)
    snapshot, config = MVU_collect_snapshot(args, config, backend, trace_paths, resources.archive, **({"collected": collected} if collected is not None else {}))
    if collected is not None:
        snapshot._collection_age = max(0, time.monotonic() - collected.completed)
        snapshot._collection_span = collected.completed - collected.started
        snapshot._collection_error = collection_error
    if args.SAMPLE and args.ANONYMIZE:
        user_to_color = user_to_color_default.copy()
        for rule in config["user_color_mappings"]:
            user_to_color.update(rule)
    return snapshot, config, backend, user_to_color, sample_filename
