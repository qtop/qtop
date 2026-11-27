## Copyright (c) 2026 qtop contributors
## SPDX-License-Identifier: MIT
"""Scheduler-neutral command collection, capture and snapshot orchestration."""

import copy
import logging
import os
import tarfile
import tempfile
import threading
import time
from typing import TYPE_CHECKING, Any, Callable, Dict, Mapping, Tuple, Type
from xml.etree.ElementTree import ParseError  # nosec B405 # nosemgrep - exception class only; parsing is in plugins/sge.py

from qtop_py import __version__, configuration, core_api, fileutils
from qtop_py import core_sample as sample
from qtop_py.colormap import user_to_color_default
from qtop_py.configuration import CONFIG_ERRORS
from qtop_py.constants import QTOPCONF_YAML
from qtop_py.core_api import GenericBatchSystem, OperationalError
from qtop_py.runtime import RunPaths, RunResources
from qtop_py.schedulers.registry import decide_batch_system
from qtop_py.snapshot import Snapshot

if TYPE_CHECKING:
    from qtop_py import core_state_types as state_types

logger = logging.getLogger(__name__)


def finalize_filepaths_schedulercommands(args: Any, config: Dict[str, Any], backend: Type[GenericBatchSystem]) -> Dict[str, Tuple[str, str]]:
    """Return {input_key: (path, command)} for the configured commands, with PID suffixes for live collection."""
    return core_api.command_lines(backend, config["schedulers"][backend.get_mnemonic()], args.workdir, args)


def command_env(backend: Type[GenericBatchSystem]) -> Dict[str, str]:
    """Return the environment for a backend's commands: qtop's own, with what the backend requires."""
    return core_api.command_env(backend, os.environ)


def _run_commands_concurrently(INPUT_FNs_commands: Mapping[str, Tuple[str, str]], config: Dict[str, Any], args: Any, backend: Type[GenericBatchSystem]) -> Dict[str, Any]:
    """Run the commands with the configured savepath and timeout; see core_api.run_commands."""
    SETTINGS = configuration.collection_settings(config)
    controls = dict(getattr(args, "_collection_controls", {}))
    cancel = controls.pop("cancel", None)
    progress = {"input_options": args} if getattr(args, "_input_progress", None) is not None else {}
    return core_api.run_commands(
        dict(INPUT_FNs_commands),
        backend,
        os.path.expandvars(SETTINGS.SAVEPATH),
        os.environ,
        timeout=SETTINGS.TIMEOUT,
        cancel=cancel,
        config_name=QTOPCONF_YAML,
        **progress,
        **controls,
    )


def get_input_filenames(INPUT_FNs_commands: Mapping[str, Tuple[str, str]], config: Dict[str, Any], args: Any, backend: Type[GenericBatchSystem]) -> Dict[str, str]:
    """Return the trace files by key: found in the -s directory, or collected by running the scheduler commands."""
    collected = None if args.SOURCEDIR else _run_commands_concurrently(INPUT_FNs_commands, config, args, backend)
    return core_api.trace_files(dict(INPUT_FNs_commands), backend, args, collected)


def fetch_scheduler_files(args: Any, config: Dict[str, Any], backend: Type[GenericBatchSystem]) -> Dict[str, str]:
    """Return the scheduler trace files, collected live or read from -s; raise OperationalError on bad commands."""
    scheduler_name = backend.get_mnemonic()
    entries = config["schedulers"].get(scheduler_name)
    if not isinstance(entries, Mapping):
        raise OperationalError("Invalid scheduler command configuration for %s in %s: expected a mapping of trace keys to commands" % (scheduler_name, QTOPCONF_YAML))
    try:
        INPUT_FNs_commands = finalize_filepaths_schedulercommands(args, config, backend)
    except ValueError as error:
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


def MVU_collect_snapshot(
    args: Any, config: Dict[str, Any], backend: Any, trace_paths: Dict[str, str], archive: Any, collected: Any = None, collection_error: str = ""
) -> Tuple[Snapshot, Dict[str, Any]]:
    """Collect one normalized snapshot, including replay and masking; the snapshot is complete when built."""
    scheduler = backend.get_mnemonic()
    savepath = config["savepath"]
    scheduling_system: state_types.Scheduler
    built = None  # the core_api collection (the poller's, or built here from trace_paths)
    try:
        if args.load_sample:
            cluster_state = args.sample_snapshot["state"]
        elif args.SAMPLE and args.ANONYMIZE:
            # Mask the built state once, so the archive and the display share one mapping.
            built = collected if collected is not None else core_api.collect(backend, args.workdir, args, config=config, files=trace_paths)
            scheduling_system, cluster_state = built.system, built.state
            cluster_state, mappings = sample.anonymize_state(cluster_state)
            config, omitted = sample.sanitize_config(config, mappings)
            snapshot_config = copy.deepcopy(config)
            config["savepath"] = savepath
            if archive is None:
                raise OperationalError("Support archive was not initialized.")
            sample.save_snapshot(archive, cluster_state, snapshot_config, args, scheduler, __version__, omitted)
            if args.masked_traces:
                from qtop_py.core_source_trace import save_masked_traces

                save_masked_traces(archive, scheduling_system, trace_paths, mappings)
        else:
            built = collected if collected is not None else core_api.collect(backend, args.workdir, args, config=config, files=trace_paths)
            cluster_state = built.state
    except (ValueError, UnicodeError, ParseError) as error:
        raise OperationalError("Cannot parse %s scheduler input in %s: %s" % (scheduler, args.workdir, error)) from error
    if getattr(args, "ANONYMIZE", False) and not args.SAMPLE and not args.load_sample:  # -A: masked once, after the state is built
        cluster_state = sample.anonymize_state(cluster_state)[0]
    # core_api.collect() already validated the state (unless -A replaced it); --strict validates it again here.
    prevalidated = built is not None and cluster_state is built.state and not getattr(args, "STRICT", False)
    collection = None if collected is None else (max(0, time.monotonic() - collected.completed), collected.completed - collected.started, collection_error)
    snapshot = Snapshot.from_cluster_state(cluster_state, validated=prevalidated, display_state=getattr(backend, "display_job_state", None), collection=collection)
    return snapshot, config


def make_collector(args: Any, config: Dict[str, Any], backend: Any, scheduler: str) -> Callable[[str, threading.Event], Any]:
    """Build a collector with a single capability probe and fallback decision."""
    capture_args, capture_config = copy.copy(args), copy.deepcopy(config)
    selected = backend
    probed = False

    def collect(directory: str, cancel: threading.Event) -> Any:
        nonlocal selected, probed
        collection_args = copy.copy(capture_args)
        collection_args.workdir = directory
        collection_args._collection_controls = {"cancel": cancel, "cwd": os.path.realpath(os.path.expandvars(capture_config["savepath"]))}
        collection_config = dict(capture_config, savepath=directory)

        def attempt() -> Any:
            files = fetch_scheduler_files(collection_args, collection_config, selected)
            return core_api.collect(selected, directory, collection_args, config=collection_config, files=files)

        try:
            built = attempt()
        except (OperationalError, ValueError) as error:
            fallback = getattr(selected, "fallback_backend", None)
            if probed or fallback is None or not selected.capability_failure(error):
                raise
            logger.warning("%s collection capability unavailable: %s; falling back to %s. %s", scheduler, error, fallback.get_mnemonic(), getattr(selected, "fallback_note", ""))
            selected = fallback
            built = attempt()
        finally:
            probed = True
        state, system, files = built.state, built.system, built.files
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
    snapshot, config = MVU_collect_snapshot(args, config, backend, trace_paths, resources.archive, collected=collected, collection_error=collection_error)
    if args.SAMPLE and args.ANONYMIZE:
        user_to_color = user_to_color_default.copy()
        for rule in config["user_color_mappings"]:
            user_to_color.update(rule)
    return snapshot, config, backend, user_to_color, sample_filename
