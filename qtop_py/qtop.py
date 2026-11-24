#!/usr/bin/env python3

##################################################
##                     qtop                     ##
##################################################

##
## qtop is a tool to monitor queuing systems - https://github.com/qtop/qtop
##
## Copyright (c) 2012 Fotis Georgatos
## Copyright (c) 2012 Sotiris Fragkiskos
## Copyright (c) 2023 Hewlett Packard Enterprise Development LP
## Copyright (c) 2026 Jacob Hatchett
##
## SPDX-License-Identifier: MIT
##
"""qtop's command line and watch loop, plus compatibility re-exports of moved helpers."""

import importlib.util
import logging
import os
import subprocess  # nosec B404 - fixed /bin/cat and /usr/bin/clear argv lists, never a shell
import sys
import tarfile
import time
import traceback
from typing import TYPE_CHECKING, Any, Dict, Iterator, Mapping, Optional, Set, Tuple, Type

from qtop_py import __version__, configuration, fileutils, utils
from qtop_py import core_process as _process
from qtop_py import core_sample as sample
from qtop_py import input_parsing as _input_parsing
from qtop_py import user_details as _user_details
from qtop_py.colormap import queue_to_color as colormap_queue_to_color
from qtop_py.configuration import CONFIG_ERRORS as CONFIG_ERRORS
from qtop_py.constants import (
    FALLBACK_TERMSIZE,
    KEYPRESS_TIMEOUT,
    QTOP_LOGFILE,
    QTOPCONF_YAML,
    SYSTEMCONFDIR,
    USERPATH,
)
from qtop_py.core_errors import InvalidScheduler as InvalidScheduler
from qtop_py.core_errors import NoSchedulerFound as NoSchedulerFound
from qtop_py.core_errors import OperationalError
from qtop_py.core_errors import SchedulerNotSpecified as SchedulerNotSpecified
from qtop_py.core_safety import visible_text
from qtop_py.core_serialiser import GenericBatchSystem
from qtop_py.mvu import (
    MVU_colorize_nodestate,
    MVU_compress_colored_line,
    MVU_gauge_core_vectors,
    MVU_keep_queue_initials_only_and_colorize,
    MVU_Model,
    MVU_update,
)
from qtop_py.mvu_query import MVU_query
from qtop_py.runtime import MVU_export_snapshot as MVU_export_snapshot
from qtop_py.runtime import RunPaths as RunPaths
from qtop_py.runtime import RunResources
from qtop_py.runtime import get_output_size as get_output_size
from qtop_py.runtime import init_dirs as init_dirs
from qtop_py.runtime import pick_frames_to_replay as pick_frames_to_replay
from qtop_py.schedulers import collector as _collector
from qtop_py.schedulers import registry as _registry
from qtop_py.schedulers.collector import MVU_collect_snapshot as MVU_collect_snapshot
from qtop_py.schedulers.collector import _collect_frame as _collect_frame
from qtop_py.schedulers.registry import PLUGIN_BATCH_SYSTEMS
from qtop_py.schedulers.registry import discover_qtop_batch_systems as discover_qtop_batch_systems
from qtop_py.ui import effects as ui_effects
from qtop_py.ui import terminal as terminal_io
from qtop_py.ui.controls import KeyboardInput
from qtop_py.ui.effects import MVU_effects as MVU_effects
from qtop_py.ui.mvu_view import MVU_view
from qtop_py.ui.viewport import Viewport
from qtop_py.user_details import MVU_resolve_user_details as MVU_resolve_user_details
from qtop_py.web import Web

if TYPE_CHECKING:
    from typing import Final

colorize_nodestate = MVU_colorize_nodestate

queue_to_color = colormap_queue_to_color  # re-exported; tools/validate_trace_export.py reads qtop.queue_to_color

compress_colored_line = MVU_compress_colored_line

gauge_core_vectors = MVU_gauge_core_vectors

keep_queue_initials_only_and_colorize = MVU_keep_queue_initials_only_and_colorize


# Explicit compatibility bindings preserve old imports after implementation moves.
extract_regex_detail = _input_parsing.extract_regex_detail
get_date_obj_from_str = _input_parsing.get_date_obj_from_str
execute_shell_batch_commands = _process.execute_shell_batch_commands
scheduler_env = _process.scheduler_env
fetch_scheduler_files = _collector.fetch_scheduler_files
finalize_filepaths_schedulercommands = _collector.finalize_filepaths_schedulercommands
get_input_filenames = _collector.get_input_filenames
auto_get_avail_batch_system = _registry.auto_get_avail_batch_system
decide_batch_system = _registry.decide_batch_system
navigate_prompt = ui_effects.navigate_prompt
write_notice = ui_effects.write_notice
SIGPIPE: "Final" = terminal_io.SIGPIPE
SIGWINCH: "Final" = terminal_io.SIGWINCH
TerminalResized = terminal_io.TerminalResized
calculate_term_size = terminal_io.calculate_term_size
menu_terminal = terminal_io.menu_terminal
raw_mode = terminal_io.raw_mode
reset_sigpipe = terminal_io.reset_sigpipe
termios = terminal_io.termios
wait_for_keypress_or_autorefresh = terminal_io.wait_for_keypress_or_autorefresh
get_detail_of_name = _user_details.get_detail_of_name
get_primary_group_name = _user_details.get_primary_group_name
get_primary_group_names = _user_details.get_primary_group_names


def scheduler_backend(name: Optional[str]) -> Type[GenericBatchSystem]:
    """Return the backend class called name, or GenericBatchSystem if there is none."""
    return next((backend for backend in PLUGIN_BATCH_SYSTEMS if backend.get_mnemonic() == name), GenericBatchSystem)


def node_job_ids(node: Mapping[str, Any]) -> Set[str]:
    """Return the job IDs on a node: its core map plus every backend's node job fields."""
    jobs = set(node["core_job_map"].values())
    for backend in PLUGIN_BATCH_SYSTEMS:
        for field in backend.node_job_fields:
            jobs.update(node.get(field, []))
    return jobs


def queue_is_blocked(state: object, scheduler_name: Optional[str] = None) -> bool:
    """Return True if the queue state is blocked, by the named scheduler's rules."""
    return scheduler_backend(scheduler_name).queue_is_blocked(state)


def check_python_version() -> None:
    """Raise OperationalError unless running on Python 3."""
    if sys.version_info[0] != 3:
        raise OperationalError("Only Python 3 is supported.")


def attempt_faster_xml_parsing(config: Dict[str, Any]) -> None:
    """Only warn: the parsers use xml.etree either way (see faster_xml_parsing in docs/documentation.md)."""
    if config["faster_xml_parsing"] and importlib.util.find_spec("lxml") is None:
        logging.warning('Module lxml is missing. Try issuing "pip install lxml". Reverting to xml module.')


def process_args(args: Any) -> Tuple[Any, int]:
    """Resolve -c AUTO and the remap and names options; return the options and NAMED_WNS."""
    if args.COLOR == "AUTO":
        args.COLOR = "ON" if (os.environ.get("QTOP_COLOR", sys.stdout.isatty()) in ("ON", True)) else "OFF"
    logging.debug("args.COLOR is now set to: %s", args.COLOR)
    args.REMAP = False  # Default value
    NAMED_WNS = 1 if args.FORCE_NAMES else 0
    return args, NAMED_WNS


def require_experimental_opt_in(args: Any) -> None:
    """Exit with status 1 if an experimental feature is requested without --experimental."""
    if args.WEB and not args.EXPERIMENTAL:  # anonymization (-A) graduated in 0.9.20261111 (#548)
        print("The web interface requires --experimental (-e). Exiting...")
        raise SystemExit(1)


def cli_error_message(error: BaseException) -> str:
    """Return the message the CLI prints for an expected error, or an empty one."""
    if isinstance(error, SchedulerNotSpecified):
        return "No scheduler could be auto-detected. Select one with -b/--batchSystem, QTOP_SCHEDULER, or qtopconf.yaml."
    if isinstance(error, (NoSchedulerFound, InvalidScheduler)):
        return ""
    return str(error)


def scheduler_error_lines(error: BaseException) -> Tuple[str, ...]:
    """Return the critical log lines that explain a scheduler the CLI could not select."""
    log = fileutils.active_log_path() or os.path.expandvars(QTOP_LOGFILE)
    if isinstance(error, InvalidScheduler):
        choices = "Selected scheduler system not supported. Available choices are %s." % ", ".join(error.choices)
        return (choices, "For help, try ./qtop.py --help", "Log file created in %s" % log)
    if isinstance(error, NoSchedulerFound):
        return (
            "No suitable scheduler was found. Please define one in a switch or env variable or in %s.\n"
            "For more help, try ./qtop.py --help\nLog file created in %s" % (QTOPCONF_YAML, log),
        )
    return ()


def report_operational_error(error: BaseException, debug: bool = False) -> None:
    """Print an expected error briefly to stderr; with debug, log the traceback too."""
    if isinstance(error, OSError):
        location = " (%s)" % error.filename if error.filename else ""
        message = "%s%s" % (error.strerror or str(error), location)
    else:
        message = str(error)
    message = visible_text(" ".join(message.splitlines()))
    sys.stderr.write("qtop: %s\n" % message)
    # Preserve exception chains even at default verbosity, without sending
    # tracebacks to normal console handlers or requiring logging to be ready.
    logger = logging.getLogger()
    record = logger.makeRecord(__name__, logging.ERROR, __file__, 0, message, (), sys.exc_info())
    for handler in logger.handlers:
        if isinstance(handler, logging.FileHandler):
            handler.handle(record)
    if debug:
        traceback.print_exc(file=sys.stderr)


collect_snapshot = MVU_collect_snapshot
export_snapshot = MVU_export_snapshot


def cli_main() -> int:
    """Run qtop and return its exit status."""
    resources = None
    try:
        with RunResources() as resources:
            main(resources)
            return 0
    except (InvalidScheduler, NoSchedulerFound, SchedulerNotSpecified) as error:
        for line in scheduler_error_lines(error):
            logging.critical("%s", line)
        message = cli_error_message(error)
        if message:
            sys.stderr.write("%s\n" % message)
        return 1
    except (OperationalError, OSError) as error:
        report_operational_error(error, debug=bool(getattr(getattr(resources, "options", None), "DEBUG", False)))
        return 1


# Locations fixed for the whole run: launch directory, package/source root and help file.


def apply_demanding(args: Any) -> None:
    """--demanding turns on the strictest checks together: -S and --strict-parents here; the collector re-validates."""
    if getattr(args, "DEMANDING", False):
        args.STRICTCHECK = args.STRICT_PARENTS = True


def _parse_run_options(resources: RunResources) -> Tuple[Any, Dict[str, Any]]:
    """Parse and validate the command line, including a --load-sample snapshot's display options."""
    args = resources.options = utils.parse_qtop_cmdline_args()
    if args.version:
        print("qtop current version: " + __version__)
        sys.exit(0)
    apply_demanding(args)
    fileutils.STRICT_PARENTS = args.STRICT_PARENTS
    utils.init_logging(args)
    resources.logfile = fileutils.active_log_path()
    dynamic_config: Dict[str, Any] = dict()
    args, dynamic_config["force_names"] = process_args(args)
    require_experimental_opt_in(args)
    if (args.raw_companion or args.masked_traces) and not (args.SAMPLE and args.ANONYMIZE and args.EXPERIMENTAL):
        raise OperationalError("--raw-companion and --masked-traces require -L -A -e")
    if args.load_sample:
        if args.BATCH_SYSTEM or args.SOURCEDIR or args.CONFFILE or args.SAMPLE or args.WATCH or args.REPLAY or args.ANONYMIZE:
            raise OperationalError("--load-sample cannot be combined with -b, -s, -f, -L, -w, -R or -A")
        try:
            args.sample_snapshot = sample.load_snapshot(args.load_sample)
            if args.sample_snapshot["scheduler"] not in discover_qtop_batch_systems():
                raise ValueError("unsupported snapshot scheduler")
            for key in sample.DISPLAY_OPTIONS:
                if key in args.sample_snapshot["options"]:
                    setattr(args, key, args.sample_snapshot["options"][key])
            args, dynamic_config["force_names"] = process_args(args)
        except (OSError, tarfile.TarError, ValueError, TypeError, KeyError) as error:
            raise OperationalError("Cannot load support snapshot: %s" % error) from error
    return args, dynamic_config


def _run_paths() -> RunPaths:
    initial_cwd = os.getcwd()
    logging.debug("Initial qtop directory: %s", initial_cwd)
    curpath = os.path.expanduser(initial_cwd)  # where qtop was invoked from
    package_path = os.path.dirname(os.path.realpath(__file__))
    source_path = os.path.dirname(package_path)
    # Source checkouts keep these user-facing files at repository root. Wheels
    # copy them into the package so an installed CLI has the same defaults.
    qtoppath = source_path if os.path.isfile(os.path.join(source_path, QTOPCONF_YAML)) else package_path
    return RunPaths(initial_cwd, curpath, qtoppath, os.path.join(qtoppath, "helpfile.txt"))


def _load_config(args: Any, paths: RunPaths) -> Tuple[Dict[str, Any], Dict[str, str], Dict[str, str]]:
    try:
        return configuration.load_yaml_config(args, paths.qtoppath, paths.curpath, SYSTEMCONFDIR, USERPATH)
    except CONFIG_ERRORS as error:
        raise OperationalError("Invalid configuration in %s: %s" % (args.CONFFILE or QTOPCONF_YAML, error or type(error).__name__)) from error


def _prepare_replay(args: Any, available_batch_systems: Mapping[str, Any], paths: RunPaths) -> Iterator[str]:
    args.WATCH = 2  # enforce that --watch mode is on, even if not in cmdline switch
    args.BATCH_SYSTEM = next(name for name, backend in available_batch_systems.items() if backend.is_simulated)
    config, _, _ = _load_config(args, paths)
    useful_frames, args.REPLAY = pick_frames_to_replay(config["savepath"], args, config)
    return useful_frames


def _start_web(web: Web, config: Dict[str, Any]) -> None:
    web.host = config.get("experimental_web_host", "127.0.0.1")
    web.port = int(config.get("experimental_web_port", 8080))
    web.start()
    logging.warning("Experimental web interface at http://%s:%s (no authentication or TLS).", web.host, web.port)


def main(resources: RunResources) -> None:
    """Run qtop: collect, render and, in watch mode, repeat until quit."""
    args, dynamic_config = _parse_run_options(resources)
    args._input_progress = fileutils.StartupProgress(sys.stderr)

    available_batch_systems = discover_qtop_batch_systems()

    stdout = resources.stdout

    keyboard = KeyboardInput(sys.stdin)
    viewport = Viewport()  # controls the part of the qtop matrix shown on screen
    max_line_len = 0

    check_python_version()
    paths = _run_paths()
    SAMPLE_FILENAME = "qtop_sample_${USER}%(datetime)s.tar"
    SAMPLE_FILENAME = "qtop_sample_anonymous%(datetime)s.tar" if args.ANONYMIZE else os.path.expandvars(SAMPLE_FILENAME)
    if args.REPLAY:
        useful_frames = _prepare_replay(args, available_batch_systems, paths)

    model = None
    web = resources.web = Web(paths.initial_cwd)
    with terminal_io.raw_mode(sys.stdin, args) as (old_attrs, new_attrs):  # single-key input; the menus switch back to old_attrs
        try:
            while True:
                config, user_to_color, nodestate_to_color = _load_config(args, paths)
                if args.WEB and not web.started:
                    _start_web(web, config)
                savepath = config["savepath"]
                timestr = time.strftime("%Y%m%dT%H%M%S")
                attempt_faster_xml_parsing(config)
                args = init_dirs(args, savepath)

                if args.LESS:
                    viewport.set_term_size(500, 9999)
                else:
                    viewport.set_term_size(*terminal_io.calculate_term_size(config, FALLBACK_TERMSIZE, viewport))
                with resources.frame(savepath, timestr) as output_fp:
                    try:
                        snapshot, config, backend, user_to_color, SAMPLE_FILENAME = _collect_frame(
                            resources, args, config, user_to_color, available_batch_systems, savepath, paths, SAMPLE_FILENAME
                        )
                    finally:
                        args._input_progress.finish()
                    MVU_export_snapshot(snapshot, args, savepath, timestr, web)
                    if model is None:
                        model = MVU_Model(config, args, viewport, display_overrides=dynamic_config, package_path=paths.qtoppath, help_file=paths.help_fp)
                    model, _ = MVU_update(model, "snapshot", (snapshot, config, args, backend, user_to_color, nodestate_to_color))
                    model, _ = MVU_update(model, "terminal", terminal_io.capture_terminal())
                    view_data = MVU_query(model)
                    cluster = view_data.cluster
                    MVU_resolve_user_details(view_data.model, view_data.occupancy)
                    display = MVU_view(view_data, savepath, SAMPLE_FILENAME, resources.logfile)

                view_data.model.viewport.max_height, max_line_len = get_output_size(max_line_len, output_fp, term_columns=viewport.h_term_size if args.WATCH else 0)
                model, _ = MVU_update(model, "layout", view_data.model.viewport)
                viewport = model.viewport

                if args.ONLYSAVETOFILE:  # no display of qtop output, will exit
                    break
                if not args.WATCH:  # one-off display of qtop output, will exit afterwards (no --watch cmdline switch)
                    cat_command = ["/bin/cat", output_fp]  # not clearing the screen beforehand is the intended behaviour here
                    _ = subprocess.call(cat_command, stdout=stdout, stderr=stdout)  # nosec B603 # nosemgrep - fixed /bin/cat argv
                    break
                # --watch
                if args.REPLAY:
                    try:
                        output_partview_fp = next(useful_frames)
                    except StopIteration:
                        logging.critical("No (more) recorded instances available to show! Exiting...")
                        break
                else:
                    shown_fp = paths.help_fp if dynamic_config.get("show_help") else output_fp
                    output_partview_fp = display.show_part_view(timestr, file=shown_fp, x=viewport.v_start, y=viewport.v_term_size)
                    logging.debug("Shown file in main loop: %s", shown_fp)
                clear_command = ["/usr/bin/clear"]
                cat_command = ["/bin/cat", output_partview_fp]
                _ = subprocess.call(clear_command, stdout=stdout, stderr=stdout)  # nosec B603 # nosemgrep - fixed /usr/bin/clear argv
                _ = subprocess.call(cat_command, stdout=stdout, stderr=stdout)  # nosec B603 # nosemgrep - fixed /bin/cat argv

                read_char = terminal_io.wait_for_keypress_or_autorefresh(viewport, config, FALLBACK_TERMSIZE, int(args.WATCH) or KEYPRESS_TIMEOUT, keyboard)
                model, effects = MVU_update(model, read_char)
                viewport, dynamic_config = model.viewport, model.display_overrides
                MVU_effects(model, effects, cluster, old_attrs, new_attrs, keyboard, paths.help_fp if dynamic_config.get("show_help") else output_fp)

                os.chdir(paths.qtoppath)
                resources.discard_frame()
                fileutils.deprecate_old_output_files(config)

        except (KeyboardInterrupt, EOFError):
            if not (args.SAMPLE and args.ANONYMIZE):
                print("\nExiting. Thank you for ...watching ;)\n")
            raise SystemExit(0) from None


if __name__ == "__main__":
    sys.exit(cli_main())
