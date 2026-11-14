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
"""qtop's command line, watch loop and the deprecated legacy adapters."""

import contextlib
import copy
import datetime
import glob
import importlib.util
import logging
import os
import re
import select
import shlex
import shutil
import signal as _signal
import subprocess
import sys
import tarfile
import tempfile
import time
import traceback
import warnings
from collections import OrderedDict
from functools import lru_cache
from math import isfinite
from signal import SIG_DFL, signal
from types import ModuleType, SimpleNamespace
from typing import TYPE_CHECKING, Any, Callable, Dict, Generator, Iterable, Iterator, List, Mapping, NamedTuple, Optional, Sequence, Set, Tuple, Type, Union
from xml.etree.ElementTree import ParseError

if TYPE_CHECKING:
    from qtop_py import state_types

    config: state_types.Config
    dynamic_config: state_types.Config

from qtop_py import __version__, configuration, fileutils, sample, utils
from qtop_py.colormap import queue_to_color as colormap_queue_to_color
from qtop_py.colormap import user_to_color_default
from qtop_py.constants import (
    FALLBACK_TERMSIZE,
    KEYPRESS_TIMEOUT,
    QTOP_LOGFILE,
    QTOPCONF_YAML,
    SYSTEMCONFDIR,
    USERPATH,
)
from qtop_py.errors import OperationalError
from qtop_py.input_parsing import extract_regex_detail, get_date_obj_from_str
from qtop_py.mvu import (
    MVU_colorize,
    MVU_colorize_nodestate,
    MVU_compress_colored_line,
    MVU_gauge_core_vectors,
    MVU_keep_queue_initials_only_and_colorize,
    MVU_Model,
    MVU_update,
)
from qtop_py.mvu_query import Cluster as MVU_Cluster
from qtop_py.mvu_query import MVU_query
from qtop_py.mvu_query import WNOccupancy as MVU_WNOccupancy
from qtop_py.process import communicate_with_timeout
from qtop_py.runtime import RunResources
from qtop_py.safety import visible_text
from qtop_py.schedulers.registry import PLUGIN_BATCH_SYSTEMS
from qtop_py.serialiser import GenericBatchSystem
from qtop_py.snapshot import Snapshot
from qtop_py.ui.controls import read_rule_choices
from qtop_py.ui.mvu_view import MVU_view
from qtop_py.ui.mvu_view import TextDisplay as MVU_TextDisplay
from qtop_py.ui.viewport import Viewport
from qtop_py.web import Web

SIGPIPE = getattr(_signal, "SIGPIPE", None)  # absent on Windows
SIGWINCH = getattr(_signal, "SIGWINCH", None)
termios: Optional[ModuleType]
try:
    import termios
except ImportError:
    termios = None

# Optional context for the deprecated legacy adapters (MVU_legacy_model, colorize, control_qtop and the
# Cluster/WNOccupancy/TextDisplay classes below), which read it via globals(); declared (not assigned) for
# type checkers. main() sets none of it: each run's state is local and passed explicitly.
args: Any
scheduler: Any
snapshot: Any
user_to_color: Dict[str, str]
viewport: Viewport
HELP_FP: str
QTOPPATH: str

Document = Snapshot  # deprecated alias, kept for one release; use Snapshot

colorize_nodestate = MVU_colorize_nodestate

queue_to_color = colormap_queue_to_color  # re-exported; tools/validate_trace_export.py reads qtop.queue_to_color

compress_colored_line = MVU_compress_colored_line

gauge_core_vectors = MVU_gauge_core_vectors

keep_queue_initials_only_and_colorize = MVU_keep_queue_initials_only_and_colorize


LEGACY_DEPRECATION = "%s is deprecated since 0.9.20261110 and will be removed in a later release; %s"


def _warn_legacy(name: str, instead: str) -> None:
    warnings.warn(LEGACY_DEPRECATION % (name, instead), DeprecationWarning, stacklevel=3)


def MVU_legacy_model(instance: Any = None) -> MVU_Model:
    """Deprecated compatibility boundary for older qtop imports; new pipeline stages never use it."""
    _warn_legacy("qtop.MVU_legacy_model()", "build an MVU_Model and pass it explicitly")
    return _legacy_model(instance)


def _legacy_model(instance: Any = None) -> MVU_Model:
    """MVU_legacy_model without the warning, for the other deprecated adapters."""
    display_overrides = globals().get("dynamic_config", {})
    config = getattr(instance, "config", globals().get("config", {})) or {}
    options = getattr(instance, "args", globals().get("args")) or SimpleNamespace(COLOR="OFF")
    name = globals().get("scheduler", config.get("scheduler", ""))
    backend = scheduler_backend(name)
    model = MVU_Model(
        config,
        options,
        getattr(instance, "viewport", globals().get("viewport")),
        display_overrides=display_overrides,
        snapshot=getattr(instance, "snapshot", globals().get("snapshot")),
        user_to_color=globals().get("user_to_color", {}),
        package_path=globals().get("QTOPPATH", ""),
        help_file=globals().get("HELP_FP", ""),
    )
    model.backend, model.scheduler = backend, name
    model.transposed_sections = globals().get("transposed_sections", [])
    return model


class MVU_LegacyAdapter(object):
    """Deprecated: qtop's Cluster/WNOccupancy/TextDisplay; use mvu_query/ui.mvu_view through MVU_query and MVU_view."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        _warn_legacy("qtop.%s" % type(self).__name__, "use MVU_Model with MVU_update, MVU_query and MVU_view")
        if kwargs.get("MVU_model") is None:
            kwargs["MVU_model"] = _legacy_model(self)
        super().__init__(*args, **kwargs)
        for name in ("config", "args", "snapshot", "viewport"):
            if hasattr(self, name):
                setattr(self.MVU_model, name, getattr(self, name))

    @property
    def MVU_model(self) -> Any:
        """Return the model set on this adapter, or one built from qtop's module globals."""
        model = self.__dict__.get("_MVU_model")
        return _legacy_model(self) if model is None else model

    @MVU_model.setter
    def MVU_model(self, model: Any) -> None:
        """Return the model set on this adapter, or one built from qtop's module globals."""
        self._MVU_model = model


class Cluster(MVU_LegacyAdapter, MVU_Cluster):
    """Deprecated: use mvu_query.Cluster through MVU_query."""


class WNOccupancy(MVU_LegacyAdapter, MVU_WNOccupancy):
    """Deprecated: use mvu_query.WNOccupancy through MVU_query."""

    def __init__(self, cluster: Any, config: Dict[str, Any], snapshot: Any, user_to_color: Dict[str, str], job_ids: Any = None, **kwargs: Any) -> None:
        super().__init__(cluster, config, snapshot, user_to_color, **kwargs)

    def strict_check_jobs(self, cluster: Any) -> None:
        """Print a mismatch message if the counted and reported running jobs differ."""
        occupancy = copy.copy(self)
        occupancy.MVU_model = _legacy_model(self)
        message = MVU_WNOccupancy.MVU_check_jobs(occupancy, cluster)
        if message:
            print(message)


class TextDisplay(MVU_LegacyAdapter, MVU_TextDisplay):
    """Deprecated: use ui.mvu_view.TextDisplay through MVU_view."""

    def display_user_accounts_pool_mappings(self, wns_occupancy: Any) -> None:
        """Resolve user details, then print the user accounts and pool mappings table."""
        self.MVU_model = self.MVU_model
        MVU_resolve_user_details(self.MVU_model, wns_occupancy, force=True)
        return super().display_user_accounts_pool_mappings(wns_occupancy)


def colorize(
    text: Any, color_func: Optional[str] = None, pattern: str = "NoPattern", mapping: Optional[Mapping[str, str]] = None, bg_color: Optional[str] = None, bold: bool = False
) -> Any:
    """Deprecated: use MVU_Model.MVU_colorize, which honours the model's own -c setting."""
    _warn_legacy("qtop.colorize()", "use MVU_Model.MVU_colorize")
    return MVU_colorize(_legacy_model(), text, color_func, pattern, mapping, bg_color, bold)


def MVU_resolve_user_details(model: Any, occupancy: Any, force: bool = False) -> None:
    """Load user full names into the model, only if the accounts table is shown (or force is set)."""
    parts = model.config.get("user_display_parts", [])
    if not force and not any(part == "user_accounts_pool_mappings" and not getattr(model.args, "sect_%s_off" % index, False) for index, part in enumerate(parts, 1)):
        model.user_details = {}
        return
    model.user_details = {
        "names": get_detail_of_name(occupancy.account_jobs_table, model.config, model.args),
        "groups": get_primary_group_names(occupancy.account_jobs_table) if getattr(model.args, "SHOW_GROUPS", False) else None,
    }


def MVU_effects(model: Any, effects: Iterable[Tuple[str, Optional[str]]], cluster: Any, old_attrs: Any, new_attrs: Any) -> None:
    """Execute requested UI I/O outside MVU_update and the snapshot query."""
    for action, value in effects:
        if action == "notice":
            print("%s %s" % (model.MVU_colorize("***", "Green_L"), value))
        elif action == "quit":
            print(model.MVU_colorize("\nExiting. Thank you for ..watching ;)\n", "Cyan_L"))
            raise SystemExit(0)
        elif action == "sort":
            choose_sort(model.viewport, model.display_overrides, old_attrs, new_attrs, model.MVU_colorize)
        elif action == "filter":
            choose_filter(model.viewport, cluster, model.display_overrides, old_attrs, new_attrs, model.MVU_colorize)
        elif action == "highlight":
            choose_highlight(model.viewport, cluster, model.display_overrides, old_attrs, new_attrs, model.MVU_colorize)


def control_qtop(viewport: Viewport, read_char: str, cluster: Any, old_attrs: Any, new_attrs: Any) -> None:
    """Deprecated key handler on module globals; main() uses MVU_update and MVU_effects on its own model."""
    global dynamic_config
    _warn_legacy("qtop.control_qtop()", "use MVU_update and MVU_effects")
    model = _legacy_model()
    model.viewport = viewport
    next_model, effects = MVU_update(model, read_char)
    viewport.__dict__.update(next_model.viewport.__dict__)
    next_model.viewport = viewport
    dynamic_config = next_model.display_overrides
    MVU_effects(next_model, effects, cluster, old_attrs, new_attrs)


def reset_sigpipe() -> None:
    """Restore the default SIGPIPE action, so piping into head exits quietly."""
    if SIGPIPE is not None:
        signal(SIGPIPE, SIG_DFL)


def _set_terminal_attrs(attrs: Any) -> None:
    """Apply terminal attributes when the platform exposes a real stdin TTY."""
    if termios is not None and sys.__stdin__ is not None:
        termios.tcsetattr(sys.__stdin__.fileno(), termios.TCSADRAIN, attrs)


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


@contextlib.contextmanager
def raw_mode(file: Any, args: Any) -> Generator[Tuple[Any, Any], None, None]:
    """Put a watch-mode terminal in cbreak mode (no echo or line buffering; Ctrl-C still works); yield (old, new) attrs.

    Without a terminal, or outside watch mode, nothing changes and the attrs are empty, so keys need Enter.
    Unread keys are dropped on exit, rather than spilling into the shell. SIGTERM and SIGHUP (kill, a closed
    ssh session) exit through the restore too, and SIGCONT (fg after Ctrl-Z) re-applies cbreak mode.
    """
    old_attrs: Any = ""
    new_attrs: Any = ""
    if args.WATCH and not args.ONLYSAVETOFILE and termios is not None:
        try:
            old_attrs = termios.tcgetattr(file.fileno())
            new_attrs = old_attrs[:]
            new_attrs[3] &= ~(termios.ECHO | termios.ICANON)
            termios.tcsetattr(file.fileno(), termios.TCSADRAIN, new_attrs)
        except (AttributeError, OSError, termios.error):
            old_attrs = new_attrs = ""  # not a terminal: nothing to restore
    if not old_attrs or termios is None:
        yield "", ""
        return
    tty = termios

    def leave(signum: int, _frame: Any) -> None:
        raise SystemExit(128 + signum)

    def resume(_signum: int, _frame: Any) -> None:
        tty.tcsetattr(file.fileno(), tty.TCSADRAIN, new_attrs)

    handlers = [(sig, _signal.signal(sig, handler)) for sig, handler in ((_signal.SIGTERM, leave), (_signal.SIGHUP, leave), (_signal.SIGCONT, resume))]
    try:
        yield old_attrs, new_attrs
    finally:
        for sig, previous in handlers:
            _signal.signal(sig, previous)
        termios.tcsetattr(file.fileno(), termios.TCSAFLUSH, old_attrs)


def calculate_term_size(config: Dict[str, Any], FALLBACK_TERM_SIZE: Sequence[int], viewport: Viewport) -> Tuple[int, int]:
    """Gets the dimensions of the terminal window where qtop will be displayed.

    ``viewport`` is passed explicitly so this helper does not depend on global
    state. If ``stty`` is absent, cannot be started, or returns unusable output,
    the viewport size is used before the configured fallback.
    """
    fallback_term_size = config.get("term_size", FALLBACK_TERM_SIZE)
    try:
        height, columns = [int(value) for value in fallback_term_size]
        if height <= 0 or columns <= 0:
            raise ValueError("dimensions must be positive")
    except (TypeError, ValueError) as error:
        raise OperationalError("Invalid configuration: term_size must contain two positive integers.") from error

    stty = shutil.which("stty")
    stty_error: Union[bytes, OSError] = b"stty not found"
    tty_size = b""
    if stty:
        try:
            command = subprocess.Popen([stty, "size"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            tty_size, stty_error = command.communicate()
        except OSError as exc:
            stty_error = exc

    if not stty_error:
        try:
            term_height, term_columns = [int(x) for x in tty_size.strip().split()]
            if term_height <= 0 or term_columns <= 0:
                raise ValueError("terminal dimensions must be positive")
        except (TypeError, ValueError):
            logging.debug("Ignoring unusable terminal dimensions reported by stty: %r", tty_size)
        else:
            logging.debug('terminal size v, h from "stty size": %s, %s' % (term_height, term_columns))
            return term_height, term_columns

    logging.warning("Failed to autodetect terminal size. (Running in an IDE? in a pipe?) Falling back via %s / viewport." % QTOPCONF_YAML)
    try:
        term_height, term_columns = [int(x) for x in viewport.get_term_size()]
        if term_height <= 0 or term_columns <= 0:
            raise ValueError("terminal dimensions must be positive")
    except (AttributeError, KeyError, OSError, TypeError, ValueError):
        term_height, term_columns = [int(x) for x in fallback_term_size]
        logging.debug("(hardcoded) fallback terminal size v, h: %s, %s" % (term_height, term_columns))
    else:
        logging.debug("viewport-provided terminal size v, h: %s, %s" % (term_height, term_columns))

    return term_height, term_columns


def finalize_filepaths_schedulercommands(args: Any, config: Dict[str, Any], backend: Any) -> Dict[str, Tuple[str, str]]:
    """Return {input_key: (path, command)}, with PID suffixes for live collection."""
    d = dict()
    fn_append = "_" + str(os.getpid()) if not args.SOURCEDIR else ""
    for fn, path_command in config["schedulers"][backend.get_mnemonic()].items():
        path, command = path_command.strip().split(", ", 1)
        path = path % {"savepath": args.workdir, "pid": fn_append}
        # Format codes stay copyable; accept legacy escaped percent signs.
        command = command.replace("%%", "%").replace("%(savepath)s", args.workdir)
        command = backend.collection_command(fn, command, args)
        d[fn] = (path, command)
    return d


def scheduler_env() -> Dict[str, str]:
    """Environment for scheduler-facing commands: C locale, so the dates and messages qtop parses are not localised."""
    env = dict(os.environ)
    env["LC_ALL"] = "C"
    return env


def auto_get_avail_batch_system(config: Dict[str, Any], backends: Mapping[str, Any]) -> str:
    """Return the first scheduler whose commands exist on this system, for scheduler auto."""
    signature_commands: Dict[str, str] = config["signature_commands"]
    prioritized_commands = sorted(signature_commands.items(), key=lambda item: backends.get(item[0], GenericBatchSystem).detection_priority)
    for system, batch_command in prioritized_commands:
        if system not in backends or backends[system].is_simulated or not shutil.which(batch_command):
            continue
        logging.debug("Auto-detected scheduler: %s", system)
        return system

    raise SchedulerNotSpecified


def execute_shell_batch_commands(batch_system_commands: Mapping[str, str], filenames: Dict[str, str], _file: str, _savepath: str, timeout: float = 30) -> str:
    """Run the scheduler commands from QTOPCONF_YAML, saving each output atomically to its trace file."""
    _batch_system_command = batch_system_commands[_file].strip()
    tempname = None
    try:
        if not _batch_system_command:
            raise OperationalError("Empty scheduler command for %s; check %s." % (_file, QTOPCONF_YAML))
        try:
            timeout = float(timeout)
            if not isfinite(timeout) or timeout <= 0:
                raise ValueError("nonpositive or nonfinite timeout")
        except (TypeError, ValueError, OverflowError) as error:
            raise OperationalError("scheduler_command_timeout must be a positive finite number of seconds") from error
        with tempfile.NamedTemporaryFile("wb", dir=_savepath, delete=False) as fin:
            logging.debug("\tFile state before subprocess call: %(fin)s" % {"fin": fin})
            logging.debug("\tWaiting on subprocess.call...")

            # splitting the command passed in so only the first item in a command
            # from the yaml file is executed with the rest of the line treated as
            # arguments, this enables shell=False, and keeps us from having
            # injected commands
            tempname = fin.name
            try:
                command_args = shlex.split(_batch_system_command)
                logging.debug(
                    "Trace collection command: (cd %s && %s > %s)",
                    shlex.quote(os.getcwd()),
                    " ".join(shlex.quote(arg) for arg in command_args),
                    shlex.quote(os.path.abspath(filenames[_file])),
                )
                command = subprocess.Popen(command_args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, start_new_session=(os.name == "posix"), env=scheduler_env())
            except OSError as error:
                raise OperationalError("Cannot execute scheduler command %r: %s. Check PATH or load the scheduler module." % (_batch_system_command, error.strerror)) from error
            except ValueError as error:
                raise OperationalError("Invalid scheduler command %r: %s. Check command quoting in %s." % (_batch_system_command, error, QTOPCONF_YAML)) from error
            try:
                stderr_data = communicate_with_timeout(command, timeout, stdout=fin)[1]
            except subprocess.TimeoutExpired as failure:
                raise OperationalError("Scheduler command %r timed out after %g seconds" % (_batch_system_command, timeout)) from failure
            if stderr_data:
                logging.warning("A message from your shell: %s", stderr_data.decode("utf-8", errors="replace").strip())
            if command.returncode:
                detail = " ".join(stderr_data.decode("utf-8", errors="replace").splitlines())[:300] or "no diagnostic output"
                raise OperationalError("Scheduler command %r failed (exit %s): %s" % (_batch_system_command, command.returncode, detail))
            logging.debug("File state after subprocess call: %(fin)s" % {"fin": fin})
        os.rename(tempname, filenames[_file])

        return filenames[_file]
    finally:
        if tempname is not None:
            with contextlib.suppress(OSError):
                os.unlink(tempname)


def get_detail_of_name(account_jobs_table: Sequence[Sequence[Any]], config: Dict[str, Any], args: Any) -> Dict[str, str]:
    """Return each user's full name, read from getent_passwd.txt (or the file QTOPCONF_YAML names)."""
    extract_info = config.get("extract_info", None)
    if not extract_info:
        return dict()

    sep = ":"
    field_idx = int(extract_info.get("field_to_use", 5))
    regex = extract_info.get("regex", None)

    command_key = "user_details_realtime" if args.GET_GECOS else "user_details_cache"
    command = extract_info.get(command_key)
    if not isinstance(command, str) or not command.strip():
        logging.warning("No valid %s command in %s; skipping user details.", command_key, QTOPCONF_YAML)
        return {}

    try:
        if args.GET_GECOS:
            users = " ".join(shlex.quote(str(line[4])) for line in account_jobs_table if line[4] != "Totals")
            command = command % users
        user_details_command = shlex.split(command)
        if not args.GET_GECOS:
            user_details_command[-1] = os.path.expandvars(user_details_command[-1])
        timeout = float(config.get("scheduler_command_timeout", 30))
        if not isfinite(timeout) or timeout <= 0:
            raise ValueError("scheduler_command_timeout must be positive and finite")
        p = subprocess.Popen(user_details_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, start_new_session=(os.name == "posix"), env=scheduler_env())
        raw_output, raw_err = communicate_with_timeout(p, timeout)
        output, err = (raw_output or b"").decode("utf-8", errors="replace"), raw_err.decode("utf-8", errors="replace")
    except (OperationalError, OSError, ValueError, TypeError, IndexError, OverflowError, subprocess.TimeoutExpired) as error:
        logging.warning("Cannot collect %s command %r: %s; skipping user details.", command_key, command, error)
        return {}
    if p.returncode:
        logging.warning("%s failed (exit %s): %s; skipping user details.", command_key, p.returncode, err.strip()[:300])
        return {}
    if err:
        logging.warning("%s: %s", command_key, err.strip()[:300])

    detail_of_name = dict()
    for line in output.split("\n"):
        try:
            user, field = line.strip().split(sep)[0 : field_idx : field_idx - 1]
        except ValueError:
            continue
        else:
            try:
                detail = extract_regex_detail(regex, field)
            except (AttributeError, TypeError, ValueError):
                detail = field.strip()
            finally:
                detail_of_name[user] = visible_text(detail)
    return detail_of_name


@lru_cache(maxsize=None)
def get_primary_group_name(user: str) -> str:
    """Resolve and cache one primary group, including through NSS/LDAP."""
    try:
        import grp
        import pwd
    except ImportError:
        logging.warning("Unix group information is unavailable on this platform.")
        return ""

    try:
        return grp.getgrgid(pwd.getpwnam(user).pw_gid).gr_name
    except (KeyError, OSError):
        logging.warning("Could not resolve the primary Unix group for %s.", user)
        return ""


def get_primary_group_names(account_jobs_table: Sequence[Sequence[Any]]) -> Dict[str, str]:
    """Resolve primary groups only for users present in the current snapshot."""
    users = (line[4] for line in account_jobs_table if line[4] != "Totals")
    groups = ((user, get_primary_group_name(user)) for user in users)
    return dict((user, group) for user, group in groups if group)


def get_input_filenames(INPUT_FNs_commands: Mapping[str, Tuple[str, str]], config: Dict[str, Any], args: Any, backend: Any) -> Dict[str, str]:
    """Return the trace files by key: found in the -s directory, or collected by running the scheduler commands."""
    filenames = dict()
    batch_system_commands = dict()
    for _file in INPUT_FNs_commands:
        filenames[_file], batch_system_commands[_file] = INPUT_FNs_commands[_file]

        if not args.SOURCEDIR:
            _savepath = os.path.realpath(os.path.expandvars(config["savepath"]))
            try:
                filenames[_file] = execute_shell_batch_commands(batch_system_commands, filenames, _file, _savepath, timeout=config.get("scheduler_command_timeout", 30))
            except OperationalError as error:
                if _file not in backend.optional_trace_keys:
                    raise
                logging.warning("%s %s", error, backend.optional_trace_warning)
                filenames.pop(_file)
                continue

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


def check_python_version() -> None:
    """Raise OperationalError unless running on Python 3."""
    if sys.version_info[0] != 3:
        raise OperationalError("Only Python 3 is supported.")


@contextlib.contextmanager
def menu_terminal(old_attrs: Any, new_attrs: Any) -> Generator[None, None, None]:
    """Show a menu with the terminal echoing input, then restore watch-mode settings; without a terminal, change nothing."""
    if not old_attrs:
        yield
        return
    _set_terminal_attrs(old_attrs)
    try:
        yield
    finally:
        _set_terminal_attrs(new_attrs)


def choose_sort(viewport: Viewport, display_overrides: Dict[str, Any], old_attrs: Any, new_attrs: Any, colorize: Callable[..., Any] = colorize) -> None:
    """Ask for a worker-node sort order and store it in the display overrides."""
    sort_map: Dict[str, Any] = OrderedDict()

    sort_map["0"] = ("sort reset", [])
    sort_map["1"] = ("sort by nodename-notnum", [])
    sort_map["2"] = ("sort by nodename-notnum length", [])
    sort_map["3"] = ("sort by all numbers", [])
    sort_map["4"] = ("sort by first letter", [])
    sort_map["5"] = ("sort by node state", [])
    sort_map["6"] = ("sort by nr of cores", [])
    sort_map["7"] = ("sort by core occupancy", [])

    print(
        "Type in sort order. This can be a single number or a sequence of numbers,\n"
        "e.g. to sort first by first word, then by all numbers then by first name length, type 132, then <enter>."
    )

    for nr, sort_method in sort_map.items():
        print("(%s): %s" % (colorize(nr, color_func="Red_L"), sort_method[0]))

    with menu_terminal(old_attrs, new_attrs):
        display_overrides["user_sort"] = []
        while True:
            sort_choice = input(
                "\nChoose sorting order, or Enter to exit:-> ",
            )
            if not sort_choice:
                break

            sort_order = list(sort_choice)
            if not set(sort_order).issubset(sort_map):
                continue

            sort_args = [sort_map[i] for i in sort_order]
            display_overrides["user_sort"] = sort_args
            break
    viewport.reset_display()


def choose_filter(viewport: Viewport, cluster: Any, display_overrides: Dict[str, Any], old_attrs: Any, new_attrs: Any, colorize: Callable[..., Any] = colorize) -> None:
    """Ask for worker-node filters and store them in the display overrides."""
    cluster.wn_filter = cluster.WNFilter(cluster.worker_nodes)
    filter_map = {
        1: "exclude_node_states",
        2: "exclude_numbers",
        3: "exclude_name_patterns",
        4: "or_include_node_states",
        5: "or_include_numbers",
        6: "or_include_name_patterns",
        7: "include_node_states",
        8: "include_numbers",
        9: "include_name_patterns",
        10: "include_queues",
    }
    print(
        "Filter out nodes by:\n%(one)s state %(two)s number"
        " %(three)s name substring or RegEx pattern"
        % {
            "one": colorize("(1)", color_func="Red_L"),
            "two": colorize("(2)", color_func="Red_L"),
            "three": colorize("(3)", color_func="Red_L"),
        }
    )
    print(
        "Filter in nodes by:\n(any) %(four)s state %(five)s number %(six)s name substring or RegEx pattern\n"
        "(all) %(seven)s state %(eight)s number %(nine)s name substring or RegEx pattern %(ten)s queue"
        % {
            "four": colorize("(4)", color_func="Red_L"),
            "five": colorize("(5)", color_func="Red_L"),
            "six": colorize("(6)", color_func="Red_L"),
            "seven": colorize("(7)", color_func="Red_L"),
            "eight": colorize("(8)", color_func="Red_L"),
            "nine": colorize("(9)", color_func="Red_L"),
            "ten": colorize("(10)", color_func="Red_L"),
        }
    )
    with menu_terminal(old_attrs, new_attrs):
        display_overrides["filtering"] = []
        read_rule_choices("Filter", filter_map, display_overrides["filtering"])
    viewport.reset_display()


def choose_highlight(viewport: Viewport, cluster: Any, display_overrides: Dict[str, Any], old_attrs: Any, new_attrs: Any, colorize: Callable[..., Any] = colorize) -> None:
    """Ask for users or queues to highlight and store them in the display overrides."""
    cluster.wn_filter = cluster.WNFilter(cluster.worker_nodes)
    filter_map = {
        1: "or_include_user_id",
        2: "or_include_user_pat",
        3: "or_include_queue",
        4: "include_user_id",
        5: "include_user_pat",
        6: "include_queue",
    }
    print(
        "Highlight cores by:\n"
        "(any) %(one)s userID %(two)s user name (regex) %(three)s queue\n"
        "(all) %(four)s userID %(five)s user name (regex) %(six)s queue"
        % {
            "one": colorize("(1)", color_func="Red_L"),
            "two": colorize("(2)", color_func="Red_L"),
            "three": colorize("(3)", color_func="Red_L"),
            "four": colorize("(4)", color_func="Red_L"),
            "five": colorize("(5)", color_func="Red_L"),
            "six": colorize("(6)", color_func="Red_L"),
        }
    )
    with menu_terminal(old_attrs, new_attrs):
        display_overrides["highlight"] = []
        read_rule_choices("Highlight", filter_map, display_overrides["highlight"])
    viewport.reset_display()


def fetch_scheduler_files(args: Any, config: Dict[str, Any], backend: Any) -> Dict[str, str]:
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


def decide_batch_system(
    cmdline_switch: Optional[str],
    env_var: Optional[str],
    config_file_batch_option: str,
    schedulers: Mapping[str, Any],
    available_batch_systems: Mapping[str, Any],
    config: Dict[str, Any],
) -> str:
    """Return the scheduler from -b, QTOP_SCHEDULER or QTOPCONF_YAML, in that order.

    With auto, guess it from the scheduler commands available on this system.
    """
    avail_systems = list(available_batch_systems.keys()) + ["auto"]
    if cmdline_switch and cmdline_switch.lower() not in avail_systems:
        logging.critical("Selected scheduler system not supported. Available choices are %s." % ", ".join(avail_systems))
        logging.critical("For help, try ./qtop.py --help")
        logging.critical("Log file created in %s" % os.path.expandvars(QTOP_LOGFILE))
        raise InvalidScheduler
    for scheduler in (cmdline_switch, env_var, config_file_batch_option):
        if scheduler is None:
            continue
        scheduler = scheduler.lower()

        if scheduler == "auto":
            scheduler = auto_get_avail_batch_system(config, available_batch_systems)
            logging.debug("Selected scheduler is %s" % scheduler)
            return scheduler
        if scheduler in schedulers:
            logging.info("User-selected scheduler: %s" % scheduler)
            return scheduler
        if scheduler and scheduler not in schedulers:  # a scheduler that does not exist is inputted
            raise NoSchedulerFound
    raise NoSchedulerFound


def get_output_size(max_line_len: int, output_fp: str, max_height: int = 0) -> Tuple[int, int]:
    """Returns the char dimensions of the entirety of the qtop output file."""
    ansi_escape = re.compile(r"\x1b[^m]*m")  # matches ANSI escape characters

    if not max_height or not max_line_len:
        with open(output_fp, "r", encoding="utf-8", errors="replace") as output_file:
            output_lines = output_file.readlines()
        if not output_lines:
            raise ValueError("There is no output from qtop *whatsoever*. Weird.")

        if not max_height:
            max_height = len(output_lines)

        if not max_line_len:
            max_line_len = max(len(ansi_escape.sub("", line.strip())) for line in output_lines)

    logging.debug("Total nr of lines: %s" % max_height)
    logging.debug("Max line length: %s" % max_line_len)

    return max_height, max_line_len


def attempt_faster_xml_parsing(config: Dict[str, Any]) -> None:
    """Only warn: the parsers use xml.etree either way (see faster_xml_parsing in docs/documentation.md)."""
    if config["faster_xml_parsing"] and importlib.util.find_spec("lxml") is None:
        logging.warning('Module lxml is missing. Try issuing "pip install lxml". Reverting to xml module.')


def init_dirs(args: Any, _savepath: str) -> Any:
    """Resolve the -s source (directory or trace archive) and the save path; return the updated options."""
    args.SOURCEDIR = os.path.realpath(args.SOURCEDIR) if args.SOURCEDIR else None
    if args.SOURCEDIR and os.path.isfile(args.SOURCEDIR):
        if tarfile.is_tarfile(args.SOURCEDIR) or args.SOURCEDIR.lower().endswith((".tar", ".tar.gz", ".tgz", ".tar.bz2", ".tbz2", ".tar.xz", ".txz")):
            args.source_archive = args.SOURCEDIR
            args.source_workspace = tempfile.TemporaryDirectory(prefix="qtop-traces-")
            args.SOURCEDIR = args.source_workspace.name
        else:
            args.SOURCEDIR = os.path.dirname(args.SOURCEDIR)
    logging.debug("User-defined source directory: %s" % args.SOURCEDIR)
    args.workdir = args.SOURCEDIR or _savepath
    logging.debug("Working directory is now: %s" % args.workdir)
    os.chdir(args.workdir)
    return args


class TerminalResized(Exception):
    """SIGWINCH arrived while qtop waited for a key."""


def _raise_resized(_signum: int, _frame: Any) -> None:
    raise TerminalResized()


def wait_for_keypress_or_autorefresh(viewport: Viewport, config: Dict[str, Any], FALLBACK_TERMSIZE: Sequence[int], KEYPRESS_TIMEOUT: float = 1) -> str:
    """Return the key pressed within the refresh interval, or a newline to redraw: at its end, or at once on a resize (SIGWINCH)."""
    previous = _signal.signal(SIGWINCH, _raise_resized) if SIGWINCH is not None else None  # only while waiting, never mid-frame
    try:
        while sys.stdin in select.select([sys.stdin], [], [], KEYPRESS_TIMEOUT)[0]:
            _read_char = sys.stdin.read(1)
            if _read_char:
                logging.debug("Pressed %s" % _read_char)
                return _read_char
    except TerminalResized:
        logging.debug("Terminal resized")
    finally:
        if SIGWINCH is not None:
            _signal.signal(SIGWINCH, previous)
    viewport.set_term_size(*calculate_term_size(config, FALLBACK_TERMSIZE, viewport))  # a resize just redraws ("r" cycles core lines)
    return "\n"


def discover_qtop_batch_systems() -> Dict[str, Any]:
    """Return the scheduler backends by name; raise OperationalError on an empty or duplicate name."""
    available_batch_systems = {}
    for batch_system in PLUGIN_BATCH_SYSTEMS:
        mnemonic = batch_system.get_mnemonic()
        if not mnemonic:
            raise OperationalError("Scheduler plugin has an empty mnemonic.")
        if mnemonic in available_batch_systems:
            raise OperationalError("Duplicate scheduler mnemonic: %s" % mnemonic)
        available_batch_systems[mnemonic] = batch_system

    return available_batch_systems


def process_args(args: Any) -> Tuple[Any, int]:
    """Resolve -c AUTO and the remap and names options; return the options and NAMED_WNS."""
    if args.COLOR == "AUTO":
        args.COLOR = "ON" if (os.environ.get("QTOP_COLOR", sys.stdout.isatty()) in ("ON", True)) else "OFF"
    logging.debug("args.COLOR is now set to: %s" % args.COLOR)
    args.REMAP = False  # Default value
    NAMED_WNS = 1 if args.FORCE_NAMES else 0
    return args, NAMED_WNS


def require_experimental_opt_in(args: Any) -> None:
    """Exit with status 1 if an experimental feature is requested without --experimental."""
    if args.WEB and not args.EXPERIMENTAL:  # anonymization (-A) graduated in 0.9.20261111 (#548)
        print("The web interface requires --experimental (-e). Exiting...")
        raise SystemExit(1)


def pick_frames_to_replay(_savepath: str, args: Any, config: Dict[str, Any]) -> Tuple[Iterator[str], Any]:
    """Return the saved frames in savepath that -R asks to replay."""
    if args.REPLAY[0] == 0:  # add default arg, if no replay start time is set in the cmdline
        time_delta = fileutils.get_timedelta(fileutils.parse_time_input(config["replay_last"]))
        some_time_ago = datetime.datetime.now() - time_delta
        args.REPLAY[0] = some_time_ago.strftime("%Y%m%dT%H%M%S")
    if len(args.REPLAY) == 1:  # add default arg, if no replay duration is set in the cmdline
        args.REPLAY.append("2m")

    time_delta = fileutils.get_timedelta(fileutils.parse_time_input(args.REPLAY[1]))
    watch_start_datetime_obj = get_date_obj_from_str(args.REPLAY[0], datetime.datetime.now())
    REC_FP_ALL = _savepath + "/*_partview*.out"
    rec_files = glob.iglob(REC_FP_ALL)
    useful_frames: List[str] = []

    for rec_file in rec_files:
        rec_file_last_modified_date = datetime.datetime.strptime(rec_file.rsplit("/", 1)[-1].split("_")[2], "%Y%m%dT%H%M%S")
        if datetime.timedelta(seconds=0) < rec_file_last_modified_date - watch_start_datetime_obj < time_delta:
            useful_frames.append(rec_file)

    return iter(useful_frames[::-1]), args.REPLAY


class NoSchedulerFound(Exception):
    """No scheduler was given and none could be detected."""

    def __init__(self) -> None:
        msg = "No suitable scheduler was found. Please define one in a switch or env variable or in %s.\nFor more help, try ./qtop.py --help\nLog file created in %s" % (
            QTOPCONF_YAML,
            os.path.expandvars(QTOP_LOGFILE),
        )
        Exception.__init__(self, msg)
        logging.critical(msg)


class SchedulerNotSpecified(Exception):
    """Auto-detection found no scheduler."""


class InvalidScheduler(Exception):
    """The requested scheduler is not one qtop supports."""


def cli_error_message(error: BaseException) -> str:
    """Return the message the CLI prints for an expected error, or an empty one."""
    if isinstance(error, SchedulerNotSpecified):
        return "No scheduler could be auto-detected. Select one with -b/--batchSystem, QTOP_SCHEDULER, or qtopconf.yaml."
    if isinstance(error, NoSchedulerFound):
        return ""
    return str(error)


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


def MVU_collect_snapshot(args: Any, config: Dict[str, Any], backend: Any, trace_paths: Dict[str, str], archive: Any) -> Tuple[Snapshot, Dict[str, Any]]:
    """Collect one normalized snapshot, including replay and masking."""
    scheduler = backend.get_mnemonic()
    savepath = config["savepath"]
    scheduling_system: "state_types.Scheduler"
    try:
        if args.load_sample:
            cluster_state = args.sample_snapshot["state"]
        elif args.SAMPLE and args.ANONYMIZE:
            # Collect originals only in memory so all backends share one mapping.
            capture_args = copy.copy(args)
            capture_args.ANONYMIZE = False
            scheduling_system = backend(trace_paths, config, capture_args)
            cluster_state = scheduling_system.get_cluster_state(capture_args)
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
            scheduling_system = backend(trace_paths, config, args)
            cluster_state = scheduling_system.get_cluster_state(args)
    except (ValueError, UnicodeError, ParseError) as error:
        raise OperationalError("Cannot parse %s scheduler input in %s: %s" % (scheduler, args.workdir, error)) from error
    return Snapshot.from_cluster_state(cluster_state), config


def MVU_export_snapshot(snapshot: Snapshot, args: Any, savepath: str, timestr: str, web: Web) -> None:
    """Write the snapshot as JSON for -E or the web view, and point the web server at it."""
    if not (args.EXPORT or args.WEB):
        return
    with tempfile.NamedTemporaryFile(delete=False, prefix="qtop_json_%s_" % timestr, suffix=".json", dir=savepath) as json_file:
        json_filename = json_file.name
    saved = False
    try:
        snapshot.save(json_filename)
        saved = True
    finally:
        if not saved:
            with contextlib.suppress(OSError):
                os.unlink(json_filename)
    if args.WEB:
        web.set_filename(json_filename)


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
        message = cli_error_message(error)
        if message:
            sys.stderr.write("%s\n" % message)
        return 1
    except (OperationalError, OSError) as error:
        report_operational_error(error, debug=bool(getattr(getattr(resources, "options", None), "DEBUG", False)))
        return 1


CONFIG_ERRORS = (ValueError, TypeError, KeyError, IndexError, AttributeError, StopIteration)

# Locations fixed for the whole run: launch directory, package/source root and help file.
RunPaths = NamedTuple("RunPaths", [("initial_cwd", str), ("curpath", str), ("qtoppath", str), ("help_fp", str)])


def _parse_run_options(resources: RunResources) -> Tuple[Any, Dict[str, Any]]:
    """Parse and validate the command line, including a --load-sample snapshot's display options."""
    args = resources.options = utils.parse_qtop_cmdline_args()
    if args.version:
        print("qtop current version: " + __version__)
        sys.exit(0)
    utils.init_logging(args)
    resources.logfile = QTOP_LOGFILE
    dynamic_config: Dict[str, Any] = dict()
    args, dynamic_config["force_names"] = process_args(args)
    require_experimental_opt_in(args)
    if (args.raw_companion or args.masked_traces) and not (args.SAMPLE and args.ANONYMIZE):
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
    logging.debug("Initial qtop directory: %s" % initial_cwd)
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
    trace_paths = {} if args.load_sample else fetch_scheduler_files(args, config, backend)
    sample_filename = fileutils.get_sample_filename(sample_filename, config)
    resources.start_capture(savepath, sample_filename, trace_paths, config, paths.qtoppath)
    snapshot, config = MVU_collect_snapshot(args, config, backend, trace_paths, resources.archive)
    if args.SAMPLE and args.ANONYMIZE:
        user_to_color = user_to_color_default.copy()
        for rule in config["user_color_mappings"]:
            user_to_color.update(rule)
    return snapshot, config, backend, user_to_color, sample_filename


def main(resources: RunResources) -> None:
    """Run qtop: collect, render and, in watch mode, repeat until quit."""
    args, dynamic_config = _parse_run_options(resources)

    available_batch_systems = discover_qtop_batch_systems()

    stdout = resources.stdout

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
    with raw_mode(sys.stdin, args) as (old_attrs, new_attrs):  # single-key input; the menus switch back to old_attrs
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
                    viewport.set_term_size(*calculate_term_size(config, FALLBACK_TERMSIZE, viewport))
                with resources.frame(savepath, timestr) as output_fp:
                    snapshot, config, backend, user_to_color, SAMPLE_FILENAME = _collect_frame(
                        resources, args, config, user_to_color, available_batch_systems, savepath, paths, SAMPLE_FILENAME
                    )
                    MVU_export_snapshot(snapshot, args, savepath, timestr, web)
                    if model is None:
                        model = MVU_Model(config, args, viewport, display_overrides=dynamic_config, package_path=paths.qtoppath, help_file=paths.help_fp)
                    model, _ = MVU_update(model, "snapshot", (snapshot, config, args, backend, user_to_color, nodestate_to_color))
                    view_data = MVU_query(model)
                    cluster = view_data.cluster
                    MVU_resolve_user_details(view_data.model, view_data.occupancy)
                    display = MVU_view(view_data, savepath, SAMPLE_FILENAME, QTOP_LOGFILE)

                view_data.model.viewport.max_height, max_line_len = get_output_size(max_line_len, output_fp)
                model, _ = MVU_update(model, "layout", view_data.model.viewport)
                viewport = model.viewport

                if args.ONLYSAVETOFILE:  # no display of qtop output, will exit
                    break
                if not args.WATCH:  # one-off display of qtop output, will exit afterwards (no --watch cmdline switch)
                    cat_command = ["/bin/cat", output_fp]  # not clearing the screen beforehand is the intended behaviour here
                    _ = subprocess.call(cat_command, stdout=stdout, stderr=stdout)
                    break
                # --watch
                if args.REPLAY:
                    try:
                        output_partview_fp = next(useful_frames)
                    except StopIteration:
                        logging.critical("No (more) recorded instances available to show! Exiting...")
                        break
                else:
                    output_partview_fp = display.show_part_view(timestr, file=dynamic_config.get("output_fp", output_fp), x=viewport.v_start, y=viewport.v_term_size)
                    logging.debug("dynamic_config filename in main loop: %s" % dynamic_config.get("output_fp", output_fp))
                clear_command = ["/usr/bin/clear"]
                cat_command = ["/bin/cat", output_partview_fp]
                _ = subprocess.call(clear_command, stdout=stdout, stderr=stdout)
                _ = subprocess.call(cat_command, stdout=stdout, stderr=stdout)

                read_char = wait_for_keypress_or_autorefresh(viewport, config, FALLBACK_TERMSIZE, int(args.WATCH) or KEYPRESS_TIMEOUT)
                model, effects = MVU_update(model, read_char)
                viewport, dynamic_config = model.viewport, model.display_overrides
                MVU_effects(model, effects, cluster, old_attrs, new_attrs)

                os.chdir(paths.qtoppath)
                resources.discard_frame()
                fileutils.deprecate_old_output_files(config)

        except (KeyboardInterrupt, EOFError):
            if not (args.SAMPLE and args.ANONYMIZE):
                print("\nExiting. Thank you for ...watching ;)\n")
            raise SystemExit(0) from None


if __name__ == "__main__":
    sys.exit(cli_main())
