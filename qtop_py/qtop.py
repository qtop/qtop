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

import contextlib
import copy
import datetime
import glob
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
from collections import OrderedDict
from functools import lru_cache
from math import isfinite
from signal import SIG_DFL, signal
from types import ModuleType, SimpleNamespace
from typing import TYPE_CHECKING, Any, Dict, Generator, Iterable, Iterator, List, Mapping, Optional, Sequence, Set, Tuple, Type, Union
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
termios: Optional[ModuleType]
try:
    import termios
except ImportError:
    termios = None

# Runtime context bound by main() through `global`; declared (not assigned) for type checkers.
args: Any
cluster: Any
scheduler: Any
snapshot: Any
user_to_color: Dict[str, str]
viewport: Viewport
web: Web
CURPATH: str
HELP_FP: str
QTOPPATH: str
SAMPLE_FILENAME: str

Document = Snapshot  # deprecated alias, kept for one release; use Snapshot

colorize_nodestate = MVU_colorize_nodestate

queue_to_color = colormap_queue_to_color  # re-exported; tools/validate_trace_export.py reads qtop.queue_to_color

compress_colored_line = MVU_compress_colored_line

gauge_core_vectors = MVU_gauge_core_vectors

keep_queue_initials_only_and_colorize = MVU_keep_queue_initials_only_and_colorize


def MVU_legacy_model(instance: Any = None) -> MVU_Model:
    """Compatibility boundary for older qtop imports; new pipeline stages never use it."""
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
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        if kwargs.get("MVU_model") is None:
            kwargs["MVU_model"] = MVU_legacy_model(self)
        super().__init__(*args, **kwargs)
        for name in ("config", "args", "snapshot", "viewport"):
            if hasattr(self, name):
                setattr(self.MVU_model, name, getattr(self, name))

    @property
    def MVU_model(self) -> Any:
        model = self.__dict__.get("_MVU_model")
        return MVU_legacy_model(self) if model is None else model

    @MVU_model.setter
    def MVU_model(self, model: Any) -> None:
        self._MVU_model = model


class Cluster(MVU_LegacyAdapter, MVU_Cluster):
    pass


class WNOccupancy(MVU_LegacyAdapter, MVU_WNOccupancy):
    def __init__(self, cluster: Any, config: Dict[str, Any], snapshot: Any, user_to_color: Dict[str, str], job_ids: Any = None, **kwargs: Any) -> None:
        super().__init__(cluster, config, snapshot, user_to_color, **kwargs)

    def strict_check_jobs(self, cluster: Any) -> None:
        occupancy = copy.copy(self)
        occupancy.MVU_model = MVU_legacy_model(self)
        message = MVU_WNOccupancy.MVU_check_jobs(occupancy, cluster)
        if message:
            print(message)


class TextDisplay(MVU_LegacyAdapter, MVU_TextDisplay):
    def display_user_accounts_pool_mappings(self, wns_occupancy: Any) -> None:
        self.MVU_model = self.MVU_model
        MVU_resolve_user_details(self.MVU_model, wns_occupancy, force=True)
        return super().display_user_accounts_pool_mappings(wns_occupancy)


def colorize(
    text: Any, color_func: Optional[str] = None, pattern: str = "NoPattern", mapping: Optional[Mapping[str, str]] = None, bg_color: Optional[str] = None, bold: bool = False
) -> Any:
    return MVU_colorize(MVU_legacy_model(), text, color_func, pattern, mapping, bg_color, bold)


def MVU_resolve_user_details(model: Any, occupancy: Any, force: bool = False) -> None:
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
            choose_sort(model.viewport, model.display_overrides, old_attrs, new_attrs)
        elif action == "filter":
            choose_filter(model.viewport, cluster, model.display_overrides, old_attrs, new_attrs)
        elif action == "highlight":
            choose_highlight(model.viewport, cluster, model.display_overrides, old_attrs, new_attrs)


def control_qtop(viewport: Viewport, read_char: str, cluster: Any, old_attrs: Any, new_attrs: Any) -> None:
    global dynamic_config
    model = MVU_legacy_model()
    model.viewport = viewport
    next_model, effects = MVU_update(model, read_char)
    viewport.__dict__.update(next_model.viewport.__dict__)
    next_model.viewport = viewport
    dynamic_config = next_model.display_overrides
    MVU_effects(next_model, effects, cluster, old_attrs, new_attrs)


def reset_sigpipe() -> None:
    if SIGPIPE is not None:
        signal(SIGPIPE, SIG_DFL)


def _disable_terminal_echo(attrs: Any) -> None:
    """Switch a termios attribute list to non-echoing, non-canonical mode."""
    if termios is not None:
        attrs[3] &= ~(termios.ECHO | termios.ICANON)


def _set_terminal_attrs(attrs: Any) -> None:
    """Apply terminal attributes when the platform exposes a real stdin TTY."""
    if termios is not None and sys.__stdin__ is not None:
        termios.tcsetattr(sys.__stdin__.fileno(), termios.TCSADRAIN, attrs)


def scheduler_backend(name: Optional[str]) -> Type[GenericBatchSystem]:
    return next((backend for backend in PLUGIN_BATCH_SYSTEMS if backend.get_mnemonic() == name), GenericBatchSystem)


def node_job_ids(node: Mapping[str, Any]) -> Set[str]:
    jobs = set(node["core_job_map"].values())
    for backend in PLUGIN_BATCH_SYSTEMS:
        for field in backend.node_job_fields:
            jobs.update(node.get(field, []))
    return jobs


def queue_is_blocked(state: object, scheduler_name: Optional[str] = None) -> bool:
    return scheduler_backend(scheduler_name).queue_is_blocked(state)


def extract_regex_detail(regex: Optional[str], field: str) -> str:
    expression = (regex or "").strip()
    if not expression:
        return field.strip()

    match = re.match(r"^re\.search\((?P<quote>['\"])(?P<pattern>.*)(?P=quote),\s*field\)\.group\((?P<group>\d+)\)$", expression, re.DOTALL)
    if not match:
        raise ValueError("Unsupported user detail regex expression in %s" % QTOPCONF_YAML)

    found = re.search(match.group("pattern"), field)
    if found is None:
        raise ValueError("User detail regex did not match field")
    return found.group(int(match.group("group")))


def get_date_obj_from_str(s: str, now: datetime.datetime) -> datetime.datetime:
    """
    Expects string s to be in either of the following formats:
    yyyymmddTHHMMSS, e.g. 20161118T182300
    HHMM, e.g. 1823 (current day is implied)
    mmddTHHMM, e.g. 1118T1823 (current year is implied)
    If it's in format #3, the the current year is assumed.
    If it's in format #2, either the current or the previous day is assumed,
    depending on whether the time provided is future or past.
    Optional ":/-" separators are also accepted between pretty much anywhere.
    returns a datetime object
    """
    s = "".join([x for x in s if x not in ":/-"])
    if "T" in s and len(s) == 15:
        inp_datetime = datetime.datetime.strptime(s, "%Y%m%dT%H%M%S")
    elif len(s) == 4:
        _inp_datetime = datetime.datetime.strptime(s, "%H%M")
        _inp_datetime = now.replace(hour=_inp_datetime.hour, minute=_inp_datetime.minute, second=0)
        inp_datetime = _inp_datetime if now > _inp_datetime else _inp_datetime - datetime.timedelta(days=1)
    elif len(s) == 9:
        _inp_datetime = datetime.datetime.strptime(s, "%m%dT%H%M")
        inp_datetime = _inp_datetime.replace(year=now.year, second=0)
    else:
        raise ValueError("The datetime format provided is incorrect.\nTry one of the formats: yyyymmddTHHMMSS, HHMM, mmddTHHMM.")
    return inp_datetime


@contextlib.contextmanager
def raw_mode(file: Any) -> Generator[None, None, None]:
    """
    Simple key listener implementation
    Taken from http://stackoverflow.com/questions/11918999/key-listeners-in-python/11919074#11919074
    Exits program with ^C or ^D
    """
    if args.ONLYSAVETOFILE or termios is None:
        yield
    elif args.WATCH:
        try:
            old_attrs = termios.tcgetattr(file.fileno())
        except (AttributeError, OSError, termios.error):
            yield
        else:
            new_attrs = old_attrs[:]
            new_attrs[3] = new_attrs[3] & ~(termios.ECHO | termios.ICANON)
            try:
                termios.tcsetattr(file.fileno(), termios.TCSADRAIN, new_attrs)
                yield
            finally:
                termios.tcsetattr(file.fileno(), termios.TCSADRAIN, old_attrs)
    else:
        yield


def calculate_term_size(config: Dict[str, Any], FALLBACK_TERM_SIZE: Sequence[int], viewport: Viewport) -> Tuple[int, int]:
    """
    Gets the dimensions of the terminal window where qtop will be displayed.

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
    """
    If the auto option is set in either env variable QTOP_SCHEDULER, QTOPCONF_YAML or in cmdline switch -b,
    qtop tries to determine which of the known batch commands are available in the current system.
    """
    signature_commands: Dict[str, str] = config["signature_commands"]
    prioritized_commands = sorted(signature_commands.items(), key=lambda item: backends.get(item[0], GenericBatchSystem).detection_priority)
    for system, batch_command in prioritized_commands:
        if system not in backends or backends[system].is_simulated or not shutil.which(batch_command):
            continue
        logging.debug("Auto-detected scheduler: %s", system)
        return system

    raise SchedulerNotSpecified


def execute_shell_batch_commands(batch_system_commands: Mapping[str, str], filenames: Dict[str, str], _file: str, _savepath: str, timeout: float = 30) -> str:
    """
    scheduler-specific commands are invoked from the shell and their output is saved *atomically* to files,
    as defined by the user in QTOPCONF_YAML
    """
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
    """
    Reads file $HOME/.local/qtop/getent_passwd.txt or whatever is put in QTOPCONF_YAML
    and extracts the fullname of the users. This shall be printed in User Accounts
    and Pool Mappings.
    """
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
    """
    If the user didn't specify --via the -s switch-- a dir where ready-made data files already exist,
    the appropriate batch commands are executed, as indicated in QTOPCONF,
    and results are saved with the respective filenames.
    """

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
    if sys.version_info[0] != 3:
        raise OperationalError("Only Python 3 is supported.")


@contextlib.contextmanager
def menu_terminal(old_attrs: Any, new_attrs: Any) -> Generator[None, None, None]:
    _disable_terminal_echo(new_attrs)
    _set_terminal_attrs(old_attrs)
    try:
        yield
    finally:
        _set_terminal_attrs(new_attrs)


def choose_sort(viewport: Viewport, display_overrides: Dict[str, Any], old_attrs: Any, new_attrs: Any) -> None:
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


def choose_filter(viewport: Viewport, cluster: Any, display_overrides: Dict[str, Any], old_attrs: Any, new_attrs: Any) -> None:
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


def choose_highlight(viewport: Viewport, cluster: Any, display_overrides: Dict[str, Any], old_attrs: Any, new_attrs: Any) -> None:
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
    """
    Qtop first checks in cmdline switches, environmental variables and the config files, in this order,
    for the scheduler type. If it's not indicated and "auto" is, it will attempt to guess the scheduler type
    from the scheduler shell commands available in the linux system.
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
    """
    Returns the char dimensions of the entirety of the qtop output file
    """
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
    if config["faster_xml_parsing"]:
        try:
            from lxml import etree
        except ImportError:
            logging.warning('Module lxml is missing. Try issuing "pip install lxml". Reverting to xml module.')
            from xml.etree import ElementTree as etree  # noqa: F401


def init_dirs(args: Any, _savepath: str) -> Any:
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


def wait_for_keypress_or_autorefresh(viewport: Viewport, FALLBACK_TERMSIZE: Sequence[int], KEYPRESS_TIMEOUT: float = 1) -> str:
    """
    This will make qtop wait for user input for a while,
    otherwise it will auto-refresh the display
    """
    while sys.stdin in select.select([sys.stdin], [], [], KEYPRESS_TIMEOUT)[0]:
        _read_char = sys.stdin.read(1)
        if _read_char:
            logging.debug("Pressed %s" % _read_char)
            break
    else:
        state = viewport.get_term_size()
        viewport.set_term_size(*calculate_term_size(config, FALLBACK_TERMSIZE, viewport))
        new_state = viewport.get_term_size()
        _read_char = "\n" if (state == new_state) else "r"
        logging.debug("Auto-advancing by pressing <Enter>")

    return _read_char


def assign_color_to_each_qname(worker_nodes: Iterable[Dict[str, Any]]) -> None:
    for worker_node in worker_nodes:
        worker_node["qname"] = [q[0] for q in worker_node["qname"]]


def discover_qtop_batch_systems() -> Dict[str, Any]:
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
    if args.COLOR == "AUTO":
        args.COLOR = "ON" if (os.environ.get("QTOP_COLOR", sys.stdout.isatty()) in ("ON", True)) else "OFF"
    logging.debug("args.COLOR is now set to: %s" % args.COLOR)
    args.REMAP = False  # Default value
    NAMED_WNS = 1 if args.FORCE_NAMES else 0
    return args, NAMED_WNS


def require_experimental_opt_in(args: Any) -> None:
    requested_features = tuple(
        name
        for enabled, name in (
            (args.ANONYMIZE, "Anonymization"),
            (args.WEB, "The web interface"),
        )
        if enabled
    )
    if requested_features and not args.EXPERIMENTAL:
        print("%s requires --experimental (-e). Exiting..." % " and ".join(requested_features))
        raise SystemExit(1)


def pick_frames_to_replay(_savepath: str) -> Tuple[Iterator[str], Any]:
    """
    getting the respective info from cmdline switch -R,
    pick the relevant qtop output from savepath to replay
    """
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
    def __init__(self) -> None:
        msg = "No suitable scheduler was found. Please define one in a switch or env variable or in %s.\nFor more help, try ./qtop.py --help\nLog file created in %s" % (
            QTOPCONF_YAML,
            os.path.expandvars(QTOP_LOGFILE),
        )
        Exception.__init__(self, msg)
        logging.critical(msg)


class SchedulerNotSpecified(Exception):
    pass


class InvalidScheduler(Exception):
    pass


def cli_error_message(error: BaseException) -> str:
    if isinstance(error, SchedulerNotSpecified):
        return "No scheduler could be auto-detected. Select one with -b/--batchSystem, QTOP_SCHEDULER, or qtopconf.yaml."
    if isinstance(error, NoSchedulerFound):
        return ""
    return str(error)


def report_operational_error(error: BaseException) -> None:
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
    if getattr(globals().get("args"), "DEBUG", False):
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
        report_operational_error(error)
        return 1


def main(resources: RunResources) -> None:
    # define global vars which are used out of scope
    global CURPATH, QTOPPATH, HELP_FP, SAMPLE_FILENAME, scheduler, config, dynamic_config, cluster, viewport, user_to_color, args, web, snapshot

    args = resources.options = utils.parse_qtop_cmdline_args()
    if args.version:
        print("qtop current version: " + __version__)
        sys.exit(0)
    utils.init_logging(args)
    resources.logfile = QTOP_LOGFILE
    dynamic_config = dict()
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
    if args.WATCH or args.REPLAY:  # this is needed for the filtering/sorting options
        if termios is None:
            old_attrs = ""
            new_attrs = ""
        else:
            try:
                old_attrs = termios.tcgetattr(0)
            except termios.error:
                old_attrs = ""
            new_attrs = old_attrs[:]

    available_batch_systems = discover_qtop_batch_systems()

    stdout = resources.stdout

    viewport = Viewport()  # controls the part of the qtop matrix shown on screen
    max_line_len = 0

    check_python_version()
    initial_cwd = os.getcwd()
    logging.debug("Initial qtop directory: %s" % initial_cwd)
    CURPATH = os.path.expanduser(initial_cwd)  # where qtop was invoked from
    package_path = os.path.dirname(os.path.realpath(__file__))
    source_path = os.path.dirname(package_path)
    # Source checkouts keep these user-facing files at repository root. Wheels
    # copy them into the package so an installed CLI has the same defaults.
    QTOPPATH = source_path if os.path.isfile(os.path.join(source_path, QTOPCONF_YAML)) else package_path
    HELP_FP = os.path.join(QTOPPATH, "helpfile.txt")
    SAMPLE_FILENAME = "qtop_sample_${USER}%(datetime)s.tar"
    SAMPLE_FILENAME = "qtop_sample_anonymous%(datetime)s.tar" if args.ANONYMIZE else os.path.expandvars(SAMPLE_FILENAME)
    if args.REPLAY:
        args.WATCH = 2  # enforce that --watch mode is on, even if not in cmdline switch
        args.BATCH_SYSTEM = next(name for name, backend in available_batch_systems.items() if backend.is_simulated)
        try:
            config, _, _ = configuration.load_yaml_config(args, QTOPPATH, CURPATH, SYSTEMCONFDIR, USERPATH)
        except (ValueError, TypeError, KeyError, IndexError, AttributeError, StopIteration) as error:
            raise OperationalError("Invalid configuration in %s: %s" % (args.CONFFILE or QTOPCONF_YAML, error or type(error).__name__)) from error
        useful_frames, args.REPLAY = pick_frames_to_replay(config["savepath"])

    model = None
    web = resources.web = Web(initial_cwd)
    with raw_mode(sys.stdin):  # key listener implementation
        try:
            while True:
                try:
                    config, user_to_color, nodestate_to_color = configuration.load_yaml_config(args, QTOPPATH, CURPATH, SYSTEMCONFDIR, USERPATH)
                except (ValueError, TypeError, KeyError, IndexError, AttributeError, StopIteration) as error:
                    raise OperationalError("Invalid configuration in %s: %s" % (args.CONFFILE or QTOPCONF_YAML, error or type(error).__name__)) from error
                if args.WEB and not web.started:
                    web.host = config.get("experimental_web_host", "127.0.0.1")
                    web.port = int(config.get("experimental_web_port", 8080))
                    web.start()
                    logging.warning("Experimental web interface at http://%s:%s (no authentication or TLS).", web.host, web.port)
                savepath = config["savepath"]
                timestr = time.strftime("%Y%m%dT%H%M%S")
                attempt_faster_xml_parsing(config)
                args = init_dirs(args, savepath)

                if args.LESS:
                    viewport.set_term_size(500, 9999)
                else:
                    viewport.set_term_size(*calculate_term_size(config, FALLBACK_TERMSIZE, viewport))
                with resources.frame(savepath, timestr) as output_fp:
                    scheduler = (
                        args.sample_snapshot["scheduler"]
                        if args.load_sample
                        else decide_batch_system(args.BATCH_SYSTEM, os.environ.get("QTOP_SCHEDULER"), config["scheduler"], config["schedulers"], available_batch_systems, config)
                    )
                    backend = available_batch_systems[scheduler]
                    trace_paths = {} if args.load_sample else fetch_scheduler_files(args, config, backend)
                    SAMPLE_FILENAME = fileutils.get_sample_filename(SAMPLE_FILENAME, config)
                    resources.start_capture(savepath, SAMPLE_FILENAME, trace_paths, config, QTOPPATH)
                    snapshot, config = MVU_collect_snapshot(args, config, backend, trace_paths, resources.archive)
                    if args.SAMPLE and args.ANONYMIZE:
                        user_to_color = user_to_color_default.copy()
                        for rule in config["user_color_mappings"]:
                            user_to_color.update(rule)
                    MVU_export_snapshot(snapshot, args, savepath, timestr, web)
                    if model is None:
                        model = MVU_Model(config, args, viewport, display_overrides=dynamic_config, package_path=QTOPPATH, help_file=HELP_FP)
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

                read_char = wait_for_keypress_or_autorefresh(viewport, FALLBACK_TERMSIZE, int(args.WATCH) or KEYPRESS_TIMEOUT)
                model, effects = MVU_update(model, read_char)
                viewport, dynamic_config = model.viewport, model.display_overrides
                MVU_effects(model, effects, cluster, old_attrs, new_attrs)

                os.chdir(QTOPPATH)
                resources.discard_frame()
                fileutils.deprecate_old_output_files(config)

        except (KeyboardInterrupt, EOFError):
            if not (args.SAMPLE and args.ANONYMIZE):
                print("\nExiting. Thank you for ...watching ;)\n")
            raise SystemExit(0) from None


if __name__ == "__main__":
    sys.exit(cli_main())
