## Copyright (c) 2026 qtop contributors
## SPDX-License-Identifier: MIT
"""Bound subprocess output while draining both pipes and preserving deadlines."""

import contextlib
import io
import logging
import os
import shlex
import signal
import subprocess  # nosec B404 - scheduler commands run as argv lists, never a shell
import tempfile
import threading
import time
from math import isfinite
from typing import IO, TYPE_CHECKING, Any, Dict, List, Mapping, Optional, Tuple, cast

from qtop_py import fileutils
from qtop_py.constants import DEFAULT_SCHEDULER_COMMAND_TIMEOUT, QTOPCONF_YAML
from qtop_py.errors import OperationalError

if TYPE_CHECKING:  # typing.Final is Python 3.8+; the quoted annotations keep 3.6 working
    from typing import Final

MAX_STDOUT_BYTES: "Final" = 256 * 1024 * 1024
MAX_STDERR_BYTES: "Final" = 1024 * 1024


def terminate(command: "subprocess.Popen[bytes]") -> None:
    """Kill a command and, on POSIX, its whole process group."""
    if os.name == "posix":
        with contextlib.suppress(ProcessLookupError):
            os.killpg(command.pid, signal.SIGKILL)
    else:
        command.kill()


def communicate_with_timeout(
    command: "subprocess.Popen[bytes]", timeout: float, stdout: Optional[IO[bytes]] = None, cancel: Optional[threading.Event] = None
) -> Tuple[Optional[bytes], bytes]:
    """Drain binary pipes with fixed memory/write limits; kill and reap on failure."""
    output: IO[bytes] = stdout if stdout is not None else io.BytesIO()
    errors = io.BytesIO()
    failures: List[BaseException] = []

    def drain(stream: IO[bytes], target: IO[bytes], limit: int) -> None:
        try:
            with stream:
                remaining = limit
                while True:
                    chunk = os.read(stream.fileno(), min(65536, remaining + 1))
                    if not chunk:
                        break
                    if len(chunk) > remaining:
                        raise OperationalError("Scheduler command output exceeds %s-byte limit" % limit)
                    target.write(chunk)
                    remaining -= len(chunk)
        except Exception as error:  # noqa: BLE001 - re-raise reader failures on the caller thread after killing and reaping the child.
            failures.append(error)
            terminate(command)

    readers = [
        threading.Thread(target=drain, args=(stream, target, limit), daemon=True)
        for stream, target, limit in ((command.stdout, output, MAX_STDOUT_BYTES), (command.stderr, errors, MAX_STDERR_BYTES))
        if stream is not None
    ]
    deadline = time.monotonic() + timeout
    failed = True
    try:
        for reader in readers:
            reader.start()
        if cancel is None:
            command.wait(timeout=timeout)
        else:
            _wait_for_command(command, timeout, deadline, cancel)
        for reader in readers:
            if cancel is None:
                reader.join(max(0, deadline - time.monotonic()))
            else:
                while reader.is_alive() and time.monotonic() < deadline:
                    if cancel.is_set():
                        raise OperationalError("Scheduler collection cancelled")
                    reader.join(min(0.1, max(0, deadline - time.monotonic())))
        if any(reader.is_alive() for reader in readers):
            raise subprocess.TimeoutExpired(command.args, timeout)
        if failures:
            raise failures[0]
        failed = False
    finally:
        if failed:
            terminate(command)
            with contextlib.suppress(subprocess.TimeoutExpired):
                command.wait(timeout=1)
            for reader in readers:
                if reader.ident is not None:
                    reader.join(1)
    return (None if stdout is not None else cast(io.BytesIO, output).getvalue()), errors.getvalue()


def _wait_for_command(command: "subprocess.Popen[bytes]", timeout: float, deadline: float, cancel: threading.Event) -> None:
    """Wait against the original deadline while allowing collection shutdown."""
    while command.poll() is None:
        if cancel.is_set():
            raise OperationalError("Scheduler collection cancelled")
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise subprocess.TimeoutExpired(command.args, timeout)
        try:
            command.wait(timeout=min(0.1, remaining))
        except subprocess.TimeoutExpired:
            continue


def scheduler_env() -> Dict[str, str]:
    """Environment for scheduler-facing commands, so the output qtop parses is complete and not localised.

    LC_ALL=C keeps dates and messages in the C locale. SLURM_BITSTR_LEN=0 stops Slurm cutting
    array task ranges (e.g. 453906_[2,6-14,...]) at its default length (qtop/qtop#606); it
    overrides a user's own setting, which suits interactive squeue but breaks parsing.
    """
    env = dict(os.environ)
    env["LC_ALL"] = "C"
    env["SLURM_BITSTR_LEN"] = "0"
    return env


def execute_shell_batch_commands(
    batch_system_commands: Mapping[str, str],
    filenames: Dict[str, str],
    _file: str,
    _savepath: str,
    timeout: float = DEFAULT_SCHEDULER_COMMAND_TIMEOUT,
    cancel: Optional[threading.Event] = None,
    cwd: Optional[str] = None,
    input_options: Any = None,
) -> str:
    """Run the scheduler commands from QTOPCONF_YAML, saving each output atomically to its trace file."""
    _batch_system_command = batch_system_commands[_file].strip()
    destination = os.path.abspath(filenames[_file])
    save_directory = os.path.realpath(_savepath)
    try:
        contained = os.path.commonpath((save_directory, os.path.realpath(os.path.dirname(destination)))) == save_directory
    except ValueError:
        contained = False
    if not contained:
        raise OperationalError("Scheduler trace output must stay inside savepath")
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
            logging.debug("\tFile state before subprocess call: %(fin)s", {"fin": fin})
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
                command = subprocess.Popen(  # nosec B603 # nosemgrep - shlex-split qtopconf.yaml command, argv list, never a shell
                    command_args,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    start_new_session=(os.name == "posix"),
                    env=scheduler_env(),
                    cwd=cwd,
                )
            except OSError as error:
                raise OperationalError("Cannot execute scheduler command %r: %s. Check PATH or load the scheduler module." % (_batch_system_command, error.strerror)) from error
            except ValueError as error:
                raise OperationalError("Invalid scheduler command %r: %s. Check command quoting in %s." % (_batch_system_command, error, QTOPCONF_YAML)) from error
            with fileutils.input_source(input_options, destination):
                try:
                    stderr_data = communicate_with_timeout(command, timeout, stdout=fin, **({"cancel": cancel} if cancel is not None else {}))[1]
                except subprocess.TimeoutExpired as failure:
                    raise OperationalError("Scheduler command %r timed out after %g seconds" % (_batch_system_command, timeout)) from failure
            if stderr_data:
                logging.warning("A message from your shell: %s", stderr_data.decode("utf-8", errors="replace").strip())
            if command.returncode:
                detail = " ".join(stderr_data.decode("utf-8", errors="replace").splitlines())[:300] or "no diagnostic output"
                raise OperationalError("Scheduler command %r failed (exit %s): %s" % (_batch_system_command, command.returncode, detail))
            logging.debug("File state after subprocess call: %(fin)s", {"fin": fin})
        # Sonar path finding: destination already contained (checked against the real savepath above); see docs/sonar-review.md
        os.rename(tempname, filenames[_file])

        return filenames[_file]
    finally:
        if tempname is not None:
            with contextlib.suppress(OSError):
                os.unlink(tempname)
