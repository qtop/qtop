## Copyright (c) 2026 qtop contributors
## SPDX-License-Identifier: MIT
"""Bound subprocess output while draining both pipes and preserving deadlines."""

import contextlib
import io
import os
import signal
import subprocess
import threading
import time
from typing import IO, List, Optional, Tuple, cast

from qtop_py.errors import OperationalError

MAX_STDOUT_BYTES = 256 * 1024 * 1024
MAX_STDERR_BYTES = 1024 * 1024


def terminate(command: "subprocess.Popen[bytes]") -> None:
    """Kill a command and, on POSIX, its whole process group."""
    if os.name == "posix":
        with contextlib.suppress(ProcessLookupError):
            os.killpg(command.pid, signal.SIGKILL)
    else:
        command.kill()


def communicate_with_timeout(command: "subprocess.Popen[bytes]", timeout: float, stdout: Optional[IO[bytes]] = None) -> Tuple[Optional[bytes], bytes]:
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
        except Exception as error:
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
        command.wait(timeout=timeout)
        for reader in readers:
            reader.join(max(0, deadline - time.monotonic()))
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
