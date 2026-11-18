## Copyright (c) 2026 qtop contributors
## SPDX-License-Identifier: MIT
"""Terminal modes, signal handling, geometry and refresh waits."""

import contextlib
import logging
import select
import shutil
import signal as _signal
import subprocess
import sys
from signal import SIG_DFL, signal
from types import ModuleType
from typing import Any, Dict, Generator, Optional, Sequence, Tuple, Union

from qtop_py.constants import QTOPCONF_YAML
from qtop_py.errors import OperationalError
from qtop_py.ui.controls import KeyboardInput
from qtop_py.ui.viewport import Viewport

SIGPIPE = getattr(_signal, "SIGPIPE", None)
SIGWINCH = getattr(_signal, "SIGWINCH", None)
termios: Optional[ModuleType]
try:
    import termios
except ImportError:
    termios = None


def reset_sigpipe() -> None:
    """Restore the default SIGPIPE action, so piping into head exits quietly."""
    if SIGPIPE is not None:
        signal(SIGPIPE, SIG_DFL)


def _set_terminal_attrs(attrs: Any) -> None:
    """Apply terminal attributes when the platform exposes a real stdin TTY."""
    if termios is not None and sys.__stdin__ is not None:
        termios.tcsetattr(sys.__stdin__.fileno(), termios.TCSADRAIN, attrs)


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


class TerminalResized(Exception):
    """SIGWINCH arrived while qtop waited for a key."""


def _raise_resized(_signum: int, _frame: Any) -> None:
    raise TerminalResized


def wait_for_keypress_or_autorefresh(
    viewport: Viewport, config: Dict[str, Any], FALLBACK_TERMSIZE: Sequence[int], KEYPRESS_TIMEOUT: float = 1, keyboard: Optional[KeyboardInput] = None
) -> str:
    """Return the key pressed within the refresh interval, or a newline to redraw: at its end, or at once on a resize (SIGWINCH)."""
    previous = _signal.signal(SIGWINCH, _raise_resized) if SIGWINCH is not None else None  # only while waiting, never mid-frame
    try:
        reader = keyboard or KeyboardInput(sys.stdin)
        if reader.pending or reader.stream in select.select([reader.stream], [], [], KEYPRESS_TIMEOUT)[0]:
            read_char = reader.read()
            logging.debug("Pressed %s", read_char)
            return read_char
    except TerminalResized:
        logging.debug("Terminal resized")
    finally:
        if SIGWINCH is not None:
            _signal.signal(SIGWINCH, previous)
    viewport.set_term_size(*calculate_term_size(config, FALLBACK_TERMSIZE, viewport))  # a resize just redraws ("r" cycles core lines)
    return "\n"
