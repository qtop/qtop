## Copyright (c) 2026 qtop contributors
## SPDX-License-Identifier: MIT
"""Shared boundaries for untrusted text and structured input (Python 3.6+)."""

import ast
import bz2
import contextlib
import gzip
import json
import logging
import lzma
import re
import tarfile
from typing import TYPE_CHECKING, Any, BinaryIO, Callable, Generator, Optional, cast

if TYPE_CHECKING:  # typing.Final is Python 3.8+; the quoted annotations keep 3.6 working
    from typing import Final

MAX_INPUT_BYTES: "Final" = 256 * 1024 * 1024
MAX_LITERAL_BYTES: "Final" = 1024 * 1024
MAX_DEPTH: "Final" = 64
MAX_ITEMS: "Final" = 2000000
MAX_ARCHIVE_BYTES: "Final" = 1024 * 1024 * 1024
MAX_ARCHIVE_MEMBERS: "Final" = 10000
# Core positions per node qtop will lay out; bounds range(np) loops and core matrices.
MAX_CORES_PER_NODE: "Final" = 65536
_CONTROLS = re.compile(r"[\x00-\x1f\x7f-\x9f\u00ad\u061c\u180e\u200b-\u200f\u2028-\u202e\u2060-\u206f\ud800-\udfff\ufeff\ufff9-\ufffb]")
_TOKENS = re.compile(r"""["'\\\[\]{}(),]""")


class ResourceLimitError(ValueError):
    """Input exceeded a documented resource budget."""


def cores_per_node(value: Any) -> Optional[int]:
    """Return a scheduler-reported per-node core count in 0..MAX_CORES_PER_NODE, or None if it is not one."""
    text = str(value).strip() if value is not None else ""
    if not re.fullmatch(r"[0-9]+", text) or int(text) > MAX_CORES_PER_NODE:
        return None
    return int(text)


def visible_text(value: Any, escape_backslashes: bool = True) -> str:
    """Escape controls and literal backslashes, keeping displayed identifiers distinct."""
    text = str(value)
    # Fast path: every character _CONTROLS matches is non-printable, so printable text needs no escaping.
    if text.isprintable() and not (escape_backslashes and "\\" in text):
        return text
    if escape_backslashes and "\\" in text:
        text = text.replace("\\", "\\\\")
    return _CONTROLS.sub(lambda match: ascii(match.group())[1:-1], text)


def rebuild(value: Any, leaf: Optional[Callable[[str], Any]] = None) -> Any:
    """Fresh copy of JSON-shaped data: new dicts and lists, `leaf` applied to every string (keys too).

    With no leaf it is a structural copy, several times faster than copy.deepcopy on this data shape.
    """
    if isinstance(value, str):
        return value if leaf is None else leaf(value)
    if isinstance(value, dict):
        return {rebuild(key, leaf): rebuild(item, leaf) for key, item in value.items()}
    if isinstance(value, list):
        return [rebuild(item, leaf) for item in value]
    return value


def visible_data(value: Any) -> Any:
    """Return a copy of value with every string made visible-safe; see visible_text."""
    return rebuild(value, visible_text)


class SafeLogFormatter(logging.Formatter):
    """Log formatter that escapes control characters in the rendered record."""

    def format(self, record: logging.LogRecord) -> str:
        # Escape the rendered record, including exception text, without changing shared records.
        """Format the record, then escape control characters in the result."""
        return visible_text(super().format(record), escape_backslashes=False)


def check_nesting(text: str) -> None:
    """Raise ResourceLimitError if JSON or literal text nests deeper than MAX_DEPTH or holds over MAX_ITEMS items."""
    depth = items = skip = 0
    quote = None
    for match in _TOKENS.finditer(text):
        position, token = match.start(), match.group()
        if position < skip:
            continue
        if quote:
            if token == "\\":
                skip = position + 2
            elif token == quote:
                quote = None
            continue
        if token in ('"', "'"):
            quote = token
            continue
        if token in ("[", "{", "("):
            depth += 1
        elif token in ("]", "}", ")"):
            depth -= 1
        elif token != ",":
            continue
        items += 1
        if depth > MAX_DEPTH or items > MAX_ITEMS:
            raise ResourceLimitError("structured input exceeds nesting or item limits")


def load_json(stream: Any, limit: Optional[int] = None) -> Any:
    """Parse JSON from a stream; raise ResourceLimitError over limit bytes (default MAX_INPUT_BYTES) or the nesting limits."""
    limit = MAX_INPUT_BYTES if limit is None else limit
    text = stream.read(limit + 1)
    if len(text if isinstance(text, bytes) else text.encode("utf-8")) > limit:
        raise ResourceLimitError("JSON input exceeds size limit")
    check_nesting(text.decode(json.detect_encoding(text)) if isinstance(text, bytes) else text)
    return json.loads(text)


def literal_eval(value: Any) -> Any:
    """ast.literal_eval within the size, nesting, parser and item limits; raise ResourceLimitError beyond them."""
    if isinstance(value, str):
        if len(value.encode("utf-8")) > MAX_LITERAL_BYTES:
            raise ResourceLimitError("literal exceeds size limit")
        check_nesting(value)
        try:
            value = ast.parse(value.strip(), mode="eval")
        except (RecursionError, MemoryError) as error:
            raise ResourceLimitError("literal exceeds parser limits") from error
        if sum(1 for _ in ast.walk(value)) > 100000:
            raise ResourceLimitError("literal exceeds item limit")
    return ast.literal_eval(value)


class LimitedReader(object):
    """File-like reader that raises ResourceLimitError once more than limit bytes are read."""

    def __init__(self, stream: Any, limit: int) -> None:
        self.stream, self.remaining = stream, limit

    def read(self, size: int = -1) -> bytes:
        """Read up to size bytes; raise ResourceLimitError once the limit is passed."""
        size = self.remaining + 1 if size < 0 else min(size, self.remaining + 1)
        data: bytes = self.stream.read(size)
        self.remaining -= len(data)
        if self.remaining < 0:
            raise ResourceLimitError("archive exceeds decompressed size limit")
        return data


@contextlib.contextmanager
def snapshot_archive(path: str) -> Generator[tarfile.TarFile, None, None]:
    """Open a plain, gzip, bzip2 or xz snapshot tarball as a stream, within MAX_ARCHIVE_BYTES."""
    with open(path, "rb") as source, contextlib.ExitStack() as stack:
        stream: Any = source  # becomes a decompressing reader below
        magic = stream.read(6)
        stream.seek(0)
        for prefix, decoder in ((b"\x1f\x8b", gzip.GzipFile), (b"BZh", bz2.BZ2File), (b"\xfd7zXZ", lzma.LZMAFile)):
            if magic.startswith(prefix):
                stream = stack.enter_context(decoder(fileobj=stream) if decoder is gzip.GzipFile else decoder(stream))
                break
        archive = stack.enter_context(tarfile.open(fileobj=cast(BinaryIO, LimitedReader(stream, MAX_ARCHIVE_BYTES)), mode="r|"))
        yield archive
