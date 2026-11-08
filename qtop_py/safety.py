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
from typing import TYPE_CHECKING, Any, BinaryIO, Generator, Optional, cast

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


def visible_text(value, escape_backslashes=True):
    """Escape controls and literal backslashes, keeping displayed identifiers distinct."""
    value = str(value)
    if escape_backslashes and "\\" in value:
        value = value.replace("\\", "\\\\")
    return _CONTROLS.sub(lambda match: ascii(match.group())[1:-1], value)


def visible_data(value: Any) -> Any:
    if isinstance(value, str):
        return visible_text(value)
    if isinstance(value, dict):
        return {visible_data(key): visible_data(item) for key, item in value.items()}
    if isinstance(value, list):
        return [visible_data(item) for item in value]
    return value


class SafeLogFormatter(logging.Formatter):
    def format(self, record):
        # Escape the rendered record, including exception text, without changing shared records.
        return visible_text(super().format(record), escape_backslashes=False)


def check_nesting(text):
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
    limit = MAX_INPUT_BYTES if limit is None else limit
    text = stream.read(limit + 1)
    if len(text if isinstance(text, bytes) else text.encode("utf-8")) > limit:
        raise ResourceLimitError("JSON input exceeds size limit")
    check_nesting(text.decode(json.detect_encoding(text)) if isinstance(text, bytes) else text)
    return json.loads(text)


def literal_eval(value):
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
    def __init__(self, stream, limit):
        self.stream, self.remaining = stream, limit

    def read(self, size=-1):
        size = self.remaining + 1 if size < 0 else min(size, self.remaining + 1)
        data = self.stream.read(size)
        self.remaining -= len(data)
        if self.remaining < 0:
            raise ResourceLimitError("archive exceeds decompressed size limit")
        return data


@contextlib.contextmanager
def snapshot_archive(path: str) -> Generator[tarfile.TarFile, None, None]:
    with open(path, "rb") as source, contextlib.ExitStack() as stack:
        stream = source
        magic = stream.read(6)
        stream.seek(0)
        for prefix, decoder in ((b"\x1f\x8b", gzip.GzipFile), (b"BZh", bz2.BZ2File), (b"\xfd7zXZ", lzma.LZMAFile)):
            if magic.startswith(prefix):
                stream = stack.enter_context(decoder(fileobj=stream) if decoder is gzip.GzipFile else decoder(stream))
                break
        archive = stack.enter_context(tarfile.open(fileobj=cast(BinaryIO, LimitedReader(stream, MAX_ARCHIVE_BYTES)), mode="r|"))
        yield archive
