## Copyright (c) 2026 qtop contributors
## SPDX-License-Identifier: MIT
"""Select one coherent scheduler capture without extracting archive paths."""

import errno
import logging
import os
import re
import shutil
import stat
import tarfile
from collections import defaultdict
from pathlib import PurePosixPath
from typing import TYPE_CHECKING, DefaultDict, Dict, Iterable, List, Mapping, Tuple

if TYPE_CHECKING:  # typing.Final is Python 3.8+; the quoted annotations keep 3.6 working
    from typing import Final

logger = logging.getLogger(__name__)

MAX_MEMBERS: "Final" = 10000
MAX_TRACE_BYTES: "Final" = 256 * 1024 * 1024
MAX_SET_BYTES: "Final" = 512 * 1024 * 1024
MAX_ARCHIVE_BYTES: "Final" = 1024 * 1024 * 1024


def _open_archive_file(path: str) -> int:
    """Open the selected archive once, without following a final symlink; refuse anything but a regular file.

    A FIFO, device or directory passed by mistake (e.g. -s /dev/stdin) is rejected instead of read.
    """
    flags = os.O_RDONLY | os.O_NOFOLLOW | getattr(os, "O_NONBLOCK", 0) | getattr(os, "O_CLOEXEC", 0)
    descriptor = os.open(path, flags)  # NOSONAR: -s archive is the caller's chosen input, opened read-only, no symlink, regular file only; see docs/sonar-review.md
    try:
        if not stat.S_ISREG(os.fstat(descriptor).st_mode):
            raise ValueError("trace archive must be a regular file")
        if hasattr(os, "O_NONBLOCK"):
            os.set_blocking(descriptor, True)
        return descriptor
    except BaseException:
        os.close(descriptor)
        raise


def _open_private_workspace(directory: str) -> int:
    """Open qtop's trace workspace as a directory descriptor: no symlink, owned by us, mode 0700."""
    flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | getattr(os, "O_CLOEXEC", 0)
    descriptor = os.open(directory, flags)  # NOSONAR: qtop's own TemporaryDirectory in savepath; ownership and mode 0700 checked below; see docs/sonar-review.md
    info = os.fstat(descriptor)
    if info.st_uid != os.geteuid() or stat.S_IMODE(info.st_mode) & 0o077:
        os.close(descriptor)
        raise PermissionError(errno.EACCES, "trace workspace must be a private directory owned by the current user", directory)
    return descriptor


def _plain_name(name: str) -> str:
    """Return name if it is a single path component, else raise."""
    if not name or name in (".", "..") or os.sep in name or (os.altsep and os.altsep in name) or "\0" in name:
        raise ValueError("configured trace filename must be a plain file name: %r" % name)
    return name


def load_trace_archive(path: str, directory: str, configured: Mapping[str, str], optional: Iterable[str] = ()) -> Dict[str, str]:
    """Extract one coherent capture of the configured traces into directory; return their paths by key.

    The archive is opened once as a regular file. Each trace is created exclusively (O_EXCL, no symlink
    follow, mode 0600) by plain name inside directory, through a descriptor of that private directory.
    """
    marker = "QTOP_CAPTURE_SUFFIX"
    patterns = {}
    destinations = {}
    specificity = {}
    for key, specification in configured.items():
        template = specification.split(", ", 1)[0].strip()
        name = os.path.basename(template % {"savepath": directory, "pid": marker})
        patterns[key] = re.compile("^" + re.escape(name).replace(marker, "(?P<capture>.*)") + "$")
        destinations[key] = _plain_name(name.replace(marker, ""))
        specificity[key] = len(name.replace(marker, ""))
    if len(set(destinations.values())) != len(destinations):
        raise ValueError("configured trace filenames must be distinct")
    required = set(configured) - set(optional)
    workspace = _open_private_workspace(directory)
    try:
        with os.fdopen(_open_archive_file(path), "rb") as archive_file, tarfile.open(fileobj=archive_file, mode="r:*") as archive:
            return _extract_selected(archive, directory, workspace, patterns, destinations, specificity, required)
    finally:
        os.close(workspace)


def _extract_selected(
    archive: tarfile.TarFile,
    directory: str,
    workspace: int,
    patterns: Mapping[str, "re.Pattern[str]"],
    destinations: Mapping[str, str],
    specificity: Mapping[str, int],
    required: "set[str]",
) -> Dict[str, str]:
    """Pick the most complete unambiguous capture in archive and write it into the workspace descriptor."""
    cohorts: DefaultDict[Tuple[str, str], DefaultDict[str, List[tarfile.TarInfo]]] = defaultdict(lambda: defaultdict(list))
    for index, member in enumerate(archive):
        if index >= MAX_MEMBERS or member.offset_data + member.size > MAX_ARCHIVE_BYTES:
            raise ValueError("trace archive exceeds member or size limits")
        member_path = PurePosixPath(member.name)
        if (
            not member.isfile()
            or member.sparse is not None
            or member_path.is_absolute()
            or ".." in member_path.parts
            or "\\" in member.name
            or any(":" in part for part in member_path.parts)
        ):
            continue
        matches = []
        for key, pattern in patterns.items():
            match = pattern.fullmatch(member_path.name)
            if match or member_path.name == key:
                suffix = (match.groupdict().get("capture") or "") if match else ""
                matches.append((len(member_path.name) + 1 if member_path.name == key else specificity[key], key, suffix))
        if matches:
            _, key, suffix = max(matches)
            cohorts[(str(member_path.parent), suffix)][key].append(member)
    eligible = [(len(files), cohort, files) for cohort, files in cohorts.items() if required <= set(files) and all(len(members) == 1 for members in files.values())]
    if not eligible:
        raise ValueError("no unambiguous complete scheduler trace set; use --load-sample for normalized snapshots")
    _, cohort, selected = max(eligible, key=lambda candidate: (candidate[0], candidate[1]))
    members = {key: entries[0] for key, entries in selected.items()}
    if any(member.size > MAX_TRACE_BYTES for member in members.values()) or sum(member.size for member in members.values()) > MAX_SET_BYTES:
        raise ValueError("selected trace set exceeds size limits")
    logger.debug("Trace archive set: directory=%r capture suffix=%r", *cohort)
    paths = {}
    for key in sorted(members):
        member = members[key]
        name = destinations[key]
        source = archive.extractfile(member)
        if source is None:
            raise ValueError("selected trace data is missing")
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | getattr(os, "O_CLOEXEC", 0)
        with source, os.fdopen(os.open(name, flags, 0o600, dir_fd=workspace), "wb") as output:
            shutil.copyfileobj(source, output, length=64 * 1024)
        target = os.path.join(directory, name)
        logger.debug("Trace archive member: %s -> %s", member.name, target)
        paths[key] = target
    return paths
