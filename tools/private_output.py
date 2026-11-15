## Copyright (c) 2026 qtop contributors
## SPDX-License-Identifier: MIT
"""Output directories for the sample-render tools that other local users cannot pre-create or redirect (CWE-377)."""

import contextlib
import os
import stat
import tempfile
from pathlib import Path


def private_output_dir(path):
    """Return ``path`` as a directory only this user controls.

    A missing directory is created with mode 0700. An existing one is accepted only if it is a real
    directory (not a symlink), owned by the current user and not group/world-writable; otherwise
    ``ValueError`` is raised, before anything is written into it. Ownership checks need POSIX.
    """
    path = Path(path)
    # Check every existing ancestor before mkdir can follow a planted link.
    for parent in (path,) + tuple(path.parents):
        if parent.is_symlink():
            raise ValueError("output directory %s has a symlink component" % path)
    if path.exists() and not path.is_dir():
        raise ValueError("output path %s exists and is not a directory" % path)
    path.mkdir(mode=0o700, parents=True, exist_ok=True)
    info = os.lstat(str(path))
    if stat.S_ISLNK(info.st_mode):
        raise ValueError("output directory %s is a symlink; refusing to write through it" % path)
    if not stat.S_ISDIR(info.st_mode):
        raise ValueError("output path %s exists and is not a directory" % path)
    if os.name == "posix":
        if info.st_uid != os.geteuid():
            raise ValueError("output directory %s is owned by another user" % path)
        if info.st_mode & (stat.S_IWGRP | stat.S_IWOTH):
            raise ValueError("output directory %s is group- or world-writable" % path)
    return path


def private_output_path(path):
    """Validate a report destination without following directory or file links."""
    path = Path(path)
    private_output_dir(path.parent)
    if path.is_symlink() or (path.exists() and not path.is_file()):
        raise ValueError("output file %s must be a regular file, not a symlink" % path)
    return path


def write_private_file(path, content):
    """Atomically replace a report without writing through existing hard links."""
    path = private_output_path(path)
    mode = "wb" if isinstance(content, bytes) else "w"
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(mode=mode, encoding=None if mode == "wb" else "utf-8", dir=str(path.parent), delete=False) as output:
            temporary = output.name
            output.write(content)
        os.replace(temporary, str(path))
    finally:
        if temporary is not None:
            with contextlib.suppress(FileNotFoundError):
                os.unlink(temporary)
