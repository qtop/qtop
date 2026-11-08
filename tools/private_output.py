## Copyright (c) 2026 qtop contributors
## SPDX-License-Identifier: MIT
"""Output directories for the sample-render tools that other local users cannot pre-create or redirect (CWE-377)."""

import os
import stat
from pathlib import Path


def private_output_dir(path):
    """
    Return ``path`` as a directory only this user controls.

    A missing directory is created with mode 0700. An existing one is accepted only if it is a real
    directory (not a symlink), owned by the current user and not group/world-writable; otherwise
    ``ValueError`` is raised, before anything is written into it. Ownership checks need POSIX.
    """
    path = Path(path)
    try:
        path.mkdir(mode=0o700, parents=True)
    except FileExistsError:
        pass
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
