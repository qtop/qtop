## Copyright (c) 2026 qtop contributors
## SPDX-License-Identifier: MIT
import contextlib
import datetime
import errno
import glob
import io
import logging
import os
import stat
import sys
import tarfile
import tempfile
from typing import Any, Dict, Generator, List, Mapping, Optional, TextIO, Tuple


def _open_owned(path: str, flags: int, mode: int, directory_fd: int) -> int:
    try:
        return os.open(path, flags, mode, dir_fd=directory_fd)
    except PermissionError:
        info = os.stat(path, dir_fd=directory_fd, follow_symlinks=False)
        expected_type = stat.S_ISDIR(info.st_mode) if flags & os.O_DIRECTORY else stat.S_ISREG(info.st_mode) and info.st_nlink == 1
        if info.st_uid != os.geteuid() or not expected_type:
            raise
        os.chmod(path, mode, dir_fd=directory_fd, follow_symlinks=False)
        return os.open(path, flags, mode, dir_fd=directory_fd)


@contextlib.contextmanager
def _private_directory(path: str) -> Generator[Tuple[str, int], None, None]:
    """Create or tighten an owned output directory without following symlinks."""
    if not isinstance(path, str) or not path.strip():
        raise ValueError("Save directory must be a nonempty path")
    if os.name != "posix":
        raise OSError(errno.ENOTSUP, "Private save directories require POSIX ownership checks", path)
    path = os.path.abspath(os.path.expandvars(path))
    private_root = os.path.abspath(os.path.expanduser("~/.local/qtop"))
    if sys.platform == "darwin":
        for alias in ("/tmp", "/var"):  # nosec B108 - only compared, to resolve macOS root-owned aliases; nothing is created
            if path == alias or path.startswith(alias + os.sep):
                info = os.lstat(alias)
                # Expand only Apple's root-owned aliases; validate every target component below.
                if stat.S_ISLNK(info.st_mode) and info.st_uid == 0 and os.readlink(alias) in ("private" + alias, "/private" + alias):
                    path = "/private" + path
                    if private_root == alias or private_root.startswith(alias + os.sep):
                        private_root = "/private" + private_root
                break
    parts = path.strip(os.sep).split(os.sep)
    if not parts[0]:
        raise PermissionError(errno.EACCES, "The filesystem root cannot be a save directory", path)
    uid = os.geteuid()
    flags = os.O_DIRECTORY | os.O_NOFOLLOW
    parent_access = getattr(os, "O_PATH", os.O_RDONLY)
    fd = os.open(os.sep, flags | parent_access)
    try:
        for index, part in enumerate(parts):
            with contextlib.suppress(FileExistsError):
                os.mkdir(part, mode=0o700, dir_fd=fd)
            parent = os.path.join(os.sep, *parts[: index + 1])
            private_directory = index == len(parts) - 1 or parent == private_root or parent.startswith(private_root + os.sep)
            access = os.O_RDONLY if private_directory else parent_access
            child_fd = _open_owned(part, flags | access, 0o700, fd) if private_directory else os.open(part, flags | access, dir_fd=fd)
            os.close(fd)
            fd = child_fd
            info = os.fstat(fd)
            writable_by_others = info.st_mode & (stat.S_IWGRP | stat.S_IWOTH)
            # Repair owned output directories and qtop's own configuration/log tree.
            if private_directory:
                if info.st_uid != uid:
                    raise PermissionError(errno.EACCES, "Save directory must be owned by the current user", path)
                os.fchmod(fd, 0o700)
            elif writable_by_others and not info.st_mode & stat.S_ISVTX:
                raise PermissionError(errno.EACCES, "Save directory has an untrusted parent (writable without sticky bit)", parent)
        yield path, fd
    finally:
        os.close(fd)


def private_save_directory(path: str) -> str:
    """Create or tighten an owned output directory without following symlinks."""
    with _private_directory(path) as (directory, _):
        return directory


def private_log_stream(path: str) -> io.TextIOWrapper:
    """Open a user-owned, singly linked regular log through a verified directory FD."""
    directory, name = os.path.split(os.path.abspath(os.path.expandvars(path)))
    with _private_directory(directory) as (_, directory_fd), contextlib.ExitStack() as allocation:
        descriptor = _open_owned(name, os.O_WRONLY | os.O_CREAT | os.O_APPEND | os.O_NOFOLLOW | os.O_NONBLOCK, 0o600, directory_fd)
        allocation.callback(os.close, descriptor)
        info = os.fstat(descriptor)
        if not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid() or info.st_nlink != 1:
            raise PermissionError(errno.EACCES, "Log must be a user-owned, singly linked regular file", path)
        os.fchmod(descriptor, 0o600)
        stream = os.fdopen(descriptor, "a", encoding="utf-8")
        allocation.pop_all()
        return stream


def check_empty_file(orig_file: str) -> None:
    if os.path.getsize(orig_file) == 0:
        raise FileEmptyError(orig_file)


def get_new_temp_file(_savepath: str, suffix: str, prefix: str) -> Tuple[int, str]:
    """
    Using mkstemp instead of NamedTemporaryFile because a file descriptor
    is needed to redirect sys.stdout to.
    """
    fd, temp_filepath = tempfile.mkstemp(suffix=suffix, prefix=prefix, dir=_savepath)  # **kwargs
    logging.debug("temp_filepath: %s" % temp_filepath)
    # out_file = os.fdopen(fd, 'w')
    return fd, temp_filepath


def safe_exit_with_file_close(handle: int, name: str, stdout: TextIO, options: Any, _savepath: str, qtop_logfile: str, sample_filename: str, delete_file: bool = False) -> None:
    sys.stdout.write("\nExiting. Thank you for ...watching ;)\n")
    sys.stdout.flush()
    if sys.stdout is not stdout:
        sys.stdout.close()
    sys.stdout = stdout
    with contextlib.suppress(OSError):
        os.close(handle)
    if delete_file:
        os.unlink(name)  # this deletes the file
    # The caller finalizes the support archive in its finally block.
    sys.exit(0)


def init_sample_file(
    options: Any, _savepath: str, SAMPLE_FILENAME: str, scheduler_output_filenames: Mapping[str, str], QTOPCONF_YAML: str, QTOPPATH: str
) -> Optional[tarfile.TarFile]:
    """
    Initialise a support archive; -LL adds runtime assets and package sources.
    The caller adds rendered output, logs and eligible scheduler traces before closing it.
    """
    if options.SAMPLE < 1:
        return None

    # clears any preexisting tar files
    tar_out = tarfile.open(os.path.join(_savepath, SAMPLE_FILENAME), mode="w")
    with contextlib.ExitStack() as initialization:
        initialization.callback(tar_out.close)
        if options.SAMPLE >= 2:
            if not getattr(options, "ANONYMIZE", False):
                add_to_sample([os.path.join(os.path.realpath(QTOPPATH), QTOPCONF_YAML)], tar_out)
            source_path = os.path.join(os.path.realpath(QTOPPATH), "qtop_py")
            if not os.path.isdir(source_path):  # Installed defaults live inside the package.
                source_path = os.path.realpath(QTOPPATH)
            source_files = glob.glob(os.path.join(source_path, "**", "*.py"), recursive=True)
            for source_file in source_files:
                subdir = os.path.dirname(os.path.join("qtop_py", os.path.relpath(source_file, source_path)))
                add_to_sample([source_file], tar_out, subdir=subdir)
            help_path = os.path.join(os.path.realpath(QTOPPATH), "helpfile.txt")
            if os.path.isfile(help_path):
                add_to_sample([help_path], tar_out)
            for asset in glob.glob(os.path.join(source_path, "web", "**", "*"), recursive=True):
                if os.path.isfile(asset):
                    subdir = os.path.dirname(os.path.join("qtop_py", os.path.relpath(asset, source_path)))
                    add_to_sample([asset], tar_out, subdir=subdir)
            licenses = [os.path.join(os.path.realpath(QTOPPATH), "LICENSE")]
            licenses += glob.glob(os.path.join(os.path.dirname(source_path), "qtop*.dist-info", "licenses", "LICENSE"))
            licenses += glob.glob(os.path.join(os.path.dirname(source_path), "qtop*.dist-info", "LICENSE"))
            license_path = next((path for path in licenses if os.path.isfile(path)), None)
            if license_path:
                add_to_sample([license_path], tar_out)
            launcher = b"#!/usr/bin/env python3\nfrom qtop_py.qtop import cli_main\nraise SystemExit(cli_main())\n"
            member = tarfile.TarInfo("qtop")
            member.mode, member.size = 0o755, len(launcher)
            tar_out.addfile(member, io.BytesIO(launcher))
        initialization.pop_all()
    return tar_out


def _without_archive_owner(member: tarfile.TarInfo) -> tarfile.TarInfo:
    member.uid = member.gid = 0
    member.uname = member.gname = ""
    return member


def save_raw_companion(savepath: str, filenames: Mapping[str, str]) -> str:
    handle, path = tempfile.mkstemp(prefix="qtop_raw_traces_", suffix=".tar", dir=savepath)
    with os.fdopen(handle, "wb") as stream, tarfile.open(fileobj=stream, mode="w", dereference=True) as archive:
        for key, source in filenames.items():
            archive.add(source, arcname=key, recursive=False, filter=_without_archive_owner)
    return path


def add_to_sample(filepaths_to_add: List[str], sample_out: tarfile.TarFile, sample_method: Any = tarfile, subdir: Optional[str] = None) -> tarfile.TarFile:
    """
    opens sample_file in path savepath and adds files filepaths_to_add
    """
    if not isinstance(filepaths_to_add, list):
        raise TypeError("filepaths_to_add must be a list")
    for filepath_to_add in filepaths_to_add:
        fn = os.path.basename(filepath_to_add)
        try:
            logging.debug("Adding %s to sample..." % filepath_to_add)
            sample_out.add(filepath_to_add, arcname=fn if not subdir else os.path.join(subdir, fn), filter=_without_archive_owner)
        except (OSError, tarfile.TarError) as error:
            logging.error("Could not add %s to the sample archive; skipping it: %s", filepath_to_add, error)
    return sample_out


def get_sample_filename(SAMPLE_FILENAME: str, config: Mapping[str, Any]) -> str:
    if config["overwrite_sample_file"]:
        SAMPLE_FILENAME = SAMPLE_FILENAME % {"datetime": ""}
    else:
        SAMPLE_FILENAME = SAMPLE_FILENAME % {"datetime": "_" + datetime.datetime.now().strftime("%Y%m%d-%H%M%S")}
    return SAMPLE_FILENAME


class FileEmptyError(Exception):
    def __init__(self, fn: str) -> None:
        msg = "File %s is empty.\nIs your batch scheduler loaded with jobs?" % fn
        Exception.__init__(self, msg)
        logging.warning(msg)
        self.fn = fn


def deprecate_old_output_files(config: Mapping[str, Any]) -> None:
    """
    deletes older json and .out files in savepath directory.
    """
    time_alive = get_timedelta(parse_time_input(config["auto_delete_old_output_files_after"]))
    _savepath = config["savepath"]
    for f in os.listdir(_savepath):
        if (not f.endswith(("json", ".out"))) or f.endswith("rec.out"):
            continue
        curpath = os.path.join(_savepath, f)
        try:
            file_modified = datetime.datetime.fromtimestamp(os.path.getmtime(curpath))
            if datetime.datetime.now() - file_modified > time_alive:
                os.remove(curpath)
        except OSError as error:
            # Another qtop process may remove the same stale file after the
            # directory listing but before stat/remove. That race is benign.
            if error.errno != errno.ENOENT:
                raise


def get_timedelta(extra_kw_args: Mapping[str, int]) -> datetime.timedelta:
    """
    This solely exists to allow timedelta.timedelta's keyword argument (minutes/seconds/hours=...)
    to be selected with a variable.
    """
    return datetime.timedelta(**extra_kw_args)


def parse_time_input(_time: str) -> Dict[str, int]:
    """
    the func accepts a _time str in either (h)ours, (m)inutes, or (s)econds, using the respective suffix,
    e.g. '5h', or '10m', or '30s'
    A tuple is returned, e.g. (5, 'hours')
    """
    if not isinstance(_time, str) or not _time.endswith(("h", "m", "s")):
        raise ValueError("Time input must be a number followed by h, m or s")
    units = {"m": "minutes", "s": "seconds", "h": "hours"}
    try:
        return {units[_time[-1]]: int(_time[:-1])}
    except ValueError as error:
        raise ValueError("Time input must be a number followed by h, m or s") from error
