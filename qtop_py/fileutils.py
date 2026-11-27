## Copyright (c) 2026 qtop contributors
## SPDX-License-Identifier: MIT
"""Output, temporary and support-archive file handling."""

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
from typing import Any, Callable, Dict, Generator, List, Mapping, Optional, Set, TextIO, Tuple, TypeVar

from qtop_py.core_safety import visible_text

logger = logging.getLogger(__name__)


class StartupProgress:
    """Report each distinct scheduler source once, until startup completes."""

    def __init__(self, output: TextIO) -> None:
        self.output: Optional[TextIO] = output
        self.seen: Set[str] = set()

    @contextlib.contextmanager
    def source(self, path: str) -> Generator[None, None, None]:
        """Print flushed dots at a source's opening and closing boundaries."""
        key = os.path.realpath(path)
        report = self.output is not None and key not in self.seen
        if report:
            self.seen.add(key)
            print(".", end="", file=self.output, flush=True)
        try:
            yield
        finally:
            if report and self.output is not None:
                print(".", end="", file=self.output, flush=True)

    def finish(self) -> None:
        """End the startup line and permanently disable further reporting."""
        if self.output is not None and self.seen:
            print(file=self.output, flush=True)
        self.output = None


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


_MAX_SYMLINK_HOPS = 40


def _site_parent_link(name: str, directory_fd: int) -> Optional[str]:
    """Return the target of a root-owned parent symlink in a directory others cannot write, else None."""
    try:
        link = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
    except OSError:
        return None
    if not stat.S_ISLNK(link.st_mode) or link.st_uid != 0 or os.fstat(directory_fd).st_mode & (stat.S_IWGRP | stat.S_IWOTH):
        return None
    return os.readlink(name, dir_fd=directory_fd)


def _path_parts(path: str) -> List[str]:
    parts = path.strip(os.sep).split(os.sep)
    if not parts[0]:
        raise PermissionError(errno.EACCES, "The filesystem root cannot be a save directory", path)
    return parts


@contextlib.contextmanager
def _private_directory(path: str, strict: bool = False) -> Generator[Tuple[str, int], None, None]:
    """Create or tighten an owned output directory without following symlinks.

    strict (--strict-parents) refuses a parent others can write without the sticky bit; off by default,
    since shared site trees (/gpfs/home, group-writable /home) would otherwise stop every run.
    """
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
    parts = _path_parts(path)
    uid = os.geteuid()
    flags = os.O_DIRECTORY | os.O_NOFOLLOW
    parent_access = getattr(os, "O_PATH", os.O_RDONLY)
    fd = os.open(os.sep, flags | parent_access)
    hops = 0
    try:
        index = 0
        while index < len(parts):
            part = parts[index]
            with contextlib.suppress(FileExistsError):
                os.mkdir(part, mode=0o700, dir_fd=fd)
            parent = os.path.join(os.sep, *parts[: index + 1])
            private_directory = index == len(parts) - 1 or parent == private_root or parent.startswith(private_root + os.sep)
            access = os.O_RDONLY if private_directory else parent_access
            try:
                child_fd = _open_owned(part, flags | access, 0o700, fd) if private_directory else os.open(part, flags | access, dir_fd=fd)
            except OSError as error:
                # Site layouts such as an autofs /home -> /nfs/home (#615): follow a parent link only if root
                # placed it where others cannot write; directories qtop owns are never followed.
                target = None
                if not private_directory and error.errno in (errno.ENOTDIR, errno.ELOOP):
                    target = _site_parent_link(part, fd)
                if target is None:
                    raise
                hops += 1
                if hops > _MAX_SYMLINK_HOPS:
                    raise OSError(errno.ELOOP, "Too many symbolic links in save directory path", path) from error
                resolved = os.path.normpath(os.path.join(os.path.dirname(parent), target))
                if private_root == parent or private_root.startswith(parent + os.sep):
                    private_root = resolved + private_root[len(parent) :]
                path = os.path.join(resolved, *parts[index + 1 :])
                parts = _path_parts(path)
                os.close(fd)
                fd = os.open(os.sep, flags | parent_access)
                index = 0
                continue
            index += 1
            os.close(fd)
            fd = child_fd
            info = os.fstat(fd)
            writable_by_others = info.st_mode & (stat.S_IWGRP | stat.S_IWOTH)
            # Repair owned output directories and qtop's own configuration/log tree.
            if private_directory:
                if info.st_uid != uid:
                    raise PermissionError(errno.EACCES, "Save directory must be owned by the current user", path)
                os.fchmod(fd, 0o700)  # nosemgrep - owner-only: rwx for the current user, nothing for group or others
            elif writable_by_others and not info.st_mode & stat.S_ISVTX:
                if strict:
                    raise PermissionError(errno.EACCES, "Save directory has an untrusted parent (writable without sticky bit; --strict-parents)", parent)
                logging.getLogger(__name__).info("Save directory parent %s is writable by others without sticky bit; continuing (see --strict-parents)", parent)
        yield path, fd
    finally:
        os.close(fd)


def private_save_directory(path: str, strict: bool = False) -> str:
    """Create or tighten an owned output directory without following symlinks."""
    with _private_directory(path, strict) as (directory, _):
        return directory


def private_log_stream(path: str, strict: bool = False) -> io.TextIOWrapper:
    """Open a user-owned, singly linked regular log through a verified directory FD."""
    directory, name = os.path.split(os.path.abspath(os.path.expandvars(path)))
    with _private_directory(directory, strict) as (_, directory_fd), contextlib.ExitStack() as allocation:
        descriptor = _open_owned(name, os.O_WRONLY | os.O_CREAT | os.O_APPEND | os.O_NOFOLLOW | os.O_NONBLOCK, 0o600, directory_fd)
        allocation.callback(os.close, descriptor)
        info = os.fstat(descriptor)
        if not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid() or info.st_nlink != 1:
            raise PermissionError(errno.EACCES, "Log must be a user-owned, singly linked regular file", path)
        os.fchmod(descriptor, 0o600)
        stream = os.fdopen(descriptor, "a", encoding="utf-8")
        allocation.pop_all()
        return stream


# Best-effort escape when savepath or the log cannot pass the checks above: a private directory
# under /dev/shm (then the system temp dir), created through the same no-follow walk.
FALLBACK_ROOTS: Tuple[str, ...] = ("/dev/shm",)  # nosec B108 - private 0700 subdirectory via _private_directory
_fallback_warnings: Set[Tuple[str, str]] = set()
_T = TypeVar("_T")


def _fallback_roots() -> List[str]:
    """QTOP_FALLBACK_DIR if set (e.g. node-local scratch), else /dev/shm and the system temp dir."""
    override = os.environ.get("QTOP_FALLBACK_DIR")
    roots = (os.path.expandvars(override),) if override else FALLBACK_ROOTS + (tempfile.gettempdir(),)
    return [root for root in dict.fromkeys(roots) if os.path.isdir(root)]


def _warn_fallback(what: str, path: str, error: OSError, fallback: str) -> None:
    if (what, path) in _fallback_warnings:
        return
    _fallback_warnings.add((what, path))
    reason = "%s%s" % (error.strerror or error, " (%s)" % error.filename if error.filename else "")
    message = visible_text("warning: cannot use %s %s: %s; using %s instead (best effort; --strict-parents stops instead)" % (what, path, reason, fallback))
    sys.stderr.write("qtop: %s\n" % message)
    if logging.getLogger().handlers:  # before init_logging, Python's last-resort handler would repeat it
        logging.getLogger(__name__).warning("%s", message)


def with_fallback(what: str, path: str, attempt: Callable[[str], _T], leaf: str = "", strict: bool = False) -> _T:
    """Return attempt(path), or attempt() on a private fallback directory with a warning if path fails.

    With strict (--strict-parents) the original error propagates instead.
    """
    try:
        return attempt(path)
    except OSError as error:
        if strict:
            raise
        for root in _fallback_roots():
            directory = os.path.join(root, "qtop_fallback_%d" % os.geteuid())
            candidate = os.path.join(directory, leaf) if leaf else directory
            try:
                result = attempt(candidate)
            except OSError:
                continue
            _warn_fallback(what, path, error, candidate)
            return result
        raise


def active_log_path() -> Optional[str]:
    """Return the file the root logger writes to, which may be a fallback, or None without one."""
    for handler in logging.getLogger().handlers:
        if isinstance(handler, logging.FileHandler):
            return handler.baseFilename
    return None


def get_new_temp_file(_savepath: str, suffix: str, prefix: str) -> Tuple[int, str]:
    """Create a temporary output file in savepath; return its descriptor and path.

    mkstemp, not NamedTemporaryFile: sys.stdout is redirected to the descriptor.
    """
    fd, temp_filepath = tempfile.mkstemp(suffix=suffix, prefix=prefix, dir=_savepath)  # **kwargs
    logger.debug("temp_filepath: %s", temp_filepath)
    return fd, temp_filepath


class _SampleArchive(tarfile.TarFile):
    """A support archive written under a hidden .part name and renamed into place on close.

    A watch stopped mid-capture (SIGTERM raises SystemExit anywhere) leaves only the .part file,
    never an empty or truncated archive under the final name.
    """

    target = ""

    def close(self) -> None:
        super().close()
        target, self.target = self.target, ""  # rename once; close() may be called again
        if target and self.name:
            os.replace(self.name, target)


def init_sample_file(
    options: Any, _savepath: str, SAMPLE_FILENAME: str, scheduler_output_filenames: Mapping[str, str], QTOPCONF_YAML: str, QTOPPATH: str
) -> Optional[tarfile.TarFile]:
    """Open the support archive for -L; -LL adds runtime assets and package sources.

    The caller adds rendered output, logs and eligible scheduler traces before closing it.
    """
    if options.SAMPLE < 1:
        return None

    # clears any preexisting tar files
    with contextlib.ExitStack() as initialization:
        target = os.path.join(_savepath, SAMPLE_FILENAME)
        partial = os.path.join(_savepath, "." + SAMPLE_FILENAME + ".part")
        tar_out = initialization.enter_context(_SampleArchive.open(partial, mode="w"))
        tar_out.target = target
        initialization.callback(tar_out.close)  # Finalize even failed captures, as before using the archive's context manager.
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
    """Archive the raw scheduler traces next to an export; return the archive path."""
    handle, path = tempfile.mkstemp(prefix="qtop_raw_traces_", suffix=".tar", dir=savepath)
    with os.fdopen(handle, "wb") as stream, tarfile.open(fileobj=stream, mode="w", dereference=True) as archive:
        for key, source in filenames.items():
            archive.add(source, arcname=key, recursive=False, filter=_without_archive_owner)
    return path


def add_to_sample(filepaths_to_add: List[str], sample_out: tarfile.TarFile, sample_method: Any = tarfile, subdir: Optional[str] = None) -> tarfile.TarFile:
    """Opens sample_file in path savepath and adds files filepaths_to_add."""
    if not isinstance(filepaths_to_add, list):
        raise TypeError("filepaths_to_add must be a list")
    for filepath_to_add in filepaths_to_add:
        fn = os.path.basename(filepath_to_add)
        try:
            logger.debug("Adding %s to sample...", filepath_to_add)
            sample_out.add(filepath_to_add, arcname=fn if not subdir else os.path.join(subdir, fn), filter=_without_archive_owner)
        except (OSError, tarfile.TarError) as error:
            logger.error("Could not add %s to the sample archive; skipping it: %s", filepath_to_add, error)
    return sample_out


def get_sample_filename(SAMPLE_FILENAME: str, config: Mapping[str, Any]) -> str:
    """Return the support-archive name, timestamped unless overwrite_sample_file is set."""
    if config["overwrite_sample_file"]:
        SAMPLE_FILENAME = SAMPLE_FILENAME % {"datetime": ""}
    else:
        SAMPLE_FILENAME = SAMPLE_FILENAME % {"datetime": "_" + datetime.datetime.now().strftime("%Y%m%d-%H%M%S")}
    return SAMPLE_FILENAME


def deprecate_old_output_files(config: Mapping[str, Any]) -> None:
    """Deletes older json and .out files in savepath directory."""
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
    """Build a timedelta from a single {unit: amount} mapping."""
    return datetime.timedelta(**extra_kw_args)


def parse_time_input(_time: str) -> Dict[str, int]:
    """Parse '5h', '10m' or '30s' into a timedelta keyword mapping, e.g. {'hours': 5}."""
    if not isinstance(_time, str) or not _time.endswith(("h", "m", "s")):
        raise ValueError("Time input must be a number followed by h, m or s")
    units = {"m": "minutes", "s": "seconds", "h": "hours"}
    try:
        return {units[_time[-1]]: int(_time[:-1])}
    except ValueError as error:
        raise ValueError("Time input must be a number followed by h, m or s") from error
