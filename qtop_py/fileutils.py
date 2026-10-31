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


def mkdir_p(path):
    try:
        os.makedirs(path)
    except OSError as exc:  # Python >2.5
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            raise


def private_save_directory(path):
    """Create and verify a private output path without following symlinks."""
    if not isinstance(path, str) or not path.strip():
        raise ValueError("Save directory must be a nonempty path")
    if os.name != "posix":
        raise OSError(errno.ENOTSUP, "Private save directories require POSIX ownership checks", path)
    path = os.path.abspath(os.path.expandvars(path))
    if sys.platform == "darwin":
        for alias in ("/tmp", "/var"):
            if path == alias or path.startswith(alias + os.sep):
                info = os.lstat(alias)
                # Expand only Apple's root-owned aliases; validate every target component below.
                if stat.S_ISLNK(info.st_mode) and info.st_uid == 0 and os.readlink(alias) in ("private" + alias, "/private" + alias):
                    path = "/private" + path
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
            access = os.O_RDONLY if index == len(parts) - 1 else parent_access
            child_fd = os.open(part, flags | access, dir_fd=fd)
            os.close(fd)
            fd = child_fd
            info = os.fstat(fd)
            writable_by_others = info.st_mode & (stat.S_IWGRP | stat.S_IWOTH)
            # Ancestors can be site-owned; the output directory must belong to this user.
            if index == len(parts) - 1:
                if info.st_uid != uid:
                    raise PermissionError(errno.EACCES, "Save directory must be owned by the current user", path)
                if writable_by_others:
                    raise PermissionError(errno.EACCES, "Save directory is writable by others; inspect it or choose a new private directory", path)
                os.fchmod(fd, 0o700)
            elif writable_by_others and not info.st_mode & stat.S_ISVTX:
                parent = os.path.join(os.sep, *parts[: index + 1])
                raise PermissionError(errno.EACCES, "Save directory has an untrusted parent (writable without sticky bit)", parent)
    finally:
        os.close(fd)
    return path


def check_empty_file(orig_file):
    if os.path.getsize(orig_file) == 0:
        raise FileEmptyError(orig_file)


def get_new_temp_file(_savepath, suffix, prefix):  # **kwargs
    """
    Using mkstemp instead of NamedTemporaryFile because a file descriptor
    is needed to redirect sys.stdout to.
    """
    fd, temp_filepath = tempfile.mkstemp(suffix=suffix, prefix=prefix, dir=_savepath)  # **kwargs
    logging.debug("temp_filepath: %s" % temp_filepath)
    # out_file = os.fdopen(fd, 'w')
    return fd, temp_filepath


def safe_exit_with_file_close(handle, name, stdout, options, _savepath, qtop_logfile, sample_filename, delete_file=False):
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


def init_sample_file(options, _savepath, SAMPLE_FILENAME, scheduler_output_filenames, QTOPCONF_YAML, QTOPPATH):
    """
    Initialise a support archive; -LL adds runtime assets and package sources.
    The caller adds rendered output, logs and eligible scheduler traces before closing it.
    """
    if options.SAMPLE < 1:
        return None

    # clears any preexisting tar files
    tar_out = tarfile.open(os.path.join(_savepath, SAMPLE_FILENAME), mode="w")
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
    return tar_out


def _without_archive_owner(member):
    member.uid = member.gid = 0
    member.uname = member.gname = ""
    return member


def save_raw_companion(savepath, filenames):
    handle, path = tempfile.mkstemp(prefix="qtop_raw_traces_", suffix=".tar", dir=savepath)
    with os.fdopen(handle, "wb") as stream:
        with tarfile.open(fileobj=stream, mode="w", dereference=True) as archive:
            for key, source in filenames.items():
                archive.add(source, arcname=key, recursive=False, filter=_without_archive_owner)
    return path


def add_to_sample(filepaths_to_add, sample_out, sample_method=tarfile, subdir=None):
    # def add_to_sample(filepaths_to_add, _savepath, sample_file, sample_method=tarfile, subdir=None):
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
    # else:
    # logging.debug('Closing sample...')
    # sample_out.close()
    return sample_out


def get_sample_filename(SAMPLE_FILENAME, config):
    if config["overwrite_sample_file"]:
        SAMPLE_FILENAME = SAMPLE_FILENAME % {"datetime": ""}
    else:
        SAMPLE_FILENAME = SAMPLE_FILENAME % {"datetime": "_" + datetime.datetime.now().strftime("%Y%m%d-%H%M%S")}
    return SAMPLE_FILENAME


class FileNotFound(Exception):
    def __init__(self, fn):
        msg = "File %s not found.\nMaybe the correct scheduler is not specified?" % fn
        Exception.__init__(self, msg)
        logging.critical(msg)
        self.fn = fn


class FileEmptyError(Exception):
    def __init__(self, fn):
        msg = "File %s is empty.\nIs your batch scheduler loaded with jobs?" % fn
        Exception.__init__(self, msg)
        logging.warning(msg)
        self.fn = fn


def deprecate_old_output_files(config):
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


def get_timedelta(extra_kw_args):
    """
    This solely exists to allow timedelta.timedelta's keyword argument (minutes/seconds/hours=...)
    to be selected with a variable.
    """
    return datetime.timedelta(**extra_kw_args)


def parse_time_input(_time):
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
