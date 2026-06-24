import datetime
import errno
import glob
import logging
import os
import posixpath
import sys
import tempfile
import tarfile


def mkdir_p(path):
    try:
        os.makedirs(path)
    except OSError as exc:  # Python >2.5
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            raise


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
    sys.stdout.write("\nExiting. Thank you for ..watching ;)\n")
    sys.stdout.flush()
    sys.stdout.close()
    try:
        os.close(handle)
    except OSError:
        pass
    if delete_file:
        os.unlink(name)  # this deletes the file
    # sys.stdout = stdout
    if options.SAMPLE >= 1:
        _ = add_to_sample([qtop_logfile], _savepath, sample_filename)
    sys.exit(0)


def init_sample_file(options, _savepath, SAMPLE_FILENAME, scheduler_output_filenames, QTOPCONF_YAML, QTOPPATH):
    """
    If the user wants to give feedback to the developers for a bugfix via the -L cmdline switch,
    this initialises a tar file, and adds:
    * the scheduler output files (-L),
    * and source files (-LL)
    to the tar file
    """
    if options.SAMPLE >= 1:
        # clears any preexisting tar files
        tar_out = tarfile.open(os.path.join(_savepath, SAMPLE_FILENAME), mode="w")

    if options.SAMPLE >= 2:
        tar_out = add_to_sample([os.path.join(os.path.realpath(QTOPPATH), QTOPCONF_YAML)], tar_out)
        source_files = glob.glob(os.path.join(os.path.realpath(QTOPPATH), "*.py"))
        tar_out = add_to_sample(source_files, tar_out, subdir="qtop_py")
    return tar_out


def add_to_sample(filepaths_to_add, sample_out, sample_method=tarfile, subdir=None):
    # def add_to_sample(filepaths_to_add, _savepath, sample_file, sample_method=tarfile, subdir=None):
    """
    opens sample_file in path savepath and adds files filepaths_to_add
    """
    assert isinstance(filepaths_to_add, list)
    for filepath_to_add in filepaths_to_add:
        filename = os.path.basename(filepath_to_add)
        archive_name = filename if not subdir else posixpath.join(subdir, filename)
        try:
            logging.debug("Adding %s to sample..." % filepath_to_add)
            sample_out.add(filepath_to_add, arcname=archive_name)
        except (tarfile.TarError, OSError) as exc:
            logging.error("Could not add %s to the sample archive: %s. Skipping...", filepath_to_add, exc)
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
        file_modified = datetime.datetime.fromtimestamp(os.path.getmtime(curpath))
        if datetime.datetime.now() - file_modified > time_alive:
            os.remove(curpath)


def get_timedelta(extra_kw_args):
    """
    This solely exists to allow timedelta.timedelta's keyword argument (minutes/seconds/hours=...)
    to be selected with a variable.
    """
    return datetime.timedelta(**extra_kw_args)


def parse_time_input(_time):
    """
    The function accepts a _time str in either (h)ours, (m)inutes, or (s)econds, using the respective suffix,
    e.g. '5h', or '10m', or '30s'
    A dictionary is returned, e.g. {"hours": 5}.
    """
    units = {"m": "minutes", "s": "seconds", "h": "hours"}
    error_message = "Time input must be a number followed by h, m, or s."
    if not isinstance(_time, str) or len(_time) < 2 or _time[-1] not in units:
        raise ValueError(error_message)

    try:
        quantity = int(_time[:-1])
    except ValueError as exc:
        raise ValueError(error_message) from exc

    return {units[_time[-1]]: quantity}
