import logging
import os
import errno
import tempfile
import tarfile
import sys
import glob
import datetime


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


def get_new_temp_file(_savepath, suffix, prefix):
    """
    Creates new temp file to store the output to be displayed.
    Using mkstemp instead of NamedTemporaryFile, because a file descriptor
    is needed to redirect sys.stdout to.
    """
    fd, temp_filepath = tempfile.mkstemp(suffix=suffix, prefix=prefix, dir=_savepath)  # **kwargs
    logging.debug('temp_filepath: %s' % temp_filepath)

    return fd, temp_filepath


def exit_safe(handle, name, conf, sample, delete_file=False):
    sys.stdout.write('\nExiting. Thank you for ..watching ;)\n')
    sys.stdout.flush()
    sys.stdout.close()

    try:
        os.close(handle)
    except OSError:
        pass

    if delete_file:
        os.unlink(name)  # this deletes the file

    if conf.cmd_options.SAMPLE >= 1:
        sample.add_to_sample([conf.QTOP_LOGFILE], conf.savepath)
    sys.exit(0)


class FileNotFound(Exception):
    def __init__(self, fn):
        msg = "File %s not found.\nMaybe the correct scheduler is not specified?" % fn
        Exception.__init__(self, msg)
        logging.critical(msg)
        self.fn = fn


class FileEmptyError(Exception):
    def __init__(self, fn):
        msg = "File %s is empty.\n" \
              "Is your batch scheduler loaded with jobs?" % fn
        Exception.__init__(self, msg)
        logging.warning(msg)
        self.fn = fn


def deprecate_old_output_files(config):
    """
    deletes older json and .out files in savepath directory.
    """
    time_alive = get_timedelta(parse_time_input(config['auto_delete_old_output_files_after']))
    _savepath = config['savepath']
    for f in os.listdir(_savepath):
        if (not f.endswith(('json', '.out'))) or f.endswith('rec.out'):
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
    the func accepts a _time str in either (h)ours, (m)inutes, or (s)econds, using the respective suffix,
    e.g. '5h', or '10m', or '30s'
    A tuple is returned, e.g. (5, 'hours')
    """
    assert _time.endswith(('h', 'm', 's'))
    try:
        int(_time[:-1])
    except ValueError:
        logging.critical('Time input given must be a number followed by the letter h/m/s. Exiting.')

    quantity, user_unit_suffix = _time[:-1], _time[-1]
    units = {'m': 'minutes', 's': 'seconds', 'h': 'hours'}
    user_unit = units[user_unit_suffix]

    return {user_unit: int(quantity)}


class Sample(object):

    def __init__(self, conf):
        self.tar_out = None
        self.options = conf.cmd_options
        self.conf = conf
        self.SAMPLE_FILENAME = os.path.expandvars('qtop_sample_${USER}%(datetime)s.tar')

    def init_sample_file(self, _savepath, scheduler_output_filenames, QTOPCONF_YAML, QTOPPATH):
        """
        If the user wants to give feedback to the developers for a bugfix via the -L cmdline switch,
        this initialises a tar file, and adds:
        * the scheduler output files (-L),
        * and source files (-LL)
        to the tar file
        """
        if self.options.SAMPLE >= 1:
            # clears any preexisting tar files
            self.tar_out = tarfile.open(os.path.join(_savepath, self.SAMPLE_FILENAME), mode='w')

        if self.options.SAMPLE >= 2:
            source_files = glob.glob(os.path.join(os.path.realpath(QTOPPATH), '*.py'))
            source_files_qtop_py = glob.glob(os.path.join(os.path.realpath(QTOPPATH + '/qtop_py'), '*.py'))
            self.add_to_sample([os.path.join(os.path.realpath(QTOPPATH), QTOPCONF_YAML)])
            self.add_to_sample(source_files)
            self.add_to_sample(source_files_qtop_py, subdir='qtop_py')

    def handle_sample(self, scheduler_output_filenames, output_fp, qtop_logfile, options):
        self.add_to_sample([output_fp])
        print "Sample files saved in %s/%s" % (self.conf.config['savepath'], self.SAMPLE_FILENAME)

        if options.SAMPLE >= 1:
            self.add_to_sample([qtop_logfile])
            # add all scheduler output files to sample
            for fn in scheduler_output_filenames:
                if os.path.isfile(scheduler_output_filenames[fn]):
                    self.add_to_sample([scheduler_output_filenames[fn]])
            self.tar_out.close()


    def add_to_sample(self, filepaths_to_add, sample_method=tarfile, subdir=None):
        """
        opens sample_file in path savepath and adds files filepaths_to_add
        """
        assert isinstance(filepaths_to_add, list)

        for fp in filepaths_to_add:
            path, fn = fp.rsplit('/', 1)
            try:
                logging.debug('Adding %s to sample...' % fp)
                self.tar_out.add(fp, arcname=fn if not subdir else os.path.join(subdir, fn))
            except tarfile.TarError:  # TODO: test what could go wrong here
                logging.error('There seems to be something wrong with the tarfile. Skipping...')

    def set_sample_filename_format_from_conf(self, config):
        if config['overwrite_sample_file']:
            self.SAMPLE_FILENAME = self.SAMPLE_FILENAME % {'datetime': ''}
        else:
            self.SAMPLE_FILENAME = self.SAMPLE_FILENAME % {'datetime': '_' + datetime.datetime.now().strftime("%Y%m%d-%H%M%S")}
