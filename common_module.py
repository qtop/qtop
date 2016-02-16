import logging
import sys
from sys import stdin, stdout
from optparse import OptionParser
import tempfile
from os.path import expandvars
import tarfile
import errno
from constants import *


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


def get_new_temp_file(suffix, prefix, config=None):  # **kwargs
    """
    Using mkstemp instead of NamedTemporaryFile because a file descriptor
    is needed to redirect sys.stdout to.
    """
    savepath = config['savepath'] if config else None
    fd, temp_filepath = tempfile.mkstemp(suffix=suffix, prefix=prefix, dir=savepath)  # **kwargs
    logging.debug('temp_filepath: %s' % temp_filepath)
    # out_file = os.fdopen(fd, 'w')
    return fd, temp_filepath


def handle_exception(exc_type, exc_value, exc_traceback):
    """
    This, when replacing sys.excepthook,
    will log uncaught exceptions to the logging module instead
    of printing them to stdout.
    """
    if issubclass(exc_type, KeyboardInterrupt):
        sys.__excepthook__(exc_type, exc_value, exc_traceback)
        return

    logger.error("Uncaught exception", exc_info=(exc_type, exc_value, exc_traceback))


def add_to_sample(filepaths_to_add, savepath, sample_file=QTOP_SAMPLE_FILENAME, sample_method=tarfile, subdir=None):
    """
    opens sample_file in path savepath and adds files filepaths_to_add
    """
    assert isinstance(filepaths_to_add, list)
    sample_out = sample_method.open(os.path.join(savepath, sample_file), mode='a')
    for filepath_to_add in filepaths_to_add:
        path, fn = filepath_to_add.rsplit('/', 1)
        try:
            logging.debug('Adding %s to sample...' % filepath_to_add)
            sample_out.add(filepath_to_add, arcname=fn if not subdir else os.path.join(subdir, fn))
        except tarfile.TarError:  # TODO: test what could go wrong here
            logging.error('There seems to be something wrong with the tarfile. Skipping...')
    else:
        logging.debug('Closing sample...')
        sample_out.close()


class JobNotFound(Exception):
    def __init__(self, job_state):
        Exception.__init__(self, "Job state %s not found" % job_state)
        self.job_state = job_state


class NoSchedulerFound(Exception):
    def __init__(self):
        msg = 'No suitable scheduler was found. ' \
              'Please define one in a switch or env variable or in %s.\n' \
              'For more help, try ./qtop.py --help\nLog file created in %s' % (QTOPCONF_YAML, expandvars(QTOP_LOGFILE))
        Exception.__init__(self, msg)
        logging.critical(msg)


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


class SchedulerNotSpecified(Exception):
    pass


class InvalidScheduler(Exception):
    pass


class EmptySystem(Exception):
    pass


parser = OptionParser()  # for more details see http://docs.python.org/library/optparse.html

parser.add_option("-1", "--disablesection1", action="store_true", dest="sect_1_off", default=False,
                  help="Disable first section of qtop, i.e. Job Accounting Summary")
parser.add_option("-2", "--disablesection2", action="store_true", dest="sect_2_off", default=False,
                  help="Disable second section of qtop, i.e. Worker Node Occupancy")
parser.add_option("-3", "--disablesection3", action="store_true", dest="sect_3_off", default=False,
                  help="Disable third section of qtop, i.e. User Accounts and Pool Mappings")
parser.add_option("-a", "--blindremapping", action="store_true", dest="BLINDREMAP", default=False,
                  help="This may be used in situations where node names are not a pure arithmetic seq (eg. rocks clusters)")
parser.add_option("-A", "--anonymize", action="store_true", dest="ANONYMIZE", default=False,
                  help="Masks unix account names and workernode names for security reasons (sending bug reports etc.)")
parser.add_option("-b", "--batchSystem", action="store", dest="BATCH_SYSTEM", default=None)
parser.add_option("-c", "--COLOR", action="store", dest="COLOR", default="AUTO", choices=['ON', 'OFF', 'AUTO'],
                  help="Enable/Disable color in qtop output. AUTO detects tty (for watch -d)")
parser.add_option("-C", "--classic", action="store_true", dest="CLASSIC", default=False,
                  help="tries to mimic legacy qtop display as much as possible")
parser.add_option("-d", "--debug", action="store_true", dest="DEBUG", default=False,
                  help="print debugging messages in stdout, not just in the log file.")
parser.add_option("-F", "--ForceNames", action="store_true", dest="FORCE_NAMES", default=False,
                  help="force names to show up instead of numbered WNs even for very small numbers of WNs")
# parser.add_option("-f", "--setCOLORMAPFILE", action="store", type="string", dest="COLORFILE")  # TODO
parser.add_option("-f", "--setCUSTOMCONFFILE", action="store", type="string", dest="CONFFILE")
parser.add_option("-g", "--get_gecos_via_getent_passwd", action="store_true", dest="GET_GECOS", default=False,
                  help="get user details by issuing getent passwd for all users mentioned in qtop input files.")
parser.add_option("-m", "--noMasking", action="store_true", dest="NOMASKING", default=False,
                  help="Don't mask early empty WNs (default: if the first 30 WNs are unused, counting starts from 31).")
parser.add_option("-o", "--option", action="append", dest="OPTION", type="string", default=[],
                  help="Override respective option in QTOPCONF_YAML file")
parser.add_option("-O", "--onlysavetofile", action="store_true", dest="ONLYSAVETOFILE", default=False,
                  help="Do not print results to stdout")
parser.add_option("-r", "--removeemptycorelines", dest="REM_EMPTY_CORELINES", action="store_true", default=False,
                  help="If a whole row consists of empty core lines, remove the row")
parser.add_option("-s", "--SetSourceDir", dest="SOURCEDIR",
                  help="Set the source directory where pbsnodes and qstat reside")
parser.add_option("-T", "--Transpose", dest="TRANSPOSE", action="store_true", default=False,
                  help="Rotate matrices' positioning by 90 degrees")
parser.add_option("-v", "--verbose", dest="verbose", action="count",
                  help="Increase verbosity (specify multiple times for more)")
# TODO: dumping to intermediate yaml files has been deprecated.
# It is now possible, instead, to dump an all-including python structure into a json file (the "document").
# This exists, but is not yet tunable.
# parser.add_option("-W", "--writemethod", dest="write_method", action="store", default="txtyaml",
#                   choices=['json'],
#                   help="Set the method used for dumping information, json, yaml, or native python (yaml format)")
parser.add_option("-w", "--watch", dest="WATCH", action="store_true", default=False,
                  help="Mimic shell's watch behaviour")
# TODO: implement this!
# parser.add_option("-z", "--quiet", action="store_false", dest="verbose", default=True,
#                   help="Don't print status messages to stdout. Not doing anything at the moment.")
parser.add_option("-L", "--sample", action="count", dest="SAMPLE", default=False,
                  help="Create a sample file. A single L creates a tarball with the log, scheduler output files, "
                       "qtop output. "
                       "Two L's additionaly include the qtop_conf yaml file, and qtop source.")

(options, args) = parser.parse_args()

if not options.verbose:
    log_level = logging.WARN
elif options.verbose == 1:
    log_level = logging.INFO
elif options.verbose >= 2:
    log_level = logging.DEBUG

QTOP_LOGFILE_PATH = QTOP_LOGFILE.rsplit('/', 1)[0]
mkdir_p(QTOP_LOGFILE_PATH)

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

if options.verbose >= 3:
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', "%Y-%m-%d %H:%M:%S")  # this prepends date time
else:
    formatter = logging.Formatter('%(levelname)s - %(message)s')

fh = logging.FileHandler(QTOP_LOGFILE)
fh.setLevel(log_level)
fh.setFormatter(formatter)
logger.addHandler(fh)

fh = logging.StreamHandler()
fh.setLevel(logging.ERROR)
# TODO originally:
# fh.setLevel(logging.ERROR) if options.DEBUG else fh.setLevel(logging.ERROR)
# ->this resulted in uncaught exceptions not printing to stderr !!
fh.setFormatter(formatter)
logger.addHandler(fh)
logger.disabled = False  # TODO: maybe make this a cmdline switch? -D ?

logging.info("\n")
logging.info("=" * 50)
logging.info("STARTING NEW LOG ENTRY...")
logging.info("=" * 50)
logging.info("\n\n")

logging.debug('Verbosity level = %s' % options.verbose)
logging.debug("input, output isatty: %s\t%s" % (stdin.isatty(), stdout.isatty()))
if options.COLOR == 'AUTO':
    options.COLOR = 'ON' if (os.environ.get("QTOP_COLOR", stdout.isatty()) in ("ON", True)) else 'OFF'
logging.debug("options.COLOR is now set to: %s" % options.COLOR)
options.REMAP = False  # Default value

sys.excepthook = handle_exception  # TODO: check if I really need this any more


