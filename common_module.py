import logging
import sys
from sys import stdin, stdout
from optparse import OptionParser
from tempfile import mkstemp
import os
import errno
try:
    import ujson as json
except ImportError:
    import json

from constants import *
import yaml_parser as yaml


def mkdir_p(path):
    try:
        os.makedirs(path)
    except OSError as exc:  # Python >2.5
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            raise


def check_empty_file(orig_file):
    if not os.path.getsize(orig_file) > 0:
        logging.critical('Your %s file is empty! Please check your directory. Exiting ...' % orig_file)
        sys.exit(0)


parser = OptionParser()  # for more details see http://docs.python.org/library/optparse.html

parser.add_option("-1", "--disablesection1", action="store_true", dest="sect_1_off", default=False,
                  help="Disable first section of qtop, i.e. Job Accounting Summary")
parser.add_option("-2", "--disablesection2", action="store_true", dest="sect_2_off", default=False,
                  help="Disable second section of qtop, i.e. Worker Node Occupancy")
parser.add_option("-3", "--disablesection3", action="store_true", dest="sect_3_off", default=False,
                  help="Disable third section of qtop, i.e. User Accounts and Pool Mappings")
parser.add_option("-a", "--blindremapping", action="store_true", dest="BLINDREMAP", default=False,
                  help="This may be used in situations where node names are not a pure arithmetic seq (eg. rocks clusters)")
parser.add_option("-b", "--batchSystem", action="store", type="string", dest="BATCH_SYSTEM", default=None)
parser.add_option("-c", "--COLOR", action="store", dest="COLOR", default="AUTO", choices=['ON', 'OFF', 'AUTO'],
                  help="Enable/Disable color in qtop output. AUTO detects tty (for watch -d)")
parser.add_option("-d", "--debug", action="store_true", dest="DEBUG", default=False,
                  help="print debugging messages in stdout, not just in the log file.")
parser.add_option("-F", "--ForceNames", action="store_true", dest="FORCE_NAMES", default=False,
                  help="force names to show up instead of numbered WNs even for very small numbers of WNs")
# parser.add_option("-f", "--setCOLORMAPFILE", action="store", type="string", dest="COLORFILE")
parser.add_option("-m", "--noMasking", action="store_true", dest="NOMASKING", default=False,
                  help="Don't mask early empty WNs (default: if the first 30 WNs are unused, counting starts from 31).")
parser.add_option("-o", "--SetVerticalSeparatorXX", action="store", dest="WN_COLON", default=0,
                  help="Put vertical bar every WN_COLON nodes.")
parser.add_option("-r", "--removeemptycorelines", dest="REM_EMPTY_CORELINES", action="store_true", default=False,
                  help="Set the method used for dumping information, json, yaml, or native python (yaml format)")
parser.add_option("-s", "--SetSourceDir", dest="SOURCEDIR",
                  help="Set the source directory where pbsnodes and qstat reside")
parser.add_option("-v", "--verbose", dest="verbose", action="count",
                  help="Increase verbosity (specify multiple times for more)")
parser.add_option("-w", "--writemethod", dest="write_method", action="store", default="txtyaml",
                  choices=['txtyaml', 'json'],
                  help="Set the method used for dumping information, json, yaml, or native python (yaml format)")
parser.add_option("-y", "--readexistingyaml", action="store_true", dest="YAML_EXISTS", default=False,
                  help="Do not remake yaml input files, read from the existing ones")
parser.add_option("-z", "--quiet", action="store_false", dest="verbose", default=True,
                  help="don't print status messages to stdout. Not doing anything at the moment.")

(options, args) = parser.parse_args()

# log_level = logging.WARNING  # default

if options.verbose == 1:
    log_level = logging.INFO
elif options.verbose >= 2:
    log_level = logging.DEBUG

QTOP_LOGFILE_PATH = QTOP_LOGFILE.rsplit('/', 1)[0]
mkdir_p(QTOP_LOGFILE_PATH)

# This is for writing only to a log file
# logging.basicConfig(filename=QTOP_LOGFILE, filemode='w', level=log_level, format='%(asctime)s - %(levelname)s - %(message)s')

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

# formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
formatter = logging.Formatter('%(levelname)s - %(message)s')

fh = logging.FileHandler(QTOP_LOGFILE)
fh.setLevel(log_level)
fh.setFormatter(formatter)
logger.addHandler(fh)

fh = logging.StreamHandler()
fh.setLevel(logging.ERROR) if options.DEBUG else fh.setLevel(logging.CRITICAL)
fh.setFormatter(formatter)
logger.addHandler(fh)
logger.disabled = False  # maybe make this a cmdline switch? -D ?

logging.info("\n")
logging.info("=" * 50)
logging.info("STARTING NEW LOG ENTRY...")
logging.info("=" * 50)
logging.info("\n\n")

logging.debug("input, output isatty: %s\t%s" % (stdin.isatty(), stdout.isatty()))
if options.COLOR == 'AUTO':
    options.COLOR = 'ON' if (os.environ.get("QTOP_COLOR", stdout.isatty()) in ("ON", True)) else 'OFF'
logging.debug("options.COLOR is now set to: %s" % options.COLOR)


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

sys.excepthook = handle_exception


def read_qstat_yaml(fn, write_method):
    """
    reads qstat YAML file and populates four lists. Returns the lists
    """
    job_ids, usernames, job_states, queue_names = [], [], [], []

    with open(fn) as fin:
        try:
            qstats = (write_method.endswith('yaml')) and yaml.load_all(fin) or json.load(fin)
        except StopIteration:
            logging.warning('File %s is empty. (No jobs found or Error!)')
        else:
            for qstat in qstats:
                job_ids.append(str(qstat['JobId']))
                usernames.append(qstat['UnixAccount'])
                job_states.append(qstat['S'])
                queue_names.append(qstat['Queue'])
    # os.remove(fn)  # that DELETES the file!! why did I do that?!!
    logging.debug('job_ids, usernames, job_states, queue_names lengths: '
        '%(job_ids)s, %(usernames)s, %(job_states)s, %(queue_names)s'
        % {
        "job_ids": len(job_ids),
        "usernames": len(usernames),
        "job_states": len(job_states),
        "queue_names": len(queue_names)
        }
    )
    return job_ids, usernames, job_states, queue_names


def get_new_temp_file(config, suffix, prefix):  # **kwargs
    fd, temp_filepath = mkstemp(suffix=suffix, prefix=prefix, dir=config['savepath'])  # **kwargs
    logging.debug('temp_filepath: %s' % temp_filepath)
    # out_file = os.fdopen(fd, 'w')
    return fd, temp_filepath
    # return out_file
