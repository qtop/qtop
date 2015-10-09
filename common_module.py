# import yaml
import logging
from sys import stdin, stdout
from constants import *
from optparse import OptionParser
import yaml_parser as yaml
try:
    import ujson as json
except ImportError:
    import json
from tempfile import mkstemp
import os, errno


# try:
#     from yaml import CLoader as Loader, CDumper as Dumper
# except ImportError:
#     try:
#         from yaml import Loader, Dumper
#     except ImportError:
#         pass

def mkdir_p(path):
    try:
        os.makedirs(path)
    except OSError as exc:  # Python >2.5
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            raise

Loader = None

parser = OptionParser()  # for more details see http://docs.python.org/library/optparse.html

parser.add_option("-a", "--blindremapping", action="store_true", dest="BLINDREMAP", default=False,
                  help="This may be used in situations where node names are not a pure arithmetic seq (eg. rocks clusters)")
parser.add_option("-b", "--batchSystem", action="store", type="string", dest="BATCH_SYSTEM")
parser.add_option("-y", "--readexistingyaml", action="store_true", dest="YAML_EXISTS", default=False,
                  help="Do not remake yaml input files, read from the existing ones")
parser.add_option("-c", "--COLOR", action="store", dest="COLOR", default="AUTO", choices=['ON', 'OFF', 'AUTO'],
                  help="Enable/Disable color in qtop output.")
# parser.add_option("-f", "--setCOLORMAPFILE", action="store", type="string", dest="COLORFILE")
parser.add_option("-m", "--noMasking", action="store_true", dest="NOMASKING", default=False,
                  help="Don't mask early empty WNs (default: if the first 30 WNs are unused, counting starts from 31).")
parser.add_option("-o", "--SetVerticalSeparatorXX", action="store", dest="WN_COLON", default=0,
                  help="Put vertical bar every WN_COLON nodes.")
parser.add_option("-s", "--SetSourceDir", dest="SOURCEDIR", default=os.path.realpath('.'),
                  help="Set the source directory where pbsnodes and qstat reside")
parser.add_option("-z", "--quiet", action="store_false", dest="verbose", default=True,
                  help="don't print status messages to stdout. Not doing anything at the moment.")
parser.add_option("-F", "--ForceNames", action="store_true", dest="FORCE_NAMES", default=False,
                  help="force names to show up instead of numbered WNs even for very small numbers of WNs")
parser.add_option("-w", "--writemethod", dest="write_method", action="store", default="txtyaml",
                  choices=['txtyaml', 'yaml', 'json'],
                  help="Set the method used for dumping information, json, yaml, or native python (yaml format)")
parser.add_option("-r", "--removeemptycorelines", dest="REM_EMPTY_CORELINES", action="store_true", default=False,
                  help="Set the method used for dumping information, json, yaml, or native python (yaml format)")
parser.add_option("-v", "--verbose", dest="verbose", action="count", help="Increase verbosity (specify multiple times for more")

(options, args) = parser.parse_args()

print "output isatty: %s" % stdout.isatty()
print "input isatty: %s" % stdin.isatty()
if options.COLOR == 'AUTO':
    options.COLOR = 'ON' if stdout.isatty() else 'OFF'



log_level = logging.WARNING  # default

if options.verbose == 1:
    log_level = logging.INFO
elif options.verbose >= 2:
    log_level = logging.DEBUG

QTOP_LOGFILE_PATH = QTOP_LOGFILE.rsplit('/', 1)[0]
mkdir_p(QTOP_LOGFILE_PATH)
logging.basicConfig(
    filename=QTOP_LOGFILE,
    filemode='w',
    level=log_level,
    # format='%(levelname)s - %(message)s'
    format='%(asctime)s - %(levelname)s - %(message)s'
)


def read_qstat_yaml(fn, write_method):
    """
    reads qstat YAML file and populates four lists. Returns the lists
    """
    job_ids, usernames, job_states, queue_names = [], [], [], []

    with open(fn) as fin:
        try:
            qstats = (write_method.endswith('yaml')) and yaml.load_all(fin, Loader=Loader) or json.load(fin)
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


def get_new_temp_file(suffix, prefix):  # **kwargs
    fd, temp_filepath = mkstemp(suffix=suffix, prefix=prefix)  # **kwargs
    logging.debug('temp_filepath: %s' % temp_filepath)
    # out_file = os.fdopen(fd, 'w')
    return fd, temp_filepath
    # return out_file
