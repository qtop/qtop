import logging
import sys
from sys import stdin, stdout
from optparse import OptionParser
from tempfile import mkstemp
import os
import tarfile
import re
from itertools import count
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


def get_new_temp_file(suffix, prefix, config=None):  # **kwargs
    savepath = config['savepath'] if config else None
    fd, temp_filepath = mkstemp(suffix=suffix, prefix=prefix, dir=savepath)  # **kwargs
    logging.debug('temp_filepath: %s' % temp_filepath)
    # out_file = os.fdopen(fd, 'w')
    return fd, temp_filepath
    # return out_file


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


class StatMaker:
    def __init__(self, config):
        self.l = list()
        self.config = config
        self.anonymize = anonymize_func()

        self.stat_mapping = {
            'txtyaml': (self.stat_write_lines, {}, 'yaml'),
            'json': (json.dump, {}, 'json')
        }

        self.statq_mapping = {
            'txtyaml': (self.statq_write_lines, {}, 'yaml'),
            'json': (json.dump, {}, 'json')}

    def stat_write_lines(self, fout):
        for qstat_values in self.l:
            fout.write('---\n')
            fout.write('JobId: ' + qstat_values['JobId'] + '\n')
            fout.write('UnixAccount: ' + qstat_values['UnixAccount'] + '\n')
            fout.write('S: ' + qstat_values['S'] + '\n')  # job state
            fout.write('Queue: ' + qstat_values['Queue'] + '\n')
            fout.write('...\n')

    def statq_write_lines(self, fout):
        last_line = self.l.pop()
        for qstatq_values in self.l:
            fout.write('---\n')
            fout.write('queue_name: ' + qstatq_values['queue_name'] + '\n')
            fout.write('state: ' + qstatq_values['state'] + '\n')  # job state
            fout.write('lm: ' + qstatq_values['lm'] + '\n')
            fout.write('run: ' + qstatq_values['run'] + '\n')  # job state
            fout.write('queued: ' + qstatq_values['queued'] + '\n')
            fout.write('...\n')
        fout.write('---\n')
        fout.write('Total_queued: ' + '"' + last_line['Total_queued'] + '"' + '\n')
        fout.write('Total_running: ' + '"' + last_line['Total_running'] + '"' + '\n')
        fout.write('...\n')

    def dump_all(self, out_file, write_func_args):
        """
        dumps the content of qstat/qstat_q files in the selected write_method format
        """
        with open(out_file, 'w') as fout:
            write_func, kwargs, _ = write_func_args
            write_func(fout, **kwargs)


class QStatMaker(StatMaker):
    def __init__(self, config):
        StatMaker.__init__(self, config)
        self.user_q_search = r'^(?P<host_name>(?P<job_id>[0-9-]+)\.(?P<domain>[\w-]+))\s+' \
                             r'(?P<name>[\w%.=+/-]+)\s+' \
                             r'(?P<user>[A-Za-z0-9.]+)\s+' \
                             r'(?P<time>\d+:\d+:?\d*|0)\s+' \
                             r'(?P<state>[CWRQE])\s+' \
                             r'(?P<queue_name>\w+)'
        self.user_q_search_prior = r'\s{0,2}' \
                                   r'(?P<job_id>\d+)\s+' \
                                   r'(?:[0-9]\.[0-9]+)\s+' \
                                   r'(?:[\w.-]+)\s+' \
                                   r'(?P<user>[\w.-]+)\s+' \
                                   r'(?P<state>[a-z])\s+' \
                                   r'(?:\d{2}/\d{2}/\d{2}|0)\s+' \
                                   r'(?:\d+:\d+:\d*|0)\s+' \
                                   r'(?P<queue_name>\w+@[\w.-]+)\s+' \
                                   r'(?:\d+)\s+' \
                                   r'(?:\w*)'

    def convert_qstat_to_yaml(self, orig_file, out_file, write_method):
        check_empty_file(orig_file)
        with open(orig_file, 'r') as fin:
            _ = fin.readline()  # header
            fin.readline()
            line = fin.readline()
            re_match_positions = ('job_id', 'user', 'state', 'queue_name')  # was: (1, 5, 7, 8), (1, 4, 5, 8)
            try:  # first qstat line determines which format qstat follows.
                re_search = self.user_q_search
                qstat_values = self.process_line(re_search, line, re_match_positions)
                self.l.append(qstat_values)
                # unused: _job_nr, _ce_name, _name, _time_use = m.group(2), m.group(3), m.group(4), m.group(6)
            except AttributeError:  # this means 'prior' exists in qstat, it's another format
                re_search = self.user_q_search_prior
                qstat_values = self.process_line(re_search, line, re_match_positions)
                self.l.append(qstat_values)
                # unused:  _prior, _name, _submit, _start_at, _queue_domain, _slots, _ja_taskID =
                # m.group(2), m.group(3), m.group(6), m.group(7), m.group(9), m.group(10), m.group(11)
            finally:  # hence the rest of the lines should follow either try's or except's same format
                for line in fin:
                    qstat_values = self.process_line(re_search, line, re_match_positions)
                    self.l.append(qstat_values)
        self.dump_all(out_file, self.stat_mapping[write_method])  # self.l,

    def convert_qstatq_to_yaml(self, orig_file, out_file, write_method):
        """
        reads QSTATQ_ORIG_FN sequentially and puts useful data in respective yaml file
        Searches for lines in the following format:
        biomed             --      --    72:00:00   --   31   0 --   E R
        (except for the last line, which contains two sums and is parsed separately)
        """
        check_empty_file(orig_file)
        anonymize = anonymize_func()
        queue_search = r'^(?P<queue_name>[\w.-]+)\s+' \
                       r'(?:--|[0-9]+[mgtkp]b[a-z]*)\s+' \
                       r'(?:--|\d+:\d+:?\d*:?)\s+' \
                       r'(?:--|\d+:\d+:?\d+:?)\s+(--)\s+' \
                       r'(?P<run>\d+)\s+' \
                       r'(?P<queued>\d+)\s+' \
                       r'(?P<lm>--|\d+)\s+' \
                       r'(?P<state>[DE] R)'
        run_qd_search = '^\s*(?P<tot_run>\d+)\s+(?P<tot_queued>\d+)'  # this picks up the last line contents

        with open(orig_file, 'r') as fin:
            fin.next()
            fin.next()
            # server_name = fin.next().split(': ')[1].strip()
            fin.next()
            fin.next().strip()  # the headers line should later define the keys in temp_dict, should they be different
            fin.next()
            for line in fin:
                line = line.strip()
                m = re.search(queue_search, line)
                n = re.search(run_qd_search, line)
                temp_dict = {}
                try:
                    queue_name = m.group('queue_name') if not options.ANONYMIZE else anonymize(m.group('queue_name'), 'qs')
                    run, queued, lm, state = m.group('run'), m.group('queued'), m.group('lm'), m.group('state')
                except AttributeError:
                    try:
                        total_running_jobs, total_queued_jobs = n.group('tot_run'), n.group('tot_queued')
                    except AttributeError:
                        continue
                else:
                    for key, value in [('queue_name', queue_name),
                                       ('run', run),
                                       ('queued', queued),
                                       ('lm', lm),
                                       ('state', state)]:
                        temp_dict[key] = value
                    self.l.append(temp_dict)
            self.l.append({'Total_running': total_running_jobs, 'Total_queued': total_queued_jobs})
        self.dump_all(out_file, self.statq_mapping[write_method])

    def process_line(self, re_search, line, re_match_positions):
        qstat_values = dict()
        m = re.search(re_search, line.strip())
        try:
            job_id, user, job_state, queue = [m.group(x) for x in re_match_positions]
        except AttributeError:
            print line.strip()
            sys.exit(0)
        job_id = job_id.split('.')[0]
        user = user if not options.ANONYMIZE else self.anonymize(user, 'users')
        for key, value in [('JobId', job_id), ('UnixAccount', user), ('S', job_state), ('Queue', queue)]:
            qstat_values[key] = value
        return qstat_values


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
# parser.add_option("-f", "--setCOLORMAPFILE", action="store", type="string", dest="COLORFILE")
parser.add_option("-f", "--setCUSTOMCONFFILE", action="store", type="string", dest="CONFFILE")
parser.add_option("-g", "--get_gecos_via_getent_passwd", action="store_true", dest="GET_GECOS", default=False,
                  help="get user details by issuing getent passwd for all users mentioned in qtop input files.")
parser.add_option("-m", "--noMasking", action="store_true", dest="NOMASKING", default=False,
                  help="Don't mask early empty WNs (default: if the first 30 WNs are unused, counting starts from 31).")
parser.add_option("-o", "--option", action="append", dest="OPTION", type="string", default=None,
                  help="Override respective option in QTOPCONF_YAML file")
parser.add_option("-O", "--onlysavetofile", action="store_true", dest="ONLYSAVETOFILE", default=False,
                  help="Do not print results to stdout")
parser.add_option("-r", "--removeemptycorelines", dest="REM_EMPTY_CORELINES", action="store_true", default=False,
                  help="Set the method used for dumping information, json, yaml, or native python (yaml format)")
parser.add_option("-s", "--SetSourceDir", dest="SOURCEDIR",
                  help="Set the source directory where pbsnodes and qstat reside")
parser.add_option("-T", "--Transpose", dest="TRANSPOSE", action="store_true", default=False,
                  help="mimic shell's watch behaviour")
parser.add_option("-v", "--verbose", dest="verbose", action="count",
                  help="Increase verbosity (specify multiple times for more)")
parser.add_option("-W", "--writemethod", dest="write_method", action="store", default="txtyaml",
                  choices=['txtyaml', 'json'],
                  help="Set the method used for dumping information, json, yaml, or native python (yaml format)")
parser.add_option("-w", "--watch", dest="WATCH", action="store_true", default=False,
                  help="Mimic shell's watch behaviour")
parser.add_option("-y", "--readexistingyaml", action="store_true", dest="YAML_EXISTS", default=False,
                  help="Do not remake yaml input files, read from the existing ones")
# TODO: implement this!
# parser.add_option("-z", "--quiet", action="store_false", dest="verbose", default=True,
#                   help="Don't print status messages to stdout. Not doing anything at the moment.")
parser.add_option("-L", "--sample", action="count", dest="SAMPLE", default=False,
                  help="Create a sample file. A single S creates a tarball with the log, original input files, "
                       "yaml files and output. "
                       "Two 's's additionaly include the qtop_conf yaml file, and qtop source.")

(options, args) = parser.parse_args()
# log_level = logging.WARNING  # default

if not options.verbose:
    log_level = logging.WARN
elif options.verbose == 1:
    log_level = logging.INFO
elif options.verbose >= 2:
    log_level = logging.DEBUG

QTOP_LOGFILE_PATH = QTOP_LOGFILE.rsplit('/', 1)[0]
mkdir_p(QTOP_LOGFILE_PATH)

# This is for writing only to a log file
# logging.basicConfig(filename=QTOP_LOGFILE, filemode='w', level=log_level, format='%(asctime)s - %(levelname)s - %(message)s')

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
fh.setLevel(logging.ERROR) if options.DEBUG else fh.setLevel(logging.CRITICAL)
fh.setFormatter(formatter)
logger.addHandler(fh)
logger.disabled = False  # maybe make this a cmdline switch? -D ?

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

sections_off = {
    1: options.sect_1_off,
    2: options.sect_2_off,
    3: options.sect_3_off
}

sys.excepthook = handle_exception


def get_jobs_info(fn, write_method=options.write_method):
    """
    reads qstat YAML/json file and populates four lists. Returns the lists
    ex read_qstat_yaml
    Common for PBS, OAR, SGE
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


def anonymize_func():
    """
    creates and returns an _anonymize_func object (closure)
    Anonymisation can be used by the user for providing feedback to the developers.
    The logs and the output should no longer contain sensitive information about the clusters ran by the user.
    """
    counters = {}
    stored_dict = {}
    for key in ['users', 'wns', 'qs']:
        counters[key] = count()

    maps = {
        'users': '_anon_user_',
        'wns': '_anon_wn_',
        'qs': '_anon_q_'
    }

    def _anonymize_func(s, a_type):
        """
        d4-p4-04 --> d_anon_wn_0
        d4-p4-05 --> d_anon_wn_1
        biomed017--> b_anon_user_0
        alice    --> a_anon_q_0
        """
        dup_counter = counters[a_type]

        s_type = maps[a_type]
        cnt = '0'
        new_name_parts = [s[0], s_type, cnt]
        if s not in stored_dict:
            cnt = str(dup_counter.next())
            new_name_parts.pop()
            new_name_parts.append(cnt)
        stored_dict.setdefault(s, (''.join(new_name_parts), s_type))
        return stored_dict[s][0]

    return _anonymize_func


def add_to_sample(filepaths_to_add, savepath, sample_file=QTOP_SAMPLE_FILENAME, sample_method=tarfile, subdir=None):
    """
    opens sample_file in path savepath and adds files filepaths_to_add
    """
    sample_out = sample_method.open(os.path.join(savepath, sample_file), mode='a')
    for filepath_to_add in filepaths_to_add:
        path, fn = filepath_to_add.rsplit('/', 1)
        try:
            logging.debug('Adding %s to sample...' % filepath_to_add)
            sample_out.add(filepath_to_add, arcname=fn if not subdir else os.path.join(subdir,fn))
        except tarfile.TarError:  # TODO: test what could go wrong here
            logging.error('There seems to be something wrong with the tarfile. Skipping...')
    else:
        logging.debug('Closing sample...')
        sample_out.close()


# TODO remember to remove here on!
__report_indent = [0]


def report(fn):
    """Decorator to print information about a function
    call for use while debugging.
    Prints function name, arguments, and call number
    when the function is called. Prints this information
    again along with the return value when the function
    returns.
    """

    def wrap(*params, **kwargs):
        call = wrap.callcount = wrap.callcount + 1

        indent = ' ' * __report_indent[0]
        fc = "%s(%s)" % (fn.__name__, ', '.join(
            [a.__repr__() for a in params] +
            ["%s = %s" % (a, repr(b)) for a, b in kwargs.items()]
        ))

        logging.debug("%s%s called [#%s]" % (indent, fc, call))
        __report_indent[0] += 1
        ret = fn(*params, **kwargs)
        __report_indent[0] -= 1
        logging.debug("%s%s returned %s [#%s]" % (indent, fc, repr(ret), call))

        return ret

    wrap.callcount = 0
    return wrap


class JobNotFound(Exception):
    def __init__(self, job_state):
        Exception.__init__(self, "Job state %s not found" % job_state)
        self.job_state = job_state


class NoSchedulerFound(Exception):
    def __init__(self):
        msg = 'No suitable scheduler was found. ' \
              'Please define one in a switch or env variable or in %s' % QTOPCONF_YAML
        Exception.__init__(self, msg)
        logging.critical(msg)


class FileNotFound(Exception):
    def __init__(self, fn):
        msg = "File %s not found.\nMaybe the correct scheduler is not specified?" % fn
        Exception.__init__(self, msg)
        logging.critical(msg)
        self.fn = fn


class SchedulerNotSpecified(Exception):
    pass


class InvalidScheduler(Exception):
    pass
