import logging
import sys
from optparse import OptionParser
import fileutils
import re
import os
import termios
from itertools import cycle
from os.path import realpath
import datetime
from qtop_py.colormap import *
import qtop_py.yaml_parser as yaml
from qtop_py import __version__
from qtop_py.constants import (SYSTEMCONFDIR, QTOPCONF_YAML, QTOP_LOGFILE, USERPATH, MAX_CORE_ALLOWED,
    MAX_UNIX_ACCOUNTS, KEYPRESS_TIMEOUT, FALLBACK_TERMSIZE)


def get_date_obj_from_str(s, now):
    """
    Expects string s to be in either of the following formats:
    yyyymmddTHHMMSS, e.g. 20161118T182300
    HHMM, e.g. 1823 (current day is implied)
    mmddTHHMM, e.g. 1118T1823 (current year is implied)
    If it's in format #3, the the current year is assumed.
    If it's in format #2, either the current or the previous day is assumed,
    depending on whether the time provided is future or past.
    Optional ":/-" separators are also accepted between pretty much anywhere.
    returns a datetime object
    """
    s = ''.join([x for x in s if x not in ':/-'])
    if 'T' in s and len(s) == 15:
        inp_datetime = datetime.datetime.strptime(s, "%Y%m%dT%H%M%S")
    elif len(s) == 4:
        _inp_datetime = datetime.datetime.strptime(s, "%H%M")
        _inp_datetime = now.replace(hour=_inp_datetime.hour, minute=_inp_datetime.minute, second=0)
        inp_datetime = _inp_datetime if now > _inp_datetime else _inp_datetime.replace(day=_inp_datetime.day-1)
    elif len(s) == 9:
        _inp_datetime = datetime.datetime.strptime(s, "%m%dT%H%M")
        inp_datetime = _inp_datetime.replace(year=now.year, second=0)
    else:
        logging.critical('The datetime format provided is incorrect.\n'
                         'Try one of the formats: yyyymmddTHHMMSS, HHMM, mmddTHHMM.')
    return inp_datetime



def _watch_callback(option, opt_str, value, parser):
    """
    This is the official example from optparse for variable arguments
    """
    assert value is None
    value = []

    def is_floatable(str):
        try:
            float(str)
            return True
        except ValueError:
            return False

    for arg in parser.rargs:
        # stop on --foo like options
        if arg[:2] == "--" and len(arg) > 2:
            break
        # stop on -a, but not on -3 or -3.0
        if arg[:1] == "-" and len(arg) > 1 and not is_floatable(arg):
            break
        value.append(arg)
    if not value:  # zero arguments!
        value.append(0)
    else:
        del parser.rargs[:len(value)]
    setattr(parser.values, option.dest, value)


class ColorStr(object):
    """
    ColorStr instances are normal strings with color information attached to them,
    to be used with colorize(), e.g.
    print colorize(s.str, color_func=s.color)
    print colorize(s, mapping=nodestate_to_color, pattern=s.initial)
    """
    def __init__(self, string='', color=''):
        self.str = string
        self.color = color
        self.initial = self.str[0]
        self.index = 0
        self.stop = len(self.str)

    def __str__(self):
        return str(self.str)

    def __repr__(self):
        return repr(self.str)

    def __len__(self):
        return len(self.initial)

    def __iter__(self):
        return self

    def next(self):
        if self.index == self.stop:
            raise StopIteration
        self.index += 1
        # return self.initial
        return self

    def __contains__(self, item):
        return item in self.str

    def __equals__(self, other):
        return self.str == other.str

    @classmethod
    def from_other_color_str(cls, color_str):
        return cls(string=color_str.str)


class CountCalls(object):
    """
    Decorator that keeps track of the number of times a function is called.
    """

    __instances = {}

    def __init__(self, f):
        self.__f = f
        self.__numcalls = 0
        CountCalls.__instances[f] = self

    def __call__(self, *args, **kwargs):
        self.__numcalls += 1
        return self.__f(*args, **kwargs)

    def count(self):
        "Return the number of times the function f was called."
        return CountCalls.__instances[self.__f].__numcalls

    @staticmethod
    def counts():
        "Return a dict of {function: # of calls} for all registered functions."
        return dict([(f.__name__, CountCalls.__instances[f].__numcalls) for f in CountCalls.__instances])


def compress_colored_line(s):
    ## TODO: black sheep
    t = [item for item in re.split(r'\x1b\[0;m', s) if item != '']

    sts = []
    st = []
    colors = []
    prev_code = t[0][:-1]
    colors.append(prev_code)
    for idx, code_letter in enumerate(t):
        code, letter = code_letter[:-1], code_letter[-1]
        if prev_code == code:
            st.append(letter)
        else:
            sts.append(st)
            st = []
            st.append(letter)
            colors.append(code)
        prev_code = code
    sts.append(st)

    final_t = []
    for color, seq in zip(colors, sts):
        final_t.append(color + "".join(seq) + '\x1b[0;m')
    return "".join(final_t)

class Configurator(object):
    def __init__(self):
        self.cmd_options, self.cmd_args = None, None
        self.dynamic_config = dict()
        self.old_attrs = ""
        self.new_attrs = ""
        self.config = {}
        self.options = None
        self.env = {}
        self.user_to_color = None
        self.nodestate_to_color = None
        self.queue_to_color = queue_to_color
        self.QTOP_LOGFILE = QTOP_LOGFILE
        self.change_mapping = cycle([('queue_to_color', 'color by queue'), ('user_to_color', 'color by user')])
        self.h_counter = cycle([0, 1])  # switches between main screen and helpfile

    def auto_config(self):
        self.parse_qtop_cmdline_args()
        self.init_logging()
        self.process_cmd_options()
        self.force_experimental_anonymize()
        self.check_python_version()
        self.adjust_term_attrs()
        self.env.update({"QTOP_SCHEDULER": os.environ.get("QTOP_SCHEDULER")})
        self.env.update({ "QTOP_COLOR": os.environ.get("QTOP_ON")})

    def parse_qtop_cmdline_args(self):
        parser = OptionParser()  # for more details see http://docs.python.org/library/optparse.html

        parser.add_option("-1", "--disablesection1", action="store_true", dest="sect_1_off", default=False,
                          help="Disable first section of qtop, i.e. Job Accounting Summary")
        parser.add_option("-2", "--disablesection2", action="store_true", dest="sect_2_off", default=False,
                          help="Disable second section of qtop, i.e. Worker Node Occupancy")
        parser.add_option("-3", "--disablesection3", action="store_true", dest="sect_3_off", default=False,
                          help="Disable third section of qtop, i.e. User Accounts and Pool Mappings")
        parser.add_option("-a", "--blindremapping", action="store_true", dest="BLINDREMAP", default=False,
                          help="This may be used in situations where node names are not a pure arithmetic seq "
                               "(e.g. rocks clusters)")
        # TODO . Must also anonymise input files, or at least exclude them from the tarball.
        parser.add_option("-A", "--anonymize", action="store_true", dest="ANONYMIZE", default=False,
                          help="Masks unix account names and workernode names for security reasons (sending bug reports etc)."
                               "Temporarily NOT to be used, as scheduler input files are not anonymised yet.")
        parser.add_option("-b", "--batchSystem", action="store", dest="BATCH_SYSTEM", default=None)
        parser.add_option("-c", "--COLOR", action="store", dest="COLOR", default="AUTO", choices=['ON', 'OFF', 'AUTO'],
                          help="Enable/Disable color in qtop output. AUTO detects tty (for watch -d)")
        parser.add_option("-C", "--classic", action="store_true", dest="CLASSIC", default=False,
                          help="tries to mimic legacy qtop display as much as possible")
        parser.add_option("-d", "--debug", action="store_true", dest="DEBUG", default=False,
                          help="print debugging messages in stdout, not just in the log file.")
        parser.add_option("-E", "--export", action="store_true", dest="EXPORT", default=False,
                          help="export cluster data to json")
        parser.add_option("-e", "--experimental", action="store_true", dest="EXPERIMENTAL", default=False,
                          help="this is mandatory for some highly experimental features! Enter at own risk.")
        parser.add_option("-F", "--ForceNames", action="store_true", dest="FORCE_NAMES", default=False,
                          help="force names to show up instead of numbered WNs even for very small numbers of WNs")
        parser.add_option("-f", "--setCUSTOMCONFFILE", action="store", type="string", dest="CONFFILE")
        parser.add_option("-G", "--get_GECOS_via_getent_passwd", action="store_true", dest="GET_GECOS", default=False,
                          help="get user details by issuing getent passwd for all users mentioned in qtop input files.")
        parser.add_option("-m", "--noMasking", action="store_true", dest="NOMASKING", default=False,
                          help="Don't mask early empty WNs (default: if the first 30 WNs are unused, counting starts from 31).")
        parser.add_option("-o", "--option", action="append", dest="OPTION", type="string", default=[],
                          help="Override respective option in QTOPCONF_YAML file")
        parser.add_option("-O", "--onlysavetofile", action="store_true", dest="ONLYSAVETOFILE", default=False,
                          help="Do not print results to stdout")
        parser.add_option("-r", "--removeemptycorelines", dest="REM_EMPTY_CORELINES", action="count", default=False,
                          help="If a whole row consists of not-really-there ('#') core lines, remove the row."
                               "If doubled (-rr), remove the row even if it also consists of free, unused cores ('_').")
        parser.add_option("-R", "--replay", action="callback", dest="REPLAY", callback=_watch_callback,
                          help="instant replay from a specific moment in time for the "
                               "cluster, and for a specified duration. The value "
                               "provided should be in either of the following formats: "
                               "yyyymmddTHHMMSS, e.g. 20161118T182300, (explicit form) "
                               "HHMM, e.g. 1823 (current day is implied),\t\t "
                               "mmddTHHMM, e.g. 1118 T1823(current year is implied).  "
                               "A second value is optional and denotes the desired "
                               "length of the playback, e.g. -R 1823 1m, "
                               "or -R 1800 1h. A default duration of 2m is used, if"
                               "no value is given.")
        parser.add_option("-s", "--SetSourceDir", dest="SOURCEDIR",
                          help="Set the source directory where the batch scheduler output files reside")
        parser.add_option("-S", "--StrictCheck", dest="STRICTCHECK", action="store_true",
                          help="Do a check on the quality of the scheduler output by comparing "
                               "the reported total running jobs against the actual ones found/displayed in qtop")
        parser.add_option("-T", "--Transpose", dest="TRANSPOSE", action="store_true", default=False,
                          help="Rotate matrices' positioning by 90 degrees")
        parser.add_option("-B", "--web", dest="WEB", action="store_true", default=False,
                          help="Enable web interface in 8080")
        parser.add_option("-v", "--verbose", dest="verbose", action="count",
                          help="Increase verbosity (specify multiple times for more)")
        parser.add_option("-V", "--version", dest="version", action="store_true",
                          help="Print qtop version")
        parser.add_option("-w", "--watch", dest="WATCH", action="callback", callback=_watch_callback,
                          help="Mimic shell's watch behaviour. Use with optional argument, e.g. '-w 10' to refresh every 10 sec"
                               "instead of the default which is 2 sec.")
        parser.add_option("-L", "--sample", action="count", dest="SAMPLE", default=False,
                          help="Create a sample file. A single L creates a tarball with the log, scheduler output files, "
                               "qtop output. Two L's additionaly include the qtop_conf yaml file, and qtop qtop_py.")
        # parser.add_option("-f", "--setCOLORMAPFILE", action="store", type="string", dest="COLORFILE")  # TODO

        (self.cmd_options, self.cmd_args) = parser.parse_args()

        if self.cmd_options.version:
            print 'qtop current version: ' + __version__
            sys.exit(0)

    def init_logging(self):
        if not self.cmd_options.verbose:
            log_level = logging.WARN
        elif self.cmd_options.verbose == 1:
            log_level = logging.INFO
        elif self.cmd_options.verbose >= 2:
            log_level = logging.DEBUG

        fileutils.mkdir_p(QTOP_LOGFILE.rsplit('/', 1)[0])  # logfile path

        logger = logging.getLogger()
        logger.setLevel(logging.DEBUG)

        if self.cmd_options.verbose >= 3:
            # this prepends date/time
            formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', "%Y-%m-%d %H:%M:%S")
        else:
            formatter = logging.Formatter('%(levelname)s - %(message)s')

        fh = logging.FileHandler(QTOP_LOGFILE)
        fh.setLevel(log_level)
        fh.setFormatter(formatter)
        logger.addHandler(fh)

        fh = logging.StreamHandler()
        fh.setLevel(logging.ERROR)
        fh.setFormatter(formatter)
        logger.addHandler(fh)
        logger.disabled = False  # TODO: maybe make this a cmdline switch? -D ?

        logging.info("\n" + "=" * 50 + "STARTING NEW LOG ENTRY..." + "=" * 50 + "\n\n")

        logging.debug('Verbosity level = %s' % self.cmd_options.verbose)
        logging.debug("input, output isatty: %s\t%s" % (sys.stdin.isatty(), sys.stdout.isatty()))

    def process_cmd_options(self):
        if self.cmd_options.COLOR == 'AUTO':
            qtop_color = self.env.get("QTOP_COLOR", sys.stdout.isatty())
            self.cmd_options.COLOR = 'ON' if (qtop_color in ("ON", True)) else 'OFF'
        logging.debug("self.cmd_options.COLOR is now set to: %s" % self.cmd_options.COLOR)
        self.cmd_options.REMAP = False  # Default value
        self.dynamic_config['force_names'] = 1 if self.cmd_options.FORCE_NAMES else 0

    def force_experimental_anonymize(self):
        if self.cmd_options.ANONYMIZE and not self.cmd_options.EXPERIMENTAL:
            print 'Anonymize should be ran with --experimental switch!! Exiting...'
            sys.exit(1)

    def check_python_version(self):
        try:
            assert sys.version_info[0] == 2
            assert sys.version_info[1] in (6, 7)
        except AssertionError:
            logging.critical("Only python versions 2.6.x and 2.7.x are supported. Exiting")

            # web.stop() TODO why is that here???
            sys.exit(1)

    def adjust_term_attrs(self):
        """
        Needed for the filtering/sorting options
        """
        if self.cmd_options.WATCH or self.cmd_options.REPLAY:
            try:
                self.old_attrs = termios.tcgetattr(0)
            except termios.error:
                self.old_attrs = ""
            self.new_attrs = self.old_attrs[:]

    def initialize_paths(self):
        initial_cwd = os.getcwd()

        logging.debug('Initial qtop directory: %s' % initial_cwd)
        self.CURPATH = os.path.expanduser(initial_cwd)  # where qtop was invoked from
        self.QTOPPATH = os.path.dirname(realpath(sys.argv[0]))  # where qtop resides
        conf.HELP_FP = os.path.join(self.QTOPPATH, 'helpfile.txt')
        self.initial_cwd = initial_cwd

    def init_dirs(self):
        options = self.cmd_options

        options.SOURCEDIR = realpath(options.SOURCEDIR) if options.SOURCEDIR else None
        logging.debug("User-defined source directory: %s" % options.SOURCEDIR)

        options.workdir = options.SOURCEDIR or self.savepath
        logging.debug('Working directory is now: %s' % options.workdir)

        os.chdir(options.workdir)

    def load_yaml_config(self):
        """
        Loads ./QTOPCONF_YAML into a dictionary and then tries to update the dictionary
        with the same-named conf file found in:
        /env
        $HOME/.local/qtop/
        in that order.
        """
        # TODO: conversion to int should be handled internally in native yaml parser
        # TODO: fix_config_list should be handled internally in native yaml parser
        QTOPPATH = self.QTOPPATH
        CURPATH = self.CURPATH
        self.QTOPCONF_YAML = QTOPCONF_YAML
        self.SYSTEMCONFDIR = SYSTEMCONFDIR
        self.USERPATH = USERPATH

        self.config = yaml.parse(os.path.join(realpath(QTOPPATH), QTOPCONF_YAML))
        logging.info('Default configuration dictionary loaded. Length: %s items' % len(self.config))

        try:
            config_env = yaml.parse(os.path.join(SYSTEMCONFDIR, QTOPCONF_YAML))
        except IOError:
            config_env = {}
            logging.info('%s could not be found in %s/' % (QTOPCONF_YAML, SYSTEMCONFDIR))
        else:
            logging.info('Env %s found in %s/' % (QTOPCONF_YAML, SYSTEMCONFDIR))
            logging.info('Env configuration dictionary loaded. Length: %s items' % len(config_env))

        try:
            config_user = yaml.parse(os.path.join(USERPATH, QTOPCONF_YAML))
        except IOError:
            config_user = {}
            logging.info('User %s could not be found in %s/' % (QTOPCONF_YAML, USERPATH))
        else:
            logging.info('User %s found in %s/' % (QTOPCONF_YAML, USERPATH))
            logging.info('User configuration dictionary loaded. Length: %s items' % len(config_user))

        self.config.update(config_env)
        self.config.update(config_user)

        if self.cmd_options.CONFFILE:
            try:
                config_user_custom = yaml.parse(os.path.join(USERPATH, self.cmd_options.CONFFILE))
            except IOError:
                try:
                    config_user_custom = yaml.parse(os.path.join(CURPATH, self.cmd_options.CONFFILE))
                except IOError:
                    config_user_custom = {}
                    logging.info(
                        'Custom User %s could not be found in %s/ or current dir' % (self.cmd_options.CONFFILE, CURPATH))
                else:
                    logging.info('Custom User %s found in %s/' % (QTOPCONF_YAML, self.CURPATH))
                    logging.info(
                        'Custom User configuration dictionary loaded. Length: %s items' % len(config_user_custom))
            else:
                logging.info('Custom User %s found in %s/' % (QTOPCONF_YAML, USERPATH))
                logging.info('Custom User configuration dictionary loaded. Length: %s items' % len(config_user_custom))
            self.config.update(config_user_custom)

        logging.info('Updated main dictionary. Length: %s items' % len(self.config))

    def process_yaml_config(self):
        config = self.config
        config['possible_ids'] = list(config['possible_ids'])
        symbol_map = dict([(chr(x), x) for x in range(33, 48) + range(58, 64) + range(91, 96) + range(123, 126)])

        if config['user_color_mappings']:  # TODO What if this key is not found in the conf file?
            user_to_color = user_to_color_default.copy()
            [user_to_color.update(d) for d in config['user_color_mappings']]
        else:
            config['user_color_mappings'] = list()

        if config['nodestate_color_mappings']:
            nodestate_to_color = nodestate_to_color_default.copy()
            [nodestate_to_color.update(d) for d in config['nodestate_color_mappings']]
        else:
            config['nodestate_color_mappings'] = list()

        if config['remapping']:  # TODO remove pass and only keep not config?
            pass
        else:
            config['remapping'] = list()

        for symbol in symbol_map:
            config['possible_ids'].append(symbol)

        self.savepath = os.path.realpath(os.path.expandvars(config['savepath']))

        if not os.path.exists(self.savepath):
            fileutils.mkdir_p(self.savepath)
            logging.debug('Directory %s created.' % self.savepath)
        else:
            logging.debug('%s files will be saved in directory %s.'
                          % (config['scheduler'], self.savepath))
        config['savepath'] = self.savepath

        for key in ('transpose_wn_matrices',
                    'fill_with_user_firstletter',
                    'faster_xml_parsing',
                    'vertical_separator_every_X_columns',
                    'overwrite_sample_file'):
            config[key] = eval(config[key])  # TODO config should not be writeable!!

        # TODO config should not be writeable!!
        config['sorting']['reverse'] = eval(config['sorting'].get('reverse', "0"))
        config['ALT_LABEL_COLORS'] = yaml.fix_config_list(
            config['workernodes_matrix'][0]['wn id lines']['alt_label_colors'])
        config['SEPARATOR'] = config['vertical_separator'].translate(None, "'")
        config['USER_CUT_MATRIX_WIDTH'] = int(config['workernodes_matrix'][0]['wn id lines']['user_cut_matrix_width'])
        self.user_to_color = user_to_color
        self.nodestate_to_color = nodestate_to_color
        self.min_masking_threshold = int(config['workernodes_matrix'][0]['wn id lines']['min_masking_threshold'])


    def update_config_with_cmdline_vars(self):
        config = self.config
        options = self.cmd_options

        config['rem_empty_corelines'] = int(config['rem_empty_corelines'])

        for opt in options.OPTION:
            key, val = Configurator.get_key_val_from_option_string(opt)
            val = eval(val) if ('True' in val or 'False' in val) else val
            config[key] = val

        if options.TRANSPOSE:
            config['transpose_wn_matrices'] = not config['transpose_wn_matrices']

        if options.REM_EMPTY_CORELINES:
            config['rem_empty_corelines'] += options.REM_EMPTY_CORELINES

    @staticmethod
    def get_key_val_from_option_string(string):
        key, val = string.split('=')
        return key, val


conf = Configurator()