#!/usr/bin/env python

################################################
#              qtop v.0.8.9-pre                #
#     Licensed under MIT-GPL licenses          #
#                     Sotiris Fragkiskos       #
#                     Fotis Georgatos          #
################################################

from operator import itemgetter
from itertools import izip, izip_longest, cycle
import subprocess
import select
import os
import re
import json
import datetime
try:
    from collections import namedtuple, OrderedDict
except ImportError:
    from legacy.namedtuple import namedtuple
    from legacy.ordereddict import OrderedDict
import os
from os.path import realpath
from signal import signal, SIGPIPE, SIG_DFL
import termios
import contextlib
import glob
import tempfile
import sys
import logging
from constants import (TMPDIR, SYSTEMCONFDIR, QTOPCONF_YAML, QTOP_LOGFILE, savepath, USERPATH, MAX_CORE_ALLOWED,
    MAX_UNIX_ACCOUNTS, KEYPRESS_TIMEOUT, FALLBACK_TERMSIZE)
import fileutils
import utils
from plugins import *
from math import ceil
from colormap import userid_pat_to_color_default, color_to_code, queue_to_color, nodestate_to_color_default
import yaml_parser as yaml
from ui.viewport import Viewport
from serialiser import GenericBatchSystem
from web import Web


# TODO make the following work with py files instead of qtop.colormap files
# if not options.COLORFILE:
#     options.COLORFILE = os.path.expandvars('$HOME/qtop/qtop/qtop.colormap')

@contextlib.contextmanager
def raw_mode(file):
    """
    Simple key listener implementation
    Taken from http://stackoverflow.com/questions/11918999/key-listeners-in-python/11919074#11919074
    Exits program with ^C or ^D
    """
    if options.ONLYSAVETOFILE:
        yield
    else:
        try:
            old_attrs = termios.tcgetattr(file.fileno())
        except:
            yield
        else:
            new_attrs = old_attrs[:]
            new_attrs[3] = new_attrs[3] & ~(termios.ECHO | termios.ICANON)
            try:
                termios.tcsetattr(file.fileno(), termios.TCSADRAIN, new_attrs)
                yield
            finally:
                termios.tcsetattr(file.fileno(), termios.TCSADRAIN, old_attrs)


def load_yaml_config():
    """
    Loads ./QTOPCONF_YAML into a dictionary and then tries to update the dictionary
    with the same-named conf file found in:
    /env
    $HOME/.local/qtop/
    in that order.
    """
    # TODO: conversion to int should be handled internally in native yaml parser
    # TODO: fix_config_list should be handled internally in native yaml parser
    config = yaml.parse(os.path.join(realpath(QTOPPATH), QTOPCONF_YAML))
    logging.info('Default configuration dictionary loaded. Length: %s items' % len(config))

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

    config.update(config_env)
    config.update(config_user)

    if options.CONFFILE:
        try:
            config_user_custom = yaml.parse(os.path.join(USERPATH, options.CONFFILE))
        except IOError:
            try:
                config_user_custom = yaml.parse(os.path.join(CURPATH, options.CONFFILE))
            except IOError:
                config_user_custom = {}
                logging.info('Custom User %s could not be found in %s/ or current dir' % (options.CONFFILE, CURPATH))
            else:
                logging.info('Custom User %s found in %s/' % (QTOPCONF_YAML, CURPATH))
                logging.info('Custom User configuration dictionary loaded. Length: %s items' % len(config_user_custom))
        else:
            logging.info('Custom User %s found in %s/' % (QTOPCONF_YAML, USERPATH))
            logging.info('Custom User configuration dictionary loaded. Length: %s items' % len(config_user_custom))
        config.update(config_user_custom)

    logging.info('Updated main dictionary. Length: %s items' % len(config))

    config['possible_ids'] = list(config['possible_ids'])
    symbol_map = dict([(chr(x), x) for x in range(33, 48) + range(58, 64) + range(91, 96) + range(123, 126)])

    if config['user_color_mappings']:
        userid_pat_to_color = userid_pat_to_color_default.copy()
        [userid_pat_to_color.update(d) for d in config['user_color_mappings']]
    else:
        config['user_color_mappings'] = list()

    if config['nodestate_color_mappings']:
        nodestate_to_color = nodestate_to_color_default.copy()
        [nodestate_to_color.update(d) for d in config['nodestate_color_mappings']]
    else:
        config['nodestate_color_mappings'] = list()

    if config['remapping']:
        pass
    else:
        config['remapping'] = list()
    for symbol in symbol_map:
        config['possible_ids'].append(symbol)

    user_selected_save_path = os.path.realpath(os.path.expandvars(config['savepath']))
    if not os.path.exists(user_selected_save_path):
        fileutils.mkdir_p(user_selected_save_path)
        logging.debug('Directory %s created.' % user_selected_save_path)
    else:
        logging.debug('%s files will be saved in directory %s.' % (config['scheduler'], user_selected_save_path))
    config['savepath'] = user_selected_save_path

    for key in ('transpose_wn_matrices',
                'fill_with_user_firstletter',
                'faster_xml_parsing',
                'vertical_separator_every_X_columns',
                'overwrite_sample_file'):
        config[key] = eval(config[key])  # TODO config should not be writeable!!
    config['sorting']['reverse'] = eval(config['sorting']['reverse'])  # TODO config should not be writeable!!
    config['ALT_LABEL_COLORS'] = yaml.fix_config_list(config['workernodes_matrix'][0]['wn id lines']['alt_label_colors'])
    config['SEPARATOR'] = config['vertical_separator'].translate(None, "'")
    config['USER_CUT_MATRIX_WIDTH'] = int(config['workernodes_matrix'][0]['wn id lines']['user_cut_matrix_width'])
    return config, userid_pat_to_color, nodestate_to_color


def calculate_term_size(config, FALLBACK_TERM_SIZE):
    """
    Gets the dimensions of the terminal window where qtop will be displayed.
    """
    fallback_term_size = config.get('term_size', FALLBACK_TERM_SIZE)
    try:
        term_height, term_columns = os.popen('stty size', 'r').read().split()
        logging.debug('0.this resulted in v, h:%s, %s' % (term_height, term_columns))
    except ValueError:
        logging.warn("Failed to autodetect terminal size. (Running in an IDE?) Trying values in %s." % QTOPCONF_YAML)
        try:
            term_height, term_columns = viewport.get_term_size()
            if not all(term_height, term_columns):
                raise ValueError
        except ValueError:
            try:
                term_height, term_columns = yaml.fix_config_list(viewport.get_term_size())
            except KeyError:
                term_height, term_columns = fallback_term_size
        except (KeyError, TypeError):  # TypeError if None was returned i.e. no setting in QTOPCONF_YAML
            term_height, term_columns = fallback_term_size

    logging.debug('Set terminal size is: %s * %s' % (term_height, term_columns))
    return int(term_height), int(term_columns)


def finalize_filepaths_schedulercommands(options, config):
    """
    returns a dictionary with contents of the form
    {fn : (filepath, schedulercommand)}, e.g.
    {'pbsnodes_file': ('<TMPDIR>/qtop_results_$USER/pbsnodes_a.txt', 'pbsnodes -a')}
    if the -s switch (set sourcedir) has been invoked, or
    {'pbsnodes_file': ('<TMPDIR>/qtop_results_$USER/pbsnodes_a<some_pid>.txt', 'pbsnodes -a')}
    if ran without the -s switch.
    TMPDIR is defined in constants.py
    """
    d = dict()
    # date = time.strftime("%Y%m%d")  #TODO
    # fn_append = "_" + str(date) if not options.SOURCEDIR else ""
    fn_append = "_" + str(os.getpid()) if not options.SOURCEDIR else ""
    for fn, path_command in config['schedulers'][scheduler].items():
        path, command = path_command.strip().split(', ')
        path = path % {"savepath": options.workdir, "pid": fn_append}
        command = command % {"savepath": options.workdir}
        d[fn] = (path, command)
    return d


def auto_get_avail_batch_system(config):
    """
    If the auto option is set in either env variable QTOP_SCHEDULER, QTOPCONF_YAML or in cmdline switch -b,
    qtop tries to determine which of the known batch commands are available in the current system.
    """
    # TODO pbsnodes etc should not be hardcoded!
    for (system, batch_command) in config['signature_commands'].items():
        NOT_FOUND = subprocess.call(['which', batch_command], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        if not NOT_FOUND:
            if system != 'demo':
                logging.debug('Auto-detected scheduler: %s' % system)
                return system

    raise SchedulerNotSpecified


def execute_shell_batch_commands(batch_system_commands, filenames, _file):
    """
    scheduler-specific commands are invoked from the shell and their output is saved *atomically* to files,
    as defined by the user in QTOPCONF_YAML
    """
    _batch_system_command = batch_system_commands[_file].strip()
    with tempfile.NamedTemporaryFile('w', dir=savepath, delete=False) as fin:
        logging.debug('Command: "%s" -- result will be saved in: %s' % (_batch_system_command, filenames[_file]))
        logging.debug('\tFile state before subprocess call: %(fin)s' % {"fin": fin})
        logging.debug('\tWaiting on subprocess.call...')

        command = subprocess.Popen(_batch_system_command, stdout=fin, stderr=subprocess.PIPE, shell=True)
        error = command.communicate()[1]
        command.wait()
        if error:
            logging.exception('A message from your shell: %s' % error)
            logging.critical('%s could not be executed. Maybe try "module load %s"?' % (_batch_system_command, scheduler))

            web.stop()
            sys.exit(1)
        tempname = fin.name
        logging.debug('File state after subprocess call: %(fin)s' % {"fin": fin})
    os.rename(tempname, filenames[_file])

    return filenames[_file]


def get_detail_of_name(account_jobs_table):
    """
    Reads file $HOME/.local/qtop/getent_passwd.txt or whatever is put in QTOPCONF_YAML
    and extracts the fullname of the users. This shall be printed in User Accounts
    and Pool Mappings.
    """
    extract_info = config.get('extract_info', None)
    if not extract_info:
        return dict()

    sep = ':'
    field_idx = int(extract_info.get('field_to_use', 5))
    regex = extract_info.get('regex', None)

    if options.GET_GECOS:
        users = ' '.join([line[4] for line in account_jobs_table])
        passwd_command = extract_info.get('user_details_realtime') % users
        passwd_command = passwd_command.split()
    else:
        passwd_command = extract_info.get('user_details_cache').split()
        passwd_command[-1] = os.path.expandvars(passwd_command[-1])

    p = subprocess.Popen(passwd_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    output, err = p.communicate("something here")
    if 'No such file or directory' in err:
        logging.warn('You have to set a proper command to get the passwd file in your %s file.' % QTOPCONF_YAML)
        logging.warn('Error returned by getent: %s\nCommand issued: %s' % (err, passwd_command))

    detail_of_name = dict()
    for line in output.split('\n'):
        try:
            user, field = line.strip().split(sep)[0:field_idx:field_idx - 1]
        except ValueError:
            break
        else:
            try:
                detail = eval(regex)
            except (AttributeError, TypeError):
                detail = field.strip()
            finally:
                detail_of_name[user] = detail
    return detail_of_name


def get_input_filenames(INPUT_FNs_commands):
    """
    If the user didn't specify --via the -s switch-- a dir where ready-made data files already exist,
    the appropriate batch commands are executed, as indicated in QTOPCONF,
    and results are saved with the respective filenames.
    """

    filenames = dict()
    batch_system_commands = dict()
    for _file in INPUT_FNs_commands:
        filenames[_file], batch_system_commands[_file] = INPUT_FNs_commands[_file]

        if not options.SOURCEDIR:
            filenames[_file] = execute_shell_batch_commands(batch_system_commands, filenames, _file)

        if not os.path.isfile(filenames[_file]):
            raise fileutils.FileNotFound(filenames[_file])
    return filenames


def get_key_val_from_option_string(string):
    key, val = string.split('=')
    return key, val


def check_python_version():
    try:
        assert sys.version_info[0] == 2
        assert sys.version_info[1] in (6,7)
    except AssertionError:
        logging.critical("Only python versions 2.6.x and 2.7.x are supported. Exiting")

        web.stop()
        sys.exit(1)


def control_movement(viewport, read_char):
    """
    Basic vi-like movement is implemented for the -w switch (linux watch-like behaviour for qtop).
    h, j, k, l for left, down, up, right, respectively.
    Both g/G and Shift+j/k go to top/bottom of the matrices
    0 and $ go to far left/right of the matrix, respectively.
    r resets the screen to its initial position (if you've drifted away from the vieweable part of a matrix).
    q quits qtop.
    """
    pressed_char_hex = '%02x' % ord(read_char)  # read_char has an initial value that resets the display ('72')

    if pressed_char_hex in ['6a', '20']:  # j, spacebar
        logging.debug('v_start: %s' % viewport.v_start)
        if viewport.scroll_down():
            logging.info('Going down...')
        else:
            logging.info('Staying put')

    elif pressed_char_hex in ['6b', '7f']:  # k, Backspace
        if viewport.scroll_up():
            logging.info('Going up...')
        else:
            logging.info('Staying put')

    elif pressed_char_hex in ['6c']:  # l
        logging.info('Going right...')
        viewport.scroll_right()

    elif pressed_char_hex in ['24']:  # $
        logging.info('Going far right...')
        viewport.scroll_far_right()
        logging.info('h_start: %s' % viewport.h_start)
        logging.info('max_line_len: %s' % max_line_len)
        logging.info('config["term_size"][1] %s' % viewport.h_term_size)
        logging.info('h_stop: %s' % viewport.h_stop)

    elif pressed_char_hex in ['68']:  # h
        logging.info('Going left...')
        viewport.scroll_left()

    elif pressed_char_hex in ['30']:   # 0
        logging.info('Going far left...')
        viewport.scroll_far_left()

    elif pressed_char_hex in ['4a', '47']:  # S-j, G
        logging.info('Going to the bottom...')
        logging.debug('v_start: %s' % viewport.v_start)
        if viewport.scroll_bottom():
            logging.info('Going to the bottom...')
        else:
            logging.info('Staying put')

    elif pressed_char_hex in ['4b', '67']:  # S-k, g
        logging.info('Going to the top...')
        logging.debug('v_start: %s' % viewport.v_start)
        viewport.scroll_top()

    elif pressed_char_hex in ['72']:  # r
        viewport.reset_display()

    elif pressed_char_hex in ['71']:  # q
        print '  Exiting...'

        web.stop()
        sys.exit(0)

    logging.debug('Area Displayed: (h_start, v_start) --> (h_stop, v_stop) '
                  '\n\t(%(h_start)s, %(v_start)s) --> (%(h_stop)s, %(v_stop)s)' %
                  {'v_start': viewport.v_start, 'v_stop': viewport.v_stop,
                   'h_start': viewport.h_start, 'h_stop': viewport.h_stop})


def fetch_scheduler_files(options, config):
    INPUT_FNs_commands = finalize_filepaths_schedulercommands(options, config)
    scheduler_output_filenames = get_input_filenames(INPUT_FNs_commands)
    return scheduler_output_filenames


def decide_batch_system(cmdline_switch, env_var, config_file_batch_option, schedulers, available_batch_systems, config):
    """
    Qtop first checks in cmdline switches, environmental variables and the config files, in this order,
    for the scheduler type. If it's not indicated and "auto" is, it will attempt to guess the scheduler type
    from the scheduler shell commands available in the linux system.
    """
    avail_systems = available_batch_systems.keys() + ['auto']
    if cmdline_switch and cmdline_switch.lower() not in avail_systems:
        logging.critical("Selected scheduler system not supported. Available choices are %s." % ", ".join(avail_systems))
        logging.critical("For help, try ./qtop.py --help")
        logging.critical("Log file created in %s" % os.path.expandvars(QTOP_LOGFILE))
        raise InvalidScheduler
    for scheduler in (cmdline_switch, env_var, config_file_batch_option):
        if scheduler is None:
            continue
        scheduler = scheduler.lower()

        if scheduler == 'auto':
            scheduler = auto_get_avail_batch_system(config)
            logging.debug('Selected scheduler is %s' % scheduler)
            return scheduler
        elif scheduler in schedulers:
            logging.info('User-selected scheduler: %s' % scheduler)
            return scheduler
        elif scheduler and scheduler not in schedulers:  # a scheduler that does not exist is inputted
            raise NoSchedulerFound
    else:
        raise NoSchedulerFound


def get_output_size(max_height, max_line_len, output_fp):
    """
    Returns the char dimensions of the entirety of the qtop output file
    """
    ansi_escape = re.compile(r'\x1b[^m]*m')  # matches ANSI escape characters

    if not max_height:
        with open(output_fp, 'r') as f:
            max_height = len(f.readlines())
            if not max_height:
                raise ValueError("There is no output from qtop *whatsoever*. Weird.")

    max_line_len = max(len(ansi_escape.sub('', line.strip())) for line in open(output_fp, 'r')) \
        if not max_line_len else max_line_len

    logging.debug('Total nr of lines: %s' % viewport.max_height)
    logging.debug('Max line length: %s' % max_line_len)

    return max_height, max_line_len


def update_config_with_cmdline_vars(options, config):
    for opt in options.OPTION:
        key, val = get_key_val_from_option_string(opt)
        val = eval(val) if ('True' in val or 'False' in val) else val
        config[key] = val

    if options.TRANSPOSE:
        config['transpose_wn_matrices'] = not config['transpose_wn_matrices']

    return config


def attempt_faster_xml_parsing(config):
    if config['faster_xml_parsing']:
        try:
            from lxml import etree
        except ImportError:
            logging.warn('Module lxml is missing. Try issuing "pip install lxml". Reverting to xml module.')
            from xml.etree import ElementTree as etree


def init_dirs(options):
    options.SOURCEDIR = realpath(options.SOURCEDIR) if options.SOURCEDIR else None
    logging.debug("User-defined source directory: %s" % options.SOURCEDIR)
    options.workdir = options.SOURCEDIR or config['savepath']
    logging.debug('Working directory is now: %s' % options.workdir)
    os.chdir(options.workdir)
    return options


def wait_for_keypress_or_autorefresh(viewport, KEYPRESS_TIMEOUT=1):
    """
    This will make qtop wait for user input for a while,
    otherwise it will auto-refresh the display
    """
    read_char = 'r'  # initial value, resets view position to beginning

    while sys.stdin in select.select([sys.stdin], [], [], KEYPRESS_TIMEOUT)[0]:
        read_char = sys.stdin.read(1)
        if read_char:
            logging.debug('Pressed %s' % read_char)
            break
    else:
        state = viewport.get_term_size()
        viewport.set_term_size(*calculate_term_size(config, FALLBACK_TERMSIZE))
        new_state = viewport.get_term_size()
        read_char = '\n' if (state == new_state) else 'r'
        logging.debug("Auto-advancing by pressing <Enter>")

    return read_char


def ensure_worker_nodes_have_qnames(worker_nodes, jobs_dict):
# def ensure_worker_nodes_have_qnames(worker_nodes, job_ids, job_queues):
    """
    This gets the first letter of the queues associated with each worker node.
    SGE systems already contain this information.
    """
    if (not worker_nodes) or ('qname' in worker_nodes[0]):
        return worker_nodes

    for worker_node in worker_nodes:
        my_jobs = worker_node['core_job_map'].values()
        my_queues = set(jobs_dict[job_id][2] for job_id in my_jobs)
        worker_node['qname'] = list(my_queues)
    return worker_nodes

def assign_color_to_each_qname(worker_nodes):
    q_to_color = dict()
    for worker_node in worker_nodes:
        worker_node['qname'] = [q[0] for q in worker_node['qname']]

def keep_queue_initials_only_and_colorize(worker_nodes, queue_to_color):
    # TODO remove monstrosity!
    for worker_node in worker_nodes:
        color_q_list = []
        for queue in worker_node['qname']:
            color_q = utils.ColorStr(queue, color=queue_to_color.get(queue, ''))
            color_q_list.append(color_q)
        worker_node['qname'] = color_q_list
    return worker_nodes


def colorize_nodestate(worker_nodes, nodestate_to_color, ffunc):
    # TODO remove monstrosity!
    for worker_node in worker_nodes:
        full_nodestate = worker_node['state']  # actual node state
        total_color_nodestate = []
        for nodestate in worker_node['state']:  # split nodestate for displaying purposes
            color_nodestate = utils.ColorStr(nodestate, color=nodestate_to_color.get(full_nodestate, ''))
            total_color_nodestate.append(color_nodestate)
        worker_node['state'] = total_color_nodestate
    return worker_nodes


def discover_qtop_batch_systems():
    batch_systems = set()

    # Find all the classes that extend GenericBatchSystem
    to_scan = [GenericBatchSystem]
    while to_scan:
        parent = to_scan.pop()
        for child in parent.__subclasses__():
            if child not in batch_systems:
                batch_systems.add(child)
                to_scan.append(child)

    # Extract those class's mnemonics
    available_batch_systems = {}
    for batch_system in batch_systems:
        mnemonic = batch_system.get_mnemonic()
        assert mnemonic
        assert mnemonic not in available_batch_systems, "Duplicate for mnemonic: '%s'" % mnemonic
        available_batch_systems[mnemonic] = batch_system

    return available_batch_systems


def process_options(options):
    if options.COLOR == 'AUTO':
        options.COLOR = 'ON' if (os.environ.get("QTOP_COLOR", sys.stdout.isatty()) in ("ON", True)) else 'OFF'
    logging.debug("options.COLOR is now set to: %s" % options.COLOR)
    options.REMAP = False  # Default value
    NAMED_WNS = 1 if options.FORCE_NAMES else 0
    return options, NAMED_WNS


# def handle_exception(exc_type, exc_value, exc_traceback):
#     """
#     This, when replacing sys.excepthook,
#     will log uncaught exceptions to the logging module instead
#     of printing them to stdout.
#     """
#     if issubclass(exc_type, KeyboardInterrupt):
#         sys.__excepthook__(exc_type, exc_value, exc_traceback)
#         return
#
#     logging.error("Uncaught exception", exc_info=(exc_type, exc_value, exc_traceback))


class WNOccupancy(object):
    def __init__(self, cluster, config, document, userid_pat_to_color):
        self.cluster = cluster
        self.config = config
        self.document = document
        self.account_jobs_table = list()
        self.user_to_id = dict()
        self.user_names, self.job_states, self.job_queues = self._get_usernames_states_queues(document.jobs_dict)

        self.calculate(document, userid_pat_to_color)

    def _get_usernames_states_queues(self, jobs_dict):
        user_names, job_states, job_queues = list(), list(), list()
        for key, value in jobs_dict.items():
            user_names.append(value.user_name)
            job_states.append(value.job_state)
            job_queues.append(value.job_queue)
        return user_names, job_states, job_queues

    def get_dyn_var(self, var_name):
        return getattr(self, var_name)

    def calculate(self, document, userid_pat_to_color):
        """
        Prints the Worker Nodes Occupancy table.
        if there are non-uniform WNs in pbsnodes.yaml, e.g. wn01, wn02, gn01, gn02, ...,  remapping is performed.
        Otherwise, for uniform WNs, i.e. all using the same numbering scheme, wn01, wn02, ... proceeds as normal.
        Number of Extra tables needed is calculated inside the calc_all_wnid_label_lines function below
        """
        if not self.cluster:
            return self  # TODO fix
        # document.jobs_dict => job_id: job name/state/queue

        user_alljobs_sorted_lot = self._produce_user_lot(self.user_names)
        user_to_id = self._create_id_for_users(user_alljobs_sorted_lot)
        user_job_per_state_counts = self._calculate_user_job_counts(self.user_names, self.job_states, user_alljobs_sorted_lot,
                                                                    user_to_id)
        _account_jobs_table = self._create_sort_acct_jobs_table(user_job_per_state_counts, user_alljobs_sorted_lot, user_to_id)
        self.account_jobs_table, self.user_to_id = self._create_account_jobs_table(user_to_id, _account_jobs_table)
        self.uid_to_uid_re_pat = self.make_uid_to_uid_re_pat(userid_pat_to_color)

        # TODO extract to another class?
        self.print_char_start, self.print_char_stop, self.extra_matrices_nr = self.find_matrices_width()
        self.wn_vert_labels = self.calc_all_wnid_label_lines(NAMED_WNS)

        # For-loop below only for user-inserted/customizeable values.
        for yaml_key, part_name, systems in yaml.get_yaml_key_part(config, scheduler, outermost_key='workernodes_matrix'):
            if scheduler in systems:
                self.__setattr__(part_name, self.calc_general_mult_attr_line(part_name, yaml_key, config))

        self.core_user_map = self._calc_core_userid_matrix(job_ids, user_names)

    def _create_account_jobs_table(self, user_to_id, account_jobs_table):
        # TODO: unix account id needs to be recomputed at this point. fix.
        for quintuplet, new_uid in zip(account_jobs_table, config['possible_ids']):
            unix_account = quintuplet[-1]
            quintuplet[0] = user_to_id[unix_account] = unix_account[0] if config['fill_with_user_firstletter'] else new_uid

        return account_jobs_table, user_to_id

    def _create_sort_acct_jobs_table(self, user_job_per_state_counts, user_all_jobs_sorted, user_to_id):
        """Calculates what is actually below the id|  R + Q /all | unix account etc line"""
        account_jobs_table = []
        for user_alljobs in user_all_jobs_sorted:
            user, alljobs_of_user = user_alljobs
            account_jobs_table.append(
                [
                    user_to_id[user],
                    user_job_per_state_counts['running_of_user'][user],
                    user_job_per_state_counts['queued_of_user'][user],
                    alljobs_of_user,
                    user
                ]
            )
        account_jobs_table.sort(key=itemgetter(3, 4), reverse=True)  # sort by All jobs, then unix account
        return account_jobs_table

    def _create_user_job_counts(self, user_names, job_states, state_abbrevs):
        """
        counting of e.g. R, Q, C, W, E attached to each user
        """
        user_job_per_state_counts = dict()
        for state_of_user in state_abbrevs.values():
            user_job_per_state_counts[state_of_user] = dict()

        for user_name, job_state in zip(user_names, job_states):
            try:
                x_of_user = state_abbrevs[job_state]
            except KeyError:
                raise JobNotFound(job_state)

            user_job_per_state_counts[x_of_user][user_name] = user_job_per_state_counts[x_of_user].get(user_name, 0) + 1

        for user_name in user_job_per_state_counts['running_of_user']:
            [user_job_per_state_counts[x_of_user].setdefault(user_name, 0) for x_of_user in user_job_per_state_counts if
             x_of_user != 'running_of_user']

        return user_job_per_state_counts

    def _produce_user_lot(self, _user_names):
        """
        Produces a list of tuples (lot) of the form (user account, all jobs count) in descending order.
        Used in the user accounts and poolmappings table
        """
        user_to_alljobs_count = {}
        for user_name in set(_user_names):
            user_to_alljobs_count[user_name] = _user_names.count(user_name)

        user_alljobs_sorted_lot = sorted(user_to_alljobs_count.items(), key=itemgetter(1), reverse=True)
        return user_alljobs_sorted_lot

    def _calculate_user_job_counts(self, user_names, job_states, user_alljobs_sorted_lot, user_to_id):
        """
        Calculates and prints what is actually below the id|  R + Q /all | unix account etc line
        :param user_names: list
        :param job_states: list
        :return: (list, list, dict)
        """
        self.config = self._expand_useraccounts_symbols(self.config, user_names)
        state_abbrevs = self.config['state_abbreviations'][scheduler]

        try:
            user_job_per_state_counts = self._create_user_job_counts(user_names, job_states, state_abbrevs)
        except JobNotFound as e:
            logging.critical('Job state %s not found. You may wish to add '
                             'that node state inside %s in state_abbreviations section.\n' % (e.job_state, QTOPCONF_YAML))

        for state_abbrev, state_of_user in state_abbrevs.items():
            missing_uids = set(user_to_id).difference(user_job_per_state_counts[state_of_user])
            [user_job_per_state_counts[state_of_user].setdefault(missing_uid, 0) for missing_uid in missing_uids]

        return user_job_per_state_counts

    def _expand_useraccounts_symbols(self, config, user_list):
        """
        In case there are more users than the sum number of all numbers and small/capital letters of the alphabet
        """
        if len(user_list) > MAX_UNIX_ACCOUNTS:
            for i in xrange(MAX_UNIX_ACCOUNTS, len(user_list) + MAX_UNIX_ACCOUNTS):
                config['possible_ids'].append(str(i)[0])
        return config

    def _create_id_for_users(self, user_alljobs_sorted_lot):
        user_to_id = {}
        for id_, user_allcount in enumerate(user_alljobs_sorted_lot):
            if self.config['fill_with_user_firstletter']:
                user_to_id[user_allcount[0]] = user_allcount[0][0]
            else:
                user_to_id[user_allcount[0]] = self.config['possible_ids'][id_]

        return user_to_id

    def make_uid_to_uid_re_pat(self, userid_pat_to_color):
        """
        First strips the numbers off of the unix accounts and tries to match this against the given color table in colormap.
        Additionally, it will try to apply the regex rules given by the user in qtopconf.yaml, overriding the colormap.
        The last matched regex is valid.
        If no matching was possible, there will be no coloring applied.
        """
        uid_to_uid_re_pat = {}
        for line in self.account_jobs_table:
            uid, user = line[0], line[4]
            account_letters = re.search('[A-Za-z]+', user).group(0)
            for re_account in userid_pat_to_color:
                match = re.search(re_account, user)
                if match is None:
                    continue  # keep trying
                account_letters = re_account  # colors the text according to the regex given by the user in qtopconf

            uid_to_uid_re_pat[uid] = account_letters if account_letters in userid_pat_to_color else 'NoPattern'

        # TODO: remove these from here
        uid_to_uid_re_pat[self.config['non_existent_node_symbol']] = '#'
        uid_to_uid_re_pat['_'] = '_'
        uid_to_uid_re_pat[self.config['SEPARATOR']] = 'account_not_colored'
        return uid_to_uid_re_pat

    def find_matrices_width(self, DEADWEIGHT=11):
        """
        masking/clipping functionality: if the earliest node number is high (e.g. 130), the first 129 WNs need not show up.
        case 1: wn_number is RemapNr, WNList is WNListRemapped
        case 2: wn_number is BiggestWrittenNode, WNList is WNList
        DEADWEIGHT is the space taken by the {__XXXX__} labels on the right of the CoreX map

        uses cluster.highest_wn, cluster.workernode_list
        """
        start = 0
        wn_number = cluster.highest_wn
        workernode_list = cluster.workernode_list
        term_columns = viewport.h_term_size
        min_masking_threshold = int(config['workernodes_matrix'][0]['wn id lines']['min_masking_threshold'])
        if options.NOMASKING and min(workernode_list) > min_masking_threshold:
            # exclude unneeded first empty nodes from the matrix
            start = min(workernode_list) - 1

        # Extra matrices may be needed if the WNs are more than the screen width can hold.
        if wn_number > start:  # start will either be 1 or (masked >= config['min_masking_threshold'] + 1)
            extra_matrices_nr = int(ceil(abs(wn_number - start) / float(term_columns - DEADWEIGHT))) - 1
        elif options.REMAP:  # was: ***wn_number < start*** and len(cluster.node_subclusters) > 1:  # Remapping
            extra_matrices_nr = int(ceil(wn_number / float(term_columns - DEADWEIGHT))) - 1
        else:
            raise (NotImplementedError, "Not foreseen")

        if config['USER_CUT_MATRIX_WIDTH']:  # if the user defines a custom cut (in the configuration file)
            stop = start + config['USER_CUT_MATRIX_WIDTH']
            self.extra_matrices_nr = wn_number / config['USER_CUT_MATRIX_WIDTH']
        elif extra_matrices_nr:  # if more matrices are needed due to lack of space, cut every matrix so that if fits to screen
            stop = start + term_columns - DEADWEIGHT
            # wns_occupancy['extra_matrices_nr'] = extra_matrices_nr
        else:  # just one matrix, small cluster!
            stop = start + wn_number
            extra_matrices_nr = 0

        logging.debug('reported term_columns, DEADWEIGHT: %s\t%s' % (term_columns, DEADWEIGHT))
        logging.debug('reported start/stop lengths: %s--> %s' % (start, stop))
        return start, stop, extra_matrices_nr

    def calc_all_wnid_label_lines(self, NAMED_WNS):  # (total_wn) in case of multiple cluster.node_subclusters
        """
        calculates the Worker Node ID number line widths. expressed by hxxxxs in the following form, e.g. for hundreds of nodes:
        '1': "00000000..."
        '2': "0000000001111111..."
        '3': "12345678901234567..."
        """
        highest_wn = self.cluster.highest_wn
        if NAMED_WNS or options.FORCE_NAMES:
            workernode_dict = self.cluster.workernode_dict
            hosts = [state_corejob_dn['host'] for _, state_corejob_dn in workernode_dict.items()]
            node_str_width = len(max(hosts, key=len))
            wn_vert_labels = OrderedDict((str(place), []) for place in range(1, node_str_width + 1))
            for node in workernode_dict:
                host = workernode_dict[node]['host']
                extra_spaces = node_str_width - len(host)
                string = "".join(" " * extra_spaces + host)
                for place in range(node_str_width):
                    wn_vert_labels[str(place + 1)].append(string[place])
        else:
            node_str_width = len(str(highest_wn))  # 4
            wn_vert_labels = dict([(str(place), []) for place in range(1, node_str_width + 1)])
            for nr in range(1, highest_wn + 1):
                extra_spaces = node_str_width - len(str(nr))  # 4 - 1 = 3, for wn0001
                string = "".join("0" * extra_spaces + str(nr))
                for place in range(1, node_str_width + 1):
                    wn_vert_labels[str(place)].append(string[place - 1])

        for wn in wn_vert_labels.keys():
            wn_vert_labels[wn] = "".join(wn_vert_labels[wn])
        return wn_vert_labels

    def calc_general_mult_attr_line(self, part_name, yaml_key, config):  # NEW
        elem_identifier = [d for d in config['workernodes_matrix'] if part_name in d][0]  # jeeez
        part_name_idx = config['workernodes_matrix'].index(elem_identifier)
        user_max_len = int(config['workernodes_matrix'][part_name_idx][part_name]['max_len'])
        try:
            real_max_len = max([len(self.cluster.workernode_dict[_node][yaml_key]) for _node in self.cluster.workernode_dict])
        except KeyError:
            logging.critical("%s lines in the matrix are not supported for %s systems. "
                             "Please remove appropriate lines from conf file. Exiting..."
                             % (part_name, config['scheduler']))

            web.stop()
            sys.exit(1)
        min_len = min(user_max_len, real_max_len)
        max_len = max(user_max_len, real_max_len)
        if real_max_len > user_max_len:
            logging.warn(
                "Some longer %(attr)ss have been cropped due to %(attr)s length restriction by user" % {"attr": part_name})

        # initialisation of lines
        multiline_map = OrderedDict()
        for line_nr in range(1, min_len + 1):
            multiline_map['attr%sline' % str(line_nr)] = []

        for _node in self.cluster.workernode_dict:
            node_attrs = self.cluster.workernode_dict[_node]
            # distribute state, qname etc to lines
            for attr_line, ch in izip_longest(multiline_map, node_attrs[yaml_key], fillvalue=' '):
                try:
                    if ch == ' ':
                        ch = utils.ColorStr(' ')
                    multiline_map[attr_line].append(ch)
                except KeyError:
                    break
                    # TODO: is this really needed?: self.cluster.workernode_dict[_node]['state_column']

        for line, attr_line in enumerate(multiline_map, 1):
            # multiline_map[attr_line] = ''.join(multiline_map[attr_line])
            if line == user_max_len:
                break
        return multiline_map

    def _calc_core_userid_matrix(self, job_ids, user_names):
        core_user_map = OrderedDict()
        user_of_job_id = dict(izip(job_ids, user_names))
        # if not user_of_job_id:
        #     return
        for core_nr in self.cluster.core_span:
            core_user_map['Core%svector' % str(core_nr)] = []  # Cpu0line, Cpu1line, Cpu2line, .. = '','','', ..

        for _node in self.cluster.workernode_dict:
            core_user_map = self._fill_node_cores_column(_node, core_user_map, self.user_to_id, self.cluster.core_span,
                                                         user_of_job_id)

        for coreline in core_user_map:
            core_user_map[coreline] = ''.join(core_user_map[coreline])

        return core_user_map

    def _fill_node_cores_column(self, _node, core_user_map, user_to_id, _core_span, user_of_job_id):
        """
        Calculates the actual contents of the map by filling in a status string for each CPU line
        """
        state_np_corejob = cluster.workernode_dict[_node]
        state = state_np_corejob['state']
        np = state_np_corejob['np']
        corejobs = state_np_corejob.get('core_job_map', dict())
        non_existent_node_symbol = config['non_existent_node_symbol']

        if state == '?':  # for non-existent machines
            for core_line in core_user_map:
                core_user_map[core_line] += [non_existent_node_symbol]
        else:
            node_cores = [str(x) for x in range(int(np))]
            node_free_cores = node_cores[:]

            for (user, core) in self._assigned_corejobs(corejobs, user_of_job_id):
                id_ = str(user_to_id[user])
                core_user_map['Core' + str(core) + 'vector'] += [id_]
                node_free_cores.remove(core)  # this is an assigned core, hence it doesn't belong to the node's free cores

            non_existent_cores = [item for item in _core_span if item not in node_cores]

            '''
            One of the two dimensions of the matrix is determined by the highest-core WN existing. If other WNs have less cores,
            these positions are filled with '#'s (or whatever is defined in config['non_existent_node_symbol']).
            '''
            for core in node_free_cores:
                core_user_map['Core' + str(core) + 'vector'] += ['_']
            for core in non_existent_cores:
                core_user_map['Core' + str(core) + 'vector'] += [non_existent_node_symbol]

        cluster.workernode_dict[_node]['core_user_vector'] = "".join([core_user_map[line][-1] for line in core_user_map])

        return core_user_map

    def _assigned_corejobs(self, corejobs, user_of_job_id):
        """
        Generator that yields only those core-job pairs that successfully match to a user
        """
        for core in corejobs:
            job = str(corejobs[core])
            try:
                user = user_of_job_id[job]
            except KeyError as KeyErrorValue:
                logging.critical('There seems to be a problem with the qstat output. '
                                 'A Job (ID %s) has gone rogue. '
                                 'Please check with the SysAdmin.' % (str(KeyErrorValue)))
                raise KeyError
            else:
                yield user, str(core)

    def is_matrix_coreless(self):
        print_char_start = self.print_char_start
        print_char_stop = self.print_char_stop
        non_existent_node_symbol = self.config['non_existent_node_symbol']
        lines = []
        core_user_map = self.core_user_map
        for ind, k in enumerate(core_user_map):
            cpu_core_line = core_user_map['Core' + str(ind) + 'vector'][print_char_start:print_char_stop]
            if options.REM_EMPTY_CORELINES and \
                    (
                                (non_existent_node_symbol * (print_char_stop - print_char_start) == cpu_core_line) or \
                                    (non_existent_node_symbol * (len(cpu_core_line)) == cpu_core_line)
                    ):
                lines.append('*')

        return len(lines) == len(core_user_map)

    def strict_check_jobs(self, cluster):
        counted_jobs = WNOccupancy._count_jobs_strict(self.core_user_map)
        if counted_jobs != cluster.total_running_jobs:
            print "Counted jobs (%s) -- Total running jobs reported (%s) MISMATCH!" % (counted_jobs, cluster.total_running_jobs)

    @staticmethod
    def _count_jobs_strict(core_user_map):
        count = 0
        for k in core_user_map:
            just_jobs = core_user_map[k].translate(None, "#_")
            count += len(just_jobs)
        return count


class Document(namedtuple('Document', ['worker_nodes', 'jobs_dict', 'queues_dict', 'total_running_jobs', 'total_queued_jobs'])):
    def save(self, filename):
        with open(filename, 'w') as outfile:
            json.dump(document, outfile)


# class Document(object):
#
#     def __init__(self, cluster, wns_occupancy):
#         self.cluster = cluster
#         self.wns_occupancy = wns_occupancy
#         self.cluster_info = (
#             self.wns_occupancy,
#             self.cluster.worker_nodes,
#             self.cluster.total_running_jobs,
#             self.cluster.total_queued_jobs,
#             self.cluster.workernode_list,
#             self.cluster.workernode_dict)
#
#     def save(self, filename):
#         with open(filename, 'w') as outfile:
#             json.dump(self.cluster_info, outfile)
#

class TextDisplay(object):
    def __init__(self, document, config, viewport, wns_occupancy, cluster):
        self.cluster = cluster
        self.wns_occupancy = wns_occupancy
        self.document = document
        self.viewport = viewport
        self.config = config
        self.wns_occupancy = wns_occupancy

    def display_selected_sections(self, savepath, SAMPLE_FILENAME, QTOP_LOGFILE):
        """
        This prints out the qtop sections selected by the user.
        The selection can be made in two different ways:
        a) in the QTOPCONF_YAML file, in user_display_parts, where the three sections are named in a list
        b) through cmdline arguments -n, where n is 1,2,3. More than one can be chained together,
        e.g. -13 will exclude sections 1 and 3
        Cmdline arguments should only be able to choose from what is available in QTOPCONF_YAML, though.
        """
        sections_off = {  # cmdline argument -n
            1: options.sect_1_off,
            2: options.sect_2_off,
            3: options.sect_3_off
        }
        display_parts = {
            'job_accounting_summary': (self.display_job_accounting_summary, (self.cluster, self.document)),
            'workernodes_matrix': (self.display_wns_occupancy, (self.wns_occupancy, self.cluster)),
            'user_accounts_pool_mappings': (self.display_user_accounts_pool_mappings, (self.wns_occupancy,))
        }

        print "\033c",  # comma is to avoid losing the whole first line. An empty char still remains, though.

        for idx, part in enumerate(config['user_display_parts'], 1):
            display_func, args = display_parts[part][0], display_parts[part][1]
            display_func(*args) if not sections_off[idx] else None

        print "\nLog file created in %s" % os.path.expandvars(QTOP_LOGFILE)
        if options.SAMPLE:
            print "Sample files saved in %s/%s" % (savepath, SAMPLE_FILENAME)
        if options.STRICTCHECK:
            strict_check_jobs(wns_occupancy, cluster)

    def display_job_accounting_summary(self, cluster, document):
        """
        Displays qtop's first section
        """
        total_running_jobs = cluster.total_running_jobs
        total_queued_jobs = cluster.total_queued_jobs
        qstatq_lod = cluster.queues_dict

        if options.REMAP:
            if options.CLASSIC:
                print '=== WARNING: --- Remapping WN names and retrying heuristics... good luck with this... ---'
            else:
                logging.warning('=== WARNING: --- Remapping WN names and retrying heuristics... good luck with this... ---')

        ansi_delete_char = "\015"  # this removes the first ever character (space) appearing in the output
        print '%(del)s%(name)s report tool. All bugs added by sfranky@gmail.com. Cross fingers now...' \
              % {'name': 'PBS' if options.CLASSIC else './qtop.py ## Queueing System', 'del': ansi_delete_char}
        if scheduler == 'demo':
            msg = "This data is simulated. As soon as you connect to one of the supported scheduling systems,\n" \
                  "you will see live data from your cluster. Press q to Quit."
            print colorize(msg, 'Blue')

        if not options.WATCH:
            print 'Please try it with watch: %s/qtop.py -s <SOURCEDIR> -w [<every_nr_of_sec>]\n' \
                  '...and thank you for watching ;)\n' % QTOPPATH
        print colorize('===> ', 'Gray_D') + colorize('Job accounting summary', 'White') + colorize(' <=== ',
                                                                                                                  'Gray_D') + \
              '%s WORKDIR = %s' % (colorize(str(datetime.datetime.today())[:-7], 'White'), QTOPPATH)

        print '%(Usage Totals)s:\t%(online_nodes)s/%(total_nodes)s %(Nodes)s | %(working_cores)s/%(total_cores)s %(Cores)s |' \
              '   %(total_run_jobs)s+%(total_q_jobs)s %(jobs)s (R + Q) %(reported_by)s' % \
              {
                  'Usage Totals': colorize('Usage Totals', 'Yellow'),
                  'online_nodes': colorize(str(cluster.total_wn - cluster.offdown_nodes), 'Red_L'),
                  'total_nodes': colorize(str(cluster.total_wn), 'Red_L'),
                  'Nodes': colorize('Nodes', 'Red_L'),
                  'working_cores': colorize(str(cluster.working_cores), 'Green_L'),
                  'total_cores': colorize(str(cluster.total_cores), 'Green_L'),
                  'Cores': colorize('cores', 'Green_L'),
                  'total_run_jobs': colorize(str(int(total_running_jobs)), 'Blue_L'),
                  'total_q_jobs': colorize(str(int(total_queued_jobs)), 'Blue_L'),
                  'jobs': colorize('jobs', 'Blue_L'),
                  'reported_by': 'reported by qstat - q' if options.CLASSIC else ''
              }

        print '%(queues)s: | ' % {'queues': colorize('Queues', 'Yellow')},
        for _queue_name, q_tuple in qstatq_lod.items():
            q_running_jobs, q_queued_jobs, q_state = q_tuple.run, q_tuple.queued, q_tuple.state
            account = _queue_name if _queue_name in queue_to_color else 'account_not_colored'
            print "{qname}{star}: {run} {q}|".format(
                qname=colorize(_queue_name, '', pattern=account, mapping=queue_to_color),
                star=colorize('*', 'Red_L') if q_tuple.state.startswith('D') or q_tuple.state.endswith('S') else '',
                run=colorize(q_running_jobs, '', pattern=account, mapping=queue_to_color),
                q='+ ' + colorize(q_queued_jobs, '', account,
                                       mapping=queue_to_color) + ' ' if q_queued_jobs != '0' else ''),
        print colorize('* implies blocked', 'Red') + '\n'
        # TODO unhardwire states from star kwarg

    def display_wns_occupancy(self, wns_occupancy, cluster):
        """
        Displays qtop's second section, the main worker node matrices.
        """
        print_char_start = self.wns_occupancy.print_char_start
        print_char_stop = self.wns_occupancy.print_char_stop

        self.display_basic_legend()
        self.display_matrix(wns_occupancy, print_char_start, print_char_stop)
        if not config['transpose_wn_matrices']:
            self.display_remaining_matrices(wns_occupancy, print_char_start, print_char_stop)

    def display_basic_legend(self):
        """Displays the Worker Nodes occupancy label plus columns explanation"""
        if self.config['transpose_wn_matrices']:
            note = "/".join(self.config['occupancy_column_order'])
        else:
            note = 'you can read vertically the node IDs; nodes in free state are noted with - '

        print colorize('===> ', 'Gray_D') + \
              colorize('Worker Nodes occupancy', 'White') + \
              colorize(' <=== ', 'Gray_D') + \
              colorize('(%s)', 'Gray_D') % note

    def display_user_accounts_pool_mappings(self, wns_occupancy):
        """
        Displays qtop's third section
        """
        try:
            account_jobs_table = self.wns_occupancy.account_jobs_table
            uid_to_uid_re_pat = self.wns_occupancy.uid_to_uid_re_pat
        except KeyError:
            account_jobs_table = dict()
            uid_to_uid_re_pat = dict()

        detail_of_name = get_detail_of_name(account_jobs_table)
        print colorize('\n===> ', 'Gray_D') + \
              colorize('User accounts and pool mappings', 'White') + \
              colorize(' <=== ', 'Gray_d') + \
              colorize("  ('all' also includes those in C and W states, as reported by qstat)"
                            if options.CLASSIC else "  ('all' includes any jobs beyond R and W)", 'Gray_D')

        print '   R +    Q /  all |       unix account [id] %(msg)s' % \
              {'msg': 'Grid certificate DN (info only available under elevated privileges)' if options.CLASSIC else
              '      GECOS field or Grid certificate DN |'}
        for line in account_jobs_table:
            uid, runningjobs, queuedjobs, alljobs, user = line
            userid_pat = uid_to_uid_re_pat[uid]

            if (options.COLOR == 'OFF' or userid_pat == 'account_not_colored' or userid_pat_to_color[userid_pat] == 'reset'):
                conditional_width = 0
                userid_pat = 'account_not_colored'
            else:
                conditional_width = 12

            print_string = ('{1:>{width4}} + {2:>{width4}} / {3:>{width4}} {sep} '
                            '{4:>{width18}} '
                            '[ {0:<{width1}}] '
                            '{5:<{width40}} {sep}').format(
                colorize(str(uid), pattern=userid_pat),
                colorize(str(runningjobs), pattern=userid_pat),
                colorize(str(queuedjobs), pattern=userid_pat),
                colorize(str(alljobs), pattern=userid_pat),
                colorize(user, pattern=userid_pat),
                colorize(detail_of_name.get(user, ''), pattern=userid_pat),
                sep=colorize(config['SEPARATOR'], pattern=userid_pat),
                width1=1 + conditional_width,
                width3=3 + conditional_width,
                width4=4 + conditional_width,
                width18=18 + conditional_width,
                width40=40 + conditional_width,
            )
            print print_string

    def display_matrix(self, wns_occupancy, print_char_start, print_char_stop):
        """
        occupancy_parts needs to be redefined for each matrix, because of changed parameter values
        """
        # was: (not wns_occupancy.user_to_id) or is_matrix_coreless
        if self.wns_occupancy.is_matrix_coreless():
            return

        wn_vert_labels = wns_occupancy.wn_vert_labels
        core_user_map = wns_occupancy.core_user_map
        extra_matrices_nr = wns_occupancy.extra_matrices_nr
        uid_to_uid_re_pat = wns_occupancy.uid_to_uid_re_pat

        occupancy_parts = {
            'wn id lines':
                (
                    self.display_wnid_lines,
                    (print_char_start, print_char_stop, cluster.highest_wn, wn_vert_labels),
                    {'inner_attrs': None}
                ),
            'core_user_map':
                (
                    self.print_core_lines,
                    (core_user_map, print_char_start, print_char_stop, transposed_matrices, uid_to_uid_re_pat),
                    {'attrs': None}
                ),
        }

        # custom part, e.g. Node state, queue state etc
        for yaml_key, part_name, systems in yaml.get_yaml_key_part(config, scheduler, outermost_key='workernodes_matrix'):
            if scheduler not in systems: continue

            new_occupancy_part = {
                part_name:
                    (
                        self.print_mult_attr_line,  # func
                        (print_char_start, print_char_stop, transposed_matrices),  # args
                        {'attr_lines': wns_occupancy.get_dyn_var(part_name), 'coloring': queue_to_color}  # kwargs
                    )
            }
            occupancy_parts.update(new_occupancy_part)

        # get additional info from QTOPCONF_YAML
        for part_dict in config['workernodes_matrix']:
            part = [k for k in part_dict][0]
            key_vals = part_dict[part]
            if scheduler not in yaml.fix_config_list(key_vals.get('systems', [scheduler])):
                continue
            occupancy_parts[part][2].update(key_vals)  # get extra options from user

            func_, args, kwargs = occupancy_parts[part][0], occupancy_parts[part][1], occupancy_parts[part][2]
            func_(*args, **kwargs)

        if config['transpose_wn_matrices']:
            order = config['occupancy_column_order']
            for idx, (item, matrix) in enumerate(zip(order, transposed_matrices)):
                matrix[0] = order.index(matrix[1])

            transposed_matrices.sort(key=lambda item: item[0])
            ###TRY###
            for line_tuple in izip_longest(*[tpl[2] for tpl in transposed_matrices], fillvalue=utils.ColorStr('  ', color='Purple', )):
                joined_list = self.join_prints(*line_tuple, sep=config.get('horizontal_separator', None))

            max_width = len(joined_list)
            self.viewport.max_width = max_width

            logging.debug('Printed horizontally from %s to %s' % (self.viewport.h_start, self.viewport.h_stop))
        else:
            self.viewport.max_width = self.viewport.get_term_size()[1]
        print

    def display_remaining_matrices(self, wns_occupancy, print_char_start, print_char_stop, DEADWEIGHT=11):
        """
        If the WNs are more than a screenful (width-wise), this calculates the extra matrices needed to display them.
        DEADWEIGHT is the space taken by the {__XXXX__} labels on the right of the CoreX map

        if the first matrix has e.g. 10 machines with 64 cores,
        and the remaining 190 machines have 8 cores, this doesn't print the non-existent
        56 cores from the next matrix on.
        """
        extra_matrices_nr = wns_occupancy.extra_matrices_nr
        # term_columns = wns_occupancy.term_columns
        term_columns = viewport.h_term_size

        # need node_state, temp
        for matrix in range(extra_matrices_nr):
            print_char_start = print_char_stop
            if config['USER_CUT_MATRIX_WIDTH']:
                print_char_stop += config['USER_CUT_MATRIX_WIDTH']
            else:
                print_char_stop += term_columns - DEADWEIGHT
            print_char_stop = min(print_char_stop, cluster.total_wn) \
                if options.REMAP else min(print_char_stop, cluster.highest_wn)

            self.display_matrix(wns_occupancy, print_char_start, print_char_stop)

    def join_prints(self, *args, **kwargs):
        joined_list = []
        for d in args:
            sys.stdout.softspace = False  # if i want to omit in-between column spaces
            joined_list.extend([utils.ColorStr(string=char) if isinstance(char, str) and len(char) == 1 else char
            for char in d])
            joined_list.append(utils.ColorStr(string=kwargs['sep']))
        print "".join([colorize(char.initial, color_func=char.color) if isinstance(char, utils.ColorStr) else char
                       for char in joined_list[self.viewport.h_start:self.viewport.h_stop]])
        return joined_list

    def print_core_lines(self, core_user_map, print_char_start, print_char_stop, transposed_matrices, uid_to_uid_re_pat, attrs,
                         options1, options2):
        signal(SIGPIPE, SIG_DFL)
        if config['transpose_wn_matrices']:
            tuple_ = [None, 'core_map', self.transpose_matrix(core_user_map, colored=True, coloring_pat=uid_to_uid_re_pat)]
            transposed_matrices.append(tuple_)
            return

        for core_line in self.get_core_lines(core_user_map, print_char_start, print_char_stop, uid_to_uid_re_pat, attrs):
            try:
                print core_line
            except IOError:
                try:
                    signal(SIGPIPE, SIG_DFL)
                    print core_line
                    sys.stdout.close()
                except IOError:
                    pass
                try:
                    sys.stderr.close()
                except IOError:
                    pass

    def display_wnid_lines(self, start, stop, highest_wn, wn_vert_labels, **kwargs):
        """
        Prints the Worker Node ID lines, after it colors them and adds separators to them.
        highest_wn determines the number of WN ID lines needed  (1/2/3/4+?)
        """
        d = OrderedDict()
        end_labels = config['workernodes_matrix'][0]['wn id lines']['end_labels']

        if not NAMED_WNS:
            node_str_width = len(str(highest_wn))  # 4 for thousands of nodes, nr of horizontal lines to be displayed

            for node_nr in range(1, node_str_width + 1):
                d[str(node_nr)] = wn_vert_labels[str(node_nr)]
            end_labels_iter = iter(end_labels[str(node_str_width)])
            self.print_wnid_lines(d, start, stop, end_labels_iter, transposed_matrices,
                                  color_func=self.color_plainly, args=('White', 'Gray_L', start > 0))
            # start > 0 is just a test for a possible future condition

        elif NAMED_WNS or options.FORCE_NAMES:  # the actual names of the worker nodes instead of numbered WNs
            node_str_width = len(wn_vert_labels)  # key, nr of horizontal lines to be displayed

            # for longer full-labeled wn ids, add more end-labels (far-right) towards the bottom
            for num in range(8, len(wn_vert_labels) + 1):
                end_labels.setdefault(str(num), end_labels['7'] + num * ['={___ID___}'])

            end_labels_iter = iter(end_labels[str(node_str_width)])
            self.print_wnid_lines(wn_vert_labels, start, stop, end_labels_iter, transposed_matrices,
                                  color_func=self.highlight_alternately, args=(config['ALT_LABEL_COLORS']))

    def highlight_alternately(self, color_a, color_b):
        colors = cycle([color_a, color_b])
        for color in colors:
            yield color

    def color_plainly(self, color_0, color_1, condition):
        while condition:
            yield color_0
        else:
            while not condition:
                yield color_1

    def print_wnid_lines(self, d, start, stop, end_labels, transposed_matrices, color_func, args):
        if self.config['transpose_wn_matrices']:
            tuple_ = [None, 'wnid_lines', self.transpose_matrix(d)]
            transposed_matrices.append(tuple_)
            return

        separators = config['vertical_separator_every_X_columns']
        for line_nr, end_label in zip(d, end_labels):
            colors = iter(color_func(*args))
            wn_id_str = self._insert_separators(d[line_nr][start:stop], config['SEPARATOR'], separators)
            wn_id_str = ''.join([colorize(elem, next(colors)) for elem in wn_id_str])
            print wn_id_str + end_label

    def print_y_lines_of_file_starting_from_x(self, file, x, y):
        """
        Prints part of the qtop output to the terminal (as fast as possible!)
        Justification for implementation:
        http://unix.stackexchange.com/questions/47407/cat-line-x-to-line-y-on-a-huge-file
        """
        temp_f = tempfile.NamedTemporaryFile(delete=False, suffix='.out', dir=TMPDIR)
        pre_cat_command = '(tail -n+%s %s | head -n%s) > %s' % (x, file, y - 1, temp_f.name)
        _ = subprocess.call(pre_cat_command, stdout=stdout, stderr=stdout, shell=True)
        cat_command = 'clear;cat %s' % temp_f.name
        return cat_command

    def print_mult_attr_line(self, print_char_start, print_char_stop, transposed_matrices, attr_lines, label, color_func=None,
                             **kwargs):
        """
        attr_lines can be e.g. Node state lines
        """
        if config['transpose_wn_matrices']:
            tuple_ = [None, label, self.transpose_matrix(attr_lines, colored=True, coloring_pat=None)]
            transposed_matrices.append(tuple_)
            return

        # TODO: fix option parameter, inserted for testing purposes
        for _line in attr_lines:
            line = attr_lines[_line][print_char_start:print_char_stop]
            # TODO: maybe put attr_line and label as kwd arguments? collect them as **kwargs
            attr_line = self._insert_separators(line, config['SEPARATOR'], config['vertical_separator_every_X_columns'])
            attr_line = ''.join([colorize(char.initial, color_func=char.color) for char in attr_line])
            print attr_line + "=" + label

    def get_core_lines(self, core_user_map, print_char_start, print_char_stop, coloring_pattern, attrs):
        """
        prints all coreX lines, except cores that don't show up
        anywhere in the given matrix
        """
        # TODO: is there a way to use is_matrix_coreless in here? avoid duplication of code
        non_existent_symbol = config['non_existent_node_symbol']
        for ind, k in enumerate(core_user_map):
            cpu_core_line = core_user_map['Core' + str(ind) + 'vector'][print_char_start:print_char_stop]
            if options.REM_EMPTY_CORELINES and \
                    (
                        (non_existent_symbol * (print_char_stop - print_char_start) == cpu_core_line) or
                            (non_existent_symbol * (len(cpu_core_line)) == cpu_core_line)
                    ):
                continue
            cpu_core_line = self._insert_separators(cpu_core_line, config['SEPARATOR'],
                                                    config['vertical_separator_every_X_columns'])
            colored_core_x = [colorize(elem, '', coloring_pattern[elem]) for elem in cpu_core_line if elem in coloring_pattern]
            cpu_core_line = ''.join(colored_core_x)
            yield cpu_core_line + colorize('=Core' + str(ind), '', 'account_not_colored')

    def transpose_matrix(self, d, colored=False, reverse=False, coloring_pat=None):
        """
        takes a dictionary whose values are lists of strings (=matrix)
        returns a transposed matrix
        colors it in the meantime, if instructed to via colored
        """
        uid_to_uid_re_pat = wns_occupancy.uid_to_uid_re_pat
        for tpl in izip_longest(*[[char for char in d[k]] for k in d], fillvalue=" "):
            if any(j != " " for j in tpl):
                tpl = (colored and coloring_pat) and \
                      [colorize(txt, '', coloring_pat[txt]) if txt in coloring_pat else txt for txt in tpl] or list(tpl)
                tpl[:] = tpl[::-1] if reverse else tpl
            yield tpl

    def _insert_separators(self, orig_str, separator, pos, stopaftern=0):
        """
        inserts separator into orig_str every pos-th position, optionally stopping after stopaftern times.
        """
        if not pos:  # default value is zero, means no vertical separators
            return orig_str
        else:
            sep_str = orig_str[:]  # insert initial vertical separator
            separator = separator if isinstance(sep_str, str) else list(separator)
            times = len(orig_str) / pos if not stopaftern else stopaftern
            sep_str = sep_str[:pos] + separator + sep_str[pos:]
            for i in range(2, times + 1):
                sep_str = sep_str[:pos * i + i - 1] + separator + sep_str[pos * i + i - 1:]
            sep_str += separator  # insert initial vertical separator
            return sep_str


class Cluster(object):
    def __init__(self, document, worker_nodes, WNFilter, config, options):
        self.worker_nodes = worker_nodes
        self.jobs_dict = document.jobs_dict
        self.queues_dict = document.queues_dict  # ex qstatq_lod is now list of namedtuples
        self.total_running_jobs = document.total_running_jobs
        self.total_queued_jobs = document.total_queued_jobs
        self.config = config
        self.options = options

        self.working_cores = 0
        self.offdown_nodes = 0
        self.total_cores = 0
        self.core_span = []
        self.highest_wn = 0
        self.node_subclusters = set()
        self.workernode_dict = {}
        self.workernode_dict_remapped = {}  # { remapnr: [state, np, (core0, job1), (core1, job1), ....]}

        self.total_wn = len(self.worker_nodes)  # == existing_nodes
        self.workernode_list = []
        self.workernode_list_remapped = range(1, self.total_wn + 1)  # leave xrange aside for now

        self.node_subclusters = set()
        self.WNFilter = WNFilter
        self.wn_filter = None

        self.analyse()

    def analyse(self):

        if not self.worker_nodes:
            return None  # TODO ? what to return instead of cluster?

        re_nodename = r'(^[A-Za-z0-9-]+)(?=\.|$)' if not self.options.ANONYMIZE else r'\w_anon_\w+'

        self.node_subclusters, self.workernode_list, self.offdown_nodes, self.working_cores, max_np, \
            _all_str_digits_with_empties = self.get_wn_list_and_stats(self.workernode_list,
                                                                          self.node_subclusters,
                                                                          self.worker_nodes,
                                                                          re_nodename)

        self.core_span = [str(x) for x in range(max_np)]
        self.options.REMAP = self.decide_remapping(_all_str_digits_with_empties)

        # nodes_drop: this amount has to be chopped off of the end of workernode_list_remapped
        nodes_drop, workernode_dict, workernode_dict_remapped = self.map_worker_nodes_to_wn_dict(self.options.REMAP)
        self.workernode_dict = workernode_dict

        if self.options.REMAP:
            self.workernode_dict_remapped = workernode_dict_remapped
            self.total_wn += nodes_drop
            self.highest_wn = self.total_wn

            nodes_drop_slice_end = None if not nodes_drop else nodes_drop
            self.workernode_list = self.workernode_list_remapped[:nodes_drop_slice_end]
            self.workernode_dict = self.workernode_dict_remapped
        else:
            self.highest_wn = max(self.workernode_list)
            self.workernode_dict = self.fill_non_existent_wn_nodes(self.workernode_dict)

        self.workernode_dict = self.do_name_remapping(self.workernode_dict)
        del self.node_subclusters  # sets are not JSON serialisable!!
        del self.workernode_list_remapped
        del self.workernode_dict_remapped

    def get_wn_list_and_stats(self, workernode_list, node_subclusters, worker_nodes, re_nodename):
        max_np = 0
        all_str_digits_with_empties = list()
        for node in worker_nodes:
            nodename_match = re.search(re_nodename, node['domainname'])
            _nodename = nodename_match.group(0)

            # get subclusters by name change
            _node_letters = ''.join(re.findall(r'\D+', _nodename))
            node_subclusters.update([_node_letters])

            node_str_digits = "".join(re.findall(r'\d+', _nodename))
            all_str_digits_with_empties.append(node_str_digits)

            self.total_cores += int(node.get('np'))  # for stats only
            max_np = max(max_np, int(node['np']))
            self.offdown_nodes += 1 if "".join(([n.str for n in node['state']])) in 'do'  else 0
            # self.offdown_nodes += 1 if ('d' in node['state'] or 'o' in node['state'])  else 0
            # self.offdown_nodes += 1 if node['state'] in 'do' else 0
            self.working_cores += len(node.get('core_job_map', dict()))

            try:
                _cur_node_nr = int(node_str_digits)
            except ValueError:
                _cur_node_nr = _nodename

            # create workernode_list
            workernode_list.append(_cur_node_nr)

        return node_subclusters, workernode_list, self.offdown_nodes, self.working_cores, max_np, all_str_digits_with_empties

    def decide_remapping(self, all_str_digits_with_empties):
        """
        Cases where remapping is enforced are:
        - the user has requested it (blindremap switch)
        - there are different WN namings, e.g. wn001, wn002, ..., ps001, ps002, ... etc
        - the first starting numbering of a WN is very high and thus would require too much unused space
        #- the numbering is strange, say the highest numbered node is named wn12500 but the total amount of WNs is 8000????
        - more than PERCENTAGE*nodes have no jobs assigned
        - there are numbering collisions,
            e.g. there's a ps001 and a wn001, or a 0x001 and a 0x1, which all would be displayed in position 1
            or there are non-numbered wns

        Reasons not enough to warrant remapping (intended future behaviour)
        - one or two unnumbered nodes (should just be put in the end of the cluster)
        """
        if not self.total_wn:  # if nothing is running on the cluster
            return None

        _all_str_digits = filter(lambda x: x != "", all_str_digits_with_empties)
        _all_digits = [int(digit) for digit in _all_str_digits]

        if options.BLINDREMAP or \
                        len(self.node_subclusters) > 1 or \
                        min(self.workernode_list) >= config['exotic_starting_wn_nr'] or \
                        self.offdown_nodes >= self.total_wn * config['percentage'] or \
                        len(all_str_digits_with_empties) != len(_all_str_digits) or \
                        len(_all_digits) != len(_all_str_digits):
            REMAP = True
        else:
            REMAP = False
        logging.info('Blind Remapping [user selected]: %s,'
                     '\n\t\t\t\t\t\t\t\t  Decided Remapping: %s' % (options.BLINDREMAP, REMAP))

        if logging.getLogger().isEnabledFor(logging.DEBUG) and REMAP:
            user_request = options.BLINDREMAP and 'The user has requested it (blindremap switch)' or False

            subclusters = len(self.node_subclusters) > 1 and \
                          'there are different WN namings, e.g. wn001, wn002, ..., ps001, ps002, ... etc' or False

            exotic_starting = min(self.workernode_list) >= config['exotic_starting_wn_nr'] and \
                              'first starting numbering of a WN very high; would thus require too much unused space' or False

            percentage_unassigned = len(all_str_digits_with_empties) != len(_all_str_digits) and \
                                    'more than %s of nodes have are down/offline' % float(config['percentage']) or False

            numbering_collisions = min(self.workernode_list) >= config['exotic_starting_wn_nr'] and \
                                   'there are numbering collisions' or False

            print
            logging.debug('Remapping decided due to: \n\t %s' % filter(
                None, [user_request, subclusters, exotic_starting, percentage_unassigned, numbering_collisions]))

        return REMAP

    def do_name_remapping(self, workernode_dict):
        """
        renames hostnames according to user remapping in conf file (for the wn id label lines)
        """
        label_max_len = int(config['workernodes_matrix'][0]['wn id lines']['max_len'])
        for _, state_corejob_dn in workernode_dict.items():
            _host = state_corejob_dn['domainname'].split('.', 1)[0]
            changed = False
            for remap_line in config['remapping']:
                pat, repl = remap_line.items()[0]
                repl = eval(repl) if repl.startswith('lambda') else repl
                if re.search(pat, _host):
                    changed = True
                    state_corejob_dn['host'] = _host = re.sub(pat, repl, _host)
            else:
                state_corejob_dn['host'] = _host if not changed else state_corejob_dn['host']
                # was: label_max_len = config['wn_labels_max_len']
                state_corejob_dn['host'] = label_max_len and state_corejob_dn['host'][-label_max_len:] or state_corejob_dn[
                    'host']
        return workernode_dict

    def fill_non_existent_wn_nodes(self, workernode_dict):
        """fill in non-existent WN nodes (absent from input files) with default values and count them"""
        for node in range(1, self.highest_wn + 1):
            if node not in workernode_dict:
                workernode_dict[node] = {'state': '?', 'np': 0, 'domainname': 'N/A', 'host': 'N/A'}
                default_values_for_empty_nodes = dict([(yaml_key, '?') for yaml_key, part_name, _ in yaml.get_yaml_key_part(
                    config, scheduler, outermost_key='workernodes_matrix')])
                workernode_dict[node].update(default_values_for_empty_nodes)
        return workernode_dict

    def map_worker_nodes_to_wn_dict(self, options_remap):
        """
        For filtering to take place,
        1) a filter should be defined in QTOPCONF_YAML
        2) remap should be either selected by the user or enforced by the circumstances
        """
        nodes_drop = 0  # count change in nodes after filtering
        workernode_dict = dict()
        workernode_dict_remapped = dict()
        user_sorting = self.config['sorting'] and self.config['sorting'].values()[0]
        user_filtering = self.config['filtering'] and self.config['filtering'][0]

        if user_sorting and options_remap:
            self.worker_nodes = self._sort_worker_nodes()

        if user_filtering and options_remap:
            len_wn_before = len(self.worker_nodes)
            self.wn_filter = self.WNFilter(self.worker_nodes)
            self.worker_nodes = self.wn_filter.filter_worker_nodes(filter_rules=config['filtering'])
            len_wn_after = len(self.worker_nodes)
            nodes_drop = len_wn_after - len_wn_before

        for (batch_node, (idx, cur_node_nr)) in zip(self.worker_nodes, enumerate(self.workernode_list)):
            # Seemingly there is an error in the for loop because self.worker_nodes and workernode_list
            # have different lengths if there's a filter in place, but it is OK, since
            # it is just the idx counter that is taken into account in remapping.
            workernode_dict[cur_node_nr] = batch_node
            workernode_dict_remapped[idx] = batch_node

        return nodes_drop, workernode_dict, workernode_dict_remapped

    def _sort_worker_nodes(self):
        try:
            self.worker_nodes.sort(key=eval(self.config['sorting']['user_sort']), reverse=self.config['sorting']['reverse'])
        except (IndexError, ValueError):
            logging.critical("There's (probably) something wrong in your sorting lambda in %s." % QTOPCONF_YAML)
            raise
        return self.worker_nodes


def colorize(text, color_func=None, pattern='NoPattern', mapping=None, bg_color=None, bold=False):
    """
    prints colored text according to at least one of the keyword arguments available:
    color_func can be a direct ansi color name or a function providing a color name.
    If color is given directly as color_func, pattern/mapping are not needed.
    If no color_func and no mapping are defined, the default mapping is used,
    which is userid_pat_to_color, mapping an account name to a color.
    A pattern must then be given, which is the key of the mapping.
    Other mappings available are: nodestate_to_color, queue_to_color.
    Examples:
    s = ColorStr(string='This is some text', color='Red_L')
    print colorize(s, color_func=s.color), # Red_L is applied directly
    print colorize(s.str, pattern='alicesgm') # mapping defaults to userid_pat_to_color
    print colorize(s.str, color_func=s.color, bg_color='BlueBG') # bg and fg colors applied directly

    state = ColorStr('running. coloring according to node state')
    print colorize(state.str, mapping=nodestate_to_color, pattern=state.initial)
    """
    bg_color = 'NOBG' if not bg_color else bg_color
    if not mapping:
        mapping = userid_pat_to_color
    try:
        ansi_color = color_to_code[color_func] if color_func else color_to_code[mapping[pattern]]
    except KeyError:
        return text
    else:
        if bold and ansi_color[0] in '01':
            ansi_color = '1' + ansi_color[1:]
        if ((options.COLOR == 'ON') and pattern != 'account_not_colored' and text != ' '):
            text = "\033[%(fg_color)s%(bg_color)sm%(text)s\033[0;m" \
                   % {'fg_color': ansi_color, 'bg_color': color_to_code[bg_color], 'text': text}

        return text


class WNFilter(object):
    def __init__(self, worker_nodes):
        self.worker_nodes = worker_nodes

    def _filter_list_out(self, the_list=None):
        if not the_list:
            the_list = []
        for idx, node in enumerate(self.worker_nodes):
            if idx in the_list:
                node['mark'] = '*'
        self.worker_nodes = filter(lambda item: not item.get('mark'), self.worker_nodes)
        return self.worker_nodes

    def _filter_list_out_by_name(self, the_list=None):
        if not the_list:
            the_list = []
        else:
            the_list[:] = [eval(i) for i in the_list]
        for idx, node in enumerate(self.worker_nodes):
            if node['domainname'].split('.', 1)[0] in the_list:
                node['mark'] = '*'
        self.worker_nodes = filter(lambda item: not item.get('mark'), self.worker_nodes)
        return self.worker_nodes

    def _filter_list_in_by_name(self, the_list=None):
        if not the_list:
            the_list = []
        else:
            the_list[:] = [eval(i) for i in the_list]
        for idx, node in enumerate(self.worker_nodes):
            if node['domainname'].split('.', 1)[0] not in the_list:
                node['mark'] = '*'
        self.worker_nodes = filter(lambda item: not item.get('mark'), self.worker_nodes)
        return self.worker_nodes

    def _filter_list_out_by_node_state(self, the_list=None):
        if not the_list:
            the_list = []
        for idx, node in enumerate(self.worker_nodes):
            if node['state'] in the_list:
                node['mark'] = '*'
        self.worker_nodes = filter(lambda item: not item.get('mark'), self.worker_nodes)
        return self.worker_nodes

    def _filter_list_out_by_name_pattern(self, the_list=None):
        if not the_list:
            the_list = []

        for idx, node in enumerate(self.worker_nodes):
            for pattern in the_list:
                match = re.search(pattern, node['domainname'].split('.', 1)[0])
                try:
                    match.group(0)
                except AttributeError:
                    pass
                else:
                    node['mark'] = '*'
        self.worker_nodes = filter(lambda item: not item.get('mark'), self.worker_nodes)
        return self.worker_nodes

    def _filter_list_in_by_name_pattern(self, the_list=None):
        if not the_list:
            the_list = []

        for idx, node in enumerate(self.worker_nodes):
            for pattern in the_list:
                match = re.search(pattern, node['domainname'].split('.', 1)[0])
                try:
                    match.group(0)
                except AttributeError:
                    node['mark'] = '*'
                else:
                    pass
        self.worker_nodes = filter(lambda item: not item.get('mark'), self.worker_nodes)
        return self.worker_nodes

    def filter_worker_nodes(self, filter_rules=None):
        """
        Filters specific nodes according to the filter rules in QTOPCONF_YAML
        """

        filter_types = {
            'list_out': self._filter_list_out,
            'list_out_by_name': self._filter_list_out_by_name,
            'list_in_by_name': self._filter_list_in_by_name,
            'list_out_by_name_pattern': self._filter_list_out_by_name_pattern,
            'list_in_by_name_pattern': self._filter_list_in_by_name_pattern,
            'list_out_by_node_state': self._filter_list_out_by_node_state
        }

        if filter_rules:
            logging.warning("WN Occupancy view is filtered.")  # TODO display this somewhere in qtop!
            for rule in filter_rules:
                filter_func = filter_types[rule.keys()[0]]
                args = rule.values()[0]
                self.worker_nodes = filter_func(args)
        return self.worker_nodes


class JobNotFound(Exception):
    def __init__(self, job_state):
        Exception.__init__(self, "Job state %s not found" % job_state)
        self.job_state = job_state


class NoSchedulerFound(Exception):
    def __init__(self):
        msg = 'No suitable scheduler was found. ' \
              'Please define one in a switch or env variable or in %s.\n' \
              'For more help, try ./qtop.py --help\nLog file created in %s' \
              % (QTOPCONF_YAML, os.path.expandvars(QTOP_LOGFILE))
        Exception.__init__(self, msg)
        logging.critical(msg)


class SchedulerNotSpecified(Exception):
    pass


class InvalidScheduler(Exception):
    pass


if __name__ == '__main__':
    options, args = utils.parse_qtop_cmdline_args()
    utils.init_logging(options)
    options, NAMED_WNS = process_options(options)
    # TODO: check if this is really needed any more
    # sys.excepthook = handle_exception

    available_batch_systems = discover_qtop_batch_systems()

    stdout = sys.stdout  # keep a copy of the initial value of sys.stdout

    viewport = Viewport()  # controls the part of the qtop matrix shown on screen
    max_line_len = 0

    check_python_version()
    initial_cwd = os.getcwd()
    logging.debug('Initial qtop directory: %s' % initial_cwd)
    CURPATH = os.path.expanduser(initial_cwd)  # where qtop was invoked from
    QTOPPATH = os.path.dirname(realpath(sys.argv[0]))  # dir where qtop resides
    SAMPLE_FILENAME = 'qtop_sample_${USER}%(datetime)s.tar'
    SAMPLE_FILENAME = os.path.expandvars(SAMPLE_FILENAME)

    web = Web(initial_cwd)
    if options.WEB:
        web.start()

    with raw_mode(sys.stdin):  # key listener implementation
        try:
            while True:
                handle, output_fp = fileutils.get_new_temp_file(prefix='qtop_', suffix='.out')  # qtop output is saved to this file
                sys.stdout = os.fdopen(handle, 'w')  # redirect everything to file, creates file object out of handle
                config, userid_pat_to_color, nodestate_to_color = load_yaml_config()  # TODO account_to_color is updated in here !!
                config = update_config_with_cmdline_vars(options, config)

                attempt_faster_xml_parsing(config)
                options = init_dirs(options)

                transposed_matrices = []
                viewport.set_term_size(*calculate_term_size(config, FALLBACK_TERMSIZE))
                scheduler = decide_batch_system(
                    options.BATCH_SYSTEM, os.environ.get('QTOP_SCHEDULER'), config['scheduler'],
                    config['schedulers'], available_batch_systems, config)
                scheduler_output_filenames = fetch_scheduler_files(options, config)
                SAMPLE_FILENAME = fileutils.get_sample_filename(SAMPLE_FILENAME, config)
                fileutils.init_sample_file(options, config, SAMPLE_FILENAME, scheduler_output_filenames, QTOPCONF_YAML, QTOPPATH)

                ###### Gather data ###############
                #
                scheduling_system = available_batch_systems[scheduler](scheduler_output_filenames, config, options)

                job_ids, user_names, job_states, job_queues = scheduling_system.get_jobs_info()
                total_running_jobs, total_queued_jobs, qstatq_lod = scheduling_system.get_queues_info() # callthis elsewhere
                worker_nodes = scheduling_system.get_worker_nodes()

                JobDoc = namedtuple('JobDoc', ['user_name', 'job_state', 'job_queue'])
                jobs_dict = dict((job_id, JobDoc(user_name, job_state, job_queue))
                    for job_id, user_name, job_state, job_queue in izip(job_ids, user_names, job_states, job_queues))

                QDoc = namedtuple('QDoc', ['lm', 'queued', 'run', 'state'])
                queues_dict = OrderedDict((qstatq['queue_name'], (QDoc(str(qstatq['lm']), qstatq['queued'],
                                                           qstatq['run'], qstatq['state']))) for qstatq in qstatq_lod)
                worker_nodes = ensure_worker_nodes_have_qnames(worker_nodes, jobs_dict)  # TODO is this bad to have beforehand?

                ###### Export data ###############
                #
                document = Document(worker_nodes, jobs_dict, queues_dict, total_running_jobs, total_queued_jobs)
                tf = tempfile.NamedTemporaryFile(delete=False, suffix='.json', dir=savepath)  # Will become doc member one day
                document.save(tf.name)  # dump json document to a file
                web.set_filename(tf.name)

                ###### Process data ###############
                #
                worker_nodes = keep_queue_initials_only_and_colorize(document.worker_nodes, queue_to_color)
                worker_nodes = colorize_nodestate(document.worker_nodes, nodestate_to_color, colorize)
                cluster = Cluster(document, worker_nodes, WNFilter, config, options)
                wns_occupancy = WNOccupancy(cluster, config, document, userid_pat_to_color)
                ###### Display data ###############
                #
                display = TextDisplay(document, config, viewport, wns_occupancy, cluster)
                display.display_selected_sections(savepath, SAMPLE_FILENAME, QTOP_LOGFILE)

                sys.stdout.flush()
                sys.stdout.close()
                sys.stdout = stdout  # sys.stdout is back to its normal function (i.e. prints to screen)

                viewport.max_height, max_line_len = get_output_size(viewport.max_height, max_line_len, output_fp)

                if options.ONLYSAVETOFILE:  # no display of qtop output, will exit
                    break
                elif not options.WATCH:  # one-off display of qtop output, will exit afterwards (no --watch cmdline switch)
                    cat_command = 'clear;cat %s' % output_fp
                    _ = subprocess.call(cat_command, stdout=stdout, stderr=stdout, shell=True)
                    break
                else:  # --watch
                    cat_command = display.print_y_lines_of_file_starting_from_x(file=output_fp, x=viewport.v_start,
                                                                                y=viewport.v_term_size)
                    _ = subprocess.call(cat_command, stdout=stdout, stderr=stdout, shell=True)

                    read_char = wait_for_keypress_or_autorefresh(viewport, int(options.WATCH[0]) or KEYPRESS_TIMEOUT)
                    control_movement(viewport, read_char)

                os.chdir(QTOPPATH)
                os.unlink(output_fp)

            if options.SAMPLE:
                fileutils.add_to_sample([output_fp], config['savepath'], SAMPLE_FILENAME)

        except (KeyboardInterrupt, EOFError) as e:
            repr(e)
            fileutils.safe_exit_with_file_close(handle, output_fp, stdout, options, config, QTOP_LOGFILE, SAMPLE_FILENAME)
        else:
            if options.SAMPLE >= 1:
                fileutils.add_to_sample([QTOP_LOGFILE], config['savepath'], SAMPLE_FILENAME)
