#!/usr/bin/env python

################################################
#                   qtop                       #
#     Licensed under MIT license               #
#                     Sotiris Fragkiskos       #
#                     Fotis Georgatos          #
################################################
import sys
here = sys.path[0]

from itertools import izip, izip_longest, cycle
import subprocess
import select
import os
import re
import json
import datetime
try:
    from collections import namedtuple, OrderedDict, Counter
except ImportError:
    from qtop_py.legacy.namedtuple import namedtuple
    from qtop_py.legacy.ordereddict import OrderedDict
    from qtop_py.legacy.counter import Counter
import os
from os.path import realpath
from signal import signal, SIGPIPE, SIG_DFL
import termios
import contextlib
import glob
import tempfile
import sys
import logging
import time

from qtop_py.constants import (SYSTEMCONFDIR, QTOPCONF_YAML, QTOP_LOGFILE, USERPATH, MAX_CORE_ALLOWED,
    MAX_UNIX_ACCOUNTS, KEYPRESS_TIMEOUT, FALLBACK_TERMSIZE)
from qtop_py import fileutils
from qtop_py import utils
from qtop_py.plugins import *
from qtop_py.colormap import user_to_color_default, color_to_code, queue_to_color, nodestate_to_color_default
import qtop_py.yaml_parser as yaml
from qtop_py.ui.viewport import Viewport
from qtop_py.serialiser import GenericBatchSystem
from qtop_py.web import Web
import WNOccupancy
from qtop_py import __version__


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
        if options.WATCH:
            try:
                conf.old_attrs = termios.tcgetattr(file.fileno())
            except:
                yield
            else:
                conf.new_attrs = conf.old_attrs[:]
                conf.new_attrs[3] = conf.new_attrs[3] & ~(termios.ECHO | termios.ICANON)
                try:
                    termios.tcsetattr(file.fileno(), termios.TCSADRAIN, conf.new_attrs)
                    yield
                finally:
                    termios.tcsetattr(file.fileno(), termios.TCSADRAIN, conf.old_attrs)
        else:
            yield


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


def execute_shell_batch_commands(batch_system_commands, filenames, _file, _savepath):
    """
    scheduler-specific commands are invoked from the shell and their output is saved *atomically* to files,
    as defined by the user in QTOPCONF_YAML
    """
    _batch_system_command = batch_system_commands[_file].strip()
    with tempfile.NamedTemporaryFile('w', dir=_savepath, delete=False) as fin:
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


def get_detail_of_name(conf, account_jobs_table):
    """
    Reads file $HOME/.local/qtop/getent_passwd.txt or whatever is put in QTOPCONF_YAML
    and extracts the fullname of the users. This shall be printed in User Accounts
    and Pool Mappings.
    """
    config = conf.config
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

    try:
        p = subprocess.Popen(passwd_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    except OSError:
        logging.critical('\nCommand "%s" could not be found in your system. \nEither remove -G switch or modify the command in '
                         'qtopconf.yaml (value of key: %s).\nExiting...' % (colorize(passwd_command[0], color_func='Red_L'),
                         'user_details_realtime'))
        sys.exit(0)
    else:
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




def control_qtop(display, read_char, cluster, conf):
    """
    Basic vi-like movement is implemented for the -w switch (linux watch-like behaviour for qtop).
    h, j, k, l for left, down, up, right, respectively.
    Both g/G and Shift+j/k go to top/bottom of the matrices
    0 and $ go to far left/right of the matrix, respectively.
    r resets the screen to its initial position (if you've drifted away from the vieweable part of a matrix).
    q quits qtop.
    """
    pressed_char_hex = '%02x' % ord(read_char)  # read_char has an initial value that resets the display ('72')
    dynamic_config = conf.dynamic_config
    viewport = display.viewport

    if pressed_char_hex in ['6a', '20']:  # j, spacebar
        logging.debug('v_start: %s' % viewport.v_start)
        if viewport.scroll_down():
            # TODO  make variable for **s, maybe factorize whole print line
            print '%s Going down...' % colorize('***', 'Green_L')
        else:
            print '%s Staying put' % colorize('***', 'Green_L')

    elif pressed_char_hex in ['6b', '7f']:  # k, Backspace
        if viewport.scroll_up():
            print '%s Going up...' % colorize('***', 'Green_L')
        else:
            print '%s Staying put' % colorize('***', 'Green_L')

    elif pressed_char_hex in ['6c']:  # l
        print '%s Going right...' % colorize('***', 'Green_L')
        viewport.scroll_right()

    elif pressed_char_hex in ['24']:  # $
        print '%s Going far right...' % colorize('***', 'Green_L')
        viewport.scroll_far_right()
        logging.info('h_start: %s' % viewport.h_start)
        # logging.info('max_line_len: %s' % max_line_len)  # TODO for now not accessible
        logging.info('config["term_size"][1] %s' % viewport.h_term_size)
        logging.info('h_stop: %s' % viewport.h_stop)

    elif pressed_char_hex in ['68']:  # h
        print '%s Going left...' % colorize('***', 'Green_L')
        viewport.scroll_left()

    elif pressed_char_hex in ['30']:   # 0
        print '%s Going far left...' % colorize('***', 'Green_L')
        viewport.scroll_far_left()

    elif pressed_char_hex in ['4a', '47']:  # S-j, G
        logging.debug('v_start: %s' % viewport.v_start)
        if viewport.scroll_bottom():
            print '%s Going to the bottom...' % colorize('***', 'Green_L')
        else:
            print '%s Staying put' % colorize('***', 'Green_L')

    elif pressed_char_hex in ['4b', '67']:  # S-k, g
        print '%s Going to the top...' % colorize('***', 'Green_L')
        logging.debug('v_start: %s' % viewport.v_start)
        viewport.scroll_top()

    elif pressed_char_hex in ['52']:  # R
        print '%s Resetting display...' % colorize('***', 'Green_L')
        viewport.reset_display()

    elif pressed_char_hex in ['74']:  # t
        print '%s Transposing matrix...' % colorize('***', 'Green_L')
        dynamic_config['transpose_wn_matrices'] = not dynamic_config.get('transpose_wn_matrices',
                                                                         config['transpose_wn_matrices'])
        viewport.reset_display()

    elif pressed_char_hex in ['6d']:  # m
        new_mapping, msg = conf.change_mapping.next()
        dynamic_config['core_coloring'] = new_mapping
        print '%s Changing to %s' % (colorize('***', 'Green_L'), msg)

    elif pressed_char_hex in ['71']:  # q
        print colorize('\nExiting. Thank you for ..watching ;)\n', 'Cyan_L')
        web.stop()
        sys.exit(0)

    elif pressed_char_hex in ['46']:  # F
        dynamic_config['force_names'] = not dynamic_config['force_names']
        print '%s Toggling full-name/incremental nr WN labels' % colorize('***', 'Green_L')

    elif pressed_char_hex in ['73']:  # s
        sort_map = OrderedDict()

        sort_map['0'] = ("sort reset", [])
        sort_map['1'] = ("sort by nodename-notnum", [])
        sort_map['2'] = ("sort by nodename-notnum length", [])
        sort_map['3'] = ("sort by all numbers", [])
        sort_map['4'] = ("sort by first letter", [])
        sort_map['5'] = ("sort by node state", [])
        sort_map['6'] = ("sort by nr of cores", [])
        sort_map['7'] = ("sort by core occupancy", [])
        sort_map['8'] = ("sort by custom definition", [])

        custom_choice = '8'

        print 'Type in sort order. This can be a single number or a sequence of numbers,\n' \
              'e.g. to sort first by first word, then by all numbers then by first name length, type 132, then <enter>.'

        for nr, sort_method in sort_map.items():
            print '(%s): %s' % (colorize(nr, color_func='Red_L'), sort_method[0])

        conf.new_attrs[3] = conf.new_attrs[3] & ~(termios.ECHO | termios.ICANON)
        termios.tcsetattr(sys.__stdin__.fileno(), termios.TCSADRAIN, conf.old_attrs)

        dynamic_config['user_sort'] = []
        while True:
            sort_choice = raw_input('\nChoose sorting order, or Enter to exit:-> ', )
            if not sort_choice:
                break
            if custom_choice in sort_choice:
                custom = raw_input('\nType in custom sorting (python RegEx, for examples check configuration file): ')
                sort_map[custom_choice][1].append(custom)

            try:
                sort_order = [m for m in sort_choice]
            except ValueError:
                break
            else:
                if not set(sort_order).issubset(set(sort_map.keys())):
                    continue

            sort_args = [sort_map[i] for i in sort_order]
            dynamic_config['user_sort'] = sort_args
            break

        termios.tcsetattr(sys.__stdin__.fileno(), termios.TCSADRAIN, conf.new_attrs)
        viewport.reset_display()


    elif pressed_char_hex in ['66']:  # f
        cluster.wn_filter = cluster.WNFilter(cluster.worker_nodes)
        filter_map = {
            1: 'exclude_node_states',
            2: 'exclude_numbers',
            3: 'exclude_name_patterns',
            4: 'or_include_node_states',
            5: 'or_include_numbers',
            6: 'or_include_name_patterns',
            7: 'include_node_states',
            8: 'include_numbers',
            9: 'include_name_patterns',
            10: 'include_queues'
        }
        print 'Filter out nodes by:\n%(one)s state %(two)s number' \
              ' %(three)s name substring or RegEx pattern' % {
                    'one': colorize("(1)", color_func='Red_L'),
                    'two': colorize("(2)", color_func='Red_L'),
                    'three': colorize("(3)", color_func='Red_L'),
        }
        print 'Filter in nodes by:\n(any) %(four)s state %(five)s number %(six)s name substring or RegEx pattern\n' \
              '(all) %(seven)s state %(eight)s number %(nine)s name substring or RegEx pattern %(ten)s queue' \
              % {'four': colorize("(4)", color_func='Red_L'),
                 'five': colorize("(5)", color_func='Red_L'),
                 'six': colorize("(6)", color_func='Red_L'),
                 'seven': colorize("(7)", color_func='Red_L'),
                 'eight': colorize("(8)", color_func='Red_L'),
                 'nine': colorize("(9)", color_func='Red_L'),
                 'ten': colorize("(10)", color_func='Red_L'),
                 }
        conf.new_attrs[3] = conf.new_attrs[3] & ~(termios.ECHO | termios.ICANON)
        termios.tcsetattr(sys.__stdin__.fileno(), termios.TCSADRAIN, conf.old_attrs)

        dynamic_config['filtering'] = []
        while True:
            filter_choice = raw_input('\nChoose Filter command, or Enter to exit:-> ',)
            if not filter_choice:
                break

            try:
                filter_choice = int(filter_choice)
            except ValueError:
                break
            else:
                if filter_choice not in filter_map:
                    break

            filter_args = []
            while True:
                user_input = raw_input('\nEnter argument, or Enter to exit:-> ')
                if not user_input:
                    break
                filter_args.append(user_input)

            dynamic_config['filtering'].append({filter_map[filter_choice]: filter_args})

        termios.tcsetattr(sys.__stdin__.fileno(), termios.TCSADRAIN, conf.new_attrs)
        viewport.reset_display()

    elif pressed_char_hex in ['48']:  # H
        cluster.wn_filter = cluster.WNFilter(cluster.worker_nodes)
        filter_map = {
            1: 'or_include_user_id',
            2: 'or_include_user_pat',
            3: 'or_include_queue',
            4: 'include_user_id',
            5: 'include_user_pat',
            6: 'include_queue',
        }
        print 'Highlight cores by:\n' \
              '(any) %(one)s userID %(two)s user name (regex) %(three)s queue\n' \
              '(all) %(four)s userID %(five)s user name (regex) %(six)s queue' \
              % {'one': colorize("(1)", color_func='Red_L'),
                 'two': colorize("(2)", color_func='Red_L'),
                 'three': colorize("(3)", color_func='Red_L'),
                 'four': colorize("(4)", color_func='Red_L'),
                 'five': colorize("(5)", color_func='Red_L'),
                 'six': colorize("(6)", color_func='Red_L'),
                 }
        conf.new_attrs[3] = conf.new_attrs[3] & ~(termios.ECHO | termios.ICANON)
        termios.tcsetattr(sys.__stdin__.fileno(), termios.TCSADRAIN, conf.old_attrs)

        dynamic_config['highlight'] = []
        while True:
            filter_choice = raw_input('\nChoose Highlight command, or Enter to exit:-> ',)
            if not filter_choice:
                break

            try:
                filter_choice = int(filter_choice)
            except ValueError:
                break
            else:
                if filter_choice not in filter_map:
                    break

            filter_args = []
            while True:
                user_input = raw_input('\nEnter argument, or Enter to exit:-> ')
                if not user_input:
                    break
                filter_args.append(user_input)

            dynamic_config['highlight'].append({filter_map[filter_choice]: filter_args})

        termios.tcsetattr(sys.__stdin__.fileno(), termios.TCSADRAIN, conf.new_attrs)
        viewport.reset_display()

    elif pressed_char_hex in ['3f']:  # ?
        viewport.reset_display()
        print '%s opening help...' % colorize('***', 'Green_L')
        if not conf.h_counter.next() % 2:
            dynamic_config['output_fp'] = display.screens[0]
        else:  # exit helpfile
            del dynamic_config['output_fp']

    elif pressed_char_hex in ['72']:  # r
        logging.debug('toggling corelines displayed')
        dynamic_config['rem_empty_corelines'] = (dynamic_config.get('rem_empty_corelines', config['rem_empty_corelines']) +1) %3
        if dynamic_config['rem_empty_corelines'] == 1:
            print '%s Hiding not-really-there ("#") corelines' % colorize('***', 'Green_L')
        elif dynamic_config['rem_empty_corelines'] == 2:
            print '%s Hiding all unused ("#" and "_") corelines' % colorize('***', 'Green_L')
        else:
            print '%s Showing all corelines' % colorize('***', 'Green_L')

        logging.debug('dynamic config corelines: %s' % dynamic_config['rem_empty_corelines'])

    logging.debug('Area Displayed: (h_start, v_start) --> (h_stop, v_stop) '
                  '\n\t(%(h_start)s, %(v_start)s) --> (%(h_stop)s, %(v_stop)s)' %
                  {'v_start': viewport.v_start, 'v_stop': viewport.v_stop,
                   'h_start': viewport.h_start, 'h_stop': viewport.h_stop})


def attempt_faster_xml_parsing(conf):
    if conf.config['faster_xml_parsing']:
        try:
            from lxml import etree
        except ImportError:
            logging.warn('Module lxml is missing. Try issuing "pip install lxml". Reverting to xml module.')
            from xml.etree import ElementTree as etree


def wait_for_keypress_or_autorefresh(display, FALLBACK_TERMSIZE, KEYPRESS_TIMEOUT=1):
    """
    This will make qtop wait for user input for a while,
    otherwise it will auto-refresh the display
    """
    _read_char = 'R'  # initial value, resets view position to beginning
    viewport = display.viewport
    while sys.stdin in select.select([sys.stdin], [], [], KEYPRESS_TIMEOUT)[0]:
        _read_char = sys.stdin.read(1)
        if _read_char:
            logging.debug('Pressed %s' % _read_char)
            break
    else:
        state = viewport.get_term_size()
        term_size = display.calculate_term_size(FALLBACK_TERMSIZE)
        viewport.set_term_size(*term_size)
        new_state = viewport.get_term_size()
        _read_char = '\n' if (state == new_state) else 'r'
        logging.debug("Auto-advancing by pressing <Enter>")

    return _read_char



class Document(namedtuple('Document', ['worker_nodes', 'jobs_dict', 'queues_dict', 'total_running_jobs', 'total_queued_jobs'])):
    def saveas(self, json_file):
        filename = json_file.name
        with open(filename, 'w') as outfile:
            json.dump(self, outfile)




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

    def __init__(self, conf, viewport):
        self.conf = conf
        self.config = self.conf.config
        self.transposed_matrices = []
        self.cluster = None
        self.document = None
        self.viewport = None
        self.wns_occupancy = None
        self.max_line_len = 0
        self.max_height = 0
        self.viewport = viewport  # controls the part of the qtop matrix shown on screen
        self.screens = [conf.HELP_FP, ]  # output_fp is not yet defined, will be appended later

    def init_display(self, output_fp, FALLBACK_TERMSIZE):
        self.screens.append(output_fp)
        self.clear_matrices()
        term_size = self.calculate_term_size(FALLBACK_TERMSIZE)
        self.viewport.set_term_size(*term_size)

    def display_selected_sections(self, _savepath, QTOP_LOGFILE, document, wns_occupancy, cluster):
        """
        This prints out the qtop sections selected by the user.
        The selection can be made in two different ways:
        a) in the QTOPCONF_YAML file, in user_display_parts, where the three sections are named in a list
        b) through cmdline arguments -n, where n is 1,2,3. More than one can be chained together,
        e.g. -13 will exclude sections 1 and 3
        Cmdline arguments should only be able to choose from what is available in QTOPCONF_YAML, though.
        """
        self.document = document
        self.wns_occupancy = wns_occupancy
        self.cluster = cluster

        sections_off = {  # cmdline argument -n
            1: options.sect_1_off,
            2: options.sect_2_off,
            3: options.sect_3_off
        }
        display_parts = {
            'job_accounting_summary': self.display_job_accounting_summary ,
            'workernodes_matrix': self.display_matrices,
            'user_accounts_pool_mappings': self.display_user_accounts_pool_mappings
        }

        if options.WATCH:
            print "\033c",  # comma is to avoid losing the whole first line. An empty char still remains, though.

        for idx, part in enumerate(config['user_display_parts'], 1):
            display_func = display_parts[part]
            display_func() if not sections_off[idx] else None

        if options.STRICTCHECK:
            WNOccupancy.strict_check_jobs(wns_occupancy, cluster)

    def display_job_accounting_summary(self):
        """
        Displays qtop's first section
        """
        cluster = self.cluster
        document = self.document
        total_running_jobs = cluster.total_running_jobs
        total_queued_jobs = cluster.total_queued_jobs
        qstatq_lod = cluster.qstatq_lod

        if options.REMAP:
            if options.CLASSIC:
                print '=== WARNING: --- Remapping WN names and retrying heuristics... good luck with this... ---'
            else:
                logging.warning('=== WARNING: --- Remapping WN names and retrying heuristics... good luck with this... ---')

        ansi_delete_char = "\015"  # this removes the first ever character (space) appearing in the output
        print '%(del)s%(name)s \nv%(version)s ## For feedback and updates, see: %(link)s' \
              % {'name': 'PBS' if options.CLASSIC else colorize('./qtop.py     ## Queueing System report tool. Press ? for '
                                                                'help', 'Cyan_L'),
                 'del': ansi_delete_char,
                 'link': colorize('https://github.com/qtop/qtop', 'Cyan_L'),
                 'version': __version__
                 }
        if scheduler == 'demo':
            msg = "This data is simulated. As soon as you connect to one of the supported scheduling systems,\n" \
                  "you will see live data from your cluster. Press q to Quit."
            print colorize(msg, 'Blue')

        if not options.WATCH:
            print 'Please try it with watch: %s/qtop.py -s <SOURCEDIR> -w [<every_nr_of_sec>]' % QTOPPATH
        print colorize('===> ', 'Gray_D') + colorize('Job accounting summary', 'White') + colorize(' <=== ', 'Gray_D') + \
              colorize(str(datetime.datetime.today())[:-7], 'White')

        print '%(Summary)s: Total:%(total_nodes)s Up:%(online_nodes)s Free:%(available_nodes)s %(Nodes)s | %(' \
              'working_cores)s/%(' \
              'total_cores)s %(Cores)s |' \
              '   %(total_run_jobs)s+%(total_q_jobs)s %(jobs)s (R + Q) %(reported_by)s' % \
              {
                  'Summary': colorize('Summary', 'Cyan_L'),
                  'total_nodes': colorize(str(cluster.total_wn), 'Red_L'),
                  'online_nodes': colorize(str(cluster.total_wn - cluster.offdown_nodes), 'Red_L'),
                  'available_nodes': colorize(str(cluster.available_wn), 'Red_L'),
                  'Nodes': colorize('Nodes', 'Red_L'),
                  'working_cores': colorize(str(cluster.working_cores), 'Green_L'),
                  'total_cores': colorize(str(cluster.total_cores), 'Green_L'),
                  'Cores': colorize('cores', 'Green_L'),
                  'total_run_jobs': colorize(str(int(total_running_jobs)), 'Blue_L'),
                  'total_q_jobs': colorize(str(int(total_queued_jobs)), 'Blue_L'),
                  'jobs': colorize('jobs', 'Blue_L'),
                  'reported_by': 'reported by qstat - q' if options.CLASSIC else ''
              }

        print '%(queues)s :' % {'queues': colorize('Queues', 'Cyan_L')},
        for a_dict in qstatq_lod:
            _queue_name, q_running_jobs, q_queued_jobs, q_state = a_dict['queue_name'], a_dict['run'], a_dict['queued'], a_dict['state']
            # q_running_jobs, q_queued_jobs, q_state = q_tuple.run, q_tuple.queued, q_tuple.state
            account = _queue_name if _queue_name in queue_to_color else 'account_not_colored'
            print "{qname}{star}: {run} {q}|".format(
                qname=colorize(_queue_name, '', pattern=account, mapping=queue_to_color),
                star=colorize('*', 'Red_L') if q_state.startswith('D') or q_state.endswith('S') else '',
                run=colorize(q_running_jobs, '', pattern=account, mapping=queue_to_color),
                q='+ ' + colorize(q_queued_jobs, '', account,
                                       mapping=queue_to_color) + ' ' if q_queued_jobs != '0' else ''),
        print colorize('* implies blocked', 'Red') + '\n'
        # TODO unhardwire states from star kwarg

    def display_matrices(self):
        """
        Displays qtop's second section, the main worker node matrices.
        """
        print_char_start = self.wns_occupancy.print_char_start
        print_char_stop = self.wns_occupancy.print_char_stop
        wns_occupancy = self.wns_occupancy
        cluster = self.cluster

        self.display_basic_legend()
        self.display_matrix(wns_occupancy, print_char_start, print_char_stop)
        # the transposed matrix is one continuous block, doesn't make sense to break into more matrices
        if not dynamic_config.get('transpose_wn_matrices', config['transpose_wn_matrices']):
            self.display_remaining_matrices(wns_occupancy, print_char_start, print_char_stop)

    def display_basic_legend(self):
        """Displays the Worker Nodes occupancy label plus columns explanation"""
        if dynamic_config.get('transpose_wn_matrices', self.config['transpose_wn_matrices']):
            note = "/".join(self.config['occupancy_column_order'])
        else:
            note = 'you can read vertically the node IDs'

        print colorize('===> ', 'Gray_D') + \
              colorize('Worker Nodes occupancy', 'White') + \
              colorize(' <=== ', 'Gray_D') + \
              colorize('(%s)', 'Gray_D') % note

    def display_user_accounts_pool_mappings(self):
        """
        Displays qtop's third section
        """
        wns_occupancy = self.wns_occupancy
        try:
            account_jobs_table = self.wns_occupancy.account_jobs_table
            userid_to_userid_re_pat = self.wns_occupancy.userid_to_userid_re_pat
        except KeyError:
            account_jobs_table = dict()
            userid_to_userid_re_pat = dict()

        detail_of_name = get_detail_of_name(self.conf, account_jobs_table)
        print colorize('\n===> ', 'Gray_D') + \
              colorize('User accounts and pool mappings', 'White') + \
              colorize(' <=== ', 'Gray_d') + \
              colorize("  ('all' also includes those in C and W states, as reported by qstat)"
                            if options.CLASSIC else "(sorting according to total nr. of jobs)", 'Gray_D')

        print '[id] unix account      |jobs >=   R +    Q | nodes | %(msg)s' % \
              {'msg': 'Grid certificate DN (info only available under elevated privileges)' if options.CLASSIC else
              '      GECOS field or Grid certificate DN |'}
        for line in account_jobs_table:
            uid, runningjobs, queuedjobs, alljobs, user, num_of_nodes = line
            userid_pat = userid_to_userid_re_pat[str(uid)]

            if (options.COLOR == 'OFF' or userid_pat == 'account_not_colored' or conf.user_to_color[userid_pat] == 'reset'):
                conditional_width = 0
                userid_pat = 'account_not_colored'
            else:
                conditional_width = 12

            print_string = ('[ {0:<{width1}}] '
                            '{4:<{width18}}{sep}'
                            '{3:>{width4}}   {1:>{width4}}   {2:>{width4}} {sep} '
                            '{6:>{width5}} {sep} '
                            '{5:<{width40}} {sep}').format(
                colorize(str(uid), pattern=userid_pat),
                colorize(str(runningjobs), pattern=userid_pat),
                colorize(str(queuedjobs), pattern=userid_pat),
                colorize(str(alljobs), pattern=userid_pat),
                colorize(user, pattern=userid_pat),
                colorize(detail_of_name.get(user, ''), pattern=userid_pat),
                colorize(num_of_nodes, pattern=userid_pat),
                sep=colorize(config['SEPARATOR'], pattern=userid_pat),
                width1=1 + conditional_width,
                width3=3 + conditional_width,
                width4=4 + conditional_width,
                width5=5 + conditional_width,
                width18=18 + conditional_width,
                width40=40 + conditional_width,
            )
            print print_string

    def display_matrix(self, wns_occupancy, print_char_start, print_char_stop):
        """
        occupancy_parts needs to be redefined for each matrix, because of changed parameter values
        """
        if self.wns_occupancy.is_matrix_coreless(print_char_start, print_char_stop):
            return

        wn_vert_labels = wns_occupancy.wn_vert_labels
        core_user_map = wns_occupancy.core_user_map
        extra_matrices_nr = wns_occupancy.extra_matrices_nr
        userid_to_userid_re_pat = wns_occupancy.userid_to_userid_re_pat
        mapping = config['core_coloring']

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
                    (core_user_map, print_char_start, print_char_stop, userid_to_userid_re_pat, mapping),
                    {'attrs': None}
                ),
        }

        # custom part, e.g. Node state, queue state etc
        for yaml_key, part_name, systems in yaml.get_yaml_key_part(config, scheduler.scheduler_name, outermost_key='workernodes_matrix'):
            if scheduler.scheduler_name not in systems: continue

            new_occupancy_part = {
                part_name:
                    (
                        self.print_mult_attr_line,  # func
                        (print_char_start, print_char_stop),  # args
                        {'attr_lines': getattr(wns_occupancy, part_name), 'coloring': queue_to_color}  # kwargs
                    )
            }
            occupancy_parts.update(new_occupancy_part)

        # get additional info from QTOPCONF_YAML
        for part_dict in config['workernodes_matrix']:
            part = [k for k in part_dict][0]
            key_vals = part_dict[part]
            if scheduler.scheduler_name not in yaml.fix_config_list(key_vals.get('systems', [scheduler.scheduler_name])):
                continue
            occupancy_parts[part][2].update(key_vals)  # get extra options from user

            func_, args, kwargs = occupancy_parts[part][0], occupancy_parts[part][1], occupancy_parts[part][2]
            func_(*args, **kwargs)

        if dynamic_config.get('transpose_wn_matrices', config['transpose_wn_matrices']):
            order = config['occupancy_column_order']
            for idx, (item, matrix) in enumerate(zip(order, self.transposed_matrices)):
                matrix[0] = order.index(matrix[1])

            self.transposed_matrices.sort(key=lambda item: item[0])
            ###TRY###
            for line_tuple in izip_longest(*[tpl[2] for tpl in self.transposed_matrices], fillvalue=utils.ColorStr('  ', color='Purple', )):
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
        term_columns = self.viewport.h_term_size

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
        s = "".join([colorize(char.initial, color_func=char.color) if isinstance(char, utils.ColorStr) else char
                     for char in joined_list[self.viewport.h_start:self.viewport.h_stop]])
        print utils.compress_colored_line(s)
        return joined_list

    def print_core_lines(self, core_user_map, print_char_start, print_char_stop,
                         userid_to_userid_re_pat, mapping, attrs, options1, options2):

        signal(SIGPIPE, SIG_DFL)
        remove_corelines = dynamic_config.get('rem_empty_corelines', config['rem_empty_corelines']) + 1

        # if corelines vertical (transposed matrix)
        if dynamic_config.get('transpose_wn_matrices', config['transpose_wn_matrices']):
            non_existent_symbol = config['non_existent_node_symbol']
            for core_x_vector, ind, k, is_corevector_removable in self.wns_occupancy.gauge_core_vectors(core_user_map,
                                                                                    print_char_start,
                                                                                    print_char_stop,
                                                                                    non_existent_symbol,
                                                                                    remove_corelines):
                if is_corevector_removable:
                    del core_user_map[k]

            tuple_ = [None, 'core_map', self.transpose_matrix(core_user_map, colored=False, coloring_pat=userid_to_userid_re_pat)]
            self.transposed_matrices.append(tuple_)
            return
        else:
            # if corelines horizontal (non-transposed matrix)
            for core_line in self.get_core_lines(core_user_map, print_char_start, print_char_stop,
                                                 userid_to_userid_re_pat, mapping, attrs):
                core_line_zipped = utils.compress_colored_line(core_line)
                try:
                    print core_line_zipped
                except IOError:
                    try:
                        signal(SIGPIPE, SIG_DFL)
                        print core_line_zipped
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

        if not dynamic_config['force_names']:
            node_str_width = len(str(highest_wn))  # 4 for thousands of nodes, nr of horizontal lines to be displayed

            for node_nr in range(1, node_str_width + 1):
                d[str(node_nr)] = wn_vert_labels[str(node_nr)]
            end_labels_iter = iter(end_labels[str(node_str_width)])
            self.print_wnid_lines(d, start, stop, end_labels_iter,
                                  color_func=self.color_plainly, args=('White', 'Gray_L', start > 0))
            # start > 0 is just a test for a possible future condition

        elif dynamic_config['force_names']:  # the actual names of the WNs instead of numbered WNs [was: or options.FORCE_NAMES]
            node_str_width = len(wn_vert_labels)  # key, nr of horizontal lines to be displayed

            # for longer full-labeled wn ids, add more end-labels (far-right) towards the bottom
            for num in range(8, len(wn_vert_labels) + 1):
                end_labels.setdefault(str(num), end_labels['7'] + num * ['={________}'])

            end_labels_iter = iter(end_labels[str(node_str_width)])
            self.print_wnid_lines(wn_vert_labels, start, stop, end_labels_iter,
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

    def print_wnid_lines(self, d, start, stop, end_labels, color_func, args):
        if dynamic_config.get('transpose_wn_matrices', self.config['transpose_wn_matrices']):
            tuple_ = [None, 'wnid_lines', self.transpose_matrix(d)]
            self.transposed_matrices.append(tuple_)
            return

        separators = config['vertical_separator_every_X_columns']
        for line_nr, end_label in zip(d, end_labels):
            colors = color_func(*args)
            wn_id_str = self._insert_separators(d[line_nr][start:stop], config['SEPARATOR'], separators)
            wn_id_str = ''.join([colorize(elem, next(colors)) for elem in wn_id_str])
            print wn_id_str + end_label

    def show_part_view(self, _timestr, file, x, y):
        """
        Prints part of the qtop output to the terminal (as fast as possible!)
        Justification for implementation:
        http://unix.stackexchange.com/questions/47407/cat-line-x-to-line-y-on-a-huge-file
        """
        temp_f = tempfile.NamedTemporaryFile(delete=False, suffix='.out', prefix='qtop_partview_%s_' % _timestr, dir=config[
            'savepath'])
        pre_cat_command = '(tail -n+%s %s | head -n%s) > %s' % (x, file, y - 1, temp_f.name)
        _ = subprocess.call(pre_cat_command, stdout=stdout, stderr=stdout, shell=True)
        logging.debug('dynamic_config filename in main loop: %s' % file)
        return temp_f.name

    def print_mult_attr_line(self, print_char_start, print_char_stop, attr_lines, label, color_func=None,
                             **kwargs):
        """
        attr_lines can be e.g. Node state lines
        """
        if dynamic_config.get('transpose_wn_matrices', config['transpose_wn_matrices']):
            tuple_ = [None, label, self.transpose_matrix(attr_lines, colored=True, coloring_pat=None)]
            self.transposed_matrices.append(tuple_)
            return

        # TODO: fix option parameter, inserted for testing purposes
        for _line in attr_lines:
            line = attr_lines[_line][print_char_start:print_char_stop]
            # TODO: maybe put attr_line and label as kwd arguments? collect them as **kwargs
            attr_line = self._insert_separators(line, config['SEPARATOR'], config['vertical_separator_every_X_columns'])
            attr_line = ''.join([colorize(char.initial, color_func=char.color) for char in attr_line])
            print attr_line + "=" + label

    def get_core_lines(self, core_user_map, print_char_start, print_char_stop, coloring_pattern, mapping, attrs):
        """
        yields all coreX lines, except cores that don't show up
        anywhere in the given matrix
        """
        non_existent_symbol = config['non_existent_node_symbol']
        remove_corelines = dynamic_config.get('rem_empty_corelines', config['rem_empty_corelines']) + 1
        for core_x_vector, ind, k, is_corevector_removable in self.wns_occupancy.gauge_core_vectors(core_user_map,
                                                                                 print_char_start,
                                                                                 print_char_stop,
                                                                                 non_existent_symbol,
                                                                                 remove_corelines):
            if is_corevector_removable:
                continue

            core_x_vector = self._insert_separators(core_x_vector, config['SEPARATOR'],
                                                    config['vertical_separator_every_X_columns'])
            colored_core_x = [colorize(elem, color_func=elem.color) for elem in core_x_vector]
            core_x_vector = ''.join([str(item) for item in colored_core_x])
            yield core_x_vector + colorize('=Core' + str(ind), pattern='account_not_colored')

    def transpose_matrix(self, d, colored=False, reverse=False, coloring_pat=None):
        """
        takes a dictionary whose values are lists of strings (=matrix)
        returns a transposed matrix
        colors it in the meantime, if instructed to via colored
        """
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

    def clear_matrices(self):
        self.transposed_matrices = []

    def set_max_line_height(self, output_fp):
        """
        Returns the char dimensions of the entirety of the qtop output file
        """
        ansi_escape = re.compile(r'\x1b[^m]*m')  # matches ANSI escape characters

        if not self.max_height:
            with open(output_fp, 'r') as f:
                self.max_height = len(f.readlines())
                if not self.max_height:
                    raise ValueError("There is no output from qtop *whatsoever*. Weird.")

        self.viewport.max_height = display.max_height
        self.max_line_len = max(len(ansi_escape.sub('', line.strip())) for line in open(output_fp, 'r')) \
            if not self.max_line_len else self.max_line_len

        logging.debug('Total nr of lines: %s' % self.max_height)
        logging.debug('Max line length: %s' % self.max_line_len)

    def calculate_term_size(self, FALLBACK_TERM_SIZE):
        """
        Gets the dimensions of the terminal window where qtop will be displayed.
        """
        config = self.conf.config
        fallback_term_size = config.get('term_size', FALLBACK_TERM_SIZE)

        _command = subprocess.Popen('stty size', stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        tty_size, error = _command.communicate()
        if not error:
            term_height, term_columns = [int(x) for x in tty_size.strip().split()]
            logging.debug('terminal size v, h from "stty size": %s, %s' % (term_height, term_columns))
        else:
            logging.warn(
                "Failed to autodetect terminal size. (Running in an IDE?in a pipe?) Trying values in %s." % QTOPCONF_YAML)
            try:
                term_height, term_columns = self.viewport.get_term_size()
                if not all(term_height, term_columns):
                    raise ValueError
            except ValueError:
                try:
                    term_height, term_columns = yaml.fix_config_list(self.viewport.get_term_size())
                except KeyError:
                    term_height, term_columns = fallback_term_size
                    logging.debug('(hardcoded) fallback terminal size v, h:%s, %s' % (term_height, term_columns))
                else:
                    logging.debug('fallback terminal size v, h:%s, %s' % (term_height, term_columns))
            except (KeyError, TypeError):  # TypeError if None was returned i.e. no setting in QTOPCONF_YAML
                term_height, term_columns = fallback_term_size
                logging.debug('(hardcoded) fallback terminal size v, h:%s, %s' % (term_height, term_columns))

        return int(term_height), int(term_columns)


class Cluster(object):
    def __init__(self, conf, worker_nodes, job_ids, user_names, job_states, job_queues, total_running_jobs, total_queued_jobs, qstatq_lod, WNFilter):
        self.conf = conf
        self.worker_nodes = worker_nodes
        # self.queues_dict = queues_dict  # ex qstatq_lod is now list of namedtuples
        self.total_running_jobs = total_running_jobs
        self.total_queued_jobs = total_queued_jobs
        self.qstatq_lod = qstatq_lod
        self.job_ids = job_ids
        self.user_names = user_names
        self.job_states = job_states
        self.job_queues = job_queues
        self.config = conf.config
        self.options = conf.cmd_options
        self.WNFilter = WNFilter

        self.wn_filter = None
        self.working_cores = 0
        self.offdown_nodes = 0
        self.total_cores = 0
        self.core_span = []
        self.highest_wn = 0
        self.node_subclusters = set()
        self.workernode_dict = {}
        self.workernode_dict_remapped = {}  # { remapnr: [state, np, (core0, job1), (core1, job1), ....]}

        self.available_wn = sum([len(node['state']) for node in self.worker_nodes if str(node['state'][0]) == '-'])
        self.total_wn = len(self.worker_nodes)  # == existing_nodes
        self.workernode_list = []
        self.workernode_list_remapped = range(1, self.total_wn + 1)  # leave xrange aside for now

    def process(self):
        self._keep_queue_initials_only_and_colorize(queue_to_color)
        self._colorize_nodestate()
        self._calculate_WN_dict()

    def _keep_queue_initials_only_and_colorize(self, queue_to_color):
        # TODO remove monstrosity!
        for worker_node in self.worker_nodes:
            color_q_list = []
            for queue in worker_node['qname']:
                color_q = utils.ColorStr(queue, color=queue_to_color.get(queue, ''))
                color_q_list.append(color_q)
            worker_node['qname'] = color_q_list

    def _colorize_nodestate(self):
        nodestate_to_color = self.conf.nodestate_to_color
        # TODO remove monstrosity!
        for worker_node in self.worker_nodes:
            full_nodestate = worker_node['state']  # actual node state
            total_color_nodestate = []
            for nodestate in worker_node['state']:  # split nodestate for displaying purposes
                color_nodestate = utils.ColorStr(nodestate, color=nodestate_to_color.get(full_nodestate, ''))
                total_color_nodestate.append(color_nodestate)
            worker_node['state'] = total_color_nodestate


    def _calculate_WN_dict(self):
        if not self.worker_nodes:
            raise ValueError("Empty Worker Node list. Exiting...")

        max_np, _all_str_digits_with_empties = self._get_wn_list_and_stats()

        self.core_span = [str(x) for x in range(max_np)]
        self.options.REMAP = self.decide_remapping(_all_str_digits_with_empties)

        nodes_drop, workernode_dict, workernode_dict_remapped = self.map_worker_nodes_to_wn_dict()
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
        self._calculate_jobs_per_node(self.workernode_dict)

        del self.node_subclusters  # sets are not JSON serialisable!!
        del self.workernode_list_remapped
        del self.workernode_dict_remapped

    def _get_wn_list_and_stats(self):
        max_np = 0
        re_nodename = r'(^[A-Za-z0-9-]+)(?=\.|$)' if not self.options.ANONYMIZE else r'\w_anon_wn_\d+'
        all_str_digits_with_empties = list()
        worker_nodes = self.worker_nodes

        for node in worker_nodes:
            nodename_match = re.search(re_nodename, node['domainname'])
            _nodename = nodename_match.group(0)

            # get subclusters by name change
            _node_letters = ''.join(re.findall(r'\D+', _nodename))
            self.node_subclusters.update([_node_letters])

            node_str_digits = "".join(re.findall(r'\d+', _nodename))
            all_str_digits_with_empties.append(node_str_digits)

            self.total_cores += int(node.get('np'))  # for stats only
            max_np = max(max_np, int(node['np']))
            self.offdown_nodes += 1 if "".join(([n.str for n in node['state']])) in 'do'  else 0
            self.working_cores += len(node.get('core_job_map', dict()))

            try:
                _cur_node_nr = int(node_str_digits)
            except ValueError:
                _cur_node_nr = _nodename

            # create workernode_list
            self.workernode_list.append(_cur_node_nr)

        # node_subclusters, workernode_list, offdown_nodes, working_cores changed here
        return max_np, all_str_digits_with_empties

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
            return False

        _all_str_digits = filter(lambda x: x != "", all_str_digits_with_empties)
        _all_digits = [int(digit) for digit in _all_str_digits]

        if (
                options.BLINDREMAP or
                len(self.node_subclusters) > 1 or
                min(self.workernode_list) >= config['exotic_starting_wn_nr'] or
                self.offdown_nodes >= self.total_wn * config['percentage'] or
                len(all_str_digits_with_empties) != len(_all_str_digits) or
                len(_all_digits) != len(_all_str_digits)
        ):
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
                workernode_dict[node] = {'state': '?', 'np': 0, 'domainname': 'N/A', 'host': 'N/A', 'core_job_map': {}}
                default_values_for_empty_nodes = dict([(yaml_key, '?') for yaml_key, part_name, _ in yaml.get_yaml_key_part(
                    config, scheduler, outermost_key='workernodes_matrix')])
                workernode_dict[node].update(default_values_for_empty_nodes)
        return workernode_dict

    def map_worker_nodes_to_wn_dict(self):
        """
        For filtering to take place,
        1) a filter should be defined in QTOPCONF_YAML
        2) remap should be either selected by the user or enforced by the circumstances
        """
        # nodes_drop: this amount has to be chopped off of the end of workernode_list_remapped
        nodes_drop = 0  # count change in nodes after filtering
        workernode_dict = dict()
        workernode_dict_remapped = dict()
        _sorting_from_conf = self.config['sorting']
        _first_sort_by = _sorting_from_conf.values

        user_sorting = dynamic_config.get('user_sort', (_sorting_from_conf and _first_sort_by()[0]))
        user_filters = dynamic_config.get('filtering', self.config['filtering'])
        user_filtering = user_filters and user_filters[0]

        if user_filtering and self.options.REMAP:
            len_wn_before = len(self.worker_nodes)
            self.wn_filter = self.WNFilter(self.worker_nodes)
            # modified: self.worker_nodes, self.offdown_nodes, self.available_wn, self.working_cores, self.total_cores
            self.worker_nodes, self.available_wn, self.working_cores, self.total_cores = self.wn_filter.filter_worker_nodes(filter_rules=user_filters)
            len_wn_after = len(self.worker_nodes)
            nodes_drop = len_wn_after - len_wn_before

        if user_sorting:
            self.worker_nodes = self._sort_worker_nodes()

        for (batch_node, (idx, cur_node_nr)) in zip(self.worker_nodes, enumerate(self.workernode_list)):
            # It looks as if there's an error in the for loop, because self.worker_nodes and workernode_list
            # have different lengths if there's a filter in place, BUT it is OK, since
            # it is just the idx counter that is taken into account in remapping.
            workernode_dict[cur_node_nr] = batch_node
            workernode_dict_remapped[idx] = batch_node

        return nodes_drop, workernode_dict, workernode_dict_remapped

    def _sort_worker_nodes(self):
        order = {
            "sort by nodename-notnum" : 're.sub(r"[^A-Za-z _.-]+", "", node["domainname"]) or "0"',
            "sort by nodename-notnum length" : "len(node['domainname'].split('.', 1)[0].split('-')[0])",
            "sort by all numbers" : 'int(re.sub(r"[A-Za-z _.-]+", "", node["domainname"]) or "0")',
            "sort by first letter" : "ord(node['domainname'][0])",
            "sort by node state" : "ord(str(node['state'][0]))",
            "sort by nr of cores" : "int(node['np'])",
            "sort by core occupancy" : "len(node['core_job_map'])",
            "sort by custom definition" : "",
            "sort reset" : "0",
            # "sort_by_num_adjacent_to_first_word" : "int(re.sub(r'[A-Za-z_.-]+', '', node['domainname'].split('.', 1)[0].split('-')[0]) or -1)",
            # "sort_by_first_word" : "node['domainname'].split('.', 1)[0].split('-')[0]",
        }
        if dynamic_config.get('user_sort'):  # live user sorting overrides yaml config sorting
            # following join content also takes custom definition argument into account
            sort_str = ", ".join(order[k[0]] or k[1][0] for k in dynamic_config.get('user_sort', []))
        elif self.config.get('sorting', {}).get('user_sort'):
            sort_str = ", ".join(order[k] for k in self.config['sorting']['user_sort'])
        else:
            return self.worker_nodes

        sort_sequence = "lambda node: (" + sort_str + ")"
        try:
            self.worker_nodes.sort(key=eval(sort_sequence), reverse=self.config['sorting']['reverse'])
        except (IndexError, ValueError):
            logging.critical("There's (probably) something wrong in your sorting lambda in %s." % QTOPCONF_YAML)
            raise
        except KeyError as e:
            msg = "Worker Nodes don't contain '%s' as a key." % e.message
            logging.error(colorize(msg, color_func='Red_L'))
        except NameError as e:
            msg = "Wrong input '%s'. Please check the examples in qtopconf.yaml." % e.message
            logging.error(colorize(msg, color_func='Red_L'))

        return self.worker_nodes

    def _calculate_jobs_per_node(self, workernode_dict):
        for node in workernode_dict:
            node_job_set = set(workernode_dict[node]['core_job_map'].values())
            workernode_dict[node]['node_job_set'] = node_job_set


def colorize(text, color_func=None, pattern='NoPattern', mapping=None, bg_color=None, bold=False):
    """
    prints colored text according to at least one of the keyword arguments available:
    color_func can be a direct ansi color name or a function providing a color name.
    If color is given directly as color_func, pattern/mapping are not needed.
    If no color_func and no mapping are defined, the default mapping is used,
    which is user_to_color, mapping an account name to a color.
    A pattern must then be given, which is the key of the mapping.
    Other mappings available are: nodestate_to_color, queue_to_color.
    Examples:
    s = ColorStr(string='This is some text', color='Red_L')
    print colorize(s, color_func=s.color), # Red_L is applied directly
    print colorize(s.str, pattern='alicesgm') # mapping defaults to user_to_color
    print colorize(s.str, color_func=s.color, bg_color='BlueBG') # bg and fg colors applied directly

    state = ColorStr('running. coloring according to node state')
    print colorize(state.str, mapping=nodestate_to_color, pattern=state.initial)
    """
    bg_color = 'NOBG' if not bg_color else bg_color
    if not mapping:
        mapping = conf.user_to_color
    try:
        ansi_color = color_to_code[color_func] if color_func else color_to_code[mapping[pattern]]
    except KeyError:
        return text
    else:
        if bold and ansi_color[0] in '01':
            ansi_color = '1' + ansi_color[1:]
        if options.COLOR == 'ON' and pattern != 'account_not_colored':
            text = "\033[%(fg_color)s%(bg_color)sm%(text)s\033[0;m" \
                   % {'fg_color': ansi_color, 'bg_color': color_to_code[bg_color], 'text': text}

        return text


def pick_frames_to_replay(conf):
    """
    getting the respective info from cmdline switch -R,
    pick the relevant qtop output from savepath to replay
    """
    _savepath = conf.savepath
    conf.options.WATCH = [0]  # enforce that --watch mode is on, even if not in cmdline switch
    conf.options.BATCH_SYSTEM = 'demo'  # default state

    if options.REPLAY[0] == 0:  # add default arg, if no replay start time is set in the cmdline
        time_delta = fileutils.get_timedelta(fileutils.parse_time_input(conf.config['replay_last']))
        some_time_ago = datetime.datetime.now() - time_delta
        options.REPLAY[0] = some_time_ago.strftime("%Y%m%dT%H%M%S")
    if len(options.REPLAY) == 1:  # add default arg, if no replay duration is set in the cmdline
        options.REPLAY.append('2m')

    time_delta = fileutils.get_timedelta(fileutils.parse_time_input(options.REPLAY[1]))
    watch_start_datetime_obj = utils.get_date_obj_from_str(options.REPLAY[0], datetime.datetime.now())
    REC_FP_ALL = _savepath + '/*_partview*.out'
    rec_files = glob.iglob(REC_FP_ALL)
    useful_frames = []

    for rec_file in rec_files:
        rec_file_last_modified_date = datetime.datetime.strptime(rec_file.rsplit('/',1)[-1].split('_')[2], "%Y%m%dT%H%M%S")
        if datetime.timedelta(seconds=0) < rec_file_last_modified_date - watch_start_datetime_obj < time_delta:
            useful_frames.append(rec_file)

    useful_frames = iter(useful_frames[::-1])
    return useful_frames



class WNFilter(object):
    def __init__(self, worker_nodes):
        self.worker_nodes = worker_nodes

    def mark_list_by_queue(self, nodes, arg_list=None):
        for idx, node in enumerate(nodes[:]):
            if set(arg_list) & set([x.str for x in node['qname']]):
                node['mark'] = '*'
        return nodes

    def mark_list_by_number(self, nodes, arg_list=None):
        for idx, node in enumerate(nodes):
            if str(idx) in arg_list:
                node['mark'] = '*'
        return nodes

    def mark_list_by_node_state(self, nodes, arg_list=None):
        for idx, node in enumerate(nodes):
            if set(["".join(state.str for state in node['state'])]) & set(arg_list):
                node['mark'] = '*'
        return nodes

    def mark_list_by_name_pattern(self, nodes, arg_list=None):
        for idx, node in enumerate(nodes):
            patterns = arg_list.values()[0] if isinstance(arg_list, dict) else arg_list
            for pattern in patterns:
                match = re.search(pattern, node['domainname'].split('.', 1)[0])
                try:
                    match.group(0)
                except AttributeError:
                    pass
                else:
                    node['mark'] = '*'
        return nodes

    def keep_marked(self, t, rule, final_pass=False):
        if (rule.startswith('or_') and not final_pass) or (not rule.startswith('or_') and final_pass):
            return t
        nodes = filter(lambda item: item.get('mark'), t)
        for item in nodes:
            if item.get('mark'):
                del item['mark']
        return nodes

    def keep_unmarked(self, t, rule, final_pass=False):
        return filter(lambda item: not item.get('mark'), t)

    def filter_worker_nodes(self, filter_rules=None):
        """
        Keeps specific nodes according to the filter rules in QTOPCONF_YAML
        """
        working_cores = cluster.working_cores
        total_cores = cluster.total_cores

        filter_types = {
            'exclude_numbers': (self.mark_list_by_number, self.keep_unmarked),
            'exclude_node_states': (self.mark_list_by_node_state, self.keep_unmarked),
            'exclude_name_patterns': (self.mark_list_by_name_pattern, self.keep_unmarked),
            'or_include_numbers': (self.mark_list_by_number, self.keep_marked),
            'or_include_node_states': (self.mark_list_by_node_state, self.keep_marked),
            'or_include_name_patterns': (self.mark_list_by_name_pattern, self.keep_marked),
            'include_queues': (self.mark_list_by_queue, self.keep_marked),
            'include_numbers': (self.mark_list_by_number, self.keep_marked),
            'include_node_states': (self.mark_list_by_node_state, self.keep_marked),
            'include_name_patterns': (self.mark_list_by_name_pattern, self.keep_marked),
        }

        if filter_rules:
            # TODO display this somewhere in qtop!
            if WNFilter.report_filtered_view.count() < 2:
                WNFilter.report_filtered_view()
            nodes = self.worker_nodes[:]
            for filter_rule in filter_rules:
                rule, args = filter_rule.items()[0]
                mark_func, keep = filter_types[rule]
                nodes = mark_func(nodes, args)
                nodes = keep(nodes, rule)
            else:
                nodes = keep(nodes, rule, final_pass=True)

            if len(nodes):
                self.worker_nodes = dict((v['domainname'], v) for v in nodes).values()
                cluster.offdown_nodes = sum([1 if "".join(([n.str for n in node['state']])) in 'do'  else 0 for node in
                                     self.worker_nodes])
                self.available_wn = sum(
                    [len(node['state']) for node in self.worker_nodes if str(node['state'][0]) == '-'])
                working_cores = sum(len(node.get('core_job_map', dict())) for node in self.worker_nodes)
                total_cores = sum(int(node.get('np')) for node in self.worker_nodes)
            else:
                logging.error(colorize('Selected filter results in empty worker node set. Cancelling.', 'Red_L'))

        return self.worker_nodes, self.available_wn, working_cores, total_cores

    @staticmethod
    @utils.CountCalls
    def report_filtered_view():
        logging.error("%s WN Occupancy view is filtered." % colorize('***', 'Green_L'))



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


class SchedulerRouter(object):
    def __init__(self, conf):
        self.conf = conf
        self.options = conf.cmd_options
        self.config = conf.config
        self.scheduler = None
        self.available_batch_systems = self._discover_qtop_batch_systems()
        self.scheduler_name = self._decide_batch_system(self.conf.env['QTOP_SCHEDULER'])
        self.scheduler_output_filenames = self._fetch_scheduler_files()

    def _pick_scheduler(self):
        return self.available_batch_systems[self.scheduler_name](self.scheduler_output_filenames, self.conf)

    def _decide_batch_system(self, env_var):
        """
        Qtop first checks in cmdline switches, environmental variables and the config files, in this order,
        for the scheduler type. If it's not indicated and "auto" is, it will attempt to guess the scheduler type
        from the scheduler shell commands available in the linux system.
        """
        config = self.conf.config
        cmdline_switch = self.conf.cmd_options.BATCH_SYSTEM
        config_file_batch_option = self.conf.config['scheduler']
        schedulers = self.conf.config['schedulers']
        avail_systems = self.available_batch_systems.keys() + ['auto']
        if cmdline_switch and cmdline_switch.lower() not in avail_systems:
            logging.critical(
                "Selected scheduler system not supported. Available choices are %s." % ", ".join(avail_systems))
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

    def _discover_qtop_batch_systems(self):
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

    def get_jobs_info(self):
        self.scheduler = self._pick_scheduler()
        return self.scheduler.get_jobs_info()

    def get_queues_info(self):
        return self.scheduler.get_queues_info()

    def get_worker_nodes(self, job_ids, job_queues, conf):
        return self.scheduler.get_worker_nodes(job_ids, job_queues, conf)

    def _fetch_scheduler_files(self):
        options = self.conf.cmd_options
        config = self.config
        INPUT_FNs_commands = self._finalize_filepaths_schedulercommands(options, config)
        scheduler_output_filenames = self._get_input_filenames(INPUT_FNs_commands, config)
        return scheduler_output_filenames

    def _finalize_filepaths_schedulercommands(self, options, config):
        """
        returns a dictionary with contents of the form
        {fn : (filepath, schedulercommand)}, e.g.
        {'pbsnodes_file': ('savepath/pbsnodes_a.txt', 'pbsnodes -a')}
        if the -s switch (set sourcedir) has been invoked, or
        {'pbsnodes_file': ('savepath/pbsnodes_a<some_pid>.txt', 'pbsnodes -a')}
        if ran without the -s switch.
        """
        d = dict()
        fn_append = "_" + str(os.getpid()) if not options.SOURCEDIR else ""
        for fn, path_command in config['schedulers'][self.scheduler_name].items():
            path, command = path_command.strip().split(', ')
            path = path % {"savepath": options.workdir, "pid": fn_append}
            command = command % {"savepath": options.workdir}
            d[fn] = (path, command)
        return d

    def _get_input_filenames(self, INPUT_FNs_commands, config):
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
                _savepath = os.path.realpath(os.path.expandvars(config['savepath']))
                filenames[_file] = execute_shell_batch_commands(batch_system_commands, filenames, _file, _savepath)

            if not os.path.isfile(filenames[_file]):
                raise fileutils.FileNotFound(filenames[_file])
        return filenames

    # def _fetch_scheduler_files(self, options, config):
    #     INPUT_FNs_commands = self._finalize_filepaths_schedulercommands(options, config)
    #     scheduler_output_filenames = self._get_input_filenames(INPUT_FNs_commands, config)
    #     return scheduler_output_filenames


if __name__ == '__main__':

    stdout = sys.stdout  # keep a copy of the initial value of sys.stdout

    conf = utils.conf
    conf.auto_config()
    conf.initialize_paths()
    conf.load_yaml_config()
    display = TextDisplay(conf, Viewport())
    conf.process_yaml_config() # TODO user_to_color is updated here !!
    conf.update_config_with_cmdline_vars()

    config = conf.config
    options = conf.cmd_options
    dynamic_config = conf.dynamic_config
    QTOPPATH = conf.QTOPPATH
    attempt_faster_xml_parsing(conf)

    if options.REPLAY:
        useful_frames = pick_frames_to_replay(conf)  # WAS conf.config['savepath'] CHECK!! 20170702

    web = Web(conf.initial_cwd)
    if options.WEB:
        web.start()

    with raw_mode(sys.stdin):  # key listener implementation
        try:
            while True:
                sample = fileutils.Sample(conf)
                savepath = conf.savepath
                timestr = time.strftime("%Y%m%dT%H%M%S")

                handle, output_fp = fileutils.get_new_temp_file(savepath,
                                                                prefix='qtop_fullview_%s_' % timestr,
                                                                suffix='.out')
                conf.init_dirs()
                display.init_display(output_fp, FALLBACK_TERMSIZE)
                sys.stdout = os.fdopen(handle, 'w')  # redirect everything to file, creates file object out of handle

                scheduler = SchedulerRouter(conf)

                if options.SAMPLE:
                    sample.set_sample_filename_format_from_conf(config)
                    sample.init_sample_file(savepath, scheduler.scheduler_output_filenames, QTOPCONF_YAML, QTOPPATH)

                ###### Gather data ###############
                #
                job_ids, user_names, job_states, job_queues = scheduler.get_jobs_info()
                total_running_jobs, total_queued_jobs, qstatq_lod = scheduler.get_queues_info()
                worker_nodes = scheduler.get_worker_nodes(job_ids, job_queues, conf)

                ###### Process data ###############
                #
                args = (conf, worker_nodes, job_ids, user_names, job_states,
                        job_queues, total_running_jobs, total_queued_jobs, qstatq_lod, WNFilter)

                cluster = Cluster(*args)
                cluster.process()
                wns_occupancy = WNOccupancy.WNOccupancy(cluster)
                # TODO: the cut into matrices should be put in the display data part. No viewport in calculations.
                wns_occupancy.calculate(conf.user_to_color, display.viewport.h_term_size, job_ids, scheduler.scheduler_name)

                ###### Export data ###############
                #
                JobDoc = namedtuple('JobDoc', ['user_name', 'job_state', 'job_queue'])
                jobs_dict = dict(
                    (re.sub(r'\[\]$', '', job_id), JobDoc(user_name, job_state, job_queue))
                     for job_id, user_name, job_state, job_queue in izip(job_ids, user_names, job_states, job_queues))

                QDoc = namedtuple('QDoc', ['lm', 'queued', 'run', 'state'])
                queues_dict = OrderedDict(
                    (qstatq['queue_name'], (QDoc(str(qstatq['lm']), qstatq['queued'], qstatq['run'], qstatq['state']) ))
                    for qstatq in qstatq_lod)

                document = Document(worker_nodes, jobs_dict, queues_dict, total_running_jobs, total_queued_jobs)

                if options.EXPORT or options.WEB:
                    json_file = tempfile.NamedTemporaryFile(delete=False,
                                                            prefix='qtop_json_%s_' % timestr,
                                                            suffix='.json',
                                                            dir=savepath)
                    document.saveas(json_file)
                if options.WEB:
                    web.set_filename(json_file)

                ###### Display data ###############
                #
                display.display_selected_sections(savepath, QTOP_LOGFILE, document, wns_occupancy, cluster)

                sys.stdout.flush()
                sys.stdout.close()
                sys.stdout = stdout  # sys.stdout is back to its normal function (i.e. prints to screen)

                display.set_max_line_height(output_fp)

                if options.ONLYSAVETOFILE:  # no display of qtop output, will exit
                    break
                elif not options.WATCH:  # one-off display of qtop output, will exit afterwards (no --watch cmdline switch)
                    cat_command = 'cat %s' % output_fp  # not clearing the screen beforehand is the intended behaviour here
                    _ = subprocess.call(cat_command, stdout=stdout, stderr=stdout, shell=True)
                    break
                else:  # --watch
                    if options.REPLAY:
                        try:
                            output_partview_fp = next(useful_frames)
                        except StopIteration:
                            logging.critical('No (more) recorded instances available to show! Exiting...')
                            break
                    else:
                        output_file = dynamic_config.get('output_fp', output_fp)
                        output_partview_fp = display.show_part_view(timestr,
                                                                    file=output_file,
                                                                    x=display.viewport.v_start,
                                                                    y=display.viewport.v_term_size)

                    cat_command = 'clear;cat %s' % output_partview_fp
                    _ = subprocess.call(cat_command, stdout=stdout, stderr=stdout, shell=True)

                    read_char = wait_for_keypress_or_autorefresh(display, FALLBACK_TERMSIZE,
                                                                 int(options.WATCH[0]) or KEYPRESS_TIMEOUT)
                    control_qtop(display, read_char, cluster, conf)

                display.screens.pop()
                os.chdir(QTOPPATH)
                os.unlink(output_fp)
                fileutils.deprecate_old_output_files(config)

            if options.SAMPLE:
                sample.add_to_sample([output_fp])

        except (KeyboardInterrupt, EOFError) as e:
            repr(e)
            fileutils.exit_safe(handle, output_fp, conf, sample)
        finally:
            print "\nLog file created in %s" % os.path.expandvars(QTOP_LOGFILE)
            if options.SAMPLE:
                print "Sample files saved in %s/%s" % (config['savepath'], sample.SAMPLE_FILENAME)
                sample.handle_sample(scheduler.scheduler_output_filenames, QTOP_LOGFILE, options)
