#!/usr/bin/env python

################################################
#                   qtop                       #
#     Licensed under MIT license               #
#                     Sotiris Fragkiskos       #
#                     Fotis Georgatos          #
################################################
import sys
here = sys.path[0]

from operator import itemgetter
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
from qtop_py.constants import (SYSTEMCONFDIR, QTOPCONF_YAML, QTOP_LOGFILE, USERPATH, MAX_CORE_ALLOWED,
    MAX_UNIX_ACCOUNTS, KEYPRESS_TIMEOUT, FALLBACK_TERMSIZE)
from qtop_py import fileutils
from qtop_py import utils
from qtop_py.plugins import *
from math import ceil
from qtop_py.colormap import user_to_color_default, color_to_code, queue_to_color, nodestate_to_color_default
import qtop_py.yaml_parser as yaml
from qtop_py.ui.viewport import Viewport
from qtop_py.serialiser import GenericBatchSystem
from qtop_py.web import Web
from qtop_py import __version__
import time


# TODO make the following work with py files instead of qtop.colormap files
# if not options.COLORFILE:
#     options.COLORFILE = os.path.expandvars('$HOME/qtop/qtop/qtop.colormap')

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


def gauge_core_vectors(core_user_map, print_char_start, print_char_stop, coreline_notthere_or_unused, non_existent_symbol,
                          remove_corelines):
    """
    generator that loops over each core user vector and yields a boolean stating whether the core vector can be omitted via
    REM_EMPTY_CORELINES or its respective switch
    """
    delta = print_char_stop - print_char_start
    for ind, k in enumerate(core_user_map):
        core_x_vector = core_user_map['Core' + str(ind) + 'vector'][print_char_start:print_char_stop]
        core_x_str = ''.join(str(x) for x in core_x_vector)
        yield core_x_vector, ind, k, coreline_notthere_or_unused(non_existent_symbol, remove_corelines, delta, core_x_str)


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
        else:
            yield


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
        user_to_color = user_to_color_default.copy()
        [user_to_color.update(d) for d in config['user_color_mappings']]
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

    _savepath = os.path.realpath(os.path.expandvars(config['savepath']))

    if not os.path.exists(_savepath):
        fileutils.mkdir_p(_savepath)
        logging.debug('Directory %s created.' % _savepath)
    else:
        logging.debug('%s files will be saved in directory %s.' % (config['scheduler'], _savepath))
    config['savepath'] = _savepath

    for key in ('transpose_wn_matrices',
                'fill_with_user_firstletter',
                'faster_xml_parsing',
                'vertical_separator_every_X_columns',
                'overwrite_sample_file'):
        config[key] = eval(config[key])  # TODO config should not be writeable!!
    config['sorting']['reverse'] = eval(config['sorting'].get('reverse', "0"))  # TODO config should not be writeable!!
    config['ALT_LABEL_COLORS'] = yaml.fix_config_list(config['workernodes_matrix'][0]['wn id lines']['alt_label_colors'])
    config['SEPARATOR'] = config['vertical_separator'].translate(None, "'")
    config['USER_CUT_MATRIX_WIDTH'] = int(config['workernodes_matrix'][0]['wn id lines']['user_cut_matrix_width'])
    return config, user_to_color, nodestate_to_color


def calculate_term_size(config, FALLBACK_TERM_SIZE):
    """
    Gets the dimensions of the terminal window where qtop will be displayed.
    """
    fallback_term_size = config.get('term_size', FALLBACK_TERM_SIZE)

    _command = subprocess.Popen('stty size', stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    tty_size, error = _command.communicate()
    if not error:
        term_height, term_columns = [int(x) for x in tty_size.strip().split()]
        logging.debug('terminal size v, h from "stty size": %s, %s' % (term_height, term_columns))
    else:
        logging.warn("Failed to autodetect terminal size. (Running in an IDE?in a pipe?) Trying values in %s." % QTOPCONF_YAML)
        try:
            term_height, term_columns = viewport.get_term_size()
            if not all(term_height, term_columns):
                raise ValueError
        except ValueError:
            try:
                term_height, term_columns = yaml.fix_config_list(viewport.get_term_size())
            except KeyError:
                term_height, term_columns = fallback_term_size
                logging.debug('(hardcoded) fallback terminal size v, h:%s, %s' % (term_height, term_columns))
            else:
                logging.debug('fallback terminal size v, h:%s, %s' % (term_height, term_columns))
        except (KeyError, TypeError):  # TypeError if None was returned i.e. no setting in QTOPCONF_YAML
            term_height, term_columns = fallback_term_size
            logging.debug('(hardcoded) fallback terminal size v, h:%s, %s' % (term_height, term_columns))

    return int(term_height), int(term_columns)


def finalize_filepaths_schedulercommands(options, config):
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


def get_input_filenames(INPUT_FNs_commands, config):
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


def control_qtop(viewport, read_char, cluster, new_attrs):
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
        logging.info('max_line_len: %s' % max_line_len)
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
        new_mapping, msg = change_mapping.next()
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

        new_attrs[3] = new_attrs[3] & ~(termios.ECHO | termios.ICANON)
        termios.tcsetattr(sys.__stdin__.fileno(), termios.TCSADRAIN, old_attrs)

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

        termios.tcsetattr(sys.__stdin__.fileno(), termios.TCSADRAIN, new_attrs)
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
        new_attrs[3] = new_attrs[3] & ~(termios.ECHO | termios.ICANON)
        termios.tcsetattr(sys.__stdin__.fileno(), termios.TCSADRAIN, old_attrs)

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

        termios.tcsetattr(sys.__stdin__.fileno(), termios.TCSADRAIN, new_attrs)
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
        new_attrs[3] = new_attrs[3] & ~(termios.ECHO | termios.ICANON)
        termios.tcsetattr(sys.__stdin__.fileno(), termios.TCSADRAIN, old_attrs)

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

        termios.tcsetattr(sys.__stdin__.fileno(), termios.TCSADRAIN, new_attrs)
        viewport.reset_display()

    elif pressed_char_hex in ['3f']:  # ?
        viewport.reset_display()
        print '%s opening help...' % colorize('***', 'Green_L')
        if not h_counter.next() % 2:  # enter helpfile
            dynamic_config['output_fp'] = help_main_switch[0]
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


def fetch_scheduler_files(options, config):
    INPUT_FNs_commands = finalize_filepaths_schedulercommands(options, config)
    scheduler_output_filenames = get_input_filenames(INPUT_FNs_commands, config)
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


def get_output_size(max_line_len, output_fp, max_height=0):
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

    logging.debug('Total nr of lines: %s' % max_height)
    logging.debug('Max line length: %s' % max_line_len)

    return max_height, max_line_len


def update_config_with_cmdline_vars(options, config):
    config['rem_empty_corelines'] = int(config['rem_empty_corelines'])
    for opt in options.OPTION:
        key, val = get_key_val_from_option_string(opt)
        val = eval(val) if ('True' in val or 'False' in val) else val
        config[key] = val

    if options.TRANSPOSE:
        config['transpose_wn_matrices'] = not config['transpose_wn_matrices']

    if options.REM_EMPTY_CORELINES:
        config['rem_empty_corelines'] += options.REM_EMPTY_CORELINES

    return config


def attempt_faster_xml_parsing(config):
    if config['faster_xml_parsing']:
        try:
            from lxml import etree
        except ImportError:
            logging.warn('Module lxml is missing. Try issuing "pip install lxml". Reverting to xml module.')
            from xml.etree import ElementTree as etree


def init_dirs(options, _savepath):
    options.SOURCEDIR = realpath(options.SOURCEDIR) if options.SOURCEDIR else None
    logging.debug("User-defined source directory: %s" % options.SOURCEDIR)
    options.workdir = options.SOURCEDIR or _savepath
    logging.debug('Working directory is now: %s' % options.workdir)
    os.chdir(options.workdir)
    return options


def wait_for_keypress_or_autorefresh(viewport, FALLBACK_TERMSIZE, KEYPRESS_TIMEOUT=1):
    """
    This will make qtop wait for user input for a while,
    otherwise it will auto-refresh the display
    """
    _read_char = 'R'  # initial value, resets view position to beginning

    while sys.stdin in select.select([sys.stdin], [], [], KEYPRESS_TIMEOUT)[0]:
        _read_char = sys.stdin.read(1)
        if _read_char:
            logging.debug('Pressed %s' % _read_char)
            break
    else:
        state = viewport.get_term_size()
        viewport.set_term_size(*calculate_term_size(config, FALLBACK_TERMSIZE))
        new_state = viewport.get_term_size()
        _read_char = '\n' if (state == new_state) else 'r'
        logging.debug("Auto-advancing by pressing <Enter>")

    return _read_char


def assign_color_to_each_qname(worker_nodes):
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
    def __init__(self, cluster, config, document, user_to_color, job_ids):
        self.cluster = cluster
        self.config = config
        self.document = document
        self.account_jobs_table = list()
        self.user_to_id = dict()
        self.jobid_to_user_to_queue = dict()
        self.user_names, self.job_states, self.job_queues = self._get_usernames_states_queues(document.jobs_dict)
        self.job_ids = job_ids

        self.calculate(document, user_to_color)

    def _get_usernames_states_queues(self, jobs_dict):
        user_names, job_states, job_queues = list(), list(), list()
        for key, value in jobs_dict.items():
            user_names.append(value.user_name)
            job_states.append(value.job_state)
            job_queues.append(value.job_queue)
        return user_names, job_states, job_queues

    def calculate(self, document, user_to_color):
        """
        Prints the Worker Nodes Occupancy table.
        if there are non-uniform WNs in pbsnodes.yaml, e.g. wn01, wn02, gn01, gn02, ...,  remapping is performed.
        Otherwise, for uniform WNs, i.e. all using the same numbering scheme, wn01, wn02, ... proceeds as normal.
        Number of Extra tables needed is calculated inside the calc_all_wnid_label_lines function below
        """
        if not self.cluster:
            return self  # TODO fix
        # document.jobs_dict => job_id: job name/state/queue

        self.jobid_to_user_to_queue = dict(izip(self.job_ids, izip(user_names, job_queues)))
        self.user_machine_use = self.calculate_user_node_use(cluster, self.jobid_to_user_to_queue, self.job_ids, user_names,
                                                             job_queues)

        user_alljobs_sorted_lot = self._produce_user_lot(self.user_names)
        user_to_id = self._create_id_for_users(user_alljobs_sorted_lot)
        user_job_per_state_counts = self._calculate_user_job_counts(self.user_names, self.job_states, user_alljobs_sorted_lot,
                                                                    user_to_id)
        _account_jobs_table = self._create_sort_acct_jobs_table(user_job_per_state_counts, user_alljobs_sorted_lot, user_to_id)
        self.account_jobs_table, self.user_to_id = self._create_account_jobs_table(user_to_id, _account_jobs_table)
        self.userid_to_userid_re_pat = self.make_pattern_out_of_mapping(mapping=user_to_color)

        # TODO extract to another class?
        self.print_char_start, self.print_char_stop, self.extra_matrices_nr = self.find_matrices_width()
        self.wn_vert_labels = self.calc_all_wnid_label_lines(dynamic_config['force_names'])

        # For-loop below only for user-inserted/customizeable values.
        for yaml_key, part_name, systems in yaml.get_yaml_key_part(config, scheduler, outermost_key='workernodes_matrix'):
            if scheduler in systems:
                self.__setattr__(part_name, self.calc_general_mult_attr_line(part_name, yaml_key, config))

        self.core_user_map = self._calc_core_matrix(self.user_to_id, self.jobid_to_user_to_queue)

    def _create_account_jobs_table(self, user_to_id, account_jobs_table):
        # TODO: unix account id needs to be recomputed at this point. fix.
        for quintuplet, new_uid in zip(account_jobs_table, config['possible_ids']):
            unix_account = quintuplet[4]
            quintuplet[0] = user_to_id[unix_account] = utils.ColorStr(unix_account[0], color='Red_L') \
                if config['fill_with_user_firstletter'] else utils.ColorStr(new_uid, color='Red_L')

        return account_jobs_table, user_to_id

    def _create_sort_acct_jobs_table(self, user_job_per_state_counts, user_all_jobs_sorted, user_to_id):
        """Calculates what is actually below the id|  jobs>=R + Q | unix account etc line"""
        account_jobs_table = []
        for user_alljobs in user_all_jobs_sorted:
            user, alljobs_of_user = user_alljobs
            account_jobs_table.append(
                [
                    user_to_id[user],
                    user_job_per_state_counts['running_of_user'][user],
                    user_job_per_state_counts['queued_of_user'][user],
                    alljobs_of_user,
                    user,
                    self.user_machine_use[user]
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
                user_to_id[user_allcount[0]] = utils.ColorStr(user_allcount[0][0])
            else:
                user_to_id[user_allcount[0]] = utils.ColorStr(self.config['possible_ids'][id_])

        return user_to_id

    def make_pattern_out_of_mapping(self, mapping):
        """
        First strips the numbers off of the unix accounts and tries to match this against the given color table in colormap.
        Additionally, it will try to apply the regex rules given by the user in qtopconf.yaml, overriding the colormap.
        The first matched regex holds (loops from bottom to top in the pattern list).
        If no matching was possible, there will be no coloring applied.
        """
        pattern = {}
        for line in self.account_jobs_table:
            uid, user = line[0], line[4]
            account_letters = re.search('[A-Za-z]+', user).group(0)
            for re_account in mapping.keys()[::-1]:
                match = re.search(re_account, user)
                if match is not None:
                    account_letters = re_account  # colors the text according to the regex given by the user in qtopconf
                    break

            pattern[str(uid)] = account_letters if account_letters in mapping else 'NoPattern'

        # TODO: remove these from here
        pattern[self.config['non_existent_node_symbol']] = '#'
        pattern['_'] = '_'
        pattern[self.config['SEPARATOR']] = 'account_not_colored'
        return pattern

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
        if NAMED_WNS:  #  or options.FORCE_NAMES
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
                    elif ch == '?':
                        ch = utils.ColorStr('?', color="Gray_D")
                    multiline_map[attr_line].append(ch)
                except KeyError:
                    break
                    # TODO: is this really needed?: self.cluster.workernode_dict[_node]['state_column']

        for line, attr_line in enumerate(multiline_map, 1):
            if line == user_max_len:
                break
        return multiline_map

    def _calc_core_matrix(self, user_to_id, jobid_to_user_to_queue):
        core_user_map = OrderedDict()

        core_coloring = dynamic_config.get('core_coloring', self.config['core_coloring'])

        for core_nr in self.cluster.core_span:
            core_user_map['Core%svector' % str(core_nr)] = []  # Cpu0vector, Cpu1vector, Cpu2vector, ... = [],[],[], ...

        for _node in self.cluster.workernode_dict:
            core_user_map = self._fill_node_cores_vector(_node, core_user_map, user_to_id, self.cluster.core_span,
                                                         jobid_to_user_to_queue, core_coloring)

        return core_user_map

    def _fill_node_cores_vector(self, _node, core_user_map, user_to_id, _core_span, jobid_to_user_to_queue, core_coloring):
        """
        Calculates the actual contents of the map by filling in a status string for each CPU line
        One of the two dimensions of the matrix is determined by the highest-core WN existing. If other WNs have less cores,
        these positions are filled with '#'s (or whatever is defined in config['non_existent_node_symbol']).
        """
        state_np_corejob = cluster.workernode_dict[_node]
        state = state_np_corejob['state']
        np = state_np_corejob['np']
        corejobs = state_np_corejob.get('core_job_map', dict())
        non_existent_node_symbol = config['non_existent_node_symbol']
        gray_hash = utils.ColorStr(non_existent_node_symbol, color='Gray_D')
        gray_underscore = utils.ColorStr('_', color='Gray_D')

        if state == '?':  # for non-existent machines
            for core_line in core_user_map:
                core_user_map[core_line].append(gray_hash)
        else:
            node_cores = [str(x) for x in range(int(np))]
            core_user_map, node_free_cores = self.color_cores_and_return_unused(node_cores, core_user_map, corejobs,
                                                                                core_coloring, jobid_to_user_to_queue)
            for core in node_free_cores:
                core_user_map['Core' + str(core) + 'vector'].append(gray_underscore)

            non_existent_node_cores = [core for core in _core_span if core not in node_cores]
            for core in non_existent_node_cores:
                core_user_map['Core' + str(core) + 'vector'].append(gray_hash)

        return core_user_map

    @staticmethod
    def get_hl_q_or_users(_highlighted_queues_or_users):
        for selection_users_queues in _highlighted_queues_or_users:
            selection = selection_users_queues.keys()[0]
            users_queues = selection_users_queues[selection]
            and_or, type = selection.rsplit('include_')
            and_or_func = all if not and_or else any

            for user_queue in users_queues:
                yield user_queue, type, and_or_func

    def color_cores_and_return_unused(self, node_cores, core_user_map, corejobs, _core_coloring, jobid_to_user_to_queue):
        """
        Adds color information to the core job, returns free cores.
        locals()[queue_or_user] transforms either 'user'=> user or 'queue'=> queue
        depending on qtopconf yaml's "core_coloring",
        or on runtime in watch mode, if user presses appropriate keybinding
        """
        node_free_cores = node_cores[:]
        queue_or_user_map = {'user_to_color': 'user_pat', 'queue_to_color': 'queue'}
        queue_or_user_str = queue_or_user_map[_core_coloring]

        selected_pat_to_color_map = globals()[_core_coloring]
        _highlighted_queues_or_users = dynamic_config.get('highlight', self.config['highlight'])

        self.id_to_user = dict(izip((str(x) for x in self.user_to_id.itervalues()), self.user_to_id.iterkeys()))
        for (user, core, queue) in self._valid_corejobs(corejobs, jobid_to_user_to_queue):
            id_ = utils.ColorStr.from_other_color_str(self.user_to_id[user])
            user_pat = self.userid_to_userid_re_pat[str(id_)]  # in case it is used in viewed_pattern
            viewed_pattern = locals()[queue_or_user_str]  # either a queue or userid pattern
            matches = []
            and_or_func = any

            for user_queue_to_highlight, type, and_or_func in WNOccupancy.get_hl_q_or_users(_highlighted_queues_or_users):
                if (type == 'user_pat'):
                    actual_user_queue = user
                elif (type == 'user_id'):
                    actual_user_queue = user
                    user_queue_to_highlight = self.id_to_user[user_queue_to_highlight]
                elif type == 'queue':
                    actual_user_queue = queue

                matches.append(re.match(user_queue_to_highlight, actual_user_queue))

            if not _highlighted_queues_or_users or and_or_func(match.group(0) if match is not None else None for match in matches):
                id_.color = selected_pat_to_color_map.get(viewed_pattern, 'White')  # queue or user decided on runtime
            else:
                id_.color = 'Gray_D'

            id_.q = queue
            core_user_map['Core' + str(core) + 'vector'].append(id_)
            node_free_cores.remove(core)  # this is an assigned core, hence it doesn't belong to the node's free cores

        return core_user_map, node_free_cores

    def _valid_corejobs(self, corejobs, jobid_to_user_to_queue):
        """
        Generator that yields only those core-job pairs that successfully match to a user
        """
        for core, _job in corejobs.items():
            job = re.sub(r'\[\d+\]', '[]', str(_job))  # also takes care of job arrays
            try:
                user_queue = jobid_to_user_to_queue[job]
            except KeyError as KeyErrorValue:
                logging.critical('There seems to be a problem with the qstat output. '
                                 'A Job (ID %s) has gone rogue. '
                                 'Please check with the SysAdmin.' % (str(KeyErrorValue)))
                raise KeyError
            else:
                user, queue = user_queue
                yield user, str(core), queue

    def is_matrix_coreless(self, print_char_start, print_char_stop):
        # print_char_start = self.print_char_start
        # print_char_stop = self.print_char_stop
        non_existent_symbol = self.config['non_existent_node_symbol']
        lines = 0
        core_user_map = self.core_user_map
        remove_corelines = dynamic_config.get('rem_empty_corelines', config['rem_empty_corelines']) + 1

        for core_x_vector, ind, k, is_corevector_removable in gauge_core_vectors(core_user_map,
                                                                                 print_char_start,
                                                                                 print_char_stop,
                                                                                 WNOccupancy.coreline_notthere_or_unused,
                                                                                 non_existent_symbol,
                                                                                 remove_corelines):
            if is_corevector_removable:
                lines += 1
        return lines == len(core_user_map)

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

    def calculate_user_node_use(self, cluster, jobid_to_user_to_queue, job_ids, user_names, job_queues):
        """
        This calculates the number of nodes each user has jobs in (shown in User accounts and pool mappings)
        """
        user_machines = []
        jobid_to_user_to_queue = dict(izip(job_ids, izip(user_names, job_queues)))
        # TODO why use variables from outer scope above?
        for node in cluster.workernode_dict:
            cluster.workernode_dict[node]['node_user_set'] = set([jobid_to_user_to_queue[job][0] for job in
                                                                  cluster.workernode_dict[node]['node_job_set']])
            user_machines.extend(list(cluster.workernode_dict[node]['node_user_set']))

        return Counter(user_machines)

    @staticmethod
    def coreline_not_there(symbol, switch, delta, core_x_str):
        """
        Checks if a line consists of only not-really-there cores, i.e. core 32 on a line of 24-core machines
        (being there because there are other machines with 32 cores on an adjacent matrix)
        """
        return switch == 2 and ((symbol * delta == core_x_str) or (symbol * len(core_x_str) == core_x_str))

    @staticmethod
    def coreline_unused(symbol, switch, print_length, core_x_str):
        """
        Checks if a line consists of either not-really-there cores or unused cores
        """
        all_symbols_in_line = set(core_x_str)
        unused_symbols = set(['_', symbol])
        only_unused_symbols_in_line = all_symbols_in_line.issubset(unused_symbols)
        return switch == 3 and only_unused_symbols_in_line and (print_length == len(core_x_str))

    @staticmethod
    def coreline_notthere_or_unused(symbol, switch, delta, core_x_str):
        return WNOccupancy.coreline_not_there(symbol, switch, delta, core_x_str) \
                or WNOccupancy.coreline_unused(symbol, switch, delta, core_x_str)


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

    def display_selected_sections(self, _savepath, SAMPLE_FILENAME, QTOP_LOGFILE):
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

        if options.WATCH:
            print "\033c",  # comma is to avoid losing the whole first line. An empty char still remains, though.

        for idx, part in enumerate(config['user_display_parts'], 1):
            display_func, args = display_parts[part][0], display_parts[part][1]
            display_func(*args) if not sections_off[idx] else None

        print "\nLog file created in %s" % os.path.expandvars(QTOP_LOGFILE)
        if options.SAMPLE:
            print "Sample files saved in %s/%s" % (_savepath, SAMPLE_FILENAME)
        if options.STRICTCHECK:
            WNOccupancy.strict_check_jobs(wns_occupancy, cluster)

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

    def display_user_accounts_pool_mappings(self, wns_occupancy):
        """
        Displays qtop's third section
        """
        try:
            account_jobs_table = self.wns_occupancy.account_jobs_table
            userid_to_userid_re_pat = self.wns_occupancy.userid_to_userid_re_pat
        except KeyError:
            account_jobs_table = dict()
            userid_to_userid_re_pat = dict()

        detail_of_name = get_detail_of_name(account_jobs_table)
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

            if (options.COLOR == 'OFF' or userid_pat == 'account_not_colored' or user_to_color[userid_pat] == 'reset'):
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
                    (core_user_map, print_char_start, print_char_stop, transposed_matrices, userid_to_userid_re_pat, mapping),
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
                        {'attr_lines': getattr(wns_occupancy, part_name), 'coloring': queue_to_color}  # kwargs
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

        if dynamic_config.get('transpose_wn_matrices', config['transpose_wn_matrices']):
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
        s = "".join([colorize(char.initial, color_func=char.color) if isinstance(char, utils.ColorStr) else char
                     for char in joined_list[self.viewport.h_start:self.viewport.h_stop]])
        print compress_colored_line(s)
        return joined_list

    def print_core_lines(self, core_user_map, print_char_start, print_char_stop, transposed_matrices,
                         userid_to_userid_re_pat, mapping, attrs, options1, options2):

        signal(SIGPIPE, SIG_DFL)
        remove_corelines = dynamic_config.get('rem_empty_corelines', config['rem_empty_corelines']) + 1

        # if corelines vertical (transposed matrix)
        if dynamic_config.get('transpose_wn_matrices', config['transpose_wn_matrices']):
            non_existent_symbol = config['non_existent_node_symbol']
            for core_x_vector, ind, k, is_corevector_removable in gauge_core_vectors(core_user_map,
                                                                                    print_char_start,
                                                                                    print_char_stop,
                                                                                    WNOccupancy.coreline_notthere_or_unused,
                                                                                    non_existent_symbol,
                                                                                    remove_corelines):
                if is_corevector_removable:
                    del core_user_map[k]

            tuple_ = [None, 'core_map', self.transpose_matrix(core_user_map, colored=False, coloring_pat=userid_to_userid_re_pat)]
            transposed_matrices.append(tuple_)
            return
        else:
            # if corelines horizontal (non-transposed matrix)
            for core_line in self.get_core_lines(core_user_map, print_char_start, print_char_stop,
                                                 userid_to_userid_re_pat, mapping, attrs):
                core_line_zipped = compress_colored_line(core_line)
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
            self.print_wnid_lines(d, start, stop, end_labels_iter, transposed_matrices,
                                  color_func=self.color_plainly, args=('White', 'Gray_L', start > 0))
            # start > 0 is just a test for a possible future condition

        elif dynamic_config['force_names']:  # the actual names of the WNs instead of numbered WNs [was: or options.FORCE_NAMES]
            node_str_width = len(wn_vert_labels)  # key, nr of horizontal lines to be displayed

            # for longer full-labeled wn ids, add more end-labels (far-right) towards the bottom
            for num in range(8, len(wn_vert_labels) + 1):
                end_labels.setdefault(str(num), end_labels['7'] + num * ['={________}'])

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
        if dynamic_config.get('transpose_wn_matrices', self.config['transpose_wn_matrices']):
            tuple_ = [None, 'wnid_lines', self.transpose_matrix(d)]
            transposed_matrices.append(tuple_)
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
        return temp_f.name

    def print_mult_attr_line(self, print_char_start, print_char_stop, transposed_matrices, attr_lines, label, color_func=None,
                             **kwargs):
        """
        attr_lines can be e.g. Node state lines
        """
        if dynamic_config.get('transpose_wn_matrices', config['transpose_wn_matrices']):
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

    def get_core_lines(self, core_user_map, print_char_start, print_char_stop, coloring_pattern, mapping, attrs):
        """
        yields all coreX lines, except cores that don't show up
        anywhere in the given matrix
        """
        non_existent_symbol = config['non_existent_node_symbol']
        remove_corelines = dynamic_config.get('rem_empty_corelines', config['rem_empty_corelines']) + 1
        for core_x_vector, ind, k, is_corevector_removable in gauge_core_vectors(core_user_map,
                                                                                 print_char_start,
                                                                                 print_char_stop,
                                                                                 WNOccupancy.coreline_notthere_or_unused,
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

        self.available_wn = sum([len(node['state']) for node in self.worker_nodes if str(node['state'][0]) == '-'])
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

        re_nodename = r'(^[A-Za-z0-9-]+)(?=\.|$)' if not self.options.ANONYMIZE else r'\w_anon_wn_\d+'

        self.node_subclusters, self.workernode_list, self.offdown_nodes, self.working_cores, max_np, \
            _all_str_digits_with_empties = self.get_wn_list_and_stats(self.workernode_list,
                                                                          self.node_subclusters,
                                                                          self.worker_nodes,
                                                                          re_nodename)

        self.core_span = [str(x) for x in range(max_np)]
        self.options.REMAP = self.decide_remapping(_all_str_digits_with_empties)

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
        self._calculate_jobs_per_node(self.workernode_dict)
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

    def map_worker_nodes_to_wn_dict(self, options_remap):
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
        user_sorting = dynamic_config.get('user_sort', (_sorting_from_conf and _sorting_from_conf.values()[0]))
        user_filters = dynamic_config.get('filtering', self.config['filtering'])
        user_filtering = user_filters and user_filters[0]

        if user_filtering and options_remap:
            len_wn_before = len(self.worker_nodes)
            self.wn_filter = self.WNFilter(self.worker_nodes)
            self.worker_nodes, self.offdown_nodes, self.available_wn, self.working_cores, self.total_cores = \
                self.wn_filter.filter_worker_nodes(self.offdown_nodes,
                                                   self.available_wn,
                                                   self.working_cores,
                                                   self.total_cores,
                                                   filter_rules=user_filters)
            len_wn_after = len(self.worker_nodes)
            nodes_drop = len_wn_after - len_wn_before

        if user_sorting:
            self.worker_nodes = self._sort_worker_nodes()

        for (batch_node, (idx, cur_node_nr)) in zip(self.worker_nodes, enumerate(self.workernode_list)):
            # Seemingly there is an error in the for loop because self.worker_nodes and workernode_list
            # have different lengths if there's a filter in place, but it is OK, since
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
        mapping = user_to_color
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


def pick_frames_to_replay(_savepath):
    """
    getting the respective info from cmdline switch -R,
    pick the relevant qtop output from savepath to replay
    """
    if options.REPLAY[0] == 0:  # add default arg, if no replay start time is set in the cmdline
        time_delta = fileutils.get_timedelta(fileutils.parse_time_input(config['replay_last']))
        some_time_ago = datetime.datetime.now() - time_delta
        options.REPLAY[0] = some_time_ago.strftime("%Y%m%dT%H%M%S")
    if len(options.REPLAY) == 1:  # add default arg, if no replay duration is set in the cmdline
        options.REPLAY.append('2m')

    time_delta = fileutils.get_timedelta(fileutils.parse_time_input(options.REPLAY[1]))
    watch_start_datetime_obj = get_date_obj_from_str(options.REPLAY[0], datetime.datetime.now())
    REC_FP_ALL = _savepath + '/*_partview*.out'
    rec_files = glob.iglob(REC_FP_ALL)
    useful_frames = []

    for rec_file in rec_files:
        rec_file_last_modified_date = datetime.datetime.strptime(rec_file.rsplit('/',1)[-1].split('_')[2], "%Y%m%dT%H%M%S")
        if datetime.timedelta(seconds=0) < rec_file_last_modified_date - watch_start_datetime_obj < time_delta:
            useful_frames.append(rec_file)

    useful_frames = iter(useful_frames[::-1])
    return useful_frames, options.REPLAY



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

    def filter_worker_nodes(self, offdown_nodes, avail_nodes, working_cores, total_cores, filter_rules=None):
        """
        Keeps specific nodes according to the filter rules in QTOPCONF_YAML
        """
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
                offdown_nodes = sum([1 if "".join(([n.str for n in node['state']])) in 'do'  else 0 for node in
                                     self.worker_nodes])
                avail_nodes = self.available_wn = sum(
                    [len(node['state']) for node in self.worker_nodes if str(node['state'][0]) == '-'])
                working_cores = sum(len(node.get('core_job_map', dict())) for node in self.worker_nodes)
                total_cores = sum(int(node.get('np')) for node in self.worker_nodes)
            else:
                logging.error(colorize('Selected filter results in empty worker node set. Cancelling.', 'Red_L'))

        return self.worker_nodes, offdown_nodes, avail_nodes, working_cores, total_cores

    @staticmethod
    @utils.CountCalls
    def report_filtered_view():
        logging.error("%s WN Occupancy view is filtered." % colorize('***', 'Green_L'))

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
    if options.version:
        print 'qtop current version: ' + __version__
        sys.exit(0)
    utils.init_logging(options)
    dynamic_config = dict()
    options, dynamic_config['force_names'] = process_options(options)
    if options.ANONYMIZE and not options.EXPERIMENTAL:
        print 'Anonymize should be ran with --experimental switch!! Exiting...'
        sys.exit(1)
    if options.WATCH or options.REPLAY:  # this is needed for the filtering/sorting options
        try:
            old_attrs = termios.tcgetattr(0)
        except termios.error:
            old_attrs = ''
        new_attrs = old_attrs[:]

    available_batch_systems = discover_qtop_batch_systems()

    stdout = sys.stdout  # keep a copy of the initial value of sys.stdout
    change_mapping = cycle([('queue_to_color', 'color by queue'), ('user_to_color', 'color by user')])
    h_counter = cycle([0, 1])

    viewport = Viewport()  # controls the part of the qtop matrix shown on screen
    max_line_len = 0


    check_python_version()
    initial_cwd = os.getcwd()
    logging.debug('Initial qtop directory: %s' % initial_cwd)
    CURPATH = os.path.expanduser(initial_cwd)  # where qtop was invoked from
    QTOPPATH = os.path.dirname(realpath(sys.argv[0]))  # dir where qtop resides
    HELP_FP = os.path.join(QTOPPATH, 'helpfile.txt')
    help_main_switch = [HELP_FP, ]  # output_fp is not yet defined, will be appended later
    SAMPLE_FILENAME = 'qtop_sample_${USER}%(datetime)s.tar'
    SAMPLE_FILENAME = os.path.expandvars(SAMPLE_FILENAME)
    if options.REPLAY:
        options.WATCH = [0]  # enforce that --watch mode is on, even if not in cmdline switch
        options.BATCH_SYSTEM = 'demo'
        config, _, _ = load_yaml_config()
        useful_frames, options.REPLAY = pick_frames_to_replay(config['savepath'])

    web = Web(initial_cwd)
    if options.WEB:
        web.start()

    with raw_mode(sys.stdin):  # key listener implementation
        try:
            while True:
                config, user_to_color, nodestate_to_color = load_yaml_config()  # TODO account_to_color is updated here !!
                config = update_config_with_cmdline_vars(options, config)
                savepath = config['savepath']
                timestr = time.strftime("%Y%m%dT%H%M%S")
                # qtop output is saved here
                handle, output_fp = fileutils.get_new_temp_file(savepath, prefix='qtop_fullview_%s_' % timestr, suffix='.out')
                help_main_switch.append(output_fp)

                attempt_faster_xml_parsing(config)
                options = init_dirs(options, savepath)

                transposed_matrices = []
                viewport.set_term_size(*calculate_term_size(config, FALLBACK_TERMSIZE))
                sys.stdout = os.fdopen(handle, 'w')  # redirect everything to file, creates file object out of handle
                scheduler = decide_batch_system(
                    options.BATCH_SYSTEM, os.environ.get('QTOP_SCHEDULER'), config['scheduler'],
                    config['schedulers'], available_batch_systems, config)
                scheduler_output_filenames = fetch_scheduler_files(options, config)
                SAMPLE_FILENAME = fileutils.get_sample_filename(SAMPLE_FILENAME, config)
                if options.SAMPLE:
                    fileutils.tar_out = fileutils.init_sample_file(options, savepath, SAMPLE_FILENAME,
                                                                   scheduler_output_filenames, QTOPCONF_YAML, QTOPPATH)

                ###### Gather data ###############
                #
                scheduling_system = available_batch_systems[scheduler](scheduler_output_filenames, config, options)

                job_ids, user_names, job_states, job_queues = scheduling_system.get_jobs_info()
                total_running_jobs, total_queued_jobs, qstatq_lod = scheduling_system.get_queues_info()
                worker_nodes = scheduling_system.get_worker_nodes(job_ids, job_queues, options)

                JobDoc = namedtuple('JobDoc', ['user_name', 'job_state', 'job_queue'])
                jobs_dict = dict((re.sub(r'\[\]$', '', job_id), JobDoc(user_name, job_state, job_queue)) for
                                 job_id, user_name, job_state, job_queue in izip(job_ids, user_names, job_states, job_queues))

                QDoc = namedtuple('QDoc', ['lm', 'queued', 'run', 'state'])
                queues_dict = OrderedDict(
                    (qstatq['queue_name'], (QDoc(str(qstatq['lm']), qstatq['queued'], qstatq['run'], qstatq['state'])))
                    for qstatq in qstatq_lod)

                document = Document(worker_nodes, jobs_dict, queues_dict, total_running_jobs, total_queued_jobs)

                ###### Export data ###############
                #
                if options.EXPORT or options.WEB:
                    json_file = tempfile.NamedTemporaryFile(delete=False, prefix='qtop_json_%s_' % timestr,
                                                            suffix='.json', dir=savepath)
                    document.save(json_file.name)
                if options.WEB:
                    web.set_filename(json_file.name)

                ###### Process data ###############
                #
                worker_nodes = keep_queue_initials_only_and_colorize(document.worker_nodes, queue_to_color)
                worker_nodes = colorize_nodestate(document.worker_nodes, nodestate_to_color, colorize)
                cluster = Cluster(document, worker_nodes, WNFilter, config, options)
                wns_occupancy = WNOccupancy(cluster, config, document, user_to_color, job_ids)

                ###### Display data ###############
                #
                display = TextDisplay(document, config, viewport, wns_occupancy, cluster)
                display.display_selected_sections(savepath, SAMPLE_FILENAME, QTOP_LOGFILE)

                sys.stdout.flush()
                sys.stdout.close()
                sys.stdout = stdout  # sys.stdout is back to its normal function (i.e. prints to screen)

                viewport.max_height, max_line_len = get_output_size(max_line_len, output_fp)

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
                        output_partview_fp = display.show_part_view(timestr,
                                                                    file=dynamic_config.get('output_fp', output_fp),
                                                                    x=viewport.v_start,
                                                                    y=viewport.v_term_size)
                        logging.debug('dynamic_config filename in main loop: %s' % dynamic_config.get('output_fp', output_fp))
                    cat_command = 'clear;cat %s' % output_partview_fp
                    _ = subprocess.call(cat_command, stdout=stdout, stderr=stdout, shell=True)

                    read_char = wait_for_keypress_or_autorefresh(viewport, FALLBACK_TERMSIZE, int(options.WATCH[0]) or
                                                                 KEYPRESS_TIMEOUT)
                    control_qtop(viewport, read_char, cluster, new_attrs)

                help_main_switch.pop()
                os.chdir(QTOPPATH)
                os.unlink(output_fp)
                fileutils.deprecate_old_output_files(config)

            if options.SAMPLE:
                fileutils.tar_out = fileutils.add_to_sample([output_fp], fileutils.tar_out)

        except (KeyboardInterrupt, EOFError) as e:
            repr(e)
            fileutils.safe_exit_with_file_close(handle, output_fp, stdout, options, savepath, QTOP_LOGFILE, SAMPLE_FILENAME)
        finally:
            if options.SAMPLE >= 1:
                fileutils.tar_out = fileutils.add_to_sample([QTOP_LOGFILE], fileutils.tar_out)
                # add all scheduler output files to sample
                for fn in scheduler_output_filenames:
                    if os.path.isfile(scheduler_output_filenames[fn]):
                        fileutils.tar_out = fileutils.add_to_sample([scheduler_output_filenames[fn]], fileutils.tar_out)
                fileutils.tar_out.close()
