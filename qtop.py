#!/usr/bin/env python

################################################
#              qtop v.0.8.4                    #
#     Licensed under MIT-GPL licenses          #
#                     Sotiris Fragkiskos       #
#                     Fotis Georgatos          #
################################################

from operator import itemgetter
import datetime
from itertools import izip, izip_longest
import subprocess
import time
import sys
import select
import os
from os import unlink, close
from os.path import realpath, expandvars, getmtime
try:
    from collections import OrderedDict
except ImportError:
    from ordereddict import OrderedDict
from signal import signal, SIGPIPE, SIG_DFL
import termios
import contextlib
# modules
from constants import *
import common_module
from common_module import logging, options, sections_off, report  #, anonymize_func
import plugin_pbs, plugin_oar, plugin_sge
from plugin_pbs import *
from plugin_oar import *
from plugin_sge import *
from math import ceil
from colormap import color_of_account, code_of_color
from yaml_parser import read_yaml_natively, fix_config_list, convert_dash_key_in_dict


@contextlib.contextmanager
def raw_mode(file):
    old_attrs = termios.tcgetattr(file.fileno())
    new_attrs = old_attrs[:]
    new_attrs[3] = new_attrs[3] & ~(termios.ECHO | termios.ICANON)
    try:
        termios.tcsetattr(file.fileno(), termios.TCSADRAIN, new_attrs)
        yield
    finally:
        termios.tcsetattr(file.fileno(), termios.TCSADRAIN, old_attrs)

# TODO make the following work with py files instead of qtop.colormap files
# if not options.COLORFILE:
#     options.COLORFILE = os.path.expandvars('$HOME/qtop/qtop/qtop.colormap')


def colorize(text, color_func=None, pattern='NoPattern', bg_color=None):
    """
    prints text colored according to a unix account pattern color.
    If color is given, pattern is not needed.
    """
    # bg_color = code_of_color['BlueBG']
    bg_color = '' if not bg_color else bg_color
    try:
        ansi_color = code_of_color[color_func] if color_func else code_of_color[color_of_account[pattern]]
    except KeyError:
        return text
    else:
        if ((options.COLOR == 'ON') and pattern != 'account_not_colored' and text != ' '):
            text = "\033[%(fg_color)s%(bg_color)sm%(text)s\033[0;m" \
                   % {'fg_color': ansi_color, 'bg_color': bg_color, 'text': text}

        return text


def decide_remapping(cluster_dict, _all_letters, _all_str_digits_with_empties):
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

    # all needed for decide_remapping
    cluster_dict['node_subclusters'] = set(_all_letters)
    cluster_dict['_all_str_digits_with_empties'] = _all_str_digits_with_empties
    cluster_dict['all_str_digits'] = filter(lambda x: x != "", _all_str_digits_with_empties)
    cluster_dict['all_digits'] = [int(digit) for digit in cluster_dict['all_str_digits']]

    if options.BLINDREMAP or \
                    len(cluster_dict['node_subclusters']) > 1 or \
                    min(cluster_dict['workernode_list']) >= config['exotic_starting_wn_nr'] or \
                    cluster_dict['offline_down_nodes'] >= cluster_dict['total_wn'] * config['percentage'] or \
                    len(cluster_dict['_all_str_digits_with_empties']) != len(cluster_dict['all_str_digits']) or \
                    len(cluster_dict['all_digits']) != len(cluster_dict['all_str_digits']):
        options.REMAP = True
    else:
        options.REMAP = False
    logging.info('Blind Remapping [user selected]: %s,'
                  '\n\t\t\t\t\t\t\t\t  Decided Remapping: %s' % (options.BLINDREMAP, options.REMAP))

    if logging.getLogger().isEnabledFor(logging.DEBUG) and options.REMAP:
        user_request = options.BLINDREMAP and 'The user has requested it (blindremap switch)' or False
        subclusters = len(cluster_dict['node_subclusters']) > 1 and \
            'there are different WN namings, e.g. wn001, wn002, ..., ps001, ps002, ... etc' or False
        exotic_starting = min(cluster_dict['workernode_list']) >= config['exotic_starting_wn_nr'] and \
            'the first starting numbering of a WN is very high and thus would require too much unused space' or False
        percentage_unassigned = len(cluster_dict['_all_str_digits_with_empties']) != len(cluster_dict['all_str_digits']) and \
            'more than %s of nodes have are down/offline' % float(config['percentage']) or False
        numbering_collisions = min(cluster_dict['workernode_list']) >= config['exotic_starting_wn_nr'] and \
            'there are numbering collisions' or False
        print
        logging.debug('Remapping decided due to: \n\t %s' % filter(None,
            [user_request, subclusters, exotic_starting, percentage_unassigned, numbering_collisions]))
    # max(cluster_dict['workernode_list']) was cluster_dict['highest_wn']


def calculate_cluster(worker_nodes):
    logging.debug('FORCE_NAMES is: %s' % options.FORCE_NAMES)
    NAMED_WNS = 0 if not options.FORCE_NAMES else 1
    cluster_dict = dict()
    for key in ['working_cores', 'total_cores', 'max_np', 'highest_wn', 'offline_down_nodes']:
        cluster_dict[key] = 0
    cluster_dict['node_subclusters'] = set()
    cluster_dict['workernode_dict'] = {}
    cluster_dict['workernode_dict_remapped'] = {}  # { remapnr: [state, np, (core0, job1), (core1, job1), ....]}

    cluster_dict['total_wn'] = len(worker_nodes)  # == existing_nodes
    cluster_dict['workernode_list'] = []
    cluster_dict['workernode_list_remapped'] = range(1, cluster_dict['total_wn'] + 1)  # leave xrange aside for now

    _all_letters = []
    _all_str_digits_with_empties = []

    re_nodename = r'(^[A-Za-z0-9-]+)(?=\.|$)' if not options.ANONYMIZE else r'\w_anon_\w+'
    for node in worker_nodes:
        nodename_match = re.search(re_nodename, node['domainname'])
        _nodename = nodename_match.group(0)

        node_letters = ''.join(re.findall(r'\D+', _nodename))
        node_str_digits = "".join(re.findall(r'\d+', _nodename))

        _all_letters.append(node_letters)
        _all_str_digits_with_empties.append(node_str_digits)

        cluster_dict['total_cores'] += int(node.get('np'))
        cluster_dict['max_np'] = max(cluster_dict['max_np'], int(node['np']))
        cluster_dict['offline_down_nodes'] += 1 if node['state'] in 'do' else 0
        try:
            cluster_dict['working_cores'] += len(node['core_job_map'])
        except KeyError:
            pass

        try:
            cur_node_nr = int(node_str_digits)
        except ValueError:
            cur_node_nr = _nodename
        finally:
            cluster_dict['workernode_list'].append(cur_node_nr)

    decide_remapping(cluster_dict, _all_letters, _all_str_digits_with_empties)

    # cluster_dict['workernode_dict'] creation
    nodes_drop = map_batch_nodes_to_wn_dicts(cluster_dict, worker_nodes, options.REMAP)

    # this amount has to be chopped off of the end of workernode_list_remapped
    nodes_drop_slice_end = None if (nodes_drop == 0) else nodes_drop
    if options.REMAP:
        cluster_dict['total_wn'] += nodes_drop
        cluster_dict['highest_wn'] = cluster_dict['total_wn']
        cluster_dict['workernode_list'] = cluster_dict['workernode_list_remapped'][:nodes_drop_slice_end]
        cluster_dict['workernode_dict'] = cluster_dict['workernode_dict_remapped']
    else:
        cluster_dict['highest_wn'] = max(cluster_dict['workernode_list'])

    # fill in non-existent WN nodes (absent from pbsnodes file) with default values and count them
    if not options.REMAP:
        for node in range(1, cluster_dict['highest_wn'] + 1):
            if node not in cluster_dict['workernode_dict']:
                cluster_dict['workernode_dict'][node] = {'state': '?', 'np': 0, 'domainname': 'N/A', 'host': 'N/A'}
                default_values_for_empty_nodes = dict([(yaml_key, '?') for yaml_key, part_name in get_yaml_key_part('workernodes_matrix')])
                cluster_dict['workernode_dict'][node].update(default_values_for_empty_nodes)

    do_name_remapping(cluster_dict)

    return cluster_dict, NAMED_WNS


def do_name_remapping(cluster_dict):
    """
    renames hostnames according to user remapping in conf file (for the wn id label lines)
    """
    label_max_len = int(config['workernodes_matrix'][0]['wn id lines']['max_len'])
    for _, state_corejob_dn in cluster_dict['workernode_dict'].items():
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
            state_corejob_dn['host'] = label_max_len and state_corejob_dn['host'][-label_max_len:] or state_corejob_dn['host']


def nodes_with_jobs(worker_nodes):
    for _, pbs_node in worker_nodes.iteritems():
        if 'core_job_map' in pbs_node:
            yield pbs_node


def display_job_accounting_summary(cluster_dict, total_running_jobs, total_queued_jobs, qstatq_list):
    if options.REMAP:
        if options.CLASSIC:
            print '=== WARNING: --- Remapping WN names and retrying heuristics... good luck with this... ---'
        else:
            logging.warning('=== WARNING: --- Remapping WN names and retrying heuristics... good luck with this... ---')
    ansi_delete_char = "\015"  # this removes the first ever character (space) appearing in the output
    print '%(del)s%(name)s report tool. All bugs added by sfranky@gmail.com. Cross fingers now...' \
        % {'name': 'PBS' if options.CLASSIC else 'Queueing System', 'del': ansi_delete_char}

    if not options.WATCH:
        print 'Please try: watch -d %s/qtop.py -s <SOURCEDIR>\n' % QTOPPATH
    print colorize('===> ', 'Gray_D') + colorize('Job accounting summary', 'White') + colorize(' <=== ', 'Gray_D') + \
          '%s WORKDIR = %s' % (colorize(str(datetime.datetime.today())[:-7], 'White'), QTOPPATH)

    print '%(Usage Totals)s:\t%(online_nodes)s/%(total_nodes)s %(Nodes)s | %(working_cores)s/%(total_cores)s %(Cores)s |' \
          '   %(total_run_jobs)s+%(total_q_jobs)s %(jobs)s (R + Q) %(reported_by)s' % \
          {
              'Usage Totals': colorize('Usage Totals', 'Yellow'),
              'online_nodes': colorize(str(cluster_dict['total_wn'] - cluster_dict['offline_down_nodes']), 'Red_L'),
              'total_nodes': colorize(str(cluster_dict['total_wn']), 'Red_L'),
              'Nodes': colorize('Nodes', 'Red_L'),
              'working_cores': colorize(str(cluster_dict['working_cores']), 'Green_L'),
              'total_cores': colorize(str(cluster_dict['total_cores']), 'Green_L'),
              'Cores': colorize('cores', 'Green_L'),
              'total_run_jobs': colorize(str(int(total_running_jobs)), 'Blue_L'),
              'total_q_jobs': colorize(str(int(total_queued_jobs)), 'Blue_L'),
              'jobs': colorize('jobs', 'Blue_L'),
              'reported_by': 'reported by qstat - q' if options.CLASSIC else ''
          }

    print '%(queues)s: | ' % {'queues': colorize('Queues', 'Yellow')},
    for q in qstatq_list:
        q_name, q_running_jobs, q_queued_jobs = q['queue_name'], q['run'], q['queued']
        account = q_name if q_name in color_of_account else 'account_not_colored'
        print "{qname}{star}: {run} {q}|".format(
            qname=colorize(q_name, '', account),
            star=colorize('*', 'Red_L') if q['state'].startswith('D') or q['state'].endswith('S') else '',
            run=colorize(q_running_jobs, '', account),
            q='+ ' + colorize(q_queued_jobs, '', account) + ' ' if q_queued_jobs != '0' else ''),
    print colorize('* implies blocked', 'Red') + '\n'
    # TODO unhardwire states from star kwarg


def calculate_job_counts(user_names, job_states):
    """
    Calculates and prints what is actually below the id|  R + Q /all | unix account etc line
    :param user_names: list
    :param job_states: list
    :return: (list, list, dict)
    """
    expand_useraccounts_symbols(config, user_names)
    state_abbrevs = config['state_abbreviations'][scheduler]

    job_counts = create_job_counts(user_names, job_states, state_abbrevs)
    user_alljobs_sorted_lot = produce_user_lot(user_names)

    id_of_username = {}
    for _id, user_allcount in enumerate(user_alljobs_sorted_lot):
        id_of_username[user_allcount[0]] = user_allcount[0][0] \
            if eval(config['fill_with_user_firstletter']) else config['possible_ids'][_id]

    # Calculates and prints what is actually below the id|  R + Q /all | unix account etc line
    for state_abbrev in state_abbrevs:
        _xjobs_of_user = job_counts[state_abbrevs[state_abbrev]]
        missing_uids = set(id_of_username).difference(_xjobs_of_user)
        [_xjobs_of_user.setdefault(missing_uid, 0) for missing_uid in missing_uids]

    return job_counts, user_alljobs_sorted_lot, id_of_username


def create_account_jobs_table(user_names, job_states, wns_occupancy):
    job_counts, user_alljobs_sorted_lot, id_of_username = calculate_job_counts(user_names, job_states)
    account_jobs_table = []
    for user_alljobs in user_alljobs_sorted_lot:
        user, alljobs_of_user = user_alljobs
        account_jobs_table.append(
            [
                id_of_username[user],
                job_counts['running_of_user'][user],
                job_counts['queued_of_user'][user],
                alljobs_of_user,
                user
             ]
        )
    account_jobs_table.sort(key=itemgetter(3, 4), reverse=True)  # sort by All jobs, then unix account
    # TODO: unix account id needs to be recomputed at this point. fix.
    for quintuplet, new_uid in zip(account_jobs_table, config['possible_ids']):
        unix_account = quintuplet[-1]
        quintuplet[0] = id_of_username[unix_account] = unix_account[0] if eval(config['fill_with_user_firstletter']) else \
            new_uid
    wns_occupancy['account_jobs_table'] = account_jobs_table
    wns_occupancy['id_of_username'] = id_of_username


def create_job_counts(user_names, job_states, state_abbrevs):
    """
    counting of R, Q, C, W, E attached to each user
    """
    job_counts = dict()
    for value in state_abbrevs.values():
        job_counts[value] = dict()

    for user_name, job_state in zip(user_names, job_states):
        try:
            x_of_user = state_abbrevs[job_state]
        except KeyError:
            logging.critical('Job state %s not found.'
                             'You may wish to add that node state inside %s in state_abbreviations section.\n'
                             'Exiting...' % (job_state, QTOPCONF_YAML))
            sys.exit(1)
        job_counts[x_of_user][user_name] = job_counts[x_of_user].get(user_name, 0) + 1

    for user_name in job_counts['running_of_user']:
        [job_counts[x_of_user].setdefault(user_name, 0) for x_of_user in job_counts if x_of_user != 'running_of_user']

    return job_counts


def produce_user_lot(_user_names):
    """
    Produces a list of tuples (lot) of the form (user account, all jobs count) in descending order.
    Used in the user accounts and poolmappings table
    """
    alljobs_of_user = {}
    for user_name in set(_user_names):
        alljobs_of_user[user_name] = _user_names.count(user_name)
    user_alljobs_sorted_lot = sorted(alljobs_of_user.items(), key=itemgetter(1), reverse=True)
    return user_alljobs_sorted_lot


def expand_useraccounts_symbols(config, user_list):
    """
    In case there are more users than the sum number of all numbers and small/capital letters of the alphabet
    """
    if len(user_list) > MAX_UNIX_ACCOUNTS:
        for i in xrange(MAX_UNIX_ACCOUNTS, len(user_list) + MAX_UNIX_ACCOUNTS):
            config['possible_ids'].append(str(i)[0])


def fill_node_cores_column(_node, core_user_map, id_of_username, max_np_range, user_of_job_id):
    """
    Calculates the actual contents of the map by filling in a status string for each CPU line
    state_np_corejob was: [state, np, (core0, job1), (core1, job1), ....]
    will be a dict!
    """
    state_np_corejob = cluster_dict['workernode_dict'][_node]
    state = state_np_corejob['state']
    np = state_np_corejob['np']
    corejobs = state_np_corejob.get('core_job_map', '')

    if state == '?':  # for non-existent machines
        for core_line in core_user_map:
            core_user_map[core_line] += [config['non_existent_node_symbol']]
    else:
        _own_np = int(np)
        own_np_range = [str(x) for x in range(_own_np)]
        own_np_empty_range = own_np_range[:]

        for corejob in corejobs:
            core, job = str(corejob['core']), str(corejob['job'])
            try:
                user = user_of_job_id[job]
            except KeyError, KeyErrorValue:
                logging.critical('There seems to be a problem with the qstat output. '
                                 'A Job (ID %s) has gone rogue. '
                                 'Please check with the SysAdmin.' % (str(KeyErrorValue)))
                raise KeyError
            else:
                # filling = eval(config['fill_with_user_firstletter']) and str(user[0]) or str(id_of_username[user])
                filling = str(id_of_username[user])
                core_user_map['Core' + str(core) + 'line'] += [filling]
                own_np_empty_range.remove(core)

        non_existent_cores = [item for item in max_np_range if item not in own_np_range]

        '''
        the height of the matrix is determined by the highest-core WN existing. If other WNs have less cores,
        these positions are filled with '#'s.
        '''
        for core in own_np_empty_range:
            core_user_map['Core' + str(core) + 'line'] += ['_']
        for core in non_existent_cores:
            core_user_map['Core' + str(core) + 'line'] += ['#']

    cluster_dict['workernode_dict'][_node]['core_user_column'] = [core_user_map[line][-1] for line in core_user_map]

    return core_user_map


def insert_separators(orig_str, separator, pos, stopaftern=0):
    """
    inserts separator into orig_str every pos-th position, optionally stopping after stopaftern times.
    """
    pos = int(pos)
    if not pos:  # default value is zero, means no vertical separators
        return orig_str
    else:
        sep_str = orig_str[:]  # insert initial vertical separator

        times = len(orig_str) / pos if not stopaftern else stopaftern
        sep_str = sep_str[:pos] + separator + sep_str[pos:]
        for i in range(2, times + 1):
            sep_str = sep_str[:pos * i + i - 1] + separator + sep_str[pos * i + i - 1:]
        sep_str += separator  # insert initial vertical separator
        return sep_str


def calc_all_wnid_label_lines(cluster_dict, wns_occupancy):  # (total_wn) in case of multiple cluster_dict['node_subclusters']
    """
    calculates the Worker Node ID number line widths. expressed by hxxxxs in the following form, e.g. for hundreds of nodes:
    '1': [ 00000000... ]
    '2': [ 0000000001111111... ]
    '3': [ 12345678901234567....]
    where list contents are strings: '0', '1' etc
    """
    highest_wn = cluster_dict['highest_wn']
    if NAMED_WNS or options.FORCE_NAMES:
        workernode_dict = cluster_dict['workernode_dict']
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

    wns_occupancy['wn_vert_labels'] = wn_vert_labels


def find_matrices_width(wns_occupancy, cluster_dict, DEADWEIGHT=11):
    """
    masking/clipping functionality: if the earliest node number is high (e.g. 130), the first 129 WNs need not show up.
    case 1: wn_number is RemapNr, WNList is WNListRemapped
    case 2: wn_number is BiggestWrittenNode, WNList is WNList
    DEADWEIGHT is the space taken by the {__XXXX__} labels on the right of the CoreX map

    uses cluster_dict['highest_wn'], cluster_dict['workernode_list']
    """
    start = 0
    wn_number = cluster_dict['highest_wn']
    workernode_list = cluster_dict['workernode_list']
    # was: term_columns = wns_occupancy['term_columns']
    term_columns = config['term_size'][1]
    min_masking_threshold = int(config['workernodes_matrix'][0]['wn id lines']['min_masking_threshold'])
    if options.NOMASKING and min(workernode_list) > min_masking_threshold:
        # exclude unneeded first empty nodes from the matrix
        start = min(workernode_list) - 1

    # Extra matrices may be needed if the WNs are more than the screen width can hold.
    if wn_number > start:  # start will either be 1 or (masked >= config['min_masking_threshold'] + 1)
        extra_matrices_nr = int(ceil(abs(wn_number - start) / float(term_columns - DEADWEIGHT))) - 1
    elif options.REMAP:  # was: ***wn_number < start*** and len(cluster_dict['node_subclusters']) > 1:  # Remapping
        extra_matrices_nr = int(ceil(wn_number / float(term_columns - DEADWEIGHT))) - 1
    else:
        raise (NotImplementedError, "Not foreseen")

    if USER_CUT_MATRIX_WIDTH:  # if the user defines a custom cut (in the configuration file)
        stop = start + USER_CUT_MATRIX_WIDTH
        wns_occupancy['extra_matrices_nr'] = wn_number / USER_CUT_MATRIX_WIDTH
    elif extra_matrices_nr:  # if more matrices are needed due to lack of space, cut every matrix so that if fits to screen
        stop = start + term_columns - DEADWEIGHT
        wns_occupancy['extra_matrices_nr'] = extra_matrices_nr
    else:  # just one matrix, small cluster!
        stop = start + wn_number
        wns_occupancy['extra_matrices_nr'] = 0

    wns_occupancy['print_char_start'] = start
    wns_occupancy['print_char_stop'] = stop


def display_wnid_lines(start, stop, highest_wn, wn_vert_labels, **kwargs):
    """
    Prints the Worker Node ID lines, after it colors them and adds separators to them.
    highest_wn determines the number of WN ID lines needed  (1/2/3/4+?)
    """
    d = OrderedDict()
    end_labels = config['workernodes_matrix'][0]['wn id lines']['end_labels']

    if not NAMED_WNS:
        node_str_width = len(str(highest_wn))  # 4 for thousands of nodes, nr of horizontal lines to be displayed

        for node_nr in range(1, node_str_width + 1):
            d[str(node_nr)] = "".join(wn_vert_labels[str(node_nr)])
        end_labels_iter = iter(end_labels[str(node_str_width)])
        print_wnid_lines(d, start, stop, end_labels_iter, transposed_matrices,
                         color_func=color_plainly, args=('White', 'Gray_L', start > 0))
        # start > 0 is just a test for a possible future condition

    elif NAMED_WNS or options.FORCE_NAMES:  # names (e.g. fruits) instead of numbered WNs
        node_str_width = len(wn_vert_labels)  # key, nr of horizontal lines to be displayed

        # for longer full-labeled wn ids, add more end-labels (far-right) towards the bottom
        for num in range(8, len(wn_vert_labels) + 1):
            end_labels.setdefault(str(num), end_labels['7'] + num * ['={___ID___}'])

        end_labels_iter = iter(end_labels[str(node_str_width)])
        print_wnid_lines(wn_vert_labels, start, stop, end_labels_iter, transposed_matrices,
                         color_func=highlight_alternately, args=(ALT_LABEL_HIGHLIGHT_COLORS))


def print_wnid_lines(d, start, stop, end_labels, transposed_matrices, color_func, args):
    colors = iter(color_func(*args))
    if eval(config['transpose_wn_matrices']):
        tuple = [None, 'wnid_lines', transpose_matrix(d)]
        transposed_matrices.append(tuple)
        return

    for line_nr, end_label, color in zip(d, end_labels, colors):
        wn_id_str = insert_separators(d[line_nr][start:stop], SEPARATOR, config['vertical_separator_every_X_columns'])
        wn_id_str = ''.join([colorize(elem, color) for elem in wn_id_str])
        print wn_id_str + end_label


def highlight_alternately(color_a, color_b):
    highlight = {0: color_a, 1: color_b}  # should obviously be customizable
    selection = 0
    while True:
        selection = 0 if selection else 1
        yield highlight[selection]


def color_plainly(color_0, color_1, condition):
    while condition:
        yield color_0
    else:
        while not condition:
            yield color_1


def is_matrix_coreless(workernodes_occupancy):
    print_char_start = workernodes_occupancy['print_char_start']
    print_char_stop = workernodes_occupancy['print_char_stop']
    lines = []
    core_user_map = workernodes_occupancy['core user map']
    for ind, k in enumerate(core_user_map):
        cpu_core_line = core_user_map['Core' + str(ind) + 'line'][print_char_start:print_char_stop]
        if options.REM_EMPTY_CORELINES and \
            (
                ('#' * (print_char_stop - print_char_start) == cpu_core_line) or \
                ('#' * (len(cpu_core_line)) == cpu_core_line)
            ):
            lines.append('*')

    return len(lines) == len(core_user_map)


def display_remaining_matrices(wn_occupancy, DEADWEIGHT=11):
    """
    If the WNs are more than a screenful (width-wise), this calculates the extra matrices needed to display them.
    DEADWEIGHT is the space taken by the {__XXXX__} labels on the right of the CoreX map

    if the first matrix has e.g. 10 machines with 64 cores,
    and the remaining 190 machines have 8 cores, this doesn't print the non-existent
    56 cores from the next matrix on.
    """
    extra_matrices_nr = wn_occupancy['extra_matrices_nr']
    # term_columns = wn_occupancy['term_columns']
    term_columns = config['term_size'][1]

    # need node_state, temp
    for matrix in range(extra_matrices_nr):
        wn_occupancy['print_char_start'] = wn_occupancy['print_char_stop']
        if USER_CUT_MATRIX_WIDTH:
            wn_occupancy['print_char_stop'] += USER_CUT_MATRIX_WIDTH
        else:
            wn_occupancy['print_char_stop'] += term_columns - DEADWEIGHT
        wn_occupancy['print_char_stop'] = min(wn_occupancy['print_char_stop'], cluster_dict['total_wn']) \
            if options.REMAP else min(wn_occupancy['print_char_stop'], cluster_dict['highest_wn'])

        display_matrix(wn_occupancy)


def display_matrix(workernodes_occupancy):
    """
    occupancy_parts needs to be redefined for each matrix, because of changed parameter values
    """
    # global transposed_matrices
    if is_matrix_coreless(workernodes_occupancy):
        return
    print_char_start = workernodes_occupancy['print_char_start']
    print_char_stop = workernodes_occupancy['print_char_stop']
    wn_vert_labels = workernodes_occupancy['wn_vert_labels']
    core_user_map = workernodes_occupancy['core user map']
    extra_matrices_nr = workernodes_occupancy['extra_matrices_nr']
    # term_columns = workernodes_occupancy['term_columns']
    term_columns = config['term_size'][1]
    pattern_of_id = workernodes_occupancy['pattern_of_id']

    occupancy_parts = {
        'wn id lines':
            (
                display_wnid_lines,
                (print_char_start, print_char_stop, cluster_dict['highest_wn'], wn_vert_labels),
                {'inner_attrs': None}
            ),
        'core user map':
            (
                print_core_lines,
                (core_user_map, print_char_start, print_char_stop, transposed_matrices, pattern_of_id),
                {'attrs': None}
            ),
    }

    # custom part
    for yaml_key, part_name in get_yaml_key_part('workernodes_matrix'):
        new_occupancy_part = {
            part_name:
                (
                    print_mult_attr_line,  # func
                    (print_char_start, print_char_stop, transposed_matrices),  # args
                    {'attr_lines': workernodes_occupancy[part_name]}  # kwargs
                )
        }
        occupancy_parts.update(new_occupancy_part)

    for part_dict in config['workernodes_matrix']:
        part = [k for k in part_dict][0]
        occupancy_parts[part][2].update(part_dict[part])  # get extra options from user
        fn, args, kwargs = occupancy_parts[part][0], occupancy_parts[part][1], occupancy_parts[part][2]
        fn(*args, **kwargs)

    if eval(config['transpose_wn_matrices']):
        order = config['occupancy_column_order']
        for idx, (item, matrix) in enumerate(zip(order, transposed_matrices)):
            matrix[0] = order.index(matrix[1])

        transposed_matrices.sort(key=lambda item: item[0])

        for line_tuple in izip_longest(*[tpl[2] for tpl in transposed_matrices], fillvalue='  '):
            join_prints(*line_tuple, sep=config.get('horizontal_separator', None))
        logging.debug('Printed horizontally from %s to %s' % (config['h_start'], h_stop))
    print


def print_mult_attr_line(print_char_start, print_char_stop, transposed_matrices, attr_lines, label, color_func=None,
                         **kwargs):  # NEW!
    """
    attr_lines can be e.g. Node state lines
    """
    if eval(config['transpose_wn_matrices']):
        tuple = [None, label, transpose_matrix(attr_lines)]
        transposed_matrices.append(tuple)
        return
    # TODO: fix option parameter, inserted for testing purposes
    for line in attr_lines:
        line = attr_lines[line][print_char_start:print_char_stop]
        # TODO: maybe put attr_line and label as kwd arguments? collect them as **kwargs
        attr_line = insert_separators(line, SEPARATOR, config['vertical_separator_every_X_columns'])
        attr_line = ''.join([colorize(char, color_func) for char in attr_line])
        print attr_line + "=" + label


def display_user_accounts_pool_mappings(account_jobs_table, pattern_of_id):
    detail_of_name = get_detail_of_name(account_jobs_table)
    print colorize('\n===> ', 'Gray_D') + \
          colorize('User accounts and pool mappings', 'White') + \
          colorize(' <=== ', 'Gray_d') + \
          colorize("  ('all' also includes those in C and W states, as reported by qstat)"
                   if options.CLASSIC else "  ('all' includes any jobs beyond R and W)", 'Gray_D')

    print '   R +    Q /  all |    unix account | id| %(msg)s' % \
          {'msg': 'Grid certificate DN (info only available under elevated privileges)' if options.CLASSIC else
          'GECOS field or Grid certificate DN'}
    for line in account_jobs_table:
        uid, runningjobs, queuedjobs, alljobs, user = line[0], line[1], line[2], line[3], line[4]
        account = pattern_of_id[uid]
        if options.COLOR == 'OFF' or account == 'account_not_colored' or color_of_account[account] == 'reset':
            extra_width = 0
            account = 'account_not_colored'
        else:
            extra_width = 12
        print_string = '{1:>{width4}} + {2:>{width4}} / {3:>{width4}} {sep} ' \
                       '{4:>{width15}} {sep} ' \
                       '{0:<{width2}}{sep} ' \
                       '{5:<{width40}} {sep}'.format(
            colorize(str(uid), '', account),
            colorize(str(runningjobs), '', account),
            colorize(str(queuedjobs), '', account),
            colorize(str(alljobs), '', account),
            colorize(user, '', account),
            colorize(detail_of_name.get(user, ''), '', account),
            sep=colorize(SEPARATOR, '', account),
            width2=2 + extra_width,
            width3=3 + extra_width,
            width4=4 + extra_width,
            width15=15 + extra_width,
            width40=40 + extra_width,
        )
        print print_string


def get_core_lines(core_user_map, print_char_start, print_char_stop, pattern_of_id, attrs):
    """
    prints all coreX lines, except cores that don't show up
    anywhere in the given matrix
    """
    # TODO: is there a way to use is_matrix_coreless in here? avoid duplication of code
    for ind, k in enumerate(core_user_map):
        cpu_core_line = core_user_map['Core' + str(ind) + 'line'][print_char_start:print_char_stop]
        if options.REM_EMPTY_CORELINES and \
            (
                ('#' * (print_char_stop - print_char_start) == cpu_core_line) or \
                ('#' * (len(cpu_core_line)) == cpu_core_line)
            ):
            continue
        cpu_core_line = insert_separators(cpu_core_line, SEPARATOR, config['vertical_separator_every_X_columns'])
        cpu_core_line = ''.join([colorize(elem, '', pattern_of_id[elem]) for elem in cpu_core_line if elem in pattern_of_id])
        yield cpu_core_line + colorize('=Core' + str(ind), '', 'account_not_colored')


def calc_core_userid_matrix(cluster_dict, wns_occupancy, job_ids, user_names):
    id_of_username = wns_occupancy['id_of_username']
    _core_user_map = OrderedDict()
    max_np_range = [str(x) for x in range(cluster_dict['max_np'])]
    user_of_job_id = dict(izip(job_ids, user_names))

    for core_nr in max_np_range:
        _core_user_map['Core%sline' % str(core_nr)] = []  # Cpu0line, Cpu1line, Cpu2line, .. = '','','', ..

    for _node in cluster_dict['workernode_dict']:
        # state_np_corejob = cluster_dict['workernode_dict'][_node]
        _core_user_map = fill_node_cores_column(_node, _core_user_map, id_of_username, max_np_range, user_of_job_id)

    for coreline in _core_user_map:
        _core_user_map[coreline] = ''.join(_core_user_map[coreline])

    wns_occupancy['core user map'] = _core_user_map


def calc_general_multiline_attr(cluster_dict, part_name, yaml_key):  # NEW
    multiline_map = OrderedDict()
    elem_identifier = [d for d in config['workernodes_matrix'] if part_name in d][0]  # jeeez
    part_name_idx = config['workernodes_matrix'].index(elem_identifier)
    user_max_len = int(config['workernodes_matrix'][part_name_idx][part_name]['max_len'])
    try:
        real_max_len = max([len(cluster_dict['workernode_dict'][_node][yaml_key]) for _node in cluster_dict['workernode_dict']])
    except KeyError:
        logging.critical("%s lines in the matrix are not supported for %s systems. "
                         "Please remove appropriate lines from conf file. Exiting..."
                         % (part_name, config['scheduler'] ))
        sys.exit(1)
    min_len = min(user_max_len, real_max_len)
    max_len = max(user_max_len, real_max_len)
    if real_max_len > user_max_len:
        logging.warn("Some longer %(attr)ss have been cropped due to %(attr)s length restriction by user" % {"attr": part_name})

    # initialisation of lines
    for line_nr in range(1, min_len + 1):
        multiline_map['attr%sline' % str(line_nr)] = []

    for _node in cluster_dict['workernode_dict']:
        state_np_corejob = cluster_dict['workernode_dict'][_node]
        # distribute_state_to_lines
        for attr_line, ch in izip_longest(multiline_map, state_np_corejob[yaml_key], fillvalue=' '):
            try:
                multiline_map[attr_line].append(ch)
            except KeyError:
                break
        # TODO: is this really needed?: cluster_dict['workernode_dict'][_node]['state_column']

    for line, attr_line in enumerate(multiline_map, 1):
        multiline_map[attr_line] = ''.join(multiline_map[attr_line])
        if line == user_max_len:
            break

    return multiline_map


def transpose_matrix(d, colored=False, reverse=False):
    """
    takes a dictionary whose values are lists of strings (=matrix)
    returns a transposed matrix
    """
    pattern_of_id = workernodes_occupancy['pattern_of_id']
    for tuple in izip_longest(*[[char for char in d[k]] for k in d], fillvalue=" "):
        if any(j != " " for j in tuple):
            tuple = colored and [colorize(j, '', pattern_of_id[j]) if j in pattern_of_id else j for j in tuple] or list(tuple)
            tuple[:] = tuple[::-1] if reverse else tuple
            yield tuple


def join_prints(*args, **kwargs):
    joined_list = []
    for d in args:
        sys.stdout.softspace = False # if i want to omit in-between column spaces
        joined_list.extend(d)
        joined_list.append(kwargs['sep'])

    print "".join(joined_list[h_start:h_stop])
    workernodes_occupancy.setdefault('max_full_line_len', len(joined_list))


def get_yaml_key_part(major_key):
    """
    only return the list items of the yaml major_key if a yaml key subkey exists
    (this signals a user-inserted value)
    """
    # e.g. major_key = 'workernodes_matrix'
    for part in config[major_key]:
        part_name = [i for i in part][0]
        part_options = part[part_name]
        # label = part_options.get('label')
        # part_nr_lines = int(part_options['max_len'])
        yaml_key = part_options.get('yaml_key')
        if yaml_key:
            yield yaml_key, part_name


def calculate_wn_occupancy(cluster_dict, user_names, job_states, job_ids):
    """
    Prints the Worker Nodes Occupancy table.
    if there are non-uniform WNs in pbsnodes.yaml, e.g. wn01, wn02, gn01, gn02, ...,  remapping is performed.
    Otherwise, for uniform WNs, i.e. all using the same numbering scheme, wn01, wn02, ... proceeds as normal.
    Number of Extra tables needed is calculated inside the calc_all_wnid_label_lines function below
    """
    wns_occupancy = dict()
    calculate_split_screen_size()  # term_columns
    create_account_jobs_table(user_names, job_states, wns_occupancy) # account_jobs_table, id_of_username
    make_pattern_of_id(wns_occupancy)  # pattern_of_id

    find_matrices_width(wns_occupancy, cluster_dict)  # print_char_start, print_char_stop, extra_matrices_nr
    calc_all_wnid_label_lines(cluster_dict, wns_occupancy)  # wn_vert_labels

    # For-loop below only for user-inserted/customizeable values.
    # e.g. wns_occupancy['node_state'] = ...workernode_dict[node]['state'] for node in workernode_dict...
    for yaml_key, part_name in get_yaml_key_part('workernodes_matrix'):
        wns_occupancy[part_name] = calc_general_multiline_attr(cluster_dict, part_name, yaml_key)

    calc_core_userid_matrix(cluster_dict, wns_occupancy, job_ids, user_names)  # core user map
    return wns_occupancy, cluster_dict


def print_core_lines(core_user_map, print_char_start, print_char_stop, transposed_matrices, pattern_of_id, attrs, options1,
                     options2):
    signal(SIGPIPE, SIG_DFL)
    if eval(config['transpose_wn_matrices']):
        tuple = [None, 'core_map', transpose_matrix(core_user_map, colored=True)]
        transposed_matrices.append(tuple)
        return
    for core_line in get_core_lines(core_user_map, print_char_start, print_char_stop, pattern_of_id, attrs):
        try:
            print core_line
        except IOError:
            # This tries to handle the broken pipe exception that occurs when doing "| head"
            # stdout is closed, no point in continuing
            # Attempt to close them explicitly to prevent cleanup problems
            # Results are not always best. misbehaviour with watch -d,
            # output gets corrupted in the terminal afterwards without watch.
            # TODO Find fix.
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


def display_wn_occupancy(workernodes_occupancy, cluster_dict):
    if eval(config['transpose_wn_matrices']):
        order = config['occupancy_column_order']
        note = "/".join(order)
    else:
        note = 'you can read vertically the node IDs; nodes in free state are noted with - '
    print colorize('===> ', 'Gray_D') + colorize('Worker Nodes occupancy', 'White') + colorize(' <=== ', 'Gray_D') \
          + colorize('(%s)', 'Gray_D') % note

    display_matrix(workernodes_occupancy)
    if not eval(config['transpose_wn_matrices']):
        display_remaining_matrices(workernodes_occupancy)


def make_pattern_of_id(wns_occupancy):
    """
    First strips the numbers off of the unix accounts and tries to match this against the given color table in colormap.
    Additionally, it will try to apply the regex rules given by the user in qtopconf.yaml, overriding the colormap.
    The last matched regex is valid.
    If no matching was possible, there will be no coloring applied.
    """
    pattern_of_id = {}
    for line in wns_occupancy['account_jobs_table']:
        uid, user = line[0], line[4]
        account = re.search('[A-Za-z]+', user).group(0)  #
        for re_account_color in config['user_color_mappings']:
            re_account = re_account_color.keys()[0]
            try:
                _ = re.search(re_account, user).group(0)
            except AttributeError:
                continue  # keep trying
            else:
                account = re_account  # colors the text according to the regex given by the user in qtopconf

        pattern_of_id[uid] = account if account in color_of_account else 'account_not_colored'

    pattern_of_id['#'] = '#'
    pattern_of_id['_'] = '_'
    pattern_of_id[SEPARATOR] = 'account_not_colored'
    wns_occupancy['pattern_of_id'] = pattern_of_id


def load_yaml_config():
    """
    Loads ./QTOPCONF_YAML into a dictionary and then tries to update the dictionary
    with the same-named conf file found in:
    /env
    $HOME/.local/qtop/
    in that order.
    """
    config = read_yaml_natively(os.path.join(realpath(QTOPPATH), QTOPCONF_YAML))
    logging.info('Default configuration dictionary loaded. Length: %s items' % len(config))

    try:
        config_env = read_yaml_natively(os.path.join(SYSTEMCONFDIR, QTOPCONF_YAML))
    except IOError:
        config_env = {}
        logging.info('%s could not be found in %s/' % (QTOPCONF_YAML, SYSTEMCONFDIR))
    else:
        logging.info('Env %s found in %s/' % (QTOPCONF_YAML, SYSTEMCONFDIR))
        logging.info('Env configuration dictionary loaded. Length: %s items' % len(config_env))

    try:
        config_user = read_yaml_natively(os.path.join(USERPATH, QTOPCONF_YAML))
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
            config_user_custom = read_yaml_natively(os.path.join(USERPATH, options.CONFFILE))
        except IOError:
            try:
                config_user_custom = read_yaml_natively(os.path.join(CURPATH, options.CONFFILE))
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
        [color_of_account.update(d) for d in config['user_color_mappings']]
    else:
        config['user_color_mappings'] = list()
    if config['remapping']:
        pass
    else:
        config['remapping'] = list()
    for symbol in symbol_map:
        config['possible_ids'].append(symbol)

    user_selected_save_path = realpath(expandvars(config['savepath']))
    if not os.path.exists(user_selected_save_path):
        mkdir_p(user_selected_save_path)
        logging.debug('Directory %s created.' % user_selected_save_path)
    else:
        logging.debug('%s files will be saved in directory %s.' % (config['scheduler'], user_selected_save_path))
    config['savepath'] = user_selected_save_path

    return config


def calculate_split_screen_size():
    """
    Calculates where to break the matrix into more matrices, because of the window size.
    """
    fallback_term_size = [53, 176]
    try:
        term_height, term_columns = os.popen('stty size', 'r').read().split()
    except ValueError:
        logging.warn("Failed to autodetect terminal size. Trying values in %s." % QTOPCONF_YAML)
        try:
            term_height, term_columns = config['term_size']
        except ValueError:
            try:
                term_height, term_columns = fix_config_list(config['term_size'])
            except KeyError:
                config['term_size'] = fallback_term_size
    finally:
        logging.debug('Set terminal size is: %s * %s' % (term_height, term_columns))
        config['term_size'] = [int(term_height), int(term_columns)]
        config['h_start'] = h_start
        config['v_stop'], config['h_stop'] = config['term_size']


def sort_batch_nodes(batch_nodes):
    try:
        batch_nodes.sort(key=eval(config['sorting']['user_sort']), reverse=eval(config['sorting']['reverse']))
    except (IndexError, ValueError):
        logging.critical("There's (probably) something wrong in your sorting lambda in %s." % QTOPCONF_YAML)
        raise


def filter_list_out(batch_nodes, the_list=None):
    if not the_list:
        the_list = []
    for idx, node in enumerate(batch_nodes):
        if idx in the_list:
            node['mark'] = '*'
    batch_nodes = filter(lambda item: not item.get('mark'), batch_nodes)
    return batch_nodes


def filter_list_out_by_name(batch_nodes, the_list=None):
    if not the_list:
        the_list = []
    else:
        the_list[:] = [eval(i) for i in the_list]
    for idx, node in enumerate(batch_nodes):
        if node['domainname'].split('.', 1)[0] in the_list:
            node['mark'] = '*'
    batch_nodes = filter(lambda item: not item.get('mark'), batch_nodes)
    return batch_nodes


def filter_list_in_by_name(batch_nodes, the_list=None):
    if not the_list:
        the_list = []
    else:
        the_list[:] = [eval(i) for i in the_list]
    for idx, node in enumerate(batch_nodes):
        if node['domainname'].split('.', 1)[0] not in the_list:
            node['mark'] = '*'
    batch_nodes = filter(lambda item: not item.get('mark'), batch_nodes)
    return batch_nodes


def filter_list_out_by_node_state(batch_nodes, the_list=None):
    if not the_list:
        the_list = []
    for idx, node in enumerate(batch_nodes):
        if node['state'] in the_list:
            node['mark'] = '*'
    batch_nodes = filter(lambda item: not item.get('mark'), batch_nodes)
    return batch_nodes


def filter_list_out_by_name_pattern(batch_nodes, the_list=None):
    if not the_list:
        the_list = []

    for idx, node in enumerate(batch_nodes):
        for pattern in the_list:
            match = re.search(pattern, node['domainname'].split('.', 1)[0])
            try:
                match.group(0)
            except AttributeError:
                pass
            else:
                node['mark'] = '*'
    batch_nodes = filter(lambda item: not item.get('mark'), batch_nodes)
    return batch_nodes


def filter_list_in_by_name_pattern(batch_nodes, the_list=None):
    if not the_list:
        the_list = []

    for idx, node in enumerate(batch_nodes):
        for pattern in the_list:
            match = re.search(pattern, node['domainname'].split('.', 1)[0])
            try:
                match.group(0)
            except AttributeError:
                node['mark'] = '*'
            else:
                pass
    batch_nodes = filter(lambda item: not item.get('mark'), batch_nodes)
    return batch_nodes


def filter_batch_nodes(batch_nodes, filter_rules=None):
    """
    Filters specific nodes according to the filter rules in QTOPCONF_YAML
    """

    filter_types = {
        'list_out': filter_list_out,
        'list_out_by_name': filter_list_out_by_name,
        'list_in_by_name': filter_list_in_by_name,
        'list_out_by_name_pattern': filter_list_out_by_name_pattern,
        'list_in_by_name_pattern': filter_list_in_by_name_pattern,
        'list_out_by_node_state': filter_list_out_by_node_state
        # 'ranges_out': func3,
    }

    if not filter_rules:
        return batch_nodes
    else:
        logging.warning("WN Occupancy view is filtered.")
        for rule in filter_rules:
            filter_func = filter_types[rule.keys()[0]]
            args = rule.values()[0]
            batch_nodes = filter_func(batch_nodes, args)
        return batch_nodes


def map_batch_nodes_to_wn_dicts(cluster_dict, batch_nodes, options_remap):
    """
    For filtering to take place,
    1) a filter should be defined in QTOPCONF_YAM
    2) remap should be either selected by the user or enforced by the circumstances
    """
    nodes_drop = 0  # count change in nodes after filtering
    user_sorting = config['sorting'] and config['sorting'].values()[0]
    user_filtering = config['filtering'] and config['filtering'][0]

    if user_sorting and options_remap:
        sort_batch_nodes(batch_nodes)

    if user_filtering and options_remap:
        batch_nodes_before = len(batch_nodes)
        batch_nodes = filter_batch_nodes(batch_nodes, config['filtering'])
        batch_nodes_after = len(batch_nodes)
        nodes_drop = batch_nodes_after - batch_nodes_before

    for (batch_node, (idx, cur_node_nr)) in zip(batch_nodes, enumerate(cluster_dict['workernode_list'])):
        # Seemingly there is an error in the for loop because batch_nodes and workernode_list
        # have different lengths if there's a filter in place, but it is OK, since
        # it is just the idx counter that is taken into account in remapping.
        cluster_dict['workernode_dict'][cur_node_nr] = batch_node
        cluster_dict['workernode_dict_remapped'][idx] = batch_node
    return nodes_drop


def convert_to_yaml(scheduler, INPUT_FNs_commands, filenames, write_method=options.write_method):

    yaml_converter = {
        'pbs': {
            'pbsnodes_file': convert_pbsnodes_to_yaml,
            'qstatq_file': QStatMaker(config).convert_qstatq_to_yaml,
            'qstat_file': QStatMaker(config).convert_qstat_to_yaml,
        },
        'oar': {
            'oarnodes_s_file': lambda x, y, z: None,
            'oarnodes_y_file': lambda x, y, z: None,
            'oarstat_file': OarStatMaker(config).convert_qstat_to_yaml,
        },
        'sge': {
            'sge_file_stat': SGEStatMaker(config).convert_qstat_to_yaml,
        }
    }
    converters = yaml_converter[scheduler]

    for _file in INPUT_FNs_commands:
        file_orig, file_out = filenames[_file], filenames[_file + '_out']
        converter_func = converters[_file]
        logging.debug('Executing %(func)s \n\t'
                      'on: %(file_orig)s,\n\t' % {"func": converter_func.__name__, "file_orig": file_orig, "file_out": file_out})
        converter_func(file_orig, file_out, write_method)


def exec_func_tuples(func_tuples):
    _commands = iter(func_tuples)
    for command in _commands:
        ffunc, args, kwargs = command[0], command[1], command[2]
        logging.debug('Executing %s' % ffunc.__name__)
        yield ffunc(*args, **kwargs)


def get_worker_nodes(scheduler):
    d = {}
    d['pbs'] = plugin_pbs._get_worker_nodes
    d['oar'] = plugin_oar._get_worker_nodes
    d['sge'] = plugin_sge._get_worker_nodes
    return d[scheduler]


def get_queues_info(scheduler):
    d = {}
    d['pbs'] = plugin_pbs._read_qstatq_yaml
    d['oar'] = lambda *args, **kwargs: (0, 0, [])
    d['sge'] = plugin_sge._get_statq_from_xml
    return d[scheduler]


def get_jobs_info(scheduler):
    return common_module.get_jobs_info


def get_yaml_files(scheduler, filenames):
    SGEStatMaker.temp_filepath = None if scheduler != 'sge' else SGEStatMaker.temp_filepath
    args = {}
    # import wdb; wdb.set_trace()
    args['pbs'] = {
        'get_worker_nodes': [filenames.get('pbsnodes_file_out')],
        'get_jobs_info': [filenames.get('qstat_file_out')],
        'get_queues_info': [filenames.get('qstatq_file_out')]
    }
    args['oar'] = {
        'get_worker_nodes': [filenames.get('oarnodes_s_file'), filenames.get('oarnodes_y_file')],
        'get_jobs_info': [filenames.get('oarstat_file_out')],
        'get_queues_info': [filenames.get('oarstat_file')],
    }
    args['sge'] = {
        'get_worker_nodes': [filenames.get('sge_file_stat')],
        'get_jobs_info': [SGEStatMaker.temp_filepath],
        'get_queues_info': [filenames.get('sge_file_stat')],
    }
    return args[scheduler]


def get_filenames_commands():
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


def auto_get_avail_batch_system():
    """
    If the auto option exists in env variable QTOP_SCHEDULER or in QTOPCONF_YAML
    (QTOP_SCHEDULER should be unset if QTOPCONF_YAML is set to auto)
    qtop tries to determine which of the known batch commands are available in the current system.
    """
    if not os.environ.get('QTOP_SCHEDULER', config['scheduler']) == 'auto':
        return None
    for (batch_command, system) in [('pbsnodes', 'pbs'), ('oarnodes', 'oar'), ('qstat', 'sge')]:
        NOT_FOUND = subprocess.call(['which', batch_command], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        if not NOT_FOUND:
            return system
    else:
        return None


def pick_batch_system():
    avail_batch_system = auto_get_avail_batch_system()
    scheduler = options.BATCH_SYSTEM or avail_batch_system or config['scheduler']
    logging.debug('cmdline switch option scheduler: %s' % options.BATCH_SYSTEM or "None")
    logging.debug('Autodetected scheduler: %s' % avail_batch_system or "None")
    if scheduler == 'auto':
        logging.critical('No suitable scheduler was found. '
                         'Please define one in a switch or env variable or in %s' % QTOPCONF_YAML)
        raise (ValueError, 'No suitable scheduler was found.')
    logging.debug('Selected scheduler is %s' % scheduler)
    return scheduler


def execute_shell_batch_commands(batch_system_commands, filenames, _file):
    _batch_system_command = batch_system_commands[_file].strip()
    with open(filenames[_file], mode='w') as fin:
        logging.debug('Command: "%s" -- result will be saved in: %s' % (_batch_system_command, filenames[_file]))
        logging.debug('\tFile state before subprocess call: %(fin)s' % {"fin": fin})
        logging.debug('\tWaiting on subprocess.call...')

        command = subprocess.Popen(_batch_system_command, stdout=fin, stderr=subprocess.PIPE, shell=True)
        error = command.communicate()[1]
        command.wait()
        if error:
            logging.exception('A message from your shell: %s' % error)
            logging.critical('%s could not be executed. Maybe try "module load %s"?' % (_batch_system_command, scheduler))
            sys.exit(1)

    logging.debug('File state after subprocess call: %(fin)s' % {"fin": fin})


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

    p = subprocess.Popen(passwd_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    output, err = p.communicate("something here")
    if 'No such file or directory' in err:
        logging.error('You have to set a proper command to get the passwd file in your %s file.' % QTOPCONF_YAML)
        logging.error('Error returned by getent: %s\nCommand issued: %s' % (err, passwd_command))

    # with open(fn, mode='r') as fin:
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


def get_input_filenames():

    parser_extension_mapping = {'txtyaml': 'yaml', 'json': 'json'}
    extension = parser_extension_mapping[options.write_method]
    logging.info('Selected method for storing data structures is: %s' % extension)

    filenames = dict()
    batch_system_commands = dict()
    for _file in INPUT_FNs_commands:
        filenames[_file], batch_system_commands[_file] = INPUT_FNs_commands[_file]

        if not options.SOURCEDIR:
            # if user didn't specify a dir where ready-made data files already exist,
            # execute the appropriate batch commands and fetch results to the respective files
            execute_shell_batch_commands(batch_system_commands, filenames, _file)
        else:
            pass

        filenames[_file + '_out'] = '{filename}_{writemethod}.{ext}'.format(
            filename=INPUT_FNs_commands[_file][0].rsplit('.')[0],
            writemethod=options.write_method,
            ext=extension
        )
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
        sys.exit(1)


def deprecate_old_yaml_files(filepath):
    """
    deletes older yaml files in savepath directory.
    experimental and loosely untested
    """
    time_alive = int(config['auto_delete_old_yaml_files_after_few_hours'])
    user_selected_save_path = realpath(expandvars(config['savepath']))
    for f in os.listdir(user_selected_save_path):
        if not f.endswith('yaml'):
            continue
        curpath = os.path.join(user_selected_save_path, f)
        file_modified = datetime.datetime.fromtimestamp(getmtime(curpath))
        if datetime.datetime.now() - file_modified > datetime.timedelta(hours=time_alive):
            os.remove(curpath)


def scroll_down(h_start, h_stop, v_start, v_stop):
    logging.debug('v_start: %s' % v_start)

    if v_stop < num_lines:
        v_start += config['term_size'][0]
        v_stop += config['term_size'][0]  # - 10
        logging.info('Going down...')
        config['v_stop'] = v_stop
    else:
        logging.info('Staying put')
    return h_start, h_stop, v_start, v_stop


def scroll_bottom(h_start, h_stop, v_start, v_stop):
    logging.debug('v_start: %s' % v_start)

    v_start = num_lines - config['term_size'][0]
    v_stop = num_lines
    logging.info('Going to the bottom...')
    config['v_stop'] = v_stop
    return h_start, h_stop, v_start, v_stop


def scroll_top(h_start, h_stop, v_start, v_stop):
    logging.debug('v_start: %s' % v_start)

    v_start = 0
    v_stop = config['term_size'][0]
    logging.info('Going to the top...')
    config['v_stop'] = v_stop
    return h_start, h_stop, v_start, v_stop


def scroll_up(h_start, h_stop, v_start, v_stop):
    if v_start - config['term_size'][0] >= 0:
        v_start -= config['term_size'][0]
        v_stop -= config['term_size'][0]
        logging.info('Going up...')
    else:
        v_start = 1
        v_stop = config['term_size'][0]
        logging.info('Staying put')
    config['v_stop'] = v_stop
    return h_start, h_stop, v_start, v_stop


def scroll_right(h_start, h_stop, v_start, v_stop):  # 'l', right
    h_start += config['term_size'][1]/2
    config['h_start'] = h_start
    h_stop += config['term_size'][1]/2
    config['h_stop'] = h_stop
    logging.info('Going right...')
    return config['h_start'], config['h_stop'], v_start, v_stop


def scroll_far_right(h_start, h_stop, v_start, v_stop):  # 'l', right
    h_start = workernodes_occupancy['max_full_line_len'] - config['term_size'][1]
    config['h_start'] = h_start
    h_stop = workernodes_occupancy['max_full_line_len']
    config['h_stop'] = h_stop
    logging.info('h_start: %s' % h_start)
    logging.info('max_line_len: %s' % max_line_len)
    logging.info('config["term_size"][1] %s' % config['term_size'][1])
    logging.info('h_stop: %s' % h_stop)
    logging.info('Going far right...')
    return config['h_start'], config['h_stop'], v_start, v_stop


def scroll_left(h_start, h_stop, v_start, v_stop):
    if h_start >= config['term_size'][1] / 2:
        h_start -= config['term_size'][1]/2
    if h_start > 0:
        h_stop -= config['term_size'][1]/2
    else:
        h_stop = config['term_size'][1]
    config['h_start'] = h_start
    config['h_stop'] = h_stop
    logging.info('Going left...')
    return h_start, h_stop, v_start, v_stop


def scroll_far_left(h_start, h_stop, v_start, v_stop):
    h_start = 0
    h_stop = config['term_size'][1]
    config['h_start'] = h_start
    config['h_stop'] = h_stop
    logging.info('Going far left...')
    return h_start, h_stop, v_start, v_stop


def reset_display(h_start, h_stop, v_start, v_stop):  # "R", reset display
    v_start = 1
    v_stop = config['term_size'][0]
    config['v_stop'] = v_stop
    h_start = 0
    h_stop = config['term_size'][1]
    return h_start, h_stop, v_start, v_stop

def quit_program(h_start, h_stop, v_start, v_stop):  # "q", quit
    try:
        raise KeyboardInterrupt
    except KeyboardInterrupt:
        print '  Exiting...'
        sys.exit(0)


def control_movement(pressed_char_hex, h_start, h_stop, v_start, v_stop):
    key_actions = {
        '6a': scroll_down,  # j
        '20': scroll_down,  # spacebar
        '6b': scroll_up,  # k
        '7f': scroll_up,  # Backspace
        '6c': scroll_right,  # l
        '24': scroll_far_right,  # $
        '68': scroll_left,  # h
        '30': scroll_far_left,  # 0
        '4a': scroll_bottom,  # S-j
        '47': scroll_bottom,  # G
        '4b': scroll_top,  # S-k
        '67': scroll_top,  # g
        '72': reset_display,  # r
        '71': quit_program,  # q
        '0a': lambda a,b,c,d:(a,b,c,d),  # Enter
    }
    h_start, h_stop, v_start, v_stop = key_actions[pressed_char_hex](h_start, h_stop, v_start, v_stop)
    # logging.debug('\n\tv_start: %s\n\tv_stop: %s\n\th_start: %s\n\th_stop: %s' % (v_start, v_stop, h_start, h_stop))
    logging.debug('Area Displayed: (h_start, v_start) --> (h_stop, v_stop) '
                  '\n\t(%(h_start)s, %(v_start)s) --> (%(h_stop)s, %(v_stop)s)' %
                  {'v_start': v_start, 'v_stop': v_stop, 'h_start': h_start, 'h_stop': h_stop})

    return h_start, h_stop, v_start, v_stop


def safe_exit_with_file_close(handle, name, stdout, delete_file=False):
    sys.stdout.flush()
    sys.stdout.close()
    close(handle)
    if delete_file:
        unlink(name)  # this deletes the file
    # sys.stdout = stdout
    if options.TAR >= 1:
        add_to_tar(QTOP_LOGFILE, self.config['savepath'])
    sys.exit(0)


if __name__ == '__main__':

    stdout = sys.stdout
    v_start = 1
    v_stop = None
    h_start = 0
    h_stop = None
    read_char = 'r'  # initial value, resets view position to beginning
    num_lines = 0
    max_line_len = 0
    timeout = 1

    check_python_version()
    initial_cwd = os.getcwd()
    logging.debug('Initial qtop directory: %s' % initial_cwd)
    CURPATH = os.path.expanduser(initial_cwd)  # where qtop was invoked from
    QTOPPATH = os.path.dirname(realpath(sys.argv[0]))  # dir where qtop resides

    with raw_mode(sys.stdin):
        try:
            while True:
                handle, name = get_new_temp_file(prefix='qtop_', suffix='.out')
                sys.stdout = os.fdopen(handle, 'w')  # redirect everything to file, creates file object out of handle
                # sys.stdout = open(fout, 'w', -1)  # redirect everything to file
                transposed_matrices = []
                config = load_yaml_config()

                if options.OPTION:
                    for opt in options.OPTION:
                        key, val = get_key_val_from_option_string(opt)
                        config[key] = val

                if config['faster_xml_parsing']:
                    try:
                        from lxml import etree
                    except ImportError:
                        logging.warn('Module lxml is missing. Try issuing "pip install lxml". Reverting to xml module.')
                        from xml.etree import ElementTree as etree

                if options.TRANSPOSE:
                    config['transpose_wn_matrices'] = 'False'

                SEPARATOR = config['vertical_separator'].translate(None, "'")  # alias
                USER_CUT_MATRIX_WIDTH = int(config['workernodes_matrix'][0]['wn id lines']['user_cut_matrix_width'])  # alias
                ALT_LABEL_HIGHLIGHT_COLORS = fix_config_list(config['workernodes_matrix'][0]['wn id lines']['alt_label_highlight_colors'])
                # TODO: int should be handled internally in native yaml parser
                # TODO: fix_config_list should be handled internally in native yaml parser

                options.SOURCEDIR = realpath(options.SOURCEDIR) if options.SOURCEDIR else None
                logging.debug("User-defined source directory: %s" % options.SOURCEDIR)
                options.workdir = options.SOURCEDIR or config['savepath']
                logging.debug('Working directory is now: %s' % options.workdir)
                os.chdir(options.workdir)

                scheduler = pick_batch_system()

                INPUT_FNs_commands = get_filenames_commands()
                input_filenames = get_input_filenames()

                # reset_yaml_files()  # either that or having a pid appended in the filename
                if options.TAR >= 1:  # clears any preexisting tar files
                    tar_out = tarfile.open(os.path.join(config['savepath'], QTOP_TARFN), mode='w')
                    tar_out.close()
                    add_to_tar(os.path.join(realpath(QTOPPATH), QTOPCONF_YAML), savepath)

                if not options.YAML_EXISTS:
                    convert_to_yaml(scheduler, INPUT_FNs_commands, input_filenames)
                    if options.TAR >=1:
                        [add_to_tar(input_filenames[fn], savepath) for fn in input_filenames if os.path.isfile(input_filenames[fn])]

                yaml_files = get_yaml_files(scheduler, input_filenames)

                worker_nodes = get_worker_nodes(scheduler)(*yaml_files['get_worker_nodes'])
                job_ids, user_names, job_states, _ = get_jobs_info(scheduler)(*yaml_files['get_jobs_info'])
                total_running_jobs, total_queued_jobs, qstatq_lod = get_queues_info(scheduler)(*yaml_files['get_queues_info'])
                deprecate_old_yaml_files(*yaml_files['get_jobs_info'])
                #  MAIN ##################################
                logging.info('CALCULATION AREA')
                cluster_dict, NAMED_WNS = calculate_cluster(worker_nodes)
                workernodes_occupancy, cluster_dict = calculate_wn_occupancy(cluster_dict, user_names, job_states, job_ids)
                h_stop = config['h_stop'] if h_stop is None else h_stop
                v_stop = config['v_stop'] if v_stop is None else v_stop

                display_parts = {
                    'job_accounting_summary': (display_job_accounting_summary, (cluster_dict, total_running_jobs, total_queued_jobs, qstatq_lod)),
                    'workernodes_matrix': (display_wn_occupancy, (workernodes_occupancy, cluster_dict)),
                    'user_accounts_pool_mappings': (display_user_accounts_pool_mappings, (workernodes_occupancy['account_jobs_table'], workernodes_occupancy['pattern_of_id']))
                }
                logging.info('DISPLAY AREA')

                print "\033c",  # the comma here is to avoid losing the whole first line. An empty char still remains, though.

                for idx, part in enumerate(config['user_display_parts'], 1):
                    display_func, args = display_parts[part][0], display_parts[part][1]
                    display_func(*args) if not sections_off[idx] else None
                print "\nLog file created in %s" % expandvars(QTOP_LOGFILE)
                sys.stdout.flush()
                sys.stdout.close()
                sys.stdout = stdout  # sys.stdout is back to its normal function (i.e. screen output)

                num_lines = sum(1 for line in open(name, 'r')) if not num_lines else num_lines
                ansi_escape = re.compile(r'\x1b[^m]*m')  # matches ANSI escape characters
                max_line_len = max(len(ansi_escape.sub('', line.strip())) for line in open(name, 'r')) \
                    if not max_line_len else max_line_len

                logging.debug('Total nr of lines: %s' % num_lines)
                logging.debug('Max line length: %s' % max_line_len)

                if not options.WATCH:
                    cat_command = 'clear;cat %s' % name
                    NOT_FOUND = subprocess.call(cat_command, stdout=stdout, stderr=stdout, shell=True)
                    break


                # justification for implementation:
                # http://unix.stackexchange.com/questions/47407/cat-line-x-to-line-y-on-a-huge-file
                line_offset = v_stop - v_start
                cat_command = 'clear;tail -n+%s %s | head -n%s' % (v_start, name, line_offset)
                NOT_FOUND = subprocess.call(cat_command, stdout=stdout, stderr=stdout, shell=True)


                while sys.stdin in select.select([sys.stdin], [], [], timeout)[0]:
                    read_char = sys.stdin.read(1)
                    if read_char:
                        logging.debug('Pressed %s' % read_char)
                        break
                else:
                    state = config['term_size']
                    calculate_split_screen_size()
                    new_state = config['term_size']
                    read_char = '\n' if (state == new_state) else 'r'
                    logging.debug("Auto-advancing by pressing <Enter>")
                pressed_char_hex = '%02x' % ord(read_char) # read_char has an initial value that resets the display ('72')
                h_start, h_stop, v_start, v_stop = control_movement(pressed_char_hex, h_start, h_stop, v_start, v_stop)
                os.chdir(QTOPPATH)
                unlink(name)

            if options.TAR:
                add_to_tar(name, config['savepath'])
        except (KeyboardInterrupt, EOFError):
            safe_exit_with_file_close(handle, name, stdout)
        else:
            if options.TAR >= 1:
                add_to_tar(QTOP_LOGFILE, config['savepath'])


