#!/usr/bin/env python

################################################
#              qtop v.0.8.1                    #
#     Licensed under MIT-GPL licenses          #
#                     Sotiris Fragkiskos       #
#                     Fotis Georgatos          #
################################################

from operator import itemgetter
import datetime
from itertools import izip, izip_longest
import subprocess
import os
try:
    from collections import OrderedDict
except ImportError:
    from ordereddict import OrderedDict
from signal import signal, SIGPIPE, SIG_DFL
# modules
from constants import *
import common_module
from common_module import logging, options
from plugin_pbs import *
from plugin_oar import *
from plugin_sge import *
from stat_maker import *
from math import ceil
from colormap import color_of_account, code_of_color
from yaml_parser import read_yaml_natively, fix_config_list, convert_dash_key_in_dict


# TODO make the following work with py files instead of qtop.colormap files
# if not options.COLORFILE:
#     options.COLORFILE = os.path.expandvars('$HOME/qtop/qtop/qtop.colormap')


def colorize(text, pattern='Nothing', color_func=None, bg_color=None):
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
        return "\033[" + '%s%s' % (ansi_color, bg_color) + "m" + text + "\033[0;m" \
            if ((options.COLOR == 'ON') and pattern != 'account_not_colored' and text != ' ') else text


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
            'more than %s*nodes have no jobs assigned' %options.PERCENTAGE or False
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

    re_nodename = r'(^[A-Za-z0-9-]+)(?=\.|$)'
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
        print '=== WARNING: --- Remapping WN names and retrying heuristics... good luck with this... ---'
    print 'PBS report tool. All bugs added by sfranky@gmail.com. Cross fingers now...'
    print 'Please try: watch -d %s/qtop.py -s %s\n' % (QTOPPATH, options.SOURCEDIR)
    print colorize('===> ', '#') + colorize('Job accounting summary', 'Normal') + colorize(' <=== ', '#') + colorize(
        '(Rev: 3000 $) %s WORKDIR = %s' % (datetime.datetime.today(), QTOPPATH), 'account_not_colored')

    print 'Usage Totals:\t%s/%s\t Nodes | %s/%s  Cores |   %s+%s jobs (R + Q) reported by qstat -q' % \
          (cluster_dict['total_wn'] - cluster_dict['offline_down_nodes'],
           cluster_dict['total_wn'],
           cluster_dict['working_cores'],
           cluster_dict['total_cores'],
           int(total_running_jobs),
           int(total_queued_jobs))

    print 'Queues: | ',
    for q in qstatq_list:
        q_name, q_running_jobs, q_queued_jobs = q['queue_name'], q['run'], q['queued']
        account = q_name if q_name in color_of_account else 'account_not_colored'
        print "{qname}: {run} {q}|".format(qname=colorize(q_name, account),
                                     run=colorize(q_running_jobs, account),
                                     q='+ ' + colorize(q_queued_jobs, account) if q_queued_jobs != '0' else ''),
    print '* implies blocked\n'


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


def create_account_jobs_table(user_names, job_states):
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
    return account_jobs_table, id_of_username


def create_job_counts(user_names, job_states, state_abbrevs):
    """
    counting of R, Q, C, W, E attached to each user
    """
    job_counts = dict()
    for value in state_abbrevs.values():
        job_counts[value] = dict()

    for user_name, job_state in zip(user_names, job_states):
        x_of_user = state_abbrevs[job_state]
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


def fill_node_cores_column(state_np_corejob, core_user_map, id_of_username, max_np_range, user_of_job_id):
    """
    Calculates the actual contents of the map by filling in a status string for each CPU line
    state_np_corejob was: [state, np, (core0, job1), (core1, job1), ....]
    will be a dict!
    """
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

    col = [core_user_map[line][-1] for line in core_user_map]

    return core_user_map, col


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


def calc_all_wnid_label_lines(highest_wn):  # (total_wn) in case of multiple cluster_dict['node_subclusters']
    """
    calculates the Worker Node ID number line widths. expressed by hxxxxs in the following form, e.g. for hundreds of nodes:
    '1': [ 00000000... ]
    '2': [ 0000000001111111... ]
    '3': [ 12345678901234567....]
    where list contents are strings: '0', '1' etc
    """
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

    return wn_vert_labels


def find_matrices_width(wn_number, workernode_list, term_columns, DEADWEIGHT=11):
    """
    masking/clipping functionality: if the earliest node number is high (e.g. 130), the first 129 WNs need not show up.
    case 1: wn_number is RemapNr, WNList is WNListRemapped
    case 2: wn_number is BiggestWrittenNode, WNList is WNList
    DEADWEIGHT is the space taken by the {__XXXX__} labels on the right of the CoreX map
    """
    start = 0
    # exclude unneeded first empty nodes from the matrix
    if options.NOMASKING and \
        min(workernode_list) > int(config['workernodes_matrix'][0]['wn id lines']['min_masking_threshold']):
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
        return start, stop, wn_number / USER_CUT_MATRIX_WIDTH
    elif extra_matrices_nr:  # if more matrices are needed due to lack of space, cut every matrix so that if fits to screen
        stop = start + term_columns - DEADWEIGHT
        return start, stop, extra_matrices_nr
    else:  # just one matrix, small cluster!
        stop = start + wn_number
        return start, stop, 0


def print_wnid_lines(start, stop, highest_wn, wn_vert_labels, **kwargs):
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
        end_label = iter(end_labels[str(node_str_width)])
        display_wnid_lines(d, start, stop, end_label, color_func=color_plainly, args=('White', 'Gray_L', start > 0))
        # start > 0 is just a test for a possible future condition

    elif NAMED_WNS or options.FORCE_NAMES:  # names (e.g. fruits) instead of numbered WNs
        node_str_width = len(wn_vert_labels)  # key, nr of horizontal lines to be displayed

        # for longer full-labeled wn ids, add more end-labels (far-right) towards the bottom
        for num in range(8, len(wn_vert_labels) + 1):
            end_labels.setdefault(str(num), end_labels['7'] + num * ['={___ID___}'])

        end_label = iter(end_labels[str(node_str_width)])
        display_wnid_lines(wn_vert_labels, start, stop, end_label,
                           color_func=highlight_alternately, args=(ALT_LABEL_HIGHLIGHT_COLORS))


def display_wnid_lines(d, start, stop, end_label, color_func, args):
    for line_nr in d:
        color = color_func(*args)
        wn_id_str = insert_separators(d[line_nr][start:stop], SEPARATOR, options.WN_COLON)
        wn_id_str = ''.join([colorize(elem, _, color.next()) for elem in wn_id_str])
        print wn_id_str + end_label.next()


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


def is_matrix_coreless(core_user_map, print_char_start, print_char_stop):
    lines = []
    for ind, k in enumerate(core_user_map):
        cpu_core_line = core_user_map['Core' + str(ind) + 'line'][print_char_start:print_char_stop]
        if options.REM_EMPTY_CORELINES and \
            (
                ('#' * (print_char_stop - print_char_start) == cpu_core_line) or \
                ('#' * (len(cpu_core_line)) == cpu_core_line)
            ):
            lines.append('*')

    return len(lines) == len(core_user_map)


def display_remaining_matrices(
        extra_matrices_nr,
        cluster_dict,
        core_user_map,
        print_char_stop,
        pattern_of_id,
        wn_vert_labels,
        term_columns,
        workernodes_occupancy,
        DEADWEIGHT=11):
    """
    If the WNs are more than a screenful (width-wise), this calculates the extra matrices needed to display them.
    DEADWEIGHT is the space taken by the {__XXXX__} labels on the right of the CoreX map

    if the first matrix has e.g. 10 machines with 64 cores,
    and the remaining 190 machines have 8 cores, this doesn't print the non-existent
    56 cores from the next matrix on.
    """
    # need node_state, temp
    for matrix in range(extra_matrices_nr):
        print_char_start = print_char_stop
        if USER_CUT_MATRIX_WIDTH:
            print_char_stop += USER_CUT_MATRIX_WIDTH
        else:
            print_char_stop += term_columns - DEADWEIGHT
        print_char_stop = min(print_char_stop, cluster_dict['total_wn']) \
            if options.REMAP else min(print_char_stop, cluster_dict['highest_wn'])

        if is_matrix_coreless(core_user_map, print_char_start, print_char_stop):
            continue

        display_selected_occupancy_parts(print_char_start,
            print_char_stop,
            wn_vert_labels,
            core_user_map,
            pattern_of_id,
            workernodes_occupancy)

        print


def display_selected_occupancy_parts(
        print_char_start,
        print_char_stop,
        wn_vert_labels,
        core_user_map,
        pattern_of_id,
        workernodes_occupancy):
    """
    occupancy_parts needs to be redefined for each matrix, because of changed parameter values
    """
    occupancy_parts = {
        'wn id lines':
            (
                print_wnid_lines,
                (print_char_start, print_char_stop, cluster_dict['highest_wn'], wn_vert_labels),
                {'inner_attrs': None}
            ),
        'core user map':
            (
                print_core_lines,
                (core_user_map, print_char_start, print_char_stop, pattern_of_id),
                {'attrs': None}
            ),
    }

    # custom part
    for yaml_key, part_name in get_yaml_key_part('workernodes_matrix'):
        new_occupancy_part = {
            part_name:
                (
                    print_mult_attr_line,  # func
                    (print_char_start, print_char_stop),  # args
                    {'attr_lines': workernodes_occupancy[part_name]}  # kwargs
                )
        }
        occupancy_parts.update(new_occupancy_part)

    for part_dict in config['workernodes_matrix']:
        part = [k for k in part_dict][0]
        occupancy_parts[part][2].update(part_dict[part])  # get extra options from user
        fn, args, kwargs = occupancy_parts[part][0], occupancy_parts[part][1], occupancy_parts[part][2]
        fn(*args, **kwargs)

    print


def print_mult_attr_line(print_char_start, print_char_stop, attr_lines, label, color_func=None, **kwargs):  # NEW!
    """
    attr_lines can be e.g. Node state lines
    """
    # TODO: fix option parameter, inserted for testing purposes
    for line in attr_lines:
        line = attr_lines[line][print_char_start:print_char_stop]
        # TODO: maybe put attr_line and label as kwd arguments? collect them as **kwargs
        attr_line = insert_separators(line, SEPARATOR, options.WN_COLON) + '=%s'  % label  # this didnt work as expected
        attr_line = ''.join([colorize(char, 'Nothing', color_func) for char in attr_line])
        print attr_line


def display_user_accounts_pool_mappings(account_jobs_table, pattern_of_id):
    detail_of_name = get_detail_of_name()
    print colorize('\n===> ', '#') + \
          colorize('User accounts and pool mappings', 'Nothing') + \
          colorize(' <=== ', '#') + \
          colorize("  ('all' also includes those in C and W states, as reported by qstat)", '#')

    print 'id|    R +    Q /  all |    unix account | Grid certificate DN (info only available under elevated privileges)'
    for line in account_jobs_table:
        uid, runningjobs, queuedjobs, alljobs, user = line[0], line[1], line[2], line[3], line[4]
        account = pattern_of_id[uid]
        if options.COLOR == 'OFF' or account == 'account_not_colored' or color_of_account[account] == 'reset':
            extra_width = 0
            account = 'account_not_colored'
        else:
            extra_width = 12
        print_string = '{0:<{width2}}{sep} ' \
                       '{1:>{width4}} + {2:>{width4}} / {3:>{width4}} {sep} ' \
                       '{4:>{width15}} {sep} ' \
                       '{5:>{width40}} {sep}'.format(
            colorize(str(uid), account),
            colorize(str(runningjobs), account),
            colorize(str(queuedjobs), account),
            colorize(str(alljobs), account),
            colorize(user, account),
            colorize(detail_of_name.get(user, ''), account),
            sep=colorize(SEPARATOR, account),
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
        cpu_core_line = insert_separators(cpu_core_line, SEPARATOR, options.WN_COLON)
        cpu_core_line = ''.join([colorize(elem, pattern_of_id[elem]) for elem in cpu_core_line if elem in pattern_of_id])
        yield cpu_core_line + colorize('=Core' + str(ind), 'account_not_colored')


def calc_core_userid_matrix(cluster_dict, id_of_username, job_ids, user_names):
    _core_user_map = OrderedDict()
    max_np_range = [str(x) for x in range(cluster_dict['max_np'])]
    user_of_job_id = dict(izip(job_ids, user_names))

    for core_nr in max_np_range:
        _core_user_map['Core%sline' % str(core_nr)] = []  # Cpu0line, Cpu1line, Cpu2line, .. = '','','', ..

    for _node in cluster_dict['workernode_dict']:
        state_np_corejob = cluster_dict['workernode_dict'][_node]
        _core_user_map, cluster_dict['workernode_dict'][_node]['core_user_column'] = fill_node_cores_column(state_np_corejob, _core_user_map, id_of_username, max_np_range, user_of_job_id)

    for coreline in _core_user_map:
        _core_user_map[coreline] = ''.join(_core_user_map[coreline])

    return _core_user_map


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
        # is this really needed?: cluster_dict['workernode_dict'][_node]['state_column']

    for line, attr_line in enumerate(multiline_map, 1):
        multiline_map[attr_line] = ''.join(multiline_map[attr_line])
        if line == user_max_len:
            break

    return multiline_map


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
    # TODO: make this readable !!!!!
    wns_occupancy = dict()
    wns_occupancy['term_columns'] = calculate_split_screen_size()
    wns_occupancy['account_jobs_table'], wns_occupancy['id_of_username'] = create_account_jobs_table(user_names, job_states)
    wns_occupancy['pattern_of_id'] = make_pattern_of_id(wns_occupancy['account_jobs_table'])
    wns_occupancy['print_char_start'], wns_occupancy['print_char_stop'], wns_occupancy['extra_matrices_nr'] = \
        find_matrices_width(cluster_dict['highest_wn'], cluster_dict['workernode_list'], wns_occupancy['term_columns'])

    wns_occupancy['wn_vert_labels'] = calc_all_wnid_label_lines(cluster_dict['highest_wn'])

    # For loop below only for user-inserted/customizeable values.
    # e.g. wns_occupancy['node_state'] = ...workernode_dict[node]['state'] for node in workernode_dict...
    for yaml_key, part_name in get_yaml_key_part('workernodes_matrix'):
        wns_occupancy[part_name] = calc_general_multiline_attr(cluster_dict, part_name, yaml_key)  # now gives map instead of single line str
        # was: wns_occupancy[part_name] = ''.join([str(cluster_dict['workernode_dict'][node][yaml_key]) for node in cluster_dict['workernode_dict']])

    wns_occupancy['core user map'] = calc_core_userid_matrix(cluster_dict, wns_occupancy['id_of_username'], job_ids, user_names)
    return wns_occupancy, cluster_dict


def print_core_lines(core_user_map, print_char_start, print_char_stop, pattern_of_id, attrs, options1, options2):
    signal(SIGPIPE, SIG_DFL)
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

    print_char_start = workernodes_occupancy['print_char_start']
    print_char_stop = workernodes_occupancy['print_char_stop']
    wn_vert_labels = workernodes_occupancy['wn_vert_labels']
    core_user_map = workernodes_occupancy['core user map']
    extra_matrices_nr = workernodes_occupancy['extra_matrices_nr']
    term_columns = workernodes_occupancy['term_columns']
    pattern_of_id = workernodes_occupancy['pattern_of_id']

    print colorize('===> ', '#') + colorize('Worker Nodes occupancy', 'Nothing') + colorize(' <=== ', '#') + colorize(
        '(you can read vertically the node IDs; nodes in free state are noted with - )', 'account_not_colored')

    if not is_matrix_coreless(core_user_map, print_char_start, print_char_stop):
        display_selected_occupancy_parts(
        print_char_start,
        print_char_stop,
        wn_vert_labels,
        core_user_map,
        pattern_of_id,
        workernodes_occupancy
        )

    display_remaining_matrices(
        extra_matrices_nr,
        cluster_dict,
        core_user_map,
        print_char_stop,
        pattern_of_id,
        wn_vert_labels,
        term_columns,
        workernodes_occupancy
    )


def make_pattern_of_id(account_jobs_table):
    """
    First strips the numbers off of the unix accounts and tries to match this against the given color table in colormap.
    Additionally, it will try to apply the regex rules given by the user in qtopconf.yaml, overriding the colormap.
    The last matched regex is valid.
    If no matching was possible, there will be no coloring applied.
    """
    pattern_of_id = {}
    for line in account_jobs_table:
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
    return pattern_of_id


def load_yaml_config():
    """
    Loads ./QTOPCONF_YAML into a dictionary and then tries to update the dictionary
    with the same-named conf file found in:
    /env
    $HOME/.local/qtop/
    in that order.
    """
    config = read_yaml_natively(os.path.join(QTOPPATH, QTOPCONF_YAML))
    logging.info('Default configuration dictionary loaded. Length: %s items' % len(config))
    # try:
    #     config = yaml.safe_load(open(os.path.join(path + "/qtopconf.yaml")))
    # except ImportError:
    #     config = read_yaml_natively(os.path.join(path + "/qtopconf.yaml"))
    # except yaml.YAMLError, exc:
    #     if hasattr(exc, 'problem_mark'):
    #         mark = exc.problem_mark
    #         print "Your YAML configuration file has an error in position: (%s:%s)" % (mark.line + 1, mark.column + 1)
    #         print "Please make sure that spaces are multiples of 2."
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

    user_selected_save_path = os.path.realpath(os.path.expandvars(config['savepath']))
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
        _, term_columns = config['term_size']
    except ValueError:
        _, term_columns = fix_config_list(config['term_size'])
    except KeyError:
        try:
            _, term_columns = os.popen('stty size', 'r').read().split()
        except ValueError:
            logging.warn("Failed to autodetect your terminal's size or read it from %s. "
                             "Using term_size: %s" % QTOPCONF_YAML, fallback_term_size)
            config['term_size'] = fallback_term_size
            _, term_columns = config['term_size']
    else:
        logging.debug('Detected terminal size is: %s * %s' % (_, term_columns))
    finally:
        term_columns = int(term_columns)
    return term_columns


def sort_batch_nodes(batch_nodes):
    try:
        batch_nodes.sort(key=eval(config['sorting']['user_sort']), reverse=eval(config['sorting']['reverse']))
    except IndexError:
        logging.critical("There's (probably) something wrong in your sorting lambda in %s." % QTOPCONF_YAML)
        raise


def filter_list_out(batch_nodes, _list=None):
    if not _list:
        _list = []
    for idx, node in enumerate(batch_nodes):
        if idx in _list:
            node['mark'] = '*'
    batch_nodes = filter(lambda item: not item.get('mark'), batch_nodes)
    return batch_nodes


def filter_list_out_by_name(batch_nodes, _list=None):
    if not _list:
        _list = []
    for idx, node in enumerate(batch_nodes):
        if node['domainname'].split('.', 1)[0] in _list:
            node['mark'] = '*'
    batch_nodes = filter(lambda item: not item.get('mark'), batch_nodes)
    return batch_nodes


def filter_list_out_by_node_state(batch_nodes, _list=None):
    if not _list:
        _list = []
    for idx, node in enumerate(batch_nodes):
        if node['state'] in _list:
            node['mark'] = '*'
    batch_nodes = filter(lambda item: not item.get('mark'), batch_nodes)
    return batch_nodes


def filter_list_out_by_name_pattern(batch_nodes, _list=None):
    if not _list:
        _list = []
    for idx, node in enumerate(batch_nodes):
        for pattern in _list:
            match = re.search(eval(pattern), node['domainname'].split('.', 1)[0])
            try:
                match.group(0)
            except AttributeError:
                pass
            else:
                node['mark'] = '*'
    batch_nodes = filter(lambda item: not item.get('mark'), batch_nodes)
    return batch_nodes


def filter_batch_nodes(batch_nodes, filter_rules=None):
    """
    Filters specific nodes according to the filter rules in QTOPCONF_YAML
    """

    filter_types = {
        'list_out': filter_list_out,
        'list_out_by_name': filter_list_out_by_name,
        'list_out_by_name_pattern': filter_list_out_by_name_pattern,
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


def convert_to_yaml(scheduler, INPUT_FNs_commands, filenames, write_method, commands):
    for _file in INPUT_FNs_commands:
        file_orig, file_out = filenames[_file], filenames[_file + '_out']
        _func = commands[_file]
        logging.debug('Executing %(func)s \n\t'
                      'on: %(file_orig)s,\n\t' % {"func": _func.__name__, "file_orig": file_orig, "file_out": file_out})
        # 'resulting in: %(file_out)s\n'
        _func(file_orig, file_out, write_method)


def exec_func_tuples(func_tuples):
    _commands = iter(func_tuples)
    for command in _commands:
        ffunc, args, kwargs = command[0], command[1], command[2]
        logging.debug('Executing %s' % ffunc.__name__)
        yield ffunc(*args, **kwargs)


def get_yaml_reader(scheduler):
    if scheduler == 'pbs':
        yaml_reader = [
            (read_pbsnodes_yaml, (filenames.get('pbsnodes_file_out'),), {'write_method': options.write_method}),
            (common_module.read_qstat_yaml, (filenames.get('qstat_file_out'),), {'write_method': options.write_method}),
            (read_qstatq_yaml, (filenames.get('qstatq_file_out'),), {'write_method': options.write_method}),
        ]
    elif scheduler == 'oar':
        yaml_reader = [
            (read_oarnodes_yaml, ([filenames.get('oarnodes_s_file'), filenames.get('oarnodes_y_file')]), {'write_method': options.write_method}),
            (common_module.read_qstat_yaml, ([filenames.get('oarstat_file_out')]), {'write_method': options.write_method}),
            (lambda *args, **kwargs: (0, 0, 0), ([filenames.get('oarstat_file')]), {'write_method': options.write_method}),
        ]
    elif scheduler == 'sge':
        yaml_reader = [
            (get_worker_nodes, ([filenames.get('sge_file_stat')]), {'write_method': options.write_method}),
            (common_module.read_qstat_yaml, ([SGEStatMaker.temp_filepath]), {'write_method': options.write_method}),
            # (lambda *args, **kwargs: (0, 0, 0), ([filenames.get('sge_file_stat')]), {'write_method': options.write_method}),
            (get_statq_from_xml, ([filenames.get('sge_file_stat')]), {'write_method': options.write_method}),
        ]
    return yaml_reader


def get_filenames_commands():
    d = dict()
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


def get_detail_of_name():
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

    passwd_command = extract_info.get('sourcefile').split()
    p = subprocess.Popen(passwd_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    output, err = p.communicate("something here")
    if 'No such file or directory' in err:
        logging.error('You have to set a proper command to get the passwd file in your %s file.' % QTOPCONF_YAML)

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


if __name__ == '__main__':

    initial_cwd = os.getcwd()
    logging.debug('Initial qtop directory: %s' % initial_cwd)
    print "Log file created in %s" % os.path.expandvars(QTOP_LOGFILE)
    CURPATH = os.path.expanduser(initial_cwd)  # ex QTOPPATH, will not work if qtop is executed from within a different dir
    QTOPPATH = os.path.dirname(sys.argv[0])  # dir where qtop resides
    config = load_yaml_config()

    SEPARATOR = config['workernodes_matrix'][0]['wn id lines']['separator'].translate(None, "'")  # alias
    USER_CUT_MATRIX_WIDTH = int(config['workernodes_matrix'][0]['wn id lines']['user_cut_matrix_width'])  # alias
    # TODO: int should be handled internally in native yaml parser
    ALT_LABEL_HIGHLIGHT_COLORS = fix_config_list(config['workernodes_matrix'][0]['wn id lines']['alt_label_highlight_colors'])
    # TODO: fix_config_list should be handled internally in native yaml parser

    options.SOURCEDIR = os.path.realpath(options.SOURCEDIR) if options.SOURCEDIR else None
    logging.debug("User-defined source directory: %s" % options.SOURCEDIR)
    options.workdir = options.SOURCEDIR or config['savepath']
    logging.debug('Working directory is now: %s' % options.workdir)
    os.chdir(options.workdir)

    scheduler = pick_batch_system()

    if config['faster_xml_parsing']:
        try:
            from lxml import etree
        except ImportError:
            logging.warn('Module lxml is missing. Try issuing "pip install lxml". Reverting to xml module.')
            from xml.etree import ElementTree as etree

    INPUT_FNs_commands = get_filenames_commands()
    parser_extension_mapping = {'txtyaml': 'yaml', 'json': 'json'}  # 'yaml': 'yaml',
    ext = parser_extension_mapping[options.write_method]
    logging.info('Selected method for storing data structures is: %s' % ext)
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
            ext=ext
        )

    yaml_converter = {
        'pbs': {
            'pbsnodes_file': make_pbsnodes,
            'qstatq_file': QStatMaker(config).make_statq,
            'qstat_file': QStatMaker(config).make_stat,
        },
        'oar': {
            'oarnodes_s_file': lambda x, y, z: None,
            'oarnodes_y_file': lambda x, y, z: None,
            'oarstat_file': OarStatMaker(config).make_stat,
        },
        'sge': {
            'sge_file_stat': SGEStatMaker(config).make_stat,
        }
    }
    commands = yaml_converter[scheduler]
    # reset_yaml_files()  # either that or having a pid appended in the filename
    if not options.YAML_EXISTS:
        convert_to_yaml(scheduler, INPUT_FNs_commands, filenames, options.write_method, commands)

    func_tuples = get_yaml_reader(scheduler)
    commands = exec_func_tuples(func_tuples)

    worker_nodes = next(commands)
    job_ids, user_names, job_states, _ = next(commands)
    total_running_jobs, total_queued_jobs, qstatq_lod = next(commands)

    #  MAIN ##################################
    logging.info('CALCULATION AREA')
    cluster_dict, NAMED_WNS = calculate_cluster(worker_nodes)
    workernodes_occupancy, cluster_dict = calculate_wn_occupancy(cluster_dict, user_names, job_states, job_ids)

    display_parts = {
        'job_accounting_summary': (display_job_accounting_summary, (cluster_dict, total_running_jobs, total_queued_jobs, qstatq_lod)),
        'workernodes_matrix': (display_wn_occupancy, (workernodes_occupancy, cluster_dict)),
        'user_accounts_pool_mappings': (display_user_accounts_pool_mappings, (workernodes_occupancy['account_jobs_table'], workernodes_occupancy['pattern_of_id']))
    }
    logging.info('DISPLAY AREA')
    sections_off = {
        1: options.sect_1_off,
        2: options.sect_2_off,
        3: options.sect_3_off
    }
    for idx, part in enumerate(config['user_display_parts'], 1):
        _func, args = display_parts[part][0], display_parts[part][1]
        _func(*args) if not sections_off[idx] else None

    # print '\nThanks for watching!'
    os.chdir(QTOPPATH)
