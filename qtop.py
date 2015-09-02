#!/usr/bin/env python

################################################
#              qtop v.0.7.0                    #
#     Licensed under MIT-GPL licenses          #
#                     Sotiris Fragkiskos       #
#                     Fotis Georgatos          #
################################################

from operator import itemgetter
from optparse import OptionParser
import datetime
from collections import Counter, OrderedDict
import os
import re
import yaml
from itertools import izip
# modules
from pbs import *
from math import ceil
from colormap import color_of_account, code_of_color

parser = OptionParser()  # for more details see http://docs.python.org/library/optparse.html
parser.add_option("-a", "--blindremapping", action="store_true", dest="BLINDREMAP", default=False,
                  help="This is used in situations where node names are not a pure arithmetic seq (eg. rocks clusters)")
parser.add_option("-c", "--NOCOLOR", action="store_true", dest="NOCOLOR", default=False,
                  help="Enable/Disable color in qtop output.")
parser.add_option("-f", "--setCOLORMAPFILE", action="store", type="string", dest="COLORFILE")
parser.add_option("-m", "--noMasking", action="store_true", dest="NOMASKING", default=False,
                  help="Don't mask early empty WNs (default: if the first 30 WNs are unused, counting starts from 31).")
parser.add_option("-o", "--SetVerticalSeparatorXX", action="store", dest="WN_COLON", default=0,
                  help="Put vertical bar every WN_COLON nodes.")
parser.add_option("-s", "--SetSourceDir", dest="SOURCEDIR",
                  help="Set the source directory where pbsnodes and qstat reside")
parser.add_option("-z", "--quiet", action="store_false", dest="verbose", default=True,
                  help="don't print status messages to stdout. Not doing anything at the moment.")
parser.add_option("-F", "--ForceNames", action="store_true", dest="FORCE_NAMES", default=False,
                  help="force names to show up instead of numbered WNs even for very small numbers of WNs")
parser.add_option("-w", "--writemethod", dest="write_method", action="store", default="txtyaml",
                  choices=['txtyaml', 'yaml', 'json'],
                  help="Set the method used for dumping information, json, yaml, or native python (yaml format)")
(options, args) = parser.parse_args()


# TODO make the following work with py files instead of qtop.colormap files
# if not options.COLORFILE:
#     options.COLORFILE = os.path.expanduser('~/qtop/qtop/qtop.colormap')


def colorize(text, pattern, color=None):
    """
    prints text coloured according to a unix account pattern color.
    """
    try:
        color_code = code_of_color[color] if color else code_of_color[color_of_account[pattern]]
    except KeyError:
        return text
    else:
        return "\033[" + color_code + "m" + text + "\033[1;m" \
            if ((not options.NOCOLOR) and pattern != 'account_not_coloured' and text != ' ') else text


def decide_remapping(node_dict):
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
    if options.BLINDREMAP or \
                    len(node_dict['node_subclusters']) > 1 or \
                    min(node_dict['wn_list']) >= 9000 or \
                    node_dict['offline_down_nodes'] >= node_dict['total_wn'] * config['percentage'] or \
                    len(node_dict['_all_str_digits_with_empties']) != len(node_dict['all_str_digits']) or \
                    len(node_dict['all_digits']) != len(node_dict['all_str_digits']):
        options.REMAP = True
    else:
        options.REMAP = False
        # max(node_dict['wn_list']) was node_dict['highest_wn']


def calculate_stuff(pbs_nodes):
    NAMED_WNS = 0 if not options.FORCE_NAMES else 1
    node_dict = dict()
    for key in ['working_cores', 'total_cores', 'max_np', 'highest_wn', 'offline_down_nodes']:
        node_dict[key] = 0
    node_dict['node_subclusters'] = set()
    node_dict['wn_dict'] = {}
    node_dict['wn_dict_remapped'] = {}  # { remapnr: [state, np, (core0, job1), (core1, job1), ....]}

    node_dict['total_wn'] = len(pbs_nodes)  # == existing_nodes
    node_dict['wn_list'] = []
    node_dict['wn_list_remapped'] = range(1, node_dict['total_wn'])  # leave xrange aside for now

    _all_letters = []
    _all_str_digits_with_empties = []

    re_nodename = r'(^[A-Za-z0-9-]+)(?=\.|$)'
    for node in pbs_nodes:

        nodename_match = re.search(re_nodename, node['domainname'])
        _nodename = nodename_match.group(0)

        node_letters = ''.join(re.findall(r'\D+', _nodename))
        node_str_digits = "".join(re.findall(r'\d+', _nodename))

        _all_letters.append(node_letters)
        _all_str_digits_with_empties.append(node_str_digits)

        node_dict['total_cores'] += int(node.get('np'))
        node_dict['max_np'] = max(node_dict['max_np'], int(node['np']))
        node_dict['offline_down_nodes'] += 1 if node['state'] in 'do' else 0
        try:
            node_dict['working_cores'] += len(node['core_job_map'])
        except KeyError:
            pass

        try:
            cur_node_nr = int(node_str_digits)
        except ValueError:
            cur_node_nr = _nodename
        finally:
            node_dict['wn_list'].append(cur_node_nr)

    node_dict['node_subclusters'] = set(_all_letters)
    node_dict['_all_str_digits_with_empties'] = _all_str_digits_with_empties
    node_dict['all_str_digits'] = filter(lambda x: x != "", _all_str_digits_with_empties)
    node_dict['all_digits'] = [int(digit) for digit in node_dict['all_str_digits']]

    decide_remapping(node_dict)
    map_pbsnodes_to_wn_dicts(node_dict, pbs_nodes)
    if options.REMAP:
        node_dict['highest_wn'] = node_dict['total_wn']
        node_dict['wn_list'] = node_dict['wn_list_remapped']
        node_dict['wn_dict'] = node_dict['wn_dict_remapped']
    else:
        node_dict['highest_wn'] = max(node_dict['wn_list'])

    # fill in non-existent WN nodes (absent from pbsnodes file) with '?' and count them
    for i in range(1, node_dict['highest_wn'] + 1):
        if i not in node_dict['wn_dict']:
            node_dict['wn_dict'][i] = {'state': '?', 'np': 0, 'domainname': 'N/A', 'host': 'N/A'}

    # custom hostnames (for the wn id label lines)
    for _, state_corejob_dn in node_dict['wn_dict'].items():
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
            label_max_len = config['wn_labels_max_len']
            state_corejob_dn['host'] = label_max_len and state_corejob_dn['host'][-label_max_len:] or state_corejob_dn['host']

    return node_dict, NAMED_WNS


def nodes_with_jobs(pbs_nodes):
    for _, pbs_node in pbs_nodes.iteritems():
        if 'core_job_map' in pbs_node:
            yield pbs_node


def display_job_accounting_summary(node_dict, total_running, total_queued, qstatq_list):
    if options.REMAP:
        print '=== WARNING: --- Remapping WN names and retrying heuristics... good luck with this... ---'
    print '\nPBS report tool. Please try: watch -d ' + QTOPPATH + \
          '. All bugs added by sfranky@gmail.com. Cross fingers now...\n'
    print colorize('===> ', '#') + colorize('Job accounting summary', 'Nothing') + colorize(' <=== ', '#') + colorize(
        '(Rev: 3000 $) %s WORKDIR = to be added', 'account_not_coloured') % (datetime.datetime.today())
    print 'Usage Totals:\t%s/%s\t Nodes | %s/%s  Cores |   %s+%s jobs (R + Q) reported by qstat -q' % \
          (node_dict['total_wn'] - node_dict['offline_down_nodes'],
           node_dict['total_wn'],
           node_dict['working_cores'],
           node_dict['total_cores'],
           int(total_running),
           int(total_queued))
    print 'Queues: | ',
    for q in qstatq_list:
        q_name, q_running_jobs, q_queued_jobs = q['queue_name'], q['run'], q['queued']
        account = q_name if q_name in color_of_account else 'account_not_coloured'
        print "{}: {} + {} |".format(colorize(q_name, account),
                                     colorize(q_running_jobs, account),
                                     colorize(q_queued_jobs, account)),
    print '* implies blocked\n'


def calculate_job_counts(user_names, job_states):
    """
    Calculates and prints what is actually below the id|  R + Q /all | unix account etc line
    :param user_names: list
    :param job_states: list
    :return: (list, list, dict)
    """
    expand_useraccounts_symbols(config, user_names)
    state_abbrevs = {'R': 'running_of_user',
                     'Q': 'queued_of_user',
                     'C': 'cancelled_of_user',
                     'W': 'waiting_of_user',
                     'E': 'exiting_of_user'}

    job_counts = create_job_counts(user_names, job_states, state_abbrevs)
    user_alljobs_sorted_lot = produce_user_lot(user_names)

    id_of_username = {}
    for _id, user_allcount in enumerate(user_alljobs_sorted_lot):
        id_of_username[user_allcount[0]] = config['possible_ids'][_id]

    # Calculates and prints what is actually below the id|  R + Q /all | unix account etc line
    # this is slower but shorter: 8mus
    for state_abbrev in state_abbrevs:
        _xjobs_of_user = job_counts[state_abbrevs[state_abbrev]]
        missing_uids = set(id_of_username).difference(_xjobs_of_user)
        [_xjobs_of_user.setdefault(missing_uid, 0) for missing_uid in missing_uids]

    # This is actually faster: 6 mus
    # for uid in id_of_username:
    #     if uid not in job_counts['running_of_user']:
    #         job_counts['running_of_user'][uid] = 0  # 4
    #     if uid not in job_counts['queued_of_user']:
    #         job_counts['queued_of_user'][uid] = 0  # 4
    #     if uid not in job_counts['cancelled_of_user']:
    #         job_counts['cancelled_of_user'][uid] = 0  # 20
    #     if uid not in job_counts['waiting_of_user']:
    #         job_counts['waiting_of_user'][uid] = 0  # 19
    #     if uid not in job_counts['exiting_of_user']:
    #         job_counts['exiting_of_user'][uid] = 0  # 20
    return job_counts, user_alljobs_sorted_lot, id_of_username


def create_account_jobs_table(user_names, job_states):
    job_counts, user_alljobs_sorted_lot, id_of_username = calculate_job_counts(user_names, job_states)
    account_jobs_table = []
    for user_alljobs in user_alljobs_sorted_lot:
        user, alljobs_of_user = user_alljobs
        account_jobs_table.append(
            [id_of_username[user],
             job_counts['running_of_user'][user],
             job_counts['queued_of_user'][user],
             alljobs_of_user,
             user]
        )
    account_jobs_table.sort(key=itemgetter(3, 4), reverse=True)  # sort by All jobs, then unix account
    # unix account id needs to be recomputed at this point. Should fix later.
    for quintuplet, new_uid in zip(account_jobs_table, config['possible_ids']):
        unix_account = quintuplet[-1]
        quintuplet[0] = id_of_username[unix_account] = new_uid
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
        job_counts['queued_of_user'].setdefault(user_name, 0)
        job_counts['cancelled_of_user'].setdefault(user_name, 0)
        job_counts['waiting_of_user'].setdefault(user_name, 0)
        job_counts['exiting_of_user'].setdefault(user_name, 0)

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
    MAX_UNIX_ACCOUNTS = 87  # was : 62
    if len(user_list) > MAX_UNIX_ACCOUNTS:
        for i in xrange(MAX_UNIX_ACCOUNTS, len(
                user_list) + MAX_UNIX_ACCOUNTS):  # was: # for i in xrange(MAX_UNIX_ACCOUNTS, len(user_list) + MAX_UNIX_ACCOUNTS):
            config['possible_ids'].append(str(i)[0])


def fill_cpucore_columns(state_np_corejob, cpu_core_dict, id_of_username, max_np_range, user_of_job_id):
    """
    Calculates the actual contents of the map by filling in a status string for each CPU line
    state_np_corejob was: [state, np, (core0, job1), (core1, job1), ....]
    will be a dict!
    """
    state = state_np_corejob['state']
    np = state_np_corejob['np']
    corejobs = state_np_corejob.get('core_job_map', '')

    if state == '?':
        for cpu_line in cpu_core_dict:
            cpu_core_dict[cpu_line] += '_'
    else:
        _own_np = int(np)
        own_np_range = [str(x) for x in range(_own_np)]
        own_np_empty_range = own_np_range[:]

        for corejob in corejobs:
            core, job = str(corejob['core']), str(corejob['job'])
            try:
                user_of_job_id[job]
            except KeyError, KeyErrorValue:
                print 'There seems to be a problem with the qstat output. A JobID has gone rogue (namely, ' + str(
                    KeyErrorValue) + '). Please check with the System Administrator.'
            cpu_core_dict['Cpu' + str(core) + 'line'] += str(id_of_username[user_of_job_id[job]])
            own_np_empty_range.remove(core)

        non_existent_cores = [item for item in max_np_range if item not in own_np_range]

        '''
        the height of the matrix is determined by the highest-core WN existing. If other WNs have less cores,
        these positions are filled with '#'s.
        '''
        for core in own_np_empty_range:
            cpu_core_dict['Cpu' + str(core) + 'line'] += '_'
        for core in non_existent_cores:
            cpu_core_dict['Cpu' + str(core) + 'line'] += '#'
    return cpu_core_dict


def line_with_separators(orig_str, separator, pos, stopaftern=0):
    """
    inserts separator into orig_str every pos-th position, optionally stopping after stopaftern times.
    """
    pos = int(pos)
    if pos:  # default value is zero, means no vertical separators
        sep = orig_str[:]  # insert initial vertical separator

        times = len(orig_str) / pos if not stopaftern else stopaftern
        sep = sep[:pos] + separator + sep[pos:]
        for i in range(2, times + 1):
            sep = sep[:pos * i + i - 1] + separator + sep[pos * i + i - 1:]
        sep += separator  # insert initial vertical separator
        return sep
    else:  # no separators
        return orig_str


def calc_all_wnid_label_lines(highest_wn):  # (total_wn) in case of multiple node_dict['node_subclusters']
    """
    calculates the Worker Node ID number line widths. expressed by hxxxxs in the following form, e.g. for hundreds of nodes:
    '1': [ 00000000... ]
    '2': [ 0000000001111111... ]
    '3': [ 12345678901234567....]
    where list contents are strings: '0', '1' etc
    """
    if NAMED_WNS or options.FORCE_NAMES:
        wn_dict = node_dict['wn_dict']
        hosts = [state_corejob_dn['host'] for _, state_corejob_dn in wn_dict.items()]
        node_str_width = len(max(hosts, key=len))
        wn_vert_labels = OrderedDict((str(place), []) for place in range(1, node_str_width + 1))
        for node in wn_dict:
            host = wn_dict[node]['host']
            extra_zeros = node_str_width - len(host)
            string = "".join(" " * extra_zeros + host)
            for place in range(node_str_width):
                wn_vert_labels[str(place + 1)].append(string[place])
    else:
        node_str_width = len(str(highest_wn))  # 4
        wn_vert_labels = {str(place): [] for place in range(1, node_str_width + 1)}
        for nr in range(1, highest_wn + 1):
            extra_zeros = node_str_width - len(str(nr))  # 4 - 1 = 3, for wn0001
            string = "".join("0" * extra_zeros + str(nr))
            for place in range(1, node_str_width + 1):
                wn_vert_labels[str(place)].append(string[place - 1])


    return wn_vert_labels


def find_matrices_width(wn_number, wn_list, term_columns, DEADWEIGHT=11):
    """
    masking/clipping functionality: if the earliest node number is high (e.g. 130), the first 129 WNs need not show up.
    case 1: wn_number is RemapNr, WNList is WNListRemapped
    case 2: wn_number is BiggestWrittenNode, WNList is WNList
    DEADWEIGHT is the space taken by the {__XXXX__} labels on the right of the CoreX map
    """
    start = 0
    # exclude unneeded first empty nodes from the matrix
    if options.NOMASKING and min(wn_list) > config['min_masking_threshold']:
        start = min(wn_list) - 1

    # Extra matrices may be needed if the WNs are more than the screen width can hold.
    if wn_number > start:  # start will either be 1 or (masked >= config['min_masking_threshold'] + 1)
        extra_matrices_nr = int(ceil(abs(wn_number - start) / float(term_columns - DEADWEIGHT))) - 1
    elif options.REMAP:  # was: ***wn_number < start*** and len(node_dict['node_subclusters']) > 1:  # Remapping
        extra_matrices_nr = int(ceil(wn_number / float(term_columns - DEADWEIGHT))) - 1
    else:
        raise (NotImplementedError, "Not foreseen")

    if config['user_cut_matrix_width']:  # if the user defines a custom cut (in the configuration file)
        stop = start + config['user_cut_matrix_width']
        return start, stop, wn_number / config['user_cut_matrix_width']
    elif extra_matrices_nr:  # if more matrices are needed due to lack of space, cut every matrix so that if fits to screen
        stop = start + term_columns - DEADWEIGHT
        return start, stop, extra_matrices_nr
    else:  # just one matrix, small cluster!
        stop = start + wn_number
        return start, stop, 0


def print_wnid_lines(start, stop, highest_wn, wn_vert_labels):
    """
    Prints the Worker Node ID lines, after it colours them and adds separators to them.
    highest_wn determines the number of WN ID lines needed  (1/2/3/4+?)
    """
    d = OrderedDict()
    end_labels = config['end_labels']

    if not NAMED_WNS:  # not used meaningfully yet
        node_str_width = len(str(highest_wn))  # 4 for thousands of nodes

        for node_nr in range(1, node_str_width + 1):
            d[str(node_nr)] = "".join(wn_vert_labels[str(node_nr)])
        size = str(len(d))  # key, nr of horizontal lines to be displayed
        end_label = iter(end_labels[size])
        for line in d:
            print line_with_separators(d[line][start:stop], SEPARATOR, options.WN_COLON) + end_label.next()

    elif NAMED_WNS or options.FORCE_NAMES:  # names (e.g. fruits) instead of numbered WNs

        # for longer full-labeled wn ids, add more end-labels (far-right) towards the bottom
        for num in range(8, len(wn_vert_labels) + 1):
            end_labels.setdefault(str(num), end_labels['7'] + num * ['={___ID___}'])

        size = str(len(wn_vert_labels))  # key, nr of horizontal lines to be displayed
        end_label = iter(end_labels[size])
        for line_nr in wn_vert_labels:
            highlight = highlight_alternately(*config['alt_label_highlight_colours'])
            color_wn_id_list = list(line_with_separators(wn_vert_labels[line_nr][start:stop], SEPARATOR, options.WN_COLON))
            color_wn_id_list = [colorize(elem, _, highlight.next()) for elem in color_wn_id_list]
            line = ''.join(color_wn_id_list)
            print line + end_label.next()


def highlight_alternately(colour_a, colour_b):
    highlight = {0: colour_a, 1: colour_b}  # should obviously be customizable
    selection = 0
    while True:
        selection = 0 if selection else 1
        yield highlight[selection]



def display_remaining_matrices(node_state,
                                 extra_matrices_nr,
                                 node_dict,
                                 cpu_core_dict,
                                 _print_end,
                                 pattern_of_id,
                                 wn_vert_labels,
                                 term_columns,
                                 DEADWEIGHT=11):
    """
    If the WNs are more than a screenful (width-wise), this calculates the extra matrices needed to display them.
    DEADWEIGHT is the space taken by the {__XXXX__} labels on the right of the CoreX map

    if the first matrix has e.g. 10 machines with 64 cores,
    and the remaining 190 machines have 8 cores, this doesn't print the non-existent
    56 cores from the next matrix on.
    """
    gray_hash = colorize('#', _, color='Red_L')  # was: # gray_hash = '\x1b[1;30m#\x1b[1;m'
    separator_between_ansi = '\x1b[0m|\x1b[1;m'

    for matrix in range(extra_matrices_nr):
        print_start = _print_end
        if config['user_cut_matrix_width']:
            _print_end += config['user_cut_matrix_width']
        else:
            _print_end += term_columns - DEADWEIGHT
        _print_end = min(_print_end, node_dict['total_wn']) if options.REMAP else min(_print_end, node_dict['highest_wn'])

        print '\n'
        print_wnid_lines(print_start, _print_end, node_dict['highest_wn'], wn_vert_labels)
        print line_with_separators(node_state[print_start:_print_end], SEPARATOR, options.WN_COLON) + '=Node state'
        for core_line in get_core_lines(cpu_core_dict, print_start, _print_end, pattern_of_id):
            if gray_hash * (_print_end - print_start) not in core_line.replace(separator_between_ansi, ''):
                print core_line


def create_user_accounts_pool_mappings(account_jobs_table, pattern_of_id):
    print colorize('\n===> ', '#') + \
          colorize('User accounts and pool mappings', 'Nothing') + \
          colorize(' <=== ', '#') + \
          colorize("  ('all' also includes those in C and W states, as reported by qstat)", '#')

    print 'id|    R +    Q /  all |    unix account | Grid certificate DN (info only available under elevated privileges)'
    for line in account_jobs_table:
        uid, runningjobs, queuedjobs, alljobs, user = line[0], line[1], line[2], line[3], line[4]
        account = pattern_of_id[uid]
        if options.NOCOLOR or account == 'account_not_coloured' or color_of_account[account] == 'normal':
            extra_width = 0
            account = 'account_not_coloured'
        else:
            extra_width = 12
        print_string = '{:<{width2}}{sep} {:>{width4}} + {:>{width4}} / {:>{width4}} {sep} {:>{width15}} {sep}'.format(
            colorize(str(uid), account),
            colorize(str(runningjobs), account),
            colorize(str(queuedjobs), account),
            colorize(str(alljobs), account),
            colorize(user, account),
            sep=colorize(SEPARATOR, account),
            width2=2 + extra_width,
            width3=3 + extra_width,
            width4=4 + extra_width,
            width15=15 + extra_width,
        )
        print print_string


def get_core_lines(cpu_core_dict, print_start, print_end, pattern_of_id):
    """
    prints all coreX lines
    """
    # lines = []
    for ind, k in enumerate(cpu_core_dict):
        color_cpu_core_list = list(
            line_with_separators(
                cpu_core_dict['Cpu' + str(ind) + 'line'][print_start:print_end],
                SEPARATOR,
                options.WN_COLON
            )
        )
        color_cpu_core_list = [colorize(elem, pattern_of_id[elem]) for elem in color_cpu_core_list if elem in pattern_of_id]
        line = ''.join(color_cpu_core_list)
        yield line + colorize('=Core' + str(ind), 'account_not_coloured')


def calc_cpu_lines(node_dict, id_of_username, job_ids, user_names):
    _cpu_core_dict = {}
    max_np_range = []
    user_of_job_id = dict(izip(job_ids, user_names))

    for core_nr in range(node_dict['max_np']):
        _cpu_core_dict['Cpu' + str(core_nr) + 'line'] = ''  # Cpu0line, Cpu1line, Cpu2line, .. = '','','', ..
        max_np_range.append(str(core_nr))

    for _node in node_dict['wn_dict']:
        state_np_corejob = node_dict['wn_dict'][_node]
        _cpu_core_dict = fill_cpucore_columns(state_np_corejob, _cpu_core_dict, id_of_username, max_np_range, user_of_job_id)

    return _cpu_core_dict


def calculate_wn_occupancy(node_dict, user_names, job_states, job_ids):
    """
    Prints the Worker Nodes Occupancy table.
    if there are non-uniform WNs in pbsnodes.yaml, e.g. wn01, wn02, gn01, gn02, ...,  remapping is performed.
    Otherwise, for uniform WNs, i.e. all using the same numbering scheme, wn01, wn02, ... proceeds as normal.
    Number of Extra tables needed is calculated inside the calc_all_wnid_label_lines function below
    """
    wn_occup = dict()
    wn_occup['term_columns'] = calculate_split_screen_size()
    wn_occup['account_jobs_table'], wn_occup['id_of_username'] = create_account_jobs_table(user_names, job_states)
    account_jobs_table = wn_occup['account_jobs_table']
    wn_occup['pattern_of_id'] = make_pattern_of_id(account_jobs_table)

    wn_occup['cpu_core_dict'] = calc_cpu_lines(node_dict, wn_occup['id_of_username'], job_ids, user_names)
    tot_length, wn_dict, wn_list = node_dict['highest_wn'], node_dict['wn_dict'], node_dict['wn_list']

    (wn_occup['print_start'], wn_occup['print_end'], wn_occup['extra_matrices_nr']) = find_matrices_width(tot_length, wn_list,
                                                                                                wn_occup['term_columns'])
    wn_occup['wn_vert_labels'] = calc_all_wnid_label_lines(tot_length)
    wn_occup['node_state'] = ''.join([wn_dict[node]['state'] for node in wn_dict])

    return wn_occup, node_dict


def display_wn_occupancy(wn_occup, node_dict):

    print_start = wn_occup['print_start']
    print_end = wn_occup['print_end']
    wn_vert_labels = wn_occup['wn_vert_labels']
    node_state = wn_occup['node_state']
    cpu_core_dict = wn_occup['cpu_core_dict']
    extra_matrices_nr = wn_occup['extra_matrices_nr']
    term_columns = wn_occup['term_columns']
    pattern_of_id = wn_occup['pattern_of_id']
    tot_length, wn_dict, wn_list = node_dict['highest_wn'], node_dict['wn_dict'], node_dict['wn_list']

    print colorize('===> ', '#') + colorize('Worker Nodes occupancy', 'Nothing') + colorize(' <=== ', '#') + colorize(
        '(you can read vertically the node IDs; nodes in free state are noted with - )', 'account_not_coloured')
    print_wnid_lines(print_start, print_end, tot_length, wn_vert_labels)
    print line_with_separators(node_state[print_start:print_end], SEPARATOR, options.WN_COLON) + '=Node state'
    for core_line in get_core_lines(cpu_core_dict, print_start, print_end, pattern_of_id):
        print core_line

    display_remaining_matrices(node_state,
                               extra_matrices_nr,
                               node_dict,
                               cpu_core_dict,
                               print_end,
                               pattern_of_id,
                               wn_vert_labels,
                               term_columns)


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
        for re_account_colour in config['user_colour_mappings']:
            re_account = re_account_colour.keys()[0]
            try:
                _ = re.search(re_account, user).group(0)
            except AttributeError:
                continue  # keep trying
            else:
                account = re_account  # colours the text according to the regex given by the user in qtopconf

        pattern_of_id[uid] = account if account in color_of_account else 'account_not_coloured'

    pattern_of_id['#'] = '#'
    pattern_of_id['_'] = '_'
    pattern_of_id[SEPARATOR] = 'account_not_coloured'
    return pattern_of_id


def reset_yaml_files():
    """
    empties the files with every run of the python script
    """
    for _file in [PBSNODES_OUT_FN, QSTATQ_OUT_FN, QSTAT_OUT_FN]:
        fin = open(_file, 'w')
        fin.close()


def load_yaml_config(path):
    try:
        config = yaml.safe_load(open(path + "/qtopconf.yaml"))
    except yaml.YAMLError, exc:
        if hasattr(exc, 'problem_mark'):
            mark = exc.problem_mark
            print "Error position: (%s:%s)" % (mark.line + 1, mark.column + 1)

    config['possible_ids'] = list(config['possible_ids'])
    symbol_map = dict([(chr(x), x) for x in range(33, 48) + range(58, 64) + range(91, 96) + range(123, 126)])

    if config['user_colour_mappings']:
        [color_of_account.update(d) for d in config['user_colour_mappings']]
    else:
        config['user_colour_mappings'] = list()
    if config['remapping']:
        pass
    else:
        config['remapping'] = list()
    for symbol in symbol_map:
        config['possible_ids'].append(symbol)
    return config


def calculate_split_screen_size():
    """
    Calculation of split screen size
    """
    try:
        _, term_columns = os.popen('stty size', 'r').read().split()  # does not work in pycharm
    except ValueError:  # probably Pycharm's fault
        # _, term_columns = [52, 211]
        _, term_columns = [53, 176]
    term_columns = int(term_columns)
    return term_columns


if __name__ == '__main__':
    # print_start, print_end = 0, None

    HOMEPATH = os.path.expanduser('~/PycharmProjects')
    QTOPPATH = os.path.expanduser('~/PycharmProjects/qtop')  # qtoppath: ~/qtop/qtop

    config = load_yaml_config(QTOPPATH)
    SEPARATOR = config['separator']  # alias

    # Name files according to unique pid
    ext = qstat_mapping[options.write_method][2]
    PBSNODES_OUT_FN = 'pbsnodes_{}.{}'.format(options.write_method, ext)  # os.getpid()
    QSTATQ_OUT_FN = 'qstat-q_{}.{}'.format(options.write_method, ext)  # os.getpid()
    QSTAT_OUT_FN = 'qstat_{}.{}'.format(options.write_method, ext)  # os.getpid()

    os.chdir(options.SOURCEDIR)
    # Location of read and created files
    PBSNODES_ORIG_FN = [f for f in os.listdir(os.getcwd()) if f.startswith('pbsnodes') and not f.endswith('.yaml')][0]
    QSTATQ_ORIG_FN = [f for f in os.listdir(os.getcwd()) if (
        f.startswith('qstat_q') or f.startswith('qstatq') or f.startswith('qstat-q') and not f.endswith('.yaml'))][0]
    QSTAT_ORIG_FN = [f for f in os.listdir(os.getcwd()) if f.startswith('qstat.') and not f.endswith('.yaml')][0]

    # input files ###################################
    # reset_yaml_files()  # either that or having a pid appended in the filename
    make_pbsnodes(PBSNODES_ORIG_FN, PBSNODES_OUT_FN, options.write_method)
    make_qstatq(QSTATQ_ORIG_FN, QSTATQ_OUT_FN, options.write_method)
    make_qstat(QSTAT_ORIG_FN, QSTAT_OUT_FN, options.write_method)

    pbs_nodes = read_pbsnodes_yaml(PBSNODES_OUT_FN, options.write_method)
    total_running, total_queued, qstatq_lod = read_qstatq_yaml(QSTATQ_OUT_FN, options.write_method)
    job_ids, user_names, job_states, _ = read_qstat_yaml(QSTAT_OUT_FN, options.write_method)  # _ == queue_names

    #  MAIN ##################################
    node_dict, NAMED_WNS = calculate_stuff(pbs_nodes)

    display_job_accounting_summary(node_dict, total_running, total_queued, qstatq_lod)
    wn_occup, node_dict = calculate_wn_occupancy(node_dict, user_names, job_states, job_ids)
    display_wn_occupancy(wn_occup, node_dict)
    create_user_accounts_pool_mappings(wn_occup['account_jobs_table'], wn_occup['pattern_of_id'])

    print '\nThanks for watching!'
    os.chdir(QTOPPATH)

# account_jobs_table, pattern_of_id, print_start, print_end, tot_length, wn_vert_labels, node_state, cpu_core_dict, extra_matrices_nr, term_columns