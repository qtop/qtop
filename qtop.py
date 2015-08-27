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


def colorize(text, pattern):
    """prints text colored according to its unix account colors"""
    return "\033[" + code_of_color[color_of_account[pattern]] + "m" + text + "\033[1;m" if not options.NOCOLOR else text


def decide_remapping(pbs_nodes, node_dict):
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
    cnt = 0
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
        except KeyError as msg:
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

    decide_remapping(pbs_nodes, node_dict)
    map_pbsnodes_to_wn_dicts(node_dict, pbs_nodes)
    if options.REMAP:
        node_dict['highest_wn'] = node_dict['total_wn']
        node_dict['wn_list'] = node_dict['wn_list_remapped']
        node_dict['wn_dict'] = node_dict['wn_dict_remapped']
    else:
        node_dict['highest_wn'] = max(node_dict['wn_list'])

    # fill in non-existent WN nodes (absent from pbsnodes file) with '?' and count them
    # is this even needed anymore?!
    for i in range(1, node_dict['highest_wn'] + 1):
        if i not in node_dict['wn_dict']:
            node_dict['wn_dict'][i] = {'state': '?', 'np': 0}  # was: node_dict['wn_dict'][i] = '?'

    return node_dict, NAMED_WNS


def nodes_with_jobs(pbs_nodes):
    for _, pbs_node in pbs_nodes.iteritems():
        if 'core_job_map' in pbs_node:
            yield pbs_node


def create_job_accounting_summary(node_dict, total_running, total_queued, qstatq_list):
    if options.REMAP:
        print '=== WARNING: --- Remapping WN names and retrying heuristics... good luck with this... ---'
    print '\nPBS report tool. Please try: watch -d ' + QTOPPATH + \
          '. All bugs added by sfranky@gmail.com. Cross fingers now...\n'
    print colorize('===> ', '#') + colorize('Job accounting summary', 'Nothing') + colorize(' <=== ', '#') + colorize(
        '(Rev: 3000 $) %s WORKDIR = to be added', 'NoColourAccount') % (datetime.datetime.today())
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
        color = q_name if q_name in color_of_account else 'Nothing'
        print "{}: {} + {} |".format(colorize(q_name, color),
                                     colorize(q_running_jobs, color),
                                     colorize(q_queued_jobs, color)),
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
             alljobs_of_user, user]
        )
    account_jobs_table.sort(key=itemgetter(3, 1, 4), reverse=True)  # sort by All jobs
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


def create_job_counts2(user_names, job_states, state_abbrevs):
    """
    counting of R,Q,C,W,E attached to user
    :param user_names: list
    :param job_states: list
    :param state_abbrevs: dict
    :return: dict
    """
    user_states = [(user_name, job_state) for user_name, job_state in zip(user_names, job_states)]
    job_counts = Counter(user_states)
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


def insert_sep(original, separator, pos, stopaftern=0):
    '''
    insert separator into original (string) every posth position, optionally stopping after stopafter times.
    '''
    pos = int(pos)
    if pos != 0:  # default value is zero, means no vertical separators
        sep = original[:]  # insert initial vertical separator
        if stopaftern == 0:
            times = len(original) / pos
        else:
            times = stopaftern
        sep = sep[:pos] + separator + sep[pos:]
        for i in range(2, times + 1):
            sep = sep[:pos * i + i - 1] + separator + sep[pos * i + i - 1:]
        sep = separator + sep  # insert initial vertical separator
        return sep
    else:  # no separators
        return original


def calculate_Total_WNIDLine_Width(_node_count):  # (total_wn) in case of multiple node_dict['node_subclusters']
    """
    calculates the worker node ID number line widths (expressed by hxxxx's)
    """
    str_nodes = len(str(_node_count))  # 4
    hxxxx = {str(place): [] for place in range(1, str_nodes + 1)}
    for nr in range(1, _node_count + 1):
        extra_zeros = str_nodes - len(str(nr))  # 4 - 1 = 3, for wn0001
        string = "".join("0" * extra_zeros + str(nr))
        for place in range(1, str_nodes + 1):
            hxxxx[str(place)].append(string[place - 1])

    return hxxxx


def find_matrices_width(wn_number, wn_list, node_dict, term_columns, DEADWEIGHT=15):
    """
    masking/clipping functionality: if the earliest node number is high (e.g. 130), the first 129 WNs need not show up.
    case 1: wn_number is RemapNr, WNList is WNListRemapped
    case 2: wn_number is BiggestWrittenNode, WNList is WNList
    DEADWEIGHT = 15  # standard columns' width on the right of the CoreX map
    """
    start = 0
    if (options.NOMASKING is True) and min(wn_list) > config['min_masking_threshold'] and type(min(wn_list)) == str:
        # in case of named instead of numbered WNs
        pass
    elif (options.NOMASKING is True) and min(wn_list) > config['min_masking_threshold'] and type(min(wn_list)) == int:
        start = min(wn_list) - 1  # exclude unneeded first empty nodes from the matrix

    # Extra matrices may be needed if the WNs are more than the screen width can hold.
    if wn_number > start:  # start will either be 1 or (masked >= config['min_masking_threshold'] + 1)
        extra_matrices_nr = abs(wn_number - start + 10) / term_columns
    elif wn_number < start and len(node_dict['node_subclusters']) > 1:  # Remapping
        extra_matrices_nr = (wn_number + 10) / term_columns
    else:
        print "This is a case I didn't foresee (wn_number vs start vs node_dict['node_subclusters'])"

    if config['user_cut_matrix_width']:  # if the user defines a custom cut (in the configuration file)
        stop = start + config['user_cut_matrix_width']
        return start, stop, wn_number / config['user_cut_matrix_width']
    elif extra_matrices_nr:  # if more matrices are needed due to lack of space, cut every matrix so that if fits to screen
        stop = start + term_columns - DEADWEIGHT
        return start, stop, extra_matrices_nr
    else:  # just one matrix, small cluster!
        stop = start + wn_number
        return start, stop, 0


def print_WN_ID_lines(start, stop, wn_number, hxxxx):
    """
    wn_number determines the number of WN ID lines needed  (1/2/3/4+?)
    """
    just_name_dict = {}
    str_nodes = len(str(wn_number))  # 4
    d = OrderedDict()
    if not NAMED_WNS:
        for place in range(1, str_nodes + 1):
            d[str(place)] = "".join(hxxxx[str(place)])
        appends = {
            '1': ['={__WNID__}'],
            '2': ['={_Worker_}', '={__Node__}'],
            '3': ['={_Worker_}', '={__Node__}', '={___ID___}'],
            '4': ['={________}', '={_Worker_}', '={__Node__}', '={___ID___}']
        }
        size = str(len(d))  # key, nr of horizontal lines to be displayed
        # d[size]  #
        for line in d:
            print insert_sep(d[line][start:stop], SEPARATOR, options.WN_COLON) + iter(appends[size]).next()
    elif NAMED_WNS or options.FORCE_NAMES:  # names (e.g. fruits) instead of numbered WNs
        raise NotImplementedError
        color = 0
        highlight = {0: 'cmsplt', 1: 'Red'}
        for line, _ in enumerate(max(node_dict['wn_list'])):
            just_name_dict[line] = ''
        for column, _1 in enumerate(node_dict['wn_list']):
            for line, _2 in enumerate(max(node_dict['wn_list'])):
                try:
                    letter = node_dict['wn_list'][column][line]
                except TypeError:
                    letter = ' '
                just_name_dict[line] += colorize(letter, highlight[color])
            color = 0 if color == 1 else 1
        for line, _ in enumerate(max(node_dict['wn_list'])):
            print just_name_dict[line] + '={__WNID__}'


def calculate_remaining_matrices(node_state,
                                 extra_matrices_nr,
                                 node_dict,
                                 cpu_core_dict,
                                 _print_end,
                                 account_nrless_of_id,
                                 hxxxx,
                                 term_columns,
                                 DEADWEIGHT=15):
    """
    If there WNs are numerous, this calculates the extra matrices needed to display them.
    """
    for i in range(extra_matrices_nr):
        print '\n'
        print_start = _print_end
        if config['user_cut_matrix_width']:
            _print_end += config['user_cut_matrix_width']
        else:
            _print_end += term_columns - DEADWEIGHT

        if options.REMAP:
            _print_end = min(_print_end, node_dict['total_wn'])
        else:
            _print_end = min(_print_end, node_dict['highest_wn'])

        print_WN_ID_lines(print_start, _print_end, node_dict['total_wn'], hxxxx)

        print insert_sep(node_state[print_start:_print_end], SEPARATOR, options.WN_COLON) + '=Node state'
        for ind, k in enumerate(cpu_core_dict):
            color_cpu_core_list = list(
                insert_sep(cpu_core_dict['Cpu' + str(ind) + 'line'][print_start:_print_end], SEPARATOR,
                           options.WN_COLON))
            nocolor_linelength = len(''.join(color_cpu_core_list))
            color_cpu_core_list = [colorize(elem, account_nrless_of_id[elem]) for elem in color_cpu_core_list if
                                   elem in account_nrless_of_id]
            line = ''.join(color_cpu_core_list)
            '''
            if the first matrix has 10 machines with 64 cores, and the rest 190 machines have 8 cores, don't print the non-existent
            56 cores from the next matrix on.
            IMPORTANT: not working if vertical separators are present!
            '''
            if '\x1b[1;30m#\x1b[1;m' * nocolor_linelength not in line:
                print line + colorize('=core' + str(ind), 'NoColourAccount')


def create_user_accounts_pool_mappings(account_jobs_table):
    print colorize('\n===> ', '#') + colorize('User accounts and pool mappings', 'Nothing') + colorize(' <=== ',
                                                                                                       '#') + colorize(
        "('all' includes those in C and W states, as reported by qstat)", 'NoColourAccount')
    print ' id |  R   +   Q  /  all |    unix account | Grid certificate DN (this info is only available under elevated privileges)'

    for line in account_jobs_table:
        uid, runningjobs, queuedjobs, alljobs, user = line[0], line[1], line[2], line[3], line[4]
        print_string = '%3s | %4s + %4s / %4s | %15s |' % (uid, runningjobs, queuedjobs, alljobs, user)
        for account in color_of_account:
            if user.startswith(account) and not options.NOCOLOR:
                print_string = '%15s | %16s + %16s / %16s | %27s %4s' % (
                    colorize(str(uid), account), colorize(str(runningjobs), account), colorize(str(queuedjobs), account),
                    colorize(str(alljobs), account), colorize(str(user), account),
                    colorize(SEPARATOR, 'NoColourAccount'))
            elif user.startswith(account) and options.NOCOLOR:
                print_string = '%2s | %3s + %3s / %3s | %14s |' % (
                    colorize(uid, account), colorize(str(runningjobs), account), colorize(str(queuedjobs), account),
                    colorize(str(alljobs), account), colorize(user, account))
        print print_string


def print_core_lines(cpu_core_dict, account_jobs_table, print_start, print_end):
    """
    prints all coreX lines
    """
    account_nrless_of_id = {}
    for line in account_jobs_table:
        just_name = re.split('[0-9]+', line[4])[0]
        account_nrless_of_id[line[0]] = just_name if just_name in color_of_account else 'NoColourAccount'

    account_nrless_of_id['#'] = '#'
    account_nrless_of_id['_'] = '_'
    account_nrless_of_id[SEPARATOR] = 'NoColourAccount'
    for ind, k in enumerate(cpu_core_dict):
        color_cpu_core_list = list(
            insert_sep(cpu_core_dict['Cpu' + str(ind) + 'line'][print_start:print_end], SEPARATOR, options.WN_COLON))
        color_cpu_core_list = [colorize(elem, account_nrless_of_id[elem]) for elem in color_cpu_core_list if
                               elem in account_nrless_of_id]
        line = ''.join(color_cpu_core_list)
        print line + colorize('=core' + str(ind), 'NoColourAccount')

    return account_nrless_of_id


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
    Number of Extra tables needed is calculated inside the calculate_Total_WNIDLine_Width function below
    """
    term_columns = calculate_split_screen_size()
    account_jobs_table, id_of_username = create_account_jobs_table(user_names, job_states)

    cpu_core_dict = calc_cpu_lines(node_dict, id_of_username, job_ids, user_names)
    print colorize('===> ', '#') + colorize('Worker Nodes occupancy', 'Nothing') + colorize(' <=== ', '#') + colorize(
        '(you can read vertically the node IDs; nodes in free state are noted with - )', 'NoColourAccount')

    highest_wn, wn_dict, wn_list = node_dict['highest_wn'], node_dict['wn_dict'], node_dict['wn_list']

    hxxxx = calculate_Total_WNIDLine_Width(highest_wn)
    node_state = ''
    for node in wn_dict:
        node_state += wn_dict[node]['state']
    (print_start, print_end, extra_matrices_nr) = find_matrices_width(highest_wn, wn_list, node_dict, term_columns)
    print_WN_ID_lines(print_start, print_end, highest_wn, hxxxx)
    print insert_sep(node_state[print_start:print_end], SEPARATOR, options.WN_COLON) + '=Node state'

    account_nrless_of_id = print_core_lines(cpu_core_dict, account_jobs_table, print_start, print_end)
    calculate_remaining_matrices(node_state,
                                 extra_matrices_nr,
                                 node_dict,
                                 cpu_core_dict,
                                 print_end,
                                 account_nrless_of_id,
                                 hxxxx, term_columns)
    return account_jobs_table


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
    print_start, print_end = 0, None
    DEADWEIGHT = 15  # standard columns' width on the right of the CoreX map

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
    job_ids, user_names, job_states, _ = read_qstat_yaml(QSTAT_OUT_FN, options.write_method)  #_ == queue_names, not used for now

    #  MAIN ##################################
    node_dict, NAMED_WNS = calculate_stuff(pbs_nodes)

    create_job_accounting_summary(node_dict, total_running, total_queued, qstatq_lod)
    account_jobs_table = calculate_wn_occupancy(node_dict, user_names, job_states, job_ids)
    create_user_accounts_pool_mappings(account_jobs_table)

    print '\nThanks for watching!'
    os.chdir(QTOPPATH)
