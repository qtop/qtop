#!/usr/bin/env python

################################################
#              qtop v.0.7.2                    #
#     Licensed under MIT-GPL licenses          #
#                     Sotiris Fragkiskos       #
#                     Fotis Georgatos          #
################################################

from operator import itemgetter
from optparse import OptionParser
import datetime
from itertools import izip
try:
    from collections import OrderedDict
except ImportError:
    from ordereddict import OrderedDict
from signal import signal, SIGPIPE, SIG_DFL
# modules
from plugin_pbs import *
from plugin_oar import *
from plugin_sge import *
from stat_maker import *
from math import ceil
from colormap import color_of_account, code_of_color
from common_module import read_qstat_yaml
from yaml_parser import read_yaml_config


parser = OptionParser()  # for more details see http://docs.python.org/library/optparse.html
parser.add_option("-a", "--blindremapping", action="store_true", dest="BLINDREMAP", default=False,
                  help="This may be used in situations where node names are not a pure arithmetic seq (eg. rocks clusters)")
parser.add_option("-b", "--batchSystem", action="store", type="string", dest="BATCH_SYSTEM")
parser.add_option("-y", "--readexistingyaml", action="store_true", dest="YAML_EXISTS", default=False,
                  help="Do not remake yaml input files, read from the existing ones")
parser.add_option("-c", "--NOCOLOR", action="store_true", dest="NOCOLOR", default=False,
                  help="Enable/Disable color in qtop output.")
# parser.add_option("-f", "--setCOLORMAPFILE", action="store", type="string", dest="COLORFILE")
parser.add_option("-m", "--noMasking", action="store_true", dest="NOMASKING", default=False,
                  help="Don't mask early empty WNs (default: if the first 30 WNs are unused, counting starts from 31).")
parser.add_option("-o", "--SetVerticalSeparatorXX", action="store", dest="WN_COLON", default=0,
                  help="Put vertical bar every WN_COLON nodes.")
parser.add_option("-s", "--SetSourceDir", dest="SOURCEDIR", default='.',
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
(options, args) = parser.parse_args()


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
            if ((not options.NOCOLOR) and pattern != 'account_not_colored' and text != ' ') else text


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
        # max(cluster_dict['workernode_list']) was cluster_dict['highest_wn']


def calculate_cluster(worker_nodes):
    NAMED_WNS = 0 if not options.FORCE_NAMES else 1
    cluster_dict = dict()
    for key in ['working_cores', 'total_cores', 'max_np', 'highest_wn', 'offline_down_nodes']:
        cluster_dict[key] = 0
    cluster_dict['node_subclusters'] = set()
    cluster_dict['workernode_dict'] = {}
    cluster_dict['workernode_dict_remapped'] = {}  # { remapnr: [state, np, (core0, job1), (core1, job1), ....]}

    cluster_dict['total_wn'] = len(worker_nodes)  # == existing_nodes
    cluster_dict['workernode_list'] = []
    cluster_dict['workernode_list_remapped'] = range(1, cluster_dict['total_wn'])  # leave xrange aside for now

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
    map_batch_nodes_to_wn_dicts(cluster_dict, worker_nodes, options.REMAP)
    if options.REMAP:
        cluster_dict['highest_wn'] = cluster_dict['total_wn']
        cluster_dict['workernode_list'] = cluster_dict['workernode_list_remapped']
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
    print 'Please try: watch -d + %s/qtop.py -s %s\n' % (QTOPPATH, options.SOURCEDIR)
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
    state_abbrevs = config['state_abbreviations'][options.BATCH_SYSTEM or scheduler]

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
            [
                id_of_username[user],
                job_counts['running_of_user'][user],
                job_counts['queued_of_user'][user],
                alljobs_of_user,
                user
             ]
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
    MAX_UNIX_ACCOUNTS = 87  # was : 62
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
            core_user_map[core_line] += [eval(config['non_existent_node_symbol'])]
    else:
        _own_np = int(np)
        own_np_range = [str(x) for x in range(_own_np)]
        own_np_empty_range = own_np_range[:]

        for corejob in corejobs:
            core, job = str(corejob['core']), str(corejob['job'])
            try:
                _ = user_of_job_id[job]
            except KeyError, KeyErrorValue:
                print 'There seems to be a problem with the qstat output. ' \
                      'A Job (ID %s) has gone rogue. ' \
                      'Please check with the SysAdmin.' % (str(KeyErrorValue))
                raise KeyError
            else:
                core_user_map['Core' + str(core) + 'line'] += [str(id_of_username[user_of_job_id[job]])]
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
    # for nr in end_labels:
    #     end_labels[nr] = [label.strip("'") for label in fix_config_list(end_labels[nr])]

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
        'wn id lines': (print_wnid_lines, (print_char_start, print_char_stop, cluster_dict['highest_wn'], wn_vert_labels),
            {'inner_attrs': None}),
        'core user map': (print_core_lines, (core_user_map, print_char_start, print_char_stop, pattern_of_id), {'attrs': None}),
        # 'temperature':
        # (print_single_attr_line, (print_char_start, print_char_stop), {'attr_line': workernodes_occupancy['temperature']}),
    }

    for yaml_key, part_name in get_yaml_key_part('workernodes_matrix'):
        new_dict_var = {
            part_name:
            (
                print_single_attr_line,
                (print_char_start, print_char_stop),
                {'attr_line': workernodes_occupancy[part_name]}
            )
        }
        occupancy_parts.update(new_dict_var)

    for _part in config['workernodes_matrix']:
        part = [k for k in _part][0]
        occupancy_parts[part][2].update(_part[part])  # get extra options from user
        fn, args, kwargs = occupancy_parts[part][0], occupancy_parts[part][1], occupancy_parts[part][2]
        fn(*args, **kwargs)

    print


def print_single_attr_line(print_char_start, print_char_stop, attr_line, label, color_func=None, **kwargs):
    """
    attr_line can be e.g. Node state
    """
    # TODO: fix option parameter, inserted for testing purposes
    line = attr_line[print_char_start:print_char_stop]
    # maybe put attr_line and label as kwd arguments? collect them as **kwargs
    attr_line = insert_separators(line, SEPARATOR, options.WN_COLON) + '=%s'  % label  # this didnt work as expected
    attr_line = ''.join([colorize(char, 'Nothing', color_func) for char in attr_line])
    print attr_line


def display_user_accounts_pool_mappings(account_jobs_table, pattern_of_id):
    print colorize('\n===> ', '#') + \
          colorize('User accounts and pool mappings', 'Nothing') + \
          colorize(' <=== ', '#') + \
          colorize("  ('all' also includes those in C and W states, as reported by qstat)", '#')

    print 'id|    R +    Q /  all |    unix account | Grid certificate DN (info only available under elevated privileges)'
    for line in account_jobs_table:
        uid, runningjobs, queuedjobs, alljobs, user = line[0], line[1], line[2], line[3], line[4]
        account = pattern_of_id[uid]
        if options.NOCOLOR or account == 'account_not_colored' or color_of_account[account] == 'reset':
            extra_width = 0
            account = 'account_not_colored'
        else:
            extra_width = 12
        print_string = '{0:<{width2}}{sep} {1:>{width4}} + {2:>{width4}} / {3:>{width4}} {sep} {4:>{width15}} {sep}'.format(
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
        _core_user_map['Core' + str(core_nr) + 'line'] = []  # Cpu0line, Cpu1line, Cpu2line, .. = '','','', ..

    for _node in cluster_dict['workernode_dict']:
        state_np_corejob = cluster_dict['workernode_dict'][_node]
        _core_user_map, cluster_dict['workernode_dict'][_node]['core_user_column'] = fill_node_cores_column(state_np_corejob, _core_user_map, id_of_username, max_np_range, user_of_job_id)

    for coreline in _core_user_map:
        _core_user_map[coreline] = ''.join(_core_user_map[coreline])

    return _core_user_map


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
    workernodes_occupancy = dict()
    workernodes_occupancy['term_columns'] = calculate_split_screen_size()
    workernodes_occupancy['account_jobs_table'], workernodes_occupancy['id_of_username'] = create_account_jobs_table(user_names, job_states)
    workernodes_occupancy['pattern_of_id'] = make_pattern_of_id(workernodes_occupancy['account_jobs_table'])
    workernodes_occupancy['print_char_start'], workernodes_occupancy['print_char_stop'], workernodes_occupancy['extra_matrices_nr'] = find_matrices_width(
        cluster_dict['highest_wn'],
        cluster_dict['workernode_list'],
        workernodes_occupancy['term_columns']
    )
    workernodes_occupancy['wn_vert_labels'] = calc_all_wnid_label_lines(cluster_dict['highest_wn'])

    for yaml_key, part_name in get_yaml_key_part('workernodes_matrix'):
        workernodes_occupancy[part_name] = ''.join([str(cluster_dict['workernode_dict'][node][yaml_key]) for node in cluster_dict['workernode_dict']])
    # e.g. workernodes_occupancy['node_state'] = ''.join([str(cluster_dict['workernode_dict'][node]['state']) for node in cluster_dict['workernode_dict']])
    workernodes_occupancy['core user map'] = calc_core_userid_matrix(cluster_dict, workernodes_occupancy['id_of_username'], job_ids, user_names)
    return workernodes_occupancy, cluster_dict


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


def load_yaml_config(path='.'):
    config = read_yaml_config(os.path.join(path + "/qtopconf.yaml"))
    # try:
    #     config = yaml.safe_load(open(os.path.join(path + "/qtopconf.yaml")))
    # except ImportError:
    #     config = read_yaml_config(os.path.join(path + "/qtopconf.yaml"))
    # except yaml.YAMLError, exc:
    #     if hasattr(exc, 'problem_mark'):
    #         mark = exc.problem_mark
    #         print "Your YAML configuration file has an error in position: (%s:%s)" % (mark.line + 1, mark.column + 1)
    #         print "Please make sure that spaces are multiples of 2."

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
    return config


def calculate_split_screen_size():
    """
    Calculates where to break the matrix into more matrices, because of the window size.
    """
    try:
        _, term_columns = config['term_size']
    except ValueError:
        _, term_columns = fix_config_list(config['term_size'])
    except KeyError:
        _, term_columns = os.popen('stty size', 'r').read().split()
    term_columns = int(term_columns)
    return term_columns


def fix_config_list(config_list):
    t = config_list
    item = t[0]
    list_items = item.split(',')
    return [nr.strip() for nr in list_items]


def sort_batch_nodes(batch_nodes):
    try:
        batch_nodes.sort(key=eval(config['sorting']['user_sort']), reverse=eval(config['sorting']['reverse']))
    except IndexError:
        print "\n**There's probably something wrong in your sorting lambda in qtopconf.yaml.**\n"
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
        for rule in filter_rules:
            filter_func = filter_types[rule.keys()[0]]
            args = rule.values()[0]
            batch_nodes = filter_func(batch_nodes, args)
        return batch_nodes


def map_batch_nodes_to_wn_dicts(cluster_dict, batch_nodes, options_remap):
    """
    """
    user_sorting = config['sorting'] and config['sorting'].values()[0]
    user_filtering = config['filtering'] and config['filtering'][0]
    if user_sorting and options_remap:
        sort_batch_nodes(batch_nodes)
    if user_filtering and options_remap:
        batch_nodes = filter_batch_nodes(batch_nodes, config['filtering'])
    for (batch_node, (idx, cur_node_nr)) in zip(batch_nodes, enumerate(cluster_dict['workernode_list'])):
        cluster_dict['workernode_dict'][cur_node_nr] = batch_node
        cluster_dict['workernode_dict_remapped'][idx] = batch_node


def convert_to_yaml(scheduler, INPUT_FNs, filenames, write_method, commands):

    for _file in INPUT_FNs:
        file_orig, file_out = filenames[_file], filenames[_file + '_out']
        _func = commands[_file]
        # print 'executing %(_func)s on file %(_file)s' % {'_func': _func, '_file': _file}
        _func(file_orig, file_out, write_method)


def exec_func_tuples(func_tuples):
    _commands = iter(func_tuples)
    for command in _commands:
        ffunc, args, kwargs = command[0], command[1], command[2]
        yield ffunc(*args, **kwargs)


def get_yaml_reader(scheduler):
    if scheduler == 'pbs':
        yaml_reader = [
            (read_pbsnodes_yaml, (filenames.get('pbsnodes_file_out'),), {'write_method': options.write_method}),
            (read_qstat_yaml, (filenames.get('qstat_file_out'),), {'write_method': options.write_method}),
            (read_qstatq_yaml, (filenames.get('qstatq_file_out'),), {'write_method': options.write_method}),
        ]
    elif scheduler == 'oar':
        yaml_reader = [
            (read_oarnodes_yaml, ([filenames.get('oarnodes_s_file'), filenames.get('oarnodes_y_file')]), {'write_method': options.write_method}),
            (read_qstat_yaml, ([filenames.get('oarstat_file_out')]), {'write_method': options.write_method}),
            (lambda *args, **kwargs: (0, 0, 0), ([filenames.get('oarstat_file')]), {'write_method': options.write_method}),
        ]
    elif scheduler == 'sge':
        yaml_reader = [
            (get_worker_nodes, ([filenames.get('sge_file_stat')]), {'write_method': options.write_method}),
            (read_qstat_yaml, ([SGEStatMaker.temp_filepath]), {'write_method': options.write_method}),
            # (lambda *args, **kwargs: (0, 0, 0), ([filenames.get('sge_file_stat')]), {'write_method': options.write_method}),
            (get_statq_from_xml, ([filenames.get('sge_file_stat')]), {'write_method': options.write_method}),
        ]
    return yaml_reader


if __name__ == '__main__':

    cwd = os.getcwd()
    USERPATH = os.path.expandvars('$HOME/.local/qtop')
    QTOPPATH = os.path.expanduser(cwd)
    try:
        config = load_yaml_config(USERPATH)
    except IOError:
        config = load_yaml_config(QTOPPATH)


    SEPARATOR = config['workernodes_matrix'][0]['wn id lines']['separator'].translate(None, "'")  # alias
    USER_CUT_MATRIX_WIDTH = int(config['workernodes_matrix'][0]['wn id lines']['user_cut_matrix_width'])  # alias
    # ALT_LABEL_HIGHLIGHT_COLORS = config['workernodes_matrix'][0]['wn id lines']['alt_label_highlight_colors']  # alias
    ALT_LABEL_HIGHLIGHT_COLORS = fix_config_list(config['workernodes_matrix'][0]['wn id lines']['alt_label_highlight_colors'])

    os.chdir(options.SOURCEDIR)
    scheduler = options.BATCH_SYSTEM or config['scheduler']
    if config['faster_xml_parsing']:
        try:
            from lxml import etree
        except ImportError:
            print 'Module lxml is missing. Reverting to xml module.'
            from xml.etree import ElementTree as etree

    INPUT_FNs = config['schedulers'][scheduler]
    parser_extension_mapping = {'txtyaml': 'yaml', 'json': 'json'}  # 'yaml': 'yaml',
    ext = parser_extension_mapping[options.write_method]
    filenames = dict()
    for _file in INPUT_FNs:
        filenames[_file] = INPUT_FNs[_file]
        # filenames[_file + '_out'] = get_new_temp_file(suffix, prefix)
        filenames[_file + '_out'] = '{filename}_{writemethod}.{ext}'.format(
            filename=INPUT_FNs[_file].rsplit('.')[0], writemethod=options.write_method, ext=ext
        )  # pid=os.getpid()

    yaml_converter = {
        'pbs': {
            'pbsnodes_file': make_pbsnodes,
            'qstatq_file': QStatMaker().make_statq,
            'qstat_file': QStatMaker().make_stat,
        },
        'oar': {
            'oarnodes_s_file': lambda x, y, z: None,
            'oarnodes_y_file': lambda x, y, z: None,
            'oarstat_file': OarStatMaker().make_stat,
        },
        'sge': {
            'sge_file_stat': SGEStatMaker().make_stat,
        }
    }
    commands = yaml_converter[scheduler]
    # reset_yaml_files()  # either that or having a pid appended in the filename
    if not options.YAML_EXISTS:
        convert_to_yaml(scheduler, INPUT_FNs, filenames, options.write_method, commands)

    func_tuples = get_yaml_reader(scheduler)
    commands = exec_func_tuples(func_tuples)

    worker_nodes = next(commands)
    job_ids, user_names, job_states, _ = next(commands)
    total_running_jobs, total_queued_jobs, qstatq_lod = next(commands)

    #  MAIN ##################################
    cluster_dict, NAMED_WNS = calculate_cluster(worker_nodes)
    workernodes_occupancy, cluster_dict = calculate_wn_occupancy(cluster_dict, user_names, job_states, job_ids)

    display_parts = {
        'job_accounting_summary': (display_job_accounting_summary, (cluster_dict, total_running_jobs, total_queued_jobs, qstatq_lod)),
        'workernodes_matrix': (display_wn_occupancy, (workernodes_occupancy, cluster_dict)),
        'user_accounts_pool_mappings': (display_user_accounts_pool_mappings, (workernodes_occupancy['account_jobs_table'], workernodes_occupancy['pattern_of_id']))
    }
    # print 'Reading: {}'.format(SGEStatMaker.temp_filepath)
    for part in config['user_display_parts']:
        _func, args = display_parts[part][0], display_parts[part][1]
        _func(*args)

    # print '\nThanks for watching!'
    os.chdir(QTOPPATH)
