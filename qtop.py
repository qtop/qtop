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
import os
import re
import yaml
from itertools import izip
# modules
from pbs import make_pbsnodes_yaml, make_qstatq_yaml, make_qstat_yaml
from colormap import color_of_account, code_of_color

parser = OptionParser()  # for more details see http://docs.python.org/library/optparse.html
parser.add_option("-a", "--blindremapping", action="store_true", dest="BLINDREMAP", default=False, help="This is used in situations where node names are not a pure arithmetic sequence (eg. rocks clusters)")
parser.add_option("-c", "--COLOR", action="store", dest="COLOR", default='ON', choices=['ON', 'OFF'], help="Enable/Disable color in qtop output. Use it with an ON/OFF switch: -c ON or -c OFF")
parser.add_option("-f", "--setCOLORMAPFILE", action="store", type="string", dest="COLORFILE")
parser.add_option("-m", "--noMasking", action="store_false", dest="MASKING", default=True, help="Don't mask early empty Worker Nodes. (default setting is: if e.g. the first 30 WNs are unused, counting starts from 31).")
parser.add_option("-o", "--SetVerticalSeparatorXX", action="store", dest="WN_COLON", default=0, help="Put vertical bar every WN_COLON nodes.")
parser.add_option("-s", "--SetSourceDir", dest="SOURCEDIR", help="Set the source directory where pbsnodes and qstat reside")
parser.add_option("-z", "--quiet", action="store_false", dest="verbose", default=True, help="don't print status messages to stdout. Not doing anything at the moment.")
parser.add_option("-F", "--ForceNames", action="store_true", dest="FORCE_NAMES", default=False, help="force names to show up instead of numbered WNs even for very small numbers of WNs")

(options, args) = parser.parse_args()

# TODO make the following work with py files instead of qtop.colormap files
# if not options.COLORFILE:
#     options.COLORFILE = os.path.expanduser('~/qtop/qtop/qtop.colormap')


def colorize(text, pattern):
    """prints text colored according to its unix account colors"""
    if options.COLOR == 'ON':
        return "\033[" + code_of_color[color_of_account[pattern]] + "m" + text + "\033[1;m"
    else:
        return text


def read_pbsnodes_yaml_into_list(yaml_fn):
    """
    Parses the pbsnodes yaml file
    :param yaml_fn: str
    :return: list
    """
    pbs_nodes = []
    with open(yaml_fn) as fin:
        _nodes = yaml.safe_load_all(fin)
        for node in _nodes:
            pbs_nodes.append(node)
    pbs_nodes.pop()  # until i figure out why the last node is None
    return pbs_nodes


def read_pbsnodes_yaml_into_dict(yaml_fn):
    """
    Parses the pbsnodes yaml file
    :param yaml_fn: str
    :return: dict
    """
    pbs_nodes = {}
    with open(yaml_fn) as fin:
        _nodes = yaml.safe_load_all(fin)
        for node in _nodes:
            try:
                pbs_nodes[node['domainname']] = node
            except TypeError:
                continue
    return pbs_nodes


def decide_remapping(pbs_nodes, state_dict):
    """
    Cases where remapping is enforced are:
    - the user has requested it (blindremap switch)
    - there are different WN namings, e.g. wn001, wn002, ..., ps001, ps002, ... etc
    - the first starting numbering of a WN is very high and thus would require too much unused space
    - the numbering is strange, say the highest numbered node is named wn12500 but the total amount of WNs is 8000
    - there are numbering collisions,
        e.g. there's a ps001 and a wn001, or a 0x001 and a 0x1, which all would be displayed in position 1
        or there are non-numbered wns

    Reasons not enough to warrant remapping (intended future behaviour)
    - one or two unnumbered nodes (should just be put in the end of the cluster)
    """
    if options.BLINDREMAP or \
        len(state_dict['node_subclusters']) > 1 or \
        min(state_dict['wn_list']) >= 9000 or \
        state_dict['highest_wn'] * config['percentage'] < state_dict['wn_list_remapped'][-1] or \
        len(state_dict['_all_str_digits_with_empties']) != len(state_dict['all_str_digits']) or \
        len(state_dict['all_digits']) != len(state_dict['all_str_digits']):
            options.REMAP = True


def calculate_stuff(pbs_nodes):
    NAMED_WNS = 0 if not options.FORCE_NAMES else 1
    state_dict = dict()
    for key in ['working_cores', 'total_cores', 'max_np', 'highest_wn', 'offline_down_nodes']:
        state_dict[key] = 0
    state_dict['node_subclusters'] = set()
    state_dict['wn_dict'] = {}
    state_dict['wn_dict_remapped'] = {}  # { remapnr: [state, np, (core0, job1), (core1, job1), ....]}

    state_dict['total_wn'] = len(pbs_nodes)  # == existing_nodes
    state_dict['wn_list'] = []
    state_dict['wn_list_remapped'] = range(1, state_dict['total_wn'])  # leave xrange aside for now

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

        state_dict['total_cores'] += node.get('np')
        state_dict['max_np'] = max(state_dict['max_np'], node['np'])
        state_dict['offline_down_nodes'] += 1 if node['state'] in 'do' else 0
        try:
            state_dict['working_cores'] += len(node['core_job_map'])
        except KeyError as msg:
            pass

        try:
            cur_node_nr = int(node_str_digits)
        except ValueError:
            cur_node_nr = _nodename
        finally:
            state_dict['wn_list'].append(cur_node_nr)

    state_dict['node_subclusters'] = set(_all_letters)
    state_dict['_all_str_digits_with_empties'] = _all_str_digits_with_empties
    state_dict['all_str_digits'] = filter(lambda x: x != "", _all_str_digits_with_empties)
    state_dict['all_digits'] = [int(digit) for digit in state_dict['all_str_digits']]

    decide_remapping(pbs_nodes, state_dict)
    map_pbsnodes_to_wn_dicts(state_dict, pbs_nodes)
    if options.REMAP:
        state_dict['highest_wn'] = state_dict['total_wn']
        state_dict['wn_list'] = state_dict['wn_list_remapped']
        state_dict['wn_dict'] = state_dict['wn_dict_remapped']
    else:
        state_dict['highest_wn'] = max(state_dict['wn_list'])

    # fill in non-existent WN nodes (absent from pbsnodes file) with '?' and count them
    # is this even needed anymore?!
    for i in range(state_dict['highest_wn']):
        if i not in state_dict['wn_dict']:
            state_dict['wn_dict'][i] = '?'


    return state_dict, NAMED_WNS


def nodes_with_jobs(pbs_nodes):
    for _, pbs_node in pbs_nodes.iteritems():
        if 'core_job_map' in pbs_node:
            yield pbs_node


def map_pbsnodes_to_wn_dicts(state_dict, pbs_nodes):
    for (pbs_node, (idx, cur_node_nr)) in zip(pbs_nodes, enumerate(state_dict['wn_list'])):
        state_dict['wn_dict'][cur_node_nr] = pbs_node
        state_dict['wn_dict_remapped'][idx] = pbs_node



def read_pbsnodes_yaml(yaml_file):
    """
    Reads the pbsnodes yaml file and extracts the node information necessary to build the tables
    """
    state_dict = dict()
    state_dict['working_cores'] = 0
    state_dict['total_cores'] = 0
    state_dict['highest_wn'] = 0
    state_dict['offline_down_nodes'] = 0
    state_dict['max_np'] = 0
    state_dict['total_wn'] = 0  # == existing_nodes
    state_dict['node_nr'] = 0
    state_dict['wn_list'] = []
    state_dict['wn_list_remapped'] = []
    state_dict['node_subclusters'] = set()
    state_dict['wn_dict_remapped'] = {}  # { remapnr: [state, np, (core0, job1), (core1, job1), ....]}
    state_dict['wn_dict'] = {}
    state = ''
    NAMED_WNS = 0 if not options.FORCE_NAMES else 1  # <=1:numbered WNs, >1: names instead (e.g. fruits)

    re_nodename = r'(^[A-Za-z0-9-]+)(?=\.|$)'
    re_nodename_letters_only = r'(^[A-Za-z-]+)'
    # re_find_all_nodename_parts = '[A-Za-z]+|[0-9]+|[A-Za-z]+[0-9]+'

    with open(yaml_file, 'r') as fin:
        for line in fin:
            line = line.strip()
            if 'domainname:' in line:
                state_dict['total_wn'] += 1  # just count all nodes
                domain_name = line.split(': ')[1]
                nodename_match = re.search(re_nodename, domain_name)
                nodename_letters_only_match = re.search(re_nodename_letters_only, domain_name)

                if nodename_match:  # host (node) name is an alphanumeric
                    _node = nodename_match.group(0)
                    node_letters = '-'.join(re.findall(r'\D+', _node))
                    node_digits = "".join(re.findall(r'\d+', domain_name))

                    if node_digits:
                        state_dict['node_nr'] = int(node_digits)
                        # thus 1x18 becomes 118, posing problems later in range(1, state_dict[ 'total_wn'] (more repetitions)
                        state_dict['wn_list'].append(state_dict['node_nr'])
                    elif nodename_letters_only_match:  # for non-numbered WNs (eg. fruit names)
                        NAMED_WNS += 1
                        # increment node_nr but only if the next nr hasn't already shown up
                        state_dict['node_nr'] += 1 if state_dict['node_nr'] + 1 not in state_dict['wn_dict'] else False
                        state_dict['wn_list'].append(node_letters)
                        state_dict['wn_list'][:] = [unnumbered_wn.rjust(len(max(state_dict['wn_list']))) for unnumbered_wn in state_dict['wn_list'] if type(unnumbered_wn) is str]

                else:  # non_alphanumeric domain_name case?
                    node_letters = domain_name
                    state_dict['node_nr'] = 0
                    state_dict['wn_list'].append(state_dict['node_nr'])
                    import pdb; pdb.set_trace()
                    import sys; sys.exit(0)

                state_dict['node_subclusters'].add(node_letters)    # for non-uniform setups of WNs, eg g01... and n01...
                state_dict['wn_dict'][state_dict['node_nr']] = []
                state_dict['wn_dict_remapped'][state_dict['total_wn']] = []
                state_dict['wn_list_remapped'].append(state_dict['total_wn'])

            elif 'state: ' in line:
                nextchar = line.split()[1].strip("'")
                state += nextchar
                state_dict['wn_dict'][state_dict['node_nr']].append(nextchar)
                state_dict['wn_dict_remapped'][state_dict['total_wn']].append(nextchar)
                if (nextchar == 'd') | (nextchar == 'o'):
                    state_dict['offline_down_nodes'] += 1
            elif 'np:' in line or 'pcpus:' in line:
                np = line.split(': ')[1].strip()
                state_dict['wn_dict'][state_dict['node_nr']].append(np)
                state_dict['wn_dict_remapped'][state_dict['total_wn']].append(np)
                state_dict['max_np'] = max(int(np), state_dict['max_np'])
                state_dict['total_cores'] += int(np)

            elif ' core: ' in line:  # this should also work for OAR's yaml file
                core = line.split(': ')[1].strip()
                state_dict['working_cores'] += 1
            elif 'job: ' in line:
                job = str(line.split(': ')[1]).strip()
                state_dict['wn_dict'][state_dict['node_nr']].append((core, job))
                state_dict['wn_dict_remapped'][state_dict['total_wn']].append((core, job))

    state_dict['highest_wn'] = max(state_dict['wn_list']) if not NAMED_WNS else max(state_dict['wn_list_remapped'])
    '''
    fill in non-existent WN nodes (absent from pbsnodes file) with '?' and count them
    '''
    if len(state_dict['node_subclusters']) > 1:
        for i in range(1, state_dict['total_wn']):  # This state_dict['total_wn'] here is a counter of nodes, it's therefore the equivalent highest_wn for the remapped case
            if i not in state_dict['wn_dict_remapped']:
                state_dict['wn_dict_remapped'][i] = '?'
    else:
        for i in range(1, state_dict['highest_wn']):
            if i not in state_dict['wn_dict']:
                state_dict['wn_dict'][i] = '?'

    if NAMED_WNS:
        state_dict['wn_list'].sort()
        state_dict['wn_list_remapped'].sort()

    if min(state_dict['wn_list']) > 9000 and type(min(state_dict['wn_list'])) == int:
        # handle exotic cases of WN numbering starting VERY high
        state_dict['wn_list'] = [element - min(state_dict['wn_list']) for element in state_dict['wn_list']]
        options.REMAP = True
    if len(state_dict['wn_list']) < config['percentage'] * state_dict['highest_wn']:
        options.REMAP = True
    return state_dict, NAMED_WNS


def read_qstat_yaml(QSTAT_YAML_FILE):
    """
    reads qstat YAML file and populates four lists. Returns the lists
    """
    job_ids, usernames, statuses, queue_names = [], [], [], []
    with open(QSTAT_YAML_FILE, 'r') as finr:
        for line in finr:
            if line.startswith('JobId:'):
                job_ids.append(line.split()[1])
            elif line.startswith('UnixAccount:'):
                usernames.append(line.split()[1])
            elif line.startswith('S:'):
                statuses.append(line.split()[1])
            elif line.startswith('Queue:'):
                queue_names.append(line.split()[1])

    return job_ids, usernames, statuses, queue_names


def read_qstatq_yaml(QSTATQ_YAML_FILE):
    """
    Reads the generated qstatq yaml file and extracts the information necessary for building the user accounts and pool
    mappings table.
    """
    tempdict = {}
    qstatq_list = []
    with open(QSTATQ_YAML_FILE, 'r') as finr:
        for line in finr:
            line = line.strip()
            if ' queue_name:' in line:
                tempdict.setdefault('queue_name', line.split(': ')[1])
            elif line.startswith('Running:'):
                tempdict.setdefault('Running', line.split(': ')[1])
            elif line.startswith('Queued:'):
                tempdict.setdefault('Queued', line.split(': ')[1])
            elif line.startswith('Lm:'):
                tempdict.setdefault('Lm', line.split(': ')[1])
            elif line.startswith('State:'):
                tempdict.setdefault('State', line.split(': ')[1])
            elif not line:
                qstatq_list.append(tempdict)
                tempdict = {}
            elif line.startswith(('Total Running:')):
                total_running = line.split(': ')[1]
            elif line.startswith(('Total Queued:')):
                total_queued = line.split(': ')[1]
    return total_running, total_queued, qstatq_list


def create_job_accounting_summary(state_dict, total_running, total_queued, qstatq_list):
    if options.REMAP:
        print '=== WARNING: --- Remapping WN names and retrying heuristics... good luck with this... ---'
    print '\nPBS report tool. Please try: watch -d ' + QTOPPATH + '. All bugs added by sfranky@gmail.com. Cross fingers now...\n'
    print colorize('===> ', '#') + colorize('Job accounting summary', 'Nothing') + colorize(' <=== ', '#') + colorize('(Rev: 3000 $) %s WORKDIR = to be added', 'NoColourAccount') % (datetime.datetime.today()) #was: added\n
    print 'Usage Totals:\t%s/%s\t Nodes | %s/%s  Cores |   %s+%s jobs (R + Q) reported by qstat -q' % (state_dict['total_wn'] - state_dict['offline_down_nodes'], state_dict['total_wn'], state_dict['working_cores'], state_dict['total_cores'], int(total_running), int(total_queued))
    print 'Queues: | ',
    if options.COLOR == 'ON':
        for queue in qstatq_list:
            if queue['queue_name'] in color_of_account:
                print colorize(queue['queue_name'], queue['queue_name']) + ': ' + colorize(queue['Running'], queue['queue_name']) + '+' + colorize(queue['Queued'], queue['queue_name']) + ' |',
            else:
                print colorize(queue['queue_name'], 'Nothing') + ': ' + colorize(queue['Running'], 'Nothing') + '+' + colorize(queue['Queued'], 'Nothing') + ' |',
    else:    
        for queue in qstatq_list:
            print queue['queue_name'] + ': ' + queue['Running'] + '+' + queue['Queued'] + ' |',
    print '* implies blocked\n'


def calculate_job_counts(user_names, statuses):
    """
    Calculates and prints what is actually below the id|  R + Q /all | unix account etc line
    """
    state_abbrevs = {'R': 'running_of_user',
                 'Q': 'queued_of_user',
                 'C': 'cancelled_of_user',
                 'W': 'waiting_of_user',
                 'E': 'exiting_of_user'}

    _job_counts = create_job_counts(user_names, statuses, state_abbrevs)
    user_sorted_list = produce_user_list(user_names)
    expand_useraccounts_symbols(config, user_sorted_list)

    id_of_username = {}
    for ind, user_name in enumerate(user_sorted_list):
        id_of_username[user_name[0]] = config['possible_ids'][ind]

    # Calculates and prints what is actually below the id|  R + Q /all | unix account etc line
    # this is slower but shorter: 8mus
    for state_abbrev in state_abbrevs:
        missing_uids = set(id_of_username).difference(_job_counts[state_abbrevs[state_abbrev]])
        [_job_counts[state_abbrevs[state_abbrev]].setdefault(uid, 0) for uid in missing_uids]

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
    return _job_counts, user_sorted_list, id_of_username


def create_account_jobs_table(user_names, statuses):
    job_counts, user_sorted_list, id_of_username = calculate_job_counts(user_names, statuses)
    account_jobs_table = []
    for uid in user_sorted_list:
        all_of_user = job_counts['cancelled_of_user'][uid[0]] + \
                      job_counts['running_of_user'][uid[0]] + \
                      job_counts['queued_of_user'][uid[0]] + \
                      job_counts['waiting_of_user'][uid[0]] + \
                      job_counts['exiting_of_user'][uid[0]]
        account_jobs_table.append([id_of_username[uid[0]], job_counts['running_of_user'][uid[0]], job_counts['queued_of_user'][
            uid[0]], all_of_user,uid])
    account_jobs_table.sort(key=itemgetter(3), reverse=True)  # sort by All jobs
    return account_jobs_table, id_of_username


def create_job_counts(user_names, statuses, state_abbrevs):
    """
    counting of R, Q, C, W, E attached to each user
    """
    job_counts = dict()
    for value in state_abbrevs.values():
        job_counts[value] = dict()

    for user_name, status in zip(user_names, statuses):
        job_counts[state_abbrevs[status]][user_name] = job_counts[state_abbrevs[status]].get(user_name, 0) + 1

    for user_name in job_counts['running_of_user']:
        job_counts['queued_of_user'].setdefault(user_name, 0)
        job_counts['cancelled_of_user'].setdefault(user_name, 0)
        job_counts['waiting_of_user'].setdefault(user_name, 0)
        job_counts['exiting_of_user'].setdefault(user_name, 0)

    return job_counts


def produce_user_list(_user_names):
    """
    produces the decrementing list of users in the user accounts and poolmappings table
    """
    occurence_dict = {}
    for user_name in _user_names:
        occurence_dict[user_name] = _user_names.count(user_name)
    user_sorted_list = sorted(occurence_dict.items(), key=itemgetter(1), reverse=True)
    return user_sorted_list


def expand_useraccounts_symbols(config, user_sorted_list):
    """
    In case there are more users than the sum number of all numbers and small/capital letters of the alphabet
    """
    MAX_UNIX_ACCOUNTS = 87  # was : 62
    if len(user_sorted_list) > MAX_UNIX_ACCOUNTS:
        for i in xrange(MAX_UNIX_ACCOUNTS, len(user_sorted_list) + MAX_UNIX_ACCOUNTS):  # was: # for i in xrange(MAX_UNIX_ACCOUNTS, len(user_sorted_list) + MAX_UNIX_ACCOUNTS):
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
                print 'There seems to be a problem with the qstat output. A JobID has gone rogue (namely, ' + str(KeyErrorValue) +'). Please check with the System Administrator.'
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


def insert_sep(original, separator, pos, stopaftern = 0):
    '''
    insert separator into original (string) every posth position, optionally stopping after stopafter times.
    '''
    pos = int(pos)
    if pos != 0: # default value is zero, means no vertical separators
        sep = original[:]  # insert initial vertical separator
        if stopaftern == 0:
            times = len(original) / pos
        else:
            times = stopaftern
        sep = sep[:pos] + separator + sep[pos:] 
        for i in range(2, times+1):
            sep = sep[:pos * i + i-1] + separator + sep[pos * i + i-1:] 
        sep = separator + sep  # insert initial vertical separator
        return sep
    else: # no separators
        return original


def calculate_Total_WNIDLine_Width(_node_count): # (total_wn) in case of multiple state_dict['node_subclusters']
    """
    calculates the worker node ID number line widths (expressed by hxxxx's)
    h1000 is the thousands' line
    h0100 is the hundreds' line
    and so on
    # h1000, h0100, h0010, h0001 = '','','',''
    """
    hxxxx = {}
    # hxxxx['h1000'] = h1000
    # hxxxx['h0100'] = h0100
    # hxxxx['h0010'] = h0010
    # hxxxx['h0001'] = h0001

    if _node_count < 10:
        u_ = '123456789'
        hxxxx['h0001'] = u_[:_node_count]

    elif _node_count < 100:
        d_ = '0' * 9 + '1' * 10 + '2' * 10 + '3' * 10 + '4' * 10 + '5' * 10 + '6' * 10 + '7' * 10 + '8' * 10 + '9' * 10
        u_ = '1234567890' * 10
        hxxxx['h0010'] = d_[:_node_count]
        hxxxx['h0001'] = u_[:_node_count]

    elif _node_count < 1000:
        cent = int(str(_node_count)[0])
        dec = int(str(_node_count)[1])
        unit = int(str(_node_count)[2])

        c_ = str(0) * 99
        for i in range(1, cent):
            c_ += str(i) * 100
        c_ += str(cent) * (int(dec)) * 10 + str(cent) * (int(unit) + 1)
        hxxxx['h0100'] = c_[:_node_count]

        d_ = '0' * 9 + '1' * 10 + '2' * 10 + '3' * 10 + '4' * 10 + '5' * 10 + '6' * 10 + '7' * 10 + '8' * 10 + '9' * 10
        d__ = d_ + (cent - 1) * (str(0) + d_) + str(0)
        d__ += d_[:int(str(dec) + str(unit))]
        hxxxx['h0010'] = d__[:_node_count]

        uc = '1234567890' * 100
        hxxxx['h0001'] = uc[:_node_count]

    elif _node_count > 1000:
        thou = int(str(_node_count)[0])
        cent = int(str(_node_count)[1])
        dec = int(str(_node_count)[2])
        unit = int(str(_node_count)[3])

        hxxxx['h1000'] += str(0) * 999
        for i in range(1, thou):
            hxxxx['h1000'] += str(i) * 1000
        hxxxx['h1000'] += str(thou) * ((int(cent)) * 100 + (int(dec)) * 10 + (int(unit) + 1))

        c_ = '0' * 99 + '1' * 100 + '2' * 100 + '3' * 100 + '4' * 100 + '5' * 100 + '6' * 100 + '7' * 100 + '8' * 100 + '9' * 100
        c__ = '0' * 100 + '1' * 100 + '2' * 100 + '3' * 100 + '4' * 100 + '5' * 100 + '6' * 100 + '7' * 100 + '8' * 100 + '9' * 100
        hxxxx['h0100'] = c_

        for i in range(1, thou):
            hxxxx['h0100'] += c__
        else:
            hxxxx['h0100'] += c__[:int(str(cent) + str(dec) +str(unit))+1]

        d_ = '0' * 10 + '1' * 10 + '2' * 10 + '3' * 10 + '4' * 10 + '5' * 10 + '6' * 10 + '7' * 10 + '8' * 10 + '9' * 10
        d__ = d_ * thou * 10  # cent * 10
        d___ = d_ * (cent - 1)
        hxxxx['h0010'] = '0' * 9 + '1' * 10 + '2' * 10 + '3' * 10 + '4' * 10 + '5' * 10 + '6' * 10 + '7' * 10 + '8' * 10 + '9' * 10
        hxxxx['h0010'] += d__
        hxxxx['h0010'] += d___
        hxxxx['h0010'] += d_[:int(str(dec) + str(unit)) + 1]

        uc = '1234567890' * 1000
        hxxxx['h0001'] = uc[:_node_count]
    return hxxxx


def find_matrices_width(wn_number, wn_list, state_dict, term_columns, DEADWEIGHT=15):
    """
    masking/clipping functionality: if the earliest node number is high (e.g. 130), the first 129 WNs need not show up.
    case 1: wn_number is RemapNr, WNList is WNListRemapped
    case 2: wn_number is BiggestWrittenNode, WNList is WNList
    DEADWEIGHT = 15  # standard columns' width on the right of the CoreX map
    """
    start = 0
    if (options.MASKING is True) and min(wn_list) > config['min_masking_threshold'] and type(min(wn_list)) == str: # in case of named instead of numbered WNs
        pass            
    elif (options.MASKING is True) and min(wn_list) > config['min_masking_threshold'] and type(min(wn_list)) == int:
        start = min(wn_list) - 1   #exclude unneeded first empty nodes from the matrix

    # Extra matrices may be needed if the WNs are more than the screen width can hold.
    if wn_number > start: # start will either be 1 or (masked >= config['min_masking_threshold'] + 1)
        extra_matrices_nr = abs(wn_number - start + 10) / term_columns
    elif wn_number < start and len(state_dict['node_subclusters']) > 1: # Remapping
        extra_matrices_nr = (wn_number + 10) / term_columns
    else:
        print "This is a case I didn't foresee (wn_number vs start vs state_dict['node_subclusters'])"

    if config['user_cut_matrix_width']: # if the user defines a custom cut (in the configuration file)
        stop = start + config['user_cut_matrix_width']
        return start, stop, wn_number/config['user_cut_matrix_width']
    elif extra_matrices_nr:  # if more matrices are needed due to lack of space, cut every matrix so that if fits to screen
        stop = start + term_columns - DEADWEIGHT
        return (start, stop, extra_matrices_nr)
    else: # just one matrix, small cluster!
        stop = start + wn_number
        return (start, stop, 0)


def print_WN_ID_lines(start, stop, wn_number, hxxxx):
    """
    h1000 is a header for the 'thousands',
    h0100 is a header for the 'hundreds',
    h0010 is a header for the 'tens',
    h0001 is a header for the 'units' in the WN_ID lines
    wn_number determines the number of WN ID lines needed  (1/2/3/4?)
    """
    just_name_dict = {}
    if not NAMED_WNS:
        if wn_number < 10:
            print insert_sep(hxxxx['h0001'][start:stop], SEPARATOR, options.WN_COLON) + '={__WNID__}'

        elif wn_number < 100:
            print insert_sep(hxxxx['h0010'][start:stop], SEPARATOR, options.WN_COLON) + '={_Worker_}'
            print insert_sep(hxxxx['h0001'][start:stop], SEPARATOR, options.WN_COLON) + '={__Node__}'

        elif wn_number < 1000:
            print insert_sep(hxxxx['h0100'][start:stop], SEPARATOR, options.WN_COLON) + '={_Worker_}'
            print insert_sep(hxxxx['h0010'][start:stop], SEPARATOR, options.WN_COLON) + '={__Node__}'
            print insert_sep(hxxxx['h0001'][start:stop], SEPARATOR, options.WN_COLON) + '={___ID___}'

        elif wn_number > 1000:
            print insert_sep(hxxxx['h1000'][start:stop], SEPARATOR, options.WN_COLON) + '={________}'
            print insert_sep(hxxxx['h0100'][start:stop], SEPARATOR, options.WN_COLON) + '={_Worker_}'
            print insert_sep(hxxxx['h0010'][start:stop], SEPARATOR, options.WN_COLON) + '={__Node__}'
            print insert_sep(hxxxx['h0001'][start:stop], SEPARATOR, options.WN_COLON) + '={___ID___}'
    elif NAMED_WNS or options.FORCE_NAMES:  # names (e.g. fruits) instead of numbered WNs
        raise NotImplementedError
        color = 0
        highlight = {0: 'cmsplt', 1: 'Red'}
        for line, _ in enumerate(max(state_dict['wn_list'])):
            just_name_dict[line] = ''
        for column, _1 in enumerate(state_dict['wn_list']):
            for line, _2 in enumerate(max(state_dict['wn_list'])):
                try:
                    letter = state_dict['wn_list'][column][line]
                except TypeError:
                    letter = ' '
                just_name_dict[line] += colorize(letter, highlight[color])
            color = 0 if color == 1 else 1
        for line, _ in enumerate(max(state_dict['wn_list'])):
            print just_name_dict[line] + '={__WNID__}'


def calculate_remaining_matrices(node_state,
                                 extra_matrices_nr,
                                 state_dict,
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
            _print_end = min(_print_end, state_dict['total_wn'])
        else:
            _print_end = min(_print_end, state_dict['highest_wn'])

        if len(state_dict['node_subclusters']) == 1:
            print_WN_ID_lines(print_start, _print_end, state_dict['total_wn'], hxxxx)
        elif len(state_dict['node_subclusters']) > 1:  # not sure that this works, these two funcs seem terribly similar!!
            print_WN_ID_lines(print_start, _print_end, state_dict['total_wn'], hxxxx)

        print insert_sep(node_state[print_start:_print_end], SEPARATOR, options.WN_COLON) + '=Node state'
        for ind, k in enumerate(cpu_core_dict):
            color_cpu_core_list = list(insert_sep(cpu_core_dict['Cpu' + str(ind) + 'line'][print_start:_print_end], SEPARATOR, options.WN_COLON))
            nocolor_linelength = len(''.join(color_cpu_core_list))
            color_cpu_core_list = [colorize(elem, account_nrless_of_id[elem]) for elem in color_cpu_core_list if elem in account_nrless_of_id]
            line = ''.join(color_cpu_core_list)
            '''
            if the first matrix has 10 machines with 64 cores, and the rest 190 machines have 8 cores, don't print the non-existent
            56 cores from the next matrix on.
            IMPORTANT: not working if vertical separators are present!
            '''
            if '\x1b[1;30m#\x1b[1;m' * nocolor_linelength not in line:
                print line + colorize('=core' + str(ind), 'NoColourAccount')


def create_user_accounts_pool_mappings(accounts_mappings, color_of_account):
    print colorize('\n===> ', '#') + colorize('User accounts and pool mappings', 'Nothing') + colorize(' <=== ', '#') + colorize('("all" includes those in C and W states, as reported by qstat)', 'NoColourAccount')
    print ' id |  R   +   Q  /  all |    unix account | Grid certificate DN (this info is only available under elevated privileges)'

    for line in accounts_mappings:
        print_string = '%3s | %4s + %4s / %4s | %15s |' % (line[0], line[1], line[2], line[3], line[4][0])
        for account in color_of_account:
            if line[4][0].startswith(account) and options.COLOR == 'ON':
                print_string = '%15s | %16s + %16s / %16s | %27s %4s' % (colorize(str(line[0]), account), colorize(str(line[1]), account), colorize(str(line[2]), account), colorize(str(line[3]), account), colorize(str(line[4][0]), account), colorize(SEPARATOR, 'NoColourAccount'))
            elif line[4][0].startswith(account) and options.COLOR == 'OFF':
                print_string = '%2s | %3s + %3s / %3s | %14s |' %(colorize(line[0], account), colorize(str(line[1]), account), colorize(str(line[2]), account), colorize(str(line[3]), account), colorize(line[4][0], account))
            else:
                pass
        print print_string


def print_core_lines(cpu_core_dict, accounts_mappings, print_start, print_end):
    """
    prints all coreX lines
    """
    account_nrless_of_id = {}
    for line in accounts_mappings:
        just_name = re.split('[0-9]+', line[4][0])[0]
        account_nrless_of_id[line[0]] = just_name if just_name in color_of_account else 'NoColourAccount'

    account_nrless_of_id['#'] = '#'
    account_nrless_of_id['_'] = '_'
    account_nrless_of_id[SEPARATOR] = 'NoColourAccount'
    for ind, k in enumerate(cpu_core_dict):
        color_cpu_core_list = list(insert_sep(cpu_core_dict['Cpu' + str(ind) + 'line'][print_start:print_end], SEPARATOR, options.WN_COLON))
        color_cpu_core_list = [colorize(elem, account_nrless_of_id[elem]) for elem in color_cpu_core_list if elem in account_nrless_of_id]
        line = ''.join(color_cpu_core_list)
        print line + colorize('=core' + str(ind), 'NoColourAccount')

    return account_nrless_of_id


def calc_cpu_lines(state_dict, id_of_username):
    _cpu_core_dict = {}
    max_np_range = []
    user_of_job_id = dict(izip(job_ids, user_names))

    for core_nr in range(state_dict['max_np']):
        _cpu_core_dict['Cpu' + str(core_nr) + 'line'] = ''  # Cpu0line, Cpu1line, Cpu2line, .. = '','','', ..
        max_np_range.append(str(core_nr))

    for _node in state_dict['wn_dict']:
        state_np_corejob = state_dict['wn_dict'][_node]
        _cpu_core_dict = fill_cpucore_columns(state_np_corejob, _cpu_core_dict, id_of_username, max_np_range, user_of_job_id)

    return _cpu_core_dict


def calculate_wn_occupancy(state_dict, user_names, statuses):
    """
    Prints the Worker Nodes Occupancy table.
    if there are non-uniform WNs in pbsnodes.yaml, e.g. wn01, wn02, gn01, gn02, ...,  remapping is performed.
    Otherwise, for uniform WNs, i.e. all using the same numbering scheme, wn01, wn02, ... proceeds as normal.
    Number of Extra tables needed is calculated inside the calculate_Total_WNIDLine_Width function below
    """
    term_columns = calculate_split_screen_size()
    account_jobs_table, id_of_username = create_account_jobs_table(user_names, statuses)

    cpu_core_dict = calc_cpu_lines(state_dict, id_of_username)
    print colorize('===> ', '#') + colorize('Worker Nodes occupancy', 'Nothing') + colorize(' <=== ', '#') + colorize('(you can read vertically the node IDs; nodes in free state are noted with - )', 'NoColourAccount')

    highest_wn, wn_dict, wn_list = state_dict['highest_wn'], state_dict['wn_dict'], state_dict['wn_list']

    hxxxx = calculate_Total_WNIDLine_Width(highest_wn)
    node_state = ''
    for node in wn_dict:
        node_state += wn_dict[node]['state']
    (print_start, print_end, extra_matrices_nr) = find_matrices_width(highest_wn, wn_list, state_dict, term_columns)
    print_WN_ID_lines(print_start, print_end, highest_wn, hxxxx)
    print insert_sep(node_state[print_start:print_end], SEPARATOR, options.WN_COLON) + '=Node state'

    account_nrless_of_id = print_core_lines(cpu_core_dict, account_jobs_table, print_start, print_end)
    calculate_remaining_matrices(node_state, extra_matrices_nr, state_dict, cpu_core_dict, print_end, account_nrless_of_id,
                                 hxxxx, term_columns)
    return account_jobs_table


def reset_yaml_files():
    """
    empties the files with every run of the python script
    """
    for _file in [PBSNODES_YAML_FILE, QSTATQ_YAML_FILE, QSTAT_YAML_FILE]:
        fin = open(_file, 'w')
        fin.close()


def load_yaml_config(path):
    try:
        config = yaml.safe_load(open(path + "/qtopconf.yaml"))
    except yaml.YAMLError, exc:
        if hasattr(exc, 'problem_mark'):
            mark = exc.problem_mark
            print "Error position: (%s:%s)" % (mark.line+1, mark.column+1)

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
    DIFFERENT_QSTAT_FORMAT_FLAG = 0

    HOMEPATH = os.path.expanduser('~/PycharmProjects')
    QTOPPATH = os.path.expanduser('~/PycharmProjects/qtop')  # qtoppath: ~/qtop/qtop
    SOURCEDIR = options.SOURCEDIR  # as set by the '-s' switch

    config = load_yaml_config(QTOPPATH)
    SEPARATOR = config['separator']  # alias

    # Name files according to unique pid
    PBSNODES_YAML_FILE = 'pbsnodes_%s.yaml' % os.getpid()
    QSTATQ_YAML_FILE = 'qstat-q_%s.yaml' % os.getpid()
    QSTAT_YAML_FILE = 'qstat_%s.yaml' % os.getpid()

    os.chdir(SOURCEDIR)
    # Location of read and created files
    PBSNODES_ORIG_FILE = [f for f in os.listdir(os.getcwd()) if f.startswith('pbsnodes') and not f.endswith('.yaml')][0]
    QSTATQ_ORIG_FILE = [f for f in os.listdir(os.getcwd()) if (f.startswith('qstat_q') or f.startswith('qstatq') or f.startswith('qstat-q') and not f.endswith('.yaml'))][0]
    QSTAT_ORIG_FILE = [f for f in os.listdir(os.getcwd()) if f.startswith('qstat.') and not f.endswith('.yaml')][0]

    #  MAIN ###################################
    reset_yaml_files()
    make_pbsnodes_yaml(PBSNODES_ORIG_FILE, PBSNODES_YAML_FILE)
    make_qstatq_yaml(QSTATQ_ORIG_FILE, QSTATQ_YAML_FILE)
    make_qstat_yaml(QSTAT_ORIG_FILE, QSTAT_YAML_FILE)

    pbs_nodes = read_pbsnodes_yaml_into_list(PBSNODES_YAML_FILE)  # was: read_pbsnodes_yaml_into_dict(PBSNODES_YAML_FILE)
    # was: state_dict, NAMED_WNS = read_pbsnodes_yaml(PBSNODES_YAML_FILE)
    total_running, total_queued, qstatq_list = read_qstatq_yaml(QSTATQ_YAML_FILE)
    job_ids, user_names, statuses, queue_names = read_qstat_yaml(QSTAT_YAML_FILE)  # populates 4 lists

    state_dict, NAMED_WNS = calculate_stuff(pbs_nodes)

    create_job_accounting_summary(state_dict, total_running, total_queued, qstatq_list)
    account_jobs_table = calculate_wn_occupancy(state_dict, user_names, statuses)
    create_user_accounts_pool_mappings(account_jobs_table, color_of_account)

    print '\nThanks for watching!'

    os.chdir(SOURCEDIR)
