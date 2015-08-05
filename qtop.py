#!/usr/bin/env python

################################################
#              qtop v.0.6.7                    #
#     Licensed under MIT-GPL licenses          #
#                     Fotis Georgatos          #
#                     Sotiris Fragkiskos       #
################################################

"""
changelog:
=========
0.6.7: created yaml files now have the pid appended to the filename
       pbs-related functions (which create the respective yaml files) have moved to a dedicated module 
       took out state_dict['highest_core_busy'], seemed useless (and unused)
       a separate read_qstatq_yaml function added, for consistency (removed from qstatq2yaml)
       change qstatq_list from list of tuples to list of dictionaries
       offline_down_nodes was moved from pbs.pbsnodes2yaml to read_pbsnodes_yaml
0.6.6: got rid of all global variables (experimental)
0.6.5: PBS now supported
0.6.4: lines that don't contain *any* actual core are now not printed in the matrices.
0.6.3: optional stopping of vertical separators (every 'n' position for x times)
       additional vertical separator in the beginning
0.6.2: WN matrix width bug ironed out.
0.6.1: Custom-cut matrices (horizontally, too!), -o switch
0.5.2: Custom-cut matrices (vertically, not horizontally), width set by user.
0.5.1: If more than 20% of the WNs are empty, perform a blind remap.
       Code Cleanup
0.5.0: Major rewrite of matrices calculation
       fixed: true blind remapping !!
       exotic cases of very high numbering schemes now handled
       more qstat entries successfully parsed
       case of many unix accounts (>62) now handled
0.4.1: now understands additional probable names for pbsnodes,qstat and qstat-q data files
0.4.0: corrected colorless switch to have ON/OFF option (default ON)
       bugfixes (qstat_q didn't recognize some faulty cpu time entries)
       now descriptions are in white, as before.
       Queues in the job accounting summary section are now coloured
0.3.0: command-line arguments (mostly empty for now)!
       non-numbered WNs can now be displayed instead of numbered WN IDs
       fixed issue with single named WN
       better regex pattern and algorithm for catching complicated numbered WN domain names
       implement colorless switch (-c)
0.2.9: handles cases of non-numbered WNs (e.g. fruit names)
       parses more complex domain names (with more than one dash)
       correction in WN ID numbers display (tens were problematic for larger numbers)
0.2.8: colour implementation for all of the tables
0.2.7: Exiting when there are two jobs on the same core reported on pbsnodes (remapping functionality to be added)
       Number of WNs >1000 is now handled
0.2.6: fixed some names not being detected (%,= chars missing from regex)
       changed name to qtop, introduced configuration file qtop.conf and
       colormap file qtop.colormap
0.2.5: Working Cores added in Usage Totals
       Feature added: map now splits into two if terminal width is smaller than
        the Worker Node number
0.2.4: implemented some stuff from PEP8
       un-hardwired the file paths
       refactored code around cpu_core_dict functionality (responsible for drawing
        the map)
0.2.3: corrected regex search pattern in make_qstat to recognize usernames like spec101u1 (number followed by number followed by letter) now handles non-uniform setups
        R + Q / all: all did not display everything (E status)
0.2.2: masking/clipping functionality (when nodes start from e.g. wn101, empty columns 1-100 are ommited)
0.2.1: Hashes displaying when the node has less cores than the max declared by a WN (its np variable)
0.2.0: unix accounts are now correctly ordered
0.1.9: All CPU lines displaying correctly
0.1.8: unix account id assignment to CPU0, 1 implemented
0.1.7: ReadQstatQ function (write in yaml format using Pyyaml)
       output up to Node state!
0.1.6: ReadPbsNodes function (write in yaml format using Pyyaml)
0.1.5: implemented saving to 3 separate files, QSTAT_ORIG_FILE, QSTATQ_ORIG_FILE, PBSNODES_ORIG_FILE
0.1.4: some "wiremelting" concerning the save directory
0.1.3: fixed tabs-to-spaces. Formatting should be correct now.
       Now each state is saved in a separate file in a results folder
0.1.2: script reads qtop-input.out files from each job and displays status for each job
0.1.1: changed implementation in get_state()
0.1.0: just read a pbsnodes-a output file and gather the results in a single line
"""

from operator import itemgetter
from optparse import OptionParser
import datetime
import os
import re
import yaml
# modules
from pbs import make_pbsnodes_yaml, make_qstatq_yaml, make_qstat_yaml
from colormap import color_of_account, code_of_color
import variables

parser = OptionParser() # for more details see http://docs.python.org/library/optparse.html
parser.add_option("-a", "--blindremapping", action="store_true", dest="BLINDREMAP", default=False, help="This is used in situations where node names are not a pure arithmetic sequence (eg. rocks clusters)")
parser.add_option("-c", "--COLOR", action="store", dest="COLOR", default='ON', choices=['ON', 'OFF'], help="Enable/Disable color in qtop output. Use it with an ON/OFF switch: -c ON or -c OFF")
parser.add_option("-f", "--setCOLORMAPFILE", action="store", type="string", dest="COLORFILE")
parser.add_option("-m", "--noMasking", action="store_false", dest="MASKING", default=True, help="Don't mask early empty Worker Nodes. (default setting is: if e.g. the first 30 WNs are unused, counting starts from 31).")
parser.add_option("-o", "--SetVerticalSeparatorXX", action="store", dest="WN_COLON", default=0, help="Put vertical bar every WN_COLON nodes.")
parser.add_option("-s", "--SetSourceDir", dest="SOURCEDIR", help="Set the source directory where pbsnodes and qstat reside")
parser.add_option("-z", "--quiet", action="store_false", dest="verbose", default=True, help="don't print status messages to stdout. Not doing anything at the moment.")
parser.add_option("-F", "--ForceNames", action="store_true", dest="FORCE_NAMES", default=False, help="force names to show up instead of numbered WNs even for very small numbers of WNs")

(options, args) = parser.parse_args()

# TODO make this work with py files instead of qtop.colormap files
# if not options.COLORFILE:
#     options.COLORFILE = os.path.expanduser('~/qtop/qtop/qtop.colormap')


def colorize(text, pattern):
    """print text colored according to its unix account colors"""
    if options.COLOR == 'ON':
        return "\033[" + code_of_color[color_of_account[pattern]] + "m" + text + "\033[1;m"
    else:
        return text


def read_pbsnodes_yaml(yaml_file, names_flag):
    """
    extracts highest node number, online nodes
    global JUST_NAMES_FLAG   --> internal copy is now names_flag
    """
    state_dict = {}
    state_dict['existing_nodes'] = 0
    state_dict['working_cores'] = 0
    state_dict['total_cores'] = 0
    state_dict['biggest_written_node'] = 0
    state_dict['offline_down_nodes'] = 0
    state_dict['max_np'] = 0
    state_dict['remap_nr'] = 0
    state_dict['node_nr'] = 0
    state_dict['wn_list'] = []
    state_dict['wn_list_remapped'] = []
    state_dict['node_subclusters'] = set()
    state_dict['all_wns_remapped_dict'] = {}  # { remapnr: [state, np, (core0, job1), (core1, job1), ....]}
    state_dict['all_wns_dict'] = {}
    state = ''
    with open(yaml_file, 'r') as fin:
        for line in fin:
            line.strip()
            search_domain_name = 'domainname: ' + '(\w+-?\w+([.-]\w+)*)'
            search_node_nr = '([A-Za-z0-9-]+)(?=\.|$)'
            search_node_nr_find = '[A-Za-z]+|[0-9]+|[A-Za-z]+[0-9]+'
            search_just_letters = '(^[A-Za-z-]+)'
            if re.search(search_domain_name, line) is not None:   # line contains domain name
                m = re.search(search_domain_name, line)
                domain_name = m.group(1)
                state_dict['remap_nr'] += 1
                '''
                extract highest node number, online nodes
                '''
                state_dict['existing_nodes'] += 1    # nodes as recorded on PBSNODES_ORIG_FILE
                if re.search(search_node_nr, domain_name) is not None:  # if a number and domain are found
                    n = re.search(search_node_nr, domain_name)
                    node_inits = n.group(0)
                    name_groups = re.findall(search_node_nr_find, node_inits)
                    node_inits = '-'.join(name_groups[0:-1])
                    if name_groups[-1].isdigit():
                        state_dict['node_nr'] = int(name_groups[-1])
                    elif len(name_groups) == 1: # if e.g. WN name is just 'gridmon'
                        if re.search(search_just_letters, domain_name) is not None:  # for non-numbered WNs (eg. fruit names)
                            names_flag += 1
                            n = re.search(search_just_letters, domain_name)
                            node_inits = n.group(1)
                            state_dict['node_nr'] += 1
                            state_dict['node_subclusters'].add(node_inits)    # for non-uniform setups of WNs, eg g01... and n01...
                            state_dict['all_wns_dict'][state_dict['node_nr']] = []
                            state_dict['all_wns_remapped_dict'][state_dict['remap_nr']] = []
                            if state_dict['node_nr'] > state_dict['biggest_written_node']:
                                state_dict['biggest_written_node'] = state_dict['node_nr']
                            state_dict['wn_list'].append(node_inits)
                            state_dict['wn_list'][:] = [unnumbered_wn.rjust(len(max(state_dict['wn_list']))) for unnumbered_wn in state_dict['wn_list'] if type(unnumbered_wn) is str ]
                            state_dict['wn_list_remapped'].append(state_dict['remap_nr'])
                    elif len(name_groups) == 2 and not name_groups[-1].isdigit() and not name_groups[-2].isdigit():
                        name_groups = '-'.join(name_groups)
                        if re.search(search_just_letters, domain_name) is not None:  # for non-numbered WNs (eg. fruit names)
                           names_flag += 1
                           n = re.search(search_just_letters, domain_name)
                           node_inits = n.group(1)
                           state_dict['node_nr'] += 1
                           state_dict['node_subclusters'].add(node_inits)    # for non-uniform setups of WNs, eg g01... and n01...
                           state_dict['all_wns_dict'][state_dict['node_nr']] = []
                           state_dict['all_wns_remapped_dict'][state_dict['remap_nr']] = []
                           if state_dict['node_nr'] > state_dict['biggest_written_node']:
                               state_dict['biggest_written_node'] = state_dict['node_nr']
                           state_dict['wn_list'].append(node_inits)
                           state_dict['wn_list'][:] = [unnumbered_wn.rjust(len(max(state_dict['wn_list']))) for unnumbered_wn in state_dict['wn_list'] if type(unnumbered_wn) is str ]
                           state_dict['wn_list_remapped'].append(state_dict['remap_nr'])
                    elif name_groups[-2].isdigit():
                        state_dict['node_nr'] = int(name_groups[-2])
                    else:
                        state_dict['node_nr'] = int(name_groups[-3])
                    # print 'NamedGroups are: ', name_groups #####DEBUGPRINT2
                    state_dict['node_subclusters'].add(node_inits)    # for non-uniform setups of WNs, eg g01... and n01...
                    state_dict['all_wns_dict'][state_dict['node_nr']] = []
                    state_dict['all_wns_remapped_dict'][state_dict['remap_nr']] = []
                    if state_dict['node_nr'] > state_dict['biggest_written_node']:
                        state_dict['biggest_written_node'] = state_dict['node_nr']
                    if names_flag <= 1:
                        state_dict['wn_list'].append(state_dict['node_nr'])
                    state_dict['wn_list_remapped'].append(state_dict['remap_nr'])
                elif re.search(search_just_letters, domain_name) is not None:  # for non-numbered WNs (eg. fruit names)
                    names_flag += 1
                    n = re.search(search_just_letters, domain_name)
                    node_inits = n.group(1)
                    state_dict['node_nr'] += 1
                    state_dict['node_subclusters'].add(node_inits)    # for non-uniform setups of WNs, eg g01... and n01...
                    state_dict['all_wns_dict'][state_dict['node_nr']] = []
                    state_dict['all_wns_remapped_dict'][state_dict['remap_nr']] = []
                    if state_dict['node_nr'] > state_dict['biggest_written_node']:
                        state_dict['biggest_written_node'] = state_dict['node_nr']
                    state_dict['wn_list'].append(node_inits)
                    state_dict['wn_list'][:] = [unnumbered_wn.rjust(len(max(state_dict['wn_list']))) for unnumbered_wn in state_dict['wn_list']]
                    state_dict['wn_list_remapped'].append(state_dict['remap_nr'])
                else:
                    state_dict['node_nr'] = 0
                    node_inits = domain_name
                    state_dict['all_wns_dict'][state_dict['node_nr']] = []
                    state_dict['all_wns_remapped_dict'][state_dict['remap_nr']] = []
                    state_dict['node_subclusters'].add(node_inits)    # for non-uniform setups of WNs, eg g01... and n01...
                    if state_dict['node_nr'] > state_dict['biggest_written_node']:
                        state_dict['biggest_written_node'] = state_dict['node_nr'] + 1
                    state_dict['wn_list'].append(state_dict['node_nr'])
                    state_dict['wn_list_remapped'].append(state_dict['remap_nr'])
            elif 'state: ' in line:
                nextchar = line.split()[1].strip("'")
                if nextchar == 'f':
                    state += '-'
                    state_dict['all_wns_dict'][state_dict['node_nr']].append('-')
                    state_dict['all_wns_remapped_dict'][state_dict['remap_nr']].append('-')
                elif (nextchar == 'd') | (nextchar == 'o'):
                    state += nextchar
                    state_dict['offline_down_nodes'] += 1
                    state_dict['all_wns_dict'][state_dict['node_nr']].append(nextchar)
                    state_dict['all_wns_remapped_dict'][state_dict['remap_nr']].append(nextchar)
                else:
                    state += nextchar
                    state_dict['all_wns_dict'][state_dict['node_nr']].append(nextchar)
                    state_dict['all_wns_remapped_dict'][state_dict['remap_nr']].append(nextchar)

            elif 'np:' in line or 'pcpus:' in line:
                np = line.split(': ')[1].strip()
                state_dict['all_wns_dict'][state_dict['node_nr']].append(np)
                state_dict['all_wns_remapped_dict'][state_dict['remap_nr']].append(np)
                if int(np) > int(state_dict['max_np']):
                    state_dict['max_np'] = int(np)
                state_dict['total_cores'] += int(np)

            elif ' core: ' in line: # this should also work for OAR's yaml file
                core = line.split(': ')[1].strip()
                state_dict['working_cores'] += 1
                # if int(core) > int(state_dict['highest_core_busy']): # state_dict['highest_core_busy'] doesn't look like it's doing anything!!
                #     state_dict['highest_core_busy'] = int(core)
            elif 'job: ' in line:
                job = str(line.split(': ')[1]).strip()
                state_dict['all_wns_dict'][state_dict['node_nr']].append((core, job))
                state_dict['all_wns_remapped_dict'][state_dict['remap_nr']].append((core, job))
        # state_dict['highest_core_busy'] += 1

    '''
    fill in non-existent WN nodes (absent from pbsnodes file) with '?' and count them
    '''
    if len(state_dict['node_subclusters']) > 1:
        for i in range(1, state_dict['remap_nr']): # This state_dict['remap_nr'] here is the LAST remapped node, it's the equivalent biggest_written_node for the remapped case
            if i not in state_dict['all_wns_remapped_dict']:
                state_dict['all_wns_remapped_dict'][i] = '?'
    elif len(state_dict['node_subclusters']) == 1:
        for i in range(1, state_dict['biggest_written_node']):
            if i not in state_dict['all_wns_dict']:
                state_dict['all_wns_dict'][i] = '?'

    if names_flag <= 1:
        state_dict['wn_list'].sort()
        state_dict['wn_list_remapped'].sort()

    if min(state_dict['wn_list']) > 9000 and type(min(state_dict['wn_list'])) == int: # handle exotic cases of WN numbering starting VERY high
        state_dict['wn_list'] = [element - min(state_dict['wn_list']) for element in state_dict['wn_list']]
        options.BLINDREMAP = True 
    if len(state_dict['wn_list']) < config['percentage'] * state_dict['biggest_written_node']:
        options.BLINDREMAP = True
    return state_dict, names_flag
    # return state_dict['existing_nodes'], state_dict['working_cores'], state_dict['total_cores'], biggest_written_node, state_dict['all_wns_dict'], state_dict['wn_list_remapped'], state_dict['all_wns_remapped_dict'], state_dict['remap_nr'], state_dict['max_np'], state_dict['wn_list'], names_flag, offline_down_nodes, state_dict['node_subclusters']

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

    for (job_id, user_name) in zip(job_ids, usernames):
        variables.UserOfJobId[job_id]  = user_name
    # variables.UserOfJobId[Jobid] = usernames
    return job_ids, usernames, statuses, queue_names


def read_qstatq_yaml(QSTATQ_YAML_FILE):
    """
    added for consistency. Originally instead of this function, 
    the following existed in qstatq2yaml:
    variables.qstatq_list.append((QueueName, Run, Queued, Lm, State))
    """
    templst = []
    tempdict = {}
    qstatq_list = []
    finr = open(QSTATQ_YAML_FILE, 'r')
    for line in finr:
        line = line.strip()
        if ' QueueName:' in line:
            tempdict.setdefault('QueueName',line.split(': ')[1])
        elif line.startswith('Running:'):
            tempdict.setdefault('Running',line.split(': ')[1])
        elif line.startswith('Queued:'):
            tempdict.setdefault('Queued',line.split(': ')[1])
        elif line.startswith('Lm:'):
            tempdict.setdefault('Lm',line.split(': ')[1])
        elif line.startswith('State:'):
            tempdict.setdefault('State',line.split(': ')[1])
        elif not line:
            qstatq_list.append(tempdict)
            tempdict = {}
        elif '---' in line:
            break
    finr.close()
    # import pdb;pdb.set_trace()
    return qstatq_list


def print_job_accounting_summary(state_dict, total_runs, total_queues, qstatq_list):
    if len(state_dict['node_subclusters']) > 1 or options.BLINDREMAP:
        print '=== WARNING: --- Remapping WN names and retrying heuristics... good luck with this... ---'
    print '\nPBS report tool. Please try: watch -d ' + QTOPPATH + '. All bugs added by sfranky@gmail.com. Cross fingers now...\n'
    print colorize('===> ', '#') + colorize('Job accounting summary', 'Nothing') + colorize(' <=== ', '#') + colorize('(Rev: 3000 $) %s WORKDIR = to be added', 'NoColourAccount') % (datetime.datetime.today()) #was: added\n
    print 'Usage Totals:\t%s/%s\t Nodes | %s/%s  Cores |   %s+%s jobs (R + Q) reported by qstat -q' % (state_dict['existing_nodes'] - state_dict['offline_down_nodes'], state_dict['existing_nodes'], state_dict['working_cores'], state_dict['total_cores'], int(total_runs), int(total_queues))
    print 'Queues: | ',
    if options.COLOR == 'ON':
        for queue in qstatq_list:
            if queue['QueueName'] in color_of_account:
                print colorize(queue['QueueName'], queue['QueueName']) + ': ' + colorize(queue['Running'], queue['QueueName']) + '+' + colorize(queue['Queued'], queue['QueueName']) + ' |',
            else:
                print colorize(queue['QueueName'], 'Nothing') + ': ' + colorize(queue['Running'], 'Nothing') + '+' + colorize(queue['Queued'], 'Nothing') + ' |',
    else:    
        for queue in qstatq_list:
            print queue['QueueName'] + ': ' + queue['Running'] + '+' + queue['Queued'] + ' |',
    print '* implies blocked\n'


def fill_cpucore_columns(state_np_corejob, cpu_core_dict, id_of_username, max_np_range):
    """
    Calculates the actual contents of the map by filling in a status string for each CPU line
    """
    if state_np_corejob[0] == '?':
        for cpu_line in cpu_core_dict:
            cpu_core_dict[cpu_line] += '_'
    else:
        _own_np = int(state_np_corejob[1])
        own_np_range = [str(x) for x in range(_own_np)]
        own_np_empty_range = own_np_range[:]

        for element in state_np_corejob[2:]:
            if type(element) == tuple:  # everytime there is a job:
                core, job = element[0], element[1]
                try: 
                    variables.UserOfJobId[job]
                except KeyError, KeyErrorValue:
                    print 'There seems to be a problem with the qstat output. A JobID has gone rogue (namely, ' + str(KeyErrorValue) +'). Please check with the System Administrator.'
                cpu_core_dict['Cpu' + str(core) + 'line'] += str(id_of_username[variables.UserOfJobId[job]])
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


def calculate_Total_WNIDLine_Width(WNnumber): # (remap_nr) in case of multiple state_dict['node_subclusters']
    '''
    calculates the worker node ID number line widths (expressed by hxxxx's)
    h1000 is the thousands' line
    h0100 is the hundreds' line
    and so on
    '''
    # global h1000, h0100, h0010, h0001
    h1000, h0100, h0010, h0001 = '','','',''

    if WNnumber < 10:
        u_ = '123456789'
        h0001 = u_[:WNnumber]

    elif WNnumber < 100:
        d_ = '0' * 9 + '1' * 10 + '2' * 10 + '3' * 10 + '4' * 10 + '5' * 10 + '6' * 10 + '7' * 10 + '8' * 10 + '9' * 10
        u_ = '1234567890' * 10
        h0010 = d_[:WNnumber]
        h0001 = u_[:WNnumber]

    elif WNnumber < 1000:
        cent = int(str(WNnumber)[0])
        dec = int(str(WNnumber)[1])
        unit = int(str(WNnumber)[2])

        c_ = str(0) * 99
        for i in range(1, cent):
            c_ += str(i) * 100
        c_ += str(cent) * (int(dec)) * 10 + str(cent) * (int(unit) + 1)
        h0100 = c_[:WNnumber]

        d_ = '0' * 9 + '1' * 10 + '2' * 10 + '3' * 10 + '4' * 10 + '5' * 10 + '6' * 10 + '7' * 10 + '8' * 10 + '9' * 10
        d__ = d_ + (cent - 1) * (str(0) + d_) + str(0)
        d__ += d_[:int(str(dec) + str(unit))]
        h0010 = d__[:WNnumber]

        uc = '1234567890' * 100
        h0001 = uc[:WNnumber]

    elif WNnumber > 1000:
        thou = int(str(WNnumber)[0])
        cent = int(str(WNnumber)[1])
        dec = int(str(WNnumber)[2])
        unit = int(str(WNnumber)[3])

        h1000 += str(0) * 999
        for i in range(1, thou):
            h1000 += str(i) * 1000
        h1000 += str(thou) * ((int(cent)) * 100 + (int(dec)) * 10 + (int(unit) + 1))

        c_ = '0' * 99 + '1' * 100 + '2' * 100 + '3' * 100 + '4' * 100 + '5' * 100 + '6' * 100 + '7' * 100 + '8' * 100 + '9' * 100
        c__ = '0' * 100 + '1' * 100 + '2' * 100 + '3' * 100 + '4' * 100 + '5' * 100 + '6' * 100 + '7' * 100 + '8' * 100 + '9' * 100
        h0100 = c_

        for i in range(1, thou):
            h0100 += c__
        else:
            h0100 += c__[:int(str(cent) + str(dec) +str(unit))+1]

        d_ = '0' * 10 + '1' * 10 + '2' * 10 + '3' * 10 + '4' * 10 + '5' * 10 + '6' * 10 + '7' * 10 + '8' * 10 + '9' * 10
        d__ = d_ * thou * 10  # cent * 10
        d___ = d_ * (cent - 1)
        h0010 = '0' * 9 + '1' * 10 + '2' * 10 + '3' * 10 + '4' * 10 + '5' * 10 + '6' * 10 + '7' * 10 + '8' * 10 + '9' * 10
        h0010 += d__
        h0010 += d___
        h0010 += d_[:int(str(dec) + str(unit)) + 1]

        uc = '1234567890' * 1000
        h0001 = uc[:WNnumber]
    return h1000, h0100, h0010, h0001


def find_matrices_width(wn_number, wn_list, state_dict, DEADWEIGHT=15):
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


def print_WN_ID_lines(start, stop, WNnumber): # WNnumber determines the number of WN ID lines needed  (1/2/3/4?)
    SEPARATOR = config['separator']
    # global h1000, h0100, h0010, h0001
    '''
    h1000 is a header for the 'thousands',
    h0100 is a header for the 'hundreds',
    h0010 is a header for the 'tens',
    h0001 is a header for the 'units' in the WN_ID lines
    '''
    # global JUST_NAMES_FLAG
    JustNameDict = {}
    if JUST_NAMES_FLAG <= 1:  # normal case, numbered WNs
        if WNnumber < 10:
            print insert_sep(h0001[start:stop], SEPARATOR, options.WN_COLON) + '={__WNID__}'

        elif WNnumber < 100:
            print insert_sep(h0010[start:stop], SEPARATOR, options.WN_COLON) + '={_Worker_}'
            print insert_sep(h0001[start:stop], SEPARATOR, options.WN_COLON) + '={__Node__}'

        elif WNnumber < 1000:
            print insert_sep(h0100[start:stop], SEPARATOR, options.WN_COLON) + '={_Worker_}'
            print insert_sep(h0010[start:stop], SEPARATOR, options.WN_COLON) + '={__Node__}'
            print insert_sep(h0001[start:stop], SEPARATOR, options.WN_COLON) + '={___ID___}'

        elif WNnumber > 1000:
            print insert_sep(h1000[start:stop], SEPARATOR, options.WN_COLON) + '={________}'
            print insert_sep(h0100[start:stop], SEPARATOR, options.WN_COLON) + '={_Worker_}'
            print insert_sep(h0010[start:stop], SEPARATOR, options.WN_COLON) + '={__Node__}'
            print insert_sep(h0001[start:stop], SEPARATOR, options.WN_COLON) + '={___ID___}'
    elif JUST_NAMES_FLAG > 1 or options.FORCE_NAMES == True: # names (e.g. fruits) instead of numbered WNs
        colour = 0
        Highlight = {0: 'cmsplt', 1: 'Red'}
        for line in range(len(max(state_dict['wn_list']))):
            JustNameDict[line] = ''
        for column in range(len(state_dict['wn_list'])): #was -1
            for line in range(len(max(state_dict['wn_list']))):
                JustNameDict[line] += colorize(state_dict['wn_list'][column][line], Highlight[colour])
            if colour == 1:
                colour = 0
            else:
                colour = 1
        for line in range(len(max(state_dict['wn_list']))):
            print JustNameDict[line] + '={__WNID__}'


def calculate_remaining_matrices(extra_matrices_nr, state_dict, cpu_core_dict, print_end, DEADWEIGHT=15):
    """
    Calculate remaining matrices
    """
    for i in range(extra_matrices_nr):
        print_start = print_end
        if config['user_cut_matrix_width']:
            print_end += config['user_cut_matrix_width']
        else:
            print_end += term_columns - DEADWEIGHT

        if options.BLINDREMAP or len(state_dict['node_subclusters']) > 1:
            if print_end >= state_dict['remap_nr']:
                print_end = state_dict['remap_nr']
        else:
            if print_end >= state_dict['biggest_written_node']:
                print_end = state_dict['biggest_written_node']
        print '\n'
        if len(state_dict['node_subclusters']) == 1:
            print_WN_ID_lines(print_start, print_end, state_dict['biggest_written_node'])
        if len(state_dict['node_subclusters']) > 1:
            print_WN_ID_lines(print_start, print_end, state_dict['remap_nr'])
        print insert_sep(node_state[print_start:print_end], SEPARATOR, options.WN_COLON) + '=Node state'
        for ind, k in enumerate(cpu_core_dict):
            colour_cpu_core_list = list(insert_sep(cpu_core_dict['Cpu' + str(ind) + 'line'][print_start:print_end], SEPARATOR, options.WN_COLON))
            nocolor_linelength = len(''.join(colour_cpu_core_list))
            colour_cpu_core_list = [colorize(elem, account_nrless_of_id[elem]) for elem in colour_cpu_core_list if elem in account_nrless_of_id]
            line = ''.join(colour_cpu_core_list)
            '''
            if the first matrix has 10 machines with 64 cores, and the rest 190 machines have 8 cores, don't print the non-existent
            56 cores from the next matrix on.
            IMPORTANT: not working if vertical separators are present!
            '''
            if '\x1b[1;30m#\x1b[1;m' * nocolor_linelength not in line:
                print line + colorize('=core' + str(ind), 'NoColourAccount')


def user_accounts_pool_mappings(colorize, accounts_mappings, color_of_account):
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


def print_core_line(cpu_core_dict):
    for ind, k in enumerate(cpu_core_dict):
        colour_cpu_core_list = list(insert_sep(cpu_core_dict['Cpu' + str(ind) + 'line'][print_start:print_end], SEPARATOR, options.WN_COLON))
        colour_cpu_core_list = [colorize(elem, account_nrless_of_id[elem]) for elem in colour_cpu_core_list if elem in account_nrless_of_id]
        line = ''.join(colour_cpu_core_list)
        print line + colorize('=core' + str(ind), 'NoColourAccount')


def calc_cpu_lines(state_dict):
    _cpu_core_dict = {}
    max_np_range = []

    for core_nr in range(state_dict['max_np']):
        _cpu_core_dict['Cpu' + str(core_nr) + 'line'] = ''  # Cpu0line, Cpu1line, Cpu2line, .. = '','','', ..
        max_np_range.append(str(core_nr))

    if len(state_dict['node_subclusters']) > 1 or options.BLINDREMAP:
        state_np_corejobs = state_dict['all_wns_remapped_dict']
    elif len(state_dict['node_subclusters']) == 1:
        state_np_corejobs = state_dict['all_wns_dict']
    for _node in state_np_corejobs:
        _cpu_core_dict = fill_cpucore_columns(state_np_corejobs[_node], _cpu_core_dict, id_of_username, max_np_range)

    return _cpu_core_dict


def reset_yaml_files():
    """
    empties the files with every run of the python script
    """
    for FILE in [PBSNODES_YAML_FILE, QSTATQ_YAML_FILE, QSTAT_YAML_FILE]:
        fin = open(FILE, 'w')
        fin.close()


def load_yaml_config(path):
    try:
        config = yaml.safe_load(open(path + "/qtopconf.yaml"))
    except yaml.YAMLError, exc:
        if hasattr(exc, 'problem_mark'):
            mark = exc.problem_mark
            print "Error position: (%s:%s)" % (mark.line+1, mark.column+1)

    symbol_map = dict([(chr(x), x) for x in range(33, 48) + range(58, 64) + range (91, 96) + range(123, 126)])
    for symbol in symbol_map:
        config['possible_ids'].append(symbol)
    return config


print_start, print_end = 0, None
JUST_NAMES_FLAG = 0 if not options.FORCE_NAMES else 1
node_state = ''

accounts_mappings = []
DIFFERENT_QSTAT_FORMAT_FLAG = 0

#  MAIN ###################################

HOMEPATH = os.path.expanduser('~/PycharmProjects')
QTOPPATH = os.path.expanduser('~/PycharmProjects/qtop')  # qtoppath: ~/qtop/qtop
# PROGDIR = os.path.expanduser('~/off/qtop')
SOURCEDIR = options.SOURCEDIR  # as set by the '-s' switch

config = load_yaml_config(QTOPPATH)

# Name files according to unique pid
PBSNODES_YAML_FILE = 'pbsnodes_%s.yaml' % os.getpid()
QSTATQ_YAML_FILE = 'qstat-q_%s.yaml' % os.getpid()
QSTAT_YAML_FILE = 'qstat_%s.yaml' % os.getpid()

os.chdir(SOURCEDIR)

# Location of read and created files
PBSNODES_ORIG_FILE = [f for f in os.listdir(os.getcwd()) if f.startswith('pbsnodes') and not f.endswith('.yaml')][0]
QSTATQ_ORIG_FILE = [f for f in os.listdir(os.getcwd()) if (f.startswith('qstat_q') or f.startswith('qstatq') or f.startswith('qstat-q') and not f.endswith('.yaml'))][0]
QSTAT_ORIG_FILE = [f for f in os.listdir(os.getcwd()) if f.startswith('qstat.') and not f.endswith('.yaml')][0]

reset_yaml_files()
make_pbsnodes_yaml(PBSNODES_ORIG_FILE, PBSNODES_YAML_FILE)

state_dict, JUST_NAMES_FLAG = read_pbsnodes_yaml(PBSNODES_YAML_FILE, JUST_NAMES_FLAG)


total_runs, total_queues = make_qstatq_yaml(QSTATQ_ORIG_FILE, QSTATQ_YAML_FILE)
variables.qstatq_list = read_qstatq_yaml(QSTATQ_YAML_FILE)
make_qstat_yaml(QSTAT_ORIG_FILE, QSTAT_YAML_FILE)
job_ids, usernames, statuses, queue_names = read_qstat_yaml(QSTAT_YAML_FILE)  # populates 4 lists

for username, jobid in zip(usernames, job_ids):
    variables.UserOfJobId[jobid] = username

os.chdir(SOURCEDIR)

#Calculation of split screen size
try:
    TermRows, term_columns = os.popen('stty size', 'r').read().split()  # does not work in pycharm
except ValueError:  # probably Pycharm's fault
    # TermRows, term_columns = [52, 211]
    TermRows, term_columns = [53, 176]
term_columns = int(term_columns)

# DEADWEIGHT = 15  # standard columns' width on the right of the CoreX map

print_job_accounting_summary(state_dict, total_runs, total_queues, variables.qstatq_list)

# counting of R, Q, C, W, E attached to each user
running_of_user, queued_of_user, cancelled_of_user, waiting_of_user, exiting_of_user = {}, {}, {}, {}, {}

for UserName, status in zip(usernames, statuses):
    if status == 'R':
        running_of_user[UserName] = running_of_user.get(UserName, 0) + 1
    elif status == 'Q':
        queued_of_user[UserName] = queued_of_user.get(UserName, 0) + 1
    elif status == 'C':
        cancelled_of_user[UserName] = cancelled_of_user.get(UserName, 0) + 1
    elif status == 'W':
        waiting_of_user[UserName] = waiting_of_user.get(UserName, 0) + 1
    elif status == 'E':
        waiting_of_user[UserName] = exiting_of_user.get(UserName, 0) + 1

for UserName in running_of_user:
    queued_of_user.setdefault(UserName, 0)
    cancelled_of_user.setdefault(UserName, 0)
    waiting_of_user.setdefault(UserName, 0)
    exiting_of_user.setdefault(UserName, 0)


# produces the decrementing list of users in the user accounts and poolmappings table
occurence_dict = {}
for UserName in usernames:
    occurence_dict[UserName] = usernames.count(UserName)
user_sorted_list = sorted(occurence_dict.items(), key=itemgetter(1), reverse=True)


'''
In case there are more users than the sum number of all numbers and 
small/capital letters of the alphabet 
'''
if len(user_sorted_list) > 87:  # was: > 62:
    for i in xrange(87, len(user_sorted_list) + 87):  # was: # for i in xrange(62, len(user_sorted_list) + 62):
        config['possible_ids'].append(str(i)[0])


id_of_username = {}
for j, username in enumerate(user_sorted_list):
    id_of_username[username[0]] = config['possible_ids'][j]

# this calculates and prints what is actually below the 
# id|  R + Q /all | unix account etc line
for uid in id_of_username:
    if uid not in running_of_user:
        running_of_user[uid] = 0
    if uid not in queued_of_user:
        queued_of_user[uid] = 0
    if uid not in cancelled_of_user:
        cancelled_of_user[uid] = 0
    if uid not in waiting_of_user:
        waiting_of_user[uid] = 0
    if uid not in exiting_of_user:
        exiting_of_user[uid] = 0


for uid in user_sorted_list:
    AllOfUser = cancelled_of_user[uid[0]] + running_of_user[uid[0]] + queued_of_user[uid[0]] + waiting_of_user[uid[0]] + exiting_of_user[uid[0]]
    accounts_mappings.append([id_of_username[uid[0]], running_of_user[uid[0]], queued_of_user[uid[0]], AllOfUser, uid])
accounts_mappings.sort(key=itemgetter(3), reverse=True)  # sort by All jobs
####################################################




cpu_core_dict = calc_cpu_lines(state_dict)

################ Node State ######################
print colorize('===> ', '#') + colorize('Worker Nodes occupancy', 'Nothing') + colorize(' <=== ', '#') + colorize('(you can read vertically the node IDs; nodes in free state are noted with - )', 'NoColourAccount')

'''
if there are non-uniform WNs in pbsnodes.yaml, e.g. wn01, wn02, gn01, gn02, ...,  remapping is performed
Otherwise, for uniform WNs, i.e. all using the same numbering scheme, wn01, wn02, ... proceed as normal
Number of Extra tables needed is calculated inside the calculate_Total_WNIDLine_Width function below
'''
if options.BLINDREMAP or len(state_dict['node_subclusters']) > 1:
    h1000, h0100, h0010, h0001 = calculate_Total_WNIDLine_Width(state_dict['remap_nr'])
    for node in state_dict['all_wns_remapped_dict']:
        node_state += state_dict['all_wns_remapped_dict'][node][0]
    (print_start, print_end, extra_matrices_nr) = find_matrices_width(state_dict['remap_nr'], state_dict['wn_list_remapped'], state_dict)
    print_WN_ID_lines(print_start, print_end, state_dict['remap_nr'])

else:  # len(state_dict['node_subclusters']) == 1 AND options.BLINDREMAP false
    h1000, h0100, h0010, h0001 = calculate_Total_WNIDLine_Width(state_dict['biggest_written_node'])
    for node in state_dict['all_wns_dict']:
        node_state += state_dict['all_wns_dict'][node][0]
    (print_start, print_end, extra_matrices_nr) = find_matrices_width(state_dict['biggest_written_node'], state_dict['wn_list'], state_dict)
    print_WN_ID_lines(print_start, print_end, state_dict['biggest_written_node'])

SEPARATOR = config['separator']  # alias
print insert_sep(node_state[print_start:print_end], SEPARATOR, options.WN_COLON) + '=Node state'

################ Node State ######################
account_nrless_of_id = {}
for line in accounts_mappings:
    just_name = re.split('[0-9]+', line[4][0])[0]
    account_nrless_of_id[line[0]] = just_name if just_name in color_of_account else 'NoColourAccount'

account_nrless_of_id['#'] = '#'
account_nrless_of_id['_'] = '_'
SEPARATOR = config['separator']
account_nrless_of_id[SEPARATOR] = 'NoColourAccount'

print_core_line(cpu_core_dict)
calculate_remaining_matrices(extra_matrices_nr, state_dict, cpu_core_dict, print_end)
user_accounts_pool_mappings(colorize, accounts_mappings, color_of_account)
print '\nThanks for watching!'

os.chdir(SOURCEDIR)
