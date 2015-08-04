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
       took out HighestCoreBusy, seemed useless (and unused)
       a separate read_qstatq_yaml function added, for consistency (removed from qstatq2yaml)
       change qstatqLst from list of tuples to list of dictionaries
       OfflineDownNodes was moved from pbs.pbsnodes2yaml to read_pbsnodes_yaml 
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
       refactored code around CPUCoreDict functionality (responsible for drawing
        the map)
0.2.3: corrected regex search pattern in make_qstat to recognize usernames like spec101u1 (number followed by number followed by letter) now handles non-uniform setups
        R + Q / all: all did not display everything (E status)
0.2.2: masking/clipping functionality (when nodes start from e.g. wn101, empty columns 1-100 are ommited)
0.2.1: Hashes displaying when the node has less cores than the max declared by a WN (its np variable)
0.2.0: unix accounts are now correctly ordered
0.1.9: All CPU lines displaying correctly
0.1.8: unix account id assignment to CPU0, 1 implemented
0.1.7: ReadQstatQ function (write in yaml format using Pyyaml)
       output up to Node state !
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
import glob
import os
import re
import sys
# modules
import pbs
import variables
from colormap import color_of_account, code_of_color

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

if not options.COLORFILE:
    options.COLORFILE = os.path.expanduser('~/qtop/qtop/qtop.colormap')
# qtopcolormap = open(options.COLORFILE, 'r')
# exec qtopcolormap


def Colorize(text, pattern):
    """print text colored according to its unix account colors"""
    if options.COLOR == 'ON':
        return "\033[" + code_of_color[color_of_account[pattern]] + "m" + text + "\033[1;m"
    else:
        return text

#for calculating the WN numbers
# h1000, h0100, h0010, h0001 = '', '', '', ''
PrintStart, PrintEnd = 0, None

if options.FORCE_NAMES == False: 
    JUST_NAMES_FLAG = 0
else:
    JUST_NAMES_FLAG = 1
# RemapNr = 0 $ ex global
NodeSubClusters = set()
OutputDirs = []

NodeState = ''

IdOfUserName = {}
AccountsMappings = []  
DIFFERENT_QSTAT_FORMAT_FLAG = 0

### CPU lines ######################################

MaxNPRange = []

AccountNrlessOfId = {}
####################################################

def read_pbsnodes_yaml(fin, namesflag):
    '''
    extracts highest node number, online nodes
    '''
    # global JUST_NAMES_FLAG   --> internal copy is now namesflag
    WNList = [] # ex global
    MaxNP = 0
    RemapNr = 0 # ex-global
    # ExistingNodes, OfflineDownNodes, WorkingCores, TotalCores, BiggestWrittenNode, NodeSubClusters, HighestCoreBusy
    NodeNr = 0 # ex-global
    AllWNsRemappedDict = {} # ex-global
    BiggestWrittenNode, TotalCores, WorkingCores = 0, 0, 0 # , HighestCoreBusy = 0 useless?
    ExistingNodes, OfflineDownNodes = 0, 0
    AllWNsDict = {}
    WNListRemapped = []
    state = ''
    for line in fin:
        line.strip()
        searchdname = 'domainname: ' + '(\w+-?\w+([.-]\w+)*)'
        searchnodenr = '([A-Za-z0-9-]+)(?=\.|$)'
        searchnodenrfind = '[A-Za-z]+|[0-9]+|[A-Za-z]+[0-9]+'
        searchjustletters = '(^[A-Za-z-]+)'
        if re.search(searchdname, line) is not None:   # line contains domain name
            m = re.search(searchdname, line)
            dname = m.group(1)
            RemapNr += 1
            '''
            extract highest node number, online nodes
            '''
            ExistingNodes += 1    # nodes as recorded on PBSNODES_ORIG_FILE
            if re.search(searchnodenr, dname) is not None:  # if a number and domain are found
                n = re.search(searchnodenr, dname)
                NodeInits = n.group(0)
                NameGroups = re.findall(searchnodenrfind, NodeInits)
                NodeInits = '-'.join(NameGroups[0:-1])
                if NameGroups[-1].isdigit():
                    NodeNr = int(NameGroups[-1])
                elif len(NameGroups) == 1: # if e.g. WN name is just 'gridmon'
                    if re.search(searchjustletters, dname) is not None:  # for non-numbered WNs (eg. fruit names)
                        namesflag += 1
                        n = re.search(searchjustletters, dname)
                        NodeInits = n.group(1)
                        NodeNr += 1
                        NodeSubClusters.add(NodeInits)    # for non-uniform setups of WNs, eg g01... and n01...
                        AllWNsDict[NodeNr] = []
                        AllWNsRemappedDict[RemapNr] = []
                        if NodeNr > BiggestWrittenNode:
                            BiggestWrittenNode = NodeNr
                        WNList.append(NodeInits)
                        # import pdb; pdb.set_trace()
                        WNList[:] = [UnNumberedWN.rjust(len(max(WNList))) for UnNumberedWN in WNList if type(UnNumberedWN) is str ]
                        WNListRemapped.append(RemapNr)                    
                elif len(NameGroups) == 2 and not NameGroups[-1].isdigit() and not NameGroups[-2].isdigit():
                    NameGroups = '-'.join(NameGroups)
                    if re.search(searchjustletters, dname) is not None:  # for non-numbered WNs (eg. fruit names)
                       namesflag += 1
                       n = re.search(searchjustletters, dname)
                       NodeInits = n.group(1)
                       NodeNr += 1
                       NodeSubClusters.add(NodeInits)    # for non-uniform setups of WNs, eg g01... and n01...
                       AllWNsDict[NodeNr] = []
                       AllWNsRemappedDict[RemapNr] = []
                       if NodeNr > BiggestWrittenNode:
                           BiggestWrittenNode = NodeNr
                       WNList.append(NodeInits)
                       WNList[:] = [UnNumberedWN.rjust(len(max(WNList))) for UnNumberedWN in WNList if type(UnNumberedWN) is str ]
                       WNListRemapped.append(RemapNr)                                  
                elif NameGroups[-2].isdigit():
                    NodeNr = int(NameGroups[-2])
                else:
                    NodeNr = int(NameGroups[-3])
                # print 'NamedGroups are: ', NameGroups #####DEBUGPRINT2  
                NodeSubClusters.add(NodeInits)    # for non-uniform setups of WNs, eg g01... and n01...
                AllWNsDict[NodeNr] = []
                AllWNsRemappedDict[RemapNr] = []
                if NodeNr > BiggestWrittenNode:
                    BiggestWrittenNode = NodeNr
                if namesflag <= 1:
                    WNList.append(NodeNr)
                WNListRemapped.append(RemapNr)
            elif re.search(searchjustletters, dname) is not None:  # for non-numbered WNs (eg. fruit names)
                namesflag += 1
                n = re.search(searchjustletters, dname)
                NodeInits = n.group(1)
                NodeNr += 1
                NodeSubClusters.add(NodeInits)    # for non-uniform setups of WNs, eg g01... and n01...
                AllWNsDict[NodeNr] = []
                AllWNsRemappedDict[RemapNr] = []
                if NodeNr > BiggestWrittenNode:
                    BiggestWrittenNode = NodeNr
                WNList.append(NodeInits)
                WNList[:] = [UnNumberedWN.rjust(len(max(WNList))) for UnNumberedWN in WNList]
                WNListRemapped.append(RemapNr)
            else:
                NodeNr = 0
                NodeInits = dname
                AllWNsDict[NodeNr] = []
                AllWNsRemappedDict[RemapNr] = []
                NodeSubClusters.add(NodeInits)    # for non-uniform setups of WNs, eg g01... and n01...
                if NodeNr > BiggestWrittenNode:
                    BiggestWrittenNode = NodeNr + 1
                WNList.append(NodeNr)
                WNListRemapped.append(RemapNr)
        elif 'state: ' in line:
            nextchar = line.split()[1].strip("'")
            if nextchar == 'f':
                state += '-'
                AllWNsDict[NodeNr].append('-')
                AllWNsRemappedDict[RemapNr].append('-')
            elif (nextchar == 'd') | (nextchar == 'o'):
                state += nextchar
                OfflineDownNodes += 1    
                AllWNsDict[NodeNr].append(nextchar)
                AllWNsRemappedDict[RemapNr].append(nextchar)
            else:
                state += nextchar
                AllWNsDict[NodeNr].append(nextchar)
                AllWNsRemappedDict[RemapNr].append(nextchar)

        elif 'np:' in line or 'pcpus:' in line:
            np = line.split(': ')[1].strip()
            AllWNsDict[NodeNr].append(np)
            AllWNsRemappedDict[RemapNr].append(np)
            if int(np) > int(MaxNP):
                MaxNP = int(np)
            TotalCores += int(np)

        elif ' core: ' in line: # this should also work for OAR's yaml file
            core = line.split(': ')[1].strip()
            WorkingCores += 1
            # if int(core) > int(HighestCoreBusy): # HighestCoreBusy doesn't look like it's doing anything!!
            #     HighestCoreBusy = int(core)
        elif 'job: ' in line:
            job = str(line.split(': ')[1]).strip()
            AllWNsDict[NodeNr].append((core, job))
            AllWNsRemappedDict[RemapNr].append((core, job))
    # HighestCoreBusy += 1

    '''
    fill in non-existent WN nodes (absent from pbsnodes file) with '?' and count them
    '''
    if len(NodeSubClusters) > 1:
        for i in range(1, RemapNr): # This RemapNr here is the LAST remapped node, it's the equivalent BiggestWrittenNode for the remapped case
            if i not in AllWNsRemappedDict:
                AllWNsRemappedDict[i] = '?'
    elif len(NodeSubClusters) == 1:
        for i in range(1, BiggestWrittenNode):
            if i not in AllWNsDict:
                AllWNsDict[i] = '?'

    if namesflag <= 1:
        WNList.sort()
        WNListRemapped.sort()

    if min(WNList) > 9000 and type(min(WNList)) == int: # handle exotic cases of WN numbering starting VERY high
        WNList = [element - min(WNList) for element in WNList]
        options.BLINDREMAP = True 
    if len(WNList) < PERCENTAGE * BiggestWrittenNode: 
        options.BLINDREMAP = True
    return ExistingNodes, WorkingCores, TotalCores, BiggestWrittenNode, AllWNsDict, WNListRemapped, AllWNsRemappedDict, RemapNr, MaxNP, WNList, namesflag, OfflineDownNodes

def read_qstat_yaml(QSTAT_YAML_FILE):
    """
    reads qstat YAML file and populates four lists. Returns the lists
    """
    JobIds, UserNames, Statuses, QueueNames = [], [], [], []
    finr = open(QSTAT_YAML_FILE, 'r')
    for line in finr:
        if line.startswith('JobId:'):
            JobIds.append(line.split()[1])
        elif line.startswith('UserName:'):
            UserNames.append(line.split()[1])
        elif line.startswith('S:'):
            Statuses.append(line.split()[1])
        elif line.startswith('Queue:'):
            QueueNames.append(line.split()[1])
    finr.close()
    # import pdb;pdb.set_trace()
    return JobIds, UserNames, Statuses, QueueNames

def read_qstatq_yaml(QSTATQ_YAML_FILE):
    """
    added for consistency. Originally instead of this function, 
    the following existed in qstatq2yaml:
    variables.qstatqLst.append((QueueName, Run, Queued, Lm, State))
    """
    templst = []
    tempdict = {}
    qstatqLst = []
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
            qstatqLst.append(tempdict)
            tempdict = {}
        elif '---' in line:
            break
    finr.close()
    # import pdb;pdb.set_trace()
    return qstatqLst

def print_job_accounting_summary(ExistingNodes, OfflineDownNodes, WorkingCores, TotalCores, TotalRuns, TotalQueues, qstatqLst):
    # import pdb;pdb.set_trace()
    if len(NodeSubClusters) > 1 or options.BLINDREMAP:
        print '=== WARNING: --- Remapping WN names and retrying heuristics... good luck with this... ---'
    print '\nPBS report tool. Please try: watch -d ' + QTOPPATH + '. All bugs added by sfranky@gmail.com. Cross fingers now...\n'
    print Colorize('===> ', '#') + Colorize('Job accounting summary', 'Nothing') + Colorize(' <=== ', '#') + Colorize('(Rev: 3000 $) %s WORKDIR = to be added', 'NoColourAccount') % (datetime.datetime.today()) #was: added\n
    print 'Usage Totals:\t%s/%s\t Nodes | %s/%s  Cores |   %s+%s jobs (R + Q) reported by qstat -q' % (ExistingNodes - OfflineDownNodes, ExistingNodes, WorkingCores, TotalCores, int(TotalRuns), int(TotalQueues))
    print 'Queues: | ',
    # import pdb;pdb.set_trace()
    if options.COLOR == 'ON':
        for queue in qstatqLst:
            if queue['QueueName'] in color_of_account:
                print Colorize(queue['QueueName'], queue['QueueName']) + ': ' + Colorize(queue['Running'], queue['QueueName']) + '+' + Colorize(queue['Queued'], queue['QueueName']) + ' |',        
            else:
                print Colorize(queue['QueueName'], 'Nothing') + ': ' + Colorize(queue['Running'], 'Nothing') + '+' + Colorize(queue['Queued'], 'Nothing') + ' |',
    else:    
        for queue in qstatqLst:
            print queue['QueueName'] + ': ' + queue['Running'] + '+' + queue['Queued'] + ' |',
    print '* implies blocked\n'

def fill_cpucore_columns(value, CPUDict):
    '''
    Calculates the actual contents of the map by filling in a status string for each CPU line
    '''
    Busy = []

    if value[0] == '?':
        for CPULine in CPUDict:
            CPUDict[CPULine] += '_'
    else:
        HAS_JOBS = 0
        OwnNP = int(value[1])
        OwnNPRange = [str(x) for x in range(OwnNP)]
        OwnNPEmptyRange = OwnNPRange[:] 

        for element in value[2:]:
            if type(element) == tuple:  # everytime there is a job:
                HAS_JOBS += 1
                Core, job = element[0], element[1]
                try: 
                    variables.UserOfJobId[job]
                except KeyError, KeyErrorValue:
                    print 'There seems to be a problem with the qstat output. A JobID has gone rogue (namely, ' + str(KeyErrorValue) +'). Please check with the System Administrator.'
                CPUDict['Cpu' + str(Core) + 'line'] += str(IdOfUserName[variables.UserOfJobId[job]])
                Busy.extend(Core)
                OwnNPEmptyRange.remove(Core)

        NonExistentCores = [item for item in MaxNPRange if item not in OwnNPRange]

        '''
        the height of the matrix is determined by the highest-core WN existing. If other WNs have less cores,
        these positions are filled with '#'s.
        '''
        for core in OwnNPEmptyRange:
            CPUDict['Cpu' + str(core) + 'line'] += '_'
        for core in NonExistentCores: 
                CPUDict['Cpu' + str(core) + 'line'] += '#'

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

def calculate_Total_WNIDLine_Width(WNnumber): # (RemapNr) in case of multiple NodeSubClusters
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

def find_Matrices_Width(WNnumber, WNList):
    '''
    masking/clipping functionality: if the earliest node number is high (e.g. 130), the first 129 WNs need not show up.
    '''
    Start = 0
    if (options.MASKING is True) and min(WNList) > MIN_MASKING_THRESHOLD and type(min(WNList)) == str: # in case of named instead of numbered WNs 
        pass            
    elif (options.MASKING is True) and min(WNList) > MIN_MASKING_THRESHOLD and type(min(WNList)) == int:
        Start = min(WNList) - 1   #exclude unneeded first empty nodes from the matrix
    '''
    Extra matrices may be needed if the WNs are more than the screen width can hold.
    '''
    if WNnumber > Start: # start will either be 1 or (masked >= MIN_MASKING_THRESHOLD + 1)
        NrOfExtraMatrices = abs(WNnumber - Start + 10) / TermColumns 
    elif WNnumber < Start and len(NodeSubClusters) > 1: # Remapping
        NrOfExtraMatrices = (WNnumber + 10) / TermColumns
    else:
        print "This is a case I didn't foresee (WNnumber vs Start vs NodeSubClusters)"

    if UserCutMatrixWidth: # if the user defines a custom cut (in the configuration file)
        Stop = Start + UserCutMatrixWidth
        return (Start, Stop, WNnumber/UserCutMatrixWidth)
    elif NrOfExtraMatrices: # if more matrices are needed due to lack of space, cut every matrix so that if fits to screen
        Stop = Start + TermColumns - DEADWEIGHT 
        return (Start, Stop, NrOfExtraMatrices)
    else: # just one matrix, small cluster!
        Stop = Start + WNnumber
        return (Start, Stop, 0)

def print_WN_ID_lines(start, stop, WNnumber): # WNnumber determines the number of WN ID lines needed  (1/2/3/4?)
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
        for line in range(len(max(WNList))):
            JustNameDict[line] = ''
        for column in range(len(WNList)): #was -1
            for line in range(len(max(WNList))):
                JustNameDict[line] += Colorize(WNList[column][line], Highlight[colour])
            if colour == 1:
                colour = 0
            else:
                colour = 1
        for line in range(len(max(WNList))):
            print JustNameDict[line] + '={__WNID__}'


def reset_yaml_files():
    """
    empties the files with every run of the python script
    """
    for FILE in [PBSNODES_YAML_FILE, QSTATQ_YAML_FILE, QSTAT_YAML_FILE]:
        fin = open(FILE, 'w')
        fin.close()    

################ MAIN ###################################

CONFIGFILE = os.path.expanduser('~/qtop/qtop/qtop.conf')
qtopconf = open(CONFIGFILE, 'r')
exec qtopconf

os.chdir(SOURCEDIR)

reset_yaml_files()
PBSNodesYamlFout = open(PBSNODES_YAML_FILE, 'a')
QSTATQYamlFout = open(QSTATQ_YAML_FILE, 'a')
QSTATYamlFout = open(QSTAT_YAML_FILE, 'a')

if not os.path.getsize(variables.PBSNODES_ORIG_FILE) > 0:  
    print 'Bailing out... Not yet ready for Sun Grid Engine clusters'
    os.chdir(HOMEPATH + 'qt')
    sys.exit(0)
    # os.chdir('..')
    # continue
else:
    pbsnodesfin = open(variables.PBSNODES_ORIG_FILE, 'r')

pbs.pbsnodes2yaml(pbsnodesfin, PBSNodesYamlFout)

PBSNodesYamlFout = open(PBSNODES_YAML_FILE, 'r')

(ExistingNodes, 
WorkingCores, 
TotalCores, 
BiggestWrittenNode, 
AllWNsDict, 
WNListRemapped, 
AllWNsRemappedDict, 
RemapNr, 
MaxNP, 
WNList, 
JUST_NAMES_FLAG,
variables.OfflineDownNodes) = read_pbsnodes_yaml(PBSNodesYamlFout, JUST_NAMES_FLAG)
PBSNodesYamlFout.close()

if not os.path.getsize(variables.QSTATQ_ORIG_FILE) > 0:  
    print 'Your ' + variables.QSTATQ_ORIG_FILE + ' file is empty! Please check your directory. Exiting ...'
    os.chdir(HOMEPATH + 'qt')
    sys.exit(0)
    # os.chdir('..')
    # continue
else:
    qstatqfin = open(variables.QSTATQ_ORIG_FILE, 'r')
TotalRuns, TotalQueues = pbs.qstatq2yaml(qstatqfin, QSTATQYamlFout)
qstatqfin.close()
QSTATQYamlFout.close()
variables.qstatqLst = read_qstatq_yaml(QSTATQ_YAML_FILE)
# import pdb;pdb.set_trace()

if not os.path.getsize(variables.QSTAT_ORIG_FILE) > 0:  
    print 'Your ' + variables.QSTAT_ORIG_FILE + ' file is empty! Please check your directory. Exiting ...'
    os.chdir(HOMEPATH + 'qt')
    sys.exit(0)
    # os.chdir('..')
    # continue
else:
    qstatfin = open(variables.QSTAT_ORIG_FILE, 'r')
pbs.qstat2yaml(qstatfin, QSTATYamlFout)
qstatfin.close()
QSTATYamlFout.close()

JobIds, UserNames, Statuses, QueueNames = read_qstat_yaml(QSTAT_YAML_FILE) # populates 4 lists
for username, jobid in zip(UserNames, JobIds):
    variables.UserOfJobId[jobid] = username

os.chdir(SOURCEDIR)
# direct = os.getcwd()


#Calculation of split screen size
TermRows, TermColumns = os.popen('stty size', 'r').read().split()
TermColumns = int(TermColumns)

DEADWEIGHT = 15  # standard columns' width on the right of the CoreX map

print_job_accounting_summary(ExistingNodes, variables.OfflineDownNodes, WorkingCores, TotalCores, TotalRuns, TotalQueues,variables.qstatqLst)

# counting of R, Q, C, W, E attached to each user
RunningOfUser, QueuedOfUser, CancelledOfUser, WaitingOfUser, ExitingOfUser = {}, {}, {}, {}, {}

for UserName, status in zip(UserNames, Statuses):
    if status == 'R':
        RunningOfUser[UserName] = RunningOfUser.get(UserName, 0) + 1
    elif status == 'Q':
        QueuedOfUser[UserName] = QueuedOfUser.get(UserName, 0) + 1
    elif status == 'C':
        CancelledOfUser[UserName] = CancelledOfUser.get(UserName, 0) + 1
    elif status == 'W':
        WaitingOfUser[UserName] = WaitingOfUser.get(UserName, 0) + 1
    elif status == 'E':
        WaitingOfUser[UserName] = ExitingOfUser.get(UserName, 0) + 1

for UserName in RunningOfUser:
    QueuedOfUser.setdefault(UserName, 0)
    CancelledOfUser.setdefault(UserName, 0)
    WaitingOfUser.setdefault(UserName, 0)
    ExitingOfUser.setdefault(UserName, 0)


#produces the decrementing list of users in the user accounts and poolmappings table
OccurenceDict = {}
for UserName in UserNames:
    OccurenceDict[UserName] = UserNames.count(UserName)
Usersortedlst = sorted(OccurenceDict.items(), key=itemgetter(1), reverse=True)


'''
In case there are more users than the sum number of all numbers and 
small/capital letters of the alphabet 
'''
j = 0
# if len(Usersortedlst) > 62: 
if len(Usersortedlst) > 87: 
    for i in xrange(87, len(Usersortedlst) + 87):
    # for i in xrange(62, len(Usersortedlst) + 62):
        POSSIBLE_IDS.append(str(i)[0])



for UserName in Usersortedlst:
    IdOfUserName[UserName[0]] = POSSIBLE_IDS[j]
    j += 1

# this calculates and prints what is actually below the 
# id|  R + Q /all | unix account etc line
for uid in IdOfUserName:
    if uid not in RunningOfUser:
        RunningOfUser[uid] = 0
    if uid not in QueuedOfUser:
        QueuedOfUser[uid] = 0
    if uid not in CancelledOfUser:
        CancelledOfUser[uid] = 0
    if uid not in WaitingOfUser:
        WaitingOfUser[uid] = 0
    if uid not in ExitingOfUser:
        ExitingOfUser[uid] = 0


for uid in Usersortedlst:  # IdOfUserName:
    AllOfUser = CancelledOfUser[uid[0]] + RunningOfUser[uid[0]] + QueuedOfUser[uid[0]] + WaitingOfUser[uid[0]] + ExitingOfUser[uid[0]]
    AccountsMappings.append([IdOfUserName[uid[0]], RunningOfUser[uid[0]], QueuedOfUser[uid[0]], AllOfUser, uid])
AccountsMappings.sort(key=itemgetter(3), reverse=True) # sort by All jobs
####################################################


### CPU lines ######################################
CPUCoreDict = {}
for i in range(MaxNP):
    CPUCoreDict['Cpu' + str(i) + 'line'] = '' # Cpu0line, Cpu1line, Cpu2line, .. = '','','', ..
    MaxNPRange.append(str(i))

if len(NodeSubClusters) > 1 or options.BLINDREMAP:
    for _, WNProperties in zip(AllWNsRemappedDict.keys(), AllWNsRemappedDict.values()):
        fill_cpucore_columns(WNProperties, CPUCoreDict)
elif len(NodeSubClusters) == 1:
    for _, WNProperties in zip(AllWNsDict.keys(), AllWNsDict.values()):
        fill_cpucore_columns(WNProperties, CPUCoreDict)

### CPU lines ######################################


################ Node State ######################
print Colorize('===> ', '#') + Colorize('Worker Nodes occupancy', 'Nothing') + Colorize(' <=== ', '#') + Colorize('(you can read vertically the node IDs; nodes in free state are noted with - )', 'NoColourAccount')

'''
if there are non-uniform WNs in pbsnodes.yaml, e.g. wn01, wn02, gn01, gn02, ...,  remapping is performed
Otherwise, for uniform WNs, i.e. all using the same numbering scheme, wn01, wn02, ... proceed as normal
Number of Extra tables needed is calculated inside the calculate_Total_WNIDLine_Width function below
'''
if options.BLINDREMAP or len(NodeSubClusters) > 1:
    h1000, h0100, h0010, h0001 = calculate_Total_WNIDLine_Width(RemapNr)
    for node in AllWNsRemappedDict:
        NodeState += AllWNsRemappedDict[node][0]
    (PrintStart, PrintEnd, NrOfExtraMatrices) = find_Matrices_Width(RemapNr, WNListRemapped)
    print_WN_ID_lines(PrintStart, PrintEnd, RemapNr)
else: # len(NodeSubClusters) == 1 AND options.BLINDREMAP false 
    h1000, h0100, h0010, h0001 = calculate_Total_WNIDLine_Width(BiggestWrittenNode)
    for node in AllWNsDict:
        NodeState += AllWNsDict[node][0]
    (PrintStart, PrintEnd, NrOfExtraMatrices) = find_Matrices_Width(BiggestWrittenNode, WNList)
    print_WN_ID_lines(PrintStart, PrintEnd, BiggestWrittenNode)


print insert_sep(NodeState[PrintStart:PrintEnd], SEPARATOR, options.WN_COLON) + '=Node state'

################ Node State ######################

for line in AccountsMappings:
    if re.split('[0-9]+', line[4][0])[0] in color_of_account:
            AccountNrlessOfId[line[0]] = re.split('[0-9]+', line[4][0])[0]
    else:
        AccountNrlessOfId[line[0]] = 'NoColourAccount'

AccountNrlessOfId['#'] = '#'
AccountNrlessOfId['_'] = '_'
AccountNrlessOfId[SEPARATOR] = 'NoColourAccount'

for ind, k in enumerate(CPUCoreDict):
    ColourCPUCoreLst = list(insert_sep(CPUCoreDict['Cpu' + str(ind) + 'line'][PrintStart:PrintEnd], SEPARATOR, options.WN_COLON))
    ColourlessLineLen = len(''.join(ColourCPUCoreLst))
    ColourCPUCoreLst = [Colorize(elem, AccountNrlessOfId[elem]) for elem in ColourCPUCoreLst if elem in AccountNrlessOfId]
    line = ''.join(ColourCPUCoreLst)
    #'''
    #don't print the non-existent core lines in the first matrix 
    #(for when the remaining tables have machines with higher cores, but not the first matrix)
    #'''    
    # if '\x1b[1;30m#\x1b[1;m' * ColourlessLineLen not in line:
    print line + Colorize('=Core' + str(ind), 'NoColourAccount')


############# Calculate remaining matrices ##################
for i in range(NrOfExtraMatrices):
    PrintStart = PrintEnd
    if UserCutMatrixWidth:
        PrintEnd += UserCutMatrixWidth
    else:
        PrintEnd += TermColumns - DEADWEIGHT # 
    
    if options.BLINDREMAP or len(NodeSubClusters) > 1:
        if PrintEnd >= RemapNr:
            PrintEnd = RemapNr
    else:
        if PrintEnd >= BiggestWrittenNode:
            PrintEnd = BiggestWrittenNode
    print '\n'
    if len(NodeSubClusters) == 1:
        print_WN_ID_lines(PrintStart, PrintEnd, BiggestWrittenNode)
    if len(NodeSubClusters) > 1:
        print_WN_ID_lines(PrintStart, PrintEnd, RemapNr)
    print insert_sep(NodeState[PrintStart:PrintEnd], SEPARATOR, options.WN_COLON) + '=Node state'
    for ind, k in enumerate(CPUCoreDict):
        ColourCPUCoreLst = list(insert_sep(CPUCoreDict['Cpu' + str(ind) + 'line'][PrintStart:PrintEnd], SEPARATOR, options.WN_COLON))
        ColourlessLineLen = len(''.join(ColourCPUCoreLst))
        ColourCPUCoreLst = [Colorize(elem, AccountNrlessOfId[elem]) for elem in ColourCPUCoreLst if elem in AccountNrlessOfId]
        line = ''.join(ColourCPUCoreLst)
        '''
        if the first matrix has 10 machines with 64 cores, and the rest 190 machines have 8 cores, don't print the non-existent
        56 cores from the next matrix on.
        IMPORTANT: not working if vertical separators are present!
        '''
        if '\x1b[1;30m#\x1b[1;m' * ColourlessLineLen not in line:
            print line + Colorize('=Core' + str(ind), 'NoColourAccount')


print Colorize('\n===> ', '#') + Colorize('User accounts and pool mappings', 'Nothing') + Colorize(' <=== ', '#') + Colorize('("all" includes those in C and W states, as reported by qstat)', 'NoColourAccount')
print ' id |  R   +   Q  /  all |    unix account | Grid certificate DN (this info is only available under elevated privileges)'
for line in AccountsMappings:
    PrintString = '%3s | %4s + %4s / %4s | %15s |' % (line[0], line[1], line[2], line[3], line[4][0])
    for account in color_of_account:
        if line[4][0].startswith(account) and options.COLOR == 'ON':
            PrintString = '%15s | %16s + %16s / %16s | %27s %4s' % (Colorize(line[0], account), Colorize(str(line[1]), account), Colorize(str(line[2]), account), Colorize(str(line[3]), account), Colorize(line[4][0], account), Colorize(SEPARATOR, 'NoColourAccount'))
        elif line[4][0].startswith(account) and options.COLOR == 'OFF':
            PrintString = '%2s | %3s + %3s / %3s | %14s |' %(Colorize(line[0], account), Colorize(str(line[1]), account), Colorize(str(line[2]), account), Colorize(str(line[3]), account), Colorize(line[4][0], account))
        else:
            pass
    print PrintString

print '\nThanks for watching!'

os.chdir(SOURCEDIR)
