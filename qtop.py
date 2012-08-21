#!/usr/bin/env python

################################################
#                                              #
#              qtop v.0.2.7                    #
#                                              #
#     Licensed under MIT-GPL licenses          #
#                                              #
#                     Fotis Georgatos          #
#                     Sotiris Fragkiskos       #
################################################

"""

changelog:
=========
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
       refactored code around CPUCoreDic functionality (responsible for drawing
        the map)
0.2.3: corrected regex search pattern in make_qstat to recognize usernames like spec101u1 (number followed by number followed by letter) now handles non-uniform setups
        R + Q / all: all did not display everything (E status)
0.2.2: clipping functionality (when nodes start from e.g. wn101, empty columns 1-100 are ommited)
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
import datetime
import glob
import itertools
import os
import re
import sys
import copy
# from qtcolormap import *
# import qtcolormap

COLORFILE = os.path.expanduser('~/qtop/qtop/qtop.colormap')
qtopcolormap = open(COLORFILE, 'r')
exec qtopcolormap



def Colorize(text, pattern):
    """print text colored according to its unix account colors"""
    return "\033[" + CodeOfColor[ColorOfAccount[pattern]] + "m" + text + \
        "\033[1;m"
     # print '\033[1;35mMagenta like Mimosa\033[1;m'

def printc(text, color):
    """Print in color."""
    print "\033[" + CodeOfColor[color] + "m" + text + "\033[0m"


def writec(text, color):
    """Write to stdout in color."""
    sys.stdout.write("\033[" + CodeOfColor[color] + "m" + text + "\033[0m")


#for calculating the WN numbers
t, c, d, u = '', '', '', ''
PrintStart, PrintEnd = 0, None

CLIPPING = True
RMWARNING = '=== WARNING: --- Remapping WN names and retrying heuristics... \
 good luck with this... ---'
RemapNr = 0
NodeSubClusters = set()
OutputDirs = []
HighestCoreBusy = 0
AllWNs, AllWNsRemapped = {}, {}
# dname = ''
BiggestWrittenNode = 0
WNList, WNListRemapped = [], []
# NodeNr = ''
NodeState = ''
LastWN = 0
ExistingNodes, OfflineDownNodes = 0, 0
MaxNP = 0 
TotalCores, WorkingCores = 0, 0
TotalRuns, TotalQueues = 0, 0  # for readQstatQ
JobIds, UnixAccounts, Statuses, Queues = [], [], [], []  # for read_qstat
qstatqLst = []
# qstatLst = []
UserOfJobId, IdOfUnixAccount = {}, {}  # keepers
AccountsMappings = []  # keeper

### CPU lines ######################################
CPUCoreDic = {}
MaxNPRange = []

AccountNrlessOfId = {}


def write_to_separate_files(filename1, filename2):
    '''
    writes the data from qstat, qstat-q, pbsnodes, which all reside in
    qtop-input.out, to a file with the corresponding name, first taking out the prefix in each line.
    '''
    fin = open(filename1, 'r')
    fout = open(filename2, 'w')
    for line in fin:
        if line.startswith(filename2.split('.')[0] + ':'):
            fout.write(line.split(':', 1)[1])
    fin.close()


def make_pbsnodes_yaml(fin, fout):
    """
    read PBSNODES_ORIG_FILE sequentially and put in respective yaml file
    """
    global OfflineDownNodes

    # NodeNr = 0
    for line in fin:
        line.strip()
        searchdname = '^\w+-?\w+(\.\w+)*'
        if re.search(searchdname, line) is not None:   # line containing domain name
            m = re.search(searchdname, line)
            dname = m.group(0)
            fout.write('domainname: ' + dname + '\n')

        elif 'state = ' in line:  # line.find('state = ')!=-1:
            nextchar = line.split()[2][0]
            if nextchar == 'f':
                state = '-'
            elif (nextchar == 'd') | (nextchar == 'o'):
                state = nextchar
                OfflineDownNodes += 1
            else:
                state = nextchar
            fout.write('state: ' + state + '\n')

        elif 'np = ' in line:   # line.find('np = ')!=-1:
            np = line.split()[2][0:]
            # TotalCores = int(np)
            fout.write('np: ' + np + '\n')

        elif 'jobs = ' in line:    # line.find('jobs = ')!=-1:
            ljobs = line.split('=')[1].split(',')
            lastcore = 150000
            for job in ljobs:
                core = job.strip().split('/')[0]
                if core == lastcore:
                    print 'There are concurrent jobs assigned to the same core!'+'\n'+'Remapping feature is not implemented yet. Exiting..'
                    sys.exit(1)
                job = job.strip().split('/')[1:][0].split('.')[0]
                fout.write('- core: ' + core + '\n')
                fout.write('  job: ' + job + '\n')
                lastcore = core

        elif 'gpus = ' in line:     # line.find('gpus = ')!=-1:
            gpus = line.split(' = ')[1]
            fout.write('gpus: ' + gpus + '\n')

        elif line.startswith('\n'):
            fout.write('\n')

    fin.close()
    fout.close()


def read_pbsnodes_yaml(fin):
    '''
    extracts highest node number, online nodes
    '''
    global ExistingNodes, OfflineDownNodes, LastWN, jobseries, BiggestWrittenNode, WNList, WNListRemapped, NodeNr, TotalCores, WorkingCores, AllWNs, AllWNsRemapped, HighestCoreBusy, MaxNP, NodeSubClusters, RemapNr

    # HighestCoreBusy = 0
    MaxNP = 0
    state = ''
    county = 0
    for line in fin:
        line.strip()
        county += 1
        searchdname = 'domainname: ' + '(\w+-?\w+(\.\w+)*)'
        searchnodenr = '([A-Za-z-]+)(\d+)'
        if re.search(searchdname, line) is not None:   # line containing domain name
            # case = 0
            m = re.search(searchdname, line)
            dname = m.group(1)
            RemapNr += 1
            '''
            extract highest node number, online nodes
            '''
            ExistingNodes += 1    # nodes as recorded on PBSNODES_ORIG_FILE
            # print 'line is ', line
            if re.search(searchnodenr, dname) is not None:
                n = re.search(searchnodenr, dname)
                NodeNr = int(n.group(2))
                nodeinits = n.group(1)
                NodeSubClusters.add(nodeinits)    # for non-uniform setups of WNs, eg g01... and n01...
                AllWNs[NodeNr] = []
                AllWNsRemapped[RemapNr] = []
                if NodeNr > BiggestWrittenNode:
                    BiggestWrittenNode = NodeNr
                WNList.append(NodeNr)
                WNListRemapped.append(RemapNr)
        elif 'state: ' in line:
            # case = 2
            nextchar = line.split()[1].strip("'")
            if nextchar == 'f':
                state += '-'
                AllWNs[NodeNr].append('-')
                AllWNsRemapped[RemapNr].append('-')
            else:
                state += nextchar
                AllWNs[NodeNr].append(nextchar)
                AllWNsRemapped[RemapNr].append(nextchar)

        elif 'np:' in line:
            # case = 3
            np = line.split(': ')[1].strip()
            AllWNs[NodeNr].append(np)
            AllWNsRemapped[RemapNr].append(np)
            if int(np) > int(MaxNP):
                MaxNP = int(np)
            TotalCores += int(np)
        elif 'core: ' in line:
            # case = 4
            core = line.split(': ')[1].strip()
            WorkingCores += 1
            if int(core) > int(HighestCoreBusy):
                HighestCoreBusy = int(core)
        elif 'job: ' in line:
            # case = 5
            job = str(line.split(': ')[1]).strip()
            AllWNs[NodeNr].append((core, job))
            AllWNsRemapped[RemapNr].append((core, job))

    LastWN = BiggestWrittenNode
    HighestCoreBusy += 1

    '''
    fill in invisible WN nodes with '?'   14/5
    and count them
    '''
    if len(NodeSubClusters) > 1:
        for i in range(1, RemapNr):
            if i not in AllWNsRemapped:
                AllWNsRemapped[i] = '?'
                # NonExistingNodes.append(i)
    elif len(NodeSubClusters) == 1:
        for i in range(1, BiggestWrittenNode):
            if i not in AllWNs:
                AllWNs[i] = '?'
                # NonExistingNodes.append(i)

    WNList.sort()
    WNListRemapped.sort()


def make_qstatq_yaml(fin, fout):
    global TotalRuns, TotalQueues  # qstatqLst
    """
    read QSTATQ_ORIG_FILE sequentially and put useful data in respective yaml file
    """
    Queuesearch = '^([a-z]+)\s+(--)\s+(--|\d+:\d+:\d+)\s+(--|\d+:\d+:\d+)\s+(--)\s+(\d+)\s+(\d+)\s+(--|\d+)\s+([DE] R)'
    RunQdSearch = '^\s*(\d+)\s+(\d+)'
    for line in fin:
        line.strip()
        # searches for something like: biomed             --      --    72:00:00   --   31   0 --   E R
        if re.search(Queuesearch, line) is not None:
            m = re.search(Queuesearch, line)
            _, QueueName, Mem, CPUtime, Walltime, Node, Run, Queued, Lm, State = m.group(0), m.group(1), m.group(2), m.group(3), m.group(4), m.group(5), m.group(6), m.group(7), m.group(8), m.group(9)
            qstatqLst.append((QueueName, Run, Queued, Lm, State))
            fout.write('- QueueName: ' + QueueName + '\n')
            fout.write('  Running: ' + Run + '\n')
            fout.write('  Queued: ' + Queued + '\n')
            fout.write('  Lm: ' + Lm + '\n')
            fout.write('  State: ' + State + '\n')
            fout.write('\n')
        elif re.search(RunQdSearch, line) is not None:
            n = re.search(RunQdSearch, line)
            TotalRuns, TotalQueues = n.group(1), n.group(2)
    fout.write('---\n')
    fout.write('Total Running: ' + str(TotalRuns) + '\n')
    fout.write('Total Queued: ' + str(TotalQueues) + '\n')


def make_qstat_yaml(fin, fout):
    """
    read QSTAT_ORIG_FILE sequentially and put useful data in respective yaml file
    """
    UserQueueSearch = '^((\d+)\.([A-Za-z-]+[0-9]*))\s+([%A-Za-z0-9_.=-]+)\s+([A-Za-z0-9]+)\s+(\d+:\d+:\d*|0)\s+([CWRQE])\s+(\w+)'
    RunQdSearch = '^\s*(\d+)\s+(\d+)'
    for line in fin:
        line.strip()
        # searches for something like: 422561.cream01             STDIN            see062          48:50:12 R see
        if re.search(UserQueueSearch, line) is not None:
            m = re.search(UserQueueSearch, line)
            Jobid, Jobnr, CEname, Name, User, TimeUse, S, Queue = m.group(1), m.group(2), m.group(3), m.group(4), m.group(5), m.group(6), m.group(7), m.group(8)
            # qstatLst.append([[Jobnr], User, S, Queue])
            Jobid = Jobid.split('.')[0]
            fout.write('---\n')
            fout.write('JobId: ' + Jobid + '\n')
            fout.write('UnixAccount: ' + User + '\n')
            fout.write('S: ' + S + '\n')
            fout.write('Queue: ' + Queue + '\n')

            # UnixOfJobId[Jobid.split('.')[0]]=User
            UserOfJobId[Jobid] = User
            fout.write('...\n')


def read_qstat():
    # global JobIds, UnixAccounts, Statuses, Queues
    finr = open(QSTAT_YAML_FILE, 'r')
    for line in finr:
        if line.startswith('JobId:'):
            JobIds.append(line.split()[1])
        elif line.startswith('UnixAccount:'):
            UnixAccounts.append(line.split()[1])
        elif line.startswith('S:'):
            Statuses.append(line.split()[1])
        elif line.startswith('Queue:'):
            Queues.append(line.split()[1])
    finr.close()


def job_accounting_summary():
    if len(NodeSubClusters) > 1:
        print RMWARNING
    print 'PBS report tool. Please try: watch -d ' + QTOPPATH + '. All bugs added by sfranky@gmail.com. Cross fingers now...\n'
    print '===> Job accounting summary <=== (Rev: 3000 $) %s WORKDIR = to be added\n' % (datetime.datetime.today())
    print 'Usage Totals:\t%s/%s\t Nodes | %s/%s\t Cores |\t %s+%s\t jobs (R + Q) reported by qstat -q' % (ExistingNodes - OfflineDownNodes, ExistingNodes, WorkingCores, TotalCores, int(TotalRuns), int(TotalQueues))
    print 'Queues: | ',
    for i in qstatqLst:
        print i[0] + ': ' + i[1] + '+' + i[2] + ' |',
    print '* implies blocked'
    print '\n'


def fill_cpucore_columns(value, CPUDic):
    '''
    Calculates the actual contents of the map by filling in a status string for each CPU line
    '''
    Busy = []

    if value[0] == '?':
        for CPULine in CPUDic:
            CPUDic[CPULine] += '_'
    else:
        HAS_JOBS = 0
        OwnNP = int(value[1])
        OwnNPRange = [str(x) for x in range(OwnNP)]
        OwnNPEmptyRange = [str(x) for x in range(OwnNP)]

        for element in value[2:]:
            if type(element) == tuple:  # everytime there is a job:
                HAS_JOBS += 1
                Core, job = element[0], element[1]
                CPUDic['Cpu' + str(Core) + 'line'] += str(IdOfUnixAccount[UserOfJobId[job]])
                Busy.extend(Core)
                OwnNPEmptyRange.remove(Core)

        NonExistentCores = [item for item in MaxNPRange if item not in OwnNPRange]

        for core in OwnNPEmptyRange:
            CPUDic['Cpu' + str(core) + 'line'] += '_'
        for core in NonExistentCores:
                CPUDic['Cpu' + str(core) + 'line'] += '#'


def number_WNs(WNnumber, WNList):
    '''
    prints the worker node ID number lines
    '''
    global t, c, d, u, PrintStart, PrintEnd, Dx, NrOfTables
    if WNnumber < 10:
        unit = str(WNnumber)[0]

        for node in range(WNnumber):
            u += str(node + 1)

    elif WNnumber < 100:
        dec = str(WNnumber)[0]
        unit = str(WNnumber)[1]

        d_ = '0' * 9 + '1' * 10 + '2' * 10 + '3' * 10 + '4' * 10 + '5' * 10 + '6' * 10 + '7' * 10 + '8' * 10 + '9' * 10
        u = '1234567890' * 10
        d = d_[:WNnumber]

    elif WNnumber < 1000:
        cent = int(str(WNnumber)[0])
        dec = int(str(WNnumber)[1])
        unit = int(str(WNnumber)[2])

        c += str(0) * 99
        for i in range(1, cent):
            c += str(i) * 100
        c += str(cent) * (int(dec)) * 10 + str(cent) * (int(unit) + 1)

        d_ = '0' * 9 + '1' * 10 + '2' * 10 + '3' * 10 + '4' * 10 + '5' * 10 + '6' * 10 + '7' * 10 + '8' * 10 + '9' * 10
        d = d_
        for i in range(1, cent):
            d += str(0) + d_
        else:
            d += str(0)
        d += d_[:int(str(dec) + str(unit))]

        uc = '1234567890' * 100
        u = uc[:WNnumber]


    elif WNnumber > 1000:
        thou = int(str(WNnumber)[0])    
        cent = int(str(WNnumber)[1])
        dec = int(str(WNnumber)[2])
        unit = int(str(WNnumber)[3])

        t += str(0) * 999
        for i in range(1, thou):
            t += str(i) * 1000
        t += str(thou) * ((int(cent)) * 100 + (int(dec)) * 10 + (int(unit) + 1))

        c_ = '0' * 99 + '1' * 100 + '2' * 100 + '3' * 100 + '4' * 100 + '5' * 100 + '6' * 100 + '7' * 100 + '8' * 100 + '9' * 100
        c__ = '0' * 100 + '1' * 100 + '2' * 100 + '3' * 100 + '4' * 100 + '5' * 100 + '6' * 100 + '7' * 100 + '8' * 100 + '9' * 100
        c = c_

        for i in range(1, thou):
            c += c__
        else:
            c += c__[:int(str(cent)+str(dec)+str(unit))+1]
            print 'the length of c is:', len(c__) # int(str(cent)+str(dec)+str(unit))

        d_ = '0' * 10 + '1' * 10 + '2' * 10 + '3' * 10 + '4' * 10 + '5' * 10 + '6' * 10 + '7' * 10 + '8' * 10 + '9' * 10
        d__ = d_ * cent * 10
        d = '0' * 9 + '1' * 10 + '2' * 10 + '3' * 10 + '4' * 10 + '5' * 10 + '6' * 10 + '7' * 10 + '8' * 10 + '9' * 10
        d += d__

        d += d_[:int(str(dec) + str(unit))]

        uc = '1234567890' * 1000
        u = uc[:WNnumber]



    '''
    masking/clipping functionality: if the earliest node number is high (e.g. 80), the first 79 WNs need not show up.
    '''
    if (CLIPPING is True) and WNList[0] > 100:
        PrintStart = WNList[0] - 1
        if PrintEnd is None:
            PrintEnd = BiggestWrittenNode
        elif PrintEnd < PrintStart:
            PrintEnd += PrintStart


    NrOfTables = (BiggestWrittenNode - PrintStart) / TermColumns + 1
    if NrOfTables > 1: 
        PrintEnd = PrintStart + TermColumns - DEADWEIGHT
    else:
        PrintEnd = None

    print_WN_ID_lines(PrintStart, PrintEnd, WNnumber)





def print_WN_ID_lines(start, stop, WNnumber):
    if WNnumber < 10:
        print u + '={__WNID__}'

    elif WNnumber < 100:
        print d            + '={_Worker_}'
        print u[:WNnumber] + '={__Node__}'

    elif WNnumber < 1000:
        print c[start:stop] + '={_Worker_}'
        print d[start:stop] + '={__Node__}'
        print u[start:stop] + '={___ID___}'

    elif WNnumber > 1000:
        print t[start:stop] + '={_This___}'
        print c[start:stop] + '={_space__}'
        print d[start:stop] + '={__for __}'
        print u[start:stop] + '={_sale___}'



def empty_yaml_files():
    """
    empties the files with every run of the python script
    """
    fin1temp = open(PBSNODES_YAML_FILE, 'w')
    fin1temp.close()

    fin2temp = open(QSTATQ_YAML_FILE, 'w')
    fin2temp.close()

    fin3temp = open(QSTAT_YAML_FILE, 'w')
    fin3temp.close()

################ MAIN ###########################

CONFIGFILE = os.path.expanduser('~/qtop/qtop/qtop.conf')
qtopconf = open(CONFIGFILE, 'r')
exec qtopconf



#Calculation of split screen size

TermRows, TermColumns = os.popen('stty size', 'r').read().split()
TermColumns = int(TermColumns)

DEADWEIGHT = 15  # standard columns on the left and right of the CPUx map

job_accounting_summary()

# solution for counting R, Q, C attached to each user
RunningOfUser, QueuedOfUser, CancelledOfUser, WaitingOfUser, ExitingOfUser = {}, {}, {}, {}, {}

for user, status in zip(UnixAccounts, Statuses):
    if status == 'R':
        RunningOfUser[user] = RunningOfUser.get(user, 0) + 1
    elif status == 'Q':
        QueuedOfUser[user] = QueuedOfUser.get(user, 0) + 1
    elif status == 'C':
        CancelledOfUser[user] = CancelledOfUser.get(user, 0) + 1
    elif status == 'W':
        WaitingOfUser[user] = WaitingOfUser.get(user, 0) + 1
    elif status == 'E':
        WaitingOfUser[user] = ExitingOfUser.get(user, 0) + 1

for account in RunningOfUser:
    QueuedOfUser.setdefault(account, 0)
    CancelledOfUser.setdefault(account, 0)
    WaitingOfUser.setdefault(account, 0)
    ExitingOfUser.setdefault(account, 0)

OccurenceDic = {}
for user in UnixAccounts:
    OccurenceDic[user] = UnixAccounts.count(user)

Usersortedlst = sorted(OccurenceDic.items(), key=itemgetter(1), reverse=True)


# IdOfUnixAccount = {}
j = 0
for unixaccount in Usersortedlst:
    IdOfUnixAccount[unixaccount[0]] = POSSIBLE_IDS[j]
    j += 1
########################## end of copied from below


### CPU lines ######################################

for i in range(MaxNP):
    CPUCoreDic['Cpu' + str(i) + 'line'] = ''  # Cpu0line, Cpu1line, Cpu2line, .. = '','','', ..
    MaxNPRange.append(str(i))

if len(NodeSubClusters) == 1:
    for _, WNProperties in zip(AllWNs.keys(), AllWNs.values()):
        fill_cpucore_columns(WNProperties, CPUCoreDic)
elif len(NodeSubClusters) > 1:
    for _, WNProperties in zip(AllWNsRemapped.keys(), AllWNsRemapped.values()):
        fill_cpucore_columns(WNProperties, CPUCoreDic)

print '===> Worker Nodes occupancy <=== (you can read vertically the node IDs; nodes in free state are noted with - )'

'''
if there are non-uniform WNs in pbsnodes.yaml, e.g. wn01, wn02, gn01, gn02, ...,  remapping is performed
Otherwise, for uniform WNs, i.e. all using the same numbering scheme, wn01, wn02, ... proceed as normal
'''




if len(NodeSubClusters) == 1:
    number_WNs(LastWN, WNList)
    for node in AllWNs:  # why are dictionaries ALWAYS ordered, when keys are '1','5','3' etc ?!!?!?
        NodeState += AllWNs[node][0]
elif len(NodeSubClusters) > 1:
    number_WNs(RemapNr, WNListRemapped)
    for node in AllWNsRemapped:
        NodeState += AllWNsRemapped[node][0]

print NodeState[PrintStart:PrintEnd] + '=Node state'

for ind, k in enumerate(CPUCoreDic):
    PrintLines = CPUCoreDic['Cpu' + str(ind) + 'line'][PrintStart:PrintEnd] + '=CPU' + str(ind)
    print PrintLines



#print remaining tables
for i in range(NrOfTables): 
    print '\n'
    PrintStart = PrintEnd
    PrintEnd += TermColumns - DEADWEIGHT
    if PrintEnd > BiggestWrittenNode:
        PrintEnd = BiggestWrittenNode
    if PrintStart == PrintEnd:
        break
    if len(NodeSubClusters) == 1:
        print_WN_ID_lines(PrintStart, PrintEnd, LastWN)
    if len(NodeSubClusters) > 1:
        print_WN_ID_lines(PrintStart, PrintEnd, RemapNr)
    print NodeState[PrintStart:PrintEnd] + '=Node state'
    for ind, k in enumerate(CPUCoreDic):
        print CPUCoreDic['Cpu' + str(ind) + 'line'][PrintStart:PrintEnd] + '=CPU' + str(ind)


################################################################################################
# this calculates and prints what is actually below the id|  R + Q /all | unix account etc line
for id in IdOfUnixAccount:
    if id not in RunningOfUser:
        RunningOfUser[id] = 0
    if id not in QueuedOfUser:
        QueuedOfUser[id] = 0
    if id not in CancelledOfUser:
        CancelledOfUser[id] = 0
    if id not in WaitingOfUser:
        WaitingOfUser[id] = 0
    if id not in ExitingOfUser:
        ExitingOfUser[id] = 0


for id in Usersortedlst:  # IdOfUnixAccount:
    AccountsMappings.append([IdOfUnixAccount[id[0]], RunningOfUser[id[0]], QueuedOfUser[id[0]], CancelledOfUser[id[0]] + RunningOfUser[id[0]] + QueuedOfUser[id[0]] + WaitingOfUser[id[0]] + ExitingOfUser[id[0]], id])
AccountsMappings.sort(key=itemgetter(3), reverse=True)

print '\n'
print '===> User accounts and pool mappings <=== ("all" includes those in C and W states, as reported by qstat)'
print 'id |   R +   Q / all |  unix account  | Grid certificate DN (this info is only available under elevated privileges)'
for line in AccountsMappings:
    PrintString = '%2s | %3s + %3s / %3s | %14s |' % (line[0], line[1], line[2], line[3], line[4][0])
    for account in ColorOfAccount:
        if line[4][0].startswith(account):
            PrintString = '%14s | %15s + %15s / %15s | %26s |' % (Colorize(line[0], account), Colorize(str(line[1]), account), Colorize(str(line[2]), account), Colorize(str(line[3]), account), Colorize(line[4][0], account))
            AccountNrlessOfId[line[0]] = account  # bgazei px 'see', oxi 'see018'
            # AccountNrlessOfId[line[0]] = line[4][0]  # bgazei px 'see042'
        else:
            pass
    print PrintString

CPUCoreDic2 = copy.deepcopy(CPUCoreDic)
PrintMap = ''
for ind in range(len(CPUCoreDic)):
    if  '1' in CPUCoreDic['Cpu' + str(ind) + 'line']:
        pass
    ## for Accountless, id in zip(AccountNrlessOfId.values(),
        # AccountNrlessOfId.keys()):
        '''
        was: range(len(CPUCoreDic)): # range(13):
        '''
        # CPUCoreDic2['Cpu' + str(ind)+'line'] = CPUCoreDic['Cpu' + str(ind)+'line'][PrintStart:PrintEnd].replace(str(id),Colorize(str(id),AccountNrlessOfId[str(id)]))
        ## CPUCoreDic['Cpu' + str(ind)+'line'] = CPUCoreDic['Cpu' + str(ind)+'line'][PrintStart:PrintEnd].replace(str(id),Colorize(str(id),AccountNrlessOfId[str(id)]))
        # Colorize(CPUCoreDic['Cpu' + str(ind)+'line'][PrintStart:PrintEnd], account)
    # PrintMap +=  CPUCoreDic['Cpu' + str(ind)+'line'][PrintStart:PrintEnd] + '=CPU' + str(ind)+'\n'
    ## PrintMap +=  CPUCoreDic['Cpu' + str(ind)+'line'][PrintStart:PrintEnd] + '=CPU' + str(ind)+'\n'

#print PrintMap
# for id in PrintMap:

print '\nThanks for watching!'

os.chdir(QTOPPATH)



#print AllWNs
print 'BiggestWrittenNode is: ', len(AllWNs), BiggestWrittenNode
print 'PrintStart, PrintEnd are: ', PrintStart, PrintEnd
# print 'Dx = TermColumns - (BiggestWrittenNode-PrintStart + DEADWEIGHT)'
# print Dx, '=', TermColumns, '-', '(', BiggestWrittenNode, '-', PrintStart, '+', DEADWEIGHT, ')'
print 'NrOfTables is:', NrOfTables
print 'TermColumns is: ', TermColumns