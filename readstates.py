#!/usr/bin/env python

################################################
#                                              #
#              qtop v.0.2.4                    #
#                                              #
#     Licensed under MIT-GPL licenses          #
#                                              #
#                     Fotis Georgatos          #
#                     Sotiris Fragkiskos       #
################################################

"""

changelog:
=========
0.2.4: implemented some stuff from PEP8
0.2.3: corrected regex search pattern in make_qstat to recognize usernames like spec101u1 (number followed by number followed by letter)
       now handles non-uniform setups
       R+Q / all: all did not display everything (E status)
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
import yaml
# from readpbsyaml import *

# savedir = '~/qtop-input/results'
# savedir = os.path.expanduser(savedir)

HOMEPATH = os.path.expanduser('~/')
OUTPUTPATH = os.path.expanduser('~/qtop-input/outputs/')
QTOPPATH = os.path.expanduser('~/qtop/qtop')
PROGDIR = os.path.expanduser('~/off/qtop')


# Files location

PBSNODES_ORIG_FILE = 'pbsnodes.out'
QSTATQ_ORIG_FILE = 'qstat-q.out'
QSTAT_ORIG_FILE = 'qstat.out'

PBSNODES_YAML_FILE = HOMEPATH + 'qt/pbsnodes.yaml'
QSTATQ_YAML_FILE = HOMEPATH + 'qt/qstat-q.yaml'
QSTAT_YAML_FILE = HOMEPATH + 'qt/qstat.yaml'




# if not os.path.exists(savedir):
#     cmd = 'mkdir '+savedir
#     fp = os.popen(cmd)   # execute cmd 'mkdir /home/sfragk/qtop-input/results'

CLIPPING = True
RMWARNING = '=== WARNING: --- Remapping WN names and retrying heuristics... good luck with this... ---'
RemapNr = 0
NodeInitials = set()
OutputDirs = []
statelst = []
maxcores = 0
# qstatqdic={} # fainetai na xrisimopoieitai mono mia fora, xrisimopoiw to qstatqLst instead
AllWNs, AllWNsRemapped={}, {}
dname = ''
BiggestWrittenNode = 0
WNList, WNListRemapped = [], []
NodeNr = ''
LastWN = 0
ExistingNodes, NonExistingNodes, OfflineDownNodes = 0, [], 0
TotalCores = 0
QueueName, Mem, CPUtime, Walltime, Node, Run, Queued, Lm, State, TotalRuns, TotalQueues = 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 # for readQstatQ
Jobid, Jobnr, CEname, Name, User, TimeUse, S, Queue = 0, 0,'','','','','','' # for readQstat
qstatqLst, qstatLst = [],[]
BigJobList = []
UserOfJobId={}
CoreOfJob={}
IdOfUnixAccount={}

AccountsMappings = []

def write_to_separate(filename1, filename2):
    '''
    writes the data from qstat, qstat-q, pbsnodes, which all reside in
    qtop-input.out, to a file with the corresponding name, first taking out the prefix in each line.
    '''
    fin = open(filename1,'r')
    fout = open(filename2,'w')
    for line in fin:
        if line.startswith(filename2.split('.')[0]+':'):
            fout.write(line.split(':', 1)[1])
    fin.close() 


'''
def get_state(fin):     # yamlstream
    """
    gets the state of each of the nodes for each given file-job (pbsnodes.yaml), appends it to variable 'status' and
    returns the status, which is of the form e.g. ----do---dddd
    """
    state = ''
    for line in fin:
        line.strip()
        # if line.find('state: ')!=-1:
        if 'state: ' in line: 
            nextchar = line.split()[1].strip("'")
            if nextchar == 'f': state += '-'
            else:
                state += nextchar
    fin.close() 
    statelst = list(state)
    return statelst
    # or
    # return state
'''
# yamlstream = open('/home/sfranky/qt/pbsnodes.yaml', 'r')
# statebeforeUnsorted = get_state(yamlstream)    

"""
def get_core_jobs(fin):   # yamlstream
    core0state, core1state = '',''
    for line in fin:
        line.strip()
        if line.find("core: '0'")!=-1:
            jobcpu1 = fin.readline().split()[1]
        elif line.find("core: '1'")!=-1:
            jobcpu2 = fin.readline().split()[1]
        elif line.find("core: '2'")!=-1:
            jobcpu3 = fin.readline().split()[1]
        elif line.find("core: '3'")!=-1:
            jobcpu4 = fin.readline().split()[1]
"""

def make_pbsnodes_yaml(fin, fout):
    """
    read PBSNODES_ORIG_FILE sequentially and put in respective yaml file
    """
    global OfflineDownNodes # NonExistingNodes, BigJobList
    
    # NodeNr = 0
    for line in fin:
        line.strip()
        searchdname = '^\w+(\.\w+)*'
        if re.search(searchdname, line) is not None:   # line containing domain name
            m = re.search(searchdname, line) 
            dname = m.group(0)
            fout.write('domainname: ' + dname + '\n')

        elif 'state = ' in line:  # line.find('state = ')!=-1:
            nextchar = line.split()[2][0]
            if nextchar == 'f': 
                state = '-'
            elif (nextchar == 'd')|(nextchar == 'o'):
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
            for job in ljobs:
                core = job.strip().split('/')[0]
                job = job.strip().split('/')[1:][0].split('.')[0]
                fout.write('- core: ' + core +'\n')
                fout.write('  job: ' + job +'\n')

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
    global ExistingNodes, NonExistingNodes, OfflineDownNodes, LastWN, BigJobList, jobseries, BiggestWrittenNode, WNList, WNListRemapped, NodeNr, TotalCores, CoreOfJob, AllWNs, AllWNsRemapped, maxcores, MaxNP, statelst, NodeInitials, RemapNr

    maxcores = 0
    MaxNP = 0
    state = ''
    county = 0
    for line in fin:
        line.strip()
        county += 1
        searchdname = 'domainname: '+'(\w+(\.\w+)*)'
        searchnodenr = '([A-Za-z]+)(\d+)'
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
                NodeInitials.add(nodeinits)    # for non-uniform setups of WNs, eg g01... and n01...
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
            if int(np)>int(MaxNP):
                MaxNP = int(np)
            TotalCores += int(np)
        elif 'core: ' in line:
            # case = 4            
            core = line.split(': ')[1].strip()
            if int(core)>int(maxcores):
                maxcores = int(core)
        elif 'job: ' in line:
            # case = 5            
            job = str(line.split(': ')[1]).strip()
            AllWNs[NodeNr].append((core, job))
            AllWNsRemapped[RemapNr].append((core, job))
        # print 'successful case was ', case

    statelst = list(state)
    LastWN = BiggestWrittenNode
    maxcores += 1

    '''
    fill in invisible WN nodes with '?'   14/5
    and count them
    '''
    if len(NodeInitials) > 1:
        for i in range(1, RemapNr):
            if i not in AllWNsRemapped:
                AllWNsRemapped[i]='?'
                NonExistingNodes.append(i)
    else:
        for i in range(1, BiggestWrittenNode):
            if i not in AllWNs:
                AllWNs[i]='?'
                NonExistingNodes.append(i)

    WNList.sort()
    WNListRemapped.sort()
    diff = 0

    
def make_qstatq(fin, fout):
    global QueueName, Mem, CPUtime, Walltime, Node, Run, Queued, Lm, State, TotalRuns, TotalQueues, qstatqLst
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
            qstatqLst.append((QueueName, Run, Queued, Lm, State))   # which one to keep?  # remember to move to ReadQstatQ
            # qstatqdic[QueueName] = [(Run, Queued, Lm, State)]    # which one to keep?  # remember to move to ReadQstatQ
            fout.write('- QueueName: ' + QueueName +'\n')
            fout.write('  Running: ' + Run +'\n')
            fout.write('  Queued: ' + Queued +'\n')
            fout.write('  Lm: ' + Lm +'\n')
            fout.write('  State: ' + State +'\n')
            fout.write('\n')
        elif re.search(RunQdSearch, line) is not None:
            n = re.search(RunQdSearch, line)
            TotalRuns, TotalQueues = n.group(1), n.group(2)
    fout.write('---\n')
    fout.write('Total Running: ' + TotalRuns + '\n')
    fout.write('Total Queued: ' + TotalQueues + '\n')

def make_qstat(fin, fout):
    global Jobid, Jobnr, CEname, Name, User, TimeUse, S, Queue, Id2Unix
    """
    read QSTAT_ORIG_FILE sequentially and put useful data in respective yaml file
    """
    UserQueueSearch = '^((\d+)\.([A-Za-z-]+[0-9]*))\s+([A-Za-z0-9_.-]+)\s+([A-Za-z0-9]+)\s+(\d+:\d+:\d*|0)\s+([CWRQE])\s+(\w+)'
    RunQdSearch = '^\s*(\d+)\s+(\d+)'
    for line in fin:
        line.strip()
        # searches for something like: 422561.cream01             STDIN            see062          48:50:12 R see       
        if re.search(UserQueueSearch, line) is not None:
            m = re.search(UserQueueSearch, line)
            Jobid, Jobnr, CEname, Name, User, TimeUse, S, Queue = m.group(1), m.group(2), m.group(3), m.group(4), m.group(5), m.group(6), m.group(7), m.group(8)
            qstatLst.append([[Jobnr], User, S, Queue])
            Jobid = Jobid.split('.')[0]
            fout.write('---\n')
            fout.write('JobId: ' + Jobid +'\n')    
            fout.write('UnixAccount: ' + User +'\n')
            fout.write('S: ' + S +'\n')
            fout.write('Queue: ' + Queue +'\n')

            # UnixOfJobId[Jobid.split('.')[0]]=User
            UserOfJobId[Jobid]=User
            fout.write('...\n')

############################################################################################

"""
empties the files with every run of the python script
"""
fin1temp = open(PBSNODES_YAML_FILE, 'w')
fin1temp.close()

fin2temp = open(QSTATQ_YAML_FILE, 'w')
fin2temp.close()

fin3temp = open(QSTAT_YAML_FILE, 'w')
fin3temp.close()



os.chdir(OUTPUTPATH)
OutputDirs += glob.glob('sfragk*') 
OutputDirs += glob.glob('fotis*') 


for dir in OutputDirs:
    # if dir == 'fotistestfiles': # OK
    # if dir == 'sfragk_tEbjFj59gTww0f46jTzyQA':  # implement clip/masking functionality !! OK
    # if dir == 'sfragk_sDNCrWLMn22KMDBH_jboLQ':  # OK
    # if dir == 'sfragk_aRk11NE12OEDGvDiX9ExUg':   # OK
    # if dir == 'sfragk_gHYT96ReT3-QxTcvjcKzrQ':  # OK
    # if dir == 'sfragk_zBwyi8fu8In5rLu7RBtLJw':  # OK
    # if dir == 'sfragk_xq9Z9Dw1YU8KQiBu-A5sQg':  # OK
    # if dir == 'sfragk_sE5OozGPbCemJxLJyoS89w':  # seems ok !
    # if dir == 'sfragk_vshrdVf9pfFBvWQ5YfrnYg':  # ##s ?
    if dir == 'sfragk_R__ngzvVl5L22epgFVZOkA':  # ##s instead of __s, wrong node state (no ??)
    # if dir == 'sfragk_iLu0q1CbVgoDFLVhh5NGNw': # diaforetiko me tou foti

        os.chdir(dir)
        yamlstream1 = open(PBSNODES_YAML_FILE, 'a')
        yamlstream2 = open(QSTATQ_YAML_FILE, 'a')
        yamlstream3 = open(QSTAT_YAML_FILE, 'a')

        fin1 = open(PBSNODES_ORIG_FILE, 'r')
        make_pbsnodes_yaml(fin1, yamlstream1)
        yamlstream1 = open(PBSNODES_YAML_FILE, 'r')
        read_pbsnodes_yaml(yamlstream1)
        yamlstream1.close()

        fin2 = open(QSTATQ_ORIG_FILE, 'r')
        make_qstatq(fin2, yamlstream2)
        fin2.close()
        yamlstream2.close()

        fin3 = open(QSTAT_ORIG_FILE, 'r')
        make_qstat(fin3, yamlstream3)
        fin3.close()
        yamlstream3.close()

        os.chdir('..')

# os.chdir(HOMEPATH+'inp/outputs/sfragk_aRk11NE12OEDGvDiX9ExUg/')            
# fin = open('PBSNODES_ORIG_FILE','r')            
# ReadPbsNodes(fin, yamlstream)


'''
if __name__ == "__main__":
    
    OutputDirs, outputFiles = [],[]

    os.chdir(OUTPUTPATH)
    OutputDirs += glob.glob('sfragk*') 

    for dir in OutputDirs:
        # create full path to each sfragk_31sdf.../qtop-input.out file and put it in list outputFiles 
        os.chdir(dir)
        if glob.glob('*.out'): # is there an actual output from the job?
            outputFile = glob.glob('*.out')[0]
            outputFiles.append(os.path.join(OUTPUTPATH, dir, outputFile))
            # here is where each .out file is broken into 3 files
            sepFiles = ['PBSNODES_ORIG_FILE','QSTAT_ORIG_FILE','QSTATQ_ORIG_FILE']
            for sepFile in sepFiles:
                write_to_separate(outputFile, sepFile)
        os.chdir('..')

    yfile=('PBSNODES_ORIG_FILE', 'r')
    ReadPbsNodes(yfile)
'''

'''
    for fullname in outputFiles:
        # get state for each job and write it to a separate file in results directory
        fullname = os.path.expanduser(fullname)
        (dirname, filename) = os.path.split(fullname)
        fin = open(fullname,"r")  
        getst = get_state(fin)
        # print getst  #--> jjjjj-----d-d----- etc
        save = dirname
        (outdir, statefile)=os.path.split(save)
        os.chdir(savedir)
        #print os.getcwd() # --> results dir
        #writeString(statefile, getst)  # need to change this as I deleted writeString. make a fin = open(statefile) etc
'''

#### QTOP  DISPLAY #######################

if len(NodeInitials) > 1:
    print RMWARNING
print 'PBS report tool. Please try: watch -d ' + QTOPPATH +'. All bugs added by sfranky@gmail.com. Cross fingers now...\n'
print '===> Job accounting summary <=== (Rev: 3000 $) %s WORKDIR = to be added\n' % (datetime.datetime.today())
print 'Usage Totals:\t%s/%s\t Nodes | x/%s\t Cores |\t %s+%s\t jobs (R+Q) reported by qstat -q' %(ExistingNodes-OfflineDownNodes, ExistingNodes, TotalCores, int(TotalRuns), int(TotalQueues) )
# print 'Queues: | '+elem[0]+': '+elem[1]+'+'+elem[2]+' \n' % [elem[0] for elem in qstatqLst], [elem[1] for elem in qstatqLst], [elem[2] for elem in qstatqLst]
print 'Queues: | ',
for i in qstatqLst:
    print i[0]+': '+i[1]+'+'+i[2]+' |',
print '* implies blocked'
print '\n'
print '===> Worker Nodes occupancy <=== (you can read vertically the node IDs; nodes in free state are noted with - )'

# prints the worker node ID number lines


if len(NodeInitials)> 1:
    if RemapNr < 10:
        unit = str(RemapNr)[0]
    elif RemapNr < 100:
        dec = str(RemapNr)[0]
        unit = str(RemapNr)[1]
    elif RemapNr < 1000:
        cent = int(str(RemapNr)[0])
        dec = int(str(RemapNr)[1])
        unit = int(str(RemapNr)[2])
else:
    if LastWN < 10:
        unit = str(LastWN)[0]
    elif LastWN < 100:
        dec = str(LastWN)[0]
        unit = str(LastWN)[1]
    elif LastWN < 1000:
        cent = int(str(LastWN)[0])
        dec = int(str(LastWN)[1])
        unit = int(str(LastWN)[2])    

c, d, d_, u = '','','',''
beginprint = 0

# if there are non-uniform WNs in pbsnodes.yaml, remapping is performed
if len(NodeInitials) == 1:

    if LastWN < 10:
        for node in range(LastWN):
            u += str(node+1)
        print u +'={__WNID__}'
    elif LastWN < 100:
        d_ = '0'*9+'1'*10+'2'*10+'3'*10+'4'*10+'5'*10+'6'*10+'7'*10+'8'*10+'9'*10
        ud = '1234567890'*10
        d = d_[:LastWN]
        print d +             '={_Worker_}'
        print ud[:LastWN] + '={__Node__}'
    elif LastWN < 1000:
        c += str(0)*99
        for i in range(1, cent):
            c += str(i)*100
        c += str(cent)*(int(dec))*10 + str(cent)*(int(unit)+1)
        
        d_ = '0'*9+'1'*10+'2'*10+'3'*10+'4'*10+'5'*10+'6'*10+'7'*10+'8'*10+'9'*10
        d = d_
        for i in range(1, cent):
            d += str(0)+d_
        else:
            d += str(0)
        d += d_[:int(str(dec)+str(unit))]
        
        uc = '1234567890'*100
        ua = uc[:LastWN]

        # clipping functionality:
        '''
        if the earliest node number is high (e.g. 80), the first 79 WNs need not show up.
        '''
        beginprint = 0
        if (CLIPPING == True) and WNList[0]> 30:
            beginprint = WNList[0]-1
        print c[beginprint:] + '={_Worker_}'
        print d[beginprint:] + '={__Node__}'
        print ua[beginprint:]+ '={___ID___}'
        # todo: remember to fix < 100 cases (do i really need to, though?)
elif len(NodeInitials) > 1:
    if RemapNr < 10:
        for node in range(RemapNr):
            u += str(node+1)
        print u+'={__WNID__}'
    elif RemapNr < 100:
        d_ = '0'*9+'1'*10+'2'*10+'3'*10+'4'*10+'5'*10+'6'*10+'7'*10+'8'*10+'9'*10
        ud = '1234567890'*10
        d = d_[:RemapNr]
        print d+            '={_Worker_}'
        print ud[:RemapNr]+'={__Node__}'
    elif RemapNr < 1000:
        c += str(0)*99
        for i in range(1, cent):
            c += str(i)*100
        c += str(cent)*dec*10 + str(cent)*(unit+1)
        
        d_ = '0'*9+'1'*10+'2'*10+'3'*10+'4'*10+'5'*10+'6'*10+'7'*10+'8'*10+'9'*10
        d = d_
        for i in range(1, cent):
            d += str(0)+d_
        else:
            d += str(0)
        d += d_[:int(str(dec)+str(unit))]
        
        uc = '1234567890'*100
        ua = uc[:RemapNr]

        # clipping functionality:
        '''
        if the earliest node number is high (e.g. 80), the first 79 WNs need not show up.
        '''
        beginprint = 0
        if (CLIPPING == True) and WNListRemapped[0]> 30:
            beginprint = WNList[0]-1
        print c[beginprint:] + '={_Worker_}'
        print d[beginprint:] + '={__Node__}'
        print ua[beginprint:]+ '={___ID___}'
        # todo: remember to fix < 100 cases (do i really need to, though?)
    


## end of code outputting workernode id number lines
###################################################


# 14/6  alternative solution for extra 'invisible' WNs
yamlstream = open(HOMEPATH+'qt/pbsnodes.yaml', 'r')
# statebeforeUnsorted = statelst               # get_state(yamlstream)
# if NonExistingNodes:
#     for nonex in NonExistingNodes:
#         statebeforeUnsorted.insert(nonex-1,'?')
#         # stateafter = statebeforeUnsorted[:NonExistingNodes[0]-1]+'?'+statebeforeUnsorted[NonExistingNodes[0]-1:]
#         stateafter = statebeforeUnsorted
# else:
#     stateafter = statebeforeUnsorted

# stateafterstr = ''
# for i in stateafter:    # or stateafterstr = ''.join(stateafter)
#     stateafterstr += i


stateafterstr = ''
if len(NodeInitials) == 1:
    for node in AllWNs:  # why are dictionaries ALWAYS ordered when keys are '1','5','3' etc ?!!?!?
        stateafterstr += AllWNs[node][0]
elif len(NodeInitials) > 1:
    for node in AllWNsRemapped:  # why are dictionaries ALWAYS ordered when keys are '1','5','3' etc ?!!?!?
        stateafterstr += AllWNsRemapped[node][0]

print stateafterstr[beginprint:]+'=Node state'

yamlstream.close()

#############################################

# kati san def readqstat ?
JobIds = []
UnixAccounts = []
Ss = []
Queues = []
finr = open(HOMEPATH+'qt/qstat.yaml', 'r')
for line in finr:
    if line.startswith('JobId:'):
        JobIds.append(line.split()[1])
        # JobIds.append(line.split()[2].split()[0])
    elif line.startswith('UnixAccount:'):
        UnixAccounts.append(line.split()[1])
        # UnixAccounts.append(line.split()[2])
    elif line.startswith('S:'):           
        Ss.append(line.split()[1])
        # Ss.append(line.split()[2])
    elif line.startswith('Queue:'):           
        Queues.append(line.split()[1])
        # Queues.append(line.split()[2])
finr.close()

# antistoixisi unix account me to jobid tou
User2JobDic={}
for user, jobid in zip(UnixAccounts, JobIds):
    User2JobDic[jobid] = user

'''
yamlstream = open(HOMEPATH+'qt/pbsnodes.yaml', 'r')
statebeforeUnsorted = get_state(yamlstream)
stateafter = statebeforeUnsorted[:nonodes[0]-1]+'?'+statebeforeUnsorted[nonodes[0]-1:]
for i in range(1, len(nonodes)):
    stateafter = stateafter[:nonodes[i]-1]+'?'+stateafter[nonodes[i]-1:]
print stateafter+'=Node state'
'''


# solution for counting R, Q, C attached to each user
UserRunningDic, UserQueuedDic, UserCancelledDic, UserWaitingDic, UserEDic = {}, {}, {}, {}, {}

for user, status in zip(UnixAccounts, Ss):
    if status == 'R':
        UserRunningDic[user] = UserRunningDic.get(user, 0) + 1
    elif status == 'Q':
        UserQueuedDic[user] = UserQueuedDic.get(user, 0) + 1
    elif status == 'C':
        UserCancelledDic[user] = UserCancelledDic.get(user, 0) + 1
    elif status == 'W':
        UserWaitingDic[user] = UserWaitingDic.get(user, 0) + 1
    elif status == 'E':
        UserWaitingDic[user] = UserEDic.get(user, 0) + 1

for account in UserRunningDic:
    UserQueuedDic.setdefault(account, 0)
    UserCancelledDic.setdefault(account, 0)
    UserWaitingDic.setdefault(account, 0)
    UserEDic.setdefault(account, 0)

occurencedic={}
for user in UnixAccounts:
    occurencedic[user] = UnixAccounts.count(user)

Usersortedlst = sorted(occurencedic.items(), key = itemgetter(1), reverse = True)


# IdOfUnixAccount = {}
j = 0
possibleIDs = '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ'
for unixaccount in Usersortedlst:
    IdOfUnixAccount[unixaccount[0]] = possibleIDs[j]
    j += 1
########################## end of copied from below

flatjoblist = []
flattened = itertools.chain.from_iterable(BigJobList)
for i in flattened:
    flatjoblist.append(i)
flatjoblist2 = []
for cnt, i in enumerate(flatjoblist):
    flatjoblist2.append((flatjoblist[cnt]['core'], flatjoblist[cnt]['job']))


### CPU lines working !!
CPUCoreDic={}
MaxNPlst = []
Maxcorelst = [str(i) for i in range(maxcores)]
UnusedAndDeclaredlst = []
for i in range(MaxNP):
    CPUCoreDic['Cpu'+str(i)+'line']=''      # Cpu0line, Cpu1line, Cpu2line, .. = '','','', ..
    MaxNPlst.append(str(i))

if len(NodeInitials) > 1:
    for NodeNr, WNPropertiesLst in zip(AllWNsRemapped.keys(), AllWNsRemapped.values()):
        MaxNPlstTmp = MaxNPlst[:] # ( ???? )
        MaxcorelstTmp = Maxcorelst[:] # ( ???? )
        if WNPropertiesLst == '?':
            for CPULine in CPUCoreDic:
                CPUCoreDic[CPULine] += '_'
        elif len(WNPropertiesLst) == 1:
            for CPULine in CPUCoreDic:
                CPUCoreDic[CPULine] += '_'
        else:
            HAS_JOBS = 0
            OwnNP = WNPropertiesLst[1]
            OwnNP = int(OwnNP)
            for element in WNPropertiesLst:
                if type(element) == tuple:  # everytime there is a job:
                    HAS_JOBS += 1
                    # print 'AllWNs[NodeNr][%r] is tuple' %i
                    Core, job = element[0], element[1]
                    CPUCoreDic['Cpu'+str(Core)+'line']+=str(IdOfUnixAccount[UserOfJobId[job]])
                    MaxNPlstTmp.remove(Core)
                    MaxcorelstTmp.remove(Core)
                    s = set(MaxcorelstTmp)
                    UnusedAndDeclaredlst = [x for x in MaxNPlstTmp if x not in s]
            
            # print MaxNPlstTmp                 # disabled it 8/7/12
            if HAS_JOBS != OwnNP:
                # for core in UnusedAndDeclaredlst:
                #     CPUCoreDic['Cpu'+str(core)+'line'] += '#'
                #     UnusedAndDeclaredlst.remove(core)
                for Core in MaxcorelstTmp:
                    CPUCoreDic['Cpu'+str(Core)+'line'] += '_'
        
            if OwnNP < MaxNP:
                for Core in UnusedAndDeclaredlst:
                    CPUCoreDic['Cpu'+str(Core)+'line'] += '#'
            elif OwnNP == MaxNP:
                for Core in UnusedAndDeclaredlst:
                    CPUCoreDic['Cpu'+str(Core)+'line'] += '_'

elif len(NodeInitials) == 1:                
    for NodeNr, WNPropertiesLst in zip(AllWNs.keys(), AllWNs.values()):
            MaxNPlstTmp = MaxNPlst[:] # ( ???? )
            MaxcorelstTmp = Maxcorelst[:] # ( ???? )
            if WNPropertiesLst == '?':
                for CPULine in CPUCoreDic:
                    CPUCoreDic[CPULine] += '_'
            elif len(WNPropertiesLst) == 1:
                for CPULine in CPUCoreDic:
                    CPUCoreDic[CPULine] += '_'
            else:
                HAS_JOBS = 0
                OwnNP = WNPropertiesLst[1]
                OwnNP = int(OwnNP)
                for element in WNPropertiesLst:
                    if type(element) == tuple:  #everytime there is a job:
                        HAS_JOBS += 1
                        Core, job = element[0], element[1]
                        CPUCoreDic['Cpu'+str(Core)+'line'] += str(IdOfUnixAccount[UserOfJobId[job]])
                        MaxNPlstTmp.remove(Core)
                        MaxcorelstTmp.remove(Core)
                        s = set(MaxcorelstTmp)
                        UnusedAndDeclaredlst = [x for x in MaxNPlstTmp if x not in s]
                
                if HAS_JOBS != OwnNP:
                    # for Core in UnusedAndDeclaredlst:
                    #     CPUCoreDic['Cpu'+str(Core)+'line']+='#'
                    #     UnusedAndDeclaredlst.remove(Core)
                    for Core in MaxcorelstTmp:
                        CPUCoreDic['Cpu'+str(Core)+'line'] += '_'
            
                if OwnNP < MaxNP:
                    for Core in UnusedAndDeclaredlst:
                        CPUCoreDic['Cpu'+str(Core)+'line'] += '#'
                elif OwnNP == MaxNP:
                    for Core in UnusedAndDeclaredlst:
                        CPUCoreDic['Cpu'+str(Core)+'line'] += '_'
                else:
                    print 'no SHIT!!'



# for cnt, state in enumerate(stateafterstr, 1):
#     '''
#     For each node, traverse the cores and jobs active, and add the respective Unix IDs to each of the CPUx lines
#     '''
#     if state == '?':
#         for CPULine in CPUCoreDic:
#             CPUCoreDic[CPULine] += '?'
#     Maxcorelst2 = Maxcorelst[:]
#     for core, job in zip(big[cnt]['core'], big[cnt]['job']):
#         '''
#         CPUCoreDic['Cpu1line'] += '8'
#         '''
#         if core in Maxcorelst2:
#             Maxcorelst2.remove(core)
#         CPUCoreDic['Cpu'+str(core)+'line'] += str(IdOfUnixAccount[UserOfJobId[job]])
#         # CPUCoreDic['Cpu'+str(unused)+'line'] += '_'
        
#     for unused in Maxcorelst2:
#         CPUCoreDic['Cpu'+str(unused)+'line'] += '_'

#CpucoreList = []
# sorted(d.items(), key = itemgetter(1))
# CpucoreList.sort(CPUCoreDic.items(), key = itemgetter(3), reverse = True)
for ind, k in enumerate(CPUCoreDic):
    # print CPUCoreDic[k]+'=CPU'+str(ind)
    print CPUCoreDic['Cpu'+str(ind)+'line'][beginprint:]+'=CPU'+str(ind)



print '\n'
print '===> User accounts and pool mappings <=== ("all" includes those in C and W states, as reported by qstat)'
print 'id |   R +   Q / all |  unix account  | Grid certificate DN (this info is only available under elevated privileges)'

qstatLst.sort(key = lambda unixaccount: unixaccount[1])   # sort by unix account

  
AssIdvalues = IdOfUnixAccount.values()
AssIdkeys = IdOfUnixAccount.keys()
# UserRunningDicValues = UserRunningDic.values()
# UserRunningDickeys = UserRunningDic.keys()
# UserCancelledDicValues = UserCancelledDic.values()
# UserCancelledDickeys = UserCancelledDic.keys()
# UserQueuedDicValues = UserQueuedDic.values()
# UserQueuedDickeys = UserQueuedDic.keys()

# this calculates and prints what is actually below the id| R+Q /all | unix account etc line
for id in IdOfUnixAccount:
    if id not in UserRunningDic:
        UserRunningDic[id]=0
    if id not in UserQueuedDic:
        UserQueuedDic[id]=0
    if id not in UserCancelledDic:
        UserCancelledDic[id]=0
    if id not in UserWaitingDic:
        UserWaitingDic[id]=0
    if id not in UserEDic:
        UserEDic[id]=0


for id in Usersortedlst:# IdOfUnixAccount:
    AccountsMappings.append([IdOfUnixAccount[id[0]], UserRunningDic[id[0]], UserQueuedDic[id[0]], UserCancelledDic[id[0]]+ UserRunningDic[id[0]]+ UserQueuedDic[id[0]]+ UserWaitingDic[id[0]]+ UserEDic[id[0]], id])
####### workaround, na brw veltistopoiisi
AccountsMappings.sort(key = itemgetter(3), reverse = True)
for line in AccountsMappings:
    print '%2s | %3s + %3s / %3s | %14s |' % (line[0], line[1], line[2], line[3], line[4][0])


os.chdir(QTOPPATH)
# print NodeInitials
# print RemapNr

print 'Thanks for watching!'