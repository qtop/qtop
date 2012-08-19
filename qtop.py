#!/usr/bin/env python

################################################
#                                              #
#              qtop v.0.2.5                    #
#                                              #
#     Licensed under MIT-GPL licenses          #
#                                              #
#                     Fotis Georgatos          #
#                     Sotiris Fragkiskos       #
################################################

"""

changelog:
=========
0.2.6: fixed some names not being detected (%,= chars missing from regex)
0.2.5: Working Cores added in Usage Totals
       Feature added: map now splits into two if terminal width is smaller than
        the Worker Node number
0.2.4: implemented some stuff from PEP8
       un-hardwired the file paths
       refactored code around CPUCoreDic functionality (responsible for drawing
        the map)
0.2.3: corrected regex search pattern in make_qstat to recognize usernames like
 spec101u1 (number followed by number followed by letter)
       now handles non-uniform setups
        R + Q / all: all did not display everything (E status)
0.2.2: clipping functionality (when nodes start from e.g. wn101, empty columns
 1-100 are ommited)
0.2.1: Hashes displaying when the node has less cores than the max declared by
a WN (its np variable)
0.2.0: unix accounts are now correctly ordered
0.1.9: All CPU lines displaying correctly
0.1.8: unix account id assignment to CPU0, 1 implemented
0.1.7: ReadQstatQ function (write in yaml format using Pyyaml)
       output up to Node state !
0.1.6: ReadPbsNodes function (write in yaml format using Pyyaml)
0.1.5: implemented saving to 3 separate files, QSTAT_ORIG_FILE,
QSTATQ_ORIG_FILE, PBSNODES_ORIG_FILE
0.1.4: some "wiremelting" concerning the save directory
0.1.3: fixed tabs-to-spaces. Formatting should be correct now.
       Now each state is saved in a separate file in a results folder
0.1.2: script reads qtop-input.out files from each job and displays status for
each job
0.1.1: changed implementation in get_state()

0.1.0: just read a pbsnodes-a output file and gather the results in a single
line


"""


from operator import itemgetter
import datetime
import glob
import itertools
import os
import re
import sys
# import yaml
import copy
# from qtcolormap import *
# import qtcolormap

ColorOfAccount = {
    'Atlassm':   'Red_L',
    'sgmatlas':  'Red_L',
    'patlas':    'Red_L',
    'satlas':    'Red_L',
    'satl':  'Red_L',
    'atlassgm':  'Red_L',
    'atlsgm':    'Red_L',
    'atlsg': 'Red_L',
    'atlasusr':  'Red_L',
    'atlass':    'Red_L',
    'laspt': 'Red_L',
    'atlpilot':  'Red_L',
    'atpilot':   'Red_L',
    'iatpilot':  'Red_L',
    'atlasplt':  'Red_L',
    'atlaspt':   'Red_L',
    'atlaspil':  'Red_L',
    'atlasplot': 'Red_L',
    'atlplot':   'Red_L',
    'atlpil':    'Red_L',
    'atlpl': 'Red_L',
    'atlaspilot': 'Red_L',
    'atlprod':   'Red',
    'atlasger':  'Red',
    'atlasde':   'Red',
    'datlas':    'Red',
    'atlasit':   'Red',
    'atlde': 'Red',
    'atlit': 'Red',
    'atlasprod': 'Red',
    'atlasprd':  'Red',
    'atlsprd':   'Red',
    'atlprd':    'Red',
    'atlpr': 'Red',
    'iatlas':    'Red',
    'iatprd':    'Red',
    'iatl':  'Red',
    'aatlpd':    'Red',
    'atlasfx':   'Red',
    'atlastw':   'Red',
    'atlx':  'Red',
    'atlp':  'Red',
    'atlu':  'Red',
    'atlhs': 'Red',
    'atlashs':   'Red',
    'atlasp':    'Red',
    'atlasfr':   'Red',
    'atlasana':  'Red',
    'Eatlas':     'Red',
    'shatlas':    'Red',
    'lcgatlas':   'Red',
    'atlasL':     'Red',
    'atlasil':    'Red',
    'atlanaly':   'Red',
    'atlas': 'Red',
    'atlbr': 'Red',
    'atls':  'Red',
    'atl':   'Red',
    'atfx':  'Red',
    'atprd': 'Red',
    'atcanpu':   'Red',
    'atcanpt':   'Red_L',
    'atcan': 'Red',
    'atcpu': 'Red',
    'atsgm': 'Red_L',
    'pilatlas':  'Red_L',
    'pilatl':    'Red_L',
    'zipilatl':  'Red_L',
    'patlit':    'Red_L',
    'platl': 'Red_L',
    'pltatlas':  'Red_L',
    'pltatl':    'Red_L',
    'atplt': 'Red_L',
    'atlplt':    'Red_L',
    'piatlas':   'Red_L',
    'piatla':    'Red_L',
    'piatl': 'Red_L',
    'sgmatl':    'Red_L',
    'zisgmatl':  'Red_L',
    # sgmatl:   'Red',
    'prdatlas':  'Red',
    'prdatl':    'Red',
    'prdat': 'Red',
    'ziprdatl':  'Red',
    'ziatlas':   'Red',
    'patls': 'Red',
    'patlit':    'Red',
    'patl':  'Red',
    'nordugrid-atlas': 'Red',
    'usatlas':   'Red',
    # CMS VO commonly found pool account names
    'cmssgm':    'Green_L',
    'cmsplt':    'Green_L',
    'pltcms':    'Green_L',
    'cmspilot':  'Green_L',
    'cmspil':    'Green_L',
    'pilcms':    'Green_L',
    'pcms':  'Green_L',
    'sgmcms':    'Green_L',
    'sgmcm': 'Green_L',
    'scms':  'Green_L',
    'priocms':   'Green_L',
    'cmsprio':   'Green_L',
    'pricms':    'Green_L',
    'cmsprod':   'Green',
    'cmsprd':    'Green',
    'cmsmcp':    'Green',
    'cmsnu': 'Green',
    'cmst1prd': 'Green',
    'cmprd': 'Green',
    'uscmsPool': 'Green',
    'uscms': 'Green',
    'cmsp':  'Green',
    'cmsusr':    'Green',
    'cmsuwu':    'Green',
    'cmsana':    'Green',
    'cmsger':    'Green',
    'cmss':  'Green',
    'cmszu': 'Green',
    'cmsau': 'Green',
    'twcms': 'Green',
    'cmsmu': 'Green',
    'cms':   'Green',
    'dcms':  'Green',
    'prdcms':    'Green',
    'icms':  'Green',
    'cms': 'Green',
    # ALICE VO commonly found pool account names
    'alicesgm':  'Cyan',
    'alice': 'Cyan',
    'alisc': 'Cyan',
    'alisgm':    'Cyan',
    'alisg': 'Cyan',
    'alibs': 'Cyan',
    'alis':  'Cyan',
    'alikn': 'Cyan',
    'ali':   'Cyan',
    'ialice':    'Cyan',
    'salice':    'Cyan',
    'sali':  'Cyan',
    'caliceuser':    'Cyan',
    'caliceusr': 'Cyan',
    'calice':    'Cyan',
    'calic': 'Cyan',
    'sgmalice':  'Cyan',
    'sgmali':    'Cyan',
    # LHCb VO commonly found pool account names
    'pdlhcb':    'Pink',
    'prdlhcb':   'Pink',
    'prdlhb':    'Pink',
    'prdlhc':    'Pink',
    'lhcbprd':   'Pink',
    'lhbprd':    'Pink',
    'lhprd': 'Pink',
    'ilhcb': 'Pink',
    'lhcbsgm':   'Purple',
    'lhcbs': 'Purple',
    'sgmlhcb':   'Purple',
    'sgmlhb':    'Purple',
    'sgmlhc':    'Purple',
    'lhcbplt':   'Purple',
    'lhcbpilot': 'Purple',
    'lhpilot':   'Purple',
    'lhcpilot':  'Purple',
    'lhcbpil':   'Purple',
    'lhbpil':    'Purple',
    'pillhcb':   'Purple',
    'plhcb': 'Purple',
    'pilhcb':    'Purple',
    'pltlhcb':   'Purple',
    'pillhb':    'Purple',
    'pllhc': 'Purple',
    'tlhcb': 'Pink',
    'lhcbhs':    'Pink',
    'lhcbp': 'Pink',
    'lhcb':  'Pink',
    'lhcp':  'Pink',
    # dteam VO commonly found pool account names
    'dteamsgm':    'Brown',
    'dteamprd':    'Brown',
    'dteamuser': 'Brown',
    'dteamusr':  'Brown',
    'dteam': 'Brown',
    'dte':   'Brown',
    # OPS VO commonly found pool account names
    'opsplt':    'Yellow',
    'opsusr':    'Yellow',
    'opssgm':    'Yellow',
    'opssg': 'Yellow',
    'opsgm': 'Yellow',
    'sgmops':    'Yellow',
    'zisgmops':  'Yellow',
    'opsprd':    'Yellow',
    'opspil':    'Yellow',
    'pilops':    'Yellow',
    'samgrid':   'Yellow',
    'opss':  'Yellow',
    'sops':  'Yellow',
    'opsiber':   'Yellow',
    'opsib': 'Yellow',
    'ops':   'Yellow',
    #
    # Other VOs from the EGEE-I,II,III era
    'egee':  'Blue_L',
    # Biomed VO
    'biomedusr': 'Blue_L',
    'biomed':    'Blue_L',
    'biomd': 'Blue_L',
    'biome': 'Blue_L',
    'biocw': 'Blue_L',
    'biostats':  'Blue_L',
    'biotech':   'Blue_L',
    'bio':   'Blue_L',
    # Gear VO
    'gearsgm':     'Blue_L',
    'gearprd':    'Blue_L',
    'gear':  'Blue_L',
    # DECH VO
    'dechsgm':     'Blue_L',
    'dechprd':   'Blue_L',
    'dechusr':   'Blue_L',
    'dech':  'Blue_L',
    # SEE VO
    'seeops':    'Blue_L',
    'seops': 'Blue_L',
    'seegrid':   'Blue_L',
    'seevo': 'Blue_L',
    'see':   'Blue_L',
    # ESR VO
    'esrsgm':    'Blue_L',
    'esr':   'Blue_L',
    'earthscience':  'Blue_L',
    # Fusion, auvergrid, compchem, enmr, voce, gaussian, balticgrid,
    # Digital Media VOs
    'fusionprd': 'Blue_L',
    'fusionsgm': 'Blue_L',
    'fusion':    'Blue_L',
    'fusio': 'Blue_L',
    'fusi':  'Blue_L',
    'fusn':  'Blue_L',
    'fus':   'Blue_L',
    'auvergrid': 'Blue_L',
    'enmr':  'Blue_L',
    'complex':   'Blue_L',
    'compchem':  'Blue_L',
    'compc': 'Blue_L',
    'compl': 'Blue_L',
    'cmplx': 'Blue_L',
    'vocesgm':   'Blue_L',
    'voceprd':   'Blue_L',
    'voce':  'Blue_L',
    'gaussian':  'Blue_L',
    'balticgrid': 'Blue_L',
    'digmedia':  'Blue_L',
    'dmedia':    'Blue_L',
    # Hone VO
    'prdhone':   'Cyan_L',
    'prdhne':    'Cyan_L',
    'prdhon':    'Cyan_L',
    'honecker':  'Cyan_L',
    'honeprd':   'Cyan_L',
    'honesgm':   'Cyan_L',
    'phone': 'Cyan_L',
    'hone':  'Cyan_L',
    'honp':  'Cyan_L',
    # Other (High Energy) Physics VOs
    'sixto': 'Cyan_L',
    'sixt':  'Cyan_L',
    'babaradm':  'Cyan_L',
    'babarpro':  'Cyan_L',
    'babar': 'Cyan_L',
    'pheno': 'Cyan_L',
    'dzerojim':  'Cyan_L',
    'dzero': 'Cyan_L',
    'dze':   'Cyan_L',
    'dzerqa':    'Cyan_L',
    'theophys_': 'Cyan_L',
    'theophys':  'Cyan_L',
    'zeususr':   'Cyan_L',
    'zeus':  'Cyan_L',
    'argo':  'Cyan_L',
    'ilcprd':    'Cyan_L',
    'ilcpr': 'Cyan_L',
    'ilcp':  'Cyan_L',
    'ilcusr':    'Cyan_L',
    'ilcger':    'Cyan_L',
    'ilc':   'Cyan_L',
    'prdilc':    'Cyan_L',
    'pilc':  'Cyan_L',
    'sgmilc':    'Cyan_L',
    'augersgm':  'Cyan_L',
    'augerprd':  'Cyan_L',
    'auger': 'Cyan_L',
    'augp':  'Cyan_L',
    'aug':   'Cyan_L',
    'chatlas':    'Cyan_L',
    'chcms':  'Cyan_L',
    'chlhcb': 'Cyan_L',
    'geant': 'Cyan_L',
    'lhcft': 'Cyan_L',
    'magic': 'Cyan_L',
    'scier': 'Cyan_L',
    'planck':    'Cyan_L',
    # Other VOs' commonly found pool account names
    # cal:  'White',
    # envir:    'White',
    # gridcc:   'White',
    # hgdemo:   'White',
    # vlemd:    'White',
    # lsgrd:    'White',
    # tsc:  'White',
    # prod: 'White',
    # argo: 'White',
    # swegr:    'White',
    # desrcs:   'White',
    # camont:   'White',
    # dorii:    'White',
    # durdagis: 'White',
    # dur:  'White',
    # cpp:  'White',
    # ssp:  'White',
    # ifc:  'White',
    # ilcp: 'White',
    # ilc:  'White',
    # comsya:   'White',
    # com:  'White',
    # isc:  'White',
    # lal:  'White',
    # sbgrid:   'White',
    # sbg:  'White',
    # osg:  'White',
    # lpsc: 'White',
    # icecubeprd: 'White',
    # icecube:  'White',
    # icep       'White',
    # ice:  'White',
    # mame: 'White',
    # hoeth:    'White',
    # lenz: 'White',
    # gridit:   'White',
    # suzanne:  'White',
    # ngs:  'White',
    # minos:    'White',
    # pamna:    'White',
    # gilda:    'White',
    # auth: 'White',
    # ctaibp:   'White',
    # ctap: 'White',
    # cta:  'White',
    # glarvu:   'White',
    # plgrid:   'White',
    # aegis:    'White',
    # seismo:   'White',
    # meteo:    'White',
    # desktopg: 'White',
    # suprms:   'White',
    # ego:  'White',
    # dte:  'White',
    # ad:   'White',
    # a tiny little gift for kaust-prefixed names
    'kaust': 'Brown',
    # Extras; these are really randomly found user names; needed for screen
    # clarity while in color more
    'mwilli':      'Brown',
    'por':     'Blue',
    'dorisf':      'Cyan',
    'campoman':    'Brown',
    'gallet':      'Blue',
    'tlemmin':     'Cyan',
    'ls': 'Cyan',
    'train': 'Blue_L',
    'train': 'Blue_L',
    'pkoro':           'Brown',
    'fotis':           'Blue',
    'astrelchenko':    'Cyan',
    'tchristoudias':   'Brown'
    # catch-all rule for many more names
    #[[:alpha:]][-_[:alnum:].]* 'Gray_L',
}

CodeOfColor = {
    'Red_L': '1;31',
    'Red': '0;31',
    'Green_L': '1;32',
    'Green': '0;32',
    'Cyan': '0;36',
    'Pink': '1;35',
    'Purple': '0;35',
    'Brown': '0;33',
    'Yellow': '1;33',
    'Blue_L': '1;34',
    'Cyan_L': '1;36',
    'White': '1;37',
    'Blue': '0;34',
    'Gray_L': '1;37',
    'normal':   '0'
}


def Colorize(text, pattern):
    """print text colored according to its unix account colors"""
    return "\033[" + CodeOfColor[ColorOfAccount[pattern]] + "m" + text + \
        "\033[1;m"
     # print '\033[1;35mMagenta like Mimosa\033[1;m'
HOMEPATH = os.path.expanduser('~/')
OUTPUTPATH = os.path.expanduser('~/qtop-input/outputs/')
QTOPPATH = os.path.expanduser('~/qtop/qtop')
PROGDIR = os.path.expanduser('~/off/qtop')
# SAVEDIR = os.path.expanduser('~/qtop-input/results')

# Location of read and created files
PBSNODES_ORIG_FILE = 'pbsnodes_a.txt'
QSTATQ_ORIG_FILE = 'qstat_q.txt'
QSTAT_ORIG_FILE = 'qstat.txt'
#PBSNODES_ORIG_FILE = 'pbsnodes.out'
#QSTATQ_ORIG_FILE = 'qstat-q.out'
#QSTAT_ORIG_FILE = 'qstat.out'

PBSNODES_YAML_FILE = HOMEPATH + 'qt/pbsnodes.yaml'
QSTATQ_YAML_FILE = HOMEPATH + 'qt/qstat-q.yaml'
QSTAT_YAML_FILE = HOMEPATH + 'qt/qstat.yaml'

# if not os.path.exists(SAVEDIR):
#     cmd = 'mkdir ' + SAVEDIR
#     fp = os.popen(cmd)   # create dir ~/qtop-input/results if it doesn't
#                          # exist already

# IDs of unix accounts, for the lower part of qtop
POSSIBLE_IDS = '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz'
#for calculating the WN numbers
c, d, u = '', '', ''
PrintStart = 0

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
        searchdname = '^\w+(\.\w+)*'
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
            for job in ljobs:
                core = job.strip().split('/')[0]
                job = job.strip().split('/')[1:][0].split('.')[0]
                fout.write('- core: ' + core + '\n')
                fout.write('  job: ' + job + '\n')

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
        searchdname = 'domainname: ' + '(\w+(\.\w+)*)'
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
        # print 'successful case was ', case

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
    global c, d, u, PrintStart, PrintEnd
    if WNnumber < 10:
        unit = str(WNnumber)[0]

        for node in range(WNnumber):
            u += str(node + 1)
        # print u + '={__WNID__}'
        print_WN_ID_lines(PrintStart, PrintEnd, WNnumber)

    elif WNnumber < 100:
        dec = str(WNnumber)[0]
        unit = str(WNnumber)[1]

        d_ = '0' * 9 + '1' * 10 + '2' * 10 + '3' * 10 + '4' * 10 + '5' * 10 + '6' * 10 + '7' * 10 + '8' * 10 + '9' * 10
        u = '1234567890' * 10
        d = d_[:WNnumber]
        # print d +            '={_Worker_}'
        # print u[:WNnumber] + '={__Node__}'
        print_WN_ID_lines(PrintStart, PrintEnd, WNnumber)

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

        '''
        masking/clipping functionality: if the earliest node number is high (e.g. 80), the first 79 WNs need not show up.
        '''
        if (CLIPPING is True) and WNList[0] > 30:
            PrintStart = WNList[0] - 1

        print_WN_ID_lines(PrintStart, PrintEnd, WNnumber)

        # todo: remember to fix < 100 cases (do i really need to, though?)


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

empty_yaml_files()

os.chdir(OUTPUTPATH)
OutputDirs += glob.glob('sfragk*')
OutputDirs += glob.glob('fotis*')
OutputDirs += glob.glob('gef*')


for dir in OutputDirs:
    # if dir == 'fotistestfiles': # periergo
    # if dir == 'sfragk_iLu0q1CbVgoDFLVhh5NGNw': # 188 WNs, double map
    # if dir == 'sfragk_tEbjFj59gTww0f46jTzyQA':  # implement clip/masking functionality !! problem me mikro width, split se normal plati o8onis
    # if dir == 'sfragk_sDNCrWLMn22KMDBH_jboLQ':  # OK
    # if dir == 'sfragk_aRk11NE12OEDGvDiX9ExUg':  # OK
    # if dir == 'sfragk_gHYT96ReT3-QxTcvjcKzrQ':  # OK
    # if dir == 'sfragk_zBwyi8fu8In5rLu7RBtLJw':  # OK
    # if dir == 'sfragk_sE5OozGPbCemJxLJyoS89w':  # seems ok !
    # if dir == 'sfragk_vshrdVf9pfFBvWQ5YfrnYg':  # OK
    # if dir == 'sfragk_R__ngzvVl5L22epgFVZOkA':  # OK - 4WNs, 8 hashes
    # if dir == 'sfragk_qWU7q3Y9qb2knm-bgb_O1Q':  # OK
    # if dir == 'gef_7vxNwO1hVGAmQW89KBdumg': #  OK (exemplar dataset)
    # if dir == 'gef_Onj4kWILiJh12VbeD5OBJg': #  OK 
    # if dir == 'gef_GfcjdUE0LRzQJcCtMiQ3Pw': #  OK
    # if dir == 'gef_6Q4OUrw5F_mx85S0JNaZpQ': #  double jobs in wn resulting in bugs
    # if dir == 'gef_8KkrK6_AmC2Fuw6QFsjcSg': #  empty pbsnodes!
    if dir == 'gef_j_tdFirMT-h7aAamev8oKg': #  bugs
    # if dir == 'gef_mplRBNMIVNEeKPvEBjPdZg': #  ok but very small
    # if dir == 'gef_Xe31ZK_keTUrLLrGGczYlw': # no WN numbering
    # if dir == 'gef_LQJsv6kz3kUu7LEj5kzoZA': #  double IDs !!

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
        make_qstatq_yaml(fin2, yamlstream2)
        fin2.close()
        yamlstream2.close()

        fin3 = open(QSTAT_ORIG_FILE, 'r')
        make_qstat_yaml(fin3, yamlstream3)
        fin3.close()
        yamlstream3.close()

        read_qstat()
        os.chdir('..')


#Calculation of split screen size

TermRows, TermColumns = os.popen('stty size', 'r').read().split()
TermColumns = int(TermColumns)

DEADWEIGHT = 15  # columns on the left and right of the CPUx map
Dx = TermColumns - (BiggestWrittenNode + DEADWEIGHT)
if Dx < 0:
    #split in x+1 pieces, where x = (BiggestWrittenNode+15)/termcolumns
    PrintEnd = TermColumns - DEADWEIGHT
else:
    PrintEnd = None


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
    print unixaccount
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

if Dx < 0:
    print '\n'
    if len(NodeSubClusters) == 1:
        print_WN_ID_lines(PrintEnd, BiggestWrittenNode, LastWN)
    if len(NodeSubClusters) > 1:
        print_WN_ID_lines(PrintEnd, BiggestWrittenNode, RemapNr)
    print NodeState[PrintEnd:BiggestWrittenNode] + '=Node state'
    for ind, k in enumerate(CPUCoreDic):
        print CPUCoreDic['Cpu' + str(ind) + 'line'][PrintEnd:BiggestWrittenNode] + '=CPU' + str(ind)

###########################################################################################################################

# qstatLst.sort(key = lambda unixaccount: unixaccount[1])   # sort by unix account


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
        print
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


def printc(text, color):
    """Print in color."""
    print "\033[" + CodeOfColor[color] + "m" + text + "\033[0m"


def writec(text, color):
    """Write to stdout in color."""
    sys.stdout.write("\033[" + CodeOfColor[color] + "m" + text + "\033[0m")


def switchColor(color):
    """Switch console color."""
    sys.stdout.write("\033[" + CodeOfColor[color] + "m")

