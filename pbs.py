import variables
import re

def make_pbsnodes_yaml(fin, fout):
    """
    read PBSNODES_ORIG_FILE sequentially and put in respective yaml file
    """
    OfflineDownNodes = 0 
    # global OfflineDownNodes

    for line in fin:
        line.strip()
        searchdname = '^\w+([.-]?\w+)*'
        if re.search(searchdname, line) is not None:   # line containing domain name
            m = re.search(searchdname, line)
            dname = m.group(0)
            fout.write('domainname: ' + dname + '\n')

        elif 'state = ' in line:
            nextchar = line.split()[2][0]
            if nextchar == 'f':
                state = '-'
            elif (nextchar == 'd') | (nextchar == 'o'):
                state = nextchar
                OfflineDownNodes += 1
            else:
                state = nextchar
            fout.write('state: ' + state + '\n')

        elif 'np = ' in line or 'pcpus = ' in line:
            np = line.split()[2][0:]
            # TotalCores = int(np)
            fout.write('np: ' + np + '\n')

        elif 'jobs = ' in line:
            ljobs = line.split('=')[1].split(',')
            lastcore = 150000
            for job in ljobs:
                # core = job.strip().split('/')[0]
                # job = job.strip().split('/')[1:][0].split('.')[0]
                core, job = job.strip().split('/')
                if len(core) > len(job): # that can't be the case, so we got it wrong (jobs format must be jobid/core instead of core/jobid)
                    core, job = job, core
                job = job.strip().split('/')[0].split('.')[0]
                if core == lastcore:
                    print 'There are concurrent jobs assigned to the same core!' + '\n' +' This kind of Remapping is not implemented yet. Exiting..'
                    sys.exit(1)
                fout.write('- core: ' + core + '\n')
                fout.write('  job: ' + job + '\n')
                lastcore = core

        elif 'gpus = ' in line:
            gpus = line.split(' = ')[1]
            fout.write('gpus: ' + gpus + '\n')

        elif line.startswith('\n'):
            fout.write('\n')

        # elif 'ntype = PBS' in line:
        #     print 'System currently not supported!'
        #     sys.exit(1)
    fin.close()
    fout.close()
    return OfflineDownNodes

def make_qstat_yaml(fin, fout):
    """
    read QSTAT_ORIG_FILE sequentially and put useful data in respective yaml file
    """
    firstline = fin.readline()
    if 'prior' not in firstline:
        UserQueueSearch = '^(([0-9-]+)\.([A-Za-z0-9-]+))\s+([A-Za-z0-9%_.=+/-]+)\s+([A-Za-z0-9.]+)\s+(\d+:\d+:?\d*|0)\s+([CWRQE])\s+(\w+)'
        RunQdSearch = '^\s*(\d+)\s+(\d+)'
        for line in fin:
            line.strip()
            # searches for something like: 422561.cream01             STDIN            see062          48:50:12 R see
            if re.search(UserQueueSearch, line) is not None:
                m = re.search(UserQueueSearch, line)
                Jobid, Jobnr, CEname, Name, User, TimeUse, S, Queue = m.group(1), m.group(2), m.group(3), m.group(4), m.group(5), m.group(6), m.group(7), m.group(8)
                Jobid = Jobid.split('.')[0]
                fout.write('---\n')
                fout.write('JobId: ' + Jobid + '\n')
                fout.write('UnixAccount: ' + User + '\n')
                fout.write('S: ' + S + '\n')
                fout.write('Queue: ' + Queue + '\n')

                variables.UserOfJobId[Jobid] = User # this actually belongs to read_qstat() !
                fout.write('...\n')
    elif 'prior' in firstline:
        # e.g. job-ID  prior   name       user         state submit/start at     queue                          slots ja-task-ID 
        DIFFERENT_QSTAT_FORMAT_FLAG = 1
        UserQueueSearch = '\s{2}(\d+)\s+([0-9]\.[0-9]+)\s+([A-Za-z0-9_.-]+)\s+([A-Za-z0-9._-]+)\s+([a-z])\s+(\d{2}/\d{2}/\d{2}|0)\s+(\d+:\d+:\d*|0)\s+([A-Za-z0-9_]+@[A-Za-z0-9_.-]+)\s+(\d+)\s+(\w*)'
        RunQdSearch = '^\s*(\d+)\s+(\d+)'
        for line in fin:
            line.strip()
            # searches for something like: 422561.cream01             STDIN            see062          48:50:12 R see
            if re.search(UserQueueSearch, line) is not None:
                m = re.search(UserQueueSearch, line)
                Jobid, Prior, Name, User, State, Submit, StartAt, Queue, QueueDomain, Slots, Ja_taskID = m.group(1), m.group(2), m.group(3), m.group(4), m.group(5), m.group(6), m.group(7), m.group(8), m.group(9), m.group(10), m.group(11)
                print Jobid, Prior, Name, User, State, Submit, StartAt, Queue, QueueDomain, Slots, Ja_taskID
                fout.write('---\n')
                fout.write('JobId: ' + Jobid + '\n')
                fout.write('UnixAccount: ' + User + '\n')
                fout.write('S: ' + State + '\n')
                fout.write('Queue: ' + Queue + '\n')

                variables.UserOfJobId[Jobid] = User
                fout.write('...\n')

def make_qstatq_yaml(fin, fout):
    # ex-global TotalRuns, TotalQueues #qstatqLst
    """
    read QSTATQ_ORIG_FILE sequentially and put useful data in respective yaml file
    """
    Queuesearch = '^([a-zA-Z0-9_.-]+)\s+(--|[0-9]+[mgtkp]b[a-z]*)\s+(--|\d+:\d+:?\d*)\s+(--|\d+:\d+:\d+)\s+(--)\s+(\d+)\s+(\d+)\s+(--|\d+)\s+([DE] R)'
    RunQdSearch = '^\s*(\d+)\s+(\d+)'
    for line in fin:
        line.strip()
        # searches for something like: biomed             --      --    72:00:00   --   31   0 --   E R
        if re.search(Queuesearch, line) is not None:
            m = re.search(Queuesearch, line)
            _, QueueName, Mem, CPUtime, Walltime, Node, Run, Queued, Lm, State = m.group(0), m.group(1), m.group(2), m.group(3), m.group(4), m.group(5), m.group(6), m.group(7), m.group(8), m.group(9)
            variables.qstatqLst.append((QueueName, Run, Queued, Lm, State))
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
    return TotalRuns, TotalQueues