import re
import sys
import os

MAX_CORE_ALLOWED = 150000


def make_pbsnodes_yaml(orig_file, yaml_file):
    """
    read PBSNODES_ORIG_FILE sequentially and put in respective yaml file
    """
    if not os.path.getsize(orig_file) > 0:
        print 'Bailing out... Not yet ready for Sun Grid Engine clusters'
        # os.chdir(HOMEPATH + 'qt')
        sys.exit(0)

    with open(orig_file, 'r') as fin, open(yaml_file, 'a') as fout:
        for line in fin:
            line.strip()
            search_domain_name = '^\w+([.-]?\w+)*'
            if re.search(search_domain_name, line) is not None:   # line containing domain name
                m = re.search(search_domain_name, line)
                domain_name = m.group(0)
                fout.write('domainname: ' + domain_name + '\n')

            elif 'state = ' in line:
                nextchar = line.split()[2][0]
                if nextchar == 'f':
                    state = '-'
                elif (nextchar == 'd') | (nextchar == 'o'):
                    state = nextchar
                    # offline_down_nodes += 1
                else:
                    state = nextchar
                fout.write('state: ' + state + '\n')

            elif 'np = ' in line or 'pcpus = ' in line:
                np = line.split()[2][0:]
                # total_cores = int(np)
                fout.write('np: ' + np + '\n')

            elif 'jobs = ' in line:
                ljobs = line.split('=')[1].split(',')
                lastcore = MAX_CORE_ALLOWED
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


def make_qstat_yaml(orig_file, yaml_file):
    """
    read QSTAT_ORIG_FILE sequentially and put useful data in respective yaml file
    """
    if not os.path.getsize(orig_file) > 0:
        print 'Your ' + orig_file + ' file is empty! Please check your directory. Exiting ...'
        os.chdir(HOMEPATH + 'qt')
        sys.exit(0)

    with open(orig_file, 'r') as fin, open(yaml_file, 'a') as fout:
        first_line = fin.readline()
        if 'prior' not in first_line:
            user_queue_search = '^(([0-9-]+)\.([A-Za-z0-9-]+))\s+([A-Za-z0-9%_.=+/-]+)\s+([A-Za-z0-9.]+)\s+(\d+:\d+:?\d*|0)\s+([CWRQE])\s+(\w+)'
            run_qd_search = '^\s*(\d+)\s+(\d+)'
            for line in fin:
                line.strip()
                # searches for something like: 422561.cream01             STDIN            see062          48:50:12 R see
                if re.search(user_queue_search, line) is not None:
                    m = re.search(user_queue_search, line)
                    job_id, Jobnr, ce_name, Name, User, time_use, S, Queue = m.group(1), m.group(2), m.group(3), m.group(4), m.group(5), m.group(6), m.group(7), m.group(8)
                    job_id = job_id.split('.')[0]
                    fout.write('---\n')
                    fout.write('JobId: ' + job_id + '\n')
                    fout.write('UnixAccount: ' + User + '\n')
                    fout.write('S: ' + S + '\n')
                    fout.write('Queue: ' + Queue + '\n')
                    fout.write('...\n')

        elif 'prior' in first_line:
            # e.g. job-ID  prior   name       user         state_dict submit/start at     queue                          slots ja-task-ID
            DIFFERENT_QSTAT_FORMAT_FLAG = 1
            user_queue_search = '\s{2}(\d+)\s+([0-9]\.[0-9]+)\s+([A-Za-z0-9_.-]+)\s+([A-Za-z0-9._-]+)\s+([a-z])\s+(\d{2}/\d{2}/\d{2}|0)\s+(\d+:\d+:\d*|0)\s+([A-Za-z0-9_]+@[A-Za-z0-9_.-]+)\s+(\d+)\s+(\w*)'
            run_qd_search = '^\s*(\d+)\s+(\d+)'
            for line in fin:
                line.strip()
                # searches for something like: 422561.cream01             STDIN            see062          48:50:12 R see
                if re.search(user_queue_search, line) is not None:
                    m = re.search(user_queue_search, line)
                    job_id, Prior, Name, User, State, Submit, start_at, Queue, queue_domain, Slots, Ja_taskID = m.group(1), m.group(2), m.group(3), m.group(4), m.group(5), m.group(6), m.group(7), m.group(8), m.group(9), m.group(10), m.group(11)
                    print job_id, Prior, Name, User, State, Submit, start_at, Queue, queue_domain, Slots, Ja_taskID
                    fout.write('---\n')
                    fout.write('JobId: ' + job_id + '\n')
                    fout.write('UnixAccount: ' + User + '\n')
                    fout.write('S: ' + State + '\n')
                    fout.write('Queue: ' + Queue + '\n')
                    fout.write('...\n')


def make_qstatq_yaml(orig_file, yaml_file):
    """
    read QSTATQ_ORIG_FILE sequentially and put useful data in respective yaml file
    """
    if not os.path.getsize(orig_file) > 0:
        print 'Your ' + orig_file + ' file is empty! Please check your directory. Exiting ...'
        # os.chdir(HOMEPATH + 'qt')  # TODO: check if this works on restarting the qtop script
        sys.exit(0)

    queue_search = '^([a-zA-Z0-9_.-]+)\s+(--|[0-9]+[mgtkp]b[a-z]*)\s+(--|\d+:\d+:?\d*)\s+(--|\d+:\d+:\d+)\s+(--)\s+(\d+)\s+(\d+)\s+(--|\d+)\s+([DE] R)'
    run_qd_search = '^\s*(\d+)\s+(\d+)'
    with open(yaml_file, 'a') as fout, open(orig_file, 'r') as fin:
        for line in fin:
            line.strip()
            # searches for something like: biomed             --      --    72:00:00   --   31   0 --   E R
            if re.search(queue_search, line) is not None:
                m = re.search(queue_search, line)
                _, queue_name, Mem, cpu_time, wall_time, node, run, queued, lm, state = m.group(0), m.group(1), m.group(2), \
                                                                                     m.group(3), m.group(4), m.group(5), m.group(6), m.group(7), m.group(8), m.group(9)
                fout.write('- queue_name: ' + queue_name + '\n')
                fout.write('  Running: ' + run + '\n')
                fout.write('  Queued: ' + queued + '\n')
                fout.write('  Lm: ' + lm + '\n')
                fout.write('  State: ' + state + '\n')
                fout.write('\n')
            elif re.search(run_qd_search, line) is not None:
                n = re.search(run_qd_search, line)
                total_runs, total_queues = n.group(1), n.group(2)
        fout.write('---\n')
        fout.write('Total Running: ' + str(total_runs) + '\n')
        fout.write('Total Queued: ' + str(total_queues) + '\n')