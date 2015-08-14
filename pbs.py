import re
import sys
import os
import yaml

MAX_CORE_ALLOWED = 150000


def make_pbsnodes_yaml(orig_file, yaml_file):
    """
    reads PBSNODES_ORIG_FILE sequentially and puts its information in a new yaml file
    """
    if not os.path.getsize(orig_file) > 0:
        print 'Bailing out... Not yet ready for Sun Grid Engine clusters'
        sys.exit(0)

    search_domain_name = '^\w+([.-]?\w+)*'
    with open(orig_file, 'r') as fin, open(yaml_file, 'a') as fout:
        for line in fin:
            line.strip()

            m = re.search(search_domain_name, line)
            if m:
                domain_name = m.group(0)
                fout.write('domainname: ' + domain_name + '\n')

            elif 'state = ' in line:
                nextchar = line.split()[2][0]
                state = "'-'" if nextchar == 'f' else nextchar
                fout.write('state: ' + state + '\n')

            elif 'np = ' in line or 'pcpus = ' in line:
                np = line.split()[2][0:]
                fout.write('np: ' + np + '\n')

            elif 'jobs = ' in line:
                ljobs = line.split('=')[1].split(',')
                lastcore = MAX_CORE_ALLOWED
                fout.write('core_job_map: \n')
                for job in ljobs:
                    core, job = job.strip().split('/')
                    if len(core) > len(job):
                        # that can't be the case, so we got it wrong (jobs format must be jobid/core instead of core/jobid)
                        core, job = job, core
                    job = job.strip().split('/')[0].split('.')[0]
                    if core == lastcore:
                        print 'There are concurrent jobs assigned to the same core!' + '\n' +\
                              ' This kind of Remapping is not implemented yet. Exiting..'
                        sys.exit(1)
                    fout.write('- core: ' + core + '\n')
                    fout.write('  job: ' + job + '\n')
                    lastcore = core

            elif 'gpus = ' in line:
                gpus = line.split(' = ')[1]
                fout.write('gpus: ' + gpus + '\n')

            elif line.startswith('\n'):
                fout.write('---\n')


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
                total_running, total_queued = n.group(1), n.group(2)
        fout.write('---\n')
        fout.write('Total Running: ' + str(total_running) + '\n')
        fout.write('Total Queued: ' + str(total_queued) + '\n')


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


def map_pbsnodes_to_wn_dicts(state_dict, pbs_nodes):
    for (pbs_node, (idx, cur_node_nr)) in zip(pbs_nodes, enumerate(state_dict['wn_list'])):
        state_dict['wn_dict'][cur_node_nr] = pbs_node
        state_dict['wn_dict_remapped'][idx] = pbs_node


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