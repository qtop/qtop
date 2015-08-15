import re
import sys
import os
import yaml

MAX_CORE_ALLOWED = 150000
try:
    from yaml import CLoader as Loader
except ImportError:
    from yaml import Loader


def check_empty_file(orig_file):
    if not os.path.getsize(orig_file) > 0:
        print 'Your ' + orig_file + ' file is empty! Please check your directory. Exiting ...'
        try:
            os.chdir(HOMEPATH + 'qt')
        finally:
            sys.exit(0)


def make_pbsnodes_yaml(orig_file, yaml_file):
    """
    reads PBSNODES_ORIG_FILE sequentially and puts its information in a new yaml file
    """
    check_empty_file(orig_file)

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


def qstat_write_sequence(fout, job_id, user, job_state, queue):
    fout.write('---\n')
    fout.write('JobId: ' + job_id + '\n')
    fout.write('UnixAccount: ' + user + '\n')
    fout.write('S: ' + job_state + '\n')
    fout.write('Queue: ' + queue + '\n')
    fout.write('...\n')


def make_qstat_yaml(orig_file, yaml_file):
    """
    reads QSTAT_ORIG_FILE sequentially and put useful data in respective yaml file.
    Some qstat files are structured a bit differently (the ones containing 'prior')
    """
    check_empty_file(orig_file)

    with open(orig_file, 'r') as fin, open(yaml_file, 'a') as fout:
        first_line = fin.readline()
        if 'prior' not in first_line:
            user_queue_search = '^(([0-9-]+)\.([A-Za-z0-9-]+))\s+([A-Za-z0-9%_.=+/-]+)\s+([A-Za-z0-9.]+)\s+(\d+:\d+:?\d*|0)\s+([CWRQE])\s+(\w+)'
            for line in fin:
                line.strip()
                # searches for something like: 422561.cream01             STDIN            see062          48:50:12 R see
                m = re.search(user_queue_search, line)
                if not m:
                    continue
                job_id, user, job_state, queue = m.group(1), m.group(5), m.group(7), m.group(8)
                # unused: _job_nr, _ce_name, _name, _time_use = m.group(2), m.group(3), m.group(4), m.group(6)
                job_id = job_id.split('.')[0]
                qstat_write_sequence(fout, job_id, user, job_state, queue)

        elif 'prior' in first_line:
            # e.g. job-ID  prior   name       user         state_dict submit/start at     queue                          slots ja-task-ID
            user_queue_search = '\s{2}(\d+)\s+([0-9]\.[0-9]+)\s+([A-Za-z0-9_.-]+)\s+([A-Za-z0-9._-]+)\s+([a-z])\s+(\d{2}/\d{2}/\d{2}|0)\s+(\d+:\d+:\d*|0)\s+([A-Za-z0-9_]+@[A-Za-z0-9_.-]+)\s+(\d+)\s+(\w*)'
            for line in fin:
                line.strip()
                m = re.search(user_queue_search, line)
                if not m:
                    continue
                job_id, user, job_state, queue = m.group(1), m.group(4), m.group(5), m.group(8)
                # unused:  _prior, _name, _submit, _start_at, _queue_domain, _slots, _ja_taskID = m.group(2), m.group(3), m.group(6), m.group(7), m.group(9), m.group(10), m.group(11)
                qstat_write_sequence(fout, job_id, user, job_state, queue)


def make_qstatq_yaml(orig_file, yaml_file):
    """
    reads QSTATQ_ORIG_FILE sequentially and put useful data in respective yaml file
    All lines are something like: searches for something like: biomed             --      --    72:00:00   --   31   0 --   E R
    except the last line which contains two sums
    """
    check_empty_file(orig_file)

    queue_search = '^([a-zA-Z0-9_.-]+)\s+(--|[0-9]+[mgtkp]b[a-z]*)\s+(--|\d+:\d+:?\d*)\s+(--|\d+:\d+:\d+)\s+(--)\s+(\d+)\s+(\d+)\s+(--|\d+)\s+([DE] R)'
    run_qd_search = '^\s*(\d+)\s+(\d+)'
    with open(yaml_file, 'a') as fout, open(orig_file, 'r') as fin:
        for line in fin:
            line.strip()
            m = re.search(queue_search, line)
            n = re.search(run_qd_search, line)

            if m:
                # unused: _, _mem, _cpu_time, _wall_time, _node, = m.group(0), m.group(2), m.group(3), m.group(4), m.group(5)
                queue_name, run, queued, lm, state = m.group(1), m.group(6), m.group(7), m.group(8), m.group(9)
                fout.write('- queue_name: ' + '"' + queue_name + '"' + '\n')
                fout.write('  Running: ' + '"' + run + '"' + '\n')
                fout.write('  Queued: ' + '"' + queued + '"' + '\n')
                fout.write('  Lm: ' + '"' + lm + '"' + '\n')
                fout.write('  State: ' + '"' + state + '"' + '\n')
                fout.write('\n')
            elif n:
                total_running, total_queued = n.group(1), n.group(2)
        fout.write('---\n')
        fout.write('Total Running: ' + '"' + str(total_running) + '"' + '\n')
        fout.write('Total Queued: ' + '"' + str(total_queued) + '"' + '\n')


def make_qstatq_yaml(orig_file, yaml_file):
    """

    :param orig_file:
    :param yaml_file:
    :return:
    """


def read_pbsnodes_yaml_into_list(yaml_fn):
    """
    Parses the pbsnodes yaml file
    :param yaml_fn: str
    :return: list
    """
    pbs_nodes = []

    with open(yaml_fn) as fin:
        _nodes = yaml.load_all(fin, Loader=Loader)
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


def read_qstat_yaml(yaml_fn):
    """
    reads qstat YAML file and populates four lists. Returns the lists
    """
    job_ids, usernames, job_states, queue_names = [], [], [], []

    with open(yaml_fn) as finr:
        qstats = yaml.load_all(finr, Loader=Loader)
        for qstat in qstats:
            job_ids.append(str(qstat['JobId']))
            usernames.append(qstat['UnixAccount'])
            job_states.append(qstat['S'])
            queue_names.append(qstat['Queue'])

    return job_ids, usernames, job_states, queue_names


def read_qstatq_yaml(yaml_fn):
    """
    Reads the generated qstatq yaml file and extracts the information necessary for building the user accounts and pool
    mappings table.
    """
    qstatq_list = []
    with open(yaml_fn, 'r') as fin:
        qstatqs_total = yaml.load_all(fin, Loader=Loader)
        qstatq_list = qstatqs_total.next()
        total = qstatqs_total.next()
        total_running, total_queued = total['Total Running'], total['Total Queued']
    return total_running, total_queued, qstatq_list

