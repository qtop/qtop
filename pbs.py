import re
import sys
import os
import yaml

MAX_CORE_ALLOWED = 150000
try:
    from yaml import CLoader as Loader, CDumper as Dumper
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
    # lastcore = MAX_CORE_ALLOWED  # why ?!!?
    lastcore = ''
    with open(orig_file, 'r') as fin, open(yaml_file, 'a') as fout:
        for line in fin:
            line = line.strip()

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
                # lastcore = MAX_CORE_ALLOWED # was here
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
    Job id                    Name             User            Time Use S Queue
    or
    job-ID  prior   name       user         state_dict submit/start at     queue                          slots ja-task-ID
    # searches for something like: 422561.cream01             STDIN            see062          48:50:12 R see
    This new version of the function takes 93ms to run, as opposed to 86.5ms of the older version. Go figure!!
    """
    check_empty_file(orig_file)

    user_queue_search = '^(([0-9-]+)\.([\w-]+))\s+([\w%.=+/-]+)\s+([A-Za-z0-9.]+)\s+(\d+:\d+:?\d*|0)\s+([CWRQE])\s+(\w+)'
    user_queue_search_prior = '\s{2}(\d+)\s+([0-9]\.[0-9]+)\s+([\w.-]+)\s+([\w.-]+)\s+([a-z])\s+(\d{2}/\d{2}/\d{' \
                              '2}|0)\s+(\d+:\d+:\d*|0)\s+(\w+@[\w.-]+)\s+(\d+)\s+(\w*)'

    fout = file(yaml_file, 'a')
    l = list()
    with open(orig_file, 'r') as fin:
        header = fin.readline()
        fin.readline()
        line = fin.readline()
        try:  # first qstat line determines which format qstat follows.
            temp_dict = dict()
            re_search = user_queue_search
            m = re.search(re_search, line)
            re_match_positions = (1, 5, 7, 8)
            job_id, user, job_state, queue = [m.group(x) for x in re_match_positions]
            # unused: _job_nr, _ce_name, _name, _time_use = m.group(2), m.group(3), m.group(4), m.group(6)
            job_id = job_id.split('.')[0]
            for key, value in [('JobId', job_id), ('UnixAccount', user), ('S', job_state), ('Queue', queue)]:
                temp_dict[key] = value
            l.append(temp_dict)
            # qstat_write_sequence(fout, job_id, user, job_state, queue)
        except AttributeError:  # this means 'prior' exists in qstat, it's another format
            temp_dict = dict()
            re_search = user_queue_search_prior
            m = re.search(re_search, line)
            re_match_positions = (1, 4, 5, 8)
            job_id, user, job_state, queue = [m.group(x) for x in re_match_positions]
            for key, value in [('JobId', job_id), ('UnixAccount', user), ('S', job_state), ('Queue', queue)]:
                temp_dict[key] = value
            l.append(temp_dict)
            # qstat_write_sequence(fout, job_id, user, job_state, queue)
            # unused:  _prior, _name, _submit, _start_at, _queue_domain, _slots, _ja_taskID = m.group(2), m.group(3), m.group(6), m.group(7), m.group(9), m.group(10), m.group(11)
        finally:  # hence the rest of the lines should follow either try's or except's same format
            for line in fin:
                temp_dict = dict()
                m = re.search(re_search, line.strip())
                job_id, user, job_state, queue = [m.group(x) for x in re_match_positions]
                job_id = job_id.split('.')[0]
                for key, value in [('JobId', job_id), ('UnixAccount', user), ('S', job_state), ('Queue', queue)]:
                    temp_dict[key] = value
                l.append(temp_dict)
                # qstat_write_sequence(fout, job_id, user, job_state, queue)
    yaml.dump_all(l, fout, Dumper=Dumper, default_flow_style=False)


def make_qstatq_yaml(orig_file, yaml_file):
    """
    reads QSTATQ_ORIG_FILE sequentially and put useful data in respective yaml file
    All lines are something like: searches for something like: biomed             --      --    72:00:00   --   31   0 --   E R
    except the last line which contains two sums
    """
    check_empty_file(orig_file)
    l = []
    queue_search = '^([\w.-]+)\s+(--|[0-9]+[mgtkp]b[a-z]*)\s+(--|\d+:\d+:?\d*)\s+(--|\d+:\d+:\d+)\s+(--)\s+(\d+)\s+(\d+)\s+(--|\d+)\s+([DE] R)'
    run_qd_search = '^\s*(\d+)\s+(\d+)'

    stream = file(yaml_file, 'w')
    with open(orig_file, 'r') as fin:
        fin.next()
        # server_name = fin.next().split(': ')[1].strip()
        fin.next()
        headers = fin.next().strip()  # this should later define the keys in temp_dict, should they be different
        fin.next()
        for line in fin:
            line = line.strip()
            m = re.search(queue_search, line)
            n = re.search(run_qd_search, line)
            temp_dict = {}
            try:
                queue_name, run, queued, lm, state = m.group(1), m.group(6), m.group(7), m.group(8), m.group(9)
            except AttributeError:
                try:
                    total_running, total_queued = n.group(1), n.group(2)
                except AttributeError:
                    continue
            else:
                for key, value in [('queue_name', queue_name), ('run', run), ('queued', queued), ('lm', lm), ('state', state)]:
                    temp_dict[key] = value
                l.append(temp_dict)
        l.append({'Total running': total_running, 'Total queued': total_queued})
    yaml.dump_all(l, stream, Dumper=Dumper, default_flow_style=False)


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
        for qstatq in qstatqs_total:
            qstatq_list.append(qstatq)
        total = qstatq_list.pop()
        total_running, total_queued = total['Total running'], total['Total queued']
    return total_running, total_queued, qstatq_list

