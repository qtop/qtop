import re
import sys
import os
import yaml
import ujson as json

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


def make_pbsnodes(orig_file, yaml_file):
    """
    reads PBSNODES_ORIG_FILE sequentially and puts its information into a new yaml file
    """
    check_empty_file(orig_file)
    blocks = read_all_blocks(orig_file)
    with open(yaml_file, 'w') as fout:
        # lastcore = ''
        for block in blocks:
            fout.write('domainname: ' + block['Domain name'] + '\n')

            nextchar = block['state'][0]
            state = "'-'" if nextchar == 'f' else nextchar
            fout.write('state: ' + state + '\n')

            fout.write('np: ' + block['np'] + '\n')
            if block.get('gpus') > 0:  # this should be rare.
                fout.write('gpus: ' + block['gpus'] + '\n')
            try:  # this should turn up more often, hence the try/except.
                _ = block['jobs']
            except KeyError:
                pass
            else:
                write_jobs_cores(block['jobs'], fout)
            fout.write('---\n')


def write_jobs_cores(jobs, fout):
    fout.write('core_job_map: \n')
    for job, core in jobs_cores(jobs):
        fout.write('- core: ' + core + '\n')
        fout.write('  job: ' + job + '\n')


def jobs_cores(jobs):  # block['jobs']
    jobs_list = jobs.split(',')
    for job in jobs_list:
        core, job = job.strip().split('/')
        if len(core) > len(job):  # we must've got this wrong (jobs format must be jobid/core, not core/jobid)
            core, job = job, core
        job = job.strip().split('/')[0].split('.')[0]
        yield job, core


def read_all_blocks(orig_file):
    """
    reads pbsnodes txt file block by block
    """
    with open(orig_file, mode='r') as fin:
        result = []
        reading = True
        while reading:
            wn_block = read_block(fin)
            if wn_block:
                result.append(wn_block)
            else:
                reading = False
    return result


def read_block(fin):
    line = fin.readline()
    if not line:
        return None

    domain_name = line.strip()
    block = {'Domain name': domain_name}
    reading = True
    while reading:
        line = fin.readline()
        if line == '\n':
            reading = False
        else:
            key, value = line.split(' = ')
            block[key.strip()] = value.strip()
    return block


def qstat_write_lines(l, fout):
    for qstat_values in l:
        fout.write('---\n')
        fout.write('JobId: ' + qstat_values['JobId'] + '\n')
        fout.write('UnixAccount: ' + qstat_values['UnixAccount'] + '\n')
        fout.write('S: ' + qstat_values['S'] + '\n')  # job state
        fout.write('Queue: ' + qstat_values['Queue'] + '\n')
        fout.write('...\n')


def qstat_dump_all(l, fout, write_method, mapping):
    """
    dumps the content of qstat/qstat_q files in the selected write_method format
    """
    try:
        fun, kwargs = mapping[write_method]
        fun(l, fout, **kwargs)
    except:
        import pdb; pdb.set_trace()
        raise NotImplementedError


# def perform(func, *args, **kwargs):
#     func(*args, **kwargs)


def make_qstat(orig_file, yaml_file, write_method):
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
            re_match_positions = (1, 5, 7, 8)
            re_search = user_queue_search
            qstat_values = process_line(re_search, line, re_match_positions)
            l.append(qstat_values)
            # unused: _job_nr, _ce_name, _name, _time_use = m.group(2), m.group(3), m.group(4), m.group(6)
        except AttributeError:  # this means 'prior' exists in qstat, it's another format
            re_match_positions = (1, 4, 5, 8)
            re_search = user_queue_search_prior
            qstat_values = process_line(re_search, line, re_match_positions)
            l.append(qstat_values)
            # unused:  _prior, _name, _submit, _start_at, _queue_domain, _slots, _ja_taskID = m.group(2), m.group(3), m.group(6), m.group(7), m.group(9), m.group(10), m.group(11)
        finally:  # hence the rest of the lines should follow either try's or except's same format
            for line in fin:
                qstat_values = process_line(re_search, line, re_match_positions)
                l.append(qstat_values)
    qstat_dump_all(l, fout, write_method, _qstat_mapping)


def process_line(re_search, line, re_match_positions):
    qstat_values = dict()
    m = re.search(re_search, line.strip())
    job_id, user, job_state, queue = [m.group(x) for x in re_match_positions]
    job_id = job_id.split('.')[0]
    for key, value in [('JobId', job_id), ('UnixAccount', user), ('S', job_state), ('Queue', queue)]:
        qstat_values[key] = value
    return qstat_values


def make_qstatq(orig_file, yaml_file, write_method):
    """
    reads QSTATQ_ORIG_FILE sequentially and put useful data in respective yaml file
    All lines are something like: searches for something like: biomed             --      --    72:00:00   --   31   0 --   E R
    except the last line which contains two sums
    """
    check_empty_file(orig_file)
    l = []
    queue_search = '^([\w.-]+)\s+(--|[0-9]+[mgtkp]b[a-z]*)\s+(--|\d+:\d+:?\d*)\s+(--|\d+:\d+:\d+)\s+(--)\s+(\d+)\s+(\d+)\s+(--|\d+)\s+([DE] R)'
    run_qd_search = '^\s*(\d+)\s+(\d+)'

    fout = file(yaml_file, 'w')
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
    qstat_dump_all(l, fout, write_method, _qstatq_mapping)


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


def qstatq_write_lines():
    raise NotImplementedError


_qstat_mapping = {'yaml': (yaml.dump_all, {'Dumper': Dumper, 'default_flow_style': False}),
                  'txtyaml': (qstat_write_lines, {}),
                  'json': (json.dumps, {})}

_qstatq_mapping = {'yaml': (yaml.dump_all, {'Dumper': Dumper, 'default_flow_style': False}),
                   'txtyaml': (qstatq_write_lines, {}),
                   'json': (json.dumps, {})}
