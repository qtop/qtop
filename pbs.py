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
        sys.exit(0)


def make_pbsnodes(orig_file, out_file, write_method):
    """
    reads PBSNODES_ORIG_FN sequentially and puts its information into a new yaml file
    """
    check_empty_file(orig_file)
    raw_blocks = _read_all_blocks(orig_file)
    all_pbs_values = []
    for block in raw_blocks:
        pbs_values = dict()
        pbs_values['domainname'] = block['domainname']

        nextchar = block['state'][0]
        state = (nextchar == 'f') and "-" or nextchar

        pbs_values['state'] = state
        try:
            pbs_values['np'] = block['np']
        except KeyError:
            pbs_values['np'] = block['pcpus']  # handle torque cases  # todo : to check

        if block.get('gpus') > 0:  # this should be rare.
            pbs_values['gpus'] = block['gpus']
        try:  # this should turn up more often, hence the try/except.
            _ = block['jobs']
        except KeyError:
            pass
        else:
            pbs_values['core_job_map'] = []
            jobs = block['jobs'].split(',')
            for job, core in get_jobs_cores(jobs):
                _d = dict()
                _d['job'] = job
                _d['core'] = core
                pbs_values['core_job_map'].append(_d)
        finally:
            all_pbs_values.append(pbs_values)
    pbs_dump_all(all_pbs_values, out_file, pbsnodes_mapping[write_method])


def pbsnodes_write_lines(l, fout):
    for _block in l:
        fout.write('---\n')
        fout.write('domainname: ' + _block['domainname'] + '\n')
        fout.write('state: ' + "'" + _block['state'] + "'" + '\n')
        fout.write('np: ' + _block['np'] + '\n')
        if _block.get('gpus') > 0:
            fout.write('gpus: ' + _block['gpus'] + '\n')
        try:  # this should turn up more often, hence the try/except.
            core_job_map = _block['core_job_map']
        except KeyError:
            pass
        else:
            _write_jobs_cores(core_job_map, fout)
        fout.write('...\n')


def _write_jobs_cores(job_cores, fout):
    fout.write('core_job_map: \n')
    # for job, core in get_jobs_cores(jobs):
    for job_core in job_cores:
        fout.write('- core: ' + job_core['core'] + '\n')
        fout.write('  job: ' + job_core['job'] + '\n')


def get_jobs_cores(jobs):  # block['jobs']
    """
    Generator that takes str of this format
    '0/10102182.f-batch01.grid.sinica.edu.tw, 1/10102106.f-batch01.grid.sinica.edu.tw, 2/10102339.f-batch01.grid.sinica.edu.tw, 3/10104007.f-batch01.grid.sinica.edu.tw'
    and spits tuples of the format (job,core)
    """
    # jobs = jobs_str.split(',')
    for core_job in jobs:
        core, job = core_job.strip().split('/')
        # core, job = job['core'], job['job']
        if len(core) > len(job):  # PBS vs torque?
            core, job = job, core
        job = job.strip().split('/')[0].split('.')[0]
        yield job, core


def _read_all_blocks(orig_file):
    """
    reads pbsnodes txt file block by block
    """
    with open(orig_file, mode='r') as fin:
        result = []
        reading = True
        while reading:
            wn_block = _read_block(fin)
            if wn_block:
                result.append(wn_block)
            else:
                reading = False
    return result


def _read_block(fin):
    domain_name = fin.readline().strip()
    if not domain_name:
        return None

    block = {'domainname': domain_name}
    reading = True
    while reading:
        line = fin.readline()
        if line == '\n':
            reading = False
        else:
            key, value = line.split(' = ')
            block[key.strip()] = value.strip()
    return block


# def qstat_write_lines(l, fout):
#     for qstat_values in l:
#         fout.write('---\n')
#         fout.write('JobId: ' + qstat_values['JobId'] + '\n')
#         fout.write('UnixAccount: ' + qstat_values['UnixAccount'] + '\n')
#         fout.write('S: ' + qstat_values['S'] + '\n')  # job state
#         fout.write('Queue: ' + qstat_values['Queue'] + '\n')
#         fout.write('...\n')


def pbs_dump_all(l, out_file, write_func_args):
    """
    dumps the content of qstat/qstat_q files in the selected write_method format
    """
    with open(out_file, 'w') as fout:
        write_func, kwargs, _ = write_func_args
        write_func(l, fout, **kwargs)


# def make_qstat(orig_file, out_file, write_method):
#     """
#     reads QSTAT_ORIG_FN sequentially and put useful data in respective yaml file.
#     Some qstat files are structured a bit differently (the ones containing 'prior')
#     Job id                    Name             User            Time Use S Queue
#     or
#     job-ID  prior   name       user         ??????? submit/start at     queue                          slots ja-task-ID
#     # searches for something like: 422561.cream01             STDIN            see062          48:50:12 R see
#     This new version of the function takes 93ms to run, as opposed to 86.5ms of the older version. Go figure!!
#     """
#     check_empty_file(orig_file)
#
#     user_queue_search = '^(([0-9-]+)\.([\w-]+))\s+([\w%.=+/-]+)\s+([A-Za-z0-9.]+)\s+(\d+:\d+:?\d*|0)\s+([CWRQE])\s+(\w+)'
#     user_queue_search_prior = '\s{2}(\d+)\s+([0-9]\.[0-9]+)\s+([\w.-]+)\s+([\w.-]+)\s+([a-z])\s+(\d{2}/\d{2}/\d{' \
#                               '2}|0)\s+(\d+:\d+:\d*|0)\s+(\w+@[\w.-]+)\s+(\d+)\s+(\w*)'
#
#     l = list()
#     with open(orig_file, 'r') as fin:
#         _ = fin.readline()  # header
#         fin.readline()
#         line = fin.readline()
#         try:  # first qstat line determines which format qstat follows.
#             re_match_positions = (1, 5, 7, 8)
#             re_search = user_queue_search
#             qstat_values = _process_line(re_search, line, re_match_positions)
#             l.append(qstat_values)
#             # unused: _job_nr, _ce_name, _name, _time_use = m.group(2), m.group(3), m.group(4), m.group(6)
#         except AttributeError:  # this means 'prior' exists in qstat, it's another format
#             re_match_positions = (1, 4, 5, 8)
#             re_search = user_queue_search_prior
#             qstat_values = _process_line(re_search, line, re_match_positions)
#             l.append(qstat_values)
#             # unused:  _prior, _name, _submit, _start_at, _queue_domain, _slots, _ja_taskID =
#             # m.group(2), m.group(3), m.group(6), m.group(7), m.group(9), m.group(10), m.group(11)
#         finally:  # hence the rest of the lines should follow either try's or except's same format
#             for line in fin:
#                 qstat_values = _process_line(re_search, line, re_match_positions)
#                 l.append(qstat_values)
#     pbs_dump_all(l, out_file, qstat_mapping[write_method])


def _process_line(re_search, line, re_match_positions):
    qstat_values = dict()
    m = re.search(re_search, line.strip())
    job_id, user, job_state, queue = [m.group(x) for x in re_match_positions]
    job_id = job_id.split('.')[0]
    for key, value in [('JobId', job_id), ('UnixAccount', user), ('S', job_state), ('Queue', queue)]:
        qstat_values[key] = value
    return qstat_values


# def make_qstatq(orig_file, out_file, write_method):
#     """
#     reads QSTATQ_ORIG_FN sequentially and put useful data in respective yaml file
#     All lines are something like: searches for something like:
#     biomed             --      --    72:00:00   --   31   0 --   E R
#     except the last line which contains two sums
#     """
#     check_empty_file(orig_file)
#     l = []
#     queue_search = '^([\w.-]+)\s+(--|[0-9]+[mgtkp]b[a-z]*)\s+(--|\d+:\d+:?\d*)\s+(--|\d+:\d+:\d+)\s+(--)\s+(\d+)\s+(\d+)\s+(--|\d+)\s+([DE] R)'
#     run_qd_search = '^\s*(\d+)\s+(\d+)'
#
#     with open(orig_file, 'r') as fin:
#         fin.next()
#         # server_name = fin.next().split(': ')[1].strip()
#         fin.next()
#         fin.next().strip()  # the headers line should later define the keys in temp_dict, should they be different
#         fin.next()
#         for line in fin:
#             line = line.strip()
#             m = re.search(queue_search, line)
#             n = re.search(run_qd_search, line)
#             temp_dict = {}
#             try:
#                 queue_name, run, queued, lm, state = m.group(1), m.group(6), m.group(7), m.group(8), m.group(9)
#             except AttributeError:
#                 try:
#                     total_running_jobs, total_queued_jobs = n.group(1), n.group(2)
#                 except AttributeError:
#                     continue
#             else:
#                 for key, value in [('queue_name', queue_name), ('run', run), ('queued', queued), ('lm', lm),
#                                    ('state', state)]:
#                     temp_dict[key] = value
#                 l.append(temp_dict)
#         l.append({'Total running': total_running_jobs, 'Total queued': total_queued_jobs})
#     pbs_dump_all(l, out_file, qstatq_mapping[write_method])


def read_pbsnodes_yaml(fn, write_method):
    """
    Parses the pbsnodes yaml file
    :param fn: str
    :return: list
    """
    pbs_nodes = []

    with open(fn) as fin:
        _nodes = (write_method.endswith('yaml')) and yaml.load_all(fin, Loader=Loader) or json.load(fin)
        for node in _nodes:
            pbs_nodes.append(node)
    # pbs_nodes.pop() if not pbs_nodes[-1] else None # until i figure out why the last node is None
    # this doesn't seem to be the case anymore, DONT KNOW WHY!!
    return pbs_nodes


def map_pbsnodes_to_wn_dicts(cluster_dict, pbs_nodes, options_remap, group_by_name=False):
    """
    """
    if group_by_name and options_remap:
        # roughly groups the nodes by name and then by number. Experimental!
        pbs_nodes.sort(key=lambda d: (len(d.values()[0].split('-')[0]), int(d.values()[0].split('-')[1])), reverse=False)

    for (pbs_node, (idx, cur_node_nr)) in zip(pbs_nodes, enumerate(cluster_dict['workernode_list'])):
        cluster_dict['workernode_dict'][cur_node_nr] = pbs_node
        cluster_dict['workernode_dict_remapped'][idx] = pbs_node


def read_qstat_yaml(fn, write_method):
    """
    reads qstat YAML file and populates four lists. Returns the lists
    """
    job_ids, usernames, job_states, queue_names = [], [], [], []

    with open(fn) as fin:
        qstats = (write_method.endswith('yaml')) and yaml.load_all(fin, Loader=Loader) or json.load(fin)
        for qstat in qstats:
            job_ids.append(str(qstat['JobId']))
            usernames.append(qstat['UnixAccount'])
            job_states.append(qstat['S'])
            queue_names.append(qstat['Queue'])

    return job_ids, usernames, job_states, queue_names


def read_qstatq_yaml(fn, write_method):
    """
    Reads the generated qstatq yaml file and extracts
    the information necessary for building the
    user accounts and pool mappings table.
    """
    qstatq_list = []
    with open(fn, 'r') as fin:
        qstatqs_total = (write_method.endswith('yaml')) and yaml.load_all(fin, Loader=Loader) or json.load(fin)
        for qstatq in qstatqs_total:
            qstatq_list.append(qstatq)
        total = qstatq_list.pop()
        total_running_jobs, total_queued_jobs = total['Total running'], total['Total queued']
    return total_running_jobs, total_queued_jobs, qstatq_list


# def qstatq_write_lines(l, fout):
#     last_line = l.pop()
#     for qstatq_values in l:
#         fout.write('---\n')
#         fout.write('queue_name: ' + qstatq_values['queue_name'] + '\n')
#         fout.write('state: ' + qstatq_values['state'] + '\n')  # job state
#         fout.write('lm: ' + qstatq_values['lm'] + '\n')
#         fout.write('run: ' + '"' + qstatq_values['run'] + '"' + '\n')  # job state
#         fout.write('queued: ' + '"' + qstatq_values['queued'] + '"' + '\n')
#         fout.write('...\n')
#     fout.write('---\n')
#     fout.write('Total queued: ' + '"' + last_line['Total queued'] + '"' + '\n')
#     fout.write('Total running: ' + '"' + last_line['Total running'] + '"' + '\n')
#     fout.write('...\n')


# qstat_mapping = {'yaml': (yaml.dump_all, {'Dumper': Dumper, 'default_flow_style': False}, 'yaml'),
#                  'txtyaml': (qstat_write_lines, {}, 'yaml'),
#                  'json': (json.dump, {}, 'json')}
#
# qstatq_mapping = {'yaml': (yaml.dump_all, {'Dumper': Dumper, 'default_flow_style': False}, 'yaml'),
#                   'txtyaml': (qstatq_write_lines, {}, 'yaml'),
#                   'json': (json.dump, {}, 'json')}
#
pbsnodes_mapping = {'yaml': (yaml.dump_all, {'Dumper': Dumper, 'default_flow_style': False}, 'yaml'),
                    'txtyaml': (pbsnodes_write_lines, {}, 'yaml'),
                    'json': (json.dump, {}, 'json')}

ext_mapping = {'yaml': 'yaml', 'txtyaml': 'yaml', 'json': 'json'}