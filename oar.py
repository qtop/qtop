__author__ = 'sfranky'
import yaml
from collections import OrderedDict


def calculate_oar_state(jobid_state_lot, nr_of_jobs, node_state_mapping):
    """
    If all resource ids within the node are either alive or dead or suspected, the respective label is given to the node.
    Otherwise, a mixed-state is reported
    """
    states = [job_state_tpl[1] for job_state_tpl in jobid_state_lot]
    alive = states.count('Alive')
    dead = states.count('Dead')
    suspected = states.count('Suspected')

    if bool(alive) + bool(dead) + bool(suspected) > 1:
        state = node_state_mapping['mixed']
        return state
    else:
        return node_state_mapping[states[0]]


def read_oarnodes_y(fn_y, write_method):
    if write_method == 'yaml':
        return read_oarnodes_y_yaml(fn_y)
    else:
        return read_oarnodes_y_textyaml(fn_y)


def read_oarnodes_yaml(fn_s, fn_y, write_method):
    nodes_resids = read_oarnodes_s_yaml(fn_s, write_method)
    # resids_jobs = read_oarnodes_y_yaml(fn_y)
    # resids_jobs = read_oarnodes_y_textyaml(fn_y)
    resids_jobs = read_oarnodes_y(fn_y, write_method)

    nodes_jobs = {}
    for node in nodes_resids:
        resids_state_lot = nodes_resids[node]
        for (resid, state) in resids_state_lot:
            nodes_jobs.setdefault(node, []).append((resids_jobs[resid], state))

    worker_nodes = list()
    node_state_mapping = {'Alive': '-', 'Dead': 'd', 'Suspected': 's', 'Mixed': '%'}
    for node in nodes_jobs:
        d = OrderedDict()
        d['domainname'] = node
        nr_of_jobs = len(nodes_jobs[node])
        d['np'] = nr_of_jobs
        d['core_job_map'] = [{'core': idx, 'job': job[0]} for idx, job in enumerate(nodes_jobs[node]) if job[0] is not None]
        if not d['core_job_map']:
            del d['core_job_map']
        d['state'] = calculate_oar_state(nodes_jobs[node], nr_of_jobs, node_state_mapping)
        worker_nodes.append(d)

    return worker_nodes


def read_oarnodes_s_yaml(fn_s, write_method):  # todo: fix write_method not being used
    with open(fn_s, mode='r') as fin:
        data = yaml.load(fin)
    nodes_resids = {node: resid_state.items() for node, resid_state in data.items()}
    return nodes_resids


def read_oarnodes_y_yaml(fn_y):
    with open('oarnodes_y', mode='r') as fin:
        data = yaml.load(fin)
    resids_jobs = {resid: info.get('jobs', None) for resid, info in data.items()}
    return resids_jobs


def read_oarnodes_y_textyaml(fn):
    oar_nodes = {}
    with open(fn, mode='r') as fin:
        fin.readline()  # '---'
        line = fin.readline().strip()  # first res_id
        while line:
            oar_node, line = read_oar_node_y_textyaml(fin, line)
            oar_nodes.update(oar_node)

        resids_jobs = {resid: info.get('jobs', None) for resid, info in oar_nodes.items()}
        return resids_jobs
        # return oar_nodes


def read_oar_node_y_textyaml(fin, line):
    _oarnode = dict()

    res_id = line.strip(': ')
    _oarnode[int(res_id)] = dict()

    line = fin.readline().strip()
    while line and not line[0].isdigit():
        key, value = line.strip().split(': ')
        _oarnode[int(res_id)][key] = value
        line = fin.readline().strip()

    return _oarnode, line