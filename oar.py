__author__ = 'sfranky'
import yaml
from collections import OrderedDict


def read_oarnodes_yaml(fn_s, fn_y, write_method):
    nodes_resids = read_oarnodes_s(fn_s, write_method)
    resids_jobs = read_oarnodes_y(fn_y, write_method)

    nodes_jobs = {}
    for node in nodes_resids:
        resids = nodes_resids[node]
        for resid in resids:
            nodes_jobs.setdefault(node, []).append(resids_jobs[resid])

    worker_nodes = list()
    for node in nodes_jobs:
        d = OrderedDict()
        d['domainname'] = node
        nr_of_jobs = len(nodes_jobs[node])
        d['np'] = nr_of_jobs
        d['core_job_map'] = [{'core':idx, 'job':job} for idx,job in enumerate(nodes_jobs[node]) if job is not None]
        if not d['core_job_map']:
            del d['core_job_map']
        d['state'] = '-'
        worker_nodes.append(d)

    return worker_nodes


def read_oarnodes_s(fn_s, write_method):
    with open(fn_s, mode='r') as fin:
        data = yaml.load(fin)
    nodes_resids = {node: resid_state.keys() for node, resid_state in data.items()}
    return nodes_resids


def read_oarnodes_y(fn_y, write_method):
    with open('oarnodes_y', mode='r') as fin:
        data = yaml.load(fin)
    resids_jobs = {resid: info.get('jobs', None) for resid, info in data.items()}
    return resids_jobs