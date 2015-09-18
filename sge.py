from collections import OrderedDict

__author__ = 'sfranky'
from lxml import etree

# fn = '/home/sfranky/PycharmProjects/results/gef_sge1/qstat.F.xml.stdout'
# tree = etree.parse(fn)
# root = tree.getroot()


# def extract_job_info(elem, elem_text, job_ids=None, usernames=None, job_states=None, jobs=None):
#     """
#     inside elem, iterates over subelems named elem_text and extracts relevant job information
#     """
#     if not all([job_ids, usernames, job_states]):
#         job_ids, usernames, job_states, jobs = [], [], [], []
#     for subelem in elem.iter(elem_text):
#         job = dict()
#         job_ids.append(subelem.find('./JB_job_number').text)
#         usernames.append(subelem.find('./JB_owner').text)
#         job_states.append(subelem.find('./state').text)
#         # job['job_name'] = subelem.find('./JB_name').text
#         job['job_slots'] = subelem.find('./slots').text
#         jobs.append(job['job_slots'])
#     return job_ids, usernames, job_states, jobs


def calc_everything(fn, write_method):
    tree = etree.parse(fn)
    root = tree.getroot()
    worker_nodes = list()
    node_names = set()
    for queue_elem in root.iter('Queue-List'):
        sge_values = dict()
        sge_values['domainname'] = queue_elem.find('./resource[@name="hostname"]').text.split('.', 1)[0]
        sge_values['np'] = queue_elem.find('./resource[@name="num_proc"]').text
        try:
            state = queue_elem.find('state').text
        except AttributeError:
            sge_values['state'] = '-'
        else:
            sge_values['state'] = state

        slots_used = queue_elem.find('./slots_used').text
        slots_resv = queue_elem.find('./slots_resv').text
        slots_total = queue_elem.find('./slots_total').text

        if sge_values['domainname'] not in node_names:
            job_ids, usernames, job_states = extract_job_info(queue_elem, 'job_list')
            sge_values['core_job_map'] = [{'core': idx, 'job': job_id} for idx, job_id in enumerate(job_ids)]
            sge_values['existing_busy_cores'] = len(sge_values['core_job_map'])
            node_names.update([sge_values['domainname']])
            worker_nodes.append(sge_values)
        else:
            for existing_d in worker_nodes:
                if existing_d['domainname'] == sge_values['domainname']:
                    job_ids, usernames, job_states = extract_job_info(queue_elem, 'job_list')
                    core_jobs = [{'core': idx, 'job': job_id} for idx, job_id in enumerate(job_ids, existing_d['existing_busy_cores'])]
                    existing_d['core_job_map'].extend(core_jobs)
                    existing_d['state'] = sge_values['state'] == '-' and existing_d['state'] or sge_values['state']
                    break

    # job_info_elem = root.find('./job_info')
    # add pending jobs
    # job_ids, usernames, job_states, jobs = extract_job_info(job_info_elem, 'job_list', job_ids, usernames, job_states, jobs)

    return worker_nodes


def get_worker_nodes(fn, write_method):
    worker_nodes = calc_everything(fn, write_method)
    return worker_nodes


# def get_stat(fn, write_method):
#     worker_nodes, job_ids, usernames, job_states, queue_names, jobs = calc_everything(fn, write_method)
#     return job_ids, usernames, job_states, queue_names

def extract_job_info(elem, elem_text):
    """
    inside elem, iterates over subelems named elem_text and extracts relevant job information
    """
    job_ids, usernames, job_states = [], [], []
    for subelem in elem.iter(elem_text):
        job_ids.append(subelem.find('./JB_job_number').text)
        usernames.append(subelem.find('./JB_owner').text)
        job_states.append(subelem.find('./state').text)
    return job_ids, usernames, job_states


def make_stat(fn, write_method):
    tree = etree.parse(fn)
    root = tree.getroot()
    job_ids, usernames, job_states, queue_names = [], [], [], []
    for queue_elem in root.iter('Queue-List'):
        queue_name = queue_elem.find('./resource[@name="qname"]').text
        job_ids, usernames, job_states = extract_job_info(queue_elem, 'job_list')
        queue_names = queue_name * len(job_ids)
