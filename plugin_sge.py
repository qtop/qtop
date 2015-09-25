__author__ = 'sfranky'
from xml.etree import ElementTree as etree


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

        # slots_used = queue_elem.find('./slots_used').text
        # slots_resv = queue_elem.find('./slots_resv').text
        # slots_total = queue_elem.find('./slots_total').text

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

    return worker_nodes


def get_worker_nodes(fn, write_method):
    worker_nodes = calc_everything(fn, write_method)
    return worker_nodes


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
        _queue_names = queue_name * len(job_ids)


def get_statq_from_xml(fn, write_method):
    tree = etree.parse(fn)
    root = tree.getroot()
    qstatq_list = []
    for queue_elem in root.iter('Queue-List'):
        queue_name = queue_elem.find('./resource[@name="qname"]').text
        FOUND = False
        for exist_d in qstatq_list:
            if queue_name == exist_d['queue_name']:
                exist_d['run'] += len(queue_elem.findall('./job_list[@state="running"]'))
                FOUND = True
                break
        if FOUND:
            continue

        d = dict()
        d['queue_name'] = queue_name
        try:
            d['state'] = queue_elem.find('./state').text
        except AttributeError:
            d['state'] = '?'
        except:
            raise
        d['run'] = len(queue_elem.findall('./job_list[@state="running"]'))
        d['lm'] = 0
        d['queued'] = 0
        qstatq_list.append(d)

    total_running_jobs = str(sum([d['run'] for d in qstatq_list]))
    for d in qstatq_list:
        d['run'] = str(d['run'])
        d['queued'] = str(d['queued'])
    total_queued_jobs = str(len(root.findall('.//job_list[@state="pending"]')))
    qstatq_list.append({'run': '0', 'queued': total_queued_jobs, 'queue_name': 'Pending', 'state': 'Q', 'lm': '0'})
    # TODO: check validity. 'state' shouldnt just be 'Q'!

    return total_running_jobs, total_queued_jobs, qstatq_list
