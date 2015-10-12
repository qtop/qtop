__author__ = 'sfranky'
from xml.etree import ElementTree as etree
from common_module import logging
import os


def calc_everything(fn, write_method):
    logging.debug('Parsing tree of %s' % fn)
    with open(fn, 'rb') as fin:
        tree = etree.parse(fin)
        root = tree.getroot()
        worker_nodes = list()
        existing_node_names = set()
        # for queue_elem in root.iter('Queue-List'):  # 2.7-only
        for queue_elem in root.findall('queue_info/Queue-List'):
            worker_node = dict()
            # worker_node['domainname'] = queue_elem.find('./resource[@name="hostname"]').text.split('.', 1)[0]  # 2.7 only
            resources = queue_elem.findall('resource')
            # TODO: find a way to loop ONCE for both hostname and qname!!
            try:
                slots_used = int(queue_elem.find('./slots_used').text)
            except AttributeError:
                slots_used = 0
            count = 0
            # worker_node.setdefault('qname', [])
            for resource in resources:
                if resource.attrib.get('name') == 'hostname':
                    worker_node['domainname'] = resource.text
                    count += 1
                    if count == 2: break
                elif resource.attrib.get('name') == 'qname':
                    worker_node['qname'] = set(resource.text[0]) if slots_used else set()
                    count += 1
                    if count == 2: break
            else:
                raise ValueError("No such resource")

            # worker_node['np'] = queue_elem.find('./resource[@name="num_proc"]').text  # python 2.7 only
            resources = queue_elem.findall('resource')
            for resource in resources:
                if resource.attrib.get('name') == 'num_proc':
                    worker_node['np'] = resource.text
                    break
            else:
                # TODO: check this for bugs, maybe raise an exception in the future?
                worker_node['np'] = 0

            try:
                state = queue_elem.find('state').text
            except AttributeError:
                worker_node['state'] = '-'
            else:
                worker_node['state'] = state

            if worker_node['domainname'] not in existing_node_names:
                job_ids, usernames, job_states = extract_job_info(queue_elem, 'job_list')
                worker_node['core_job_map'] = [{'core': idx, 'job': job_id} for idx, job_id in enumerate(job_ids)]
                worker_node['existing_busy_cores'] = len(worker_node['core_job_map'])
                existing_node_names.update([worker_node['domainname']])
                worker_nodes.append(worker_node)
            else:
                for existing_wn in worker_nodes:
                    if worker_node['domainname'] != existing_wn['domainname']:
                        continue
                    job_ids, usernames, job_states = extract_job_info(queue_elem, 'job_list')
                    core_jobs = [{'core': idx, 'job': job_id} for idx, job_id in enumerate(job_ids, existing_wn[
                        'existing_busy_cores'])]
                    existing_wn['core_job_map'].extend(core_jobs)
                    # don't change the node state to free.
                    # Just keep the state reported in the last queue mentioning the node.
                    existing_wn['state'] = (worker_node['state'] == '-') and existing_wn['state'] or worker_node['state']
                    existing_wn['qname'].update(worker_node['qname'])
                    break
    logging.debug('Closing %s' % fn)
    logging.info('worker_nodes contains %s entries' % len(worker_nodes))
    return worker_nodes


def get_worker_nodes(fn, write_method):
    worker_nodes = calc_everything(fn, write_method)
    return worker_nodes


def extract_job_info(elem, elem_text):
    """
    inside elem, iterates over subelems named elem_text and extracts relevant job information
    """
    job_ids, usernames, job_states = [], [], []
    for subelem in elem.findall(elem_text):
        job_ids.append(subelem.find('./JB_job_number').text)
        usernames.append(subelem.find('./JB_owner').text)
        job_states.append(subelem.find('./state').text)
    return job_ids, usernames, job_states


# def make_stat(fn, write_method):
#
#     tree = etree.parse(fn)
#     root = tree.getroot()
#     job_ids, usernames, job_states, queue_names = [], [], [], []
#     # for queue_elem in root.iter('Queue-List'):  # python 2.7-only
#     for queue_elem in root.find('queue_info/Queue-List'):
#         # queue_name = queue_elem.find('./resource[@name="qname"]').text  # 2.7 only
#         queue_names = queue_elem.findall('resource')
#         for _queue_name in queue_names:
#             if _queue_name.attrib.get('name') == 'qname':
#                 queue_name = _queue_name.text
#                 break
#         else:
#             raise ValueError("No such resource")
#         job_ids, usernames, job_states = extract_job_info(queue_elem, 'job_list')
#         _queue_names = queue_name * len(job_ids)


def get_statq_from_xml(fn, write_method):
    logging.debug("Parsing tree of %s" % fn)
    with open(fn, mode='rb') as fin:
        try:
            tree = etree.parse(fin)
        except etree.ParseError:
            logging.critical("Something happened during the parsing of the XML file. Exiting...")
        except:
            logging.debug("XML file state %s" % fin)
            logging.debug("thinking...")
            import sys
            sys.exit(1)

        root = tree.getroot()
        qstatq_list = []
        # for queue_elem in root.iter('Queue-List'):  # python 2.7-only
        for queue_elem in root.findall('queue_info/Queue-List'):
            # queue_name = queue_elem.find('./resource[@name="qname"]').text  # python 2.7-only
            queue_names = queue_elem.findall('resource')
            for _queue_name in queue_names:
                if _queue_name.attrib.get('name') == 'qname':
                    queue_name = _queue_name.text
                    break
            else:
                raise ValueError("No such resource")
            FOUND = False
            for exist_d in qstatq_list:
                if queue_name == exist_d['queue_name']:
                    # exist_d['run'] += len(queue_elem.findall('./job_list[@state="running"]'))  # python 2.7 only
                    jobs = queue_elem.findall('job_list')
                    run_count = 0
                    for _run in jobs:
                        if _run.attrib.get('state') == 'running':
                            run_count += 1
                    exist_d['run'] += run_count
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
            # d['run'] = len(queue_elem.findall('./job_list[@state="running"]'))  # python 2.7 only
            job_lists = queue_elem.findall('job_list')
            run_count = 0
            for _run in job_lists:
                if _run.attrib.get('state') == 'running':
                    run_count += 1
            d['run'] = run_count
            d['lm'] = 0
            d['queued'] = 0
            qstatq_list.append(d)

        total_running_jobs = str(sum([d['run'] for d in qstatq_list]))
        logging.info('Total running jobs found: %s' % total_running_jobs)
        for d in qstatq_list:
            d['run'] = str(d['run'])
            d['queued'] = str(d['queued'])
        # total_queued_jobs = str(len(root.findall('.//job_list[@state="pending"]')))  # python 2.7 only
        total_queued_jobs_elems = root.findall('job_info/job_list')
        pending_count = 0
        for job in total_queued_jobs_elems:
            if job.attrib.get('state') == 'pending':
                pending_count += 1
        total_queued_jobs = str(pending_count)
        logging.info('Total queued jobs found: %s' % total_queued_jobs)
        qstatq_list.append({'run': '0', 'queued': total_queued_jobs, 'queue_name': 'Pending', 'state': 'Q', 'lm': '0'})
        logging.debug('qstatq_list contains %s elements' % len(qstatq_list))
        # TODO: check validity. 'state' shouldnt just be 'Q'!
    logging.debug("Closing %s" % fn)
    return total_running_jobs, total_queued_jobs, qstatq_list
