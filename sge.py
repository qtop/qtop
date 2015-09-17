from collections import OrderedDict

__author__ = 'sfranky'
from lxml import etree

fn = '/home/sfranky/PycharmProjects/results/gef_sge1/qstat.F.xml.stdout'
tree = etree.parse(fn)
root = tree.getroot()


def extract_job_info(elem, elem_text):
    """
    inside elem, iterates over subelems named elem_text and extracts relevant job information
    """
    jobs = []
    for subelem in elem.iter(elem_text):
        job = dict()
        job['job_state']  = subelem.find('./state').text
        job['job_name'] = subelem.find('./JB_name').text
        job['job_owner'] = subelem.find('./JB_owner').text
        job['job_slots'] = subelem.find('./slots').text
        job['job_nr'] = subelem.find('./JB_job_number').text
        jobs.append(job)
        # print '\t' + job['job_state'], job['job_name'], job['job_owner'], job['job_slots'], job['job_nr']
    return jobs


worker_nodes = list()
for queue_elem in root.iter('Queue-List'):
    d = OrderedDict()
    queue_name = queue_elem.find('./resource[@name="qname"]').text
    d['domainname'] = host_name = queue_elem.find('./resource[@name="hostname"]').text
    slots_total = queue_elem.find('./slots_total').text
    d['np'] = queue_elem.find('./resource[@name="num_proc"]').text
    slots_used = queue_elem.find('./slots_used').text
    slots_resv = queue_elem.find('./slots_resv').text
    # print queue_name, host_name, slots_total, slots_used, slots_resv

    running_jobs = extract_job_info(queue_elem, 'job_list')
    d['core_job_map'] = [{'core': idx, 'job': job['job_nr']} for idx, job in enumerate(running_jobs)]
    worker_nodes.append(d)


job_info_elem = root.find('./job_info')
# print 'PENDING JOBS'
pending_jobs = extract_job_info(job_info_elem, 'job_list')




