__author__ = 'sfranky'
import tarfile
import os
from xml.etree import ElementTree as etree
from common_module import logging, check_empty_file, StatMaker, get_new_temp_file, options, anonymize_func, report, add_to_sample
from constants import *


def _get_worker_nodes(fn, write_method=options.write_method):
    worker_nodes = _calc_everything(fn, write_method)
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


def _get_statq_from_xml(fn, write_method=options.write_method):
    logging.debug("Parsing tree of %s" % fn)
    check_empty_file(fn)
    anonymize = anonymize_func()
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
                    queue_name = _queue_name.text if not options.ANONYMIZE else anonymize(_queue_name.text, 'qs')
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


class SGEStatMaker(StatMaker):
    def __init__(self, config):
        StatMaker.__init__(self, config)

    def convert_qstat_to_yaml(self, orig_file, out_file, write_method):
        out_file = out_file.rsplit('/', 1)[1]
        try:
            tree = etree.parse(orig_file)
        except etree.ParseError:
            logging.critical("This is an XML parse error (??)")
            raise
        except IOError:
            raise
        except:
            print "File %(filename)s does not appear to contain a proper XML structure. Exiting.." % {"filename": orig_file}
            raise
        else:
            root = tree.getroot()
        # for queue_elem in root.iter('Queue-List'):  # 2.7 only
        for queue_elem in root.findall('queue_info/Queue-List'):
            # queue_name = queue_elem.find('./resource[@name="qname"]').text  # 2.7 only
            queue_name_elems = queue_elem.findall('resource')
            for queue_name_elem in queue_name_elems:
                if queue_name_elem.attrib.get('name') == 'qname':
                    # import wdb; wdb.set_trace()
                    queue_name_elem.text = queue_name_elem.text if not options.ANONYMIZE else self.anonymize(queue_name_elem.text, 'qs')
                    break
            else:
                raise ValueError("No such queue name")

            self._extract_job_info(queue_elem, 'job_list', queue_name=queue_name_elem.text)  # puts it into self.l

        job_info_elem = root.find('./job_info')
        if job_info_elem is None:
            logging.debug('No pending jobs found!')
        else:
            self._extract_job_info(job_info_elem, 'job_list', queue_name='Pending')  # puts it into self.l

        prefix, suffix = out_file.split('.')
        prefix += '_'
        suffix = '.' + suffix
        SGEStatMaker.fd, SGEStatMaker.temp_filepath = get_new_temp_file(prefix=prefix, suffix=suffix, config=self.config)

        if options.SAMPLE >= 1:
            tree.write(orig_file)  # TODO anonymize rest of the sensitive information within xml file
            # add_to_sample(orig_file, self.config['savepath'])

        self.dump_all(SGEStatMaker.fd, self.stat_mapping[write_method])

    def _extract_job_info(self, elem, elem_text, queue_name):
        """
        inside elem, iterates over subelems named elem_text and extracts relevant job information
        """
        for subelem in elem.findall(elem_text):
            qstat_values = dict()
            qstat_values['JobId'] = subelem.find('./JB_job_number').text
            qstat_values['UnixAccount'] = subelem.find('./JB_owner').text \
                if not options.ANONYMIZE else self.anonymize(subelem.find('./JB_owner').text, 'users')
            qstat_values['S'] = subelem.find('./state').text
            qstat_values['Queue'] = queue_name
            self.l.append(qstat_values)
        if not self.l:
            logging.info('No jobs found in XML file!')

    def dump_all(self, fd, write_func_args):
        """
        dumps the content of qstat/qstat_q files in the selected write_method format
        fd here is already a file descriptor
        """
        # prefix, suffix  = out_file.split('.')
        # out_file = get_new_temp_file(prefix=prefix, suffix=suffix)
        out_file = os.fdopen(fd, 'w')
        try:
            logging.debug('File state: %s' % out_file)
            write_func, kwargs, _ = write_func_args
            write_func(out_file, **kwargs)
        finally:
            out_file.close()
        # if options.SAMPLE >= 1:
        #     add_to_sample(SGEStatMaker.temp_filepath, self.config['savepath'])

    def __repr__(self):
        return 'SGEStatMaker Instance'


def _calc_everything(fn, write_method):
    logging.debug('Parsing tree of %s' % fn)
    anonymize = anonymize_func()
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
                    worker_node['domainname'] = resource.text if not options.ANONYMIZE else anonymize(resource.text, 'wns')
                    count += 1
                    if count == 2: break
                elif resource.attrib.get('name') == 'qname':
                    if not slots_used:
                        worker_node['qname'] = set()
                    else:
                        worker_node['qname'] = set(resource.text[0]) \
                            if not options.ANONYMIZE else set(anonymize(resource.text[0], 'qs'))
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
                    core_jobs = [{'core': idx, 'job': job_id}
                                 for idx, job_id in enumerate(job_ids, existing_wn['existing_busy_cores'])]
                    existing_wn['core_job_map'].extend(core_jobs)
                    # don't change the node state to free.
                    # Just keep the state reported in the last queue mentioning the node.
                    existing_wn['state'] = (worker_node['state'] == '-') and existing_wn['state'] or worker_node['state']
                    existing_wn['qname'].update(worker_node['qname'])
                    break
    logging.debug('Closing %s' % fn)
    logging.info('worker_nodes contains %s entries' % len(worker_nodes))
    return worker_nodes


