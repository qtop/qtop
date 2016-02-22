__author__ = 'sfranky'
try:
    import ujson as json
except ImportError:
    import json
import sys
from serialiser import *
from xml.etree import ElementTree as etree
from common_module import logging, check_empty_file, options
from constants import *


class SGEStatExtractor(StatExtractor):
    def __init__(self, config):
        StatExtractor.__init__(self, config)

    def extract_qstat(self, orig_file):
        all_values = list()
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

        for queue_elem in root.findall('queue_info/Queue-List'):
            queue_name_elems = queue_elem.findall('resource')
            for queue_name_elem in queue_name_elems:
                if queue_name_elem.attrib.get('name') == 'qname':
                    queue_name_elem.text = queue_name_elem.text if not options.ANONYMIZE else self.anonymize(queue_name_elem.text, 'qs')
                    break
            else:
                raise ValueError("No such queue name")

            try:
                all_values = self._extract_job_info(all_values, queue_elem, 'job_list', queue_name=queue_name_elem.text)
            except ValueError:
                logging.info('No jobs found in XML file!')

        job_info_elem = root.find('./job_info')
        if job_info_elem is None:
            logging.debug('No pending jobs found!')
        else:
            try:
                all_values = self._extract_job_info(all_values, job_info_elem, 'job_list', queue_name='Pending')
            except ValueError:
                logging.info('No jobs found in XML file!')

        if options.SAMPLE >= 1:
            tree.write(orig_file)  # TODO anonymize rest of the sensitive information within xml file
        return all_values

    def _extract_job_info(self, all_values, elem, elem_text, queue_name):
        """
        inside elem, iterates over subelems named elem_text and extracts relevant job information
        TODO: check difference between extract_job_info and _extract_job_info
        """
        for subelem in elem.findall(elem_text):
            qstat_values = dict()
            qstat_values['JobId'] = subelem.find('./JB_job_number').text
            qstat_values['UnixAccount'] = subelem.find('./JB_owner').text \
                if not options.ANONYMIZE else self.anonymize(subelem.find('./JB_owner').text, 'users')
            qstat_values['S'] = subelem.find('./state').text
            qstat_values['Queue'] = queue_name
            all_values.append(qstat_values)
        if not all_values:
            raise ValueError('No jobs found in XML file!')

        return all_values


class SGEBatchSystem(GenericBatchSystem):

    @staticmethod
    def get_mnemonic():
        return "sge"

    def __init__(self, scheduler_output_filenames, config):
        self.sge_file_stat = scheduler_output_filenames.get('sge_file_stat')
        self.config = config
        self.sge_stat_maker = SGEStatExtractor(self.config)

    def get_queues_info(self):
        logging.debug("Parsing tree of %s" % self.sge_file_stat)
        check_empty_file(self.sge_file_stat)
        anonymize = self.sge_stat_maker.anonymize_func()

        tree = self._get_xml_tree(self.sge_file_stat)
        root = tree.getroot()

        qstatq_list = []
        for queue_elem in root.findall('queue_info/Queue-List'):

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

            job_lists = queue_elem.findall('job_list')
            run_count = 0
            for _run in job_lists:
                if _run.attrib.get('state') == 'running':
                    run_count += 1
            d['run'] = run_count
            d['lm'] = 0
            d['queued'] = 0
            qstatq_list.append(d)

        total_running_jobs = sum([d['run'] for d in qstatq_list])

        logging.info('Total running jobs found: %s' % total_running_jobs)
        for d in qstatq_list:
            d['run'] = str(d['run'])
            d['queued'] = str(d['queued'])

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
        logging.debug("Closing %s" % self.sge_file_stat)

        return total_running_jobs, int(eval(str(total_queued_jobs))), qstatq_list

    def get_worker_nodes(self):
        logging.debug('Parsing tree of %s' % self.sge_file_stat)
        anonymize = self.sge_stat_maker.anonymize_func()

        with open(self.sge_file_stat, 'rb') as fin:
            tree = etree.parse(fin)

        root = tree.getroot()
        worker_nodes = list()
        existing_node_names = set()

        for queue_elem in root.findall('queue_info/Queue-List'):
            worker_node = self._get_host_qname_np(queue_elem, anonymize)
            worker_node['state'] = self._get_state(queue_elem)

            if worker_node['domainname'] not in existing_node_names:
                job_ids, _, _ = self._extract_job_info(queue_elem, 'job_list')
                worker_node['core_job_map'] = dict((idx, job_id) for idx, job_id in enumerate(job_ids))
                worker_node['existing_busy_cores'] = len(worker_node['core_job_map'])
                worker_node['np'] = max(int(worker_node['np']), len(worker_node['core_job_map']))

                existing_node_names.update([worker_node['domainname']])
                worker_nodes.append(worker_node)
            else:
                for existing_wn in worker_nodes:
                    if worker_node['domainname'] != existing_wn['domainname']:
                        continue

                    job_ids, _, _ = self._extract_job_info(queue_elem, 'job_list')
                    core_jobs = dict((idx, job_id) for idx, job_id in enumerate(job_ids, existing_wn['existing_busy_cores']))
                    existing_wn['core_job_map'].update(core_jobs)
                    # don't change the node state to free.
                    # Just keep the state reported in the last queue mentioning the node.
                    existing_wn['state'] = (worker_node['state'] == '-') and existing_wn['state'] or worker_node['state']
                    existing_wn['qname'].update(worker_node['qname'])
                    existing_wn['np'] = max(int(existing_wn['np']), len(existing_wn['core_job_map']))
                    break

        logging.debug('Closing %s' % self.sge_file_stat)
        logging.info('worker_nodes contains %s entries' % len(worker_nodes))
        for worker_node in worker_nodes:
            worker_node['qname'] = list(worker_node['qname'])
        return worker_nodes

    def get_jobs_info(self):
        job_ids, usernames, job_states, queue_names = [], [], [], []

        all_values = self.sge_stat_maker.extract_qstat(self.sge_file_stat)
        # TODO: needs better glueing
        for qstat in all_values:
            job_ids.append(str(qstat['JobId']))
            usernames.append(qstat['UnixAccount'])
            job_states.append(qstat['S'])
            queue_names.append(qstat['Queue'])

        logging.debug('job_ids, usernames, job_states, queue_names lengths: '
                      '%(job_ids)s, %(usernames)s, %(job_states)s, %(queue_names)s'
                      % {
                          "job_ids": len(job_ids),
                          "usernames": len(usernames),
                          "job_states": len(job_states),
                          "queue_names": len(queue_names)
                      }
                      )
        return job_ids, usernames, job_states, queue_names

    def _extract_job_info(self, elem, elem_text):
        """
        inside elem, iterates over subelems named elem_text and extracts relevant job information
        TODO: check difference between extract_job_info and _extract_job_info
        """
        job_ids, usernames, job_states = [], [], []
        for subelem in elem.findall(elem_text):
            job_ids.append(subelem.find('./JB_job_number').text)
            usernames.append(subelem.find('./JB_owner').text)
            job_states.append(subelem.find('./state').text)
        return job_ids, usernames, job_states

    def _get_xml_tree(self, xml_file):
        with open(xml_file, mode='rb') as fin:
            try:
                tree = etree.parse(fin)
            except etree.ParseError:
                logging.critical("Something happened during the parsing of the XML file. Exiting...")
            except:
                logging.debug("XML file state %s" % fin)
                logging.debug("thinking...")
                sys.exit(1)
        return tree

    def _get_host_qname_np(self, queue_elem, anonymize):
        worker_node = dict()
        count = 0
        try:
            slots_used = int(queue_elem.find('./slots_used').text)
        except AttributeError:
            slots_used = 0

        resources = queue_elem.findall('resource')
        for resource in resources:
            if resource.attrib.get('name') == 'hostname':
                worker_node['domainname'] = resource.text if not options.ANONYMIZE else anonymize(resource.text, 'wns')
                count += 1
            elif resource.attrib.get('name') == 'qname':
                qname = resource.text[0]
                if not slots_used:
                    worker_node['qname'] = set()
                else:
                    # if slots are reportedly used, the queue will be displayed even if no actual running jobs exist
                    worker_node['qname'] = set(qname) if not options.ANONYMIZE else set(anonymize(qname, 'qs'))
                count += 1
            elif resource.attrib.get('name') == 'num_proc':
                worker_node['np'] = resource.text
                count += 1
            if count == 3:
                break
        else:
            # out of the 3, np information is the most likely to be missing from a node
            worker_node['np'] = 0

        return worker_node

    def _get_state(self, queue_elem):
        try:
            _state = queue_elem.find('state').text
        except AttributeError:
            _state = '-'
        finally:
            return _state
