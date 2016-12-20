__author__ = 'sfranky'
try:
    import ujson as json
except ImportError:
    import json
import logging
import sys
from qtop_py.serialiser import StatExtractor, GenericBatchSystem
from xml.etree import ElementTree as etree
import qtop_py.fileutils as fileutils


class SGEStatExtractor(StatExtractor):
    def __init__(self, config, options, scheduler_output_filenames):
        StatExtractor.__init__(self, config, options)
        self.scheduler_output_filenames = scheduler_output_filenames

    def get_xml_tree(self, xml_file):
        with open(xml_file, mode='rb') as fin:
            try:
                tree = etree.parse(fin)
            except etree.ParseError:
                logging.critical("Something happened during the parsing of the XML file. Exiting...")
                raise
            except IOError:
                raise
            except:
                logging.debug("XML file state %s" % fin)
                logging.debug("thinking...")
                sys.exit(1)
            else:
                root = tree.getroot()
        return tree, root

    def extract_qstat(self, orig_file):
        all_values = list()
        self.orig_file = orig_file
        self.tree, self.root = self.get_xml_tree(orig_file)
        tree, root = self.tree, self.root

        for queue_elem in root.findall('queue_info/Queue-List'):
            queue_name_elems = queue_elem.findall('resource')
            queue_list_nametag = queue_elem.find('name')
            queue_list_nametag.text = self.anonymize_queue_list_nametag(queue_list_nametag)
            _q_name_elems = iter(queue_name_elems)
            for queue_name_elem in _q_name_elems:
                if queue_name_elem.attrib.get('name') == 'qname':
                    queue_name_elem.text = self.anonymize(queue_name_elem.text, 'qs')
                    next_q_name_elem = next(_q_name_elems)
                    # if queue_name_elem.attrib.get('name') == 'hostname': # assume next must be hostname?
                    next_q_name_elem.text = self.anonymize(next_q_name_elem.text, 'wns')
                    break
            else:
                raise ValueError("No such queue name")

            try:
                all_values = self._extract_job_info(all_values, queue_elem, 'job_list', queue_name=queue_name_elem.text)
            except ValueError:
                logging.warn('No jobs found in XML file!')

        # look for the remaining, pending jobs, found later in the xml file
        job_info_elem = root.find('./job_info')
        if job_info_elem is None:
            logging.debug('No pending jobs found!')
        else:
            try:
                all_values = self._extract_job_info(all_values, job_info_elem, 'job_list', queue_name='Pending')
            except ValueError:
                logging.warn('No jobs found in XML file!')

        return all_values

    def _extract_job_info(self, all_values, elem, elem_text, queue_name):
        """
        inside elem, iterates over subelems named elem_text and extracts relevant job information
        TODO: check difference between extract_job_info and _extract_job_info
        """
        for subelem in elem.findall(elem_text):
            owner = subelem.find('./JB_owner').text = self.anonymize(subelem.find('./JB_owner').text, 'users')
            job_num = subelem.find('./JB_job_number').text = self.anonymize(subelem.find('./JB_job_number').text, 'jobnums')
            subelem.find('./JB_name').text = self.anonymize(subelem.find('./JB_name').text, 'jobnames')
            subm_time = subelem.find('./JB_submission_time')
            if subm_time is not None:
                subm_time.text = self.anonymize(subelem.find('./JB_submission_time').text, 'jobtimes')
            queue_name = self.anonymize(queue_name, 'qs')
            qstat_values = dict()
            qstat_values['JobId'] = job_num
            qstat_values['UnixAccount'] = owner
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

    def __init__(self, scheduler_output_filenames, config, options):
        self.sge_file = scheduler_output_filenames.get('sge_file')
        self.config = config
        self.options = options
        self.sge_stat_maker = SGEStatExtractor(self.config, self.options, scheduler_output_filenames)
        if self.options.ANONYMIZE:
            self.anonymize = self.sge_stat_maker.anonymize_func()
        else:
            self.anonymize = self.sge_stat_maker.eponymize_func()

    def get_queues_info(self):
        logging.debug("Parsing tree of %s" % self.sge_file)
        fileutils.check_empty_file(self.sge_file)

        tree, root = self.sge_stat_maker.tree, self.sge_stat_maker.root

        qstatq_list = self._extract_queues('queue_info/Queue-List', root)

        total_running_jobs = sum([d['run'] for d in qstatq_list])
        logging.info('Total running jobs found: %s' % total_running_jobs)

        for d in qstatq_list:
            d['run'] = str(d['run'])
            d['queued'] = str(d['queued'])

        total_queued_jobs = self._get_total_queued_jobs('job_info/job_list', root)

        qstatq_list.append({'run': '0', 'queued': total_queued_jobs, 'queue_name': 'Pending', 'state': 'Q', 'lm': '0'})
        logging.debug('qstatq_list contains %s elements' % len(qstatq_list))
        # TODO: check validity. 'state' shouldnt just be 'Q'!
        logging.debug("Closing %s" % self.sge_file)

        return total_running_jobs, int(eval(str(total_queued_jobs))), qstatq_list

    def get_worker_nodes(self, job_ids, job_queues, options):
        logging.debug('Parsing tree of %s' % self.sge_file)

        tree, root = self.sge_stat_maker.tree, self.sge_stat_maker.root

        existing_wns = list()
        existing_node_names = set()

        for queue_elem in root.findall('queue_info/Queue-List'):
            worker_node = self._get_host_qname_np(queue_elem)
            worker_node['state'] = self._get_state(queue_elem)

            if worker_node['domainname'] not in existing_node_names:
                job_ids, _, _ = self._extract_job_info(queue_elem, 'job_list')
                worker_node['core_job_map'] = dict((idx, job_id) for idx, job_id in enumerate(job_ids))
                worker_node['existing_busy_cores'] = len(worker_node['core_job_map'])
                worker_node['np'] = max(int(worker_node['np']), len(worker_node['core_job_map']))

                existing_node_names.update([worker_node['domainname']])
                existing_wns.append(worker_node)
            else:
                for existing_wn in existing_wns:
                    if worker_node['domainname'] != existing_wn['domainname']:
                        continue

                    job_ids, _, _ = self._extract_job_info(queue_elem, 'job_list')
                    core_jobs = dict((idx, job_id) for idx, job_id in enumerate(job_ids, existing_wn['existing_busy_cores']))
                    existing_wn['core_job_map'].update(core_jobs)
                    existing_wn['existing_busy_cores'] = len(existing_wn['core_job_map'])
                    # don't change the node state to free.
                    # Just keep the state reported in the last queue mentioning the node.
                    existing_wn['state'] = (worker_node['state'] == '-') and existing_wn['state'] or worker_node['state']
                    existing_wn['qname'].update(worker_node['qname'])
                    existing_wn['np'] = max(int(existing_wn['np']), len(existing_wn['core_job_map']))
                    break

        logging.debug('Closing %s' % self.sge_file)
        logging.info('existing_wns contains %s entries' % len(existing_wns))
        for existing_wn in existing_wns:
            existing_wn['qname'] = list(existing_wn['qname'])

        # last to be reading the xml file, can now write back if anonymizing..
        if self.options.SAMPLE >= 1:
            anon_file = self.sge_stat_maker.orig_file + '%s' % ('_anon' if self.options.ANONYMIZE else '')
            self.sge_stat_maker.scheduler_output_filenames['sge_file'] = anon_file
            tree.write(anon_file)
        return existing_wns

    def get_jobs_info(self):
        job_ids, usernames, job_states, queue_names = [], [], [], []

        all_values = self.sge_stat_maker.extract_qstat(self.sge_file)
        # TODO: needs better glueing
        for qstat in all_values:
            job_id = str(qstat['JobId'])
            job_id = self.anonymize(job_id, 'jobnums')
            job_ids.append(job_id)
            unix_account = qstat['UnixAccount']
            unix_account = self.anonymize(unix_account, 'users')
            usernames.append(unix_account)
            job_states.append(qstat['S'])
            q_name = qstat['Queue']
            q_name = self.anonymize(q_name, 'qs')
            queue_names.append(q_name)

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
            state = subelem.get('state')
            if state != 'running':
                continue

            owner = subelem.find('./JB_owner').text = self.anonymize(subelem.find('./JB_owner').text, 'users')
            job_num = subelem.find('./JB_job_number').text = self.anonymize(subelem.find('./JB_job_number').text, 'jobnums')
            subelem.find('./JB_name').text = self.anonymize(subelem.find('./JB_name').text, 'jobnames')
            subelem.find('./JAT_start_time').text = self.anonymize(subelem.find('./JAT_start_time').text, 'jobtimes')
            job_ids.append(job_num)
            usernames.append(owner)
            job_states.append(subelem.find('./state').text)
        return job_ids, usernames, job_states

    def _get_host_qname_np(self, queue_elem):
        worker_node = dict()
        count = 0
        try:
            slots_used = int(queue_elem.find('./slots_used').text)
        except AttributeError:
            slots_used = 0

        resources = queue_elem.findall('resource')
        for resource in resources:
            if resource.attrib.get('name') == 'hostname':
                worker_node['domainname'] = self.anonymize(resource.text, 'wns')
                count += 1
            elif resource.attrib.get('name') == 'qname':
                qname = resource.text
                if not slots_used:
                    worker_node['qname'] = set()
                else:
                    # if slots are reportedly used, the queue will be displayed even if no actual running jobs exist
                    worker_node['qname'] = set([self.anonymize(qname, 'qs')])
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

    def _extract_queues(self, xpath, root):
        qstatq_list = []
        for queue_elem in root.findall(xpath):

            queue_names = queue_elem.findall('resource')
            for _queue_name in queue_names:
                if _queue_name.attrib.get('name') == 'qname':
                    queue_name = self.anonymize(_queue_name.text, 'qs')
                    break
            else:
                raise ValueError("No such resource")

            for exist_d in qstatq_list:
                if queue_name == exist_d['queue_name']:

                    jobs = queue_elem.findall('job_list')
                    run_count = 0
                    for _run in jobs:
                        if _run.attrib.get('state') == 'running':
                            run_count += 1
                    exist_d['run'] += run_count
                    break
            else:  # first instance of queue in the xml
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

        return qstatq_list

    def _get_total_queued_jobs(self, xpath, root):
        total_queued_jobs_elems = root.findall(xpath)
        pending_count = 0
        for job in total_queued_jobs_elems:
            if job.attrib.get('state') == 'pending':
                pending_count += 1
        total_queued_jobs = str(pending_count)
        logging.info('Total queued jobs found: %s' % total_queued_jobs)

        return total_queued_jobs
