try:
    import ujson as json
except ImportError:
    import json
import logging
import re
from qtop_py.serialiser import StatExtractor, GenericBatchSystem
import qtop_py.fileutils as fileutils
import itertools


class PBSStatExtractor(StatExtractor):
    def __init__(self, config, options):
        StatExtractor.__init__(self, config, options)
        self.user_q_search = r'^(?P<host_name>(?P<job_id>[0-9\[\]-]+)\.(?P<domain>[\w-]+))\s+' \
                             r'(?P<name>[\w%.=+/{}-]+)\s+' \
                             r'(?P<user>[A-Za-z0-9.]+)\s+' \
                             r'(?P<time>\d+:\d+:?\d*|0)\s+' \
                             r'(?P<state>[BCEFHMQRSTUWX])\s+' \
                             r'(?P<queue_name>\w+)'

        self.user_q_search_prior = r'\s{0,2}' \
                                   r'(?P<job_id>\d+)\s+' \
                                   r'(?:[0-9]\.[0-9]+)\s+' \
                                   r'(?:[\w.-]+)\s+' \
                                   r'(?P<user>[\w.-]+)\s+' \
                                   r'(?P<state>[a-z])\s+' \
                                   r'(?:\d{2}/\d{2}/\d{2}|0)\s+' \
                                   r'(?:\d+:\d+:\d*|0)\s+' \
                                   r'(?P<queue_name>\w+@[\w.-]+)\s+' \
                                   r'(?:\d+)\s+' \
                                   r'(?:\w*)'

    def extract_qstat(self, orig_file):
        try:
            fileutils.check_empty_file(orig_file)
        except fileutils.FileEmptyError:
            logging.error('File %s seems to be empty.' % orig_file)
            all_qstat_values = []
        else:
            all_qstat_values = list()
            with open(orig_file, 'r') as fin:
                _ = fin.readline()  # header
                fin.readline()
                line = fin.readline()
                re_match_positions = ('job_id', 'user', 'state', 'queue_name')  # was: (1, 5, 7, 8), (1, 4, 5, 8)
                try:  # first qstat line determines which format qstat follows.
                    re_search = self.user_q_search
                    qstat_values = self._process_qstat_line(re_search, line, re_match_positions)
                    # unused: _job_nr, _ce_name, _name, _time_use = m.group(2), m.group(3), m.group(4), m.group(6)
                except AttributeError:  # this means 'prior' exists in qstat, it's another format
                    re_search = self.user_q_search_prior
                    qstat_values = self._process_qstat_line(re_search, line, re_match_positions)
                    # unused:  _prior, _name, _submit, _start_at, _queue_domain, _slots, _ja_taskID =
                    # m.group(2), m.group(3), m.group(6), m.group(7), m.group(9), m.group(10), m.group(11)
                finally:
                    all_qstat_values.append(qstat_values)

                # hence the rest of the lines should follow either try's or except's same format
                for line in fin:
                    qstat_values = self._process_qstat_line(re_search, line, re_match_positions)
                    all_qstat_values.append(qstat_values)

        return all_qstat_values

    def extract_qstatq(self, orig_file):
        """
        reads QSTATQ_ORIG_FN sequentially and returns useful data
        Searches for lines in the following format:
        biomed             --      --    72:00:00   --   31   0 --   E R
        (except for the last line, which contains two sums and is parsed separately)
        """
        try:
            fileutils.check_empty_file(orig_file)
        except fileutils.FileEmptyError:
            all_values = []
        else:
            anonymize = self.anonymize_func()
            queue_search = r'^(?P<queue_name>[\w.-]+)\s+' \
                           r'(?:--|[0-9]+[mgtkp]b[a-z]*)\s+' \
                           r'(?:--|\d+:\d+:?\d*:?)\s+' \
                           r'(?:--|\d+:\d+:?\d+:?)\s+(--)\s+' \
                           r'(?P<run>\d+)\s+' \
                           r'(?P<queued>\d+)\s+' \
                           r'(?P<lm>--|\d+)\s+' \
                           r'(?P<state>[DE] R)'
            run_qd_search = '^\s*(?P<tot_run>\d+)\s+(?P<tot_queued>\d+)'  # this picks up the last line contents

            all_values = list()
            with open(orig_file, 'r') as fin:
                fin.next()
                fin.next()
                # server_name = fin.next().split(': ')[1].strip()
                fin.next()
                fin.next().strip()  # the headers line should later define the keys in temp_dict, should they be different
                fin.next()
                for line in fin:
                    line = line.strip()
                    m = re.search(queue_search, line)
                    n = re.search(run_qd_search, line)
                    temp_dict = {}
                    try:
                        queue_name = m.group('queue_name') if not self.options.ANONYMIZE else anonymize(m.group('queue_name'), 'qs')
                        run, queued, lm, state = m.group('run'), m.group('queued'), m.group('lm'), m.group('state')
                    except AttributeError:
                        try:
                            total_running_jobs, total_queued_jobs = n.group('tot_run'), n.group('tot_queued')
                        except AttributeError:
                            continue
                    else:
                        for key, value in [('queue_name', queue_name),
                                           ('run', run),
                                           ('queued', queued),
                                           ('lm', lm),
                                           ('state', state)]:
                            temp_dict[key] = value
                        all_values.append(temp_dict)
                all_values.append({'Total_running': total_running_jobs, 'Total_queued': total_queued_jobs})
        finally:
            return all_values


class PBSBatchSystem(GenericBatchSystem):

    @staticmethod
    def get_mnemonic():
        return "pbs"

    def __init__(self, scheduler_output_filenames, config, options):
        self.pbsnodes_file = scheduler_output_filenames.get('pbsnodes_file')
        self.qstat_file = scheduler_output_filenames.get('qstat_file')
        self.qstatq_file = scheduler_output_filenames.get('qstatq_file')

        self.config = config
        self.options = options
        self.qstat_maker = PBSStatExtractor(self.config, self.options)

    def get_worker_nodes(self, job_ids, job_queues, options):
        try:
            fileutils.check_empty_file(self.pbsnodes_file)
        except fileutils.FileEmptyError:
            all_pbs_values = []
            return all_pbs_values

        raw_blocks = self._read_all_blocks(self.pbsnodes_file)
        all_pbs_values = []
        anonymize = self.qstat_maker.anonymize_func()
        for block in raw_blocks:
            pbs_values = dict()
            pbs_values['domainname'] = block['domainname'] if not self.options.ANONYMIZE else anonymize(block['domainname'], 'wns')

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
                pbs_values['core_job_map'] = dict()  # change of behaviour: all entries should contain the key even if no value
            else:
                # jobs = re.split(r'(?<=[A-Za-z0-9]),\s?', block['jobs'])
                jobs = re.findall(r'[0-9][0-9,-]*/[^,]+', block['jobs'])
                pbs_values['core_job_map'] = dict((core, job) for job, core in self._get_jobs_cores(jobs))
            finally:
                all_pbs_values.append(pbs_values)

        all_pbs_values = self.ensure_worker_nodes_have_qnames(all_pbs_values, job_ids, job_queues)
        return all_pbs_values

    def get_jobs_info(self):
        """
        reads qstat YAML/json file and populates four lists. Returns the lists
        ex read_qstat_yaml
        Common for PBS, OAR, SGE
        """
        job_ids, usernames, job_states, queue_names = [], [], [], []

        qstats = self.qstat_maker.extract_qstat(self.qstat_file)
        for qstat in qstats:
            job_ids.append(re.sub(r'\[\]$', '', str(qstat['JobId'])))
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

    def get_queues_info(self):
        """
        Parses the generated qstatq yaml/json file and extracts
        the information necessary for building the
        user accounts and pool mappings table.
        """
        qstatq_list = []
        qstatqs_total = self.qstat_maker.extract_qstatq(self.qstatq_file)

        for qstatq in qstatqs_total[:-1]:
            qstatq_list.append(qstatq)
        for _total in qstatqs_total[-1:]:  # this is at most one item
            total_running_jobs, total_queued_jobs = _total['Total_running'], _total['Total_queued']
            break
        else:
            total_running_jobs, total_queued_jobs = 0, 0

        return int(eval(str(total_running_jobs))), int(eval(str(total_queued_jobs))), qstatq_list

    @staticmethod
    def _get_jobs_cores(jobs):  # block['jobs']
        """
        Generator that takes str of this format
        '0/10102182.f-batch01.grid.sinica.edu.tw, 1/10102106.f-batch01.grid.sinica.edu.tw, 2/10102339.f-batch01.grid.sinica.edu.tw, 3/10104007.f-batch01.grid.sinica.edu.tw'
        and spits tuples of the format (0, 10102182)    (job,core)
        """
        for core_job in jobs:
            core, job = core_job.strip().split('/')
            if (',' in core) or ('-' in core):
                for (subcore, subjob) in PBSBatchSystem.get_corejob_from_range(core, job):
                    subjob = subjob.strip().split('/')[0].split('.')[0]
                    yield subjob, subcore
            else:
                if len(core) > len(job):  # PBS vs torque?
                    core, job = job, core
                job = job.strip().split('/')[0].split('.')[0]
                job = re.sub(r'\[\d*\]$', '', job)
                yield job, core

    def _read_all_blocks(self, orig_file):
        """
        reads pbsnodes txt file block by block
        """
        with open(orig_file, mode='r') as fin:
            result = []
            reading = True
            while reading:
                wn_block = self._read_block(fin)
                if wn_block:
                    result.append(wn_block)
                else:
                    reading = False
        return result

    @staticmethod
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
                try:
                    key, value = line.split(' = ')
                except ValueError:  # e.g. if line is 'jobs =' with no jobs
                    pass
                else:
                    block[key.strip()] = value.strip()
        return block

    @staticmethod
    def get_corejob_from_range(core_selections, job):
        _cores = list()
        subselections = core_selections.split(',')
        for subselection in subselections:
            if '-' in subselection:
                range_ = map(int, subselection.split('-'))
                range_[-1] += 1
                _cores.extend([map(str, range(*range_))])
            else:
                _cores.append([subselection])
        all_cores = list(itertools.chain.from_iterable(_cores))
        for core in all_cores:
            yield core, job
