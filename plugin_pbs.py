try:
    import ujson as json
except ImportError:
    import json
from serialiser import *
from common_module import check_empty_file, options


class PBSBatchSystem(GenericBatchSystem):
    def __init__(self, scheduler_output_filenames, config):
        self.pbsnodes_file = scheduler_output_filenames.get('pbsnodes_file')
        self.qstat_file = scheduler_output_filenames.get('qstat_file')
        self.qstatq_file = scheduler_output_filenames.get('qstatq_file')

        self.config = config
        self.qstat_maker = QStatExtractor(self.config)

    def get_worker_nodes(self):
        try:
            check_empty_file(self.pbsnodes_file)
        except FileEmptyError:
            all_pbs_values = []
            return all_pbs_values

        raw_blocks = self._read_all_blocks(self.pbsnodes_file)
        all_pbs_values = []
        anonymize = self.qstat_maker.anonymize_func()
        for block in raw_blocks:
            pbs_values = dict()
            pbs_values['domainname'] = block['domainname'] if not options.ANONYMIZE else anonymize(block['domainname'], 'wns')

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
                pass
            else:
                pbs_values['core_job_map'] = []
                jobs = block['jobs'].split(',')
                for job, core in self._get_jobs_cores(jobs):
                    _d = dict()
                    _d['job'] = job
                    _d['core'] = core
                    pbs_values['core_job_map'].append(_d)
            finally:
                all_pbs_values.append(pbs_values)
        return all_pbs_values

    def get_jobs_info(self):
        qstats = self.qstat_maker.get_qstat(self.qstat_file)
        return GenericBatchSystem.get_jobs_info(self, qstats)

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
        and spits tuples of the format (job,core)
        """
        for core_job in jobs:
            core, job = core_job.strip().split('/')
            # core, job = job['core'], job['job']
            if len(core) > len(job):  # PBS vs torque?
                core, job = job, core
            job = job.strip().split('/')[0].split('.')[0]
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
