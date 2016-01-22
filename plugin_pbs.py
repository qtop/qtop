import re
import sys
import os
try:
    import ujson as json
except ImportError:
    import json

import yaml_parser as yaml
from constants import *
from serialiser import *
from common_module import logging, check_empty_file, options, anonymize_func, add_to_sample
import common_module


class PBSBatchSystem(GenericBatchSystem):
    def __init__(self, in_out_filenames, config):
        self.pbsnodes_file = in_out_filenames.get('pbsnodes_file')
        self.pbsnodes_file_out = in_out_filenames.get('pbsnodes_file_out')
        self.qstat_file = in_out_filenames.get('qstat_file')
        self.qstat_file_out = in_out_filenames.get('qstat_file_out')
        self.qstatq_file = in_out_filenames.get('qstatq_file')
        self.qstatq_file_out = in_out_filenames.get('qstatq_file_out')

        self.config = config
        self.qstat_maker = QStatMaker(self.config)

    def convert_inputs(self):
        self._serialise_pbs_input(self.pbsnodes_file, self.pbsnodes_file_out)
        self._serialise_qstatq(self.qstatq_file, self.qstatq_file_out)
        self._serialise_qstat(self.qstat_file, self.qstat_file_out)

    def get_worker_nodes(self):
        return self._read_serialised_pbsnodes(self.pbsnodes_file_out)

    def get_jobs_info(self):
        return GenericBatchSystem.get_jobs_info(self, self.qstat_file_out)

    def get_queues_info(self):
        return self._read_serialised_qstatq(self.qstatq_file_out)

    def _serialise_pbs_input(self, orig_file, out_file, write_method=options.write_method):
        """
        reads PBSNODES_ORIG_FN sequentially and puts its information into a new yaml file
        """
        all_pbs_values = self._get_pbsnodes_values(orig_file, out_file)

        with open(out_file, 'w') as fout:
            if write_method == 'txtyaml':
                self._pbsnodes_write_lines(all_pbs_values, fout)
            elif write_method == 'json':
                json.dump(all_pbs_values, fout)

    def _serialise_qstatq(self, qstatq_file, qstatq_file_out, write_method=options.write_method):
        return self.qstat_maker.serialise_qstatq(qstatq_file, qstatq_file_out, write_method)  # TODO FIX ASAP

    def _serialise_qstat(self, qstat_file, qstat_file_out, write_method=options.write_method):
        return self.qstat_maker.serialise_qstat(qstat_file, qstat_file_out, write_method)  # TODO FIX ASAP

    @staticmethod
    def _read_serialised_pbsnodes(fn, write_method=options.write_method):
        """
        Parses the pbsnodes yaml/json file
        :param fn: str
        :return: list
        """
        pbs_nodes = []

        with open(fn) as fin:
            if write_method.endswith('yaml'):
                _nodes = yaml.load_all(fin)
            else:
                _nodes = json.load(fin)
            for node in _nodes:
                pbs_nodes.append(node)
        return pbs_nodes

    @staticmethod
    def _read_serialised_qstatq( fn, write_method=options.write_method):
        """
        Parses the generated qstatq yaml/json file and extracts
        the information necessary for building the
        user accounts and pool mappings table.
        """
        qstatq_list = []
        logging.debug("Opening %s" % fn)
        with open(fn, 'r') as fin:
            if write_method.endswith('yaml'):
                qstatqs_total = yaml.load_all(fin)
            else:
                qstatqs_total = json.load(fin)

        for qstatq in qstatqs_total[:-1]:
            qstatq_list.append(qstatq)
        for _total in qstatqs_total[-1:]:  # this is at most one item
            total_running_jobs, total_queued_jobs = _total['Total_running'], _total['Total_queued']
            break
        else:
            total_running_jobs, total_queued_jobs = 0, 0

        # logging.critical('total is: %s' % qstatqs_total[-1:])
        return int(eval(str(total_running_jobs))), int(eval(str(total_queued_jobs))), qstatq_list

    def _get_pbsnodes_values(self, orig_file, out_file):
        try:
            check_empty_file(orig_file)
        except FileEmptyError:
            all_pbs_values = []
            return all_pbs_values

        raw_blocks = self._read_all_blocks(orig_file)
        all_pbs_values = []
        anonymize = anonymize_func()
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

    def _pbsnodes_write_lines(self, l, fout):
        for _block in l:
            fout.write('---\n')
            fout.write('domainname: ' + _block['domainname'] + '\n')
            fout.write('state: ' + _block['state'] + '\n')
            fout.write('np: ' + _block['np'] + '\n')
            if _block.get('gpus') > 0:
                fout.write('gpus: ' + _block['gpus'] + '\n')
            try:  # this should turn up more often, hence the try/except.
                core_job_map = _block['core_job_map']
            except KeyError:
                pass
            else:
                self._write_jobs_cores(core_job_map, fout)
            fout.write('...\n')

    @staticmethod
    def _write_jobs_cores(job_cores, fout):
        fout.write('core_job_map: \n')
        for job_core in job_cores:
            fout.write('  - core: ' + job_core['core'] + '\n')
            fout.write('    job: ' + job_core['job'] + '\n')

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
