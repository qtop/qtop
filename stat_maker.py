__author__ = 'sfranky'

import re
# import yaml
import yaml_parser as yaml
try:
    import ujson as json
except ImportError:
    import json
from xml.etree import ElementTree as etree
import os
import sys
from common_module import *

MAX_CORE_ALLOWED = 150000
try:
    from yaml import CLoader as Loader, CDumper as Dumper
except ImportError:
    try:
        from yaml import Loader, Dumper
    except ImportError:
        pass


def check_empty_file(orig_file):
    if not os.path.getsize(orig_file) > 0:
        print 'Your ' + orig_file + ' file is empty! Please check your directory. Exiting ...'
        sys.exit(0)


class StatMaker:

    def __init__(self):
        self.l = list()

        self.stat_mapping = {
            # 'yaml': (yaml.dump_all, {'Dumper': Dumper, 'default_flow_style': False}, 'yaml'),
            'txtyaml': (self.stat_write_lines, {}, 'yaml'),
            'json': (json.dump, {}, 'json')
        }

        self.statq_mapping = {
            # 'yaml': (yaml.dump_all, {'Dumper': Dumper, 'default_flow_style': False}, 'yaml'),
            'txtyaml': (self.statq_write_lines, {}, 'yaml'),
            'json': (json.dump, {}, 'json')}

    def stat_write_lines(self, fout):
        for qstat_values in self.l:
            fout.write('---\n')
            fout.write('JobId: ' + qstat_values['JobId'] + '\n')
            fout.write('UnixAccount: ' + qstat_values['UnixAccount'] + '\n')
            fout.write('S: ' + qstat_values['S'] + '\n')  # job state
            fout.write('Queue: ' + qstat_values['Queue'] + '\n')
            fout.write('...\n')

    def statq_write_lines(self, fout):
        last_line = self.l.pop()
        for qstatq_values in self.l:
            fout.write('---\n')
            fout.write('queue_name: ' + qstatq_values['queue_name'] + '\n')
            fout.write('state: ' + qstatq_values['state'] + '\n')  # job state
            fout.write('lm: ' + qstatq_values['lm'] + '\n')
            fout.write('run: ' + '"' + qstatq_values['run'] + '"' + '\n')  # job state
            fout.write('queued: ' + '"' + qstatq_values['queued'] + '"' + '\n')
            fout.write('...\n')
        fout.write('---\n')
        fout.write('Total queued: ' + '"' + last_line['Total queued'] + '"' + '\n')
        fout.write('Total running: ' + '"' + last_line['Total running'] + '"' + '\n')
        fout.write('...\n')

    @staticmethod
    def dump_all(out_file, write_func_args):
        """
        dumps the content of qstat/qstat_q files in the selected write_method format
        """
        with open(out_file, 'w') as fout:
            write_func, kwargs, _ = write_func_args
            write_func(fout, **kwargs)


class QStatMaker(StatMaker):

    def __init__(self):
        StatMaker.__init__(self)
        self.user_q_search = r'^(?P<host_name>(?P<job_id>[0-9-]+)\.(?P<domain>[\w-]+))\s+' \
                             r'(?P<name>[\w%.=+/-]+)\s+' \
                             r'(?P<user>[A-Za-z0-9.]+)\s+' \
                             r'(?P<time>\d+:\d+:?\d*|0)\s+' \
                             r'(?P<state>[CWRQE])\s+' \
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

    def make_stat(self, orig_file, out_file, write_method):
        check_empty_file(orig_file)
        with open(orig_file, 'r') as fin:
            _ = fin.readline()  # header
            fin.readline()
            line = fin.readline()
            re_match_positions = ('job_id', 'user', 'state', 'queue_name')  # was: (1, 5, 7, 8), (1, 4, 5, 8)
            try:  # first qstat line determines which format qstat follows.
                re_search = self.user_q_search
                qstat_values = self.process_line(re_search, line, re_match_positions)
                self.l.append(qstat_values)
                # unused: _job_nr, _ce_name, _name, _time_use = m.group(2), m.group(3), m.group(4), m.group(6)
            except AttributeError:  # this means 'prior' exists in qstat, it's another format
                re_search = self.user_q_search_prior
                qstat_values = self.process_line(re_search, line, re_match_positions)
                self.l.append(qstat_values)
                # unused:  _prior, _name, _submit, _start_at, _queue_domain, _slots, _ja_taskID =
                # m.group(2), m.group(3), m.group(6), m.group(7), m.group(9), m.group(10), m.group(11)
            finally:  # hence the rest of the lines should follow either try's or except's same format
                for line in fin:
                    qstat_values = self.process_line(re_search, line, re_match_positions)
                    self.l.append(qstat_values)
        self.dump_all(out_file, self.stat_mapping[write_method])  # self.l,

    def make_statq(self, orig_file, out_file, write_method):
        """
        reads QSTATQ_ORIG_FN sequentially and puts useful data in respective yaml file
        Searches for lines in the following format:
        biomed             --      --    72:00:00   --   31   0 --   E R
        (except for the last line, which contains two sums and is parsed separately)
        """
        check_empty_file(orig_file)
        queue_search = r'^(?P<queue_name>[\w.-]+)\s+' \
                       r'(?:--|[0-9]+[mgtkp]b[a-z]*)\s+' \
                       r'(?:--|\d+:\d+:?\d*:?)\s+' \
                       r'(?:--|\d+:\d+:?\d+:?)\s+(--)\s+' \
                       r'(?P<run>\d+)\s+' \
                       r'(?P<queued>\d+)\s+' \
                       r'(?P<lm>--|\d+)\s+' \
                       r'(?P<state>[DE] R)'
        run_qd_search = '^\s*(?P<tot_run>\d+)\s+(?P<tot_queued>\d+)'  # this picks up the last line contents

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
                    queue_name, run, queued, lm, state = m.group('queue_name'), m.group('run'), m.group('queued'), \
                                                         m.group('lm'), m.group('state')
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
                    self.l.append(temp_dict)
            self.l.append({'Total running': total_running_jobs, 'Total queued': total_queued_jobs})
        self.dump_all(out_file, self.statq_mapping[write_method])

    @staticmethod
    def process_line(re_search, line, re_match_positions):
        qstat_values = dict()
        m = re.search(re_search, line.strip())
        try:
            job_id, user, job_state, queue = [m.group(x) for x in re_match_positions]
        except AttributeError:
            print line.strip()
            sys.exit(0)
        job_id = job_id.split('.')[0]
        for key, value in [('JobId', job_id), ('UnixAccount', user), ('S', job_state), ('Queue', queue)]:
            qstat_values[key] = value
        return qstat_values


class OarStatMaker(QStatMaker):
    def __init__(self):
        StatMaker.__init__(self)
        self.user_q_search = r'^(?P<job_id>[0-9]+)\s+' \
                             r'(?P<name>[0-9A-Za-z_.-]+)?\s+' \
                             r'(?P<user>[0-9A-Za-z_.-]+)\s+' \
                             r'(?:\d{4}-\d{2}-\d{2})\s+' \
                             r'(?:\d{2}:\d{2}:\d{2})\s+' \
                             r'(?P<job_state>[RWF])\s+' \
                             r'(?P<queue>default|besteffort)'

    def make_stat(self, orig_file, out_file, write_method):
        with open(orig_file, 'r') as fin:
            logging.debug('File state before OarStatMaker.make_stat: %(fin)s' % {"fin": fin})
            _ = fin.readline()  # header
            fin.readline()  # dashes
            re_match_positions = ('job_id', 'user', 'job_state', 'queue')
            re_search = self.user_q_search
            for line in fin:
                qstat_values = self.process_line(re_search, line, re_match_positions)
                self.l.append(qstat_values)

        logging.debug('File state after OarStatMaker.make_stat: %(fin)s' % {"fin": fin})
        self.dump_all(out_file, self.stat_mapping[write_method])


class SGEStatMaker(StatMaker):
    def __init__(self):
        StatMaker.__init__(self)

    def make_stat(self, orig_file, out_file, write_method):
        out_file = out_file.rsplit('/', 1)[1]
        try:
            tree = etree.parse(orig_file)
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
                    queue_name = queue_name_elem.text
                    break
            else:
                raise ValueError("No such queue name")

            self._extract_job_info(queue_elem, 'job_list', queue_name=queue_name)

        job_info_elem = root.find('./job_info')
        if not job_info_elem:
            logging.debug('No pending jobs found!')
        else:
            self._extract_job_info(job_info_elem, 'job_list', queue_name='Pending')
        prefix, suffix = out_file.split('.')
        prefix += '_'
        suffix = '.' + suffix
        SGEStatMaker.fd, SGEStatMaker.temp_filepath = get_new_temp_file(prefix=prefix, suffix=suffix)
        self.dump_all(SGEStatMaker.fd, self.stat_mapping[write_method])

    def _extract_job_info(self, elem, elem_text, queue_name):
        """
        inside elem, iterates over subelems named elem_text and extracts relevant job information
        """
        for subelem in elem.findall(elem_text):
            qstat_values = dict()
            qstat_values['JobId'] = subelem.find('./JB_job_number').text
            qstat_values['UnixAccount'] = subelem.find('./JB_owner').text
            qstat_values['S'] = subelem.find('./state').text
            qstat_values['Queue'] = queue_name
            self.l.append(qstat_values)
        if not self.l:
            logging.info('No jobs found in XML file!')


    @staticmethod
    def dump_all(fd, write_func_args):
        """
        dumps the content of qstat/qstat_q files in the selected write_method format
        fd here is already a file descriptor
        """
        # prefix, suffix  = out_file.split('.')
        # out_file = get_new_temp_file(prefix=prefix, suffix=suffix)
        out_file = os.fdopen(fd, 'w')
        logging.debug('File state: %s' % out_file)
        write_func, kwargs, _ = write_func_args
        write_func(out_file, **kwargs)
        out_file.close()
