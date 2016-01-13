"""
Contains classes necessary to convert input sources to yaml/json format.
"""
try:
    import ujson as json
except ImportError:
    import json
from common_module import *


class StatMaker:
    """
    Converts to yaml/json some of the input files
    coming from PBS, OAR, SGE Batch systems
    """

    def __init__(self, config):
        self.config = config
        self.anonymize = anonymize_func()

    @staticmethod
    def stat_write_lines(values, fout):
        for qstat_values in values:
            fout.write('---\n')
            fout.write('JobId: ' + qstat_values['JobId'] + '\n')
            fout.write('UnixAccount: ' + qstat_values['UnixAccount'] + '\n')
            fout.write('S: ' + qstat_values['S'] + '\n')  # job state
            fout.write('Queue: ' + qstat_values['Queue'] + '\n')
            fout.write('...\n')

    @staticmethod
    def statq_write_lines(all_qstatq_values, fout):
        for qstatq_values in all_qstatq_values[:-1]:
            fout.write('---\n')
            fout.write('queue_name: ' + qstatq_values['queue_name'] + '\n')
            fout.write('state: ' + qstatq_values['state'] + '\n')  # job state
            fout.write('lm: ' + qstatq_values['lm'] + '\n')
            fout.write('run: ' + qstatq_values['run'] + '\n')  # job state
            fout.write('queued: ' + qstatq_values['queued'] + '\n')
            fout.write('...\n')
        try:
            last_line = all_qstatq_values[-1]
        except IndexError:  # all_qstatq_values is an empty list
            pass
        else:
            fout.write('---\n')
            fout.write('Total_queued: ' + '"' + last_line['Total_queued'] + '"' + '\n')
            fout.write('Total_running: ' + '"' + last_line['Total_running'] + '"' + '\n')
            fout.write('...\n')

    def dump_all(self, values, out_file, write_method):
        """
        dumps the content of qstat/qstat_q files in the selected write_method format
        """
        with open(out_file, 'w') as fout:
            if write_method == 'txtyaml':
                self.stat_write_lines(values, fout)
            elif write_method == 'json':
                json.dump(values, fout)


class QStatMaker(StatMaker):
    def __init__(self, config):
        StatMaker.__init__(self, config)
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

    def serialise_qstat(self, orig_file, out_file, write_method):
        try:
            check_empty_file(orig_file)
        except FileEmptyError:
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
                    qstat_values = self._process_line(re_search, line, re_match_positions)
                    all_qstat_values.append(qstat_values)
                    # unused: _job_nr, _ce_name, _name, _time_use = m.group(2), m.group(3), m.group(4), m.group(6)
                except AttributeError:  # this means 'prior' exists in qstat, it's another format
                    re_search = self.user_q_search_prior
                    qstat_values = self._process_line(re_search, line, re_match_positions)
                    all_qstat_values.append(qstat_values)
                    # unused:  _prior, _name, _submit, _start_at, _queue_domain, _slots, _ja_taskID =
                    # m.group(2), m.group(3), m.group(6), m.group(7), m.group(9), m.group(10), m.group(11)
                finally:  # hence the rest of the lines should follow either try's or except's same format
                    for line in fin:
                        qstat_values = self._process_line(re_search, line, re_match_positions)
                        all_qstat_values.append(qstat_values)
        finally:
            self.dump_all(all_qstat_values, out_file, write_method)

    def serialise_qstatq(self, orig_file, out_file, write_method):
        """
        reads QSTATQ_ORIG_FN sequentially and puts useful data in respective yaml file
        Searches for lines in the following format:
        biomed             --      --    72:00:00   --   31   0 --   E R
        (except for the last line, which contains two sums and is parsed separately)
        """
        try:
            check_empty_file(orig_file)
        except FileEmptyError:
            all_values = []
        else:
            anonymize = anonymize_func()
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
                        queue_name = m.group('queue_name') if not options.ANONYMIZE else anonymize(m.group('queue_name'), 'qs')
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
            self.dump_statq(all_values, out_file, write_method)

    def dump_statq(self, values, out_file, write_method):
        """
        dumps the content of qstat/qstat_q files in the selected write_method format
        """
        with open(out_file, 'w') as fout:
            if write_method == 'txtyaml':
                self.statq_write_lines(values, fout)
            elif write_method == 'json':
                json.dump(values, fout)

    def _process_line(self, re_search, line, re_match_positions):
        qstat_values = dict()
        m = re.search(re_search, line.strip())
        try:
            job_id, user, job_state, queue = [m.group(x) for x in re_match_positions]
        except AttributeError:
            print line.strip()
            sys.exit(0)
        job_id = job_id.split('.')[0]
        user = user if not options.ANONYMIZE else self.anonymize(user, 'users')
        for key, value in [('JobId', job_id), ('UnixAccount', user), ('S', job_state), ('Queue', queue)]:
            qstat_values[key] = value
        return qstat_values


class GenericBatchSystem(object):
    def __init__(self):
        pass

    def convert_inputs(self):
        raise NotImplementedError

    def get_queues_info(self):
        raise NotImplementedError

    def get_worker_nodes(self):
        raise NotImplementedError

    def get_jobs_info(self, fn, write_method=options.write_method):
        """
        reads qstat YAML/json file and populates four lists. Returns the lists
        ex read_qstat_yaml
        Common for PBS, OAR, SGE
        """
        job_ids, usernames, job_states, queue_names = [], [], [], []

        with open(fn) as fin:
            try:
                qstats = (write_method.endswith('yaml')) and yaml.load_all(fin) or json.load(fin)
            except StopIteration:
                # import wdb; wdb.set_trace()
                logging.warning('File %s is empty. (No jobs found or Error!)')
                qstats = []
            else:
                for qstat in qstats:
                    job_ids.append(str(qstat['JobId']))
                    usernames.append(qstat['UnixAccount'])
                    job_states.append(qstat['S'])
                    queue_names.append(qstat['Queue'])
        # os.remove(fn)  # that DELETES the file!! why did I do that?!!
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
