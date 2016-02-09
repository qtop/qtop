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
        self.anonymize = self.anonymize_func()

    def anonymize_func(self):
        """
        creates and returns an _anonymize_func object (closure)
        Anonymisation can be used by the user for providing feedback to the developers.
        The logs and the output should no longer contain sensitive information about the clusters ran by the user.
        """
        counters = {}
        stored_dict = {}
        for key in ['users', 'wns', 'qs']:
            counters[key] = count()

        maps = {
            'users': '_anon_user_',
            'wns': '_anon_wn_',
            'qs': '_anon_q_'
        }

        def _anonymize_func(s, a_type):
            """
            d4-p4-04 --> d_anon_wn_0
            d4-p4-05 --> d_anon_wn_1
            biomed017--> b_anon_user_0
            alice    --> a_anon_q_0
            """
            dup_counter = counters[a_type]

            s_type = maps[a_type]
            cnt = '0'
            new_name_parts = [s[0], s_type, cnt]
            if s not in stored_dict:
                cnt = str(dup_counter.next())
                new_name_parts.pop()
                new_name_parts.append(cnt)
            stored_dict.setdefault(s, (''.join(new_name_parts), s_type))
            return stored_dict[s][0]

        return _anonymize_func


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

    def get_qstat(self, orig_file):
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
            return all_qstat_values

    def serialise_qstatq(self, orig_file, out_file):
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
            return all_values

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

    def get_queues_info(self):
        raise NotImplementedError

    def get_worker_nodes(self):
        raise NotImplementedError

    def get_jobs_info(self, qstats):
        """
        reads qstat YAML/json file and populates four lists. Returns the lists
        ex read_qstat_yaml
        Common for PBS, OAR, SGE
        """
        job_ids, usernames, job_states, queue_names = [], [], [], []

        for qstat in qstats:
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
