"""
TODO: better organised the information in here
"""
from common_module import *
import re
from itertools import count


class StatExtractor(object):
    """
    Extracts qstat/qstatq information from files coming from the respective batch system commmands
    (PBS, OAR, SGE etc)
    """

    def __init__(self, config):
        self.config = config
        self.anonymize = self.anonymize_func()

    def _process_qstat_line(self, re_search, line, re_match_positions):
        """
        extracts data from a tabular qstat-like file
        returns a list
        """
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

    def anonymize_func(self):
        """
        creates and returns an _anonymize_func object (closure)
        Anonymisation can be used by the user for providing feedback to the developers, without leaking cluster's private data.
        i.e. the logs and the output should no longer contain sensitive information about the clusters ran by the user.
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


class GenericBatchSystem(object):
    def __init__(self):
        pass

    def get_queues_info(self):
        raise NotImplementedError

    def get_worker_nodes(self):
        raise NotImplementedError

    def get_jobs_info(self, qstats):
        raise NotImplementedError

    @staticmethod
    def get_mnemonic():
        raise NotImplementedError
