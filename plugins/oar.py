from serialiser import *
from common_module import *
import yaml_parser as yaml
try:
    from collections import OrderedDict
except ImportError:
    from legacy.ordereddict import OrderedDict


class OarStatExtractor(StatExtractor):
    def __init__(self, config):
        StatExtractor.__init__(self, config)
        self.user_q_search = r'^(?P<job_id>[0-9]+)\s+' \
                             r'(?P<name>[0-9A-Za-z_.-]+)?\s+' \
                             r'(?P<user>[0-9A-Za-z_.-]+)\s+' \
                             r'(?:\d{4}-\d{2}-\d{2})\s+' \
                             r'(?:\d{2}:\d{2}:\d{2})\s+' \
                             r'(?P<job_state>[RWF])\s+' \
                             r'(?P<queue>default|besteffort)'

    def extract_qstat(self, orig_file):
        all_values = list()
        with open(orig_file, 'r') as fin:
            logging.debug('File state before OarStatExtractor.extract_qstat: %(fin)s' % {"fin": fin})
            _ = fin.readline()  # header
            fin.readline()  # dashes
            re_match_positions = ('job_id', 'user', 'job_state', 'queue')
            re_search = self.user_q_search
            for line in fin:
                qstat_values = self._process_qstat_line(re_search, line, re_match_positions)
                all_values.append(qstat_values)

        return all_values


class OARBatchSystem(GenericBatchSystem):

    @staticmethod
    def get_mnemonic():
        return "oar"

    def __init__(self, scheduler_output_filenames, config):
        self.oarnodes_s_file = scheduler_output_filenames.get('oarnodes_s_file')
        self.oarnodes_y_file = scheduler_output_filenames.get('oarnodes_y_file')
        self.oarstat_file = scheduler_output_filenames.get('oarstat_file')

        self.config = config
        self.oar_stat_maker = OarStatExtractor(self.config)

    def get_worker_nodes(self):
        nodes_resids = self._read_oarnodes_s_yaml(self.oarnodes_s_file)
        resids_jobs = self._read_oarnodes_y_textyaml(self.oarnodes_y_file)

        nodes_jobs = {}
        for node in nodes_resids:
            resids_state_lot = nodes_resids[node]
            for (resid, state) in resids_state_lot:
                nodes_jobs.setdefault(node, []).append((resids_jobs[int(resid)], state))

        worker_nodes = list()
        # TODO: make user-tuneable
        node_state_mapping = {'Alive': '-', 'Dead': 'd', 'Suspected': 's', 'Mixed': '%'}
        for node in nodes_jobs:
            d = OrderedDict()
            d['domainname'] = node
            nr_of_jobs = len(nodes_jobs[node])
            d['np'] = nr_of_jobs
            # d['core_job_map'] = [{'core': idx, 'job': job[0]} for idx, job in enumerate(nodes_jobs[node]) if job[0] is not None]
            d['core_job_map'] = dict((idx, job[0]) for idx, job in enumerate(nodes_jobs[node]) if job[0] is not None)
            d['state'] = self._calculate_oar_state(nodes_jobs[node], nr_of_jobs, node_state_mapping)
            worker_nodes.append(d)

        logging.info('worker_nodes contains %s entries' % len(worker_nodes))
        return worker_nodes

    def get_jobs_info(self):
        job_ids, usernames, job_states, queue_names = [], [], [], []
        qstats = self.oar_stat_maker.extract_qstat(self.oarstat_file)
        # TODO: clumsily glued, should be more naturally connected
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

    def get_queues_info(self):
        """
        OAR does not provide this info.
        """
        total_running_jobs = 0
        total_queued_jobs = 0
        qstatq_lod = []
        return total_running_jobs, total_queued_jobs, qstatq_lod

    def _read_oarnodes_s_yaml(self, fn_s):
        assert os.path.isfile(fn_s)
        anonymize = self.oar_stat_maker.anonymize_func()
        logging.debug('File %s exists: %s' % (fn_s, os.path.isfile(fn_s)))
        try:
            assert os.stat(fn_s).st_size != 0
        except AssertionError:
            logging.critical('File %s is empty!! Exiting...\n' % fn_s)
            raise
        data = yaml.safe_load(fn_s, DEF_INDENT=4)
        if options.ANONYMIZE:
            nodes_resids = dict([(anonymize(node, 'wns'), resid_state.items()) for node, resid_state in data.items()])
        else:
            nodes_resids = dict([(node, resid_state.items()) for node, resid_state in data.items()])
        return nodes_resids

    def _read_oarnodes_y_textyaml(self, fn):
        oar_nodes = {}
        logging.debug("Before opening %s" % fn)
        with open(fn, mode='r') as fin:
            logging.debug("File state %s" % fin)
            fin.readline()  # '---'
            line = fin.readline().strip()  # first res_id
            while line:
                oar_node, line = self._read_oar_node_y_textyaml(fin, line)
                oar_nodes.update(oar_node)

            resids_jobs = dict([(resid, info.get('jobs', None)) for resid, info in oar_nodes.items()])
        return resids_jobs

    @staticmethod
    def _read_oar_node_y_textyaml(fin, line):
        _oarnode = dict()

        res_id = line.strip(': ')
        _oarnode[int(res_id)] = dict()

        line = fin.readline().strip()
        while line and not line[0].isdigit():
            key, value = line.strip().split(': ')
            _oarnode[int(res_id)][key] = value
            line = fin.readline().strip()

        return _oarnode, line

    def _calculate_oar_state(self, jobid_state_lot, nr_of_jobs, node_state_mapping):
        """
        If all resource ids within the node are either alive or dead or suspected, the respective label is given to the node.
        Otherwise, a mixed-state is reported
        """
        # todo: make user-tuneable
        states = [job_state_tpl[1] for job_state_tpl in jobid_state_lot]
        alive = states.count('Alive')
        dead = states.count('Dead')
        suspected = states.count('Suspected')

        if bool(alive) + bool(dead) + bool(suspected) > 1:
            state = node_state_mapping['Mixed']  # TODO: investigate!
            return state
        else:
            return node_state_mapping[states[0]]
