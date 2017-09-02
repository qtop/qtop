from math import ceil
import logging
import sys
import os
import subprocess
try:
    from collections import namedtuple, OrderedDict, Counter
except ImportError:
    from qtop_py.legacy.namedtuple import namedtuple
    from qtop_py.legacy.ordereddict import OrderedDict
    from qtop_py.legacy.counter import Counter
from itertools import izip, izip_longest
import re
import qtop_py.yaml_parser as yaml
from qtop_py import utils
from qtop_py.colormap import user_to_color_default, color_to_code, queue_to_color, nodestate_to_color_default
from qtop_py.constants import (SYSTEMCONFDIR, QTOPCONF_YAML, QTOP_LOGFILE, USERPATH, MAX_CORE_ALLOWED,
    MAX_UNIX_ACCOUNTS, KEYPRESS_TIMEOUT, FALLBACK_TERMSIZE)


try:
    from collections import namedtuple, OrderedDict, Counter
except ImportError:
    from qtop_py.legacy.namedtuple import namedtuple
    from qtop_py.legacy.ordereddict import OrderedDict
    from qtop_py.legacy.counter import Counter


class WNOccupancy(object):
    def __init__(self, cluster, colorize):
        self.cluster = cluster
        self.conf = cluster.conf
        self.config = self.conf.config
        self.dynamic_config = self.conf.dynamic_config
        self.colorize = colorize
        self.user_names = self.cluster.user_names
        self.job_ids = self.cluster.job_ids
        self.job_states = self.cluster.job_states
        self.job_queues = self.cluster.job_queues

        self.table = list()
        self.user_to_id = dict()
        self.id_to_user = None
        self.jobid_to_user_to_queue = None
        self.user_node_use = None  # Counter Object
        self.userid_to_userid_re_pat = dict()
        self.detail_of_name = dict()
        self.print_char_start = None
        self.print_char_stop = None
        self.extra_matrices_nr = None
        self.wn_vert_labels = dict()
        self.header_row = None

    def set_start(self):
        workernode_list = self.cluster.workernode_list
        start = 0

        if self.conf.cmd_options.NOMASKING and min(workernode_list) > self.conf.min_masking_threshold:
            # exclude unneeded first empty nodes from the matrix
            start = min(workernode_list) - 1
        return start

    def _find_matrices_width(self, h_term_size, DEADWEIGHT=11):
        """
        masking/clipping functionality: if the earliest node number is high (e.g. 130), the first 129 WNs need not show up.
        case 1: wn_number is RemapNr, WNList is WNListRemapped
        case 2: wn_number is BiggestWrittenNode, WNList is WNList
        DEADWEIGHT is the space taken by the {__XXXX__} labels on the right of the CoreX map

        uses cluster.highest_wn, cluster.workernode_list
        """
        wn_number = self.cluster.highest_wn
        term_columns = h_term_size # term_columns = display.viewport.h_term_size



        start = self.set_start()

        # Extra matrices may be needed if the WNs are more than the screen width can hold.
        if wn_number > start:  # start will either be 1 or (masked >= config['min_masking_threshold'] + 1)
            extra_matrices_nr = int(ceil(abs(wn_number - start) / float(term_columns - DEADWEIGHT))) - 1
        elif self.conf.cmd_options.REMAP:  # was: ***wn_number < start*** and len(cluster.node_subclusters) > 1:  # Remapping
            extra_matrices_nr = int(ceil(wn_number / float(term_columns - DEADWEIGHT))) - 1
        else:
            raise (NotImplementedError, "Not foreseen")

        if self.conf.config['USER_CUT_MATRIX_WIDTH']:  # if the user defines a custom cut (in the configuration file)
            stop = start + self.conf.config['USER_CUT_MATRIX_WIDTH']
            self.extra_matrices_nr = wn_number / self.conf.config['USER_CUT_MATRIX_WIDTH']
        elif extra_matrices_nr:  # if more matrices are needed due to lack of space, cut every matrix so that if fits to screen
            stop = start + term_columns - DEADWEIGHT
        else:  # just one matrix, small cluster!
            stop = start + wn_number
            extra_matrices_nr = 0

        logging.debug('reported term_columns, DEADWEIGHT: %s\t%s' % (term_columns, DEADWEIGHT))
        logging.debug('reported start/stop lengths: %s--> %s' % (start, stop))
        return start, stop, extra_matrices_nr

    def calc_all_wnid_label_lines(self):  # (total_wn) in case of multiple cluster.node_subclusters
        """
        calculates the Worker Node ID number line widths. expressed by hxxxxs in the following form, e.g. for hundreds of nodes:
        '1': "00000000..."
        '2': "0000000001111111..."
        '3': "12345678901234567..."
        """
        NAMED_WNS = self.dynamic_config['force_names']
        highest_wn = self.cluster.highest_wn
        if NAMED_WNS:  #  or options.FORCE_NAMES
            workernode_dict = self.cluster.workernode_dict
            hosts = [state_corejob_dn['host'] for _, state_corejob_dn in workernode_dict.items()]
            node_str_width = len(max(hosts, key=len))
            wn_vert_labels = OrderedDict((str(place), []) for place in range(1, node_str_width + 1))
            for node in workernode_dict:
                host = workernode_dict[node]['host']
                extra_spaces = node_str_width - len(host)
                string = "".join(" " * extra_spaces + host)
                for place in range(node_str_width):
                    wn_vert_labels[str(place + 1)].append(string[place])
        else:
            node_str_width = len(str(highest_wn))  # 4
            wn_vert_labels = dict([(str(place), []) for place in range(1, node_str_width + 1)])
            for nr in range(1, highest_wn + 1):
                extra_spaces = node_str_width - len(str(nr))  # 4 - 1 = 3, for wn0001
                string = "".join("0" * extra_spaces + str(nr))
                for place in range(1, node_str_width + 1):
                    wn_vert_labels[str(place)].append(string[place - 1])

        for wn in wn_vert_labels.keys():
            wn_vert_labels[wn] = "".join(wn_vert_labels[wn])
        return wn_vert_labels

    def calc_general_mult_attr_line(self, part_name, yaml_key, config):  # NEW
        elem_identifier = [d for d in config['workernodes_matrix'] if part_name in d][0]  # jeeez
        part_name_idx = config['workernodes_matrix'].index(elem_identifier)
        user_max_len = int(config['workernodes_matrix'][part_name_idx][part_name]['max_len'])
        try:
            real_max_len = max([len(self.cluster.workernode_dict[_node][yaml_key]) for _node in self.cluster.workernode_dict])
        except KeyError:
            logging.critical("%s lines in the matrix are not supported for %s systems. "
                             "Please remove appropriate lines from conf file. Exiting..."
                             % (part_name, config['scheduler']))

            # web.stop()
            sys.exit(1)
        min_len = min(user_max_len, real_max_len)
        max_len = max(user_max_len, real_max_len)
        if real_max_len > user_max_len:
            logging.warn(
                "Some longer %(attr)ss have been cropped due to %(attr)s length restriction by user" % {"attr": part_name})

        # initialisation of lines
        multiline_map = OrderedDict()
        for line_nr in range(1, min_len + 1):
            multiline_map['attr%sline' % str(line_nr)] = []

        for _node in self.cluster.workernode_dict:
            node_attrs = self.cluster.workernode_dict[_node]
            # distribute state, qname etc to lines
            for attr_line, ch in izip_longest(multiline_map, node_attrs[yaml_key], fillvalue=' '):
                try:
                    if ch == ' ':
                        ch = utils.ColorStr(' ')
                    elif ch == '?':
                        ch = utils.ColorStr('?', color="Gray_D")
                    multiline_map[attr_line].append(ch)
                except KeyError:
                    break
                    # TODO: is this really needed?: self.cluster.workernode_dict[_node]['state_column']

        for line, attr_line in enumerate(multiline_map, 1):
            if line == user_max_len:
                break
        return multiline_map

    def _calc_core_matrix(self, user_to_id, jobid_to_user_to_queue):
        core_user_map = OrderedDict()

        core_coloring = self.conf.dynamic_config.get('core_coloring', self.config['core_coloring'])

        for core_nr in self.cluster.core_span:
            core_user_map['Core%svector' % str(core_nr)] = []  # Cpu0vector, Cpu1vector, Cpu2vector, ... = [],[],[], ...

        for _node in self.cluster.workernode_dict:
            core_user_map = self._fill_node_cores_vector(_node, core_user_map, user_to_id, self.cluster.core_span,
                                                         jobid_to_user_to_queue, core_coloring)

        return core_user_map

    def _fill_node_cores_vector(self, _node, core_user_map, user_to_id, _core_span, jobid_to_user_to_queue, core_coloring):
        """
        Calculates the actual contents of the map by filling in a status string for each CPU line
        One of the two dimensions of the matrix is determined by the highest-core WN existing. If other WNs have less cores,
        these positions are filled with '#'s (or whatever is defined in config['non_existent_node_symbol']).
        """
        state_np_corejob = self.cluster.workernode_dict[_node]
        state = state_np_corejob['state']
        np = state_np_corejob['np']
        corejobs = state_np_corejob.get('core_job_map', dict())
        non_existent_node_symbol = self.conf.config['non_existent_node_symbol']
        gray_hash = utils.ColorStr(non_existent_node_symbol, color='Gray_D')
        gray_underscore = utils.ColorStr('_', color='Gray_D')

        if state == '?':  # for non-existent machines
            for core_line in core_user_map:
                core_user_map[core_line].append(gray_hash)
        else:
            node_cores = [str(x) for x in range(int(np))]
            core_user_map, node_free_cores = self.color_cores_and_return_unused(node_cores, core_user_map, corejobs,
                                                                                core_coloring, jobid_to_user_to_queue)
            for core in node_free_cores:
                core_user_map['Core' + str(core) + 'vector'].append(gray_underscore)

            non_existent_node_cores = [core for core in _core_span if core not in node_cores]
            for core in non_existent_node_cores:
                core_user_map['Core' + str(core) + 'vector'].append(gray_hash)

        return core_user_map

    @staticmethod
    def get_hl_q_or_users(_highlighted_queues_or_users):
        for selection_users_queues in _highlighted_queues_or_users:
            selection = selection_users_queues.keys()[0]
            users_queues = selection_users_queues[selection]
            and_or, type = selection.rsplit('include_')
            and_or_func = all if not and_or else any

            for user_queue in users_queues:
                yield user_queue, type, and_or_func

    def color_cores_and_return_unused(self, node_cores, core_user_map, corejobs, _core_coloring, jobid_to_user_to_queue):
        """
        Adds color information to the core job, returns free cores.
        locals()[queue_or_user] transforms either 'user'=> user or 'queue'=> queue
        depending on qtopconf yaml's "core_coloring",
        or on runtime in watch mode, if user presses appropriate keybinding
        """
        node_free_cores = node_cores[:]
        queue_or_user_map = {'user_to_color': 'user_pat', 'queue_to_color': 'queue'}
        queue_or_user_str = queue_or_user_map[_core_coloring]

        selected_pat_to_color_map = self.conf.__dict__[_core_coloring]
        _highlighted_queues_or_users = self.conf.dynamic_config.get('highlight', self.config['highlight'])

        self.id_to_user = dict(izip((str(x) for x in self.user_to_id.itervalues()), self.user_to_id.iterkeys()))
        for (user, core, queue) in self._valid_corejobs(corejobs, jobid_to_user_to_queue):
            id_ = utils.ColorStr.from_other_color_str(self.user_to_id[user])
            user_pat = self.userid_to_userid_re_pat[str(id_)]  # in case it is used in viewed_pattern
            viewed_pattern = locals()[queue_or_user_str]  # either a queue or userid pattern
            matches = []
            and_or_func = any

            for user_queue_to_highlight, type, and_or_func in WNOccupancy.get_hl_q_or_users(_highlighted_queues_or_users):
                if (type == 'user_pat'):
                    actual_user_queue = user
                elif (type == 'user_id'):
                    actual_user_queue = user
                    user_queue_to_highlight = self.id_to_user[user_queue_to_highlight]
                elif type == 'queue':
                    actual_user_queue = queue

                matches.append(re.match(user_queue_to_highlight, actual_user_queue))

            if not _highlighted_queues_or_users or and_or_func(match.group(0) if match is not None else None for match in matches):
                id_.color = selected_pat_to_color_map.get(viewed_pattern, 'White')  # queue or user decided on runtime
            else:
                id_.color = 'Gray_D'

            id_.q = queue
            core_user_map['Core' + str(core) + 'vector'].append(id_)
            node_free_cores.remove(core)  # this is an assigned core, hence it doesn't belong to the node's free cores

        return core_user_map, node_free_cores

    def _valid_corejobs(self, corejobs, jobid_to_user_to_queue):
        """
        Generator that yields only those core-job pairs that successfully match to a user
        """
        for core, _job in corejobs.items():
            job = re.sub(r'\[\d+\]', '[]', str(_job))  # also takes care of job arrays
            try:
                user_queue = jobid_to_user_to_queue[job]
            except KeyError as KeyErrorValue:
                logging.critical('There seems to be a problem with the qstat output. '
                                 'A Job (ID %s) has gone rogue. '
                                 'Please check with the SysAdmin.' % (str(KeyErrorValue)))
                raise KeyError
            else:
                user, queue = user_queue
                yield user, str(core), queue

    def is_matrix_coreless(self, print_char_start, print_char_stop):
        non_existent_symbol = self.config['non_existent_node_symbol']
        lines = 0
        core_user_map = self.core_user_map
        remove_corelines = self.conf.dynamic_config.get('rem_empty_corelines', self.conf.config['rem_empty_corelines']) + 1

        for core_x_vector, ind, k, is_corevector_removable in self.gauge_core_vectors(core_user_map,
                                                                                 print_char_start,
                                                                                 print_char_stop,
                                                                                 non_existent_symbol,
                                                                                 remove_corelines):
            if is_corevector_removable:
                lines += 1
        return lines == len(core_user_map)

    def strict_check_jobs(self, cluster):
        counted_jobs = WNOccupancy._count_jobs_strict(self.core_user_map)
        if counted_jobs != cluster.total_running_jobs:
            print "Counted jobs (%s) -- Total running jobs reported (%s) MISMATCH!" % (counted_jobs, cluster.total_running_jobs)

    @staticmethod
    def _count_jobs_strict(core_user_map):
        count = 0
        for k in core_user_map:
            just_jobs = core_user_map[k].translate(None, "#_")
            count += len(just_jobs)
        return count

    def calculate_user_node_use(self):
        """
        This calculates the number of nodes each user has jobs in (shown in User accounts and pool mappings)
        """
        user_nodes = []

        for (node_idx, node) in self.cluster.workernode_dict.items():
            node['node_user_set'] = set([self.jobid_to_user_to_queue[job][0] for job in node['node_job_set']])
            user_nodes.extend(list(node['node_user_set']))

        return Counter(user_nodes)

    @staticmethod
    def coreline_not_there(symbol, switch, delta, core_x_str):
        """
        Checks if a line consists of only not-really-there cores, i.e. core 32 on a line of 24-core machines
        (being there because there are other machines with 32 cores on an adjacent matrix)
        """
        return switch == 2 and ((symbol * delta == core_x_str) or (symbol * len(core_x_str) == core_x_str))

    @staticmethod
    def coreline_unused(symbol, switch, print_length, core_x_str):
        """
        Checks if a line consists of either not-really-there cores or unused cores
        """
        all_symbols_in_line = set(core_x_str)
        unused_symbols = set(['_', symbol])
        only_unused_symbols_in_line = all_symbols_in_line.issubset(unused_symbols)
        return switch == 3 and only_unused_symbols_in_line and (print_length == len(core_x_str))

    @staticmethod
    def coreline_notthere_or_unused(symbol, switch, delta, core_x_str):
        return WNOccupancy.coreline_not_there(symbol, switch, delta, core_x_str) \
                or WNOccupancy.coreline_unused(symbol, switch, delta, core_x_str)

    def gauge_core_vectors(self, core_user_map, print_char_start, print_char_stop, non_existent_symbol, remove_corelines):
        """
        generator that loops over each core user vector and yields a boolean stating whether the core vector can be omitted via
        REM_EMPTY_CORELINES or its respective switch
        """
        delta = print_char_stop - print_char_start
        for ind, k in enumerate(core_user_map):
            core_x_vector = core_user_map['Core' + str(ind) + 'vector'][print_char_start:print_char_stop]
            core_x_str = ''.join(str(x) for x in core_x_vector)
            yield core_x_vector, ind, k, self.coreline_notthere_or_unused(non_existent_symbol, remove_corelines, delta,
                                                                          core_x_str)

    def get_detail_of_name(self, table):
        """
        Reads file $HOME/.local/qtop/getent_passwd.txt or whatever is put in QTOPCONF_YAML
        and extracts the fullname of the users. This shall be printed in User Accounts
        and Pool Mappings.
        """
        conf = self.conf
        config = self.conf.config
        extract_info = config.get('extract_info', None)
        if not extract_info:
            return dict()

        sep = ':'
        user_field_idx = int(extract_info.get('user_field', 5))
        regex = extract_info.get('user_regex', None)

        if self.conf.cmd_options.GET_GECOS:
            users = ' '.join([line[8] for line in table])  ## todo: replace index with order.index (changes everytime?)
            passwd_command = extract_info.get('user_details_realtime') % users
            passwd_command = passwd_command.split()
        else:
            passwd_command = extract_info.get('user_details_cache').split()
            passwd_command[-1] = os.path.expandvars(passwd_command[-1])

        try:
            p = subprocess.Popen(passwd_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        except OSError:
            logging.critical(
                '\nCommand "%s" could not be found in your system. \nEither remove -G switch or modify the command in '
                'qtopconf.yaml (value of key: %s).\nExiting...' % (self.colorize(passwd_command[0], color_func='Red_L'),
                                                                   'user_details_realtime'))
            sys.exit(0)
        else:
            output, err = p.communicate("something here")
            if 'No such file or directory' in err:
                logging.warn('You have to set a proper command to get the passwd file in your %s file.' % QTOPCONF_YAML)
                logging.warn('Error returned by getent: %s\nCommand issued: %s' % (err, passwd_command))

        detail_of_name = dict()
        for line in output.split('\n'):
            try:
                user, field = line.strip().split(sep)[0:user_field_idx:user_field_idx - 1]
            except ValueError:
                break
            else:
                try:
                    detail = eval(regex)
                except (AttributeError, TypeError):
                    detail = field.strip()
                finally:
                    detail_of_name[user] = detail
        return detail_of_name

    def get_group_of_name(self, table):
        """
        Gets the user group via cmd id -nG <user>.
        This shall be printed in User Accounts and Pool Mappings.
        """
        conf = self.conf
        config = self.conf.config
        extract_info = config.get('extract_info', None)
        if not extract_info:
            return dict()

        sep = ' '
        grp_field_idx = int(extract_info.get('grp_field', 1))  # should later be regexable
        users = ' '.join([line[8] for line in table])  # TODO: fix index to order.index
        user_group_command = extract_info.get('user_group_cmd', None) % users
        try:
            p = subprocess.Popen(user_group_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        except OSError:
            logging.critical(
                '\nCommand "%s" could not be found in your system. \nEither remove -G switch or modify the command in '
                'qtopconf.yaml (value of key: %s).\nExiting...' % (self.colorize(user_group_command[0], color_func='Red_L'),
                                                                   'user_details_realtime'))
            sys.exit(0)
        else:
            # output, err = pr[str(idx-1)].communicate("something here")
            output, err= p.communicate("something here")
            if 'No such file or directory' in err:
                logging.warn('You have to set a proper command to get the passwd file in your %s file.' % QTOPCONF_YAML)
                logging.warn('Error returned by getent: %s\nCommand issued: %s' % (err, user_group_command))

        detail_of_group = dict()
        for line in output.split('\n'):
            try:
                user, group = line.strip().split(sep)
            except ValueError:
                continue
            else:
                detail_of_group[user] = group

        detail_of_group['sotiris'] = 'a_group'
        detail_of_group['alicesgm'] = 'alico'
        detail_of_group['biomed017'] = 'biomed'
        return detail_of_group

    def calculate_matrices(self, display, scheduler):
        """
        Prints the Worker Nodes Occupancy table.
        if there are non-uniform WNs in pbsnodes.yaml, e.g. wn01, wn02, gn01, gn02, ...,  remapping is performed.
        Otherwise, for uniform WNs, i.e. all using the same numbering scheme, wn01, wn02, ... proceeds as normal.
        Number of Extra tables needed is calculated inside the calc_all_wnid_label_lines function below
        """
        h_term_size = display.viewport.h_term_size
        scheduler_name = scheduler.scheduler_name
        self.print_char_start, self.print_char_stop, self.extra_matrices_nr = self._find_matrices_width(h_term_size)
        self.wn_vert_labels = self.calc_all_wnid_label_lines()

        # Loop below only for user-inserted/customizeable values.
        for yaml_key, part_name, systems in yaml.get_yaml_key_part(self.config, scheduler_name, outermost_key='workernodes_matrix'):
            if scheduler_name in systems:
                self.__setattr__(part_name, self.calc_general_mult_attr_line(part_name, yaml_key, self.config))

        self.core_user_map = self._calc_core_matrix(self.user_to_id, self.jobid_to_user_to_queue)

    def process(self):
        # also included self.calculate_account_jobs(self.job_ids)
        self.jobid_to_user_to_queue = dict(izip(self.job_ids, izip(self.user_names, self.job_queues)))  # TODOTODAY: shove this somewhere
