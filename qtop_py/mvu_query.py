## Copyright (c) 2026 qtop contributors
## SPDX-License-Identifier: MIT
"""Snapshot queries and layout with explicitly supplied model state."""

import copy
import logging
import re
from collections import Counter, OrderedDict, namedtuple
from math import ceil
from operator import itemgetter
from typing import Any, Callable, Collection, Dict, Iterable, Iterator, List, Mapping, Optional, Sequence, Set, Tuple, Union

from qtop_py import configuration, utils, yaml_parser
from qtop_py.constants import DEFAULT_USER_ID_RANKING, QTOPCONF_YAML, SYMBOL_LONG_TAIL_USER
from qtop_py.errors import JobNotFound, OperationalError
from qtop_py.mvu import MVU_colorize_nodestate, MVU_keep_queue_initials_only_and_colorize
from qtop_py.mvu import MVU_gauge_core_vectors as gauge_core_vectors
from qtop_py.mvu import MVU_node_job_ids as node_job_ids
from qtop_py.mvu import MVU_reserved_pattern_mappings as _reserved_pattern_mappings
from qtop_py.serialiser import resolve_job_id

ACCOUNT_LETTERS = re.compile(r"[A-Za-z]+")


def rank_users(
    all_counts: "Mapping[str, int]",
    running: "Mapping[str, int]",
    queued: "Mapping[str, int]",
    USER_ID_RANKING: "Sequence[str]" = DEFAULT_USER_ID_RANKING,
) -> "List[Tuple[str, int]]":
    """Order users for ID assignment: count keys rank higher counts first, the final name key breaks ties.

    Returns (user, all jobs count) pairs, as the accounts table expects.
    """
    COUNTS = {
        "R+Q": lambda user: running.get(user, 0) + queued.get(user, 0),
        "R": lambda user: running.get(user, 0),
        "Q": lambda user: queued.get(user, 0),
        "all": lambda user: all_counts[user],
    }
    COUNT_KEYS, NAME_KEY = USER_ID_RANKING[:-1], USER_ID_RANKING[-1]
    # Two stable passes: names first, then counts (descending); equal counts keep the name order.
    users = sorted(all_counts, reverse=NAME_KEY == "name_desc")
    users.sort(key=lambda user: tuple(COUNTS[key](user) for key in COUNT_KEYS), reverse=True)
    return [(user, all_counts[user]) for user in users]


class WNOccupancy(object):
    """Compute the accounts table, user IDs and core matrices behind the worker-node occupancy view."""

    def __init__(self, cluster: "Cluster", config: Dict[str, Any], snapshot: Any, user_to_color: Dict[str, str], MVU_model: Any) -> None:
        self.MVU_model = MVU_model
        self.cluster = cluster
        self.config = config
        self.snapshot = snapshot
        self.account_jobs_table: List[List[Any]] = list()
        self.user_to_id: Dict[str, Any] = dict()
        self.job_to_user_and_queue: Dict[str, Tuple[str, str]] = dict()
        self.user_names, self.job_states = self._get_usernames_and_states(snapshot.jobs_dict)

        self.calculate(snapshot, user_to_color)

    def _get_usernames_and_states(self, jobs_dict: Mapping[str, Any]) -> Tuple[List[str], List[str]]:
        user_names, job_states = [], []
        for value in jobs_dict.values():
            user_names.append(value.user_name)
            job_states.append(value.job_state)
        return user_names, job_states

    def calculate(self, snapshot: Any, user_to_color: Dict[str, str]) -> None:
        """Compute the accounts table, user IDs, matrix widths, node ID lines and attribute lines for one frame.

        Non-uniform node names (wn01, gn01, ...) are remapped; uniform ones keep their numbers.
        """
        if not self.cluster:
            return
        # snapshot.jobs_dict => job_id: job name/state/queue

        self.job_to_user_and_queue = {job_id: (job.user_name, job.job_queue) for job_id, job in snapshot.jobs_dict.items()}
        self.user_machine_use = self.calculate_user_node_use(self.cluster, self.job_to_user_and_queue)

        user_job_per_state_counts = self._calculate_user_job_counts(self.user_names, self.job_states)
        users_by_job_count = self._produce_user_lot(self.user_names, user_job_per_state_counts)
        user_to_id = self._create_id_for_users(users_by_job_count)
        _account_jobs_table = self._create_sort_acct_jobs_table(user_job_per_state_counts, users_by_job_count, user_to_id)
        self.account_jobs_table, self.user_to_id = self._create_account_jobs_table(user_to_id, _account_jobs_table)
        self.user_color_patterns = self.make_pattern_out_of_mapping(mapping=user_to_color)

        self.node_column_start, self.node_column_stop, self.extra_matrices_nr = self.find_matrices_width()
        self.wn_vert_labels = self.calc_all_wnid_label_lines(self.MVU_model.display_overrides["force_names"])

        # For-loop below only for user-inserted/customizeable values.
        for yaml_key, part_name, systems in yaml_parser.get_yaml_key_part(self.config, self.MVU_model.scheduler, outermost_key="workernodes_matrix"):
            if self.MVU_model.scheduler in systems:
                self.__setattr__(part_name, self.calc_general_mult_attr_line(part_name, yaml_key, self.config, self.MVU_model.backend))

        self.core_user_map = self._calc_core_matrix(self.user_to_id, self.job_to_user_and_queue)

    def _create_account_jobs_table(self, user_to_id: Dict[str, Any], account_jobs_table: List[List[Any]]) -> Tuple[List[List[Any]], Dict[str, Any]]:
        for row in account_jobs_table:
            unix_account = row[4]
            userid = user_to_id.get(unix_account, utils.ColorStr(SYMBOL_LONG_TAIL_USER))
            row[0] = user_to_id[unix_account] = utils.ColorStr(str(userid), color="Red_L")

        return account_jobs_table, user_to_id

    def _create_sort_acct_jobs_table(
        self, user_job_per_state_counts: Dict[str, Any], user_all_jobs_sorted: Sequence[Tuple[str, int]], user_to_id: Dict[str, Any]
    ) -> List[List[Any]]:
        """Calculates what is actually below the id|  jobs>=R + Q | unix account etc line."""
        account_jobs_table = [
            [
                user_to_id[user_name],
                user_job_per_state_counts["running_of_user"][user_name],
                user_job_per_state_counts["queued_of_user"][user_name],
                total_jobs_for_user,
                user_name,
                self.user_machine_use[user_name],
                {state_key: counts.get(user_name, 0) for state_key, counts in user_job_per_state_counts.items()},
            ]
            for user_name, total_jobs_for_user in user_all_jobs_sorted
        ]
        firstletter_ids = self.config["fill_with_user_firstletter"]
        id_order = {} if firstletter_ids else dict((symbol, index) for index, symbol in enumerate(configuration.available_possible_ids(self.config)))

        def account_order(row: Sequence[Any]) -> Tuple[bool, Any, Any]:
            symbol = str(row[0])
            position = symbol if firstletter_ids else id_order.get(symbol, len(id_order))
            return symbol == SYMBOL_LONG_TAIL_USER, position, row[4]

        account_jobs_table.sort(key=account_order)  # displayed ID first, then username ascending; overflow last
        return account_jobs_table

    def _create_user_job_counts(self, user_names: List[str], job_states: List[str], state_abbrevs: Mapping[str, str]) -> Dict[str, Any]:
        """Counting of e.g. R, Q, C, W, E attached to each user."""
        user_job_per_state_counts: Dict[str, Any] = dict()
        for state_count_key in state_abbrevs.values():
            user_job_per_state_counts[state_count_key] = dict()

        for user_name, job_state in zip(user_names, job_states):
            try:
                state_count_key = state_abbrevs[job_state]
            except KeyError as error:
                raise JobNotFound(job_state) from error

            user_job_per_state_counts[state_count_key][user_name] = user_job_per_state_counts[state_count_key].get(user_name, 0) + 1

        for user_name in user_job_per_state_counts["running_of_user"]:
            for state_count_key in user_job_per_state_counts:
                if state_count_key != "running_of_user":
                    user_job_per_state_counts[state_count_key].setdefault(user_name, 0)

        return user_job_per_state_counts

    def _produce_user_lot(self, _user_names: List[str], user_job_per_state_counts: Dict[str, Any]) -> List[Tuple[str, int]]:
        """Return (user, all jobs count) pairs in ID order, ranked by user_id_ranking (default: R+Q, R, name_desc)."""
        USER_ID_RANKING = self.config.get("user_id_ranking") or DEFAULT_USER_ID_RANKING
        return rank_users(
            Counter(_user_names),
            user_job_per_state_counts.get("running_of_user", {}),
            user_job_per_state_counts.get("queued_of_user", {}),
            USER_ID_RANKING,
        )

    @staticmethod
    def job_state_totals(job_states: Iterable[str], state_abbrevs: Mapping[str, str]) -> Dict[str, int]:
        """Count jobs per state key (e.g. running_of_user); states missing from state_abbrevs count as unknown_of_user."""
        totals: Dict[str, int] = {}
        for job_state in job_states:
            state_key = state_abbrevs.get(job_state, "unknown_of_user")
            totals[state_key] = totals.get(state_key, 0) + 1
        return totals

    def _calculate_user_job_counts(self, user_names: List[str], job_states: List[str]) -> Dict[str, Any]:
        """Count each user's jobs per state; unknown states are logged and counted as unknown_of_user."""
        self.config = self._expand_useraccounts_symbols(self.config, user_names)
        state_abbrevs = dict(self.config["state_abbreviations"][self.MVU_model.scheduler])
        for job_state in job_states:
            if job_state not in state_abbrevs:
                logging.warning("Unknown job state %s; retaining job in totals without classifying it as running or queued.", job_state)
                state_abbrevs[job_state] = "unknown_of_user"

        user_job_per_state_counts = self._create_user_job_counts(user_names, job_states, state_abbrevs)

        for state_count_key in state_abbrevs.values():
            missing_user_ids = set(user_names).difference(user_job_per_state_counts[state_count_key])
            for missing_user_id in missing_user_ids:
                user_job_per_state_counts[state_count_key].setdefault(missing_user_id, 0)

        return user_job_per_state_counts

    def _expand_useraccounts_symbols(self, config: Dict[str, Any], user_list: Sequence[Any]) -> Dict[str, Any]:
        """Keep the configured symbol alphabet stable. Overflow users share the long-tail symbol."""
        return config

    def _create_id_for_users(self, users_by_job_count: Sequence[Tuple[str, int]]) -> Dict[str, Any]:
        user_to_id = {}
        possible_ids = configuration.available_possible_ids(self.config)
        reserved_symbols = configuration.reserved_user_symbols(self.config)
        for id_, user_allcount in enumerate(users_by_job_count):
            unix_account = user_allcount[0]
            if self.config["fill_with_user_firstletter"]:
                firstletter = unix_account[0] if unix_account else SYMBOL_LONG_TAIL_USER
                userid = firstletter if firstletter not in reserved_symbols else SYMBOL_LONG_TAIL_USER
                user_to_id[unix_account] = utils.ColorStr(userid)
            elif len(possible_ids) > id_:
                user_to_id[unix_account] = utils.ColorStr(possible_ids[id_])
            else:
                user_to_id[unix_account] = utils.ColorStr(SYMBOL_LONG_TAIL_USER)

        return user_to_id

    def make_pattern_out_of_mapping(self, mapping: Mapping[str, str]) -> Dict[str, str]:
        """Map each account to a colour: digits stripped, matched against the colour map and qtopconf regexes.

        User regexes override the colour map; the last matching rule wins. Unmatched accounts stay uncoloured.
        """
        pattern = {}
        PATTERNS_LAST_FIRST = list(mapping.keys())[::-1]
        # Compiled on first use, so an invalid pattern still fails exactly when it is first reached.
        compiled: Dict[str, Any] = {}
        # Preserve shared-ID color precedence from the previous job-count order.
        for line in sorted(self.account_jobs_table, key=itemgetter(3, 4), reverse=True):
            uid, user = line[0], line[4]
            # Single-expression fallback, portable to py3.6 / PyPy / MicroPython (no walrus, no findall).
            # The appended " NoPattern" sentinel guarantees a match: an all-numeric user (e.g. a numeric
            # scheduler-provided uid) yields "NoPattern". The space is load-bearing: without it "abc" -> "abcNoPattern".
            account_match = ACCOUNT_LETTERS.search(user + " NoPattern")
            account_letters = account_match.group(0) if account_match else "NoPattern"
            for re_account in PATTERNS_LAST_FIRST:
                rx = compiled.get(re_account)
                if rx is None:
                    rx = compiled[re_account] = re.compile(re_account)
                match = rx.search(user)
                if match is not None:
                    account_letters = re_account  # colors the text according to the regex given by the user in qtopconf
                    break

            pattern[str(uid)] = account_letters if account_letters in mapping else "NoPattern"

        pattern.update(_reserved_pattern_mappings(self.config))
        return pattern

    def find_matrices_width(self, DEADWEIGHT: int = 11) -> Tuple[int, int, int]:
        """Return the first and last node columns of the first matrix and the number of extra matrices.

        With -N masking, leading empty nodes (e.g. the first 129 when numbering starts at 130) are skipped.
        DEADWEIGHT is the width of the {__XXXX__} labels right of the core matrix.
        """
        start = 0
        wn_number = self.cluster.highest_wn
        workernode_list = self.cluster.workernode_list
        term_columns = self.MVU_model.viewport.h_term_size
        min_masking_threshold = int(self.config["workernodes_matrix"][0]["wn id lines"]["min_masking_threshold"])
        if not wn_number or not workernode_list:
            return 0, 0, 0

        numeric_workernodes = [wn for wn in workernode_list if isinstance(wn, int)]
        first_workernode = min(numeric_workernodes) if numeric_workernodes else 1
        if not self.MVU_model.args.NOMASKING and first_workernode > min_masking_threshold:  # -m keeps them
            # exclude unneeded first empty nodes from the matrix
            start = first_workernode - 1

        # Extra matrices may be needed if the WNs are more than the screen width can hold.
        if wn_number > start:  # start will either be 1 or (masked >= config['min_masking_threshold'] + 1)
            extra_matrices_nr = int(ceil(abs(wn_number - start) / float(term_columns - DEADWEIGHT))) - 1
        elif self.MVU_model.args.REMAP:  # was: ***wn_number < start*** and len(cluster.node_name_prefixes) > 1:  # Remapping
            extra_matrices_nr = int(ceil(wn_number / float(term_columns - DEADWEIGHT))) - 1
        else:
            extra_matrices_nr = 0

        if self.config["USER_CUT_MATRIX_WIDTH"]:  # if the user defines a custom cut (in the configuration file)
            stop = start + self.config["USER_CUT_MATRIX_WIDTH"]
            self.extra_matrices_nr = wn_number / self.config["USER_CUT_MATRIX_WIDTH"]
        elif extra_matrices_nr:  # if more matrices are needed due to lack of space, cut every matrix so that if fits to screen
            stop = start + term_columns - DEADWEIGHT
        else:  # just one matrix, small cluster!
            stop = start + wn_number
            extra_matrices_nr = 0

        logging.debug("reported term_columns, DEADWEIGHT: %s\t%s" % (term_columns, DEADWEIGHT))
        logging.debug("reported start/stop lengths: %s--> %s" % (start, stop))
        return start, stop, extra_matrices_nr

    def calc_all_wnid_label_lines(self, NAMED_WNS: Any) -> Dict[str, Any]:  # (total_wn) in case of multiple cluster.node_name_prefixes
        """Return the vertical worker-node ID label lines, one per digit, e.g. for hundreds of nodes.

        '1': "00000000..."
        '2': "0000000001111111..."
        '3': "12345678901234567..."
        """
        highest_wn = self.cluster.highest_wn
        if NAMED_WNS:  # or args.FORCE_NAMES
            workernode_dict = self.cluster.workernode_dict
            hosts = [state_corejob_dn["host"] for _, state_corejob_dn in workernode_dict.items()]
            node_str_width = len(max(hosts, key=len, default=""))
            wn_vert_labels: Dict[str, Any] = OrderedDict((str(place), []) for place in range(1, node_str_width + 1))
            for node in workernode_dict:
                host = workernode_dict[node]["host"]
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

        for wn in list(wn_vert_labels.keys()):
            wn_vert_labels[wn] = "".join(wn_vert_labels[wn])
        return wn_vert_labels

    def calc_general_mult_attr_line(self, part_name: str, yaml_key: str, config: Dict[str, Any], backend: Any) -> Dict[str, Any]:  # NEW
        """Return the per-node attribute lines (state, queue, ...) for a workernodes_matrix part, cropped to max_len."""
        elem_identifier = [d for d in config["workernodes_matrix"] if part_name in d][0]  # jeeez
        part_name_idx = config["workernodes_matrix"].index(elem_identifier)
        user_max_len = int(config["workernodes_matrix"][part_name_idx][part_name]["max_len"])
        if not self.cluster.workernode_dict:
            return OrderedDict()
        try:
            real_max_len = max(len(self.cluster.workernode_dict[_node][yaml_key]) for _node in self.cluster.workernode_dict)
        except KeyError as error:
            raise OperationalError(
                "%s lines in the matrix are not supported for %s systems; remove them from the configuration." % (part_name, self.MVU_model.scheduler)
            ) from error
        min_len = min(user_max_len, real_max_len)
        if real_max_len > user_max_len:
            logging.warning("Some longer %(attr)ss have been cropped due to %(attr)s length restriction by user" % {"attr": part_name})

        # initialisation of lines
        multiline_map: Dict[str, Any] = OrderedDict()
        for line_nr in range(1, min_len + 1):
            multiline_map["attr%sline" % str(line_nr)] = []

        for _node in self.cluster.workernode_dict:
            node_attrs = self.cluster.workernode_dict[_node]
            # distribute state, qname etc to lines
            attribute = list(node_attrs[yaml_key][:min_len])
            marker = backend.node_state_marker(node_attrs)
            if yaml_key == "state" and marker and attribute:
                attribute[0] = utils.ColorStr(marker, color=attribute[0].color)
            attribute.extend(" " for _ in range(min_len - len(attribute)))
            for attr_line, ch in zip(multiline_map, attribute):
                if ch == " ":
                    ch = utils.ColorStr(" ")
                elif ch == "?":
                    ch = utils.ColorStr("?", color="Gray_D")
                multiline_map[attr_line].append(ch)
        return multiline_map

    def _calc_core_matrix(self, user_to_id: Dict[str, Any], job_to_user_and_queue: Mapping[str, Tuple[str, str]]) -> Dict[str, Any]:
        core_user_map: Dict[str, Any] = OrderedDict()

        core_coloring = self.MVU_model.display_overrides.get("core_coloring", self.config["core_coloring"])

        for core_nr in self.cluster.core_span:
            core_user_map["Core%svector" % str(core_nr)] = []  # Cpu0vector, Cpu1vector, Cpu2vector, ... = [],[],[], ...
        # The same lists by core position, so filling a cell needs no key built per core.
        core_vectors = list(core_user_map.values())

        for _node in self.cluster.workernode_dict:
            self._fill_node_cores_vector(_node, core_vectors, job_to_user_and_queue, core_coloring)

        return core_user_map

    def _fill_node_cores_vector(
        self,
        _node: Any,
        core_vectors: Sequence[List[Any]],
        job_to_user_and_queue: Mapping[str, Tuple[str, str]],
        core_coloring: str,
    ) -> None:
        """Append one node's column of the core matrix (core_vectors[i] is core line i) with job or status symbols.

        Nodes with fewer cores than the largest one are padded with non_existent_node_symbol.
        """
        node = self.cluster.workernode_dict[_node]
        state = node["state"]
        corejobs = node.get("core_job_map", dict())
        non_existent_node_symbol = self.config["non_existent_node_symbol"]
        gray_hash = utils.ColorStr(non_existent_node_symbol, color="Gray_D")
        gray_underscore = utils.ColorStr("_", color="Gray_D")

        if state == "?":  # for non-existent machines
            for vector in core_vectors:
                vector.append(gray_hash)
        else:
            np = int(node["np"])
            for core in self.color_cores_and_return_unused(np, core_vectors, corejobs, core_coloring, job_to_user_and_queue):
                core_vectors[core].append(gray_underscore)
            for vector in core_vectors[np:]:  # core lines beyond this node's cores
                vector.append(gray_hash)

    @staticmethod
    def get_hl_q_or_users(_highlighted_queues_or_users: Iterable[Mapping[str, Any]]) -> Iterator[Tuple[Any, str, Callable[..., bool]]]:
        """Yield (user or queue, highlight kind, all/any) for each highlight rule."""
        for selection_users_queues in _highlighted_queues_or_users:
            selection = list(selection_users_queues.keys())[0]
            users_queues = selection_users_queues[selection]
            and_or, highlight_kind = selection.rsplit("include_")
            and_or_func = all if not and_or else any

            for user_queue in users_queues:
                yield user_queue, highlight_kind, and_or_func

    def color_cores_and_return_unused(
        self, np: int, core_vectors: Sequence[List[Any]], corejobs: Mapping[Any, Any], _core_coloring: str, job_to_user_and_queue: Mapping[str, Tuple[str, str]]
    ) -> List[int]:
        """Colour each busy core of an np-core node by user or queue, as core_coloring says; return the free core positions in order."""
        free = bytearray(b"\x01") * np
        # Valid core keys are 0..np-1 as int or as their str(); anything else raises as before.
        positions: Dict[str, int] = self.__dict__.setdefault("_core_positions", {})  # per frame: lives on this WNOccupancy
        if len(positions) < np:
            positions.update((str(core), core) for core in range(len(positions), np))

        # One cell per (user, queue) per frame: every input to its colour is frame-constant,
        # and cells are never modified after creation, so cores of the same job share it.
        cells = self.__dict__.setdefault("_core_cells", {})  # per frame: lives on this WNOccupancy
        for core, job in corejobs.items():  # one pass: each assigned core resolves its job once
            position = core if type(core) is int else positions.get(str(core), np)
            if not 0 <= position < np or not free[position]:  # an invalid or repeated core
                raise ValueError("list.remove(x): x not in list")
            free[position] = 0
            resolved = self._resolve(str(job), job_to_user_and_queue)
            user_queue = job_to_user_and_queue.get(resolved)
            if user_queue is None:  # as str(KeyError(resolved)) in the former second pass
                logging.warning("Scheduler node allocation references an unlisted job (ID %r). Please check with the SysAdmin.", resolved)
                cell = utils.ColorStr("?", color="Yellow")
            else:
                cell = cells.get(user_queue)
                if cell is None:
                    cell = cells[user_queue] = self._core_cell(user_queue[0], user_queue[1], _core_coloring)
            core_vectors[position].append(cell)

        return [core for core in range(np) if free[core]]

    def _core_cell(self, user: str, queue: str, _core_coloring: str) -> "utils.ColorStr":
        """Build the coloured matrix cell for a user's job in a queue."""
        selected_pat_to_color_map = getattr(self.MVU_model, _core_coloring)
        highlight = self.__dict__.get("_highlight")  # per frame: lives on this WNOccupancy
        if highlight is None:
            _highlighted_queues_or_users = self.MVU_model.display_overrides.get("highlight", self.config["highlight"])
            highlight_rules = list(WNOccupancy.get_hl_q_or_users(_highlighted_queues_or_users))
            if any(kind == "user_id" for _, kind, _ in highlight_rules):  # user IDs become user names once
                id_to_user = dict(zip((str(x) for x in self.user_to_id.values()), self.user_to_id.keys()))
                highlight_rules = [(id_to_user[rule] if kind == "user_id" else rule, kind, func) for rule, kind, func in highlight_rules]
            highlight = self._highlight = (_highlighted_queues_or_users, highlight_rules, highlight_rules[-1][2] if highlight_rules else any)
        _highlighted_queues_or_users, highlight_rules, and_or_func = highlight
        id_: utils.ColorStr = utils.ColorStr.from_other_color_str(self.user_to_id[user])
        viewed_pattern = self.user_color_patterns[str(id_)] if _core_coloring == "user_to_color" else queue
        matches = []
        for user_queue_to_highlight, highlight_kind, _ in highlight_rules:
            if highlight_kind == "user_pat":
                actual_user_queue = user
            elif highlight_kind == "user_id":
                actual_user_queue = user
            elif highlight_kind == "queue":
                actual_user_queue = queue
            matches.append(re.match(user_queue_to_highlight, actual_user_queue))
        if not _highlighted_queues_or_users or and_or_func(match.group(0) if match is not None else None for match in matches):
            id_.color = selected_pat_to_color_map.get(viewed_pattern, "White")  # queue or user decided on runtime
        else:
            id_.color = "Gray_D"
        id_.q = queue
        return id_

    def _resolve(self, job: str, job_to_user_and_queue: "Mapping[str, Tuple[str, str]]") -> str:
        """resolve_job_id, memoized per frame: cores of one job resolve once, not once per core."""
        memo: Dict[str, str] = self.__dict__.setdefault("_resolved_jobs", {})  # per frame: lives on this WNOccupancy
        resolved = memo.get(job)
        if resolved is None:
            resolved = memo[job] = resolve_job_id(job, job_to_user_and_queue)
        return resolved

    def is_matrix_coreless(self, node_column_start: int, node_column_stop: int) -> bool:
        """Return True if every core line of this matrix slice would be removed as empty."""
        non_existent_symbol = self.config["non_existent_node_symbol"]
        lines = 0
        core_user_map = self.core_user_map
        remove_corelines = self.MVU_model.display_overrides.get("rem_empty_corelines", self.config["rem_empty_corelines"]) + 1

        for _core_x_vector, _ind, _k, is_corevector_removable in gauge_core_vectors(
            core_user_map, node_column_start, node_column_stop, WNOccupancy.coreline_notthere_or_unused, non_existent_symbol, remove_corelines
        ):
            if is_corevector_removable:
                lines += 1
        return lines == len(core_user_map)

    def MVU_check_jobs(self, cluster: "Cluster") -> Optional[str]:
        """Return a mismatch message if the running jobs counted on nodes differ from the scheduler's total."""
        states = self.config["state_abbreviations"][self.MVU_model.scheduler]
        running_ids = {job_id for job_id, job in self.snapshot.jobs_dict.items() if states.get(job.job_state) == "running_of_user"}
        counted_jobs = WNOccupancy._count_jobs_strict(self.snapshot.worker_nodes, running_ids, self.MVU_model.MVU_node_job_ids)
        if counted_jobs != cluster.total_running_jobs:
            return "Counted jobs (%s) -- Total running jobs reported (%s) MISMATCH!" % (counted_jobs, cluster.total_running_jobs)
        return None

    @staticmethod
    def _count_jobs_strict(worker_nodes: Iterable[Mapping[str, Any]], job_ids: Optional[Collection[str]] = None, node_job_ids: Callable[..., Set[str]] = node_job_ids) -> int:
        jobs = set()
        for node in worker_nodes:
            jobs.update(node_job_ids(node))
        jobs.discard("?")
        if job_ids is not None:
            jobs = {resolve_job_id(job, job_ids) for job in jobs}
        return len(jobs if job_ids is None else jobs.intersection(job_ids))

    def calculate_user_node_use(self, cluster: "Cluster", job_to_user_and_queue: Mapping[str, Tuple[str, str]]) -> "Counter[str]":
        """This calculates the number of nodes each user has jobs in (shown in User accounts and pool mappings)."""
        user_machines = []
        for node in cluster.workernode_dict.values():
            node["node_user_set"] = {job_to_user_and_queue[job][0] for job in node["node_job_set"] if job in job_to_user_and_queue}
            user_machines.extend(node["node_user_set"])

        return Counter(user_machines)

    @staticmethod
    def coreline_not_there(symbol: str, switch: int, delta: int, core_x_str: str) -> bool:
        """Return True if a core line has only non-existent cores, e.g. core 32 on a matrix of 24-core nodes."""
        return switch == 2 and ((symbol * delta == core_x_str) or (symbol * len(core_x_str) == core_x_str))

    @staticmethod
    def coreline_unused(symbol: str, switch: int, print_length: int, core_x_str: str) -> bool:
        """Checks if a line consists of either not-really-there cores or unused cores."""
        all_symbols_in_line = set(core_x_str)
        unused_symbols = set(["_", symbol])
        only_unused_symbols_in_line = all_symbols_in_line.issubset(unused_symbols)
        return switch == 3 and only_unused_symbols_in_line and (print_length == len(core_x_str))

    @staticmethod
    def coreline_notthere_or_unused(symbol: str, switch: int, delta: int, core_x_str: str) -> bool:
        """Return True if a core line is non-existent or unused, so it may be removed."""
        return WNOccupancy.coreline_not_there(symbol, switch, delta, core_x_str) or WNOccupancy.coreline_unused(symbol, switch, delta, core_x_str)


class Cluster(object):
    """Cluster-wide node statistics, numbering and remapping derived from a snapshot."""

    def __init__(self, snapshot: Any, worker_nodes: List[Dict[str, Any]], WNFilter: Any, config: Dict[str, Any], args: Any, MVU_model: Any) -> None:
        self.MVU_model = MVU_model
        self.worker_nodes = worker_nodes
        self.jobs_dict = snapshot.jobs_dict
        self.queues_dict = snapshot.queues_dict
        self.total_running_jobs = snapshot.total_running_jobs
        self.total_queued_jobs = snapshot.total_queued_jobs
        self.config = config
        self.args = args

        self.working_cores = 0
        self.offdown_nodes = 0
        self.total_cores = 0
        self.core_span: List[str] = []
        self.highest_wn = 0
        self.workernode_dict: Dict[Any, Dict[str, Any]] = {}

        self.available_wn = sum([len(node["state"]) for node in self.worker_nodes if str(node["state"][0]) == "-"])
        self.total_wn = len(self.worker_nodes)  # == existing_nodes
        self.workernode_list: Sequence[Any] = []

        self.WNFilter = WNFilter
        self.wn_filter: Any = None

        self.analyse()

    def analyse(self) -> "Cluster":
        """Derive node numbers, prefixes, down nodes, working cores and remapping from the worker nodes."""
        if not self.worker_nodes:
            return self

        re_nodename = r"(^[A-Za-z0-9_-]+)(?=\.|$)" if not self.args.ANONYMIZE else r"\w_anon_wn_\d+"

        node_name_prefixes, self.workernode_list, self.offdown_nodes, self.working_cores, max_np, _all_str_digits_with_empties = self.get_wn_list_and_stats(
            self.workernode_list, set(), self.worker_nodes, re_nodename
        )

        self.core_span = [str(x) for x in range(max_np)]
        self.args.REMAP = self.decide_remapping(_all_str_digits_with_empties, node_name_prefixes)

        remapped_node_numbers = range(1, self.total_wn + 1)
        nodes_drop, workernode_dict, remapped_nodes = self.map_worker_nodes_to_wn_dict(self.args.REMAP)
        self.workernode_dict = workernode_dict

        if self.args.REMAP:
            self.total_wn += nodes_drop
            self.highest_wn = self.total_wn

            nodes_drop_slice_end = nodes_drop if nodes_drop else None
            self.workernode_list = remapped_node_numbers[:nodes_drop_slice_end]
            self.workernode_dict = remapped_nodes
        else:
            self.highest_wn = max(self.workernode_list)
            self.workernode_dict = self.fill_non_existent_wn_nodes(self.workernode_dict)

        self.workernode_dict = self.do_name_remapping(self.workernode_dict)
        self._calculate_jobs_per_node(self.workernode_dict)
        return self

    def get_wn_list_and_stats(
        self, workernode_list: Any, node_name_prefixes: Set[str], worker_nodes: Sequence[Any], re_nodename: str
    ) -> Tuple[Set[str], Any, int, int, int, List[str]]:
        """Return the node name prefixes, node numbers, down-node and core counts, the largest core count and node digits."""
        max_np = 0
        all_str_digits_with_empties = list()
        for node in worker_nodes:
            nodename_match = re.search(re_nodename, node["domainname"])
            if nodename_match is None:
                raise ValueError("Unsupported worker node name %r" % node["domainname"])
            _nodename = nodename_match.group(0)

            # get subclusters by name change
            _node_letters = "".join(re.findall(r"\D+", _nodename))
            node_name_prefixes.update([_node_letters])

            node_str_digits = "".join(re.findall(r"\d+", _nodename))
            all_str_digits_with_empties.append(node_str_digits)

            self.total_cores += int(node.get("np"))  # for stats only
            max_np = max(max_np, int(node["np"]))
            self.offdown_nodes += 1 if "".join(([n.str for n in node["state"]])) in "do" else 0
            self.working_cores += len(node.get("core_job_map", dict()))

            _cur_node_nr: Union[int, str]
            try:
                _cur_node_nr = int(node_str_digits)
            except ValueError:
                _cur_node_nr = _nodename

            # create workernode_list
            workernode_list.append(_cur_node_nr)

        return node_name_prefixes, workernode_list, self.offdown_nodes, self.working_cores, max_np, all_str_digits_with_empties

    def decide_remapping(self, all_str_digits_with_empties: List[str], node_name_prefixes: Set[str]) -> bool:
        """Return True if node numbers must be remapped to consecutive positions.

        Remapping is enforced when:
        - the user asks for it (blindremap switch) or worker-node filters are active;
        - nodes have different name prefixes, e.g. wn001 and ps001;
        - numbering starts so high that most of the matrix would be empty;
        - more than PERCENTAGE*nodes have no jobs assigned;
        - numbers collide (ps001 and wn001, or 0x001 and 0x1, all at position 1), or nodes are unnumbered.

        One or two unnumbered nodes should not be enough (intended future behaviour: append them at the end).
        """
        if not self.total_wn:  # if nothing is running on the cluster
            return False

        _all_str_digits = list(filter(lambda x: x != "", all_str_digits_with_empties))
        _all_digits = [int(digit) for digit in _all_str_digits]
        numeric_workernodes = [wn for wn in self.workernode_list if isinstance(wn, int)]
        has_mixed_or_non_numeric_wns = len(numeric_workernodes) != len(self.workernode_list)
        has_exotic_starting_wn = bool(numeric_workernodes) and min(numeric_workernodes) >= int(self.config["exotic_starting_wn_nr"])

        user_filters = self.MVU_model.display_overrides.get("filtering", self.config["filtering"])
        if (
            self.args.BLINDREMAP
            or (user_filters and user_filters[0])
            or len(node_name_prefixes) > 1
            or has_exotic_starting_wn
            or self.offdown_nodes >= self.total_wn * float(self.config["percentage"])
            or len(all_str_digits_with_empties) != len(_all_str_digits)
            or len(_all_digits) != len(_all_str_digits)
            or has_mixed_or_non_numeric_wns
        ):
            REMAP = True
        else:
            REMAP = False
        logging.info("Blind Remapping [user selected]: %s,\n\t\t\t\t\t\t\t\t  Decided Remapping: %s" % (self.args.BLINDREMAP, REMAP))

        if logging.getLogger().isEnabledFor(logging.DEBUG) and REMAP:
            user_request = "The user has requested it (blindremap switch)" if self.args.BLINDREMAP else False

            subclusters = "there are different WN namings, e.g. wn001, wn002, ..., ps001, ps002, ... etc" if len(node_name_prefixes) > 1 else False

            exotic_starting = "first starting numbering of a WN very high; would thus require too much unused space" if has_exotic_starting_wn else False

            percentage_unassigned = (
                "more than %s of nodes have are down/offline" % float(self.config["percentage"]) if len(all_str_digits_with_empties) != len(_all_str_digits) else False
            )

            numbering_collisions = "there are numbering collisions or non-numbered WNs" if has_mixed_or_non_numeric_wns else False

            logging.debug("Remapping decided due to: \n\t %s" % list(filter(None, [user_request, subclusters, exotic_starting, percentage_unassigned, numbering_collisions])))

        return REMAP

    def do_name_remapping(self, workernode_dict: Dict[Any, Dict[str, Any]]) -> Dict[Any, Dict[str, Any]]:
        """Renames hostnames according to user remapping in conf file (for the wn id label lines)."""
        label_max_len = int(self.config["workernodes_matrix"][0]["wn id lines"]["max_len"])
        for _, state_corejob_dn in workernode_dict.items():
            _host = state_corejob_dn["domainname"].split(".", 1)[0]
            changed = False
            for remap_line in self.config["remapping"]:
                pat, repl = list(remap_line.items())[0]
                if isinstance(repl, str) and repl.strip().startswith("lambda"):
                    raise ValueError("lambda remapping is no longer supported in %s because eval is disabled" % QTOPCONF_YAML)
                if re.search(pat, _host):
                    changed = True
                    state_corejob_dn["host"] = _host = re.sub(pat, repl, _host)
            state_corejob_dn["host"] = _host if not changed else state_corejob_dn["host"]
            # label_max_len was formerly the wn_labels_max_len setting
            state_corejob_dn["host"] = state_corejob_dn["host"][-label_max_len:] if label_max_len else state_corejob_dn["host"]
        return workernode_dict

    def fill_non_existent_wn_nodes(self, workernode_dict: Dict[Any, Dict[str, Any]]) -> Dict[Any, Dict[str, Any]]:
        """Fill in non-existent WN nodes (absent from input files) with default values and count them."""
        for node in range(1, self.highest_wn + 1):
            if node not in workernode_dict:
                workernode_dict[node] = {"state": "?", "np": 0, "domainname": "N/A", "host": "N/A", "core_job_map": {}}
                default_values_for_empty_nodes = dict(
                    [(yaml_key, "?") for yaml_key, part_name, _ in yaml_parser.get_yaml_key_part(self.config, self.MVU_model.scheduler, outermost_key="workernodes_matrix")]
                )
                workernode_dict[node].update(default_values_for_empty_nodes)
        return dict(sorted(workernode_dict.items()))  # matrix columns follow node numbers, not discovery order

    def map_worker_nodes_to_wn_dict(self, args_remap: bool) -> Tuple[int, Dict[Any, Dict[str, Any]], Dict[int, Dict[str, Any]]]:
        """Apply active filters and sorting, then build original and compact node mappings."""
        nodes_drop = 0  # count change in nodes after filtering
        workernode_dict = dict()
        remapped_nodes = dict()
        _sorting_from_conf = self.config["sorting"]
        user_sorting = self.MVU_model.display_overrides.get("user_sort", (_sorting_from_conf and list(_sorting_from_conf.values())[0]))
        user_filters = self.MVU_model.display_overrides.get("filtering", self.config["filtering"])
        user_filtering = user_filters and user_filters[0]

        if user_filtering and args_remap:
            len_wn_before = len(self.worker_nodes)
            self.wn_filter = self.WNFilter(self.worker_nodes)
            self.worker_nodes, self.offdown_nodes, self.available_wn, self.working_cores, self.total_cores = self.wn_filter.filter_worker_nodes(
                self.offdown_nodes, self.available_wn, self.working_cores, self.total_cores, filter_rules=user_filters
            )
            len_wn_after = len(self.worker_nodes)
            nodes_drop = len_wn_after - len_wn_before

        node_numbers = {id(node): number for node, number in zip(self.worker_nodes, self.workernode_list)}  # before sorting reorders them
        if user_sorting:
            self.worker_nodes = self._sort_worker_nodes()

        for idx, batch_node in enumerate(self.worker_nodes):
            # Without remapping a node keeps its own number as its column; remapping numbers the (sorted, filtered) nodes in turn.
            workernode_dict[node_numbers.get(id(batch_node), idx)] = batch_node
            remapped_nodes[idx] = batch_node

        return nodes_drop, workernode_dict, remapped_nodes

    def _sort_worker_nodes(self) -> List[Dict[str, Any]]:
        order: Dict[str, Callable[[Dict[str, Any]], Any]] = {
            "sort by nodename-notnum": lambda node: re.sub(r"[^A-Za-z _.-]+", "", node["domainname"]) or "0",
            "sort by nodename-notnum length": lambda node: len(node["domainname"].split(".", 1)[0].split("-")[0]),
            "sort by all numbers": lambda node: int(re.sub(r"[A-Za-z _.-]+", "", node["domainname"]) or "0"),
            "sort by first letter": lambda node: ord(node["domainname"][0]),
            "sort by node state": lambda node: ord(str(node["state"][0])),
            "sort by nr of cores": lambda node: int(node["np"]),
            "sort by core occupancy": lambda node: len(node["core_job_map"]),
            "sort reset": lambda node: 0,
        }
        if self.MVU_model.display_overrides.get("user_sort"):  # live user sorting overrides yaml config sorting
            sort_labels = [k[0] for k in self.MVU_model.display_overrides.get("user_sort", [])]
        elif self.config.get("sorting", {}).get("user_sort"):
            sort_labels = self.config["sorting"]["user_sort"]
        else:
            return self.worker_nodes

        def sort_key(node: Dict[str, Any]) -> Tuple[Any, ...]:
            values = []
            for label in sort_labels:
                if label == "sort by custom definition":
                    raise ValueError("custom Python sorting expressions are no longer supported because eval is disabled")
                values.append(order[label](node))
            return tuple(values)

        try:
            return sorted(self.worker_nodes, key=sort_key, reverse=self.config["sorting"]["reverse"])
        except (IndexError, ValueError):
            logging.critical("There's (probably) something wrong in your sorting configuration in %s." % QTOPCONF_YAML)
            raise
        except KeyError as e:
            msg = "Worker Nodes don't contain '%s' as a key." % str(e)
            logging.error(self.MVU_model.MVU_colorize(msg, color_func="Red_L"))
        except NameError as e:
            msg = "Wrong input '%s'. Please check the examples in qtopconf.yaml." % str(e)
            logging.error(self.MVU_model.MVU_colorize(msg, color_func="Red_L"))

        return self.worker_nodes

    def _calculate_jobs_per_node(self, workernode_dict: Dict[Any, Dict[str, Any]]) -> None:
        for node in workernode_dict:
            node_job_set = self.MVU_model.MVU_node_job_ids(workernode_dict[node])
            workernode_dict[node]["node_job_set"] = node_job_set


class WNFilter(object):
    """Apply the worker-node filters (by queue, number, state or name pattern) by marking and keeping nodes."""

    def __init__(self, worker_nodes: List[Any]) -> None:
        self.worker_nodes = worker_nodes

    def mark_list_by_queue(self, nodes: List[Any], arg_list: Optional[Sequence[str]] = None) -> List[Any]:
        """Mark the nodes that serve any of the given queues."""
        arg_list = arg_list or []
        for _idx, node in enumerate(nodes[:]):
            if set(arg_list) & {x.str for x in node["qname"]}:
                node["mark"] = "*"
        return nodes

    def mark_list_by_number(self, nodes: List[Any], arg_list: Optional[Sequence[str]] = None) -> List[Any]:
        """Mark the nodes at the given positions."""
        arg_list = arg_list or []
        for idx, node in enumerate(nodes):
            if str(idx) in arg_list:
                node["mark"] = "*"
        return nodes

    def mark_list_by_node_state(self, nodes: List[Any], arg_list: Optional[Sequence[str]] = None) -> List[Any]:
        """Mark the nodes in any of the given states."""
        arg_list = arg_list or []
        for node in nodes:
            if {"".join(state.str for state in node["state"])} & set(arg_list):
                node["mark"] = "*"
        return nodes

    def mark_list_by_name_pattern(self, nodes: List[Any], arg_list: Optional[Sequence[str]] = None) -> List[Any]:
        """Mark the nodes whose short name matches any of the given regexes."""
        arg_list = arg_list or []
        for node in nodes:
            patterns = next(iter(arg_list.values())) if isinstance(arg_list, dict) else arg_list
            for pattern in patterns:
                match = re.search(pattern, node["domainname"].split(".", 1)[0])
                if match is not None:
                    node["mark"] = "*"
        return nodes

    def keep_marked(self, t: Iterable[Any], rule: Any, final_pass: bool = False) -> Iterable[Any]:
        """Keep the marked nodes and clear their marks; or_ rules apply only on the final pass."""
        if (rule.startswith("or_") and not final_pass) or (not rule.startswith("or_") and final_pass):
            return t
        nodes = list(filter(lambda item: item.get("mark"), t))
        for item in nodes:
            if item.get("mark"):
                del item["mark"]
        return nodes

    def keep_unmarked(self, t: Iterable[Any], rule: Any, final_pass: bool = False) -> Iterable[Any]:
        """Keep the nodes that are not marked."""
        return filter(lambda item: not item.get("mark"), t)

    def filter_worker_nodes(
        self, offdown_nodes: int, avail_nodes: int, working_cores: int, total_cores: int, filter_rules: Optional[List[Any]] = None
    ) -> Tuple[Any, int, int, int, int]:
        """Keeps specific nodes according to the filter rules in QTOPCONF_YAML."""
        filter_types = {
            "exclude_numbers": (self.mark_list_by_number, self.keep_unmarked),
            "exclude_node_states": (self.mark_list_by_node_state, self.keep_unmarked),
            "exclude_name_patterns": (self.mark_list_by_name_pattern, self.keep_unmarked),
            "or_include_numbers": (self.mark_list_by_number, self.keep_marked),
            "or_include_node_states": (self.mark_list_by_node_state, self.keep_marked),
            "or_include_name_patterns": (self.mark_list_by_name_pattern, self.keep_marked),
            "include_queues": (self.mark_list_by_queue, self.keep_marked),
            "include_numbers": (self.mark_list_by_number, self.keep_marked),
            "include_node_states": (self.mark_list_by_node_state, self.keep_marked),
            "include_name_patterns": (self.mark_list_by_name_pattern, self.keep_marked),
        }

        if filter_rules:
            if WNFilter.report_filtered_view.count() < 2:
                WNFilter.report_filtered_view()
            nodes = self.worker_nodes[:]
            for filter_rule in filter_rules:
                rule, args = list(filter_rule.items())[0]
                mark_func, keep = filter_types[rule]
                nodes = mark_func(nodes, args)
                nodes = list(keep(nodes, rule))
            nodes = list(keep(nodes, rule, final_pass=True))
            if len(nodes):
                self.worker_nodes = list(dict((v["domainname"], v) for v in nodes).values())
                offdown_nodes = sum([1 if "".join(([n.str for n in node["state"]])) in "do" else 0 for node in self.worker_nodes])
                avail_nodes = self.available_wn = sum([len(node["state"]) for node in self.worker_nodes if str(node["state"][0]) == "-"])
                working_cores = sum(len(node.get("core_job_map", dict())) for node in self.worker_nodes)
                total_cores = sum(int(node.get("np")) for node in self.worker_nodes)
            else:
                logging.error("Selected filter results in empty worker node set. Cancelling.")

        return self.worker_nodes, offdown_nodes, avail_nodes, working_cores, total_cores

    @staticmethod
    @utils.CountCalls
    def report_filtered_view() -> None:
        """Log that the occupancy view is filtered."""
        logging.error("*** WN Occupancy view is filtered.")


MVU_ViewData = namedtuple("MVU_ViewData", "model cluster occupancy")


def MVU_query(model: Any) -> MVU_ViewData:
    """Build private derived nodes/layout; never mutate the canonical snapshot or input model."""
    render_model = copy.copy(model)
    render_model.args = copy.copy(model.args)
    render_model.viewport = copy.copy(model.viewport)
    render_model.transposed_sections = []
    # The transforms build new per-node dicts; nested node values are only read downstream
    # (checked by test_MVU_query_and_view_need_no_runtime_globals_or_mutable_snapshot), so no deep copy.
    nodes = MVU_keep_queue_initials_only_and_colorize(model.snapshot.worker_nodes, model.queue_to_color)
    nodes = MVU_colorize_nodestate(nodes, model.nodestate_to_color)
    cluster = Cluster(model.snapshot, nodes, WNFilter, model.config, render_model.args, MVU_model=render_model)
    occupancy = WNOccupancy(cluster, model.config, model.snapshot, model.user_to_color, MVU_model=render_model)
    return MVU_ViewData(render_model, cluster, occupancy)
