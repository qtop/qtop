## Copyright (c) 2026 qtop contributors
## SPDX-License-Identifier: MIT
"""Terminal presentation of an explicit MVU view_data."""

import datetime
import logging
import os
import subprocess
import tempfile
from collections import OrderedDict
from functools import lru_cache, partial
from itertools import cycle, zip_longest
from typing import TYPE_CHECKING, Any, Callable, Dict, Iterable, Iterator, List, Mapping, Optional, Sequence, Tuple, overload

if TYPE_CHECKING:
    from qtop_py.state_types import Config

from qtop_py import __version__, utils, yaml_parser
from qtop_py.constants import DEFAULT_USER_ACCOUNTS_HEADER, DEFAULT_USER_ID_RANKING
from qtop_py.mvu import MVU_compress_colored_line as compress_colored_line
from qtop_py.mvu import MVU_gauge_core_vectors as gauge_core_vectors
from qtop_py.mvu_query import WNOccupancy
from qtop_py.safety import visible_text

# Accounts-table state columns when accounts_states is unset: jobs >= R + Q.
DEFAULT_ACCOUNT_STATES = ("running_of_user", "queued_of_user")


def user_accounts_header(config: "Config") -> str:
    """Expand user_accounts_header; {ranking} becomes the active user_id_ranking. Plain replace: no format-string evaluation."""
    USER_ACCOUNTS_HEADER: Optional[str] = config.get("user_accounts_header")
    if USER_ACCOUNTS_HEADER is None:
        USER_ACCOUNTS_HEADER = DEFAULT_USER_ACCOUNTS_HEADER
    USER_ID_RANKING = config.get("user_id_ranking") or DEFAULT_USER_ID_RANKING
    return visible_text(USER_ACCOUNTS_HEADER.replace("{ranking}", ", ".join(USER_ID_RANKING)), escape_backslashes=False)


DEMO_WATCH_KEYS = "Keys: q quit, ? help, m colour, t transpose, H highlight, f filter, s sort, hjkl scroll"
BOLD = "\033[1m%s\033[0m"


class TextDisplay(object):
    """Render a frame's sections: job accounting summary, worker-node occupancy and user accounts."""

    def __init__(self, snapshot: Any, config: "Config", viewport: Any, wns_occupancy: Any, cluster: Any, args: Any, MVU_model: Any) -> None:
        self.MVU_model = MVU_model
        self.cluster = cluster
        self.snapshot = snapshot
        self.viewport = viewport
        self.config = config
        self.wns_occupancy = wns_occupancy
        self.args = args
        # A cell's escape sequence depends only on (char, colour) within a frame: compute each pair once.
        self._cell_text = lru_cache(maxsize=None)(lambda char, color: MVU_model.MVU_colorize(char, color_func=color))

    def display_selected_sections(self, _savepath: str, SAMPLE_FILENAME: str, QTOP_LOGFILE: str) -> None:
        """Print the sections listed in user_display_parts, minus those disabled with -1, -2 or -3."""
        sections_off = {  # cmdline argument -n
            1: self.args.sect_1_off,
            2: self.args.sect_2_off,
            3: self.args.sect_3_off,
        }
        display_parts: Dict[str, Tuple[Callable[..., Any], Tuple[Any, ...]]] = {
            "job_accounting_summary": (self.display_job_accounting_summary, (self.cluster, self.snapshot)),
            "workernodes_matrix": (self.display_wns_occupancy, (self.wns_occupancy, self.cluster)),
            "user_accounts_pool_mappings": (self.display_user_accounts_pool_mappings, (self.wns_occupancy,)),
        }

        if self.args.WATCH:
            print("\033c", end=" ")  # comma is to avoid losing the whole first line. An empty char still remains, though.

        for idx, part in enumerate(self.config["user_display_parts"], 1):
            display_func, opts = display_parts[part][0], display_parts[part][1]
            display_func(*opts) if not sections_off[idx] else None

        self.display_backend_messages()

        if not self.args.ANONYMIZE:
            print("\nLog file created in %s" % os.path.expandvars(QTOP_LOGFILE))
        if self.args.STRICTCHECK:
            message = self.wns_occupancy.MVU_check_jobs(self.cluster)
            if message:
                print(message)

    def display_job_accounting_summary(self, cluster: Any, snapshot: Any) -> None:
        """Displays qtop's first section."""
        total_running_jobs = cluster.total_running_jobs
        total_queued_jobs = cluster.total_queued_jobs
        queues = cluster.queues_dict

        if self.args.REMAP:
            logging.warning("Remapping WN names and retrying heuristics")

        ansi_delete_char = "\015"  # this removes the first ever character (space) appearing in the output
        print(
            "%(del)s%(name)s \nv%(version)s ## For feedback and updates, see: %(link)s"
            % {
                "name": self.MVU_model.backend.classic_report_name
                if self.args.CLASSIC
                else self.MVU_model.MVU_colorize("./qtop.py     ## Queueing System report tool. Press ? for help", "Cyan_L"),
                "del": ansi_delete_char,
                "link": self.MVU_model.MVU_colorize("https://github.com/qtop/qtop", "Cyan_L"),
                "version": __version__,
            }
        )
        if self.MVU_model.backend.is_simulated:
            msg = "This data is simulated. As soon as you connect to one of the supported scheduling systems,\nyou will see live data from your cluster. Press q to Quit."
            print(self.MVU_model.MVU_colorize(msg, "Blue_L"))

        if not self.args.WATCH:
            command = "qtop" if self.args.ANONYMIZE else "%s/qtop.py" % self.MVU_model.package_path
            print("Please try it with watch: %s -s <SOURCEDIR> -w [<every_nr_of_sec>]" % command)
        if self.MVU_model.backend.is_simulated and self.args.WATCH:
            print(self.MVU_model.MVU_colorize(DEMO_WATCH_KEYS, "Yellow") if self.args.COLOR == "ON" else BOLD % DEMO_WATCH_KEYS)  # bold without colour
        print(
            self.MVU_model.MVU_colorize("===> ", "Gray_D")
            + self.MVU_model.MVU_colorize("Job accounting summary", "White")
            + self.MVU_model.MVU_colorize(" <=== ", "Gray_D")
            + self.MVU_model.MVU_colorize(str(datetime.datetime.today())[:-7], "White")
        )

        summary_states = self.state_columns("summary_states")
        if summary_states is None:
            job_counts = [total_running_jobs, total_queued_jobs]
            job_labels = "R + Q"
        else:
            job_states = [job.job_state for job in snapshot.jobs_dict.values()]
            totals = WNOccupancy.job_state_totals(job_states, self.config["state_abbreviations"][self.MVU_model.scheduler])
            totals["other_of_user"] = len(job_states) - sum(totals.get(key, 0) for key, _ in summary_states if key != "other_of_user")
            job_counts = [totals.get(key, 0) for key, _ in summary_states]
            job_labels = " + ".join(label for _, label in summary_states)
        print(
            "%(Summary)s: Total:%(total_nodes)s Up:%(online_nodes)s Free:%(available_nodes)s %(Nodes)s | %("
            "working_cores)s/%("
            "total_cores)s %(Cores)s |"
            "   %(job_counts)s %(jobs)s (%(job_labels)s) %(reported_by)s"
            % {
                "Summary": self.MVU_model.MVU_colorize("Summary  ", "Cyan_L"),
                "total_nodes": self.MVU_model.MVU_colorize(str(cluster.total_wn), "Red_L"),
                "online_nodes": self.MVU_model.MVU_colorize(str(cluster.total_wn - cluster.offdown_nodes), "Red_L"),
                "available_nodes": self.MVU_model.MVU_colorize(str(cluster.available_wn), "Red_L"),
                "Nodes": self.MVU_model.MVU_colorize("Nodes", "Red_L"),
                "working_cores": self.MVU_model.MVU_colorize(str(cluster.working_cores), "Green_L"),
                "total_cores": self.MVU_model.MVU_colorize(str(cluster.total_cores), "Green_L"),
                "Cores": self.MVU_model.MVU_colorize("cores", "Green_L"),
                "job_counts": "+".join(self.MVU_model.MVU_colorize(str(int(count)), "Blue_L") for count in job_counts),
                "job_labels": job_labels,
                "jobs": self.MVU_model.MVU_colorize("jobs", "Blue_L"),
                "reported_by": self.MVU_model.backend.classic_job_count_source if self.args.CLASSIC else "",
            }
        )

        expanded = self.MVU_model.display_overrides.get("queue_summary_expanded", getattr(self.args, "EXPAND_QUEUES", False))
        print("%(queues)s :" % {"queues": self.MVU_model.MVU_colorize("Queueing", "Cyan_L")}, end="\n" if expanded else " ")
        for _queue_name, q_tuple in queues.items():
            q_running_jobs, q_queued_jobs = q_tuple.run, q_tuple.queued
            account = _queue_name if _queue_name in self.MVU_model.queue_to_color else "account_not_colored"
            print(
                "{indent}{qname}{star}: {run} {q}|".format(
                    indent="  " if expanded else "",
                    qname=self.MVU_model.MVU_colorize(_queue_name, "", pattern=account, mapping=self.MVU_model.queue_to_color),
                    star=self.MVU_model.MVU_colorize("*", "Red_L") if self.MVU_model.backend.queue_is_blocked(q_tuple.state) else "",
                    run=self.MVU_model.MVU_colorize(q_running_jobs, "", pattern=account, mapping=self.MVU_model.queue_to_color),
                    q="+ " + self.MVU_model.MVU_colorize(q_queued_jobs, "", account, mapping=self.MVU_model.queue_to_color) + " " if q_queued_jobs != "0" else "",
                ),
                end="\n" if expanded else " ",
            )
        blocked_note = self.MVU_model.backend.blocked_queue_note
        print(self.MVU_model.MVU_colorize("* implies blocked", "Red") + blocked_note + "\n")
        if getattr(self.args, "SHOW_QUEUE_NODES", False):
            from qtop_py.queue_nodes import queue_node_counts

            try:
                counts = queue_node_counts(snapshot.worker_nodes, list(snapshot.queues_dict), self.MVU_model.backend, self.config.get("experimental_queue_nodes"))
            except ValueError as error:
                print("Queue nodes unavailable: %s" % error)
            else:
                print("Queue nodes (experimental, idle/total): " + " | ".join("%s: %s/%s" % tuple("?" if value is None else value for value in row) for row in counts))

    def display_wns_occupancy(self, wns_occupancy: Any, cluster: Any) -> None:
        """Displays qtop's second section, the main worker node matrices."""
        node_column_start = self.wns_occupancy.node_column_start
        node_column_stop = self.wns_occupancy.node_column_stop

        self.display_basic_legend()
        self.display_matrix(wns_occupancy, node_column_start, node_column_stop)
        # the transposed matrix is one continuous block, doesn't make sense to break into more matrices
        if not self.MVU_model.display_overrides.get("transpose_wn_matrices", self.config["transpose_wn_matrices"]):
            self.display_remaining_matrices(wns_occupancy, node_column_start, node_column_stop)

    def display_backend_messages(self) -> None:
        """Print the scheduler's notes, if any, under Scheduler messages."""
        backend = self.MVU_model.backend
        nodes = getattr(self.cluster, "worker_nodes", [])
        lines = [line for section in ("snapshot", "accounting", "placement", "resources") for line in backend.display_messages(section, self.snapshot, nodes, self.config)]
        if lines:
            print("Scheduler messages:")
            print("\n".join(lines))
            print()

    def display_basic_legend(self) -> None:
        """Displays the Worker Nodes occupancy label plus columns explanation."""
        if self.MVU_model.display_overrides.get("transpose_wn_matrices", self.config["transpose_wn_matrices"]):
            note = "/".join(self.config["occupancy_column_order"])
        else:
            note = "you can read vertically the node IDs"

        print(
            self.MVU_model.MVU_colorize("===> ", "Gray_D")
            + self.MVU_model.MVU_colorize("Worker Nodes occupancy", "White")
            + self.MVU_model.MVU_colorize(" <=== ", "Gray_D")
            + self.MVU_model.MVU_colorize("(%s)", "Gray_D") % note
        )

    def display_user_accounts_pool_mappings(self, wns_occupancy: Any) -> None:
        """Displays qtop's third section."""
        try:
            account_jobs_table = self.wns_occupancy.account_jobs_table
            user_color_patterns = self.wns_occupancy.user_color_patterns
        except KeyError:
            account_jobs_table = []  # empty, like the former dict()
            user_color_patterns = dict()

        detail_of_name = self.MVU_model.user_details.get("names", {})
        group_of_name = self.MVU_model.user_details.get("groups")
        print(
            self.MVU_model.MVU_colorize("\n===> ", "Gray_D")
            + self.MVU_model.MVU_colorize("User accounts and pool mappings", "White")
            + self.MVU_model.MVU_colorize(" <=== ", "Gray_d")
            + self.MVU_model.MVU_colorize(
                self.MVU_model.backend.classic_account_note if self.args.CLASSIC else user_accounts_header(self.MVU_model.config),
                "Gray_D",
            )
        )

        if getattr(self.args, "SHOW_GROUPS", False):
            detail_header = "GECOS field              | primary group        |"
        else:
            detail_header = "Grid certificate DN (info only available under elevated privileges)" if self.args.CLASSIC else "      GECOS field or Grid certificate DN |"
        labels = [label for _, label in self.state_columns("accounts_states", DEFAULT_ACCOUNT_STATES)]
        print("[id] unix account      |jobs >=%s | nodes | %s" % (" + ".join(label.rjust(4) for label in labels), detail_header))
        for line in account_jobs_table:
            uid = line[0]
            userid_pat = user_color_patterns[str(uid)]
            self.display_account_jobs_line(line, userid_pat, detail_of_name, group_of_name)

        totals = self.account_jobs_totals(account_jobs_table) if getattr(self.args, "SHOW_ACCOUNT_TOTALS", False) else None
        if totals:
            self.display_account_jobs_line(totals, "account_not_colored", detail_of_name, group_of_name)

    @staticmethod
    def account_jobs_totals(account_jobs_table: Sequence[Sequence[Any]]) -> Optional[List[Any]]:
        """Return the per-state job totals over all accounts, or None if there are no accounts."""
        if not account_jobs_table:
            return None

        state_totals: Dict[str, int] = {}
        for line in account_jobs_table:
            for state_key, count in TextDisplay.row_state_counts(line).items():
                state_totals[state_key] = state_totals.get(state_key, 0) + int(count)
        return [
            "T",
            sum(int(line[1]) for line in account_jobs_table),
            sum(int(line[2]) for line in account_jobs_table),
            sum(int(line[3]) for line in account_jobs_table),
            "Totals",
            sum(int(line[5]) for line in account_jobs_table),
            state_totals,
        ]

    @staticmethod
    def row_state_counts(line: Sequence[Any]) -> Dict[str, Any]:
        """Per-state job counts of an accounts row; rows without them (six fields) carry only R and Q."""
        return dict(line[6]) if len(line) > 6 else {"running_of_user": line[1], "queued_of_user": line[2]}

    @overload
    def state_columns(self, config_key: str) -> Optional[List[Tuple[str, str]]]: ...

    @overload
    def state_columns(self, config_key: str, default: Sequence[Any]) -> List[Tuple[str, str]]: ...

    def state_columns(self, config_key: str, default: Optional[Sequence[Any]] = None) -> Optional[List[Tuple[str, str]]]:
        """Return (state key, label) pairs from accounts_states or summary_states.

        An entry is a state key or a {state_key: label} mapping; the default label is the key's first letter, uppercased.
        other_of_user counts jobs in no listed state.
        """
        entries = self.config.get(config_key) or default
        if entries is None:
            return None
        columns = []
        for entry in entries:
            key, label = next(iter(entry.items())) if isinstance(entry, dict) else (entry, None)
            columns.append((key, str(label) if label else ("O" if key == "other_of_user" else key[0].upper())))
        return columns

    def display_account_jobs_line(self, line: Sequence[Any], userid_pat: str, detail_of_name: Mapping[str, str], group_of_name: Optional[Mapping[str, str]] = None) -> None:
        """Print one account's row of the user accounts table."""
        uid, runningjobs, queuedjobs, alljobs, user, num_of_nodes = line[:6]
        state_columns = self.state_columns("accounts_states", DEFAULT_ACCOUNT_STATES)
        counts = self.row_state_counts(line)
        counts["other_of_user"] = int(alljobs) - sum(int(counts.get(key, 0)) for key, _ in state_columns if key != "other_of_user")

        if self.args.COLOR == "OFF" or userid_pat == "account_not_colored" or self.MVU_model.user_to_color.get(userid_pat) == "reset":
            conditional_width = 0
            userid_pat = "account_not_colored"
        else:
            conditional_width = len(self.MVU_model.MVU_colorize("", pattern=userid_pat))

        detail_width = 25 if group_of_name is not None else 40
        group_column = "{sep} {group:<{width20}} " if group_of_name is not None else ""
        state_cells = "   ".join("{%d:>{width4}}" % (7 + index) for index in range(len(state_columns)))
        row_format = "[ {0:<{width1}}] {4:<{width18}}{sep}{3:>{width4}}   " + state_cells + " {sep} {6:>{width5}} {sep} {5:<{detail_width}} "
        print_string = (row_format + group_column + "{sep}").format(
            self.MVU_model.MVU_colorize(str(uid), pattern=userid_pat),
            self.MVU_model.MVU_colorize(str(runningjobs), pattern=userid_pat),
            self.MVU_model.MVU_colorize(str(queuedjobs), pattern=userid_pat),
            self.MVU_model.MVU_colorize(str(alljobs), pattern=userid_pat),
            self.MVU_model.MVU_colorize(user, pattern=userid_pat),
            self.MVU_model.MVU_colorize(detail_of_name.get(user, ""), pattern=userid_pat),
            self.MVU_model.MVU_colorize(str(num_of_nodes), pattern=userid_pat),
            *[self.MVU_model.MVU_colorize(str(counts.get(key, 0)), pattern=userid_pat) for key, _ in state_columns],
            group=self.MVU_model.MVU_colorize((group_of_name or {}).get(user, ""), pattern=userid_pat),
            sep=self.MVU_model.MVU_colorize(self.config["SEPARATOR"], pattern=userid_pat),
            width1=1 + conditional_width,
            width4=4 + conditional_width,
            width5=5 + conditional_width,
            width18=18 + conditional_width,
            detail_width=detail_width + conditional_width,
            width20=20 + conditional_width,
        )
        print(print_string)

    def display_matrix(self, wns_occupancy: Any, node_column_start: int, node_column_stop: int) -> None:
        """occupancy_parts needs to be redefined for each matrix, because of changed parameter values."""
        if self.wns_occupancy.is_matrix_coreless(node_column_start, node_column_stop):
            return

        wn_vert_labels = wns_occupancy.wn_vert_labels
        core_user_map = wns_occupancy.core_user_map
        user_color_patterns = wns_occupancy.user_color_patterns
        mapping = self.config["core_coloring"]

        occupancy_parts: Dict[str, Tuple[Callable[..., Any], Tuple[Any, ...], Dict[str, Any]]] = {
            "wn id lines": (self.display_wnid_lines, (node_column_start, node_column_stop, self.cluster.highest_wn, wn_vert_labels), {"inner_attrs": None}),
            "core_user_map": (
                self.print_core_lines,
                (core_user_map, node_column_start, node_column_stop, self.MVU_model.transposed_sections, user_color_patterns, mapping),
                {"attrs": None},
            ),
        }

        # custom part, e.g. Node state, queue state etc
        for _yaml_key, part_name, systems in yaml_parser.get_yaml_key_part(self.config, self.MVU_model.scheduler, outermost_key="workernodes_matrix"):
            if self.MVU_model.scheduler not in systems:
                continue

            new_occupancy_part = {
                part_name: (
                    self.print_mult_attr_line,  # func
                    (node_column_start, node_column_stop, self.MVU_model.transposed_sections),  # opts
                    {"attr_lines": getattr(wns_occupancy, part_name), "coloring": self.MVU_model.queue_to_color},  # kwargs
                )
            }
            occupancy_parts.update(new_occupancy_part)

        # get additional info from QTOPCONF_YAML
        for part_dict in self.config["workernodes_matrix"]:
            part = [k for k in part_dict][0]
            key_vals = part_dict[part]
            if self.MVU_model.scheduler not in yaml_parser.fix_config_list(key_vals.get("systems", [self.MVU_model.scheduler])):
                continue
            occupancy_parts[part][2].update(key_vals)  # get extra args from user

            func_, opts, kwargs = occupancy_parts[part][0], occupancy_parts[part][1], occupancy_parts[part][2]
            func_(*opts, **kwargs)

        if self.MVU_model.display_overrides.get("transpose_wn_matrices", self.config["transpose_wn_matrices"]):
            order = self.config["occupancy_column_order"]
            for _idx, (_item, matrix) in enumerate(zip(order, self.MVU_model.transposed_sections)):
                matrix[0] = order.index(matrix[1])

            self.MVU_model.transposed_sections.sort(key=lambda item: item[0])
            # TRY
            for line_tuple in zip_longest(
                *[tpl[2] for tpl in self.MVU_model.transposed_sections],
                fillvalue=[
                    utils.ColorStr(
                        "  ",
                        color="Purple",
                    )
                ],
            ):
                joined_list = self.join_prints(*line_tuple, sep=self.config.get("horizontal_separator", None), nocutoff=self.args.LESS)

            max_width = len(joined_list)
            self.viewport.max_width = max_width

            logging.debug("Printed horizontally from %s to %s" % (self.viewport.h_start, self.viewport.h_stop))
        else:
            self.viewport.max_width = self.viewport.get_term_size()[1]
        print()

    def display_remaining_matrices(self, wns_occupancy: Any, node_column_start: int, node_column_stop: int, DEADWEIGHT: int = 11) -> None:
        """Print the extra matrices needed when the nodes do not fit the screen width.

        DEADWEIGHT is the width of the {__XXXX__} labels right of the core matrix. Cores that no node
        in a matrix has are not printed, e.g. 64-core rows for a matrix of 8-core nodes.
        """
        extra_matrices_nr = wns_occupancy.extra_matrices_nr
        term_columns = self.MVU_model.viewport.h_term_size

        # need node_state, temp
        for _matrix in range(extra_matrices_nr):
            node_column_start = node_column_stop
            if self.config["USER_CUT_MATRIX_WIDTH"]:
                node_column_stop += self.config["USER_CUT_MATRIX_WIDTH"]
            else:
                node_column_stop += term_columns - DEADWEIGHT
            node_column_stop = min(node_column_stop, self.cluster.total_wn) if self.args.REMAP else min(node_column_stop, self.cluster.highest_wn)

            self.display_matrix(wns_occupancy, node_column_start, node_column_stop)

    def join_prints(self, *args: Any, **kwargs: Any) -> List[Any]:
        """Join the given rows into one line of ColorStr cells, separated by sep."""
        joined_list = []
        for d in args:
            joined_list.extend([utils.ColorStr(string=char) if isinstance(char, str) and len(char) == 1 else char for char in d])
            joined_list.append(utils.ColorStr(string=kwargs["sep"]))
        # display the full output if nocutoff is True
        cells = joined_list if kwargs.get("nocutoff", False) else joined_list[self.viewport.h_start : self.viewport.h_stop]
        s = "".join([self._cell_text(char.initial, char.color) if isinstance(char, utils.ColorStr) else char for char in cells])
        print(compress_colored_line(s))
        return joined_list

    def print_core_lines(
        self,
        core_user_map: Dict[str, Any],
        node_column_start: int,
        node_column_stop: int,
        transposed_sections: List[Any],
        user_color_patterns: Any,
        mapping: Any,
        attrs: Any,
        options1: Any,
        options2: Any,
    ) -> None:
        """Print the core lines of a matrix slice, transposed if asked."""
        self.MVU_model.MVU_reset_sigpipe()
        remove_corelines = self.MVU_model.display_overrides.get("rem_empty_corelines", self.config["rem_empty_corelines"]) + 1

        # if corelines vertical (transposed matrix)
        if self.MVU_model.display_overrides.get("transpose_wn_matrices", self.config["transpose_wn_matrices"]):
            non_existent_symbol = self.config["non_existent_node_symbol"]
            for _core_x_vector, _ind, k, is_corevector_removable in gauge_core_vectors(
                core_user_map, node_column_start, node_column_stop, WNOccupancy.coreline_notthere_or_unused, non_existent_symbol, remove_corelines
            ):
                if is_corevector_removable:
                    del core_user_map[k]

            tuple_ = [None, "core_map", self.transpose_matrix(core_user_map, colored=False, coloring_pat=user_color_patterns)]
            transposed_sections.append(tuple_)
            return
        # if corelines horizontal (non-transposed matrix)
        for core_line in self.get_core_lines(core_user_map, node_column_start, node_column_stop, user_color_patterns, mapping, attrs):
            core_line_zipped = compress_colored_line(core_line)
            try:
                print(core_line_zipped)
            except IOError:
                return

    def display_wnid_lines(self, start: int, stop: int, highest_wn: int, wn_vert_labels: Dict[str, Any], **kwargs: Any) -> None:
        """Print the coloured worker-node ID lines; highest_wn sets how many are needed."""
        d = OrderedDict()
        end_labels = self.config["workernodes_matrix"][0]["wn id lines"]["end_labels"]

        if not self.MVU_model.display_overrides["force_names"]:
            node_str_width = len(str(highest_wn))  # 4 for thousands of nodes, nr of horizontal lines to be displayed

            for node_nr in range(1, node_str_width + 1):
                d[str(node_nr)] = wn_vert_labels[str(node_nr)]
            end_labels_iter = iter(end_labels[str(node_str_width)])
            self.print_wnid_lines(d, start, stop, end_labels_iter, self.MVU_model.transposed_sections, colors_factory=partial(self.color_plainly, "White", "Gray_L", start > 0))
            # start > 0 is just a test for a possible future condition

        elif self.MVU_model.display_overrides["force_names"]:  # the actual names of the WNs instead of numbered WNs [was: or args.FORCE_NAMES]
            node_str_width = len(wn_vert_labels)  # key, nr of horizontal lines to be displayed

            # for longer full-labeled wn ids, add more end-labels (far-right) towards the bottom
            for num in range(8, len(wn_vert_labels) + 1):
                end_labels.setdefault(str(num), end_labels["7"] + num * ["={________}"])

            end_labels_iter = iter(end_labels[str(node_str_width)])
            self.print_wnid_lines(
                wn_vert_labels,
                start,
                stop,
                end_labels_iter,
                self.MVU_model.transposed_sections,
                colors_factory=partial(self.highlight_alternately, *self.config["ALT_LABEL_COLORS"]),
            )

    def highlight_alternately(self, color_a: str, color_b: str) -> Iterator[str]:
        """Yield color_a and color_b alternately, forever."""
        colors = cycle([color_a, color_b])
        for color in colors:
            yield color

    def color_plainly(self, color_0: str, color_1: str, condition: bool) -> Iterator[str]:
        """Yield color_0 if condition holds, otherwise color_1, forever."""
        color = color_0 if condition else color_1
        while True:
            yield color

    def print_wnid_lines(
        self, d: Dict[str, Any], start: int, stop: int, end_labels: Iterable[str], transposed_sections: List[Any], colors_factory: Callable[[], Iterator[str]]
    ) -> None:
        """Print the worker-node ID lines of a matrix slice, transposed if asked."""
        if self.MVU_model.display_overrides.get("transpose_wn_matrices", self.config["transpose_wn_matrices"]):
            tuple_ = [None, "wnid_lines", self.transpose_matrix(d)]
            transposed_sections.append(tuple_)
            return

        separators = self.config["vertical_separator_every_X_columns"]
        for line_nr, end_label in zip(d, end_labels):
            colors = colors_factory()
            wn_id_str = self._insert_separators(d[line_nr][start:stop], self.config["SEPARATOR"], separators)
            wn_id_str = "".join([self.MVU_model.MVU_colorize(elem, next(colors)) for elem in wn_id_str])
            print(wn_id_str + end_label)

    def show_part_view(self, _timestr: str, file: str, x: int, y: int) -> str:
        """Print the visible part of the rendered output to the terminal, as fast as possible.

        See http://unix.stackexchange.com/questions/47407/cat-line-x-to-line-y-on-a-huge-file
        """
        with tempfile.NamedTemporaryFile(delete=False, suffix=".out", prefix="qtop_partview_%s_" % _timestr, dir=self.config["savepath"]) as temp_f:
            temp_filename = temp_f.name
        tail_command = ["tail", "-n+" + str(x), file]
        head_command = ["head", "-n" + str(y - 1)]
        with open(temp_filename, "w", encoding="utf-8") as output_file, subprocess.Popen(tail_command, stdout=subprocess.PIPE) as process_tail, subprocess.Popen(
            head_command, stdin=process_tail.stdout, stdout=output_file
        ) as process_head:
            if process_tail.stdout is not None:
                process_tail.stdout.close()
            process_head.communicate()
        return temp_filename

    def print_mult_attr_line(
        self, node_column_start: int, node_column_stop: int, transposed_sections: List[Any], attr_lines: Any, label: str, color_func: Any = None, **_config_metadata: Any
    ) -> None:
        """attr_lines can be e.g. Node state lines."""
        if self.MVU_model.display_overrides.get("transpose_wn_matrices", self.config["transpose_wn_matrices"]):
            tuple_ = [None, label, self.transpose_matrix(attr_lines, colored=True, coloring_pat=None)]
            transposed_sections.append(tuple_)
            return

        for _line in attr_lines:
            line = attr_lines[_line][node_column_start:node_column_stop]
            attr_line = self._insert_separators(line, self.config["SEPARATOR"], self.config["vertical_separator_every_X_columns"])
            attr_line = "".join([self.MVU_model.MVU_colorize(char.initial, color_func=char.color) for char in attr_line])
            print(attr_line + "=" + label)

    def get_core_lines(self, core_user_map: Dict[str, Any], node_column_start: int, node_column_stop: int, coloring_pattern: Any, mapping: Any, attrs: Any) -> Iterator[Any]:
        """Yield the core lines, skipping cores that no node in the matrix has."""
        non_existent_symbol = self.config["non_existent_node_symbol"]
        remove_corelines = self.MVU_model.display_overrides.get("rem_empty_corelines", self.config["rem_empty_corelines"]) + 1
        for core_x_vector, ind, _k, is_corevector_removable in gauge_core_vectors(
            core_user_map, node_column_start, node_column_stop, WNOccupancy.coreline_notthere_or_unused, non_existent_symbol, remove_corelines
        ):
            if is_corevector_removable:
                continue

            core_x_vector = self._insert_separators(core_x_vector, self.config["SEPARATOR"], self.config["vertical_separator_every_X_columns"])
            colored_core_x = [self.MVU_model.MVU_colorize(elem, color_func=elem.color) for elem in core_x_vector]
            core_x_vector = "".join([str(item) for item in colored_core_x])
            yield core_x_vector + self.MVU_model.MVU_colorize("=Core" + str(ind), pattern="account_not_colored")

    def transpose_matrix(self, d: Dict[str, Any], colored: bool = False, reverse: bool = False, coloring_pat: Any = None) -> Iterator[Sequence[str]]:
        """Return the matrix (a mapping of string lists) transposed, coloured if asked."""
        for tpl in zip_longest(*[[char for char in d[k]] for k in d], fillvalue=" "):
            row: Sequence[str] = tpl
            if any(j != " " for j in tpl):
                cells = (colored and coloring_pat) and [self.MVU_model.MVU_colorize(txt, "", coloring_pat[txt]) if txt in coloring_pat else txt for txt in tpl] or list(tpl)
                cells[:] = cells[::-1] if reverse else cells
                row = cells
            yield row

    def _insert_separators(self, orig_str: Any, separator: str, pos: int, stopaftern: int = 0) -> Any:
        """Inserts separator into orig_str every pos-th position, optionally stopping after stopaftern times."""
        if not pos:  # default value is zero, means no vertical separators
            return orig_str
        sep_str = orig_str[:]  # insert initial vertical separator
        sep: Any = separator if isinstance(sep_str, str) else list(separator)
        times = stopaftern if stopaftern else len(orig_str) // pos
        sep_str = sep_str[:pos] + sep + sep_str[pos:]
        for i in range(2, times + 1):
            sep_str = sep_str[: pos * i + i - 1] + sep + sep_str[pos * i + i - 1 :]
        sep_str += sep  # insert initial vertical separator
        return sep_str


def MVU_view(view_data: Tuple[Any, Any, Any], savepath: str, sample_filename: str, logfile: str) -> TextDisplay:
    """Render the model's frame to stdout and return the display."""
    model, cluster, occupancy = view_data
    display = TextDisplay(model.snapshot, model.config, model.viewport, occupancy, cluster, model.args, MVU_model=model)
    display.display_selected_sections(savepath, sample_filename, logfile)
    return display
