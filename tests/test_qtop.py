##
## qtop is a tool to monitor queuing systems - https://github.com/qtop/qtop
##
## Copyright (c) 2016 Fotis Georgatos
## Copyright (c) 2016 Sotiris Fragkiskos
## Copyright (c) 2023 Hewlett Packard Enterprise Development LP
## Copyright (c) 2026 Jacob Hatchett
##
## SPDX-License-Identifier: MIT
##

import datetime
import logging
import os
import re
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from mvu_support import explicit_model

from qtop_py import configuration, user_details
from qtop_py import qtop as qtop_module
from qtop_py import utils as qtop_utils
from qtop_py.colormap import color_to_code
from qtop_py.constants import SYMBOL_LONG_TAIL_USER, SYMBOL_UNKNOWN_NODE_STATE
from qtop_py.core_errors import JobNotFound
from qtop_py.input_parsing import extract_regex_detail
from qtop_py.mvu import MVU_Model
from qtop_py.mvu_query import Cluster, WNFilter, WNOccupancy
from qtop_py.qtop import (
    NoSchedulerFound,
    SchedulerNotSpecified,
    decide_batch_system,
    get_date_obj_from_str,
    get_output_size,
    init_dirs,
)
from qtop_py.schedulers import registry
from qtop_py.ui import mvu_view
from qtop_py.ui import terminal as terminal_io
from qtop_py.ui.viewport import Viewport

SYMBOL_NON_EXISTENT_NODE = "#"
SYMBOL_SEPARATOR = "|"
SYMBOL_UNUSED_CORE = "_"
SYMBOL_POSSIBLE_IDS_WITH_RESERVED = [
    "0",
    SYMBOL_NON_EXISTENT_NODE,
    SYMBOL_UNUSED_CORE,
    SYMBOL_LONG_TAIL_USER,
    SYMBOL_UNKNOWN_NODE_STATE,
    "1",
    SYMBOL_SEPARATOR,
]


def user_symbol_config(fill_with_user_firstletter=None, possible_ids=None):
    config = {
        "non_existent_node_symbol": SYMBOL_NON_EXISTENT_NODE,
        "SEPARATOR": SYMBOL_SEPARATOR,
    }
    if fill_with_user_firstletter is not None:
        config["fill_with_user_firstletter"] = fill_with_user_firstletter
    if possible_ids is not None:
        config["possible_ids"] = possible_ids
    return config


@pytest.fixture
def config():
    return {}


@pytest.mark.parametrize("config,base", [({}, 0), ({"rem_empty_corelines": 0}, 0), ({"rem_empty_corelines": 2}, 2), ({"rem_empty_corelines": "1"}, 1)])
@pytest.mark.parametrize("remove", [0, 1, 2])
def test_update_config_defaults_missing_coreline_setting(config, base, remove):
    args = SimpleNamespace(OPTION=[], TRANSPOSE=False, REM_EMPTY_CORELINES=remove)
    assert configuration.update_config_with_cmdline_vars(args, dict(config))["rem_empty_corelines"] == base + remove


def test_sort_worker_nodes_uses_named_sort_keys():
    cluster = Cluster.__new__(Cluster)
    cluster.MVU_model = explicit_model()
    cluster.config = {"sorting": {"user_sort": ["sort by all numbers"], "reverse": False}}
    cluster.worker_nodes = [
        {"domainname": "node10", "state": "-", "np": "1", "core_job_map": {}},
        {"domainname": "node2", "state": "-", "np": "1", "core_job_map": {}},
    ]

    assert [node["domainname"] for node in cluster._sort_worker_nodes()] == ["node2", "node10"]


WATCH_ARGS = SimpleNamespace(ONLYSAVETOFILE=False, WATCH=True)


def test_raw_mode_allows_watch_mode_without_terminal_attrs(monkeypatch):
    class NonTerminalFile:
        def fileno(self):
            raise OSError("not a terminal")

    entered_context = False
    with terminal_io.raw_mode(NonTerminalFile(), WATCH_ARGS):
        entered_context = True

    assert entered_context


class FakeTermios:
    ECHO, ICANON, TCSADRAIN, TCSAFLUSH = 8, 2, 1, 2

    class error(Exception):
        pass

    def __init__(self, fail_set=False):
        self.calls, self.fail_set = [], fail_set

    def tcgetattr(self, fd):
        return [0, 0, 0, 8 | 2 | 1, 0, 0]

    def tcsetattr(self, fd, when, attrs):
        if self.fail_set:
            raise self.error("cannot set")
        self.calls.append((when, attrs[3]))


class TerminalFile:
    def fileno(self):
        return 0


@pytest.mark.parametrize("failure", (None, RuntimeError))
def test_raw_mode_yields_cbreak_attrs_and_restores_flushing(monkeypatch, failure):
    fake = FakeTermios()
    monkeypatch.setattr(terminal_io, "termios", fake)
    try:
        with terminal_io.raw_mode(TerminalFile(), WATCH_ARGS) as (old_attrs, new_attrs):
            assert (old_attrs[3], new_attrs[3]) == (11, 1)  # echo and line buffering off; other flags kept
            if failure:
                raise failure()
    except RuntimeError:
        assert failure
    assert fake.calls == [(fake.TCSADRAIN, 1), (fake.TCSAFLUSH, 11)]  # restored even after an error


def test_raw_mode_restores_the_terminal_on_sigterm_and_reapplies_cbreak_on_sigcont(monkeypatch):
    import os
    import signal

    fake = FakeTermios()
    monkeypatch.setattr(terminal_io, "termios", fake)
    previous = signal.getsignal(signal.SIGTERM)
    with pytest.raises(SystemExit) as exited:
        with terminal_io.raw_mode(TerminalFile(), WATCH_ARGS):
            os.kill(os.getpid(), signal.SIGCONT)  # as after fg
            os.kill(os.getpid(), signal.SIGTERM)  # as from kill
    assert exited.value.code == 128 + signal.SIGTERM
    assert fake.calls == [(fake.TCSADRAIN, 1), (fake.TCSADRAIN, 1), (fake.TCSAFLUSH, 11)]
    assert signal.getsignal(signal.SIGTERM) is previous


def test_raw_mode_degrades_when_the_terminal_refuses_new_attrs(monkeypatch):
    monkeypatch.setattr(terminal_io, "termios", FakeTermios(fail_set=True))
    caught = False
    try:
        with terminal_io.raw_mode(TerminalFile(), WATCH_ARGS) as attrs:
            assert attrs == ("", "")
            raise ValueError("in the watch loop")
    except ValueError as raised:
        assert raised.__context__ is None  # the loop no longer runs inside the termios except block
        caught = True
    assert caught, "raw_mode swallowed the watch-loop error"


def test_menus_leave_a_non_terminal_alone(monkeypatch):
    terminal = Mock()
    monkeypatch.setattr(terminal_io, "_set_terminal_attrs", terminal)
    with terminal_io.menu_terminal("", ""):
        pass
    assert terminal.call_count == 0


def test_raw_mode_does_not_swallow_unexpected_fileno_errors(monkeypatch):
    class BrokenFile:
        def fileno(self):
            raise RuntimeError("unexpected")

    with pytest.raises(RuntimeError, match="unexpected"):
        with terminal_io.raw_mode(BrokenFile(), WATCH_ARGS):
            pass


def test_terminal_resize_redraws_without_cycling_core_lines(monkeypatch):
    viewport = qtop_module.Viewport()
    viewport.set_term_size(40, 120)
    monkeypatch.setattr(terminal_io.select, "select", lambda *_args: ([], [], []))  # no key within the refresh interval
    monkeypatch.setattr(terminal_io, "calculate_term_size", lambda *_args: (50, 160))  # the terminal was resized
    assert terminal_io.wait_for_keypress_or_autorefresh(viewport, {}, (40, 120), 0) == "\n"
    assert viewport.get_term_size() == (50, 160)


@pytest.mark.skipif(terminal_io.SIGWINCH is None, reason="no SIGWINCH on this platform")
def test_terminal_resize_redraws_at_once_on_sigwinch(monkeypatch):
    import os
    import signal

    def resize_while_waiting(*_args):
        os.kill(os.getpid(), signal.SIGWINCH)  # the user resizes the window mid-wait
        raise AssertionError("SIGWINCH should end the wait")

    viewport = qtop_module.Viewport()
    viewport.set_term_size(40, 120)
    monkeypatch.setattr(terminal_io.select, "select", resize_while_waiting)
    monkeypatch.setattr(terminal_io, "calculate_term_size", lambda *_args: (50, 160))
    previous = signal.getsignal(signal.SIGWINCH)
    assert terminal_io.wait_for_keypress_or_autorefresh(viewport, {}, (40, 120), 60) == "\n"
    assert viewport.get_term_size() == (50, 160) and signal.getsignal(signal.SIGWINCH) is previous


def test_sort_worker_nodes_rejects_custom_python_sorting():
    cluster = Cluster.__new__(Cluster)
    cluster.MVU_model = explicit_model()
    cluster.config = {"sorting": {"user_sort": ["sort by custom definition"], "reverse": False}}
    cluster.worker_nodes = [{"domainname": "node1", "state": "-", "np": "1", "core_job_map": {}}]

    with pytest.raises(ValueError, match="custom Python sorting expressions"):
        cluster._sort_worker_nodes()


def test_reset_sigpipe_is_noop_when_platform_has_no_sigpipe(monkeypatch):
    calls = []

    monkeypatch.setattr(terminal_io, "SIGPIPE", None)
    monkeypatch.setattr(terminal_io, "signal", lambda *args: calls.append(args))

    terminal_io.reset_sigpipe()

    assert calls == []


def test_reset_sigpipe_restores_default_when_available(monkeypatch):
    calls = []

    monkeypatch.setattr(terminal_io, "SIGPIPE", 13)
    monkeypatch.setattr(terminal_io, "SIG_DFL", "default")
    monkeypatch.setattr(terminal_io, "signal", lambda *args: calls.append(args))

    terminal_io.reset_sigpipe()

    assert calls == [(13, "default")]


def test_raw_mode_is_noop_when_platform_has_no_termios(monkeypatch):
    monkeypatch.setattr(terminal_io, "termios", None)

    with terminal_io.raw_mode(sys.stdin, WATCH_ARGS):
        pass


def test_scheduler_autodetection_skips_demo_and_missing_commands(monkeypatch):
    available = {"echo": "/bin/echo", "qhost": "/usr/bin/qhost"}
    monkeypatch.setattr(registry.shutil, "which", available.get)
    config = {"signature_commands": {"demo": "echo", "pbs": "pbsnodes", "sge": "qhost"}}

    assert registry.auto_get_avail_batch_system(config, registry.discover_qtop_batch_systems()) == "sge"


def test_scheduler_autodetection_fails_when_only_demo_is_available(monkeypatch):
    monkeypatch.setattr(registry.shutil, "which", lambda command: "/bin/echo" if command == "echo" else None)
    config = {"signature_commands": {"demo": "echo", "pbs": "pbsnodes"}}

    with pytest.raises(SchedulerNotSpecified):
        registry.auto_get_avail_batch_system(config, registry.discover_qtop_batch_systems())


def test_scheduler_autodetection_prioritizes_slurm(monkeypatch):
    available = {"qstat": "/site/bin/qstat", "sinfo": "/usr/bin/sinfo"}
    monkeypatch.setattr(registry.shutil, "which", available.get)
    config = {"signature_commands": {"sge": "qstat", "slurm": "sinfo"}}

    assert registry.auto_get_avail_batch_system(config, registry.discover_qtop_batch_systems()) == "slurm"


def test_experimental_features_require_explicit_opt_in(capsys):
    with pytest.raises(SystemExit) as exc_info:
        qtop_module.require_experimental_opt_in(SimpleNamespace(ANONYMIZE=False, WEB=True, EXPERIMENTAL=False))

    assert exc_info.value.code == 1
    assert "The web interface" in capsys.readouterr().out


def test_anonymization_is_no_longer_experimental():  # qtop/qtop#548
    assert qtop_module.require_experimental_opt_in(SimpleNamespace(ANONYMIZE=True, WEB=False, EXPERIMENTAL=False)) is None


def test_experimental_features_accept_explicit_opt_in():
    args = SimpleNamespace(ANONYMIZE=True, WEB=True, EXPERIMENTAL=True)

    assert qtop_module.require_experimental_opt_in(args) is None


def test_queue_colorization_returns_new_nodes_without_mutating_input():
    worker_nodes = [
        {
            "domainname": "node1",
            "state": "-",
            "qname": ["batch", "urgent"],
            "core_job_map": {},
        }
    ]

    result = qtop_module.keep_queue_initials_only_and_colorize(
        worker_nodes,
        {"batch": "green", "urgent": "red"},
    )

    assert result is not worker_nodes
    assert result[0] is not worker_nodes[0]
    assert worker_nodes[0]["qname"] == ["batch", "urgent"]
    assert [str(queue) for queue in result[0]["qname"]] == ["batch", "urgent"]
    assert [queue.color for queue in result[0]["qname"]] == ["green", "red"]


def test_node_state_colorization_returns_new_nodes_without_mutating_input():
    worker_nodes = [
        {
            "domainname": "node1",
            "state": "-X",
            "qname": [qtop_utils.ColorStr("batch", color="green")],
            "core_job_map": {},
        }
    ]

    result = qtop_module.colorize_nodestate(
        worker_nodes,
        {"-X": "red"},
    )

    assert result is not worker_nodes
    assert result[0] is not worker_nodes[0]
    assert worker_nodes[0]["state"] == "-X"
    assert result[0]["qname"] is worker_nodes[0]["qname"]
    assert [str(state) for state in result[0]["state"]] == ["-", "X"]
    assert [state.color for state in result[0]["state"]] == ["red", "red"]


def test_color_str_iteration_matches_string_and_is_repeatable():
    colored = qtop_utils.ColorStr("abc", color="Blue")

    assert list(colored) == ["a", "b", "c"]
    assert list(colored) == ["a", "b", "c"]


@pytest.mark.parametrize("max_height", [0, 9])
@pytest.mark.parametrize("max_line_len", [0, 7])
def test_get_output_size_ignores_ansi_sequences(tmp_path, max_height, max_line_len):
    output_file = tmp_path / "qtop.out"
    output_file.write_text("\x1b[1;31mred\x1b[0;m\nplain\n", encoding="utf-8")

    assert get_output_size(max_line_len, str(output_file), max_height) == (max_height or 2, max_line_len or 5)


@pytest.mark.parametrize(
    "domain_name, match",
    (
        ("lrms123", "lrms123"),
        ("td123.pic.es", "td123"),
        ("gridmon.ucc.ie", "gridmon"),
        ("gridmon.cs.tcd.ie", "gridmon"),
        ("wn123.grid.ucc.ie", "wn123"),
        ("lcg123.gridpp.rl.ac.uk", "lcg123"),
        ("compute-123-123", "compute-123-123"),
        ("wn-hp-123.egi.local", "wn-hp-123"),
        ("woinic-123.egi.local", "woinic-123"),
        ("wn123-ara.bifi.unizar.es", "wn123-ara"),
        ("c123-123-123.gridka.de", "c123-123-123"),
        ("n123-iep-grid.saske.sk", "n123-iep-grid"),
    ),
)
def test_re_node(domain_name, match):
    re_node = r"([A-Za-z0-9-]+)(?=\.|$)"
    m = re.search(re_node, domain_name)
    try:
        assert m.group(0) == match
    except AttributeError:
        pytest.fail("node name did not match")


@pytest.mark.parametrize(
    "domain_name, number",
    (
        ("wn067.grid.cs.tcd.ie", 67),
        ("gridmon.cs.tcs.ie", -1),
        ("wn003.cs.tcs.ie", 3),
        ("wn01-03-003.cs.tcs.ie", 103003),
    ),
)
def test_batch_nodes_sorting(domain_name, number):
    domain_name = domain_name.split(".", 1)[0]
    assert int(re.sub(r"[A-Za-z_-]+", "", domain_name) or -1) == number


def test_create_job_counts():  # user_names, job_states, state_abbrevs
    user_names = ["sotiris", "kostas", "yannis", "petros"]
    state_abbrevs = {"C": "cancelled_of_user", "E": "exiting_of_user", "r": "running_of_user"}
    job_states = ["r", "E", "r", "C"]

    class Snapshot(object):
        jobs_dict = {}

    snapshot = Snapshot()

    wns_occupancy = WNOccupancy(None, None, snapshot, None, MVU_model=explicit_model())
    assert wns_occupancy._create_user_job_counts(user_names, job_states, state_abbrevs) == {
        "cancelled_of_user": {"sotiris": 0, "yannis": 0, "petros": 1},
        "exiting_of_user": {"sotiris": 0, "kostas": 1, "yannis": 0},
        "running_of_user": {"sotiris": 1, "yannis": 1},
    }


def test_create_user_job_counts_raises_jobnotfound():  # user_names, job_states, state_abbrevs
    user_names = ["sotiris", "kostas", "yannis", "petros"]
    state_abbrevs = {"C": "cancelled_of_user", "E": "exiting_of_user", "r": "running_of_user"}
    job_states = ["r", "E", "x", "C"]

    class Snapshot(object):
        jobs_dict = {}

    snapshot = Snapshot()
    wns_occupancy = WNOccupancy(None, None, snapshot, None, MVU_model=explicit_model())
    with pytest.raises(JobNotFound):
        wns_occupancy._create_user_job_counts(user_names, job_states, state_abbrevs)


@pytest.mark.parametrize("switches, shown", (([], True), (["-4"], False), (["--noaccounttotals"], False), (["--accounttotals"], True)))
def test_account_totals_are_shown_unless_hidden_with_4(monkeypatch, switches, shown):
    monkeypatch.setattr(sys, "argv", ["qtop"] + switches)

    args = qtop_utils.parse_qtop_cmdline_args()

    assert args.SHOW_ACCOUNT_TOTALS is shown


@pytest.mark.parametrize(
    "switch, attribute",
    (("--show-groups", "SHOW_GROUPS"), ("--expand-queues", "EXPAND_QUEUES"), ("--no-wrap-queues", "NO_WRAP_QUEUES")),
)
def test_optional_accounting_summary_switches(monkeypatch, switch, attribute):
    monkeypatch.setattr(sys, "argv", ["qtop", switch])

    args = qtop_utils.parse_qtop_cmdline_args()

    assert getattr(args, attribute) is True


def test_init_dirs_uses_parent_when_source_is_a_file(tmp_path, monkeypatch):
    source_file = tmp_path / "sinfo.txt"
    source_file.write_text("node01|debug|idle|8\n", encoding="utf-8")
    changed_to = []
    args = SimpleNamespace(SOURCEDIR=str(source_file))
    monkeypatch.setattr(qtop_module.os, "chdir", changed_to.append)

    result = init_dirs(args, str(tmp_path / "fallback"))

    assert result.SOURCEDIR == str(tmp_path)
    assert result.workdir == str(tmp_path)
    assert changed_to == [str(tmp_path)]


def test_display_account_totals_is_separate_from_user_rows(monkeypatch, capsys):
    class Args(object):
        COLOR = "OFF"
        CLASSIC = False
        SHOW_ACCOUNT_TOTALS = True

    class WNSOccupancy(object):
        account_jobs_table = [
            ["0", 2, 3, 5, "alice", 1],
            ["1", 4, 0, 4, "bob", 2],
        ]
        nodes_in_use = 2  # alice's node is one of bob's two
        user_color_patterns = {
            "0": "account_not_colored",
            "1": "account_not_colored",
        }

    args = Args()
    config = {"SEPARATOR": "|"}
    wns_occupancy = WNSOccupancy()
    display = mvu_view.TextDisplay(None, config, None, wns_occupancy, None, args, MVU_model=explicit_model(config, args))

    display.display_user_accounts_pool_mappings(wns_occupancy)
    assert "[ T] Totals" not in capsys.readouterr().out
    display.display_account_totals()
    output = capsys.readouterr().out
    assert "Account totals" in output
    assert "[ T] Totals" in output
    assert "|   9      6      3      0 |" in output
    assert "|     2 |" in output  # distinct nodes, not the 1 + 2 per-user sum


@pytest.mark.parametrize("realtime", [False, True])
@pytest.mark.parametrize("value", [None, "", " \t", False, 0])
def test_missing_user_details_command_is_optional(monkeypatch, caplog, realtime, value):
    key = "user_details_realtime" if realtime else "user_details_cache"
    info = {"field_to_use": 5}
    if value is not None:
        info[key] = value
    options = SimpleNamespace(GET_GECOS=realtime)
    config = {"extract_info": info}
    monkeypatch.setattr(qtop_module.subprocess, "Popen", lambda *args, **kwargs: pytest.fail("No command should run"))
    assert user_details.get_detail_of_name([], config, options) == {}
    assert key in caplog.text


@pytest.mark.parametrize("realtime", [False, True])
def test_configured_user_details_command_still_returns_details(monkeypatch, realtime):
    key = "user_details_realtime" if realtime else "user_details_cache"
    command = "getent passwd %s" if realtime else "cat $HOME/passwd"
    calls = []
    process = SimpleNamespace(returncode=0, communicate=lambda *args, **kwargs: ("alice:x:1000:1000:Alice Example:/home/alice:/bin/sh\n", ""))
    monkeypatch.setenv("HOME", "/tmp/qtop-user-details")
    options = SimpleNamespace(GET_GECOS=realtime)
    config = {"extract_info": {key: command}}
    monkeypatch.setattr(qtop_module.subprocess, "Popen", lambda command, **kwargs: calls.append(command) or process)
    monkeypatch.setattr(user_details, "communicate_with_timeout", lambda *args: (b"alice:x:1000:1000:Alice Example:/home/alice:/bin/sh\n", b""))
    assert user_details.get_detail_of_name([["0", 1, 0, 1, "alice", 1]], config, options) == {"alice": "Alice Example"}
    assert calls == [["getent", "passwd", "alice"] if realtime else ["cat", "/tmp/qtop-user-details/passwd"]]


@pytest.mark.parametrize("realtime", [False, True])
def test_unavailable_user_details_command_is_optional(monkeypatch, caplog, realtime):
    key = "user_details_realtime" if realtime else "user_details_cache"
    options = SimpleNamespace(GET_GECOS=realtime)
    config = {"extract_info": {key: "missing-command %s" if realtime else "missing-command"}}

    def unavailable(*args, **kwargs):
        raise FileNotFoundError("executable not found")

    monkeypatch.setattr(qtop_module.subprocess, "Popen", unavailable)
    assert user_details.get_detail_of_name([], config, options) == {}
    assert key in caplog.text
    assert "missing-command" in caplog.text


def test_display_user_accounts_pool_mappings_hides_totals_by_default(monkeypatch, capsys):
    class Args(object):
        COLOR = "OFF"
        CLASSIC = False
        SHOW_ACCOUNT_TOTALS = False

    class WNSOccupancy(object):
        account_jobs_table = [
            ["0", 2, 3, 5, "alice", 1],
            ["1", 4, 0, 4, "bob", 2],
        ]
        user_color_patterns = {
            "0": "account_not_colored",
            "1": "account_not_colored",
        }

    args = Args()
    config = {"SEPARATOR": "|"}
    wns_occupancy = WNSOccupancy()
    display = mvu_view.TextDisplay(None, config, None, wns_occupancy, None, args, MVU_model=explicit_model(config, args))

    display.display_user_accounts_pool_mappings(wns_occupancy)

    output = capsys.readouterr().out
    assert "alice" in output
    assert "[ T] Totals" not in output


def test_display_user_accounts_can_show_primary_groups(monkeypatch, capsys):
    args = SimpleNamespace(COLOR="OFF", CLASSIC=False, SHOW_ACCOUNT_TOTALS=False, SHOW_GROUPS=True)
    occupancy = SimpleNamespace(
        account_jobs_table=[["0", 2, 3, 5, "alice", 1]],
        user_color_patterns={"0": "account_not_colored"},
    )
    config = {"SEPARATOR": "|"}
    monkeypatch.setattr(user_details, "get_primary_group_names", lambda table: {"alice": "research"})
    model = explicit_model(config, args)
    user_details.MVU_resolve_user_details(model, occupancy, force=True)

    mvu_view.TextDisplay(None, config, None, occupancy, None, args, MVU_model=model).display_user_accounts_pool_mappings(occupancy)

    output = capsys.readouterr().out
    assert "primary group" in output
    assert "research" in output


@pytest.mark.parametrize("color", ["White", "Gray2BG", "Red_LOnGrayBG", "reset", "unknown"])
@pytest.mark.parametrize("mode", ["ON", "OFF"])
@pytest.mark.parametrize("groups", [None, {"alice": "research"}])
def test_account_row_colors_preserve_visible_alignment(monkeypatch, capsys, color, mode, groups):
    args = SimpleNamespace(COLOR=mode)
    config = {"SEPARATOR": "|"}
    monkeypatch.setitem(color_to_code, "Gray2BG", ";47")
    display = mvu_view.TextDisplay(None, config, None, None, None, args, MVU_model=explicit_model(config, args, user_to_color={"user": color}))
    line = ["0", 2, 3, 5, "alice", 1]
    display.display_account_jobs_line(line, "user", {"alice": "Alice"}, groups)
    rendered = capsys.readouterr().out
    args.COLOR = "OFF"
    display.display_account_jobs_line(line, "user", {"alice": "Alice"}, groups)
    plain = capsys.readouterr().out
    assert re.sub(r"\x1b\[[0-9;]*m", "", rendered) == plain


def test_strict_check_counts_unique_jobs_across_slots_nodes_and_shared_resources():
    nodes = [
        {"core_job_map": {0: "101", 1: "101"}, "oar_job_ids": ["101", "102"]},
        {"core_job_map": {0: "101", 1: "103", 2: "?"}},
    ]
    assert WNOccupancy._count_jobs_strict(nodes) == 3
    arrays = [{"core_job_map": {0: "42[1]", 1: "42[2]"}}]
    assert WNOccupancy._count_jobs_strict(arrays, {"42"}) == 1
    assert WNOccupancy._count_jobs_strict(arrays, {"42[1]", "42[2]"}) == 2
    snapshot = SimpleNamespace(worker_nodes=nodes, jobs_dict={"101": SimpleNamespace(job_state="R"), "102": SimpleNamespace(job_state="R"), "103": SimpleNamespace(job_state="F")})
    config = {"state_abbreviations": {"oar": {"R": "running_of_user", "F": "Finishing"}}}
    occupancy = SimpleNamespace(snapshot=snapshot, config=config, MVU_model=explicit_model(config, scheduler="oar"))
    assert WNOccupancy.MVU_check_jobs(occupancy, SimpleNamespace(total_running_jobs=2)) is None
    assert "Counted jobs (2)" in WNOccupancy.MVU_check_jobs(occupancy, SimpleNamespace(total_running_jobs=3))
    snapshot.jobs_dict = {"101": SimpleNamespace(job_state="r"), "102": SimpleNamespace(job_state="Rr"), "103": SimpleNamespace(job_state="s")}
    occupancy.config = {"state_abbreviations": {"sge": {"r": "running_of_user", "Rr": "running_of_user", "s": "suspended_of_user"}}}
    occupancy.MVU_model = explicit_model(occupancy.config, scheduler="sge")
    assert WNOccupancy.MVU_check_jobs(occupancy, SimpleNamespace(total_running_jobs=2)) is None


def test_queueing_summary_preserves_order_and_expands(capsys):
    class Cluster(object):
        total_running_jobs = 2
        total_queued_jobs = 1
        queues_dict = {
            "compute": SimpleNamespace(run="1", queued="1", state="E"),
            "debug": SimpleNamespace(run="1", queued="0", state="E"),
        }
        total_wn = 2
        offdown_nodes = 0
        available_wn = 1
        working_cores = 2
        total_cores = 4

    args = SimpleNamespace(REMAP=False, CLASSIC=False, WATCH=True, EXPAND_QUEUES=True, COLOR="OFF")
    model = MVU_Model({}, args, None, backend=qtop_module.scheduler_backend("slurm"))
    mvu_view.TextDisplay(None, {}, None, None, Cluster(), args, MVU_model=model).display_job_accounting_summary(Cluster(), None)

    output = capsys.readouterr().out
    assert "Queueing :\n             Total   R   Q   Other\n  compute  :     2 = 1 + 1 +     0\n  debug    :     1 = 1 + 0 +     0\n" in output


@pytest.mark.parametrize(
    "queues, watch, overrides, expanded",
    ((150, True, {}, True), (2, True, {}, False), (150, False, {}, False), (150, True, {"queue_summary_expanded": False}, False)),
)
def test_queue_summary_expands_in_watch_mode_when_wider_than_the_terminal(capsys, queues, watch, overrides, expanded):
    queues_dict = {"queue_%03d" % index: SimpleNamespace(run="1", queued="2", state="E") for index in range(queues)}
    cluster = SimpleNamespace(total_running_jobs=0, total_queued_jobs=0, queues_dict=queues_dict, total_wn=0, offdown_nodes=0, available_wn=0, working_cores=0, total_cores=0)
    args = SimpleNamespace(REMAP=False, CLASSIC=False, WATCH=watch, ANONYMIZE=True, NO_WRAP_QUEUES=False, COLOR="OFF")
    viewport = Viewport()
    viewport.set_term_size(40, 100)
    model = MVU_Model({}, args, viewport, backend=qtop_module.scheduler_backend("pbs"), display_overrides=overrides)
    mvu_view.TextDisplay(None, {}, viewport, None, cluster, args, MVU_model=model).display_job_accounting_summary(cluster, None)

    output = capsys.readouterr().out
    assert ("\n  queue_000  :     3 = 1 + 2 +     0\n" in output) is expanded
    assert ("Queueing : queue_000: 1 + 2 |" in output) is not expanded


@pytest.mark.parametrize("color", ("OFF", "ON"))
def test_expanded_queues_align_and_count_other_jobs_per_queue(capsys, monkeypatch, color):
    monkeypatch.setenv("TERM", "xterm-256color")
    queues = {"batch": SimpleNamespace(run="2", queued="1", state="E"), "q": SimpleNamespace(run="0", queued="105", state="D")}
    listing = (("R", "batch"), ("R", "batch"), ("Q", "batch"), ("C", "batch"), ("E", "batch"), ("W", "q"))  # q's W is in its 105 already
    jobs = {str(n): SimpleNamespace(job_state=state, job_queue=queue) for n, (state, queue) in enumerate(listing)}
    cluster = SimpleNamespace(total_running_jobs=12, total_queued_jobs=108, queues_dict=queues, total_wn=0, offdown_nodes=0, available_wn=0, working_cores=0, total_cores=0)
    args = SimpleNamespace(REMAP=False, CLASSIC=False, WATCH=False, EXPAND_QUEUES=True, ANONYMIZE=True, COLOR=color)
    model = MVU_Model({}, args, None, backend=qtop_module.scheduler_backend("pbs"))
    config = {"state_abbreviations": {"pbs": PBS_STATES}}
    mvu_view.TextDisplay(SimpleNamespace(jobs_dict=jobs), config, None, None, cluster, args, MVU_model=model).display_job_accounting_summary(
        cluster, SimpleNamespace(jobs_dict=jobs)
    )

    lines = [re.sub(r"\x1b\[[0-9;]*m", "", line) for line in capsys.readouterr().out.splitlines()]
    start = lines.index("Queueing :")
    assert lines[start + 1 : start + 4] == ["           Total   R     Q   Other", "  batch  :     5 = 2 +   1 +     2", "  q*     :   105 = 0 + 105 +     0"]


def test_demo_notice_uses_high_contrast_blue(monkeypatch):
    class Cluster(object):
        total_running_jobs = 0
        total_queued_jobs = 0
        queues_dict = {}
        total_wn = 0
        offdown_nodes = 0
        available_wn = 0
        working_cores = 0
        total_cores = 0

    args = SimpleNamespace(REMAP=False, CLASSIC=False, WATCH=True, COLOR="OFF")
    model = MVU_Model({}, args, None, backend=qtop_module.scheduler_backend("demo"))
    color_calls = []

    def record_color(text, color_func=None, *unused_args, **unused_kwargs):
        color_calls.append((text, color_func))
        return text

    monkeypatch.setattr(model, "MVU_colorize", record_color)

    display = mvu_view.TextDisplay(None, {}, None, None, Cluster(), args, MVU_model=model)
    display.display_job_accounting_summary(Cluster(), None)

    demo_calls = [(text, color) for text, color in color_calls if text.startswith("This data is simulated.")]
    assert demo_calls == [
        (
            "This data is simulated. As soon as you connect to one of the supported scheduling systems,\nyou will see live data from your cluster. Press q to Quit.",
            "Blue_L",
        )
    ]


@pytest.mark.parametrize(
    "scheduler, watch, color, key_style",
    (("demo", True, "ON", "\x1b[1;33m"), ("demo", True, "OFF", "\x1b[1m"), ("demo", False, "OFF", None), ("slurm", True, "OFF", "\x1b[1m"), ("slurm", False, "ON", None)),
)
def test_watch_mode_lists_keys_above_the_summary(capsys, scheduler, watch, color, key_style):
    cluster = SimpleNamespace(total_running_jobs=0, total_queued_jobs=0, queues_dict={}, total_wn=0, offdown_nodes=0, available_wn=0, working_cores=0, total_cores=0)
    args = SimpleNamespace(REMAP=False, CLASSIC=False, WATCH=watch, ANONYMIZE=True, COLOR=color)
    model = MVU_Model({}, args, None, backend=qtop_module.scheduler_backend(scheduler))
    mvu_view.TextDisplay(None, {}, None, None, cluster, args, MVU_model=model).display_job_accounting_summary(cluster, None)

    lines = capsys.readouterr().out.splitlines()
    plain = [re.sub(r"\x1b\[[0-9;]*m", "", line) for line in lines]
    summary = next(index for index, line in enumerate(plain) if "Job accounting summary" in line)
    assert ("\n".join(plain[summary - 2 : summary]) == mvu_view.WATCH_KEYS_TEXT) is (key_style is not None)
    assert "\n".join(plain).count(mvu_view.WATCH_KEYS_TEXT) == (key_style is not None)
    if key_style:
        keys = ("\x1b[1;34mq\x1b[0;m\x1b[1;33m quit", "\x1b[1;34m1,2,3,4\x1b[0;m") if color == "ON" else ("\x1b[4mq\x1b[24m quit", "\x1b[4m1,2,3,4\x1b[24m")
        assert lines[summary - 2].startswith(key_style + "Keys: ")
        assert keys[0] in lines[summary - 2] and keys[1] in lines[summary - 1]
        assert plain[summary - 2].endswith("s sort,") and plain[summary - 1].startswith("      Q queue layout,")  # keys stand out from their labels


def test_available_possible_ids_filters_reserved_user_symbols():
    config = user_symbol_config(possible_ids=SYMBOL_POSSIBLE_IDS_WITH_RESERVED)

    assert configuration.available_possible_ids(config) == ["0", "1"]


def test_compress_colored_line_groups_adjacent_colors_and_accepts_empty_input():
    red = "\x1b[31m"
    blue = "\x1b[34m"
    reset = "\x1b[0;m"

    assert qtop_module.compress_colored_line("") == ""
    assert qtop_module.compress_colored_line("dn0foo|0350__####|-|tu |") == "dn0foo|0350__####|-|tu |"
    assert qtop_module.compress_colored_line(red + "A" + reset + red + "B" + reset + blue + "C" + reset) == red + "AB" + reset + blue + "C" + reset


def test_normalize_config_returns_a_runtime_copy():
    raw_config = {
        "possible_ids": "01",
        "transpose_wn_matrices": "False",
        "fill_with_user_firstletter": "True",
        "faster_xml_parsing": "False",
        "vertical_separator_every_X_columns": "5",
        "overwrite_sample_file": "False",
        "sorting": {"reverse": "True"},
        "vertical_separator": "'|'",
        "workernodes_matrix": [{"wn id lines": {"alt_label_colors": ["White, Blue_L"], "user_cut_matrix_width": "12"}}],
    }

    normalized = configuration.normalize_config(raw_config)

    assert raw_config["possible_ids"] == "01"
    assert raw_config["sorting"]["reverse"] == "True"
    assert normalized["possible_ids"] == ["0", "1"]
    assert normalized["sorting"]["reverse"] is True
    assert normalized["ALT_LABEL_COLORS"] == ["White", "Blue_L"]
    assert normalized["SEPARATOR"] == "|"
    assert normalized["USER_CUT_MATRIX_WIDTH"] == 12


def test_load_yaml_config_keeps_user_symbol_pool_bounded(tmp_path):
    config, _, _ = configuration.load_yaml_config(
        SimpleNamespace(CONFFILE=None), str(Path(__file__).resolve().parents[1]), str(tmp_path), str(tmp_path / "etc"), str(tmp_path / "user")
    )

    assert config["possible_ids"] == list("0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz")
    assert "}" not in config["possible_ids"]


def test_user_symbols_use_asterisk_for_long_tail_users():
    wns_occupancy = WNOccupancy.__new__(WNOccupancy)
    wns_occupancy.config = user_symbol_config(
        fill_with_user_firstletter=False,
        possible_ids=SYMBOL_POSSIBLE_IDS_WITH_RESERVED[:-1],
    )

    user_lot = [
        ("alice", 4),
        ("bob", 3),
        ("charlie", 2),
        ("dora", 1),
    ]
    user_to_id = wns_occupancy._create_id_for_users(user_lot)

    assert [str(user_to_id[user]) for user, _ in user_lot] == ["0", "1", SYMBOL_LONG_TAIL_USER, SYMBOL_LONG_TAIL_USER]


def test_firstletter_user_symbols_do_not_reuse_reserved_markers():
    wns_occupancy = WNOccupancy.__new__(WNOccupancy)
    wns_occupancy.config = user_symbol_config(fill_with_user_firstletter=True, possible_ids=["0", "1"])

    user_lot = [
        (SYMBOL_UNUSED_CORE + "hidden", 4),
        (SYMBOL_NON_EXISTENT_NODE + "system", 3),
        (SYMBOL_UNKNOWN_NODE_STATE + "unknown", 2),
        (SYMBOL_LONG_TAIL_USER + "wildcard", 1),
        ("alice", 1),
    ]
    user_to_id = wns_occupancy._create_id_for_users(user_lot)

    assert str(user_to_id[SYMBOL_UNUSED_CORE + "hidden"]) == SYMBOL_LONG_TAIL_USER
    assert str(user_to_id[SYMBOL_NON_EXISTENT_NODE + "system"]) == SYMBOL_LONG_TAIL_USER
    assert str(user_to_id[SYMBOL_UNKNOWN_NODE_STATE + "unknown"]) == SYMBOL_LONG_TAIL_USER
    assert str(user_to_id[SYMBOL_LONG_TAIL_USER + "wildcard"]) == SYMBOL_LONG_TAIL_USER
    assert str(user_to_id["alice"]) == "a"


def test_account_jobs_table_preserves_precomputed_user_symbols():
    wns_occupancy = WNOccupancy.__new__(WNOccupancy)
    user_to_id = {
        "overflow": qtop_utils.ColorStr(SYMBOL_LONG_TAIL_USER),
        "alice": qtop_utils.ColorStr("0"),
    }
    account_jobs_table = [
        ["placeholder", 2, 0, 2, "overflow", 1],
        ["placeholder", 1, 0, 1, "alice", 1],
    ]

    account_jobs_table, user_to_id = wns_occupancy._create_account_jobs_table(user_to_id, account_jobs_table)

    assert str(account_jobs_table[0][0]) == SYMBOL_LONG_TAIL_USER
    assert account_jobs_table[0][0].color == "Red_L"
    assert str(account_jobs_table[1][0]) == "0"
    assert account_jobs_table[1][0].color == "Red_L"
    assert str(user_to_id["overflow"]) == SYMBOL_LONG_TAIL_USER


def test_long_tail_symbol_is_not_colored_like_a_single_user():
    wns_occupancy = WNOccupancy.__new__(WNOccupancy)
    wns_occupancy.config = user_symbol_config()
    wns_occupancy.account_jobs_table = [
        [qtop_utils.ColorStr("0"), 1, 0, 1, "alice", 1],
        [qtop_utils.ColorStr(SYMBOL_LONG_TAIL_USER), 1, 0, 1, "overflow", 1],
    ]

    pattern = wns_occupancy.make_pattern_out_of_mapping({"alice": "Red_L"})

    assert pattern["0"] == "alice"
    assert pattern[SYMBOL_LONG_TAIL_USER] == "account_not_colored"
    assert pattern[SYMBOL_NON_EXISTENT_NODE] == SYMBOL_NON_EXISTENT_NODE
    assert pattern[SYMBOL_UNUSED_CORE] == SYMBOL_UNUSED_CORE
    assert pattern[SYMBOL_SEPARATOR] == "account_not_colored"


@pytest.mark.parametrize(
    "cmdline_switch, env_var, config_file_batch_option, returned_scheduler",
    (
        (None, None, "sge", "sge"),
        (None, "oar", "sge", "oar"),
        ("sge", None, None, "sge"),
        ("oar", None, "sge", "oar"),
        ("sge", None, "auto", "sge"),
        ("sge", "auto", None, "sge"),
        ("oar", "pbs", "sge", "oar"),
    ),
)
def test_get_selected_batch_system(cmdline_switch, env_var, config_file_batch_option, returned_scheduler):
    schedulers = ["sge", "oar", "pbs"]
    available_batch_systems = {name: qtop_module.scheduler_backend(name) for name in schedulers}
    assert (
        decide_batch_system(
            cmdline_switch,
            env_var,
            config_file_batch_option,
            schedulers,
            available_batch_systems,
            config,
        )
        == returned_scheduler
    )


@pytest.mark.parametrize(
    "cmdline_switch, env_var, config_file_batch_option, returned_scheduler",
    (
        ("auto", None, "sge", "should_raise_SchedulerNotSpecified"),
        ("auto", "pbs", "sge", "should_raise_SchedulerNotSpecified"),
        (None, "auto", "sge", "should_raise_SchedulerNotSpecified"),
        (None, None, "auto", "should_raise_SchedulerNotSpecified"),
    ),
)
def test_get_selected_batch_system_raises_scheduler_not_specified(
    cmdline_switch,
    env_var,
    config_file_batch_option,
    returned_scheduler,
):
    schedulers = ["sge", "oar", "pbs"]
    available_batch_systems = {name: qtop_module.scheduler_backend(name) for name in schedulers}
    config = {"signature_commands": {"pbs": "pbsnodes", "oar": "oarnodes", "sge": "qhost", "demo": "echo"}}

    with pytest.raises(SchedulerNotSpecified):
        decide_batch_system(
            cmdline_switch,
            env_var,
            config_file_batch_option,
            schedulers,
            available_batch_systems,
            config,
        )


@pytest.mark.parametrize(
    "cmdline_switch, env_var, config_file_batch_option, returned_scheduler",
    (
        (None, None, None, "should_raise_NoSchedulerFound"),
        (None, None, "NotAScheduler", "should_raise_NoSchedulerFound"),
    ),
)
def test_get_selected_batch_system_raises_no_scheduler_found(
    cmdline_switch,
    env_var,
    config_file_batch_option,
    returned_scheduler,
):
    schedulers = ["sge", "oar", "pbs"]
    available_batch_systems = {name: qtop_module.scheduler_backend(name) for name in schedulers}
    with pytest.raises(NoSchedulerFound):
        decide_batch_system(
            cmdline_switch,
            env_var,
            config_file_batch_option,
            schedulers,
            available_batch_systems,
            config,
        )


@pytest.mark.parametrize(
    "s, now, day_meant",
    (
        (
            "21:00",
            datetime.datetime(year=2016, month=11, day=20, hour=1, minute=10, second=0),
            datetime.datetime(year=2016, month=11, day=19, hour=20, minute=10, second=0).day,
        ),
        (
            "21:00",
            datetime.datetime(year=2016, month=11, day=20, hour=22, minute=10, second=0),
            datetime.datetime(year=2016, month=11, day=20, hour=20, minute=10, second=0).day,
        ),
    ),
)
def test_get_date_obj_from_str(s, now, day_meant):
    """
    Two cases:
    at 01:00 in the morning, the user inputs 21:00 (the previous day is implied)
    at 22:10 at night, the user inputs again 21:00 (the same day is implied)
    """
    assert get_date_obj_from_str(s, now).day == day_meant


@pytest.mark.parametrize(
    "now,expected",
    (
        ((2026, 11, 1, 1), (2026, 10, 31, 21)),
        ((2027, 1, 1, 1), (2026, 12, 31, 21)),
        ((2028, 3, 1, 1), (2028, 2, 29, 21)),
        ((2026, 3, 1, 1), (2026, 2, 28, 21)),
        ((2026, 11, 1, 22), (2026, 11, 1, 21)),
    ),
)
def test_short_replay_time_handles_calendar_boundaries(now, expected):
    assert get_date_obj_from_str("2100", datetime.datetime(*now)) == datetime.datetime(*expected)


@pytest.mark.parametrize("value, expected", (("11-18T18:23", (2028, 11, 18, 18, 23)), ("0229T1200", (2028, 2, 29, 12, 0))))
def test_month_day_replay_time_implies_the_current_year(value, expected):
    """Leap day included: parsing without the year used 1900, which has no 29 February."""
    assert get_date_obj_from_str(value, datetime.datetime(2028, 12, 1, 9)) == datetime.datetime(*expected)


@pytest.mark.parametrize(
    "regex, field, expected",
    (
        ("re.search('^([^,]*)', field).group(1)", "Alice Example, Room 7", "Alice Example"),
        ('re.search("(R[a-z]+ [0-9]+)", field).group(1)', "Alice Example, Room 7", "Room 7"),
        ("field.split(',')[0]", "Alice", ValueError),  # only re.search(...).group(n) is accepted
        ("re.search('^Bob', field).group(0)", "Alice", ValueError),  # no match
    ),
)
def test_user_detail_regex_expressions(regex, field, expected):
    if expected is ValueError:
        with pytest.raises(ValueError):
            extract_regex_detail(regex, field)
    else:
        assert extract_regex_detail(regex, field) == expected


def test_user_details_apply_the_regex_and_keep_unmatched_fields_whole(monkeypatch, caplog):
    passwd = b"alice:x:1:1:Alice Example, Room 7:/home/alice:/bin/sh\nbob:x:2:2:Bob:/home/bob:/bin/sh\nmalformed\n"
    monkeypatch.setattr(user_details, "start_command", lambda *args, **kwargs: SimpleNamespace(returncode=0))
    monkeypatch.setattr(user_details, "communicate_with_timeout", lambda *args: (passwd, b"getent: partial result\n"))
    config = {"extract_info": {"user_details_cache": "cat passwd", "regex": "re.search('^([^,]*),', field).group(1)"}}
    assert user_details.get_detail_of_name([], config, SimpleNamespace(GET_GECOS=False)) == {"alice": "Alice Example", "bob": "Bob"}
    assert "getent: partial result" in caplog.text


def test_primary_groups_resolve_known_users_and_skip_unknown_ones(monkeypatch, caplog):
    grp, pwd = pytest.importorskip("grp"), pytest.importorskip("pwd")
    me = pwd.getpwuid(os.getuid()).pw_name
    user_details.get_primary_group_name.cache_clear()
    table = [["0", 1, 0, 1, me, 1], ["0", 1, 0, 1, "qtop-no-such-user", 1], ["", 2, 0, 2, "Totals", 2]]
    assert user_details.get_primary_group_names(table) == {me: grp.getgrgid(pwd.getpwnam(me).pw_gid).gr_name}
    assert "qtop-no-such-user" in caplog.text
    user_details.get_primary_group_name.cache_clear()
    monkeypatch.setitem(sys.modules, "grp", None)  # as on Windows: no Unix group database
    assert user_details.get_primary_group_names(table) == {}
    assert "unavailable on this platform" in caplog.text
    user_details.get_primary_group_name.cache_clear()


def test_unknown_scheduler_job_state_preserves_accounting(caplog):
    state_abbrevs = {"R": "running_of_user", "PD": "queued_of_user"}
    occupancy = WNOccupancy.__new__(WNOccupancy)
    occupancy.config = {"state_abbreviations": {"slurm": state_abbrevs}}
    occupancy.MVU_model = explicit_model(occupancy.config, scheduler="slurm")
    users = ["alice", "bob", "alice"]
    counts = occupancy._calculate_user_job_counts(users, ["R", "NEW_STATE", "PD"])
    assert counts["running_of_user"] == {"alice": 1, "bob": 0}
    assert counts["queued_of_user"] == {"alice": 1, "bob": 0}
    assert counts["unknown_of_user"] == {"alice": 0, "bob": 1}
    assert state_abbrevs == {"R": "running_of_user", "PD": "queued_of_user"}
    assert "Unknown job state NEW_STATE" in caplog.text


@pytest.mark.parametrize(
    "firstletter, alphabet, user_ids, expected",
    (
        (False, ["z", "a", "0"], {"alice": "0", "bob": "a", "carol": "z", "zoe": "*", "dave": "*"}, ["carol", "bob", "alice", "dave", "zoe"]),
        (True, ["z", "a", "0"], {"alice": "a", "adam": "a", "bob": "b", "zoe": "*", "dave": "*"}, ["adam", "alice", "bob", "dave", "zoe"]),
        (False, ["0", "1"], {"alice": "0", "adam": "0", "bob": "1", "zoe": "*", "dave": "*"}, ["adam", "alice", "bob", "dave", "zoe"]),
    ),
)
def test_section_three_orders_by_displayed_id_then_username(firstletter, alphabet, user_ids, expected):
    occupancy = WNOccupancy.__new__(WNOccupancy)
    occupancy.config = user_symbol_config(fill_with_user_firstletter=firstletter, possible_ids=alphabet)
    users = list(user_ids)
    # Job counts deliberately disagree with both ID order and username order.
    totals = dict((user, index + 1) for index, user in enumerate(users))
    occupancy.user_machine_use = dict((user, 1) for user in users)
    counts = {"running_of_user": totals, "queued_of_user": dict((user, 0) for user in users)}
    symbols = dict((user, qtop_utils.ColorStr(symbol)) for user, symbol in user_ids.items())
    table = occupancy._create_sort_acct_jobs_table(counts, list(totals.items()), symbols)
    table, symbols = occupancy._create_account_jobs_table(symbols, table)
    assert [row[4] for row in table] == expected
    assert dict((row[4], str(row[0])) for row in table) == user_ids
    assert dict((row[4], row[3]) for row in table) == totals
    assert all(row[0].color == "Red_L" for row in table)
    # Input order cannot override the alphabetical tie-breaker.
    reversed_table = occupancy._create_sort_acct_jobs_table(counts, list(reversed(list(totals.items()))), symbols)
    assert [row[4] for row in reversed_table] == expected


@pytest.mark.parametrize("totals, expected_pattern", (({"alice": 1, "adam": 5}, "alice"), ({"alice": 3, "adam": 3}, "adam")))
def test_shared_firstletter_id_keeps_legacy_color_mapping(totals, expected_pattern):
    occupancy = WNOccupancy.__new__(WNOccupancy)
    occupancy.config = user_symbol_config(fill_with_user_firstletter=True, possible_ids=["0", "1"])
    occupancy.user_machine_use = dict((user, 1) for user in totals)
    counts = {"running_of_user": totals, "queued_of_user": dict((user, 0) for user in totals)}
    symbols = occupancy._create_id_for_users(list(totals.items()))
    occupancy.account_jobs_table = occupancy._create_sort_acct_jobs_table(counts, list(totals.items()), symbols)
    assert [row[4] for row in occupancy.account_jobs_table] == ["adam", "alice"]
    patterns = occupancy.make_pattern_out_of_mapping({"alice": "Green", "adam": "Blue"})
    assert patterns["a"] == expected_pattern
    assert all(str(symbol) == "a" for symbol in symbols.values())


@pytest.mark.parametrize("dynamic", [False, True])
@pytest.mark.parametrize(
    "rule, expected",
    [
        ({"include_name_patterns": ["^node03$"]}, ["node03"]),
        ({"exclude_name_patterns": ["^N/A$"]}, ["node01", "node03"]),
    ],
)
def test_node_filters_enable_compact_mapping(dynamic, rule, expected):
    nodes = [{"domainname": name, "state": [qtop_utils.ColorStr("-")], "np": 2, "core_job_map": {0: job}} for name, job in [("node01", "100"), ("node03", "200")]]
    config = {
        "filtering": [] if dynamic else [rule],
        "sorting": {},
        "remapping": [],
        "percentage": 0.8,
        "exotic_starting_wn_nr": 9000,
        "workernodes_matrix": [{"wn id lines": {"max_len": 0}}],
    }
    snapshot = SimpleNamespace(jobs_dict={}, queues_dict={}, total_running_jobs=2, total_queued_jobs=0)
    args = SimpleNamespace(ANONYMIZE=False, BLINDREMAP=False)

    model = explicit_model(config, args, display_overrides={"filtering": [rule]} if dynamic else {})
    cluster = Cluster(snapshot, nodes, WNFilter, config, args, MVU_model=model)

    assert args.REMAP is True
    assert [node["domainname"] for node in cluster.workernode_dict.values()] == expected
    assert cluster.total_wn == cluster.available_wn == len(expected)
    assert cluster.working_cores == len(expected)
    assert cluster.total_cores == 2 * len(expected)


@pytest.mark.parametrize("filters", [None, [], [{}]])
def test_empty_dynamic_filters_do_not_force_remapping(filters):
    cluster = Cluster.__new__(Cluster)
    cluster.MVU_model = explicit_model(display_overrides={"filtering": filters})
    cluster.config = {"filtering": [{"include_name_patterns": ["node"]}], "percentage": 0.8, "exotic_starting_wn_nr": 9000}
    cluster.args = SimpleNamespace(BLINDREMAP=False)
    cluster.total_wn = 2
    cluster.workernode_list = [1, 3]
    cluster.offdown_nodes = 0

    assert cluster.decide_remapping(["01", "03"], {"node"}) is False


@pytest.mark.parametrize("value", ["", "abc", "12345", "20260101T1200"])
def test_unsupported_date_formats_raise_value_error(value):
    with pytest.raises(ValueError, match="datetime format provided is incorrect"):
        get_date_obj_from_str(value, datetime.datetime(2026, 10, 25, 12))


def test_output_size_with_known_dimensions_does_not_read_file(tmp_path):
    assert get_output_size(7, str(tmp_path / "missing.out"), 9) == (9, 7)


@pytest.mark.parametrize("width, height", [(0, 0), (8, 0), (0, 8)])
def test_output_size_rejects_empty_file_when_dimensions_are_missing(tmp_path, width, height):
    output = tmp_path / "empty.out"
    output.write_text("")
    with pytest.raises(ValueError, match="no output"):
        get_output_size(width, str(output), height)


@pytest.mark.parametrize("verbose, expected", [(None, logging.WARNING), (0, logging.WARNING), (-1, logging.WARNING), (1, logging.INFO), (2, logging.DEBUG), (3, logging.DEBUG)])
def test_init_logging_sets_file_level(tmp_path, monkeypatch, verbose, expected):
    monkeypatch.setattr(qtop_utils, "QTOP_LOGFILE", str(tmp_path / "qtop.log"))
    logger = logging.getLogger()
    old_handlers = list(logger.handlers)
    old_level, old_disabled = logger.level, logger.disabled
    try:
        qtop_utils.init_logging(SimpleNamespace(verbose=verbose, DEBUG=False))
        added = [handler for handler in logger.handlers if handler not in old_handlers]
        assert next(handler for handler in added if isinstance(handler, logging.FileHandler)).level == expected
    finally:
        for handler in list(logger.handlers):
            if handler not in old_handlers:
                logger.removeHandler(handler)
                handler.close()
        logger.setLevel(old_level)
        logger.disabled = old_disabled


@pytest.mark.parametrize("invalid_utf8", (False, True), ids=("large-frame", "decode-failure"))
def test_viewport_closes_its_output_on_success_and_failure(tmp_path, monkeypatch, invalid_utf8):
    source = tmp_path / "frame with spaces.out"
    source.write_bytes(b"\xff" if invalid_utf8 else "".join("%s\n" % number for number in range(100000)).encode())
    display = mvu_view.TextDisplay.__new__(mvu_view.TextDisplay)
    display.config = {"savepath": str(tmp_path)}
    temporary_file = mvu_view.tempfile.NamedTemporaryFile
    outputs = []

    def track_file(**kwargs):
        output = temporary_file(**kwargs)
        outputs.append(output)
        return output

    monkeypatch.setattr(mvu_view.tempfile, "NamedTemporaryFile", track_file)
    if invalid_utf8:
        with pytest.raises(UnicodeDecodeError):
            display.show_part_view("test", str(source), 2, 3)
    else:
        result = display.show_part_view("test", str(source), 2, 3)
        assert Path(result).read_text() == "2\n3\n"
    assert len(outputs) == 1 and outputs[0].closed


@pytest.mark.parametrize("failure", ["timeout", "exit", "quoting"])
def test_user_details_failures_are_optional_and_processes_reaped(tmp_path, monkeypatch, caplog, failure):
    import shlex
    import time

    script = tmp_path / "user lookup.py"
    script.write_text(
        "import sys, time\n"
        + ("time.sleep(10)\n" if failure == "timeout" else "print('alice:x:1:1:Partial Name:/home/alice:/bin/sh')\nprint('lookup unavailable', file=sys.stderr)\nsys.exit(7)\n")
    )
    command = " ".join(shlex.quote(value) for value in (sys.executable, str(script))) if failure != "quoting" else "unterminated '"
    options = SimpleNamespace(GET_GECOS=False)
    config = {"extract_info": {"user_details_cache": command}, "scheduler_command_timeout": 1}  # 0.2 s flaked under parallel load
    processes = []
    popen = qtop_module.subprocess.Popen

    def track(*args, **kwargs):
        process = popen(*args, **kwargs)
        processes.append(process)
        return process

    monkeypatch.setattr(qtop_module.subprocess, "Popen", track)
    started = time.monotonic()
    assert user_details.get_detail_of_name([], config, options) == {}
    assert time.monotonic() - started < 5
    assert all(process.returncode is not None for process in processes)
    assert "skipping user details" in caplog.text


@pytest.mark.parametrize("realtime", [False, True])
def test_user_details_preserve_quoted_arguments_and_skip_malformed_rows(tmp_path, monkeypatch, realtime):
    import shlex

    script = tmp_path / "user lookup.py"
    script.write_text("import sys\nassert sys.argv[1:] == ['alice smith']\nprint('malformed')\nprint('alice smith:x:1:1:Alice Smith:/home/alice:/bin/sh')\n")
    command = " ".join(shlex.quote(value) for value in (sys.executable, str(script))) + (" %s" if realtime else " 'alice smith'")
    key = "user_details_realtime" if realtime else "user_details_cache"
    options = SimpleNamespace(GET_GECOS=realtime)
    config = {"extract_info": {key: command}}
    assert user_details.get_detail_of_name([["0", 1, 0, 1, "alice smith", 1]], config, options) == {"alice smith": "Alice Smith"}


PBS_STATES = {"R": "running_of_user", "Q": "queued_of_user", "C": "cancelled_of_user", "E": "exiting_of_user", "X": "expired_of_user"}


def _plain_accounts(monkeypatch, capsys, config, table, totals=False, color="OFF", nodes_in_use=2):
    args = SimpleNamespace(COLOR=color, CLASSIC=False, SHOW_ACCOUNT_TOTALS=totals, SHOW_GROUPS=False)
    occupancy = SimpleNamespace(account_jobs_table=table, nodes_in_use=nodes_in_use, user_color_patterns={str(row[0]): "account_not_colored" for row in table})
    config = dict(config, SEPARATOR="|")
    display = mvu_view.TextDisplay(None, config, None, occupancy, None, args, MVU_model=explicit_model(config, args))
    display.display_user_accounts_pool_mappings(occupancy)
    rows = capsys.readouterr().out.splitlines()
    if totals:
        display.display_account_totals()
        rows += capsys.readouterr().out.splitlines()[-1:]
    return rows


@pytest.mark.parametrize("color, tty, style", (("ON", False, "colour"), ("OFF", True, "reverse"), ("OFF", False, None)))
def test_totals_row_counts_take_the_summary_colours(monkeypatch, capsys, color, tty, style):
    monkeypatch.setenv("TERM", "xterm-256color")
    monkeypatch.setattr(sys, "__stdout__", SimpleNamespace(isatty=lambda: tty))
    rows = [["0", 2, 3, 6, "alice", 1, {"running_of_user": 2, "queued_of_user": 3}], ["1", 1, 0, 1, "bob", 1, {"running_of_user": 1}]]
    totals = _plain_accounts(monkeypatch, capsys, {"accounts_states": ["running_of_user", "queued_of_user", "other_of_user"]}, rows, totals=True, color=color)[-1]
    codes = {
        "colour": ("\x1b[1;37;44m%s\x1b[0;m", "\x1b[0;30;42m%s\x1b[0;m", "\x1b[0;30;48;5;208m%s\x1b[0;m", "\x1b[0;30;43m%s\x1b[0;m"),
        "reverse": ("\x1b[7m%s\x1b[0m",) * 4,
        None: ("%s",) * 4,
    }[style]
    assert "|   %s      %s      %s      %s |" % tuple(code % count for code, count in zip(codes, (7, 3, 3, 1))) in totals


def test_account_state_columns_default_to_r_q_and_other_for_old_and_new_rows(monkeypatch, capsys):
    old_row = ["0", 2, 3, 6, "alice", 1]
    new_row = ["0", 2, 3, 6, "alice", 1, {"running_of_user": 2, "queued_of_user": 3, "cancelled_of_user": 1}]
    old, new = (_plain_accounts(monkeypatch, capsys, {}, [row]) for row in (old_row, new_row))
    assert old == new
    assert "|jobs >=   R +    Q +    O | nodes |" in new[-2]
    assert new[-1].startswith("[ 0] alice             |   6      2      3      1 |     1 |")  # O: the cancelled job


def test_account_state_columns_are_tunable_with_labels_and_other(monkeypatch, capsys):
    config = {"accounts_states": ["running_of_user", "cancelled_of_user", "queued_of_user", {"expired_of_user": "X"}, "other_of_user"]}
    row = ["0", 2, 3, 9, "alice", 1, {"running_of_user": 2, "queued_of_user": 3, "cancelled_of_user": 1, "exiting_of_user": 3}]
    lines = _plain_accounts(monkeypatch, capsys, config, [row, ["1", 1, 0, 1, "bob", 1, {"running_of_user": 1}]], totals=True)
    assert "|jobs >=   R +    C +    Q +    X +    O | nodes |" in lines[-4]
    assert lines[-3].startswith("[ 0] alice             |   9      2      1      3      0      3 |     1 |")
    assert lines[-2].startswith("[ 1] bob               |   1      1      0      0      0      0 |     1 |")
    assert lines[-1].startswith("[ T] Totals            |  10      3      1      3      0      3 |     2 |")


# A Torque-like listing behind qstat -q's 7 running and 4 queued: its Que also counts the 2 W jobs.
LISTED = ["R"] * 7 + ["Q", "Q", "W", "W", "C"]


def _summary(config, jobs_states, color="OFF", queues=True):
    class Cluster(object):
        total_running_jobs, total_queued_jobs = 7, 4
        queues_dict = {"batch": SimpleNamespace(run="7", queued="4", state="E")} if queues else {}
        total_wn = offdown_nodes = available_wn = working_cores = total_cores = 0

    args = SimpleNamespace(REMAP=False, CLASSIC=False, WATCH=True, COLOR=color)
    model = MVU_Model({}, args, None, backend=qtop_module.scheduler_backend("pbs"))
    snapshot = SimpleNamespace(jobs_dict={str(n): SimpleNamespace(job_state=state) for n, state in enumerate(jobs_states)})
    config = dict(config, state_abbreviations={"pbs": PBS_STATES})
    mvu_view.TextDisplay(None, config, None, None, Cluster(), args, MVU_model=model).display_job_accounting_summary(Cluster(), snapshot)


def test_summary_states_default_to_scheduler_reported_r_plus_q(capsys):
    _summary({}, LISTED)  # R and Q as the scheduler reports them; Other is the one listed job beyond them (C), not W again
    assert "|   12=7+4+1 jobs (Total=R+Q+Other)" in capsys.readouterr().out


def test_summary_counts_the_job_list_without_queue_totals(capsys):  # e.g. an empty qstat -q
    _summary({}, LISTED, queues=False)
    assert "|   12=7+2+3 jobs (Total=R+Q+Other)" in capsys.readouterr().out


@pytest.mark.parametrize(
    "term, colorterm, orange",
    (("xterm-256color", "", "0;30;48;5;208"), ("xterm", "truecolor", "0;30;48;5;208"), ("linux", "", "1;37;41"), ("xterm", "", "1;37;41")),
)
def test_summary_counts_have_blue_green_orange_yellow_backgrounds(capsys, monkeypatch, term, colorterm, orange):
    monkeypatch.setenv("TERM", term)
    monkeypatch.setenv("COLORTERM", colorterm)
    _summary({}, LISTED, color="ON")
    counts = ["\x1b[%sm%s\x1b[0;m" % pair for pair in zip(("1;37;44", "0;30;42", orange, "0;30;43"), ("12", "7", "4", "1"))]
    assert counts[0] + "=" + "+".join(counts[1:]) in capsys.readouterr().out


def test_summary_colours_are_configurable(capsys):
    _summary({"summary_colours": ["BlackOnBlue", "BlackOnRed"]}, LISTED, color="ON")  # two colours, reused in turn
    counts = ["\x1b[%sm%s\x1b[0;m" % pair for pair in zip(("0;30;44", "0;30;41", "0;30;44", "0;30;41"), ("12", "7", "4", "1"))]
    assert counts[0] + "=" + "+".join(counts[1:]) in capsys.readouterr().out


@pytest.mark.parametrize("tty", (True, False))
def test_monochrome_summary_counts_are_reversed_only_on_a_terminal(capsys, monkeypatch, tty):
    monkeypatch.setattr(sys, "__stdout__", SimpleNamespace(isatty=lambda: tty))
    _summary({}, LISTED)
    expected = "\x1b[7m12\x1b[0m=\x1b[7m7\x1b[0m+\x1b[7m4\x1b[0m+\x1b[7m1\x1b[0m" if tty else "12=7+4+1"
    assert "|   %s jobs (Total=R+Q+Other)" % expected in capsys.readouterr().out


def test_summary_states_count_the_job_list_when_tuned(capsys):
    _summary({"summary_states": ["running_of_user", {"queued_of_user": "Qd"}, "other_of_user"]}, ["R", "R", "Q", "C", "Z"])
    assert "|   5=2+1+2 jobs (Total=R+Qd+O)" in capsys.readouterr().out


def test_job_state_totals_keep_unknown_states():
    assert WNOccupancy.job_state_totals(["R", "R", "Z"], {"R": "running_of_user"}) == {"running_of_user": 2, "unknown_of_user": 1}


@pytest.mark.parametrize(
    "term,colours,code",
    [
        ("xterm-256color", None, "0;30;48;5;208"),
        ("linux", None, "1;37;41"),
        ("xterm", ["BlackOnBlue", "BlackOnRed"], "0;30;44"),
        ("xterm", ["BlackOnBlue", "BlackOnGreen", "BlackOnRed"], "0;30;41"),
    ],
    ids=("orange", "red-fallback", "cycled-config", "custom-config"),
)
@pytest.mark.parametrize("user_colour", ("Red_L", "BlackOnOrange256"), ids=("foreground-user", "background-user"))
def test_queued_counts_share_summary_background_without_changing_table_alignment(monkeypatch, capsys, term, colours, code, user_colour):
    monkeypatch.setenv("TERM", term)
    monkeypatch.delenv("COLORTERM", raising=False)
    config = {"accounts_states": [{"queued_of_user": "WAIT"}, "running_of_user"]}
    if colours:
        config["summary_colours"] = colours
    _summary(config, LISTED, color="ON")
    lines = capsys.readouterr().out.splitlines()
    for label in ("Summary", "Queueing"):
        assert "\x1b[%sm4\x1b[0;m" % code in next(line for line in lines if re.sub(r"\x1b\[[0-9;]*m", "", line).startswith(label))
    row = ["0", 2, 3, 5, "alice", 1, {"running_of_user": 2, "queued_of_user": 3}]
    plain = _plain_accounts(monkeypatch, capsys, config, [row])[-1]
    args = SimpleNamespace(COLOR="ON", CLASSIC=False)
    model = MVU_Model({}, args, None, user_to_color={"alice": user_colour})
    display = mvu_view.TextDisplay(None, dict(config, SEPARATOR="|"), None, None, None, args, MVU_model=model)
    display.display_account_jobs_line(row, "alice", {})
    coloured = capsys.readouterr().out.rstrip("\n")
    assert "\x1b[%sm3\x1b[0;m" % code in coloured
    colours_by_state = mvu_view.summary_count_colours(config)
    for count, index in ((5, 0), (2, 1)):
        assert model.MVU_colorize(str(count), colours_by_state[index % len(colours_by_state)]) in coloured
    assert re.sub(r"\x1b\[[0-9;]*m", "", coloured) == plain


@pytest.mark.parametrize("color", ("ON", "OFF"))
@pytest.mark.parametrize("clipped", (False, True), ids=("compact", "clipped"))
def test_no_wrap_queues_clips_only_the_queueing_row(capsys, color, clipped):
    queues = {"queue_%03d" % index: SimpleNamespace(run="1", queued="2", state="E") for index in range(150)}
    cluster = SimpleNamespace(total_running_jobs=150, total_queued_jobs=300, queues_dict=queues, total_wn=0, offdown_nodes=0, available_wn=0, working_cores=0, total_cores=0)
    args = SimpleNamespace(REMAP=False, CLASSIC=False, WATCH=True, NO_WRAP_QUEUES=clipped, COLOR=color)
    viewport = Viewport()
    viewport.set_term_size(40, 40)
    model = MVU_Model({}, args, viewport, backend=qtop_module.scheduler_backend("pbs"), display_overrides={} if clipped else {"queue_summary_layout": "compact"})
    mvu_view.TextDisplay(None, {}, viewport, None, None, args, MVU_model=model).display_job_accounting_summary(cluster, None)
    output = re.sub(r"\x1b\[[0-9;]*m", "", capsys.readouterr().out)
    queue_line = next(line for line in output.splitlines() if line.startswith("Queueing"))
    assert queue_line.startswith("Queueing : queue_000: 1 + 2 |")
    assert (len(queue_line) <= 40) == clipped
    assert ("queue_149" in queue_line) != clipped
    if clipped:
        assert any(line.startswith("* implies blocked") for line in output.splitlines())
    assert "Job accounting summary" in output and "Summary" in output


@pytest.mark.parametrize("color", ("ON", "OFF"))
def test_hidden_queue_layout_drops_only_the_queueing_lines(capsys, color):
    queues = {"long": SimpleNamespace(run="1", queued="2", state="E")}
    cluster = SimpleNamespace(total_running_jobs=1, total_queued_jobs=2, queues_dict=queues, total_wn=0, offdown_nodes=0, available_wn=0, working_cores=0, total_cores=0)
    args = SimpleNamespace(REMAP=False, CLASSIC=False, WATCH=True, NO_WRAP_QUEUES=True, COLOR=color)
    viewport = Viewport()
    viewport.set_term_size(40, 80)
    model = MVU_Model({}, args, viewport, backend=qtop_module.scheduler_backend("pbs"), display_overrides={"queue_summary_layout": "hidden"})
    mvu_view.TextDisplay(None, {}, viewport, None, None, args, MVU_model=model).display_job_accounting_summary(cluster, None)
    output = re.sub(r"\x1b\[[0-9;]*m", "", capsys.readouterr().out)
    assert "Queueing :" not in output and "implies blocked" not in output  # the title banner also says "Queueing"
    assert "Job accounting summary" in output and "Summary" in output


@pytest.mark.parametrize("expanded", (False, True), ids=("compact", "expanded"))
@pytest.mark.parametrize("configured", (False, True), ids=("fallback", "configured"))
def test_queue_labels_match_matrix_letter_backgrounds(capsys, expanded, configured):
    from qtop_py.mvu import MVU_keep_queue_initials_only_and_colorize

    config = {"palettes": {"PALETTES": ["custom"], "custom": {"White": "0;30;43", "BlackOnBlue": "0;30;44"}}}
    args = SimpleNamespace(REMAP=False, CLASSIC=False, WATCH=False, EXPAND_QUEUES=expanded, ANONYMIZE=True, COLOR="ON")
    model = MVU_Model(config, args, None, backend=qtop_module.scheduler_backend("slurm"), user_to_color={"NoPattern": "White"})
    model.queue_to_color = {"batch": "BlackOnBlue"} if configured else {}
    queue = SimpleNamespace(run="1", queued="0", state="up")
    cluster = SimpleNamespace(total_running_jobs=1, total_queued_jobs=0, queues_dict={"batch": queue}, total_wn=1, offdown_nodes=0, available_wn=0, working_cores=1, total_cores=1)
    display = mvu_view.TextDisplay(None, config, None, None, cluster, args, MVU_model=model)
    display.display_job_accounting_summary(cluster, None)
    output = capsys.readouterr().out
    cell = MVU_keep_queue_initials_only_and_colorize([{"qname": ["batch"]}], model.queue_to_color)[0]["qname"][0]
    expected = model.MVU_colorize(cell.initial, color_func=cell.color)
    assert "\x1b[0;30;%sm" % ("44" if configured else "43") in expected
    assert expected.replace("mb\x1b", "mbatch\x1b") in output
