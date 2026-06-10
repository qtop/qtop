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

import pytest
import re
import datetime
import sys
from pathlib import Path
from types import SimpleNamespace
from qtop_py import qtop as qtop_module
from qtop_py import utils as qtop_utils
from qtop_py.constants import SYMBOL_LONG_TAIL_USER, SYMBOL_UNKNOWN_NODE_STATE
from qtop_py.qtop import (
    WNOccupancy,
    decide_batch_system,
    JobNotFound,
    SchedulerNotSpecified,
    NoSchedulerFound,
    get_date_obj_from_str,
    TextDisplay,
)

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


def test_sort_worker_nodes_uses_named_sort_keys(monkeypatch):
    import qtop_py.qtop as qtop

    monkeypatch.setattr(qtop, "dynamic_config", {}, raising=False)
    cluster = qtop.Cluster.__new__(qtop.Cluster)
    cluster.config = {"sorting": {"user_sort": ["sort by all numbers"], "reverse": False}}
    cluster.worker_nodes = [
        {"domainname": "node10", "state": "-", "np": "1", "core_job_map": {}},
        {"domainname": "node2", "state": "-", "np": "1", "core_job_map": {}},
    ]

    assert [node["domainname"] for node in cluster._sort_worker_nodes()] == ["node2", "node10"]


def test_sort_worker_nodes_rejects_custom_python_sorting(monkeypatch):
    import qtop_py.qtop as qtop

    monkeypatch.setattr(qtop, "dynamic_config", {}, raising=False)
    cluster = qtop.Cluster.__new__(qtop.Cluster)
    cluster.config = {"sorting": {"user_sort": ["sort by custom definition"], "reverse": False}}
    cluster.worker_nodes = [{"domainname": "node1", "state": "-", "np": "1", "core_job_map": {}}]

    with pytest.raises(ValueError, match="custom Python sorting expressions"):
        cluster._sort_worker_nodes()


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
        assert False


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

    class Document(object):
        jobs_dict = {}

    document = Document()

    wns_occupancy = WNOccupancy(None, None, document, None, None)
    assert wns_occupancy._create_user_job_counts(user_names, job_states, state_abbrevs) == {
        "cancelled_of_user": {"sotiris": 0, "yannis": 0, "petros": 1},
        "exiting_of_user": {"sotiris": 0, "kostas": 1, "yannis": 0},
        "running_of_user": {"sotiris": 1, "yannis": 1},
    }


def test_create_user_job_counts_raises_jobnotfound():  # user_names, job_states, state_abbrevs
    user_names = ["sotiris", "kostas", "yannis", "petros"]
    state_abbrevs = {"C": "cancelled_of_user", "E": "exiting_of_user", "r": "running_of_user"}
    job_states = ["r", "E", "x", "C"]

    class Document(object):
        jobs_dict = {}

    document = Document()
    wns_occupancy = WNOccupancy(None, None, document, None, None)
    with pytest.raises(JobNotFound):
        wns_occupancy._create_user_job_counts(user_names, job_states, state_abbrevs) == {
            "cancelled_of_user": {"sotiris": 0, "yannis": 0, "petros": 1},
            "exiting_of_user": {"sotiris": 0, "kostas": 1, "yannis": 0},
            "running_of_user": {"sotiris": 1, "yannis": 1},
        }


@pytest.mark.parametrize("switch", ("-4", "--accounttotals"))
def test_account_totals_cli_switch(monkeypatch, switch):
    monkeypatch.setattr(sys, "argv", ["qtop", switch])

    args = qtop_utils.parse_qtop_cmdline_args()

    assert args.SHOW_ACCOUNT_TOTALS is True


def test_display_user_accounts_pool_mappings_adds_totals(monkeypatch, capsys):
    class Args(object):
        COLOR = "OFF"
        CLASSIC = False
        SHOW_ACCOUNT_TOTALS = True

    class WNSOccupancy(object):
        account_jobs_table = [
            ["0", 2, 3, 5, "alice", 1],
            ["1", 4, 0, 4, "bob", 2],
        ]
        userid_to_userid_re_pat = {
            "0": "account_not_colored",
            "1": "account_not_colored",
        }

    args = Args()
    monkeypatch.setattr(qtop_module, "args", args, raising=False)
    monkeypatch.setattr(qtop_module, "config", {"SEPARATOR": "|"}, raising=False)
    monkeypatch.setattr(qtop_module, "user_to_color", {}, raising=False)

    wns_occupancy = WNSOccupancy()
    display = TextDisplay(None, qtop_module.config, None, wns_occupancy, None, args)

    display.display_user_accounts_pool_mappings(wns_occupancy)

    output = capsys.readouterr().out
    assert "[ T] Totals" in output
    assert "|   9      6      3 |" in output
    assert "|     3 |" in output


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
        userid_to_userid_re_pat = {
            "0": "account_not_colored",
            "1": "account_not_colored",
        }

    args = Args()
    monkeypatch.setattr(qtop_module, "args", args, raising=False)
    monkeypatch.setattr(qtop_module, "config", {"SEPARATOR": "|"}, raising=False)
    monkeypatch.setattr(qtop_module, "user_to_color", {}, raising=False)

    wns_occupancy = WNSOccupancy()
    display = TextDisplay(None, qtop_module.config, None, wns_occupancy, None, args)

    display.display_user_accounts_pool_mappings(wns_occupancy)

    output = capsys.readouterr().out
    assert "alice" in output
    assert "[ T] Totals" not in output


def test_available_possible_ids_filters_reserved_user_symbols():
    config = user_symbol_config(possible_ids=SYMBOL_POSSIBLE_IDS_WITH_RESERVED)

    assert qtop_module._available_possible_ids(config) == ["0", "1"]


def test_load_yaml_config_keeps_user_symbol_pool_bounded(monkeypatch, tmp_path):
    monkeypatch.setattr(qtop_module, "QTOPPATH", str(Path(__file__).resolve().parents[1]), raising=False)
    monkeypatch.setattr(qtop_module, "SYSTEMCONFDIR", str(tmp_path / "etc"))
    monkeypatch.setattr(qtop_module, "USERPATH", str(tmp_path / "user"))
    monkeypatch.setattr(qtop_module, "CURPATH", str(tmp_path), raising=False)
    monkeypatch.setattr(qtop_module, "args", SimpleNamespace(CONFFILE=None), raising=False)
    monkeypatch.setattr(qtop_module.fileutils, "mkdir_p", lambda path: None)

    config, _, _ = qtop_module.load_yaml_config()

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
    # monkeypatch.setitem(config, "schedulers", ['oar', 'sge', 'pbs'])
    schedulers = ["sge", "oar", "pbs"]
    available_batch_systems = {"sge": None, "oar": None, "pbs": None}
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
    available_batch_systems = {"sge": None, "oar": None, "pbs": None}
    config = {"signature_commands": {"pbs": "pbsnodes", "oar": "oarnodes", "sge": "qhost", "demo": "echo"}}

    with pytest.raises(SchedulerNotSpecified):
        decide_batch_system(
            cmdline_switch,
            env_var,
            config_file_batch_option,
            schedulers,
            available_batch_systems,
            config,
        ) == returned_scheduler


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
    available_batch_systems = {"sge": None, "oar": None, "pbs": None}
    with pytest.raises(NoSchedulerFound):
        decide_batch_system(
            cmdline_switch,
            env_var,
            config_file_batch_option,
            schedulers,
            available_batch_systems,
            config,
        ) == returned_scheduler


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
