##
## qtop is a tool to monitor queuing systems - https://github.com/qtop/qtop
##
## Copyright (c) 2016 Fotis Georgatos
## Copyright (c) 2016 Sotiris Fragkiskos
## Copyright (c) 2023 Hewlett Packard Enterprise Development LP
##
## SPDX-License-Identifier: MIT
##

import pytest
import re
import datetime
from qtop_py.qtop import (
    WNOccupancy,
    decide_batch_system,
    extract_regex_detail,
    load_yaml_config,
    update_config_with_cmdline_vars,
    JobNotFound,
    SchedulerNotSpecified,
    NoSchedulerFound,
    get_date_obj_from_str,
    get_detail_of_name,
)


@pytest.fixture
def config():
    return {}


def test_load_yaml_config_does_not_execute_config_values(monkeypatch, tmp_path):
    class Args:
        CONFFILE = None

    sentinel = tmp_path / "executed"
    dangerous_value = "__import__('pathlib').Path(%r).touch()" % str(sentinel)

    def parse_config(_path):
        return {
            "possible_ids": "01",
            "user_color_mappings": [{"alice": "Blue"}],
            "nodestate_color_mappings": [{"au": "BlackOnRed"}],
            "remapping": [],
            "savepath": str(tmp_path / "qtop-results"),
            "scheduler": "pbs",
            "transpose_wn_matrices": "True",
            "fill_with_user_firstletter": "False",
            "faster_xml_parsing": "False",
            "vertical_separator_every_X_columns": "0",
            "overwrite_sample_file": dangerous_value,
            "sorting": {"reverse": "False"},
            "workernodes_matrix": [{"wn id lines": {"alt_label_colors": ["White, Blue_L"], "user_cut_matrix_width": "0"}}],
            "vertical_separator": "'|'",
        }

    import qtop_py.qtop as qtop

    monkeypatch.setattr(qtop.yaml, "parse", parse_config)
    monkeypatch.setattr(qtop, "QTOPPATH", str(tmp_path), raising=False)
    monkeypatch.setattr(qtop, "SYSTEMCONFDIR", str(tmp_path / "system"))
    monkeypatch.setattr(qtop, "USERPATH", str(tmp_path / "user"))
    monkeypatch.setattr(qtop, "args", Args(), raising=False)

    config, _, _ = load_yaml_config()

    assert config["transpose_wn_matrices"] is True
    assert config["vertical_separator_every_X_columns"] == 0
    assert config["overwrite_sample_file"] == dangerous_value
    assert not sentinel.exists()


def test_extract_regex_detail_supports_configured_re_search():
    field = "User Name <alice@example.org>"

    assert extract_regex_detail("re.search('(?<=<)[^<>]+(?=>)', field).group(0)", field) == "alice@example.org"


def test_get_detail_of_name_falls_back_for_unsupported_regex(monkeypatch):
    import qtop_py.qtop as qtop

    class Args:
        GET_GECOS = False

    class Process:
        def communicate(self, _input):
            return ("alice:x:1:1:Alice Example:/home/alice:/bin/sh\n", "")

    monkeypatch.setattr(qtop, "args", Args(), raising=False)
    monkeypatch.setattr(
        qtop,
        "config",
        {
            "extract_info": {
                "field_to_use": "5",
                "regex": "__import__('pathlib').Path('/tmp/should-not-run').touch()",
                "user_details_cache": "cat /dev/null",
            }
        },
        raising=False,
    )
    monkeypatch.setattr(qtop.subprocess, "Popen", lambda *args, **kwargs: Process())

    assert get_detail_of_name([]) == {"alice": "Alice Example"}


def test_update_config_with_cmdline_vars_does_not_execute_option_values(tmp_path):
    class Args:
        OPTION = ["enabled=True", "dangerous=__import__('pathlib').Path(%r).touch()" % str(tmp_path / "executed")]
        TRANSPOSE = False
        REM_EMPTY_CORELINES = 0

    config = {"rem_empty_corelines": "0", "transpose_wn_matrices": True}

    updated = update_config_with_cmdline_vars(Args(), config)

    assert updated["enabled"] is True
    assert updated["dangerous"].startswith("__import__")
    assert not (tmp_path / "executed").exists()


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
    with pytest.raises(JobNotFound) as e:  # noqa: F841  ## FIXME
        wns_occupancy._create_user_job_counts(user_names, job_states, state_abbrevs) == {
            "cancelled_of_user": {"sotiris": 0, "yannis": 0, "petros": 1},
            "exiting_of_user": {"sotiris": 0, "kostas": 1, "yannis": 0},
            "running_of_user": {"sotiris": 1, "yannis": 1},
        }


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

    with pytest.raises(SchedulerNotSpecified) as e:  # noqa: F841  ## FIXME
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
    with pytest.raises(NoSchedulerFound) as e:  # noqa: F841  ## FIXME
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
        ("21:00", datetime.datetime(year=2016, month=11, day=20, hour=1, minute=10, second=0), datetime.datetime(year=2016, month=11, day=19, hour=20, minute=10, second=0).day),
        ("21:00", datetime.datetime(year=2016, month=11, day=20, hour=22, minute=10, second=0), datetime.datetime(year=2016, month=11, day=20, hour=20, minute=10, second=0).day),
    ),
)
def test_get_date_obj_from_str(s, now, day_meant):
    """
    Two cases:
    at 01:00 in the morning, the user inputs 21:00 (the previous day is implied)
    at 22:10 at night, the user inputs again 21:00 (the same day is implied)
    """
    assert get_date_obj_from_str(s, now).day == day_meant
