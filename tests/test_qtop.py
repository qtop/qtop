##
## qtop is a tool to monitor queuing systems - https://github.com/qtop/qtop
##
## Copyright (c) 2016 Fotis Georgatos
## Copyright (c) 2016 Sotiris Fragkiskos
## Copyright (c) 2023 Hewlett Packard Enterprise Development LP
##
## SPDX-License-Identifier: MIT
##

import re
import datetime
import sys
from types import SimpleNamespace

import pytest
from qtop_py.qtop import Cluster, WNOccupancy, decide_batch_system, load_yaml_config, JobNotFound, SchedulerNotSpecified, NoSchedulerFound, get_date_obj_from_str


@pytest.fixture
def config():
    return {}


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
        ("trueno_ita00.csic.es", "trueno_ita00"),
        ("wn-hp-123.egi.local", "wn-hp-123"),
        ("woinic-123.egi.local", "woinic-123"),
        ("wn123-ara.bifi.unizar.es", "wn123-ara"),
        ("c123-123-123.gridka.de", "c123-123-123"),
        ("n123-iep-grid.saske.sk", "n123-iep-grid"),
    ),
)
def test_re_node(domain_name, match):
    re_node = r"([A-Za-z0-9_-]+)(?=\.|$)"
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


def test_decide_remapping_handles_mixed_numbered_and_named_nodes():
    cluster = Cluster.__new__(Cluster)
    cluster.args = SimpleNamespace(BLINDREMAP=False)
    cluster.config = {"exotic_starting_wn_nr": "30", "percentage": "0.8"}
    cluster.node_subclusters = {"node"}
    cluster.workernode_list = [1, "login"]
    cluster.total_wn = 2
    cluster.offdown_nodes = 0

    assert cluster.decide_remapping(["1", ""]) is True


def test_valid_corejobs_skips_jobs_missing_from_qstat():
    corejobs = {"0": "1234", "1": "stale"}
    jobid_to_user_to_queue = {"1234": ("alice", "workq")}

    assert list(WNOccupancy._valid_corejobs(None, corejobs, jobid_to_user_to_queue)) == [("alice", "0", "workq")]


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
    with pytest.raises(JobNotFound) as e:
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

    with pytest.raises(SchedulerNotSpecified) as e:
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
    with pytest.raises(NoSchedulerFound) as e:
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
