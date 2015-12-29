import pytest
import re

from nose.tools import assert_raises

from qtop import create_job_counts, pick_batch_system
from common_module import JobNotFound


@pytest.mark.parametrize('domain_name, match',
        (
            ('lrms123', 'lrms123'),
            ('td123.pic.es', 'td123'),
            ('gridmon.ucc.ie', 'gridmon'),
            ('gridmon.cs.tcd.ie', 'gridmon'),
            ('wn123.grid.ucc.ie', 'wn123'),
            ('lcg123.gridpp.rl.ac.uk', 'lcg123'),
            ('compute-123-123', 'compute-123-123'),
            ('wn-hp-123.egi.local', 'wn-hp-123'),
            ('woinic-123.egi.local', 'woinic-123'),
            ('wn123-ara.bifi.unizar.es', 'wn123-ara'),
            ('c123-123-123.gridka.de', 'c123-123-123'),
            ('n123-iep-grid.saske.sk', 'n123-iep-grid'),
        ),
    )
def test_re_node(domain_name, match):
    re_node = '([A-Za-z0-9-]+)(?=\.|$)'
    m = re.search(re_node, domain_name)
    try:
        assert m.group(0) == match
    except AttributeError:
        assert False


@pytest.mark.parametrize('domain_name, number',
    (
         ('wn067.grid.cs.tcd.ie', 67),
         ('gridmon.cs.tcs.ie', -1),
         ('wn003.cs.tcs.ie', 3),
         ('wn01-03-003.cs.tcs.ie', 103003),
    ),
)
def test_batch_nodes_sorting(domain_name, number):
    domain_name = domain_name.split('.', 1)[0]
    assert int(re.sub(r'[A-Za-z_-]+', '', domain_name) or -1) == number


def test_create_job_counts():  # user_names, job_states, state_abbrevs
    user_names = ['sotiris', 'kostas', 'yannis', 'petros']
    state_abbrevs = {'C': 'cancelled_of_user', 'E': 'exiting_of_user', 'r': 'running_of_user'}
    job_states = ['r', 'E', 'r', 'C']
    assert create_job_counts(user_names, job_states, state_abbrevs) == {
        'cancelled_of_user': {'sotiris': 0, 'yannis': 0, 'petros': 1},
        'exiting_of_user': {'sotiris': 0, 'kostas': 1, 'yannis': 0},
        'running_of_user': {'sotiris': 1, 'yannis': 1},
    }


def test_create_job_counts_raises_jobnotfound():  # user_names, job_states, state_abbrevs
    user_names = ['sotiris', 'kostas', 'yannis', 'petros']
    state_abbrevs = {'C': 'cancelled_of_user', 'E': 'exiting_of_user', 'r': 'running_of_user'}
    job_states = ['r', 'E', 'x', 'C']
    with pytest.raises(JobNotFound) as e:
        create_job_counts(user_names, job_states, state_abbrevs) == {
            'cancelled_of_user': {'sotiris': 0, 'yannis': 0, 'petros': 1},
            'exiting_of_user': {'sotiris': 0, 'kostas': 1, 'yannis': 0},
            'running_of_user': {'sotiris': 1, 'yannis': 1},
        }


@pytest.mark.parametrize('cmdline_switch, env_var, config_file_batch_option, returned_scheduler',
     (
         (None, None, 'sge', 'sge'),
         ('auto', None, 'sge', 'sge'),
         ('oar', None, 'sge', 'oar'),
         ('oar', 'pbs', 'sge', 'oar'),
         ('auto', 'pbs', 'sge', 'pbs'),
     ),
)
def test_pick_batch_system(cmdline_switch, env_var, config_file_batch_option, returned_scheduler):
    assert pick_batch_system(cmdline_switch, env_var, config_file_batch_option) == returned_scheduler

