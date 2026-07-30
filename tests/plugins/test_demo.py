##
## qtop is a tool to monitor queuing systems - https://github.com/qtop/qtop
##
## Copyright (c) 2016 Fotis Georgatos
## Copyright (c) 2016 Sotiris Fragkiskos
## Copyright (c) 2026 Utkarsh Sinha
##
## SPDX-License-Identifier: MIT
##

"""Tests for ``DemoBatchSystem.get_jobs_info``.

The method used to build four parallel lists by hand, with a TODO noting they
"have to be of the same length". They are now projected from a single list of
``_JobInfo`` records, so the equal-length invariant holds by construction. The
public return contract (four lists, same order) is unchanged; these tests pin
both the invariant and the exact mapping.
"""

from qtop_py.plugins import demo


class _FakeSim:
    """Minimal LittleGridSimulator stand-in with deterministic data."""

    # node 0: core0 idle (0), core1 -> job 101
    # node 1: core0 -> job 102, core1 idle (0), core2 -> job 103
    core_job_map = [[0, 101], [102, 0, 103]]
    job_meta = {
        101: ("batch", "alice"),
        102: ("urgent", "bob"),
        103: ("batch", "carol"),
    }
    job_state = {101: "R", 102: "Q", 103: "R"}


def _make_demo(monkeypatch):
    monkeypatch.setattr(demo, "WORKER_NODES", 2)
    system = demo.DemoBatchSystem(scheduler_output_filenames=None, config=None, options=None)
    system.sim = _FakeSim()
    return system


def test_get_jobs_info_preserves_mapping(monkeypatch):
    system = _make_demo(monkeypatch)
    job_ids, usernames, job_states, queue_names = system.get_jobs_info()

    assert job_ids == [101, 102, 103]
    assert usernames == ["alice", "bob", "carol"]
    assert job_states == ["R", "Q", "R"]
    assert queue_names == ["batch", "urgent", "batch"]


def test_get_jobs_info_lists_are_same_length(monkeypatch):
    system = _make_demo(monkeypatch)
    lengths = {len(column) for column in system.get_jobs_info()}
    assert lengths == {3}


def test_get_jobs_info_empty_grid(monkeypatch):
    system = _make_demo(monkeypatch)
    system.sim = _FakeSim()
    system.sim.core_job_map = [[0, 0], [0]]  # no jobs anywhere
    assert system.get_jobs_info() == ([], [], [], [])
