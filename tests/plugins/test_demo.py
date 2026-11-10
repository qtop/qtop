## Copyright (c) 2026 qtop contributors
## SPDX-License-Identifier: MIT
##
## qtop is a tool to monitor queuing systems - https://github.com/qtop/qtop
##

"""Tests for ``DemoBatchSystem.get_jobs_info``.

The method used to build four parallel lists by hand. They are now projected from a single list of
``_JobInfo`` records, so the equal-length invariant holds by construction. The
public return contract (four lists, same order) is unchanged; these tests pin
both the invariant and the exact mapping.
"""

from qtop_py.plugins import demo


class _FakeSim:
    """Minimal LittleClusterSimulator stand-in with deterministic data."""

    # node 0: core0 idle (0), core1 -> job 101
    # node 1: core0 -> job 102, core1 idle (0), core2 -> job 103
    core_job_map = [[0, 101], [102, 0, 103]]
    job_meta = {
        101: ("batch", "alice"),
        102: ("urgent", "bob"),
        103: ("batch", "carol"),
        104: ("transfer", "dave"),
    }
    job_state = {101: "R", 102: "Q", 103: "R", 104: "Q"}
    queue_jobs = {"urgent": [], "transfer": [104], "batch": []}
    queue_state = {"urgent": "E", "transfer": "E", "batch": "E"}

    def get_total_queued(self):
        return sum(len(jobs) for jobs in self.queue_jobs.values())


def _make_demo(monkeypatch):
    monkeypatch.setattr(demo, "WORKER_NODES", 2)
    system = demo.DemoBatchSystem(scheduler_output_filenames=None, config=None, options=None)
    system.sim = _FakeSim()
    return system


def test_get_jobs_info_preserves_mapping(monkeypatch):
    system = _make_demo(monkeypatch)
    job_ids, usernames, job_states, queue_names = system.get_jobs_info()

    assert job_ids == [101, 102, 103, 104]  # jobs on cores, then the queued ones
    assert usernames == ["alice", "bob", "carol", "dave"]
    assert job_states == ["R", "Q", "R", "Q"]
    assert queue_names == ["batch", "urgent", "batch", "transfer"]


def test_get_jobs_info_lists_are_same_length(monkeypatch):
    system = _make_demo(monkeypatch)
    lengths = {len(column) for column in system.get_jobs_info()}
    assert lengths == {4}


def test_get_jobs_info_empty_grid(monkeypatch):
    system = _make_demo(monkeypatch)
    system.sim = _FakeSim()
    system.sim.core_job_map = [[0, 0], [0]]  # no jobs anywhere
    system.sim.queue_jobs = {"urgent": [], "transfer": [], "batch": []}
    assert system.get_jobs_info() == ([], [], [], [])


def test_summary_and_accounts_count_the_same_demo_jobs(monkeypatch):
    system = _make_demo(monkeypatch)
    system.sim.job_state = {101: "R", 102: "C", 103: "R", 104: "Q"}  # 102 is finishing: it holds its core but no longer runs
    running, queued, _queues = system.get_queues_info()
    _job_ids, _users, job_states, _queue_names = system.get_jobs_info()
    assert (running, queued) == (job_states.count("R"), job_states.count("Q")) == (2, 1)
