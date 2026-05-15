##
## qtop is a tool to monitor queuing systems - https://github.com/qtop/qtop
##
## Copyright (c) 2016 Fotis Georgatos
## Copyright (c) 2016 Sotiris Fragkiskos
## Copyright (c) 2023 Hewlett Packard Enterprise Development LP
##
## SPDX-License-Identifier: MIT
##

from qtop_py.plugins import slurm
import pytest


@pytest.mark.parametrize(
    "nodelist, result",
    (
        ("wn001", ["wn001"]),
        ("wn[001-003]", ["wn001", "wn002", "wn003"]),
        ("wn[001-003,005]", ["wn001", "wn002", "wn003", "wn005"]),
        ("wn[001,003,005-007]", ["wn001", "wn003", "wn005", "wn006", "wn007"]),
        ("(Priority)", []),
        ("(Resources)", []),
        ("(None)", []),
        ("node01,node02,node03", ["node01", "node02", "node03"]),
        ("gpu[01-02]", ["gpu01", "gpu02"]),
        ("compute[001-002,005]", ["compute001", "compute002", "compute005"]),
    ),
)
def test_expand_slurm_nodelist(nodelist, result):
    assert slurm.expand_slurm_nodelist(nodelist) == result


@pytest.mark.parametrize(
    "node_to_jobs_input, ncpus, expected_core_map",
    (
        # Single job occupying 4 of 8 cores
        ([("101001", 4)], 8, {0: "101001", 1: "101001", 2: "101001", 3: "101001"}),
        # Two jobs sharing cores: 4 + 2
        (
            [("101001", 4), ("101002", 2)],
            8,
            {0: "101001", 1: "101001", 2: "101001", 3: "101001", 4: "101002", 5: "101002"},
        ),
        # Single job filling all cores
        (
            [("101003", 8)],
            8,
            {0: "101003", 1: "101003", 2: "101003", 3: "101003", 4: "101003", 5: "101003", 6: "101003", 7: "101003"},
        ),
        # No jobs: empty map
        ([], 8, {}),
    ),
)
def test_build_core_job_map(node_to_jobs_input, ncpus, expected_core_map):
    """Test that core_job_map is built correctly from (job_id, cpus) pairs."""
    core_job_map = {}
    core_idx = 0
    for job_id, cpus in node_to_jobs_input:
        for _ in range(cpus):
            if core_idx < ncpus:
                core_job_map[core_idx] = job_id
                core_idx += 1
    assert core_job_map == expected_core_map


@pytest.mark.parametrize(
    "raw_state, expected",
    (
        ("alloc", "b"),
        ("allocated", "b"),
        ("mix", "%"),
        ("mixed", "%"),
        ("idle", "-"),
        ("down", "d"),
        ("drain", "x"),
        ("drained", "x"),
        ("comp", "c"),
        ("unknown", "?"),
    ),
)
def test_sinfo_state_map(raw_state, expected):
    assert slurm.SlurmBatchSystem._SINFO_STATE_MAP.get(raw_state) == expected
