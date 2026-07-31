##
## qtop is a tool to monitor queuing systems - https://github.com/qtop/qtop
##
## Copyright (c) 2016 Fotis Georgatos
## Copyright (c) 2016 Sotiris Fragkiskos
## Copyright (c) 2023 Hewlett Packard Enterprise Development LP
## Copyright (c) 2026 Mateo Rojas
##
## SPDX-License-Identifier: MIT
##

from qtop_py.plugins import pbs
import pytest
from types import SimpleNamespace


@pytest.mark.parametrize(
    "core_selections, result",
    (
        ("0-4,30-31", ["0", "1", "2", "3", "4", "30", "31"]),
        ("5-9", ["5", "6", "7", "8", "9"]),
        ("0,3,5-7,11", ["0", "3", "5", "6", "7", "11"]),
        ("1,2-4,5-6,9", ["1", "2", "3", "4", "5", "6", "9"]),
    ),
)
def test_get_corejob_from_range(core_selections, result, job=None):
    result = iter(result)
    for core, job in pbs.PBSBatchSystem.get_corejob_from_range(core_selections, job):
        assert core == next(result)


@pytest.mark.parametrize(
    "jobs, result",
    (
        (["0/10102182.f-batch01.grid.sinica.edu.tw", "1/10102106.f-batch01.grid.sinica.edu.tw"], [("10102182", "0"), ("10102106", "1")]),
        (["2/10102339.f-batch01.grid.sinica.edu.tw", "3/10104007.f-batch01.grid.sinica.edu.tw"], [("10102339", "2"), ("10104007", "3")]),
        (["3-5/10102339.f-batch01.grid.sinica.edu.tw"], [("10102339", "3"), ("10102339", "4"), ("10102339", "5")]),
        (["2257887.cluster-pbs5/0", "2257887.cluster-pbs5/1"], [("2257887", "0"), ("2257887", "1")]),
        (["2257887.cluster-pbs5/2", "2257887.cluster-pbs5/3"], [("2257887", "2"), ("2257887", "3")]),
    ),
)
def test_get_jobs_cores(jobs, result):
    result = iter(result)
    for job, core in pbs.PBSBatchSystem._get_jobs_cores(jobs):
        assert (job, core) == next(result)


def test_qstatq_regex_derives_totals_when_summary_line_is_missing(tmp_path):
    qstatq_file = tmp_path / "qstat_q.txt"
    qstatq_file.write_text(
        "Server: cluster\n"
        "\n"
        "Queue            Memory CPU Time Walltime Node Run Que Lm  State\n"
        "---------------- ------ -------- -------- ---- --- --- --- -----\n"
        "\n"
        "workq            --     --       --       --   3   2   --  E R\n"
        "short            --     --       --       --   1   4   --  E R\n"
    )
    extractor = pbs.PBSStatExtractor({}, SimpleNamespace(ANONYMIZE=False))

    queues = extractor.extract_qstatq(str(qstatq_file))

    assert queues[-1] == {"Total_running": 4, "Total_queued": 6}


def test_qstatq_regex_returns_zero_totals_for_header_only_output(tmp_path):
    qstatq_file = tmp_path / "qstat_q.txt"
    qstatq_file.write_text(
        "Server: cluster\n\nQueue            Memory CPU Time Walltime Node Run Que Lm  State\n---------------- ------ -------- -------- ---- --- --- --- -----\n\n"
    )
    extractor = pbs.PBSStatExtractor({}, SimpleNamespace(ANONYMIZE=False))

    assert extractor.extract_qstatq(str(qstatq_file)) == [{"Total_running": 0, "Total_queued": 0}]
