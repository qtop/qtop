##
## qtop is a tool to monitor queuing systems - https://github.com/qtop/qtop
##
## Copyright (c) 2016 Fotis Georgatos
## Copyright (c) 2016 Sotiris Fragkiskos
## Copyright (c) 2023 Hewlett Packard Enterprise Development LP
##
## SPDX-License-Identifier: MIT
##

from qtop_py.plugins import pbs
from qtop_py.serialiser import GenericBatchSystem
from types import SimpleNamespace
import io
import pytest


def make_extractor():
    return pbs.PBSStatExtractor({}, SimpleNamespace(ANONYMIZE=False))


@pytest.mark.parametrize('core_selections, result',
     (
             ('0-4,30-31', ["0","1","2","3","4","30","31"]),
             ('5-9', ["5","6","7","8","9"]),
             ('0,3,5-7,11', ["0","3","5","6","7","11"]),
             ('1,2-4,5-6,9', ["1","2","3","4","5","6","9"])
     ),
)
def test_get_corejob_from_range(core_selections, result, job=None):
    result = iter(result)
    for (core, job) in pbs.PBSBatchSystem.get_corejob_from_range(core_selections, job):
        assert core == next(result)

@pytest.mark.parametrize('jobs, result',
     (
             (["0/10102182.f-batch01.grid.sinica.edu.tw", "1/10102106.f-batch01.grid.sinica.edu.tw"], [("10102182", "0"), ("10102106", "1")]),
             (["2/10102339.f-batch01.grid.sinica.edu.tw", "3/10104007.f-batch01.grid.sinica.edu.tw"], [("10102339", "2"), ("10104007", "3")]),
             (["3-5/10102339.f-batch01.grid.sinica.edu.tw"], [("10102339", "3"), ("10102339", "4"), ("10102339", "5")]),
             (["2257887.cluster-pbs5/0", "2257887.cluster-pbs5/1"], [("2257887", "0"), ("2257887", "1")]),
             (["2257887.cluster-pbs5/2", "2257887.cluster-pbs5/3"], [("2257887", "2"), ("2257887", "3")])
     ),
)
def test_get_jobs_cores(jobs, result):
    result = iter(result)
    for job, core in pbs.PBSBatchSystem._get_jobs_cores(jobs):
        assert (job, core) == next(result)


def test_extract_qstat_regex_handles_hyphenated_user_and_queue(tmp_path):
    qstat_file = tmp_path / "qstat.txt"
    qstat_file.write_text(
        "\n".join(
            [
                "Job id                    Name             User            Time Use S Queue",
                "------------------------- ---------------- --------------- -------- - -----",
                "621705.server00           cream_741408101  grid-user       00:00:01 R grid-csic",
            ]
        ),
        encoding="utf-8",
    )

    assert make_extractor()._extract_qstat_regex(str(qstat_file)) == [
        {"JobId": "621705", "UnixAccount": "grid-user", "S": "R", "Queue": "grid-csic"}
    ]


def test_extract_qstat_regex_handles_prior_queue_with_dot_before_at(tmp_path):
    qstat_file = tmp_path / "qstat.txt"
    qstat_file.write_text(
        "\n".join(
            [
                "job-ID prior name user state submit/start at queue slots ja-task-ID",
                "---------------------------------------------------------------",
                " 183997 0.55500 cream2_8449 alice r 08/09/12 00:02:40 cream2.q@ht04.t2.ucy.ac.cy 1",
            ]
        ),
        encoding="utf-8",
    )

    assert make_extractor()._extract_qstat_regex(str(qstat_file)) == [
        {"JobId": "183997", "UnixAccount": "alice", "S": "R", "Queue": "cream2.q@ht04.t2.ucy.ac.cy"}
    ]


def test_extract_qstat_regex_skips_lines_that_do_not_match_selected_format(tmp_path):
    qstat_file = tmp_path / "qstat.txt"
    qstat_file.write_text(
        "\n".join(
            [
                "Job id                    Name             User            Time Use S Queue",
                "------------------------- ---------------- --------------- -------- - -----",
                "621705.server00           cream_741408101  alice           00:00:01 R workq",
                "this is a scheduler warning, not a qstat row",
            ]
        ),
        encoding="utf-8",
    )

    assert make_extractor()._extract_qstat_regex(str(qstat_file)) == [
        {"JobId": "621705", "UnixAccount": "alice", "S": "R", "Queue": "workq"}
    ]


def test_extract_qstatq_regex_defaults_missing_totals_to_zero(tmp_path):
    qstatq_file = tmp_path / "qstat_q.txt"
    qstatq_file.write_text(
        "\n".join(
            [
                "",
                "server: example",
                "",
                "Queue            Memory CPU Time Walltime Node  Run Que Lm  State",
                "---------------- ------ -------- -------- ----  --- --- --  -----",
                "workq              --      --       --      --    1   2 --   E R",
            ]
        ),
        encoding="utf-8",
    )

    assert make_extractor()._extract_qstatq_regex(str(qstatq_file))[-1] == {"Total_running": "0", "Total_queued": "0"}


def test_read_block_handles_eof_without_blank_line():
    block = pbs.PBSBatchSystem._read_block(io.StringIO("node01\n     state = free\n     np = 4\n"))

    assert block == {"domainname": "node01", "state": "free", "np": "4"}


def test_ensure_worker_nodes_have_qnames_ignores_stale_jobs():
    worker_nodes = [{"core_job_map": {"0": "1234", "1": "stale"}}]

    assert GenericBatchSystem.ensure_worker_nodes_have_qnames(worker_nodes, ["1234"], ["workq"]) == [
        {"core_job_map": {"0": "1234", "1": "stale"}, "qname": ["workq"]}
    ]
