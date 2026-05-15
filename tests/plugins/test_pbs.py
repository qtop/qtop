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
import pytest
from types import SimpleNamespace


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


@pytest.fixture
def qstat_extractor():
    return pbs.PBSStatExtractor({}, SimpleNamespace(ANONYMIZE=False))


def test_extract_qstat_regex_accepts_job_names_with_colons(tmp_path, qstat_extractor):
    qstat_file = tmp_path / "qstat.txt"
    qstat_file.write_text(
        "\n".join(
            [
                "Job id                    Name             User            Time Use S Queue",
                "------------------------- ---------------- --------------- -------- - -----",
                "11894868.pbs-goegrid       ML:DROP:ANGLE    marcel.langenbe 08:24:54 R shorttime",
            ]
        )
    )

    assert qstat_extractor._extract_qstat_regex(str(qstat_file)) == [
        {
            "JobId": "11894868",
            "UnixAccount": "marcel.langenbe",
            "S": "R",
            "Queue": "shorttime",
        }
    ]


def test_extract_qstat_regex_accepts_prior_table_format(tmp_path, qstat_extractor):
    qstat_file = tmp_path / "qstat.txt"
    qstat_file.write_text(
        "\n".join(
            [
                "job-ID  prior   name       user         state submit/start at     queue                          slots ja-task-ID",
                "-----------------------------------------------------------------------------------------------------------------",
                "7214025 0.50103 cccreamcel dteam018     r     08/10/2012 01:46:35 medium@ccwsge0454.in2p3.fr         1",
            ]
        )
    )

    assert qstat_extractor._extract_qstat_regex(str(qstat_file)) == [
        {
            "JobId": "7214025",
            "UnixAccount": "dteam018",
            "S": "r",
            "Queue": "medium@ccwsge0454.in2p3.fr",
        }
    ]


def test_extract_qstat_regex_accepts_request_name_sections(tmp_path, qstat_extractor):
    qstat_file = tmp_path / "qstat.txt"
    qstat_file.write_text(
        "\n".join(
            [
                "cert;  type=BATCH;  [ENABLED];  pri=90",
                "1 run;   0 wait;",
                "",
                "        REQUEST NAME        REQUEST ID          USER   STATE",
                "   1:   cream_878672            238085     dteam031  RUNNING",
                "short;  type=BATCH;  [ENABLED];  pri=40",
                "   2:   cr001_215824            625164     dteam032  WAITING",
            ]
        )
    )

    assert qstat_extractor._extract_qstat_regex(str(qstat_file)) == [
        {
            "JobId": "238085",
            "UnixAccount": "dteam031",
            "S": "R",
            "Queue": "",
        },
        {
            "JobId": "625164",
            "UnixAccount": "dteam032",
            "S": "W",
            "Queue": "",
        },
    ]
