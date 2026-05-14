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
from qtop_py.qtop import Cluster, WNOccupancy
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


def test_extract_qstat_prior_format_accepts_four_digit_year(tmp_path):
    qstat_file = tmp_path / "qstat.txt"
    qstat_file.write_text(
        """job-ID  prior   name       user         state submit/start at     queue                          slots ja-task-ID
-----------------------------------------------------------------------------------------------------------------
7214019 0.50103 cccreamcel dteam013     r     08/10/2012 01:46:34 medium@ccwsge0443.in2p3.fr         1
""",
        encoding="utf-8",
    )

    extractor = pbs.PBSStatExtractor({}, SimpleNamespace(ANONYMIZE=False))

    assert extractor._extract_qstat_regex(str(qstat_file)) == [
        {
            "JobId": "7214019",
            "UnixAccount": "dteam013",
            "S": "R",
            "Queue": "medium@ccwsge0443.in2p3.fr",
        }
    ]


def test_valid_corejobs_skips_stale_pbsnodes_job():
    corejobs = {"0": "26304097", "1": "7214019"}
    jobs = {"7214019": ("dteam013", "medium")}

    assert list(WNOccupancy._valid_corejobs(None, corejobs, jobs)) == [
        ("dteam013", "1", "medium"),
    ]


def test_decide_remapping_handles_mixed_numeric_and_named_nodes():
    cluster = Cluster.__new__(Cluster)
    cluster.total_wn = 2
    cluster.args = SimpleNamespace(BLINDREMAP=False)
    cluster.node_subclusters = {"wn"}
    cluster.workernode_list = [1, "login"]
    cluster.config = {"exotic_starting_wn_nr": "9000", "percentage": "0.8"}
    cluster.offdown_nodes = 0

    assert cluster.decide_remapping(["1", ""]) is True
