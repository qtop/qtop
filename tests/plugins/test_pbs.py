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


def test_get_queues_info_does_not_eval_total_counts(tmp_path):
    class QstatMaker:
        def extract_qstatq(self, _qstatq_file):
            sentinel = tmp_path / "pbs-total-executed"
            dangerous_value = "__import__('pathlib').Path(%r).touch()" % str(sentinel)
            return [
                {"queue_name": "workq", "run": "1", "queued": "0", "lm": "--", "state": "E R"},
                {"Total_running": dangerous_value, "Total_queued": "0"},
            ]

    batch_system = pbs.PBSBatchSystem.__new__(pbs.PBSBatchSystem)
    batch_system.qstat_maker = QstatMaker()
    batch_system.qstatq_file = "unused"

    with pytest.raises(ValueError):
        batch_system.get_queues_info()

    assert not (tmp_path / "pbs-total-executed").exists()
