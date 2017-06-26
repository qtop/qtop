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


@pytest.mark.parametrize('jobline, result',
     (
             ("27/1282133.node-00,28/1282134.node-00,31/1282135.node-00,15,21-22,24/1282033.node-00,2,6,8-9/1282034.node-00,10,12,14,20/1282038.node-00", ["27/1282133.node-00", "28/1282134.node-00", "31/1282135.node-00", "15,21-22,24/1282033.node-00", "2,6,8-9/1282034.node-00", "10,12,14,20/1282038.node-00"]),
             ('678090.delta/12, 678090.delta/13, 678160.delta/14, 678161.delta/15', ["678090.delta/12", "678090.delta/13", "678160.delta/14", "678161.delta/15"]),
     ),
)
def test_get_jobs_from_jobline(jobline, result):
    assert pbs.PBSBatchSystem.get_jobs_from_jobline(jobline) == result
