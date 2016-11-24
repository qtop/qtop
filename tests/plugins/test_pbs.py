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
