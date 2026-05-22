import pytest

from qtop_py.plugins.pbs import PBSBatchSystem


class _Options:
    ANONYMIZE = False


class _StubExtractor:
    def __init__(self, totals):
        self._totals = totals

    def extract_qstatq(self, _path):
        return [
            {"queue_name": "workq", "run": "2", "queued": "1", "lm": "--", "state": "E R"},
            self._totals,
        ]


def _make_batch(totals):
    batch = PBSBatchSystem({}, {}, _Options())
    batch.qstat_maker = _StubExtractor(totals)
    return batch


def test_pbs_queue_totals_are_cast_to_ints():
    total_running, total_queued, _ = _make_batch({"Total_running": "2", "Total_queued": "1"}).get_queues_info()

    assert total_running == 2
    assert total_queued == 1


def test_pbs_queue_totals_do_not_execute_payload(tmp_path):
    sentinel = tmp_path / "pbs-total-eval-executed"
    payload = "__import__('pathlib').Path(%r).touch()" % str(sentinel)

    with pytest.raises(ValueError):
        _make_batch({"Total_running": payload, "Total_queued": "1"}).get_queues_info()

    assert not sentinel.exists()


def test_pbs_queued_total_payload_is_not_executed(tmp_path):
    sentinel = tmp_path / "pbs-queued-total-eval-executed"
    payload = "__import__('pathlib').Path(%r).touch()" % str(sentinel)

    with pytest.raises(ValueError):
        _make_batch({"Total_running": "1", "Total_queued": payload}).get_queues_info()

    assert not sentinel.exists()
