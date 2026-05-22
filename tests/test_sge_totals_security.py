import pytest

import qtop_py.plugins.sge as sge_module
from qtop_py.plugins.sge import SGEBatchSystem


class _Options:
    ANONYMIZE = False


def _make_batch(monkeypatch, queued_total):
    batch = SGEBatchSystem({"sge_file": "dummy.xml"}, {}, _Options())

    class _StubStatMaker:
        tree = None
        root = object()

    batch.sge_stat_maker = _StubStatMaker()

    monkeypatch.setattr(sge_module.fileutils, "check_empty_file", lambda _path: None)
    monkeypatch.setattr(
        batch,
        "_extract_queues",
        lambda _xpath, _root: [{"queue_name": "main", "run": 2, "queued": 0, "lm": "0", "state": "r"}],
    )
    monkeypatch.setattr(batch, "_get_total_queued_jobs", lambda _xpath, _root: queued_total)
    return batch


def test_sge_queued_total_is_cast_to_int(monkeypatch):
    total_running, total_queued, qstatq_list = _make_batch(monkeypatch, "3").get_queues_info()

    assert total_running == 2
    assert total_queued == 3
    assert qstatq_list[-1]["queued"] == "3"


def test_sge_queued_total_payload_is_not_executed(monkeypatch, tmp_path):
    sentinel = tmp_path / "sge-total-eval-executed"
    payload = "__import__('pathlib').Path(%r).touch()" % str(sentinel)

    with pytest.raises(ValueError):
        _make_batch(monkeypatch, payload).get_queues_info()

    assert not sentinel.exists()
