##
## qtop is a tool to monitor queuing systems - https://github.com/qtop/qtop
##
## SPDX-License-Identifier: MIT
##

from pathlib import Path

from qtop_py.plugins.sge import SGEBatchSystem


class Options(object):
    ANONYMIZE = False


def test_contrib_queue_counts_remain_numeric_while_aggregating():
    sample = Path(__file__).resolve().parents[2] / "qtop_py" / "contrib" / "qstat.F.xml.stdout"
    sge = SGEBatchSystem({"sge_file": str(sample)}, {}, Options())

    sge.get_jobs_info()
    total_running, total_queued, queues = sge.get_queues_info()

    assert isinstance(total_running, int)
    assert isinstance(total_queued, int)
    assert total_running == sum(int(queue["run"]) for queue in queues)
    assert next(queue for queue in queues if queue["queue_name"] == "Pending")["state"] == "E"
