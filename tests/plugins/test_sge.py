import os

from qtop_py.plugins.sge import SGEBatchSystem


class Options(object):
    ANONYMIZE = False
    SAMPLE = 0


def scheduler_files(sample_name):
    return {"sge_file": os.path.join(os.path.dirname(__file__), "sge_samples", sample_name, "qstat.xml")}


def test_sge_command_trace_sample():
    sge = SGEBatchSystem(scheduler_files("basic"), {}, Options())

    assert sge.get_jobs_info() == (["101", "102"], ["alice", "bob"], ["r", "qw"], ["compute.q", "Pending"])
    assert sge.get_queues_info() == (
        1,
        1,
        [
            {"queue_name": "compute.q", "run": "1", "queued": "0", "lm": 0, "state": "-"},
            {"queue_name": "Pending", "run": "0", "queued": "1", "lm": "0", "state": "Q"},
        ],
    )
    assert sge.get_worker_nodes(["101", "102"], ["compute.q", "Pending"], Options()) == [
        {"domainname": "node001", "np": 4, "state": "-", "qname": ["compute.q"], "core_job_map": {0: "101"}, "existing_busy_cores": 1}
    ]
