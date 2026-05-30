##
## qtop is a tool to monitor queuing systems - https://github.com/qtop/qtop
##
## SPDX-License-Identifier: MIT
##

import os

from qtop_py.plugins.sge import SGEBatchSystem


class Options(object):
    ANONYMIZE = False
    SAMPLE = 0


def scheduler_files():
    repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    return {"sge_file": os.path.join(repo_root, "qtop_py", "contrib", "qstat.F.xml.stdout")}


def test_sge_contrib_sample_renders_queues_and_nodes():
    sge = SGEBatchSystem(scheduler_files(), {}, Options())

    job_ids, _usernames, _job_states, job_queues = sge.get_jobs_info()
    assert job_ids
    assert job_queues

    total_running, total_queued, queues = sge.get_queues_info()
    assert total_running > 0
    assert total_queued >= 0
    assert queues

    worker_nodes = sge.get_worker_nodes(job_ids, job_queues, Options())
    assert worker_nodes
    assert all("domainname" in node for node in worker_nodes)
