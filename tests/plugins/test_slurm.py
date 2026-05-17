from types import SimpleNamespace

from qtop_py.plugins.slurm import SlurmBatchSystem


def build_options():
    return SimpleNamespace(ANONYMIZE=False)


def scheduler_files(case_id):
    return {
        "squeue_file": f"tests/inputs/slurm_squeue_{case_id}.txt",
        "sinfo_file": f"tests/inputs/slurm_sinfo_{case_id}.txt",
        "scontrol_file": f"tests/inputs/slurm_scontrol_{case_id}.txt",
    }


def test_slurm_case_1():
    batch = SlurmBatchSystem(scheduler_files(1), {}, build_options())
    job_ids, users, states, queues = batch.get_jobs_info()
    total_run, total_queue, queue_rows = batch.get_queues_info()
    workers = batch.get_worker_nodes([], [], build_options())

    assert job_ids == ["101", "102"]
    assert users == ["alice", "bob"]
    assert states == ["R", "PD"]
    assert queues == ["compute", "debug"]
    assert total_run == 3
    assert total_queue == 1
    assert queue_rows[0]["queue_name"] == "compute"
    assert workers[0]["domainname"] == "node01"
    assert workers[0]["np"] == "16"


def test_slurm_case_2():
    batch = SlurmBatchSystem(scheduler_files(2), {}, build_options())
    job_ids, users, states, queues = batch.get_jobs_info()
    total_run, total_queue, queue_rows = batch.get_queues_info()
    workers = batch.get_worker_nodes([], [], build_options())

    assert job_ids == ["2001", "2002", "2003"]
    assert users == ["carol", "dave", "erin"]
    assert states == ["R", "CG", "PD"]
    assert queues == ["long", "long", "short"]
    assert total_run == 5
    assert total_queue == 2
    assert len(queue_rows) == 2
    assert len(workers) == 2


def test_slurm_case_3():
    batch = SlurmBatchSystem(scheduler_files(3), {}, build_options())
    job_ids, users, states, queues = batch.get_jobs_info()
    total_run, total_queue, queue_rows = batch.get_queues_info()
    workers = batch.get_worker_nodes([], [], build_options())

    assert job_ids == ["30001"]
    assert users == ["frank"]
    assert states == ["R"]
    assert queues == ["gpu"]
    assert total_run == 8
    assert total_queue == 0
    assert queue_rows[0]["state"] == "up"
    assert workers[0]["state"] == "a"
