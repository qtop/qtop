##
## qtop is a tool to monitor queuing systems - https://github.com/qtop/qtop
##
## Copyright (c) 2016 Fotis Georgatos
## Copyright (c) 2016 Sotiris Fragkiskos
## Copyright (c) 2023 Hewlett Packard Enterprise Development LP
##
## SPDX-License-Identifier: MIT
##

from argparse import Namespace

from qtop_py.plugins import slurm


def test_expand_nodelist_with_ranges():
    assert slurm.SlurmBatchSystem._expand_nodelist("node[001-003,010],gpu01") == ["node001", "node002", "node003", "node010", "gpu01"]


def test_extract_squeue(tmp_path):
    squeue = tmp_path / "squeue.txt"
    squeue.write_text("123|alice|RUNNING|debug|2|node001\n124|bob|PENDING|batch|1|(None assigned)\n")

    extractor = slurm.SlurmStatExtractor({}, Namespace(ANONYMIZE=False))

    assert extractor.extract_squeue(str(squeue)) == [
        {"JobId": "123", "UnixAccount": "alice", "S": "RUNNING", "Queue": "debug", "Cpus": 2, "NodeList": "node001"},
        {"JobId": "124", "UnixAccount": "bob", "S": "PENDING", "Queue": "batch", "Cpus": 1, "NodeList": "(None assigned)"},
    ]


def test_get_worker_nodes(tmp_path):
    scontrol = tmp_path / "scontrol_nodes.txt"
    scontrol.write_text(
        "NodeName=node001 Arch=x86_64 CPUTot=4 State=ALLOCATED\n"
        "NodeName=node002 Arch=x86_64 CPUTot=4 State=IDLE\n"
    )
    squeue = tmp_path / "squeue.txt"
    squeue.write_text("123|alice|RUNNING|debug|2|node001\n")

    batch_system = slurm.SlurmBatchSystem(
        {"scontrol_nodes_file": str(scontrol), "squeue_file": str(squeue)},
        {},
        Namespace(ANONYMIZE=False),
    )

    worker_nodes = batch_system.get_worker_nodes(["123"], ["debug"], Namespace())

    assert worker_nodes == [
        {"domainname": "node001", "state": "-", "np": 4, "core_job_map": {"0": "123", "1": "123"}, "qname": ["debug"]},
        {"domainname": "node002", "state": "-", "np": 4, "core_job_map": {}, "qname": []},
    ]


def test_get_jobs_and_queues_info(tmp_path):
    scontrol = tmp_path / "scontrol_nodes.txt"
    scontrol.write_text("NodeName=node001 CPUTot=4 State=ALLOCATED\n")
    squeue = tmp_path / "squeue.txt"
    squeue.write_text("123|alice|RUNNING|debug|2|node001\n124|bob|PENDING|debug|1|(None assigned)\n")

    batch_system = slurm.SlurmBatchSystem(
        {"scontrol_nodes_file": str(scontrol), "squeue_file": str(squeue)},
        {},
        Namespace(ANONYMIZE=False),
    )

    assert batch_system.get_jobs_info() == (["123", "124"], ["alice", "bob"], ["R", "Q"], ["debug", "debug"])
    assert batch_system.get_queues_info() == (1, 1, [{"queue_name": "debug", "run": 1, "queued": 1, "lm": "--", "state": "E"}])
