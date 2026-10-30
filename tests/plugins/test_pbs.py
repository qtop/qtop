##
## qtop is a tool to monitor queuing systems - https://github.com/qtop/qtop
##
## Copyright (c) 2016 Fotis Georgatos
## Copyright (c) 2016 Sotiris Fragkiskos
## Copyright (c) 2023 Hewlett Packard Enterprise Development LP
##
## SPDX-License-Identifier: MIT
##

from io import StringIO
from types import SimpleNamespace

import pytest

from qtop_py.plugins import pbs


@pytest.mark.parametrize("raw, invalid", [(None, False), ("0", False), ("2", False), ("", True), ("N/A", True), ("two", True), ("1.5", True), ("-1", True)])
def test_optional_gpu_counts_cannot_hide_pbs_nodes_or_jobs(tmp_path, caplog, raw, invalid):
    path = tmp_path / "pbsnodes.txt"
    path.write_text("node1\n    state = free\n    np = 2\n    jobs = 0/101.server\n" + ("    gpus = %s\n" % raw if raw is not None else ""))
    options = SimpleNamespace(ANONYMIZE=False)
    backend = pbs.PBSBatchSystem({"pbsnodes_file": str(path)}, {}, options)
    nodes = backend.get_worker_nodes(["101"], ["workq"], options)
    assert len(nodes) == 1
    assert nodes[0]["domainname"] == "node1"
    assert nodes[0]["core_job_map"] == {"0": "101"}
    assert nodes[0]["qname"] == ["workq"]
    assert nodes[0].get("gpus") == ("2" if raw == "2" else None)
    assert ("Ignoring invalid PBS GPU count" in caplog.text) is invalid


@pytest.mark.parametrize(
    "core_selections, result",
    (
        ("0-4,30-31", ["0", "1", "2", "3", "4", "30", "31"]),
        ("5-9", ["5", "6", "7", "8", "9"]),
        ("0,3,5-7,11", ["0", "3", "5", "6", "7", "11"]),
        ("1,2-4,5-6,9", ["1", "2", "3", "4", "5", "6", "9"]),
    ),
)
def test_get_corejob_from_range(core_selections, result, job=None):
    result = iter(result)
    for core, job in pbs.PBSBatchSystem.get_corejob_from_range(core_selections, job):
        assert core == next(result)


@pytest.mark.parametrize(
    "jobs, result",
    (
        (["0/10102182.f-batch01.grid.sinica.edu.tw", "1/10102106.f-batch01.grid.sinica.edu.tw"], [("10102182", "0"), ("10102106", "1")]),
        (["2/10102339.f-batch01.grid.sinica.edu.tw", "3/10104007.f-batch01.grid.sinica.edu.tw"], [("10102339", "2"), ("10104007", "3")]),
        (["3-5/10102339.f-batch01.grid.sinica.edu.tw"], [("10102339", "3"), ("10102339", "4"), ("10102339", "5")]),
        (["2257887.cluster-pbs5/0", "2257887.cluster-pbs5/1"], [("2257887", "0"), ("2257887", "1")]),
        (["2257887.cluster-pbs5/2", "2257887.cluster-pbs5/3"], [("2257887", "2"), ("2257887", "3")]),
    ),
)
def test_get_jobs_cores(jobs, result):
    result = iter(result)
    for job, core in pbs.PBSBatchSystem._get_jobs_cores(jobs):
        assert (job, core) == next(result)


def test_read_block_ignores_incomplete_values_and_stops_at_eof():
    block = pbs.PBSBatchSystem._read_block(StringIO("node01\nstate = free\njobs =\n"))

    assert block == {"domainname": "node01", "state": "free"}


def test_blank_separators_and_wrapped_allocations_preserve_every_node(tmp_path):
    path = tmp_path / "pbsnodes.txt"
    path.write_text(
        "\n\nnode1\n    state = free\n    np = 2\n    jobs = 0/101.server,\n        1/102.server\n\n \n\nnode2\n    state = free\n    np = 1\n    jobs = 0/103.server\n"
    )
    options = SimpleNamespace(ANONYMIZE=False)
    backend = pbs.PBSBatchSystem({"pbsnodes_file": str(path)}, {}, options)
    nodes = backend.get_worker_nodes(["101", "102", "103"], ["workq"] * 3, options)
    assert [node["domainname"] for node in nodes] == ["node1", "node2"]
    assert [node["core_job_map"] for node in nodes] == [{"0": "101", "1": "102"}, {"0": "103"}]


@pytest.mark.parametrize("allocation", ["0/101[7].server", "0-1/101[7].server", "101[7].server/0", "101[7].server/0-1"])
def test_task_identity_does_not_depend_on_slot_format(allocation):
    jobs = list(pbs.PBSBatchSystem._get_jobs_cores([allocation]))
    assert {job for job, _ in jobs} == {"101[7]"}
    assert [core for _, core in jobs] == (["0", "1"] if "0-1" in allocation else ["0"])


@pytest.mark.parametrize("known, expected", [(["101[7]", "101"], "101[7]"), (["101"], "101"), (["101[]"], "101[]"), (["999"], "101[7]")])
def test_array_allocations_resolve_only_against_reported_jobs(tmp_path, known, expected):
    path = tmp_path / "pbsnodes.txt"
    path.write_text("node1\n    state = free\n    np = 2\n    jobs = 0-1/101[7].server\n")
    options = SimpleNamespace(ANONYMIZE=False)
    backend = pbs.PBSBatchSystem({"pbsnodes_file": str(path)}, {}, options)
    node = backend.get_worker_nodes(known, ["workq"] * len(known), options)[0]
    assert node["core_job_map"] == {"0": expected, "1": expected}
    assert node["qname"] == (["workq"] if expected in known else [None])
