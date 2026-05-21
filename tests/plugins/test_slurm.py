from qtop_py.plugins import slurm


class Options(object):
    ANONYMIZE = False


def test_expand_slurm_nodelist_simple_range():
    assert slurm.expand_slurm_nodelist("wn[001-003,005]") == ["wn001", "wn002", "wn003", "wn005"]


def test_expand_slurm_nodelist_multiple_groups():
    assert slurm.expand_slurm_nodelist("rack[1-2]n[01-02]") == ["rack1n01", "rack1n02", "rack2n01", "rack2n02"]


def test_extract_squeue(tmp_path):
    squeue_file = tmp_path / "squeue.txt"
    squeue_file.write_text("1001|batch|alice|R|4|wn[001-002]\n1002|debug|bob|PD|2|(Priority)\n")

    extractor = slurm.SlurmStatExtractor({}, Options())

    assert extractor.extract_squeue(str(squeue_file)) == [
        {"JobId": "1001", "Queue": "batch", "UnixAccount": "alice", "S": "R", "CPUs": 4, "NodeList": "wn[001-002]"},
        {"JobId": "1002", "Queue": "debug", "UnixAccount": "bob", "S": "PD", "CPUs": 2, "NodeList": "(Priority)"},
    ]


def test_extract_sinfo(tmp_path):
    sinfo_file = tmp_path / "sinfo.txt"
    sinfo_file.write_text("wn[001-002]|batch*|mix|64\nwn003|debug|idle|32\nwn004|batch|down|64\nwn001|debug|alloc|32\n")

    extractor = slurm.SlurmStatExtractor({}, Options())
    nodes = extractor.extract_sinfo(str(sinfo_file))

    assert nodes[0]["domainname"] == "wn001"
    assert nodes[0]["state"] == "%"
    assert nodes[0]["np"] == 64
    assert nodes[0]["qname"] == ["batch", "debug"]
    assert nodes[2]["state"] == "-"
    assert nodes[3]["state"] == "d"


def test_get_worker_nodes_maps_running_jobs(tmp_path):
    sinfo_file = tmp_path / "sinfo.txt"
    squeue_file = tmp_path / "squeue.txt"
    sinfo_file.write_text("wn[001-002]|batch|idle|4\n")
    squeue_file.write_text("1001|batch|alice|R|4|wn[001-002]\n1002|batch|bob|PD|1|(Priority)\n")
    batch = slurm.SlurmBatchSystem({"sinfo_file": str(sinfo_file), "squeue_file": str(squeue_file)}, {}, Options())

    nodes = batch.get_worker_nodes(["1001", "1002"], ["batch", "batch"], Options())

    assert nodes[0]["core_job_map"] == {"0": "1001", "1": "1001"}
    assert nodes[1]["core_job_map"] == {"0": "1001", "1": "1001"}
    assert nodes[0]["qname"] == ["batch"]
