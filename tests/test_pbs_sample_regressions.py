from collections import OrderedDict
from types import SimpleNamespace

from qtop_py import qtop
from qtop_py.plugins.pbs import PBSStatExtractor


def test_find_matrices_width_handles_empty_worker_nodes():
    qtop.cluster = SimpleNamespace(highest_wn=0, workernode_list=[])
    qtop.viewport = SimpleNamespace(h_term_size=80)
    qtop.config = {
        "workernodes_matrix": [{"wn id lines": {"min_masking_threshold": 30}}],
        "USER_CUT_MATRIX_WIDTH": None,
    }
    qtop.args = SimpleNamespace(NOMASKING=False, REMAP=False)

    occupancy = qtop.WNOccupancy.__new__(qtop.WNOccupancy)

    assert occupancy.find_matrices_width() == (0, 0, 0)


def test_general_attribute_lines_handle_empty_worker_node_dict():
    occupancy = qtop.WNOccupancy.__new__(qtop.WNOccupancy)
    occupancy.cluster = SimpleNamespace(workernode_dict={})
    config = {"scheduler": "pbs", "workernodes_matrix": [{"state": {"max_len": 1}}]}

    assert occupancy.calc_general_mult_attr_line("state", "state", config) == OrderedDict()


def test_pbs_qstat_regex_skips_unmatched_lines(tmp_path):
    qstat_file = tmp_path / "qstat.txt"
    qstat_file.write_text(
        "Job id           Name             User             Time Use S Queue\n"
        "---------------- ---------------- ---------------- -------- - -----\n"
        "this line does not match either supported qstat format\n"
        "1234.server      myjob            alice            00:01:00 R workq\n"
    )
    extractor = PBSStatExtractor({}, SimpleNamespace(ANONYMIZE=False))

    assert extractor._extract_qstat_regex(str(qstat_file)) == [
        {"JobId": "1234", "UnixAccount": "alice", "S": "R", "Queue": "workq"}
    ]


def test_decide_remapping_handles_mixed_numeric_and_named_nodes():
    cluster = qtop.Cluster.__new__(qtop.Cluster)
    cluster.total_wn = 2
    cluster.args = SimpleNamespace(BLINDREMAP=False)
    cluster.node_subclusters = {"wn"}
    cluster.workernode_list = [1, "trueno_ita"]
    cluster.offdown_nodes = 0
    cluster.config = {"exotic_starting_wn_nr": 100, "percentage": 0.5}

    assert cluster.decide_remapping(["1", ""]) is True


def test_filtering_name_patterns_removes_nodes_without_user_remap(monkeypatch):
    args = SimpleNamespace(ANONYMIZE=False, BLINDREMAP=False, COLOR="OFF", REMAP=False)
    monkeypatch.setattr(qtop, "args", args, raising=False)
    monkeypatch.setattr(qtop, "dynamic_config", {}, raising=False)
    monkeypatch.setattr(qtop, "user_to_color", {}, raising=False)
    config = {"exotic_starting_wn_nr": 1000, "filtering": [{"exclude_name_patterns": ["wn002"]}], "percentage": 0.8, "remapping": [], "sorting": {}, "workernodes_matrix": [{"wn id lines": {"max_len": 0}}]}
    nodes = [{"domainname": name, "state": [qtop.utils.ColorStr("-")], "np": 1, "core_job_map": {}} for name in ["wn001.example", "wn002.example", "wn003.example"]]
    document = SimpleNamespace(jobs_dict={}, queues_dict={}, total_running_jobs=0, total_queued_jobs=0)

    cluster = qtop.Cluster(document, nodes, qtop.WNFilter, config, args)

    assert [node["domainname"] for node in cluster.workernode_dict.values()] == ["wn001.example", "wn003.example"]


def test_dynamic_empty_filtering_does_not_force_remapping(monkeypatch):
    monkeypatch.setattr(qtop, "dynamic_config", {"filtering": []}, raising=False)
    cluster = qtop.Cluster.__new__(qtop.Cluster)
    cluster.total_wn = 3
    cluster.args = SimpleNamespace(BLINDREMAP=False)
    cluster.node_subclusters = {"wn"}
    cluster.workernode_list = [1, 2, 3]
    cluster.offdown_nodes = 0
    cluster.config = {"exotic_starting_wn_nr": 1000, "filtering": [{"exclude_name_patterns": ["wn002"]}], "percentage": 0.8}

    assert cluster.decide_remapping(["1", "2", "3"]) is False


def test_valid_corejobs_skips_jobs_missing_from_qstat():
    occupancy = qtop.WNOccupancy.__new__(qtop.WNOccupancy)

    assert list(occupancy._valid_corejobs({"0": "9999"}, {})) == []


def test_worker_node_name_regex_accepts_underscores():
    re_nodename = r"(^[A-Za-z0-9_-]+)(?=\.|$)"
    import re

    assert re.search(re_nodename, "trueno_ita00.csic.es").group(0) == "trueno_ita00"
