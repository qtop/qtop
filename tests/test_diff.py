from pathlib import Path

import pytest

from qtop_py import diff as diff_module
from qtop_py.diff import (
    DiffInputError,
    _build_jobs,
    _build_nodes,
    _deep_merge,
    _natural_key,
    _safe_display,
    _stable_hash_id,
    _stringify,
    anonymize_snapshot,
    build_cluster_snapshot,
    compare_snapshots,
    load_config,
    main,
    render_report,
    scheduler_files_for_source,
    write_json_report,
)


ROOT = Path(__file__).resolve().parents[1]
SAMPLES = ROOT / "tests" / "plugins" / "slurm_samples"


def test_build_cluster_snapshot_from_slurm_source_dir():
    config = load_config(None)

    snapshot = build_cluster_snapshot("alpha", "slurm", SAMPLES / "diff_cluster_a", config)

    assert snapshot.metrics()["nodes_total"] == 4
    assert snapshot.metrics()["cores_total"] == 24
    assert snapshot.metrics()["jobs_reported_running"] == 3
    assert snapshot.metrics()["jobs_reported_queued"] == 1
    assert snapshot.nodes["node002"].busy_cores == 1


def test_compare_snapshots_highlights_first_step_differences():
    config = load_config(None)
    left = build_cluster_snapshot("alpha", "slurm", SAMPLES / "diff_cluster_a", config)
    right = build_cluster_snapshot("beta", "slurm", SAMPLES / "diff_cluster_b", config)

    report = compare_snapshots(left, right)
    output = render_report(report, color=False)

    assert report.has_differences
    assert "jobs_reported_running" in output
    assert "1002" in output
    assert "node003" in output
    assert "node004" in output
    assert "user=bob state=R queue=compute" in output
    assert "user=bob state=PD queue=compute" in output


def test_cli_fail_on_diff_returns_two(capsys):
    exit_code = main(
        [
            "--scheduler",
            "slurm",
            "--left-source",
            str(SAMPLES / "diff_cluster_a"),
            "--right-source",
            str(SAMPLES / "diff_cluster_b"),
            "--left-name",
            "alpha",
            "--right-name",
            "beta",
            "--color",
            "never",
            "--fail-on-diff",
        ]
    )

    assert exit_code == 2
    assert "qtop differential report" in capsys.readouterr().out


def test_anonymize_snapshot_hashes_sensitive_values_deterministically():
    config = load_config(None)
    left = build_cluster_snapshot("alpha", "slurm", SAMPLES / "diff_cluster_a", config)
    right = build_cluster_snapshot("beta", "slurm", SAMPLES / "diff_cluster_b", config)

    left_anon = anonymize_snapshot(left)
    right_anon = anonymize_snapshot(right)
    job_key = _stable_hash_id("job", "1001")
    queue_key = _stable_hash_id("queue", "compute")
    node_key = _stable_hash_id("node", "node001")

    assert left_anon.jobs[job_key].job_id == job_key
    assert left_anon.jobs[job_key].user == right_anon.jobs[job_key].user
    assert left_anon.jobs[job_key].queue == right_anon.jobs[job_key].queue
    assert left_anon.queues[queue_key].limit == _stable_hash_id("queue-limit", "--")
    assert left_anon.nodes[node_key].name == node_key
    assert left_anon.source == _stable_hash_id("source", left.source)
    assert "alice" not in left_anon.jobs[job_key].user
    assert "compute" not in left_anon.jobs[job_key].queue
    assert "node001" not in left_anon.nodes[node_key].name
    assert left.source not in left_anon.source
    assert all("100" not in job_id for node in left_anon.nodes.values() for job_id in node.jobs)
    assert set(left_anon.queues) & set(right_anon.queues)


def test_stable_hash_id_uses_salt_and_longer_digest():
    first = _stable_hash_id("user", "alice", "salt-a")
    second = _stable_hash_id("user", "alice", "salt-b")

    assert first != second
    assert len(first.rsplit("_", 1)[1]) == 20
    assert _stable_hash_id("empty", "") == ""


def test_build_nodes_counts_only_active_jobs_as_busy_cores():
    nodes = _build_nodes(
        [
            {
                "domainname": "node001",
                "state": "-",
                "np": "4",
                "qname": "compute",
                "core_job_map": {0: "1001", 1: None, 2: "1002", 3: "1002"},
            }
        ]
    )

    assert nodes["node001"].busy_cores == 3
    assert nodes["node001"].jobs == ("1001", "1002")
    assert nodes["node001"].queues == ("compute",)


def test_natural_key_never_compares_int_to_str():
    values = ["node2", "node10", "nodeA", "node1a", "node1"]

    assert sorted(values, key=_natural_key) == ["node1", "node1a", "node2", "node10", "nodeA"]


def test_deep_merge_preserves_sibling_scheduler_config():
    merged = _deep_merge(
        {"schedulers": {"slurm": {"sinfo_file": "old"}, "pbs": {"qstat_file": "keep"}}},
        {"schedulers": {"slurm": {"squeue_file": "new"}}},
    )

    assert merged["schedulers"]["slurm"] == {"sinfo_file": "old", "squeue_file": "new"}
    assert merged["schedulers"]["pbs"] == {"qstat_file": "keep"}


def test_build_jobs_rejects_mismatched_scheduler_vectors():
    with pytest.raises(DiffInputError, match="mismatched job vector lengths"):
        _build_jobs(["1", "2"], ["alice"], ["R", "PD"], ["compute", "long"])


def test_build_jobs_preserves_duplicate_scheduler_ids_with_synthetic_keys():
    jobs = _build_jobs(["1", "1"], ["alice", "bob"], ["R", "PD"], ["compute", "long"])

    assert list(jobs) == ["1", "1#2"]
    assert jobs["1"].job_id == "1"
    assert jobs["1#2"].job_id == "1"
    assert jobs["1"].user == "alice"
    assert jobs["1#2"].user == "bob"


def test_build_nodes_rejects_non_mapping_worker_node():
    with pytest.raises(DiffInputError, match="worker node entry must be a mapping"):
        _build_nodes(["not-a-node"])


def test_build_nodes_rejects_non_mapping_core_job_map():
    with pytest.raises(DiffInputError, match="node 'node001' core_job_map must be a mapping"):
        _build_nodes([{"domainname": "node001", "state": "-", "np": "4", "core_job_map": "not-a-map"}])


def test_load_config_treats_none_parses_as_empty_dict(monkeypatch, tmp_path):
    default_config = tmp_path / "qtopconf.yaml"
    overlay_config = tmp_path / "overlay.yaml"
    default_config.write_text("", encoding="utf-8")
    overlay_config.write_text("", encoding="utf-8")
    monkeypatch.setattr(diff_module, "default_config_path", lambda: default_config)
    monkeypatch.setattr(diff_module.yaml, "parse", lambda _path: None)

    assert load_config(overlay_config) == {}


def test_load_config_rejects_non_mapping_parse(monkeypatch, tmp_path):
    default_config = tmp_path / "qtopconf.yaml"
    default_config.write_text("", encoding="utf-8")
    monkeypatch.setattr(diff_module, "default_config_path", lambda: default_config)
    monkeypatch.setattr(diff_module.yaml, "parse", lambda _path: ["not", "a", "mapping"])

    with pytest.raises(DiffInputError, match="must be a mapping"):
        load_config(None)


def test_scheduler_files_for_source_accepts_comma_without_space(tmp_path):
    source = tmp_path / "cluster"
    source.mkdir()
    (source / "sinfo.txt").write_text("node001|compute|idle|4\n", encoding="utf-8")
    config = {"schedulers": {"slurm": {"sinfo_file": "%(savepath)s/sinfo%(pid)s.txt,sinfo -N"}}}

    assert Path(scheduler_files_for_source(source, "slurm", config)["sinfo_file"]) == source / "sinfo.txt"


def test_scheduler_files_for_source_rejects_non_string_template(tmp_path):
    config = {"schedulers": {"slurm": {"sinfo_file": ["not-a-template"]}}}

    with pytest.raises(DiffInputError, match="must be a string"):
        scheduler_files_for_source(tmp_path, "slurm", config)


def test_safe_display_strips_terminal_escape_sequences():
    assert _safe_display("left\x1b[31mred\x1b[0mright\x1bc\x00") == "leftredright?"


def test_stringify_handles_none_and_callable_str_attribute():
    class Value(object):
        def str(self):
            return "from-method"

    assert _stringify(None) == ""
    assert _stringify(Value()) == "from-method"


def test_write_json_report_creates_parent_directory(tmp_path):
    config = load_config(None)
    left = build_cluster_snapshot("alpha", "slurm", SAMPLES / "diff_cluster_a", config)
    right = build_cluster_snapshot("beta", "slurm", SAMPLES / "diff_cluster_b", config)
    output_path = tmp_path / "nested" / "qtop-diff.json"

    write_json_report(compare_snapshots(left, right), output_path)

    assert output_path.is_file()
