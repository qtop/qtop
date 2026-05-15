#!/usr/bin/env python3
"""
Regression test for qtop against PBS sample data.
Run via: make test  or  python3 -m pytest tests/
Compares qtop output with expected output for 447 PBS cluster samples.
"""
import subprocess
import sys
import os
import json
import glob
import re

QTOP_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SAMPLES_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                           "tests", "pbs_samples")
# If samples are in the test repo, allow override via env var
SAMPLES_DIR = os.environ.get("QTOP_SAMPLES_DIR", SAMPLES_DIR)


def run_qtop_json(sample_dir):
    """Run qtop on a sample directory and return the JSON output."""
    env = os.environ.copy()
    env["TERM"] = "xterm"

    cmd = [
        sys.executable, "-m", "qtop_py.cli",
        "-b", "pbs",
        "-s", sample_dir,
        "-c", "OFF",
        "-O",
        "-E",
        "-1", "-2", "-3",
    ]

    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        timeout=30,
        env=env,
        cwd=QTOP_DIR,
    )

    savepath = f"/tmp/qtop_results_{os.environ.get('USER', 'mac')}"
    json_files = sorted(glob.glob(os.path.join(savepath, "qtop_json_*.json")))
    if json_files:
        latest = json_files[-1]
        with open(latest) as f:
            return json.load(f)
    return None


def run_qtop_text(sample_dir):
    """Run qtop and return text output."""
    env = os.environ.copy()
    env["TERM"] = "xterm"

    cmd = [
        sys.executable, "-m", "qtop_py.cli",
        "-b", "pbs",
        "-s", sample_dir,
        "-c", "OFF",
        "-l",
    ]

    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        timeout=30,
        env=env,
        cwd=QTOP_DIR,
    )
    return result.stdout


def find_expected_out(sample_dir):
    """Find the expected qtop output file."""
    expected = os.path.join(sample_dir, "qtop.out")
    if os.path.exists(expected):
        return expected
    return None


def extract_metrics(text):
    """Extract key metrics from qtop text output."""
    metrics = {}
    m = re.search(r'(\d+)/(\d+)\s*Nodes', text)
    if m:
        metrics["nodes"] = (int(m.group(1)), int(m.group(2)))
    m = re.search(r'(\d+)/(\s*\d+)\s*Cores', text)
    if m:
        metrics["cores"] = (int(m.group(1)), int(m.group(2).strip()))
    m = re.search(r'(\d+)\+(\d+)\s*jobs', text)
    if m:
        metrics["jobs"] = (int(m.group(1)), int(m.group(2)))
    return metrics


def test_all_pbs_samples():
    """Test that qtop produces correct output for all PBS samples."""
    sample_dirs = sorted(glob.glob(os.path.join(SAMPLES_DIR, "gef_*")))
    assert len(sample_dirs) >= 100, \
        f"Expected at least 100 PBS samples, found {len(sample_dirs)}"

    errors = []
    for sd in sample_dirs:
        name = os.path.basename(sd)
        expected_file = find_expected_out(sd)
        if not expected_file:
            continue

        with open(expected_file) as f:
            expected_text = f.read()

        try:
            actual_text = run_qtop_text(sd)
        except Exception as e:
            errors.append(f"{name}: runtime error - {e}")
            continue

        exp_metrics = extract_metrics(expected_text)
        act_metrics = extract_metrics(actual_text)

        for key in ["nodes", "cores", "jobs"]:
            if exp_metrics.get(key) and act_metrics.get(key) and \
               exp_metrics[key] != act_metrics[key]:
                errors.append(
                    f"{name}: {key} mismatch "
                    f"expected={exp_metrics[key]} got={act_metrics[key]}"
                )

    assert not errors, \
        f"Regression test failed with {len(errors)} mismatches:\n" + \
        "\n".join(errors[:20])


def test_pbs_json_export():
    """Test that JSON export contains expected structure."""
    sample_dirs = sorted(glob.glob(os.path.join(SAMPLES_DIR, "gef_*")))
    assert len(sample_dirs) > 0, "No sample directories found"

    # Test the first sample
    sd = sample_dirs[0]
    data = run_qtop_json(sd)
    assert data is not None, "JSON export returned None"
    assert len(data) == 5, f"Expected 5-element JSON, got {len(data)}"

    worker_nodes, job_info, queue_info, total_running, total_queued = data
    assert isinstance(worker_nodes, list), "worker_nodes should be a list"
    assert isinstance(job_info, dict), "job_info should be a dict"
    assert isinstance(queue_info, dict), "queue_info should be a dict"


def test_compress_colored_line_empty():
    """Test that compress_colored_line handles empty input."""
    from qtop_py.qtop import compress_colored_line
    assert compress_colored_line("") == ""
    assert compress_colored_line("plain text") == "plain text"
    assert compress_colored_line("\x1b[1;31mhello\x1b[0;m") != ""


def test_get_corejob_from_range():
    """Test core job range parsing."""
    from qtop_py.plugins.pbs import PBSBatchSystem
    result = list(PBSBatchSystem.get_corejob_from_range("0-4,30-31", "10102182"))
    cores = [c for c, j in result]
    assert cores == ["0", "1", "2", "3", "4", "30", "31"], \
        f"Unexpected cores: {cores}"


def test_ensure_worker_nodes_have_qnames():
    """Test that qname doesn't contain None values."""
    from qtop_py.serialiser import GenericBatchSystem
    worker_nodes = [
        {"core_job_map": {"0": "123", "1": "456"}, "domainname": "node1"},
    ]
    job_ids = ["123"]
    job_queues = ["workq"]
    result = GenericBatchSystem.ensure_worker_nodes_have_qnames(
        worker_nodes, job_ids, job_queues
    )
    assert None not in result[0]["qname"], \
        f"qname contains None: {result[0]['qname']}"