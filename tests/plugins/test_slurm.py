"""
tests/plugins/test_slurm.py

Conformance tests for the Slurm qtop plugin (3 synthetic test cases).
"""
import os

import pytest

from qtop_py.plugins.slurm import SlurmBatchSystem

FIXTURES_DIR = os.path.join(os.path.dirname(__file__), "..", "slurm_fixtures")


class FakeOptions:
    ANONYMIZE = False


class FakeConfig:
    pass


def make_system(case):
    filenames = {
        "squeue_file": os.path.join(FIXTURES_DIR, case, "squeue.txt"),
        "sinfo_file": os.path.join(FIXTURES_DIR, case, "sinfo.txt"),
    }
    return SlurmBatchSystem(filenames, FakeConfig(), FakeOptions())


# ---------------------------------------------------------------------------
# Test Case 1: mixed running / pending, compact nodelist expansion
# ---------------------------------------------------------------------------

class TestCase1MixedCluster:
    def test_jobs_count(self):
        s = make_system("slurm_test1")
        ids, _, _, _ = s.get_jobs_info()
        assert len(ids) == 10

    def test_running_job(self):
        s = make_system("slurm_test1")
        ids, _, states, _ = s.get_jobs_info()
        sm = dict(zip(ids, states))
        assert sm["1001"] == "R"

    def test_pending_job(self):
        s = make_system("slurm_test1")
        ids, _, states, _ = s.get_jobs_info()
        sm = dict(zip(ids, states))
        assert sm["1004"] == "Q"

    def test_queues_totals(self):
        s = make_system("slurm_test1")
        tr, tq, _ = s.get_queues_info()
        assert tr >= 1 and tq >= 1

    def test_normal_queue_exists(self):
        s = make_system("slurm_test1")
        _, _, ql = s.get_queues_info()
        assert "normal" in [q["queue_name"] for q in ql]

    def test_worker_node_count(self):
        s = make_system("slurm_test1")
        ids, _, _, queues = s.get_jobs_info()
        wns = s.get_worker_nodes(ids, queues, FakeOptions())
        assert len(wns) == 12

    def test_alloc_node_state(self):
        s = make_system("slurm_test1")
        ids, _, _, queues = s.get_jobs_info()
        wns = s.get_worker_nodes(ids, queues, FakeOptions())
        node01 = next(w for w in wns if w["domainname"] == "node01")
        assert node01["state"] == "R"

    def test_idle_node_state(self):
        s = make_system("slurm_test1")
        ids, _, _, queues = s.get_jobs_info()
        wns = s.get_worker_nodes(ids, queues, FakeOptions())
        node06 = next(w for w in wns if w["domainname"] == "node06")
        assert node06["state"] == "-"

    def test_nodelist_expansion(self):
        s = make_system("slurm_test1")
        ids, _, _, queues = s.get_jobs_info()
        wns = s.get_worker_nodes(ids, queues, FakeOptions())
        node01 = next(w for w in wns if w["domainname"] == "node01")
        assert "1001" in node01["core_job_map"].values()

    def test_node_np(self):
        s = make_system("slurm_test1")
        ids, _, _, queues = s.get_jobs_info()
        wns = s.get_worker_nodes(ids, queues, FakeOptions())
        node01 = next(w for w in wns if w["domainname"] == "node01")
        assert int(node01["np"]) == 32


# ---------------------------------------------------------------------------
# Test Case 2: down / drained nodes, suspended jobs
# ---------------------------------------------------------------------------

class TestCase2DrainDown:
    def test_jobs_count(self):
        s = make_system("slurm_test2")
        ids, _, _, _ = s.get_jobs_info()
        assert len(ids) == 13

    def test_suspended_job(self):
        s = make_system("slurm_test2")
        ids, _, states, _ = s.get_jobs_info()
        assert dict(zip(ids, states))["2013"] == "S"

    def test_down_node(self):
        s = make_system("slurm_test2")
        ids, _, _, queues = s.get_jobs_info()
        wns = s.get_worker_nodes(ids, queues, FakeOptions())
        assert next(w for w in wns if w["domainname"] == "wn18")["state"] == "d"

    def test_drain_node(self):
        s = make_system("slurm_test2")
        ids, _, _, queues = s.get_jobs_info()
        wns = s.get_worker_nodes(ids, queues, FakeOptions())
        assert next(w for w in wns if w["domainname"] == "wn19")["state"] == "d"

    def test_worker_node_count(self):
        s = make_system("slurm_test2")
        ids, _, _, queues = s.get_jobs_info()
        wns = s.get_worker_nodes(ids, queues, FakeOptions())
        assert len(wns) == 20

    def test_queues_batch_and_express(self):
        s = make_system("slurm_test2")
        _, _, ql = s.get_queues_info()
        names = [q["queue_name"] for q in ql]
        assert "batch" in names and "express" in names

    @pytest.mark.parametrize("nodename", ["wn01", "wn02", "wn03", "wn04"])
    def test_compact_nodelist(self, nodename):
        s = make_system("slurm_test2")
        ids, _, _, queues = s.get_jobs_info()
        wns = s.get_worker_nodes(ids, queues, FakeOptions())
        node = next(w for w in wns if w["domainname"] == nodename)
        assert "2001" in node["core_job_map"].values()


# ---------------------------------------------------------------------------
# Test Case 3: GPU cluster, COMPLETING / FAILED states
# ---------------------------------------------------------------------------

class TestCase3GPUCluster:
    def test_completing_state(self):
        s = make_system("slurm_test3")
        ids, _, states, _ = s.get_jobs_info()
        assert dict(zip(ids, states))["3007"] == "C"

    def test_failed_state(self):
        s = make_system("slurm_test3")
        ids, _, states, _ = s.get_jobs_info()
        assert dict(zip(ids, states))["3008"] == "F"

    def test_gpu_node_np(self):
        s = make_system("slurm_test3")
        ids, _, _, queues = s.get_jobs_info()
        wns = s.get_worker_nodes(ids, queues, FakeOptions())
        assert int(next(w for w in wns if w["domainname"] == "gpu01")["np"]) == 8

    def test_mixed_state_node(self):
        s = make_system("slurm_test3")
        ids, _, _, queues = s.get_jobs_info()
        wns = s.get_worker_nodes(ids, queues, FakeOptions())
        assert next(w for w in wns if w["domainname"] == "gpu07")["state"] == "R"

    def test_drain_node(self):
        s = make_system("slurm_test3")
        ids, _, _, queues = s.get_jobs_info()
        wns = s.get_worker_nodes(ids, queues, FakeOptions())
        assert next(w for w in wns if w["domainname"] == "maint02")["state"] == "d"

    def test_gpu_queue_exists(self):
        s = make_system("slurm_test3")
        _, _, ql = s.get_queues_info()
        assert "gpu" in [q["queue_name"] for q in ql]

    def test_gpu_queue_pending(self):
        s = make_system("slurm_test3")
        _, _, ql = s.get_queues_info()
        gpu_q = next(q for q in ql if q["queue_name"] == "gpu")
        assert gpu_q["queued"] >= 3

    def test_total_jobs(self):
        s = make_system("slurm_test3")
        ids, _, _, _ = s.get_jobs_info()
        assert len(ids) == 10

    @pytest.mark.parametrize("nodename", ["gpu01", "gpu02", "gpu03", "gpu04"])
    def test_gpu_nodelist_expansion(self, nodename):
        s = make_system("slurm_test3")
        ids, _, _, queues = s.get_jobs_info()
        wns = s.get_worker_nodes(ids, queues, FakeOptions())
        node = next(w for w in wns if w["domainname"] == nodename)
        assert "3001" in node["core_job_map"].values()


# ---------------------------------------------------------------------------
# Nodelist expansion unit tests
# ---------------------------------------------------------------------------

class TestNodelistExpansion:
    def test_plain_name(self):
        assert SlurmBatchSystem._expand_nodelist("node01") == ["node01"]

    def test_simple_range(self):
        assert SlurmBatchSystem._expand_nodelist("node[01-03]") == ["node01", "node02", "node03"]

    def test_comma_list(self):
        assert SlurmBatchSystem._expand_nodelist("node[01,03,05]") == ["node01", "node03", "node05"]

    def test_mixed_range_and_list(self):
        assert SlurmBatchSystem._expand_nodelist("node[01-02,05]") == ["node01", "node02", "node05"]

    def test_zero_padded(self):
        assert SlurmBatchSystem._expand_nodelist("wn[001-003]") == ["wn001", "wn002", "wn003"]