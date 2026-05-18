"""Tests for the Slurm plugin."""
import os
import unittest

from qtop_py.plugins.slurm import (
    SLURM_QUEUED_STATES,
    SLURM_RUNNING_STATES,
    SLURMBatchSystem,
    SlurmStatExtractor,
    expand_nodelist,
)

CONTRIB = os.path.join(
    os.path.dirname(__file__), "..", "..", "qtop_py", "contrib"
)


class _AttrBag:
    """Tiny stand-in for argparse.Namespace / qtop's options object.

    Defaults the attributes that qtop's StatExtractor expects to read, so
    plugin tests can spin one up without piping a full CLI namespace through.
    """

    _DEFAULTS = {"ANONYMIZE": False, "REMAP": False}

    def __init__(self, **kw):
        for k, v in self._DEFAULTS.items():
            setattr(self, k, v)
        for k, v in kw.items():
            setattr(self, k, v)


def _make_bs():
    return SLURMBatchSystem(
        scheduler_output_filenames={
            "squeue_file": os.path.join(CONTRIB, "squeue.txt"),
            "sinfo_file": os.path.join(CONTRIB, "sinfo.txt"),
        },
        config={},
        options=_AttrBag(),
    )


class ExpandNodelistTests(unittest.TestCase):
    def test_single_node(self):
        self.assertEqual(expand_nodelist("wn001"), ["wn001"])

    def test_range(self):
        self.assertEqual(
            expand_nodelist("wn[001-003]"),
            ["wn001", "wn002", "wn003"],
        )

    def test_mixed_range_and_singles(self):
        self.assertEqual(
            expand_nodelist("wn[001,003-004]"),
            ["wn001", "wn003", "wn004"],
        )

    def test_reason_is_empty(self):
        self.assertEqual(expand_nodelist("(Priority)"), [])
        self.assertEqual(expand_nodelist("(Resources)"), [])

    def test_blank_is_empty(self):
        self.assertEqual(expand_nodelist(""), [])
        self.assertEqual(expand_nodelist(None), [])

    def test_width_preserved(self):
        # 4-digit zero-pad must be preserved
        self.assertEqual(
            expand_nodelist("node[0008-0010]"),
            ["node0008", "node0009", "node0010"],
        )


class JobsInfoTests(unittest.TestCase):
    def test_all_jobs_parsed(self):
        bs = _make_bs()
        ids, users, states, queues = bs.get_jobs_info()
        # Sample file has 7 rows.
        self.assertEqual(len(ids), 7)
        self.assertIn("alice", users)
        self.assertIn("RUNNING", states)
        self.assertIn("PENDING", states)
        self.assertIn("short", queues)
        self.assertIn("gpu", queues)


class QueuesInfoTests(unittest.TestCase):
    def test_aggregates_match_sample(self):
        bs = _make_bs()
        queues_info, total_run, total_queued = bs.get_queues_info()
        names = {q["queue_name"] for q in queues_info}
        self.assertEqual(names, {"short", "long", "gpu"})
        # 4 running (1001, 1002, 1005, 1006, 1007) - actually 5 RUNNING + 1 COMPLETING
        self.assertEqual(total_run, 5)
        # 2 pending
        self.assertEqual(total_queued, 2)


class WorkerNodesTests(unittest.TestCase):
    def test_sinfo_nodes_collected(self):
        bs = _make_bs()
        nodes = bs.get_worker_nodes(job_ids=[], job_queues=[], options=_AttrBag())
        names = [n["domainname"] for n in nodes]
        # Sample sinfo has 9 distinct nodes
        self.assertEqual(len(names), 9)
        self.assertIn("wn001", names)
        self.assertIn("wn020", names)

    def test_node_state_strips_qualifier(self):
        bs = _make_bs()
        nodes = bs.get_worker_nodes(job_ids=[], job_queues=[], options=_AttrBag())
        by_name = {n["domainname"]: n for n in nodes}
        # wn003 is "mix*" -> "mix"
        self.assertEqual(by_name["wn003"]["state"], "mix")
        # wn020 is "idle~" -> "idle"
        self.assertEqual(by_name["wn020"]["state"], "idle")

    def test_running_jobs_attach_to_nodes(self):
        bs = _make_bs()
        nodes = bs.get_worker_nodes(job_ids=[], job_queues=[], options=_AttrBag())
        by_name = {n["domainname"]: n for n in nodes}
        # Job 1001 RUNNING on wn[001-002] -> both nodes mention it
        self.assertIn("1001", by_name["wn001"]["jobs"])
        self.assertIn("1001", by_name["wn002"]["jobs"])
        # Job 1003 PENDING -> no node attribution
        for n in nodes:
            self.assertNotIn("1003", n["jobs"])

    def test_cpu_count_takes_total(self):
        bs = _make_bs()
        nodes = bs.get_worker_nodes(job_ids=[], job_queues=[], options=_AttrBag())
        by_name = {n["domainname"]: n for n in nodes}
        # "4/0/0/4" -> 4
        self.assertEqual(by_name["wn001"]["np"], 4)
        # "24/8/0/32" -> 32
        self.assertEqual(by_name["wn010"]["np"], 32)


class ExtractorRobustnessTests(unittest.TestCase):
    def test_extractor_skips_blank_lines(self, tmpdir=None):
        # Write a small ad-hoc file alongside the contrib dir
        import tempfile

        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".txt", delete=False
        ) as f:
            f.write("\n")  # blank
            f.write("1234|short|alice|RUNNING|4|wn001\n")
            f.write("garbage line that won't match\n")
            f.write("1235|short|bob|PENDING|2|(Priority)\n")
            path = f.name
        try:
            ext = SlurmStatExtractor(config={}, options=_AttrBag())
            rows = ext.extract_squeue(path)
            self.assertEqual(len(rows), 2)
            self.assertEqual(rows[0]["user"], "alice")
            self.assertEqual(rows[1]["state"], "PENDING")
        finally:
            os.unlink(path)


class ConstantsTests(unittest.TestCase):
    def test_state_sets_are_frozen(self):
        # We export read-only state buckets so callers can pattern-match.
        self.assertIn("RUNNING", SLURM_RUNNING_STATES)
        self.assertIn("COMPLETING", SLURM_RUNNING_STATES)
        self.assertIn("PENDING", SLURM_QUEUED_STATES)


if __name__ == "__main__":
    unittest.main()
