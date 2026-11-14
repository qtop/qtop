# Copyright (c) 2026 qtop contributors
# SPDX-License-Identifier: MIT
"""Runtime boundary contracts requiring only Python 3.6's standard library."""

import copy
import json
import os
import subprocess
import sys
import tarfile
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from qtop_py import configuration, sample, yaml_parser
from qtop_py.process import communicate_with_timeout
from qtop_py.snapshot import Snapshot

ROOT = Path(__file__).resolve().parents[1]


class LegacyRuntimeContracts(unittest.TestCase):
    def test_configuration_rejects_invalid_timeout_before_execution(self):
        config = yaml_parser.parse(str(ROOT / "qtopconf.yaml"))
        configuration.validate_config(config)
        for value in (0, -1, "nan", "inf", True):
            with self.subTest(value=value):
                invalid = copy.deepcopy(config)
                invalid["scheduler_command_timeout"] = value
                with self.assertRaises(ValueError):
                    configuration.validate_config(invalid)

    def test_timeout_kills_and_reaps_scheduler_client(self):
        process = subprocess.Popen(
            [sys.executable, "-c", "import time; time.sleep(30)"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            start_new_session=os.name == "posix",
        )
        try:
            with self.assertRaises(subprocess.TimeoutExpired):
                communicate_with_timeout(process, 0.05)
            self.assertIsNotNone(process.poll())
            self.assertTrue(process.stdout.closed)
            self.assertTrue(process.stderr.closed)
        finally:
            if process.poll() is None:
                process.kill()
            process.wait()

    def test_successful_collection_drains_both_pipes(self):
        process = subprocess.Popen(
            [sys.executable, "-c", "import sys; sys.stdout.write('frame'); sys.stderr.write('diagnostic')"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            start_new_session=os.name == "posix",
        )
        self.assertEqual(communicate_with_timeout(process, 10), (b"frame", b"diagnostic"))
        self.assertEqual(process.returncode, 0)

    def test_versioned_snapshot_capture_import_and_json_export(self):
        for version in (1, 2, 3):
            with self.subTest(version=version), tempfile.TemporaryDirectory() as directory:
                state = {"jobs": [{"id": "42", "user": "alice", "state": "R", "queue": "debug"}], "nodes": [], "queues": []}
                if version >= 2:
                    state.update(schema_version=version, source={"backend": "slurm", "cluster_id": "cluster"})
                    state["jobs"][0]["qid"] = "slurm:cluster:42"
                if version == 3:
                    state["schema_revision"] = "beta1"
                    state["jobs"][0]["parent_qid"] = "slurm:cluster:unobserved-parent"
                masked, _ = sample.anonymize_state(state)
                archive_path = Path(directory) / "sample.tar"
                with tarfile.open(str(archive_path), "w") as archive:
                    sample.save_snapshot(archive, masked, {"extract_info": None}, SimpleNamespace(ANONYMIZE=True, COLOR="OFF"), "slurm", "test", [])
                restored = sample.load_snapshot(str(archive_path))
                self.assertEqual(restored["state"], masked)
                self.assertFalse(restored["options"]["GET_GECOS"])
                self.assertIsNone(restored["config"]["extract_info"])
                output = Path(directory) / "state.json"
                Snapshot.from_cluster_state(restored["state"]).save(str(output))
                self.assertEqual(json.loads(output.read_text()), masked)
