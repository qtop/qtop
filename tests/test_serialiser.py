##
## qtop is a tool to monitor queuing systems - https://github.com/qtop/qtop
##
## Copyright (c) 2026 Jacob Hatchett
##
## SPDX-License-Identifier: MIT
##

"""Tests for qtop_py.serialiser StatExtractor anonymization.

Covers both anonymize_func and eponymize_func paths in StatExtractor.
"""

import pytest

from qtop_py.serialiser import StatExtractor, GenericBatchSystem


class MockOptions:
    """Minimal options mock matching what StatExtractor expects."""

    def __init__(self, anonymize=False):
        self.ANONYMIZE = anonymize


class MockConfig:
    """Minimal config mock."""
    pass


class TestStatExtractorAnonymize:
    """Tests for StatExtractor anonymization functionality."""

    def test_anonymize_returns_callable(self):
        extractor = StatExtractor(MockConfig(), MockOptions(anonymize=True))
        assert callable(extractor.anonymize)

    def test_anonymize_replaces_username(self):
        extractor = StatExtractor(MockConfig(), MockOptions(anonymize=True))
        result = extractor.anonymize("alice", "users")
        assert result != "alice"
        assert "_anon_user_" in result

    def test_anonymize_same_input_same_output(self):
        extractor = StatExtractor(MockConfig(), MockOptions(anonymize=True))
        first = extractor.anonymize("alice", "users")
        second = extractor.anonymize("alice", "users")
        assert first == second

    def test_anonymize_different_inputs_different_outputs(self):
        extractor = StatExtractor(MockConfig(), MockOptions(anonymize=True))
        result_alice = extractor.anonymize("alice", "users")
        result_bob = extractor.anonymize("bob", "users")
        assert result_alice != result_bob

    def test_anonymize_worker_node(self):
        extractor = StatExtractor(MockConfig(), MockOptions(anonymize=True))
        result = extractor.anonymize("node001", "wns")
        assert "_anon_wn_" in result

    def test_anonymize_queue(self):
        extractor = StatExtractor(MockConfig(), MockOptions(anonymize=True))
        result = extractor.anonymize("batch", "qs")
        assert "_anon_q_" in result

    def test_anonymize_job_number(self):
        extractor = StatExtractor(MockConfig(), MockOptions(anonymize=True))
        result = extractor.anonymize("12345", "jobnums")
        assert "_anon_jn_" in result


class TestStatExtractorEponymize:
    """Tests for StatExtractor passthrough (non-anonymized) mode."""

    def test_eponymize_returns_same_string(self):
        extractor = StatExtractor(MockConfig(), MockOptions(anonymize=False))
        assert extractor.anonymize("alice", "users") == "alice"

    def test_eponymize_preserves_any_type(self):
        extractor = StatExtractor(MockConfig(), MockOptions(anonymize=False))
        assert extractor.anonymize("node001", "wns") == "node001"
        assert extractor.anonymize("batch", "qs") == "batch"


class TestGenericBatchSystem:
    """Tests that GenericBatchSystem raises NotImplementedError for abstract methods."""

    def test_get_queues_info_raises(self):
        bs = GenericBatchSystem()
        with pytest.raises(NotImplementedError):
            bs.get_queues_info()

    def test_get_worker_nodes_raises(self):
        bs = GenericBatchSystem()
        with pytest.raises(NotImplementedError):
            bs.get_worker_nodes([], [], None)

    def test_get_jobs_info_raises(self):
        bs = GenericBatchSystem()
        with pytest.raises(NotImplementedError):
            bs.get_jobs_info([])

    def test_get_mnemonic_raises(self):
        with pytest.raises(NotImplementedError):
            GenericBatchSystem.get_mnemonic()

    def test_ensure_worker_nodes_empty_returns_empty(self):
        result = GenericBatchSystem.ensure_worker_nodes_have_qnames([], [], [])
        assert result == []

    def test_ensure_worker_nodes_assigns_qnames(self):
        worker_nodes = [
            {"core_job_map": {"0": "job1", "1": "job2"}, "qname": []},
        ]
        job_ids = ["job1", "job2"]
        job_queues = ["batch", "batch"]
        result = GenericBatchSystem.ensure_worker_nodes_have_qnames(worker_nodes, job_ids, job_queues)
        assert len(result) == 1
        assert "batch" in result[0]["qname"]
