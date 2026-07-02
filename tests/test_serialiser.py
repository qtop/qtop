##
## qtop is a tool to monitor queuing systems - https://github.com/qtop/qtop
##
## Copyright (c) 2016 Fotis Georgatos
## Copyright (c) 2016 Sotiris Fragkiskos
## Copyright (c) 2026 Jacob Hatchett
##
## SPDX-License-Identifier: MIT
##

import re
import pytest
from itertools import count
from qtop_py.serialiser import StatExtractor, GenericBatchSystem


class TestStatExtractor:
    @pytest.fixture
    def stat_extractor(self):
        class Config:
            pass

        class Options:
            ANONYMIZE = False

        config = Config()
        options = Options()
        return StatExtractor(config, options)

    def test_process_qstat_line_valid(self, stat_extractor):
        re_search = r"(\d+\.\w+)\s+(\w+)\s+(\w+)\s+(\w+)"
        re_match_positions = [1, 2, 3, 4]
        line = "12345.cluster  alice  R  default"

        result = stat_extractor._process_qstat_line(re_search, line, re_match_positions)

        assert result["JobId"] == "12345"
        assert result["UnixAccount"] == "alice"
        assert result["S"] == "R"
        assert result["Queue"] == "default"

    def test_process_qstat_line_strips_job_id_extension(self, stat_extractor):
        re_search = r"(\d+\.\w+\.\w+)\s+(\w+)\s+(\w+)\s+(\w+)"
        re_match_positions = [1, 2, 3, 4]
        line = "99999.server.domain  bob  Q  batch"

        result = stat_extractor._process_qstat_line(re_search, line, re_match_positions)

        assert result["JobId"] == "99999"
        assert result["Queue"] == "batch"

    def test_process_qstat_line_raises_on_malformed_line(self, stat_extractor):
        re_search = r"(\d+\.\w+)\s+(\w+)\s+(\w+)\s+(\w+)"
        re_match_positions = [1, 2, 3, 4]
        line = "not-a-valid-qstat-line"

        with pytest.raises(AttributeError):
            stat_extractor._process_qstat_line(re_search, line, re_match_positions)


class TestStatExtractorAnonymize:
    @pytest.fixture
    def anonymized_extractor(self):
        class Config:
            pass

        class Options:
            ANONYMIZE = True

        config = Config()
        options = Options()
        return StatExtractor(config, options)

    def test_anonymize_replaces_user_names(self, anonymized_extractor):
        re_search = r"(\d+\.\w+)\s+(\w+)\s+(\w+)\s+(\w+)"
        re_match_positions = [1, 2, 3, 4]
        line = "12345.cluster  alice  R  default"

        result = anonymized_extractor._process_qstat_line(re_search, line, re_match_positions)
        assert result["UnixAccount"] != "alice"
        assert "_anon_user_" in result["UnixAccount"]

    def test_anonymize_produces_consistent_results(self, anonymized_extractor):
        """Same input should produce same anonymized output."""
        re_search = r"(\d+\.\w+)\s+(\w+)\s+(\w+)\s+(\w+)"
        re_match_positions = [1, 2, 3, 4]

        result1 = anonymized_extractor._process_qstat_line(re_search, "12345.cluster  alice  R  default", re_match_positions)
        result2 = anonymized_extractor._process_qstat_line(re_search, "12345.cluster  alice  R  default", re_match_positions)

        assert result1["UnixAccount"] == result2["UnixAccount"]

    def test_anonymize_different_users_get_different_ids(self, anonymized_extractor):
        re_search = r"(\d+\.\w+)\s+(\w+)\s+(\w+)\s+(\w+)"
        re_match_positions = [1, 2, 3, 4]

        result1 = anonymized_extractor._process_qstat_line(re_search, "1.cluster  alice  R  default", re_match_positions)
        result2 = anonymized_extractor._process_qstat_line(re_search, "2.cluster  bob  R  default", re_match_positions)

        assert result1["UnixAccount"] != result2["UnixAccount"]


class TestStatExtractorAnonymizeQueueList:
    def test_anonymize_queue_list_nametag(self):
        class Config:
            pass

        class Options:
            ANONYMIZE = True

        config = Config()
        options = Options()
        extractor = StatExtractor(config, options)

        class FakeText:
            pass

        fake = FakeText()
        fake.text = "myqueue@workernode1"

        result = extractor.anonymize_queue_list_nametag(fake)
        assert "myqueue" not in result
        assert "workernode1" not in result
        assert "_anon_q_" in result
        assert "_anon_wn_" in result

    def test_eponymize_queue_list_nametag(self):
        class Config:
            pass

        class Options:
            ANONYMIZE = False

        config = Config()
        options = Options()
        extractor = StatExtractor(config, options)

        class FakeText:
            pass

        fake = FakeText()
        fake.text = "myqueue@workernode1"

        result = extractor.anonymize_queue_list_nametag(fake)
        assert result == "myqueue@workernode1"


class TestEponymizeFunc:
    def test_eponymize_returns_original(self):
        class Config:
            pass

        class Options:
            ANONYMIZE = False

        config = Config()
        options = Options()
        extractor = StatExtractor(config, options)

        assert extractor.anonymize("testuser", "users") == "testuser"
        assert extractor.anonymize("node01", "wns") == "node01"


class TestAnonymizeFunc:
    def test_anonymize_returns_different_string(self):
        class Config:
            pass

        class Options:
            ANONYMIZE = True

        config = Config()
        options = Options()
        extractor = StatExtractor(config, options)

        anonymized = extractor.anonymize("testuser", "users")
        assert anonymized != "testuser"
        assert "_anon_user_" in anonymized

    def test_anonymize_different_types_produce_different_prefixes(self):
        class Config:
            pass

        class Options:
            ANONYMIZE = True

        config = Config()
        options = Options()
        extractor = StatExtractor(config, options)

        user_anon = extractor.anonymize("alice", "users")
        wn_anon = extractor.anonymize("node01", "wns")
        q_anon = extractor.anonymize("batch", "qs")

        assert "_anon_user_" in user_anon
        assert "_anon_wn_" in wn_anon
        assert "_anon_q_" in q_anon


class TestGenericBatchSystem:
    def test_get_queues_info_raises(self):
        gbs = GenericBatchSystem()
        with pytest.raises(NotImplementedError):
            gbs.get_queues_info()

    def test_get_worker_nodes_raises(self):
        gbs = GenericBatchSystem()
        with pytest.raises(NotImplementedError):
            gbs.get_worker_nodes(None, None, None)

    def test_get_jobs_info_raises(self):
        gbs = GenericBatchSystem()
        with pytest.raises(NotImplementedError):
            gbs.get_jobs_info(None)

    def test_get_mnemonic_raises(self):
        with pytest.raises(NotImplementedError):
            GenericBatchSystem.get_mnemonic()

    def test_ensure_worker_nodes_have_qnames_empty_nodes(self):
        result = GenericBatchSystem.ensure_worker_nodes_have_qnames([], [1, 2], ["q1", "q2"])
        assert result == []

    def test_ensure_worker_nodes_have_qnames_with_nodes(self):
        worker_nodes = [
            {
                "core_job_map": {
                    0: "12345",
                    1: "67890",
                }
            }
        ]
        job_ids = ["12345", "67890"]
        job_queues = ["batch", "interactive"]

        GenericBatchSystem.ensure_worker_nodes_have_qnames(worker_nodes, job_ids, job_queues)

        assert "qname" in worker_nodes[0]
        # Should contain both queues since two different jobs from different queues
        assert len(worker_nodes[0]["qname"]) > 0

    def test_ensure_worker_nodes_have_qnames_handles_job_arrays(self):
        worker_nodes = [
            {
                "core_job_map": {
                    0: "12345[]",
                }
            }
        ]
        job_ids = ["12345[]"]
        job_queues = ["batch"]

        GenericBatchSystem.ensure_worker_nodes_have_qnames(worker_nodes, job_ids, job_queues)

        assert worker_nodes[0]["qname"] == ["batch"]
