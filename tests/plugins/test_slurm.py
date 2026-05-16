##
## qtop is a tool to monitor queuing systems - https://github.com/qtop/qtop
##
## Copyright (c) 2016 Fotis Georgatos
## Copyright (c) 2016 Sotiris Fragkiskos
## Copyright (c) 2023 Hewlett Packard Enterprise Development LP
##
## SPDX-License-Identifier: MIT
##

import pytest
import os
import tempfile
from collections import namedtuple

from qtop_py.plugins import slurm


def make_options(ANONYMIZE=False):
    """Create a mock options object."""
    opts = namedtuple("Options", ["ANONYMIZE"])
    return opts(ANONYMIZE=ANONYMIZE)


def make_config():
    """Create a minimal config dict."""
    return {}


class TestSlurmStatExtractor:
    """Tests for SlurmStatExtractor class."""

    def test_extract_squeue_basic(self):
        """Test basic squeue file parsing."""
        config = make_config()
        options = make_options(ANONYMIZE=False)
        extractor = slurm.SlurmStatExtractor(config, options)

        with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as f:
            f.write("""             JOBID PARTITION     NAME     USER STATE       TIME TIME_LIMIT  NODES NODELIST(REASON)
               12345      batch    python  alice      R       1:00      2:00:00      1     node001
               12346      batch    mpi_job    bob      R       0:30      4:00:00      2     node002,node003
               12347      debug  shortjob    charlie   PD       0:00     00:30:00      1 (Resources)
""")
            fname = f.name

        try:
            result = extractor.extract_squeue(fname)
            assert len(result) == 3
            assert result[0]["JobId"] == "12345"
            assert result[0]["UnixAccount"] == "alice"
            assert result[0]["S"] == "R"
            assert result[1]["JobId"] == "12346"
            assert result[1]["S"] == "R"
            assert result[2]["JobId"] == "12347"
            assert result[2]["S"] == "Q"  # PD maps to Q
        finally:
            os.unlink(fname)

    def test_extract_squeue_anonymized(self):
        """Test that anonymization works."""
        config = make_config()
        options = make_options(ANONYMIZE=True)
        extractor = slurm.SlurmStatExtractor(config, options)

        with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as f:
            f.write("""             JOBID PARTITION     NAME     USER STATE       TIME TIME_LIMIT  NODES NODELIST(REASON)
               12345      batch    python  alice      R       1:00      2:00:00      1     node001
""")
            fname = f.name

        try:
            result = extractor.extract_squeue(fname)
            assert len(result) == 1
            # anonymized user should start with underscore or mapped name
            assert result[0]["UnixAccount"] != "alice"
            assert result[0]["Queue"] != "batch"
        finally:
            os.unlink(fname)

    def test_state_mapping(self):
        """Test that Slurm state codes are correctly mapped."""
        config = make_config()
        options = make_options(ANONYMIZE=False)
        extractor = slurm.SlurmStatExtractor(config, options)

        # Test various state mappings
        assert extractor._map_state("PD") == "Q"
        assert extractor._map_state("R") == "R"
        assert extractor._map_state("CG") == "E"
        assert extractor._map_state("CD") == "E"
        assert extractor._map_state("F") == "F"
        assert extractor._map_state("CA") == "C"
        assert extractor._map_state("S") == "S"
        assert extractor._map_state("PR") == "S"
        assert extractor._map_state("TO") == "E"
        assert extractor._map_state("NF") == "F"
        assert extractor._map_state("DL") == "E"
        assert extractor._map_state("???") == "?"


class TestSlurmBatchSystem:
    """Tests for SlurmBatchSystem class."""

    def test_get_mnemonic(self):
        """Test that mnemonic is 'slurm'."""
        assert slurm.SlurmBatchSystem.get_mnemonic() == "slurm"

    def test_get_jobs_info(self):
        """Test job info extraction from squeue."""
        config = make_config()
        options = make_options(ANONYMIZE=False)

        with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as f:
            f.write("""             JOBID PARTITION     NAME     USER STATE       TIME TIME_LIMIT  NODES NODELIST(REASON)
               12345      batch    python  alice      R       1:00      2:00:00      1     node001
               12346      batch    mpi_job    bob      R       0:30      4:00:00      2     node002,node003
               12347      debug  shortjob    charlie   PD       0:00     00:30:00      1 (Resources)
""")
            squeue_fname = f.name

        scheduler_output_filenames = {
            "squeue_file": squeue_fname,
            "scontrol_file": "/dev/null",
            "sinfo_file": "/dev/null",
        }

        try:
            batch_system = slurm.SlurmBatchSystem(scheduler_output_filenames, config, options)
            job_ids, usernames, job_states, queue_names = batch_system.get_jobs_info()

            assert len(job_ids) == 3
            assert "12345" in job_ids
            assert "12346" in job_ids
            assert "12347" in job_ids
            assert "alice" in usernames
            assert "bob" in usernames
            assert "charlie" in usernames
            # Check states are correctly mapped
            assert "R" in job_states
            assert "Q" in job_states  # PD -> Q
        finally:
            os.unlink(squeue_fname)

    def test_worker_node_parsing(self):
        """Test parsing of scontrol show node output."""
        config = make_config()
        options = make_options(ANONYMIZE=False)

        with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as f:
            f.write("""NodeName=node001 State=ALLOCATED CPUs=16 CoresPerSocket=8 SocketsPerBoard=2 ThreadsPerCore=2 RealMemory=64000
   Jobs=(12345) NumNodes=1 NumCPUs=4 Nodelist=node001
   Partitions=batch
NodeName=node002 State=IDLE CPUs=16 CoresPerSocket=8 SocketsPerBoard=2 ThreadsPerCore=2 RealMemory=64000
   Partitions=batch
NodeName=node003 State=DOWN CPUs=16 CoresPerSocket=8 SocketsPerBoard=2 ThreadsPerCore=2 RealMemory=64000
   Partitions=batch
""")
            scontrol_fname = f.name

        scheduler_output_filenames = {
            "squeue_file": "/dev/null",
            "scontrol_file": scontrol_fname,
            "sinfo_file": "/dev/null",
        }

        try:
            batch_system = slurm.SlurmBatchSystem(scheduler_output_filenames, config, options)
            worker_nodes = batch_system.get_worker_nodes([], [], options)

            assert len(worker_nodes) == 3

            # Check node001 (ALLOCATED -> busy)
            node001 = next(n for n in worker_nodes if n["domainname"] == "node001")
            assert node001["state"] == "b"
            assert node001["np"] == 16

            # Check node002 (IDLE -> free)
            node002 = next(n for n in worker_nodes if n["domainname"] == "node002")
            assert node002["state"] == "-"

            # Check node003 (DOWN -> down)
            node003 = next(n for n in worker_nodes if n["domainname"] == "node003")
            assert node003["state"] == "d"
        finally:
            os.unlink(scontrol_fname)

    def test_get_queues_info(self):
        """Test queue info extraction from sinfo."""
        config = make_config()
        options = make_options(ANONYMIZE=False)

        with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as f:
            f.write("""PARTITION       AVAIL  TIMELIMIT  NODES  NODES(A/I/O/T)  NODELIST
batch*          up     2-00:00:0      4          4/0/0/4    node[001-004]
debug           up     01:00:00       2          2/0/0/2    node[005-006]
""")
            sinfo_fname = f.name

        with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as f:
            f.write("""             JOBID PARTITION     NAME     USER STATE       TIME TIME_LIMIT  NODES NODELIST(REASON)
               12345      batch    python  alice      R       1:00      2:00:00      1     node001
               12346      batch    mpi_job    bob      R       0:30      4:00:00      2     node002,node003
               12347      debug  shortjob    charlie   PD       0:00     00:30:00      1 (Resources)
""")
            squeue_fname = f.name

        scheduler_output_filenames = {
            "squeue_file": squeue_fname,
            "scontrol_file": "/dev/null",
            "sinfo_file": sinfo_fname,
        }

        try:
            batch_system = slurm.SlurmBatchSystem(scheduler_output_filenames, config, options)
            total_running, total_queued, qstatq_list = batch_system.get_queues_info()

            assert total_running == 2  # 2 running jobs (R state)
            assert total_queued == 1   # 1 pending job (PD -> Q state)
            assert len(qstatq_list) == 2  # 2 partitions
        finally:
            os.unlink(sinfo_fname)
            os.unlink(squeue_fname)

    def test_anonymized_worker_nodes(self):
        """Test that worker node names are anonymized when ANONYMIZE=True."""
        config = make_config()
        options = make_options(ANONYMIZE=True)

        with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as f:
            f.write("""NodeName=compute-node-001 State=IDLE CPUs=16 CoresPerSocket=8 SocketsPerBoard=2 ThreadsPerCore=2 RealMemory=64000
   Partitions=batch
""")
            scontrol_fname = f.name

        scheduler_output_filenames = {
            "squeue_file": "/dev/null",
            "scontrol_file": scontrol_fname,
            "sinfo_file": "/dev/null",
        }

        try:
            batch_system = slurm.SlurmBatchSystem(scheduler_output_filenames, config, options)
            worker_nodes = batch_system.get_worker_nodes([], [], options)

            assert len(worker_nodes) == 1
            # Name should be anonymized
            assert worker_nodes[0]["domainname"] != "compute-node-001"
            assert worker_nodes[0]["domainname"].startswith("_anon")
        finally:
            os.unlink(scontrol_fname)


class TestSlurmIntegration:
    """Integration tests using test fixture data files."""

    def test_slurm_test_case_1(self):
        """Test with the first set of slurm test data."""
        config = make_config()
        options = make_options(ANONYMIZE=False)

        test_dir = os.path.join(os.path.dirname(__file__), "..", "inputs")
        squeue_file = os.path.join(test_dir, "slurm_squeue_1.txt")
        scontrol_file = os.path.join(test_dir, "slurm_scontrol_1.txt")
        sinfo_file = os.path.join(test_dir, "slurm_sinfo_1.txt")

        if not os.path.exists(squeue_file):
            pytest.skip("Test input files not found")

        scheduler_output_filenames = {
            "squeue_file": squeue_file,
            "scontrol_file": scontrol_file,
            "sinfo_file": sinfo_file,
        }

        batch_system = slurm.SlurmBatchSystem(scheduler_output_filenames, config, options)

        # Test jobs info
        job_ids, usernames, job_states, queue_names = batch_system.get_jobs_info()
        assert len(job_ids) == 4
        assert "12345" in job_ids
        assert "alice" in usernames
        assert "bob" in usernames
        assert "R" in job_states
        assert "Q" in job_states  # PD -> Q
        assert "E" in job_states  # CG -> E

        # Test worker nodes
        worker_nodes = batch_system.get_worker_nodes(job_ids, queue_names, options)
        assert len(worker_nodes) > 0

        # Test queue info
        total_running, total_queued, qstatq_list = batch_system.get_queues_info()
        assert total_running >= 0
        assert total_queued >= 0

    def test_slurm_test_case_2(self):
        """Test with the second set of slurm test data."""
        config = make_config()
        options = make_options(ANONYMIZE=False)

        test_dir = os.path.join(os.path.dirname(__file__), "..", "inputs")
        squeue_file = os.path.join(test_dir, "slurm_squeue_2.txt")
        scontrol_file = os.path.join(test_dir, "slurm_scontrol_2.txt")
        sinfo_file = os.path.join(test_dir, "slurm_sinfo_2.txt")

        if not os.path.exists(squeue_file):
            pytest.skip("Test input files not found")

        scheduler_output_filenames = {
            "squeue_file": squeue_file,
            "scontrol_file": scontrol_file,
            "sinfo_file": sinfo_file,
        }

        batch_system = slurm.SlurmBatchSystem(scheduler_output_filenames, config, options)

        job_ids, usernames, job_states, queue_names = batch_system.get_jobs_info()
        assert len(job_ids) == 8
        assert "99999" in job_ids
        assert "dave" in usernames
        assert "eve" in usernames

    def test_slurm_test_case_3(self):
        """Test with the third set of slurm test data - more complex with GPU nodes."""
        config = make_config()
        options = make_options(ANONYMIZE=False)

        test_dir = os.path.join(os.path.dirname(__file__), "..", "inputs")
        squeue_file = os.path.join(test_dir, "slurm_squeue_3.txt")
        scontrol_file = os.path.join(test_dir, "slurm_scontrol_3.txt")
        sinfo_file = os.path.join(test_dir, "slurm_sinfo_3.txt")

        if not os.path.exists(squeue_file):
            pytest.skip("Test input files not found")

        scheduler_output_filenames = {
            "squeue_file": squeue_file,
            "scontrol_file": scontrol_file,
            "sinfo_file": sinfo_file,
        }

        batch_system = slurm.SlurmBatchSystem(scheduler_output_filenames, config, options)

        job_ids, usernames, job_states, queue_names = batch_system.get_jobs_info()
        assert len(job_ids) == 10
        assert "50001" in job_ids
        assert "alice" in usernames
        assert "bob" in usernames
        assert "charlie" in usernames

        # Check various states
        assert "R" in job_states
        assert "Q" in job_states
        assert "E" in job_states  # CG -> E
        assert "F" in job_states  # F -> F

        # Test worker nodes include GPU nodes
        worker_nodes = batch_system.get_worker_nodes(job_ids, queue_names, options)
        node_names = [n["domainname"] for n in worker_nodes]
        assert "gpu001" in node_names
        assert "gpu003" in node_names  # DOWN node should be included

    def test_slurm_anonymization_integration(self):
        """Test that anonymization works end-to-end with test data."""
        config = make_config()
        options = make_options(ANONYMIZE=True)

        test_dir = os.path.join(os.path.dirname(__file__), "..", "inputs")
        squeue_file = os.path.join(test_dir, "slurm_squeue_1.txt")
        scontrol_file = os.path.join(test_dir, "slurm_scontrol_1.txt")
        sinfo_file = os.path.join(test_dir, "slurm_sinfo_1.txt")

        if not os.path.exists(squeue_file):
            pytest.skip("Test input files not found")

        scheduler_output_filenames = {
            "squeue_file": squeue_file,
            "scontrol_file": scontrol_file,
            "sinfo_file": sinfo_file,
        }

        batch_system = slurm.SlurmBatchSystem(scheduler_output_filenames, config, options)

        job_ids, usernames, job_states, queue_names = batch_system.get_jobs_info()

        # Check that no real user names or queue names appear
        assert "alice" not in usernames
        assert "bob" not in usernames
        assert "batch" not in queue_names
        assert "debug" not in queue_names

        # All names should be anonymized (start with _anon)
        for name in usernames + queue_names:
            assert name.startswith("_anon")
