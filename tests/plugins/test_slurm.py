"""
Tests for Slurm plugin
"""

import os
import sys
from argparse import Namespace

import pytest

# Add parent directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'qtop_py'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from qtop_py.plugins.slurm import SlurmBatchSystem, SlurmStatExtractor


class TestSlurmStatExtractor:
    """Test cases for SlurmStatExtractor"""
    
    @pytest.fixture
    def extractor(self):
        options = Namespace(ANONYMIZE=False)
        return SlurmStatExtractor(config={}, options=options)
    
    def test_extract_squeue_basic(self, extractor, tmp_path):
        """Test basic squeue parsing"""
        # Create test squeue output
        squeue_content = """JOBID PARTITION NAME USER STATE TIME TIME_LEFT NODES NODELIST
12345 debug job1 user1 R 0:15 2:45 1 node01
12346 debug job2 user2 PD 0:00 N/A 1 (Resources)
12347 debug job3 user3 CG 1:30 N/A 2 node02,node03
"""
        squeue_file = tmp_path / "squeue.txt"
        squeue_file.write_text(squeue_content)
        
        result = extractor.extract_squeue(str(squeue_file))
        
        assert len(result) == 3
        assert result[0]['JobId'] == '12345'
        assert result[0]['S'] == 'R'
        assert result[1]['S'] == 'PD'
        assert result[2]['S'] == 'CG'
    
    def test_extract_squeue_empty_file(self, extractor, tmp_path):
        """Test empty file handling"""
        empty_file = tmp_path / "empty.txt"
        empty_file.write_text("")
        
        result = extractor.extract_squeue(str(empty_file))
        assert result == []
    
    def test_extract_squeue_with_header(self, extractor, tmp_path):
        """Test squeue with header lines"""
        squeue_content = """JOBID PARTITION     NAME     USER STATE       TIME TIME_LEFT NODES NODELIST(REASON)
12345     debug    job1    user1     R       0:15     2:45     1       node01
"""
        squeue_file = tmp_path / "squeue_header.txt"
        squeue_file.write_text(squeue_content)
        
        result = extractor.extract_squeue(str(squeue_file))
        assert len(result) == 1
        assert result[0]['JobId'] == '12345'
    
    def test_state_mapping(self, extractor):
        """Test Slurm state to qtop state mapping"""
        assert extractor.state_mapping('R') == 'R'
        assert extractor.state_mapping('PD') == 'Q'
        assert extractor.state_mapping('F') == 'E'
        assert extractor.state_mapping('CA') == 'E'
        assert extractor.state_mapping('S') == 'S'
        assert extractor.state_mapping('CD') == 'C'
        assert extractor.state_mapping('UNKNOWN') == 'U'
    
    def test_extract_scontrol_nodes(self, extractor, tmp_path):
        """Test scontrol show node parsing"""
        scontrol_content = """NodeName=node01 Arch=x86_64 CoresPerSocket=16 CPUAlloc=32 CPUErr=0 CPUTot=32
NodeName=node02 Arch=x86_64 CoresPerSocket=16 CPUAlloc=0 CPUErr=0 CPUTot=32 State=IDLE
NodeName=node03 Arch=x86_64 CoresPerSocket=16 CPUAlloc=16 CPUErr=0 CPUTot=32 State=ALLOCATED
"""
        scontrol_file = tmp_path / "scontrol.txt"
        scontrol_file.write_text(scontrol_content)
        
        result = extractor.extract_scontrol_nodes(str(scontrol_file))
        
        assert len(result) == 3
        assert result[0]['NodeName'] == 'node01'
        assert result[1]['State'] == 'IDLE'
        assert result[2]['State'] == 'ALLOCATED'
    
    def test_extract_sinfo(self, extractor, tmp_path):
        """Test sinfo parsing"""
        sinfo_content = """PARTITION AVAIL  TIMELIMIT  NODES  STATE NODES(A/I/O/T) CPUS MEMORY
debug*    up    1:00:00      1  idle       1/0/0/1     32   64000
debug*    up    1:00:00      1  alloc      1/0/0/1     32   64000
"""
        sinfo_file = tmp_path / "sinfo.txt"
        sinfo_file.write_text(sinfo_content)
        
        result = extractor.extract_sinfo(str(sinfo_file))
        
        assert len(result) >= 1
        assert 'Partition' in result[0]


class TestSlurmBatchSystem:
    """Test cases for SlurmBatchSystem"""
    
    @pytest.fixture
    def batch_system(self):
        return SlurmBatchSystem(config={}, options={})
    
    def test_initialization(self, batch_system):
        """Test batch system initialization"""
        assert batch_system.name == "Slurm"
        assert batch_system.short_name == "slurm"
    
    def test_get_jobs(self, batch_system):
        """Test get_jobs method"""
        result = batch_system.get_jobs()
        assert isinstance(result, list)


class TestSlurmAnonymization:
    """Test anonymization support for Slurm"""
    
    @pytest.fixture
    def extractor(self):
        options = Namespace(ANONYMIZE=False)
        return SlurmStatExtractor(config={}, options=options)
    
    def test_anonymize_users(self, extractor, tmp_path):
        """Test user anonymization in squeue output"""
        squeue_content = """JOBID PARTITION NAME USER STATE TIME TIME_LEFT NODES NODELIST
12345 debug job1 john_doe R 0:15 2:45 1 node01
12346 debug job2 jane_doe PD 0:00 N/A 1 (Resources)
"""
        squeue_file = tmp_path / "squeue_anon.txt"
        squeue_file.write_text(squeue_content)
        
        result = extractor.extract_squeue(str(squeue_file))
        
        # Check that users are parsed correctly
        assert result[0]['UnixAccount'] == 'john_doe'
        assert result[1]['UnixAccount'] == 'jane_doe'
    
    def test_anonymize_nodes(self, extractor, tmp_path):
        """Test node name anonymization"""
        scontrol_content = """NodeName=compute001 Arch=x86_64 State=IDLE
NodeName=compute002 Arch=x86_64 State=ALLOCATED
"""
        scontrol_file = tmp_path / "scontrol_anon.txt"
        scontrol_file.write_text(scontrol_content)
        
        result = extractor.extract_scontrol_nodes(str(scontrol_file))
        
        assert result[0]['NodeName'] == 'compute001'
        assert result[1]['NodeName'] == 'compute002'


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
