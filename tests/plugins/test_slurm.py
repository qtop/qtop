##
## qtop is a tool to monitor queuing systems - https://github.com/qtop/qtop
##
## SPDX-License-Identifier: MIT
##

from qtop_py.plugins import slurm


def test_parse_key_values():
    line = "NodeName=node001 CPUTot=64 CPUAlloc=2 State=MIXED"
    assert slurm.SlurmStatExtractor.parse_key_values(line) == {
        "NodeName": "node001",
        "CPUTot": "64",
        "CPUAlloc": "2",
        "State": "MIXED",
    }


def test_normalize_node_state():
    assert slurm.SlurmStatExtractor.normalize_node_state("IDLE") == "-"
    assert slurm.SlurmStatExtractor.normalize_node_state("MIXED") == "b"
    assert slurm.SlurmStatExtractor.normalize_node_state("ALLOCATED+") == "b"
    assert slurm.SlurmStatExtractor.normalize_node_state("DOWN*") == "d"


def test_expand_nodelist():
    assert slurm.SlurmStatExtractor.expand_nodelist("node001") == ["node001"]
    assert slurm.SlurmStatExtractor.expand_nodelist("node[001-003]") == ["node001", "node002", "node003"]
    assert slurm.SlurmStatExtractor.expand_nodelist("node[001,003-004]") == ["node001", "node003", "node004"]
    assert slurm.SlurmStatExtractor.expand_nodelist("(Priority)") == []
