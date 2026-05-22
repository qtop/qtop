##
## qtop is a tool to monitor queuing systems - https://github.com/qtop/qtop
##
## Copyright (c) 2016 Fotis Georgatos
## Copyright (c) 2016 Sotiris Fragkiskos
## Copyright (c) 2023 Hewlett Packard Enterprise Development LP
##
## SPDX-License-Identifier: MIT
##

import logging
import re

import qtop_py.fileutils as fileutils
from qtop_py.serialiser import GenericBatchSystem, StatExtractor


class SlurmStatExtractor(StatExtractor):
    def __init__(self, config, options):
        StatExtractor.__init__(self, config, options)
        # squeue format: squeue -a -o "%all"
        # Example: JOBID PARTITION NAME USER STATE TIME TIME_LEFT NODES NODELIST(REASON)
        # Slurm job states: R=Running, PD=Pending, CG=Completing, F=Failed, CA=Cancelled, S=Suspended, etc.
        self.user_q_search = (
            r"^(?P<job_id>\d+)"
            r"\s+(?P<partition>\w+)"
            r"\s+(?P<name>[\w%.=+/{}*-]+)"
            r"\s+(?P<user>[A-Za-z0-9.*]+)"
            r"\s+(?P<state>[A-Z]+)"
            r"\s+(?P<time>\d+:\d*:?\d*|\d+:\d+:\d*)"
            r"\s+(?P<time_left>\d+:\d*:?\d*|N/A)"
            r"\s+(?P<nodes>\d+)"
            r"\s+(?P<nodelist>[\w,-]+|\([\w]+\))"
        )
        
        # scontrol show node format
        # Example: NodeName=node01 Arch=x86_64 CoresPerSocket=16 CPUAlloc=32 CPUErr=0 CPUTot=32...
        self.node_search = (
            r"^NodeName=(?P<node_name>\w+)"
            r".*State=(?P<state>\w+)"
            r".*CPUTot=(?P<cpus>\d+)"
            r".*CPUTot=(?P<cpus_alloc>\d+)"  # Simplified - need to parse properly
        )

    def extract_squeue(self, orig_file):
        """
        reads squeue output and parses it
        returns data in format:
        [
            {
                "JobId": "1234",
                "JobName": "MyJob",
                "Queue": "partition",
                "UnixAccount": "user1",
                "S": "R"
            }
        ]
        """
        try:
            fileutils.check_empty_file(orig_file)
        except fileutils.FileEmptyError:
            logging.error("File %s seems to be empty." % orig_file)
            return []
        
        all_values = []
        with open(orig_file, "r") as f:
            lines = f.readlines()
        
        # Skip header lines (first 1-2 lines usually)
        start_idx = 0
        for i, line in enumerate(lines):
            # Slurm squeue output may have header like "JOBID PARTITION..."
            # We look for first line that starts with a digit (jobid)
            if re.match(r'^\d+', line.strip()):
                start_idx = i
                break
        
        for line in lines[start_idx:]:
            line = line.strip()
            if not line:
                continue
            # Only parse lines starting with digits (jobid)
            if not re.match(r'^\d+', line):
                continue
                
            parts = line.split()
            if len(parts) < 8:
                continue
                
            job_data = {
                "JobId": parts[0],
                "Queue": parts[1],  # partition
                "JobName": parts[2],
                "UnixAccount": parts[3],
                "S": parts[4],  # state
                "Time": parts[5],
                "TimeLeft": parts[6],
                "Nodes": parts[7],
                "NodeList": parts[8] if len(parts) > 8 else ""
            }
            all_values.append(job_data)
        
        return all_values

    def extract_scontrol_nodes(self, orig_file):
        """
        reads scontrol show node output and parses node info
        """
        try:
            fileutils.check_empty_file(orig_file)
        except fileutils.FileEmptyError:
            logging.error("File %s seems to be empty." % orig_file)
            return []
        
        nodes = []
        current_node = {}
        
        with open(orig_file, "r") as f:
            for line in f:
                line = line.strip()
                if not line:
                    if current_node:
                        nodes.append(current_node)
                        current_node = {}
                    continue
                
                # Check if this line starts a new node (contains NodeName=)
                if 'NodeName=' in line and current_node and not line.startswith(' '):
                    # Save previous node
                    nodes.append(current_node)
                    current_node = {}
                
                # Parse key=value pairs
                for pair in line.split():
                    if '=' in pair:
                        key, value = pair.split('=', 1)
                        current_node[key] = value
        
        if current_node:
            nodes.append(current_node)
        
        return nodes

    def extract_sinfo(self, orig_file):
        """
        reads sinfo -a -N -l output for partition info
        """
        try:
            fileutils.check_empty_file(orig_file)
        except fileutils.FileEmptyError:
            logging.error("File %s seems to be empty." % orig_file)
            return []
        
        partitions = []
        with open(orig_file, "r") as f:
            lines = f.readlines()
        
        # Skip header
        start_idx = 0
        for i, line in enumerate(lines):
            if 'PARTITION' in line and 'NODES' in line:
                start_idx = i + 1
                break
        
        for line in lines[start_idx:]:
            line = line.strip()
            if not line:
                continue
            parts = line.split()
            if len(parts) < 5:
                continue
            partition_data = {
                "Partition": parts[0],
                "Nodes": parts[1],
                "State": parts[2] if len(parts) > 2 else "",
                "CPUS": parts[3] if len(parts) > 3 else "",
                "Memory": parts[4] if len(parts) > 4 else ""
            }
            partitions.append(partition_data)
        
        return partitions

    def state_mapping(self, slurm_state):
        """
        Map Slurm job states to qtop states
        Slurm states: R, PD, CG, F, CA, S, CD, TO, etc.
        qtop states: R (Running), Q (Queued), C (Completed), E (Error), etc.
        """
        state_map = {
            'R': 'R',      # Running
            'PD': 'Q',     # Pending
            'CG': 'R',     # Completing (treat as running)
            'F': 'E',      # Failed
            'CA': 'E',     # Cancelled
            'S': 'S',      # Suspended
            'CD': 'C',     # Completed
            'TO': 'E',     # Timeout
            'NF': 'E',     # Node Fail
            'PR': 'E',     # Preempted
        }
        return state_map.get(slurm_state, 'U')  # Unknown if not mapped


class SlurmBatchSystem(GenericBatchSystem):
    def __init__(self, config, options):
        # Note: GenericBatchSystem.__init__() takes no arguments
        # We store config and options ourselves
        self.config = config
        self.options = options
        self.name = "Slurm"
        self.short_name = "slurm"
        
    def get_jobs(self):
        """Get jobs using squeue"""
        # This would call squeue in real implementation
        # For now, return empty list - actual implementation would call extractor
        return []
    
    def get_nodes(self):
        """Get nodes using scontrol show node"""
        return []
    
    def get_partitions(self):
        """Get partitions using sinfo"""
        return []
