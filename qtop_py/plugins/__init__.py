## Copyright (c) 2026 qtop contributors
## SPDX-License-Identifier: MIT
"""Scheduler backends: PBS/Torque, SGE, OAR, Slurm, LSF, HTCondor and the demo simulator.

Each module here is a self-standing backend: schedulers.registry finds every GenericBatchSystem
subclass defined in it, with its commands, signature command and state names. Other packages add
backends through the qtop.backends entry-point group.
"""
