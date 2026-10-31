## SPDX-License-Identifier: MIT
from qtop_py.plugins.demo import DemoBatchSystem
from qtop_py.plugins.oar import OARBatchSystem
from qtop_py.plugins.pbs import PBSBatchSystem
from qtop_py.plugins.sge import SGEBatchSystem
from qtop_py.plugins.slurm import SlurmBatchSystem

PLUGIN_BATCH_SYSTEMS = (DemoBatchSystem, OARBatchSystem, PBSBatchSystem, SGEBatchSystem, SlurmBatchSystem)
