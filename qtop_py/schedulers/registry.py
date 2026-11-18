## Copyright (c) 2026 qtop contributors
## SPDX-License-Identifier: MIT
"""The available scheduler backends, by name."""

import logging
import os
import shutil
from typing import Any, Dict, Mapping, Optional

from qtop_py.constants import QTOP_LOGFILE
from qtop_py.errors import InvalidScheduler, NoSchedulerFound, OperationalError, SchedulerNotSpecified
from qtop_py.plugins.demo import DemoBatchSystem
from qtop_py.plugins.oar import OARBatchSystem
from qtop_py.plugins.pbs import PBSBatchSystem
from qtop_py.plugins.sge import SGEBatchSystem
from qtop_py.plugins.slurm import SlurmBatchSystem
from qtop_py.plugins.slurm_json import SlurmJSONBatchSystem
from qtop_py.serialiser import GenericBatchSystem

PLUGIN_BATCH_SYSTEMS = (DemoBatchSystem, OARBatchSystem, PBSBatchSystem, SGEBatchSystem, SlurmBatchSystem, SlurmJSONBatchSystem)


def auto_get_avail_batch_system(config: Dict[str, Any], backends: Mapping[str, Any]) -> str:
    """Return the first scheduler whose commands exist on this system, for scheduler auto."""
    signature_commands: Dict[str, str] = config["signature_commands"]
    prioritized_commands = sorted(signature_commands.items(), key=lambda item: backends.get(item[0], GenericBatchSystem).detection_priority)
    for system, batch_command in prioritized_commands:
        if system not in backends or backends[system].is_simulated or not shutil.which(batch_command):
            continue
        logging.debug("Auto-detected scheduler: %s", system)
        return system

    raise SchedulerNotSpecified


def decide_batch_system(
    cmdline_switch: Optional[str],
    env_var: Optional[str],
    config_file_batch_option: str,
    schedulers: Mapping[str, Any],
    available_batch_systems: Mapping[str, Any],
    config: Dict[str, Any],
) -> str:
    """Return the scheduler from -b, QTOP_SCHEDULER or QTOPCONF_YAML, in that order.

    With auto, guess it from the scheduler commands available on this system.
    """
    avail_systems = list(available_batch_systems.keys()) + ["auto"]
    if cmdline_switch and cmdline_switch.lower() not in avail_systems:
        logging.critical("Selected scheduler system not supported. Available choices are %s." % ", ".join(avail_systems))
        logging.critical("For help, try ./qtop.py --help")
        logging.critical("Log file created in %s" % os.path.expandvars(QTOP_LOGFILE))
        raise InvalidScheduler
    for scheduler in (cmdline_switch, env_var, config_file_batch_option):
        if scheduler is None:
            continue
        scheduler = scheduler.lower()

        if scheduler == "auto":
            scheduler = auto_get_avail_batch_system(config, available_batch_systems)
            logging.debug("Selected scheduler is %s" % scheduler)
            return scheduler
        if scheduler in schedulers:
            logging.info("User-selected scheduler: %s" % scheduler)
            return scheduler
        if scheduler and scheduler not in schedulers:  # a scheduler that does not exist is inputted
            raise NoSchedulerFound
    raise NoSchedulerFound


def discover_qtop_batch_systems() -> Dict[str, Any]:
    """Return the scheduler backends by name; raise OperationalError on an empty or duplicate name."""
    available_batch_systems = {}
    for batch_system in PLUGIN_BATCH_SYSTEMS:
        mnemonic = batch_system.get_mnemonic()
        if not mnemonic:
            raise OperationalError("Scheduler plugin has an empty mnemonic.")
        if mnemonic in available_batch_systems:
            raise OperationalError("Duplicate scheduler mnemonic: %s" % mnemonic)
        available_batch_systems[mnemonic] = batch_system

    return available_batch_systems
