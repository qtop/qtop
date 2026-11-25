## Copyright (c) 2026 qtop contributors
## SPDX-License-Identifier: MIT
"""The available scheduler backends, by name."""

import importlib
import logging
import pkgutil
import shutil
from typing import TYPE_CHECKING, Any, Dict, List, Mapping, Optional, Type

from qtop_py import plugins
from qtop_py.core_errors import InvalidScheduler, NoSchedulerFound, OperationalError, SchedulerNotSpecified
from qtop_py.core_serialiser import GenericBatchSystem

if TYPE_CHECKING:  # typing.Final is Python 3.8+; the quoted annotations keep 3.6 working
    from typing import Final

logger = logging.getLogger(__name__)

ENTRY_POINT_GROUP: "Final" = "qtop.backends"


def _defined_in(module: Any) -> "List[Type[GenericBatchSystem]]":
    return [value for value in vars(module).values() if isinstance(value, type) and issubclass(value, GenericBatchSystem) and value.__module__ == module.__name__]


def _builtin_backends() -> "List[Type[GenericBatchSystem]]":
    """Every backend class defined in a qtop_py/plugins module: adding the module adds the backend."""
    names = sorted(info.name for info in pkgutil.iter_modules(plugins.__path__))
    return [backend for name in names for backend in _defined_in(importlib.import_module(plugins.__name__ + "." + name))]


def _installed_backends() -> "List[Type[GenericBatchSystem]]":
    """Backends other packages register in the qtop.backends entry-point group (Python 3.8+)."""
    try:
        from importlib.metadata import entry_points
    except ImportError:  # Python 3.6/3.7: built-in backends only
        return []
    found: Any = entry_points()
    group = found.select(group=ENTRY_POINT_GROUP) if hasattr(found, "select") else found.get(ENTRY_POINT_GROUP, ())
    backends = []
    for entry in group:
        try:
            backends.append(entry.load())
        except Exception as error:  # noqa: BLE001 - a broken third-party backend must not stop qtop
            logger.warning("Skipping scheduler backend %s: %s", entry.name, error)
    return backends


PLUGIN_BATCH_SYSTEMS: "Final" = tuple(_builtin_backends() + _installed_backends())


def backend_defaults(backends: Mapping[str, Any]) -> Dict[str, Dict[str, Any]]:
    """Return the configuration sections each backend declares on its own class (not inherited)."""
    defaults: Dict[str, Dict[str, Any]] = {"schedulers": {}, "signature_commands": {}, "state_abbreviations": {}}
    for name, backend in backends.items():
        own = vars(backend)
        if own.get("default_commands"):
            defaults["schedulers"][name] = dict(own["default_commands"])
        if own.get("signature_command"):
            defaults["signature_commands"][name] = own["signature_command"]
        if own.get("state_abbreviations"):
            defaults["state_abbreviations"][name] = dict(own["state_abbreviations"])
    return defaults


def auto_get_avail_batch_system(config: Dict[str, Any], backends: Mapping[str, Any]) -> str:
    """Return the first scheduler whose commands exist on this system, for scheduler auto."""
    signature_commands: Dict[str, str] = config["signature_commands"]
    prioritized_commands = sorted(signature_commands.items(), key=lambda item: backends.get(item[0], GenericBatchSystem).detection_priority)
    for system, batch_command in prioritized_commands:
        if system not in backends or backends[system].is_simulated or not shutil.which(batch_command):
            continue
        logger.debug("Auto-detected scheduler: %s", system)
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
        raise InvalidScheduler(cmdline_switch, avail_systems)
    for scheduler in (cmdline_switch, env_var, config_file_batch_option):
        if scheduler is None:
            continue
        scheduler = scheduler.lower()

        if scheduler == "auto":
            scheduler = auto_get_avail_batch_system(config, available_batch_systems)
            logger.debug("Selected scheduler is %s", scheduler)
            return scheduler
        if scheduler in schedulers:
            logger.info("User-selected scheduler: %s", scheduler)
            return scheduler
        if scheduler and scheduler not in schedulers:  # a scheduler that does not exist is inputted
            raise NoSchedulerFound(scheduler)
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
