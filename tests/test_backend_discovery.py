## Copyright (c) 2026 qtop contributors
## SPDX-License-Identifier: MIT
"""Backends are self-standing: a plugin module brings its own commands, signature command and states."""

import logging
import re
from pathlib import Path
from types import SimpleNamespace

from qtop_py import configuration, plugins
from qtop_py import core_yaml_parser as yaml_parser
from qtop_py.schedulers import registry

ROOT = Path(__file__).resolve().parents[1]


def test_every_plugin_module_is_found_with_the_defaults_it_declares():
    backends = registry.discover_qtop_batch_systems()
    assert list(backends) == ["demo", "htcondor", "lsf", "oar", "pbs", "sge", "slurm", "slurm_json"]
    defaults = registry.backend_defaults(backends)
    assert set(defaults["schedulers"]) == set(defaults["signature_commands"]) == set(backends)
    assert "slurm_json" not in defaults["state_abbreviations"]  # declared on SlurmBatchSystem, not inherited


def test_a_module_dropped_into_plugins_registers_itself(tmp_path, monkeypatch):
    (tmp_path / "tiny.py").write_text(
        "from qtop_py.core_api import GenericBatchSystem\n"
        "class TinyBatchSystem(GenericBatchSystem):\n"
        "    signature_command = 'tinystat'\n"
        "    default_commands = {'jobs_file': '%(savepath)s/tiny%(pid)s.txt, tinystat'}\n"
        "    @staticmethod\n"
        "    def get_mnemonic():\n"
        "        return 'tiny'\n"
    )
    monkeypatch.setattr(plugins, "__path__", list(plugins.__path__) + [str(tmp_path)])
    found = [backend.get_mnemonic() for backend in registry._builtin_backends()]
    assert "tiny" in found and "slurm" in found
    tiny = next(backend for backend in registry._builtin_backends() if backend.get_mnemonic() == "tiny")
    assert registry.backend_defaults({"tiny": tiny})["schedulers"] == {"tiny": {"jobs_file": "%(savepath)s/tiny%(pid)s.txt, tinystat"}}


def test_installed_backends_load_and_a_broken_one_is_skipped(monkeypatch, caplog):
    import importlib.metadata

    class Entry:
        def __init__(self, name, target):
            self.name, self.target = name, target

        def load(self):
            if isinstance(self.target, Exception):
                raise self.target
            return self.target

    entries = [Entry("good", registry.GenericBatchSystem), Entry("broken", ImportError("no module named lib"))]
    monkeypatch.setattr(importlib.metadata, "entry_points", lambda: SimpleNamespace(select=lambda group: entries if group == "qtop.backends" else []))
    with caplog.at_level(logging.WARNING):
        assert registry._installed_backends() == [registry.GenericBatchSystem]
    assert "Skipping scheduler backend broken: no module named lib" in caplog.text


def test_a_layer_overrides_one_command_and_keeps_the_rest(tmp_path, monkeypatch):
    """Layers used to replace the whole schedulers block, dropping every other backend's commands."""
    defaults, system, user = (tmp_path / name for name in ("defaults", "system", "user"))
    for directory in (defaults, system, user):
        directory.mkdir()
    (defaults / "qtopconf.yaml").write_text((ROOT / "qtopconf.yaml").read_text())
    (user / "qtopconf.yaml").write_text("schedulers:\n  slurm:\n    sinfo_file: %(savepath)s/sinfo%(pid)s.txt, sinfo -N -h\n\nsignature_commands:\n  lsf: lsid\n")
    monkeypatch.setattr(configuration.fileutils, "private_save_directory", lambda path, strict=False: path)
    options = SimpleNamespace(CONFFILE=None, OPTION=["savepath=" + str(tmp_path / "out")])
    config, _, _ = configuration.load_yaml_config(options, str(defaults), str(tmp_path), str(system), str(user))
    plugin = registry.backend_defaults(registry.discover_qtop_batch_systems())
    assert config["schedulers"]["slurm"]["sinfo_file"].endswith("sinfo -N -h")
    assert config["schedulers"]["slurm"]["squeue_file"] == plugin["schedulers"]["slurm"]["squeue_file"]
    assert config["schedulers"]["pbs"] == plugin["schedulers"]["pbs"]
    assert config["signature_commands"]["lsf"] == "lsid" and config["signature_commands"]["slurm"] == "sinfo"
    assert config["state_abbreviations"] == plugin["state_abbreviations"]


def test_qtopconf_lists_the_plugins_default_commands_verbatim(tmp_path):
    """The commented copy in qtopconf.yaml, for users to read and paste, must match the plugins."""
    block = re.search(r"^# schedulers:\n((?:#   .*\n)+)", (ROOT / "qtopconf.yaml").read_text(), re.M).group(1)
    path = tmp_path / "schedulers.yaml"
    path.write_text("schedulers:\n" + re.sub(r"^# ", "", block, flags=re.M))
    assert yaml_parser.parse(str(path))["schedulers"] == registry.backend_defaults(registry.discover_qtop_batch_systems())["schedulers"]


def test_backend_options_come_from_the_plugins_once():
    """slurm_json inherits Slurm's option hooks; they must register once, or argparse would refuse them."""
    from qtop_py import utils
    from qtop_py.core_api import BackendOptions

    assert [backend.get_mnemonic() for backend in utils.backend_option_hooks()] == ["slurm"]
    options = BackendOptions(SOURCEDIR="/traces", ALL_PARTITIONS=True)
    assert (options.SOURCEDIR, options.ALL_PARTITIONS, getattr(options, "SLURM_SUMMARY", False)) == ("/traces", True, False)


def test_backend_settings_are_checked_by_their_plugin_and_retired_keys_are_ignored():
    config = configuration.with_backend_defaults(yaml_parser.parse(str(ROOT / "qtopconf.yaml")))
    config["faster_xml_parsing"] = True  # removed setting: an old configuration still loads
    configuration.validate_config(config)
    config["sge_whole_queues"] = ["world", " "]
    try:
        configuration.validate_config(config)
    except ValueError as error:
        assert str(error).startswith("sge_whole_queues: expected")
    else:
        raise AssertionError("SGEBatchSystem.check_config must reject a blank queue pattern")
