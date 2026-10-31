## SPDX-License-Identifier: MIT
"""Check invalid configuration at the boundary, before rendering or commands."""

import copy
from pathlib import Path
from types import SimpleNamespace

import pytest

from qtop_py import configuration, yaml_parser
from qtop_py.configuration import validate_config


@pytest.fixture
def config():
    return yaml_parser.parse(str(Path(__file__).resolve().parents[1] / "qtopconf.yaml"))


@pytest.mark.parametrize(
    "path,value",
    [
        ("savepath", None),
        ("transpose_wn_matrices", "maybe"),
        ("scheduler_command_timeout", "nan"),
        ("scheduler_command_timeout", float("inf")),
        ("scheduler_command_timeout", 0),
        ("scheduler_command_timeout", True),
        ("vertical_separator_every_X_columns", -1),
        ("rem_empty_corelines", 1.5),
        ("possible_ids", []),
        ("possible_ids", ["too long"]),
        ("sorting", []),
        ("sorting.reverse", "maybe"),
        ("sorting.user_sort", "sort by all numbers"),
        ("user_color_mappings", ["Red"]),
        ("user_display_parts", ["unknown"]),
        ("workernodes_matrix", []),
        ("workernodes_matrix", [{"wn id lines": []}]),
        ("term_size", [0, 80]),
        ("term_size", [25]),
        ("extract_info.field_to_use", 1),
        ("extract_info.regex", []),
        ("schedulers", {"custom": {"native_file": "no delimiter"}}),
        ("signature_commands", {"custom": None}),
    ],
)
def test_invalid_config_identifies_key_before_side_effects(config, path, value, monkeypatch):
    parent = config
    keys = path.split(".")
    for key in keys[:-1]:
        parent = parent[key]
    parent[keys[-1]] = value
    monkeypatch.setattr(configuration.fileutils, "private_save_directory", lambda *args: pytest.fail("Invalid config must not create directories"))
    with pytest.raises(ValueError, match=path.split(".")[0]):
        configuration.prepare_config(config)


def test_validation_accepts_defaults_normalized_values_and_extensions(config):
    config["custom_backend_settings"] = {"new_field": "future value"}
    validate_config(config)
    validate_config(configuration.normalize_config(config))
    assert config["transpose_wn_matrices"] == "True"


@pytest.mark.parametrize("value", ["0.25", 0.25, 30])
def test_positive_finite_timeouts_are_valid(config, value):
    config["scheduler_command_timeout"] = value
    validate_config(config)


@pytest.mark.parametrize("sorting", [{}, {"reverse": False}, {"user_sort": None}, {"user_sort": []}])
def test_sorting_can_be_disabled(config, sorting):
    config["sorting"] = sorting
    validate_config(config)


@pytest.mark.parametrize("custom_location", [None, "cwd", "user"])
def test_loading_preserves_precedence_and_applies_cli_once(tmp_path, monkeypatch, custom_location):
    root = Path(__file__).resolve().parents[1]
    defaults, system, user, initial_cwd = (tmp_path / name for name in ("defaults", "system", "user", "cwd"))
    for directory in (defaults, system, user, initial_cwd):
        directory.mkdir()
    (defaults / "qtopconf.yaml").write_text((root / "qtopconf.yaml").read_text())
    (system / "qtopconf.yaml").write_text("marker: system\nsystem_only: kept\n")
    (user / "qtopconf.yaml").write_text("marker: user\n")
    if custom_location:
        (initial_cwd / "custom.yaml").write_text("marker: custom-cwd\n")
        if custom_location == "user":
            (user / "custom.yaml").write_text("marker: custom-user\n")
    output = str(tmp_path / "cli-output")
    calls = []
    monkeypatch.setattr(configuration.fileutils, "private_save_directory", lambda path: calls.append(path) or path)
    options = SimpleNamespace(CONFFILE="custom.yaml" if custom_location else None, OPTION=["savepath=" + output], TRANSPOSE=True, REM_EMPTY_CORELINES=2)
    # Watch mode can reload after init_dirs has changed cwd to the trace directory.
    monkeypatch.chdir(system)
    config, _, _ = configuration.load_yaml_config(options, str(defaults), str(initial_cwd), str(system), str(user))
    assert config["marker"] == ("custom-" + custom_location if custom_location else "user")
    assert config["system_only"] == "kept"
    assert config["transpose_wn_matrices"] is False
    assert config["rem_empty_corelines"] == 2
    assert config["savepath"] == output
    assert calls == [output]
    options.OPTION.append("marker=cli")
    second, _, _ = configuration.load_yaml_config(options, str(defaults), str(initial_cwd), str(system), str(user))
    assert second["marker"] == "cli"
    assert second["rem_empty_corelines"] == 2
    assert second["transpose_wn_matrices"] is False


def test_snapshot_configuration_is_isolated_from_file_layers_and_mutation(config, monkeypatch):
    original = copy.deepcopy(config)
    monkeypatch.setattr(configuration.yaml, "parse", lambda path: pytest.fail("Snapshot replay must not read local configuration"))
    monkeypatch.setattr(configuration.fileutils, "private_save_directory", lambda path: path)
    options = SimpleNamespace(load_sample="support.tar", sample_snapshot={"config": config}, OPTION=["scheduler_command_timeout=4"], TRANSPOSE=False)
    prepared, colors, _ = configuration.load_yaml_config(options, "unused", "unused", "unused", "unused")
    assert prepared["scheduler_command_timeout"] == 4
    prepared["sorting"]["user_sort"].append("sort by node state")
    colors["custom"] = "Red"
    assert config == original


def test_cli_validation_precedes_output_directory_creation(config, monkeypatch):
    monkeypatch.setattr(configuration.fileutils, "private_save_directory", lambda path: pytest.fail("Invalid overrides must not create directories"))
    with pytest.raises(ValueError, match="term_size"):
        configuration.prepare_config(config, SimpleNamespace(OPTION=["term_size=[0, 80]"]))


def test_missing_custom_config_names_both_search_locations(tmp_path):
    root = Path(__file__).resolve().parents[1]
    with pytest.raises(configuration.OperationalError) as error:
        configuration.load_yaml_config(SimpleNamespace(CONFFILE="missing.yaml"), str(root), str(tmp_path), str(tmp_path / "etc"), str(tmp_path / "user"))
    assert "missing.yaml" in str(error.value)
    assert str(tmp_path / "user") in str(error.value)
    assert str(tmp_path) in str(error.value)
