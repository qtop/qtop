## Copyright (c) 2026 qtop contributors
## SPDX-License-Identifier: MIT
"""Check invalid configuration at the boundary, before rendering or commands."""

import copy
import os
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
        ("summary_colours", ["NoSuchColour"]),
        ("summary_colours", "BlackOnBlue"),
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
        ("sge_whole_queues", "world"),
        ("sge_whole_queues", [""]),
        ("sge_whole_queues", [1]),
        ("accounts_states", []),
        ("accounts_states", "running_of_user"),
        ("accounts_states", ["runing_of_user"]),
        ("accounts_states", [{"running_of_user": ""}]),
        ("summary_states", [{"running_of_user": "R", "queued_of_user": "Q"}]),
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


def _uncomment(text, block):
    commented = "\n".join("# " + line for line in block.splitlines())
    assert commented in text
    return text.replace(commented, block)


def test_state_column_examples_parse_and_validate(tmp_path):
    text = (Path(__file__).resolve().parents[1] / "qtopconf.yaml").read_text()
    states = "  - running_of_user\n  - cancelled_of_user\n  - queued_of_user\n  - other_of_user"
    text = _uncomment(text, "accounts_states:\n" + states).replace(states, "  - exiting_of_user: Ex\n" + states, 1)
    text = _uncomment(text, "summary_states:\n  - running_of_user\n  - queued_of_user\n  - other_of_user")
    path = tmp_path / "qtopconf.yaml"
    path.write_text(text)
    config = yaml_parser.parse(str(path))
    assert config["accounts_states"] == [{"exiting_of_user": "Ex"}, "running_of_user", "cancelled_of_user", "queued_of_user", "other_of_user"]
    assert config["summary_states"] == ["running_of_user", "queued_of_user", "other_of_user"]
    validate_config(config)


def test_summary_colours_example_parses_and_validates(tmp_path):
    text = (Path(__file__).resolve().parents[1] / "qtopconf.yaml").read_text()
    colours = ["WhiteOnBlue", "BlackOnGreen", "BlackOnOrange256", "BlackOnGold"]
    path = tmp_path / "qtopconf.yaml"
    path.write_text(
        text.replace("# summary_colours:\n" + "".join("#   - %s\n" % colour for colour in colours), "summary_colours:\n" + "".join("  - %s\n" % colour for colour in colours))
    )
    config = yaml_parser.parse(str(path))
    assert config["summary_colours"] == colours
    validate_config(config)


def test_sge_whole_queues_example_parses_and_validates(tmp_path):
    text = (Path(__file__).resolve().parents[1] / "qtopconf.yaml").read_text()
    path = tmp_path / "qtopconf.yaml"
    path.write_text(text.replace("# sge_whole_queues:\n#   - world\n#   - whole*", "sge_whole_queues:\n  - world\n  - whole*"))
    config = yaml_parser.parse(str(path))
    assert config["sge_whole_queues"] == ["world", "whole*"]
    validate_config(config)


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
    monkeypatch.setattr(configuration.yaml_parser, "parse", lambda path: pytest.fail("Snapshot replay must not read local configuration"))
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


def _layered_dirs(tmp_path):
    root = Path(__file__).resolve().parents[1]
    defaults, system, user, initial_cwd = (tmp_path / name for name in ("defaults", "system", "user", "cwd"))
    for directory in (defaults, system, user, initial_cwd):
        directory.mkdir()
    (defaults / "qtopconf.yaml").write_text((root / "qtopconf.yaml").read_text())
    (defaults / "palettes.yaml").write_text((root / "palettes.yaml").read_text())
    (user / "qtopconf.yaml").write_text("marker: user\n")
    return defaults, system, user, initial_cwd


def _age(*paths, seconds=60):
    """Backdate files beyond the racy window, so the cache may keep them."""
    for path in paths:
        stamp = path.stat().st_mtime - seconds
        os.utime(str(path), (stamp, stamp))


@pytest.fixture
def counted_parse(monkeypatch):
    configuration.clear_layer_cache()
    parsed = []
    parse = yaml_parser.parse
    monkeypatch.setattr(configuration.yaml_parser, "parse", lambda path: (os.path.exists(path) and parsed.append(Path(path).name)) or parse(path))  # count real parses only
    monkeypatch.setattr(configuration.fileutils, "private_save_directory", lambda path: path)
    yield parsed
    configuration.clear_layer_cache()


def test_unchanged_layers_are_parsed_once_and_each_frame_gets_its_own_copy(tmp_path, counted_parse):
    defaults, system, user, initial_cwd = _layered_dirs(tmp_path)
    _age(defaults / "qtopconf.yaml", defaults / "palettes.yaml", user / "qtopconf.yaml")
    options = SimpleNamespace(CONFFILE=None, OPTION=["savepath=" + str(tmp_path / "out")])
    first, _, _ = configuration.load_yaml_config(options, str(defaults), str(initial_cwd), str(system), str(user))
    assert sorted(counted_parse) == ["palettes.yaml", "qtopconf.yaml", "qtopconf.yaml"]
    first["sorting"]["user_sort"].append("mutated")
    first["palettes"]["PALETTES"].append("mutated")
    first["palettes"]["gruvbox"].clear()
    first["workernodes_matrix"].clear()
    second, _, _ = configuration.load_yaml_config(options, str(defaults), str(initial_cwd), str(system), str(user))
    assert len(counted_parse) == 3  # served from the cache
    assert "mutated" not in second["sorting"]["user_sort"]
    assert "mutated" not in second["palettes"]["PALETTES"] and second["palettes"]["gruvbox"]
    assert second["workernodes_matrix"] and second["marker"] == "user"
    assert second is not first and second["sorting"] is not first["sorting"]


def test_edited_added_and_removed_layers_invalidate_the_cache(tmp_path, counted_parse):
    defaults, system, user, initial_cwd = _layered_dirs(tmp_path)
    user_conf = user / "qtopconf.yaml"
    _age(defaults / "qtopconf.yaml", defaults / "palettes.yaml", user_conf)
    options = SimpleNamespace(CONFFILE=None, OPTION=[])

    def load():
        return configuration.load_yaml_config(options, str(defaults), str(initial_cwd), str(system), str(user))[0]

    assert load()["marker"] == "user"
    user_conf.write_text("marker: edited\n")  # same size as "user", new mtime
    _age(user_conf, seconds=30)
    assert load()["marker"] == "edited"
    (system / "qtopconf.yaml").write_text("marker: system\nsystem_only: kept\n")
    assert load()["system_only"] == "kept"
    user_conf.unlink()
    assert load()["marker"] == "system"
    (system / "qtopconf.yaml").unlink()
    assert "marker" not in load()
    (user / "palettes.yaml").write_text("default:\n  Red: '0;31'\n")
    assert load()["palettes"]["default"]["Red"] == "0;31"


def test_recently_modified_layers_are_reparsed(tmp_path, counted_parse):
    layer = tmp_path / "fresh.yaml"
    layer.write_text("marker: one\n")
    assert configuration.parse_layer(str(layer)) == {"marker": "one"}
    assert configuration.parse_layer(str(layer)) == {"marker": "one"}
    assert counted_parse == ["fresh.yaml", "fresh.yaml"]  # inside the racy window: never cached
    _age(layer)
    configuration.parse_layer(str(layer))
    configuration.parse_layer(str(layer))
    assert len(counted_parse) == 3


def test_invalid_layers_are_not_cached(tmp_path, counted_parse):
    layer = tmp_path / "broken.yaml"
    layer.write_text("")
    _age(layer)
    for _ in range(2):
        with pytest.raises(ValueError, match="empty"):
            configuration.parse_layer(str(layer))
    assert counted_parse == ["broken.yaml", "broken.yaml"]


def test_palettes_set_by_a_layer_skip_palettes_yaml(tmp_path, counted_parse, monkeypatch):
    defaults, system, user, initial_cwd = _layered_dirs(tmp_path)
    (user / "qtopconf.yaml").write_text("palettes:\n  default:\n    Red: '0;31'\n")
    monkeypatch.setattr(configuration, "load_palettes", lambda directories: pytest.fail("palettes.yaml must not be read"))
    config, _, _ = configuration.load_yaml_config(SimpleNamespace(CONFFILE=None, OPTION=[]), str(defaults), str(initial_cwd), str(system), str(user))
    assert config["palettes"] == {"default": {"Red": "0;31"}}
    assert "palettes.yaml" not in counted_parse
