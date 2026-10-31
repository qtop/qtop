## SPDX-License-Identifier: MIT
"""Check invalid configuration at the boundary, before rendering or commands."""

from pathlib import Path

import pytest

from qtop_py import qtop, yaml_parser
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
    monkeypatch.setattr(qtop.fileutils, "private_save_directory", lambda *args: pytest.fail("Invalid config must not create directories"))
    with pytest.raises(ValueError, match=path.split(".")[0]):
        qtop.prepare_config(config)


def test_validation_accepts_defaults_normalized_values_and_extensions(config):
    config["custom_backend_settings"] = {"new_field": "future value"}
    validate_config(config)
    validate_config(qtop.normalize_config(config))
    assert config["transpose_wn_matrices"] == "True"


@pytest.mark.parametrize("value", ["0.25", 0.25, 30])
def test_positive_finite_timeouts_are_valid(config, value):
    config["scheduler_command_timeout"] = value
    validate_config(config)


@pytest.mark.parametrize("sorting", [{}, {"reverse": False}, {"user_sort": None}, {"user_sort": []}])
def test_sorting_can_be_disabled(config, sorting):
    config["sorting"] = sorting
    validate_config(config)
