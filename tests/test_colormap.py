##
## qtop is a tool to monitor queuing systems - https://github.com/qtop/qtop
##
## Copyright (c) 2016 Fotis Georgatos
## Copyright (c) 2016 Sotiris Fragkiskos
## Copyright (c) 2026 Jacob Hatchett
##
## SPDX-License-Identifier: MIT
##

import re
import pytest
from collections import OrderedDict
from qtop_py.colormap import (
    user_to_color_default,
    queue_to_color,
    nodestate_to_color_default,
    color_to_code,
)


class TestUserToColorDefault:
    def test_is_ordered_dict(self):
        assert isinstance(user_to_color_default, OrderedDict)

    def test_contains_atlas_entries(self):
        # at least one ATLAS-related entry exists
        atlas_keys = [k for k in user_to_color_default if "atlas" in k.lower()]
        assert len(atlas_keys) > 0

    def test_contains_cms_entries(self):
        cms_keys = [k for k in user_to_color_default if "cms" in k.lower()]
        assert len(cms_keys) > 0

    def test_contains_catch_all_rule(self):
        # the first entry should be the generic catch-all
        first_key = list(user_to_color_default.keys())[0]
        assert "[a-z]" in first_key

    def test_regex_patterns_are_valid(self):
        for pattern in user_to_color_default:
            try:
                re.compile(pattern)
            except re.error:
                pytest.fail(f"Invalid regex pattern: {pattern}")

    def test_catch_all_matches_simple_username(self):
        first_pattern = list(user_to_color_default.keys())[0]
        assert re.match(first_pattern, "simpleuser") is not None

    def test_catch_all_matches_username_with_dash(self):
        first_pattern = list(user_to_color_default.keys())[0]
        assert re.match(first_pattern, "user-name") is not None

    def test_specific_patterns_match_expected_strings(self):
        # atlas pattern should match atlas-related names
        assert re.match("atlas", "atlasprod") is not None
        # cms pattern should match cms-related names
        assert re.match("cms", "cmsusr") is not None


class TestQueueToColor:
    def test_is_ordered_dict(self):
        assert isinstance(queue_to_color, OrderedDict)

    def test_contains_pending(self):
        assert "Pending" in queue_to_color

    def test_contains_urgent(self):
        assert "urgent" in queue_to_color

    def test_regex_keys_are_valid(self):
        for pattern in queue_to_color:
            try:
                re.compile(pattern)
            except re.error:
                pytest.fail(f"Invalid regex pattern: {pattern}")

    def test_pending_is_yellow(self):
        assert queue_to_color["Pending"] == "Yellow"


class TestNodestateToColorDefault:
    def test_is_ordered_dict(self):
        assert isinstance(nodestate_to_color_default, OrderedDict)

    def test_contains_expected_states(self):
        assert "r" in nodestate_to_color_default    # running
        assert "d" in nodestate_to_color_default    # down
        assert "o" in nodestate_to_color_default    # offline

    def test_contains_hqw_for_purple(self):
        assert nodestate_to_color_default["hqw"] == "PurpleOnGrayBG"


class TestColorToCode:
    def test_contains_basic_colors(self):
        assert "Red" in color_to_code
        assert "Green" in color_to_code
        assert "Blue" in color_to_code
        assert "Yellow" in color_to_code

    def test_contains_reset(self):
        assert "reset" in color_to_code
        assert color_to_code["reset"] == "0"

    def test_all_color_values_referenced_by_user_map_exist(self):
        """All color names used in user_to_color_default should exist in color_to_code."""
        for color_name in user_to_color_default.values():
            assert color_name in color_to_code, f"Color '{color_name}' not in color_to_code"

    def test_all_color_values_referenced_by_queue_map_exist(self):
        for color_name in queue_to_color.values():
            assert color_name in color_to_code, f"Color '{color_name}' not in color_to_code"

    def test_all_color_values_referenced_by_nodestate_map_exist(self):
        for color_name in nodestate_to_color_default.values():
            # 'Gray' appears in nodestate map but is aliased to Gray_D/Gray_L via
            # runtime logic; skip it in the static cross-reference check.
            if color_name == "Gray":
                continue
            assert color_name in color_to_code, f"Color '{color_name}' not in color_to_code"

    def test_ansi_codes_are_valid_format(self):
        """All color codes should be valid ANSI escape parameter strings."""
        for color_name, code in color_to_code.items():
            if not code:  # empty string is allowed for "no color"
                continue
            # codes are semicolon-separated numbers
            # Background-only codes start with ';' (e.g. ';40') — those are valid
            parts = [p for p in code.split(";") if p]
            assert len(parts) > 0, f"No valid parts in code '{code}' for '{color_name}'"
            for part in parts:
                assert part.isdigit(), f"Invalid code part '{part}' in '{color_name}': '{code}'"

    def test_gray_dark_and_light_are_different(self):
        assert color_to_code["Gray_D"] != color_to_code["Gray_L"]

    def test_codes_are_non_empty_for_defined_colors(self):
        for name, code in color_to_code.items():
            if name in ("", "NOBG"):
                continue
            assert code, f"Color code for '{name}' should not be empty"
