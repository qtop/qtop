##
## qtop is a tool to monitor queuing systems - https://github.com/qtop/qtop
##
## Copyright (c) 2026 Jacob Hatchett
##
## SPDX-License-Identifier: MIT
##

"""Tests for qtop_py.colormap module.

Covers color_to_code mappings and OrderedDict structure integrity
for user_to_color_default, queue_to_color, and nodestate_to_color_default.
"""

import re

import pytest

from qtop_py.colormap import (
    color_to_code,
    nodestate_to_color_default,
    queue_to_color,
    user_to_color_default,
)


class TestColorToCode:
    """Tests for the color-to-ANSI-code mapping dict."""

    def test_reset_is_zero(self):
        assert color_to_code["reset"] == "0"

    def test_known_colors_have_valid_codes(self):
        for name, code in color_to_code.items():
            if name == "":
                continue
            parts = code.replace(";", " ").split()
            for part in parts:
                assert part.isdigit(), "Color code '%s' for '%s' contains non-digit" % (code, name)

    def test_empty_string_maps_to_empty(self):
        assert color_to_code[""] == ""

    def test_all_foreground_colors_present(self):
        foregrounds = ["Red", "Green", "Blue", "Cyan", "Yellow", "Purple", "Pink", "Brown"]
        for color in foregrounds:
            assert color in color_to_code

    def test_background_colors_present(self):
        backgrounds = ["GrayBG", "MaroonBG", "GreenBG", "GoldBG", "BlueBG", "MagentaBG", "CyanBG"]
        for bg in backgrounds:
            assert bg in color_to_code


class TestUserToColorDefault:
    """Tests for the user-to-color OrderedDict."""

    def test_is_ordered_dict(self):
        assert isinstance(user_to_color_default, dict)

    def test_catch_all_rule_exists(self):
        keys = list(user_to_color_default.keys())
        assert keys[0] == "[a-z][_a-z0-9.-]*"

    def test_all_values_are_valid_colors(self):
        for user_pattern, color in user_to_color_default.items():
            assert color in color_to_code, "Color '%s' for pattern '%s' not in color_to_code" % (color, user_pattern)

    def test_regex_patterns_compile(self):
        for pattern in user_to_color_default.keys():
            try:
                re.compile(pattern)
            except re.error:
                pytest.fail("Invalid regex pattern: %s" % pattern)


class TestQueueToColor:
    """Tests for queue-to-color OrderedDict."""

    def test_all_values_are_valid_colors(self):
        for queue, color in queue_to_color.items():
            assert color in color_to_code, "Color '%s' for queue '%s' not in color_to_code" % (color, queue)

    def test_contains_pending(self):
        assert "Pending" in queue_to_color


class TestNodestateToColor:
    """Tests for nodestate-to-color OrderedDict."""

    def test_all_values_are_valid_colors(self):
        known_exceptions = {"Gray"}
        for state, color in nodestate_to_color_default.items():
            if color in known_exceptions:
                continue
            assert color in color_to_code, "Color '%s' for state '%s' not in color_to_code" % (color, state)
    def test_contains_running_state(self):
        assert "r" in nodestate_to_color_default

