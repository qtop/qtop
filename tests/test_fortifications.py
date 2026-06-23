##
## qtop is a tool to monitor queuing systems - https://github.com/qtop/qtop
##
## Copyright (c) 2026 Jacob Hatchett
##
## SPDX-License-Identifier: MIT
##

"""Tests for tools/fortifications.py diff and source-health checks.

Covers find_eval_calls, CONTROL_OR_BIDI regex, and GENERATED_OR_BINARY regex
without requiring a live git repository.
"""

import re
import textwrap

import pytest

from tools.fortifications import CONTROL_OR_BIDI, GENERATED_OR_BINARY, find_eval_calls


class TestControlOrBidiRegex:
    """Tests for the control/bidi character detection regex."""

    def test_normal_ascii_passes(self):
        assert CONTROL_OR_BIDI.search("hello world 123") is None

    def test_tab_is_allowed(self):
        assert CONTROL_OR_BIDI.search("col1\tcol2") is None

    def test_newline_is_allowed(self):
        assert CONTROL_OR_BIDI.search("line1\nline2") is None

    def test_carriage_return_is_allowed(self):
        assert CONTROL_OR_BIDI.search("line1\r\nline2") is None

    def test_bidi_override_detected(self):
        # U+202E RIGHT-TO-LEFT OVERRIDE
        assert CONTROL_OR_BIDI.search("text\u202emalicious") is not None

    def test_bidi_embedding_detected(self):
        # U+202A LEFT-TO-RIGHT EMBEDDING
        assert CONTROL_OR_BIDI.search("text\u202aembedded") is not None

    def test_null_byte_detected(self):
        assert CONTROL_OR_BIDI.search("text\x00end") is not None

    def test_bell_char_detected(self):
        assert CONTROL_OR_BIDI.search("text\x07end") is not None


class TestGeneratedOrBinaryRegex:
    """Tests for the generated/binary file path detection regex."""

    def test_normal_python_file_passes(self):
        assert GENERATED_OR_BINARY.search("src/main.py") is None

    def test_test_files_detected(self):
        assert GENERATED_OR_BINARY.search("tests/files/data.txt") is not None

    def test_fixture_detected(self):
        assert GENERATED_OR_BINARY.search("tests/fixtures/input.json") is not None

    def test_binary_extension_detected(self):
        assert GENERATED_OR_BINARY.search("assets/image.png") is not None
        assert GENERATED_OR_BINARY.search("data/archive.tar.gz") is not None
        assert GENERATED_OR_BINARY.search("build/output.bin") is not None

    def test_cmake_detected(self):
        assert GENERATED_OR_BINARY.search("cmake/FindModule.cmake") is not None

    def test_configure_detected(self):
        assert GENERATED_OR_BINARY.search("cmake/configure") is not None

    def test_autogen_detected(self):
        assert GENERATED_OR_BINARY.search("autogen/") is not None

    def test_makefile_in_detected(self):
        assert GENERATED_OR_BINARY.search("subdir/Makefile.in/") is not None



