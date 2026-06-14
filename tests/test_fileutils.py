##
## qtop is a tool to monitor queuing systems - https://github.com/qtop/qtop
##
## Copyright (c) 2026 Jacob Hatchett
##
## SPDX-License-Identifier: MIT
##

"""Tests for qtop_py.fileutils module.

Covers mkdir_p, parse_time_input, get_timedelta, get_sample_filename,
FileNotFound, FileEmptyError, and check_empty_file.
"""

import datetime
import os
import tempfile

import pytest

from qtop_py.fileutils import (
    FileEmptyError,
    FileNotFound,
    check_empty_file,
    get_sample_filename,
    get_timedelta,
    mkdir_p,
    parse_time_input,
)


class TestMkdirP:
    """Tests for the mkdir_p helper."""

    def test_creates_nested_directory(self, tmp_path):
        target = tmp_path / "a" / "b" / "c"
        assert not target.exists()
        mkdir_p(str(target))
        assert target.is_dir()

    def test_existing_directory_does_not_raise(self, tmp_path):
        target = tmp_path / "existing"
        target.mkdir()
        mkdir_p(str(target))
        assert target.is_dir()

    def test_creates_single_directory(self, tmp_path):
        target = tmp_path / "single"
        mkdir_p(str(target))
        assert target.is_dir()


class TestParseTimeInput:
    """Tests for parse_time_input string-to-timedelta conversion."""

    def test_hours(self):
        assert parse_time_input("5h") == {"hours": 5}

    def test_minutes(self):
        assert parse_time_input("10m") == {"minutes": 10}

    def test_seconds(self):
        assert parse_time_input("30s") == {"seconds": 30}

    def test_zero_hours(self):
        assert parse_time_input("0h") == {"hours": 0}

    def test_large_value(self):
        assert parse_time_input("999h") == {"hours": 999}

    def test_invalid_suffix_raises(self):
        with pytest.raises(AssertionError):
            parse_time_input("5x")

    def test_missing_suffix_raises(self):
        with pytest.raises(AssertionError):
            parse_time_input("5")


class TestGetTimedelta:
    """Tests for get_timedelta wrapper."""

    def test_hours(self):
        result = get_timedelta({"hours": 5})
        assert result == datetime.timedelta(hours=5)

    def test_minutes(self):
        result = get_timedelta({"minutes": 10})
        assert result == datetime.timedelta(minutes=10)

    def test_seconds(self):
        result = get_timedelta({"seconds": 30})
        assert result == datetime.timedelta(seconds=30)

    def test_combined_keywords(self):
        """get_timedelta accepts multiple keywords via **kwargs unpacking."""
        result = get_timedelta({"hours": 1, "minutes": 2})
        assert result == datetime.timedelta(hours=1, minutes=2)


class TestGetSampleFilename:
    """Tests for get_sample_filename with overwrite toggle."""

    def test_overwrite_returns_empty_datetime(self):
        config = {"overwrite_sample_file": True}
        result = get_sample_filename("sample_%(datetime)s.tar", config)
        assert result == "sample_.tar"

    def test_no_overwrite_contains_datetime(self):
        config = {"overwrite_sample_file": False}
        result = get_sample_filename("sample_%(datetime)s.tar", config)
        assert result.startswith("sample__")
        assert result.endswith(".tar")
        assert len(result) > len("sample__.tar")


class TestFileExceptions:
    """Tests for custom exception classes."""

    def test_file_not_found_message(self):
        exc = FileNotFound("missing.txt")
        assert "missing.txt" in str(exc)
        assert exc.fn == "missing.txt"

    def test_file_empty_error_message(self):
        exc = FileEmptyError("empty.txt")
        assert "empty.txt" in str(exc)
        assert exc.fn == "empty.txt"


class TestCheckEmptyFile:
    """Tests for check_empty_file validation."""

    def test_raises_on_empty_file(self, tmp_path):
        empty_file = tmp_path / "empty.txt"
        empty_file.write_text("")
        with pytest.raises(FileEmptyError):
            check_empty_file(str(empty_file))

    def test_does_not_raise_on_nonempty_file(self, tmp_path):
        normal_file = tmp_path / "normal.txt"
        normal_file.write_text("data")
        check_empty_file(str(normal_file))


