import os
import tempfile
import datetime
import pytest
from qtop_py.fileutils import (
    mkdir_p,
    check_empty_file,
    get_new_temp_file,
    parse_time_input,
    get_timedelta,
    get_sample_filename,
    FileNotFound,
    FileEmptyError,
)


class TestMkdirP:
    def test_creates_directory(self, tmp_path):
        new_dir = tmp_path / "new_subdir"
        assert not new_dir.exists()
        mkdir_p(str(new_dir))
        assert new_dir.exists()
        assert new_dir.is_dir()

    def test_existing_directory_ok(self, tmp_path):
        existing = tmp_path / "existing"
        existing.mkdir()
        # should not raise
        mkdir_p(str(existing))
        assert existing.exists()

    def test_raises_on_other_oserror(self, monkeypatch):
        # Simulate an OSError with errno != EEXIST
        def fake_makedirs(path):
            raise OSError(13, "Permission denied")
        monkeypatch.setattr(os, "makedirs", fake_makedirs)
        with pytest.raises(OSError):
            mkdir_p("/nonexistent/path")


class TestCheckEmptyFile:
    def test_non_empty_ok(self, tmp_path):
        f = tmp_path / "nonempty.txt"
        f.write_text("hello")
        # should not raise
        check_empty_file(str(f))

    def test_empty_raises_FileEmptyError(self, tmp_path):
        f = tmp_path / "empty.txt"
        f.write_text("")
        with pytest.raises(FileEmptyError):
            check_empty_file(str(f))

    def test_nonexistent_raises_oserror(self, tmp_path):
        f = tmp_path / "nonexistent.txt"
        with pytest.raises(FileNotFoundError):
            check_empty_file(str(f))


class TestGetNewTempFile:
    def test_returns_valid_fd_and_path(self, tmp_path):
        fd, path = get_new_temp_file(str(tmp_path), suffix=".tmp", prefix="test_")
        assert isinstance(fd, int)
        assert fd >= 0
        assert os.path.exists(path)
        assert path.startswith(str(tmp_path))
        assert path.endswith(".tmp")
        # clean up
        os.close(fd)
        os.unlink(path)


class TestParseTimeInput:
    def test_hours(self):
        assert parse_time_input("5h") == {"hours": 5}

    def test_minutes(self):
        assert parse_time_input("10m") == {"minutes": 10}

    def test_seconds(self):
        assert parse_time_input("30s") == {"seconds": 30}

    def test_invalid_suffix_raises_assertion(self):
        with pytest.raises(AssertionError):
            parse_time_input("5d")

    def test_non_numeric_raises_value_error(self):
        with pytest.raises(ValueError):
            parse_time_input("abch")


class TestGetTimedelta:
    def test_returns_correct_timedelta(self):
        td = get_timedelta({"hours": 2})
        assert td == datetime.timedelta(hours=2)

        td = get_timedelta({"minutes": 30})
        assert td == datetime.timedelta(minutes=30)

        td = get_timedelta({"seconds": 45})
        assert td == datetime.timedelta(seconds=45)


class TestGetSampleFilename:
    def test_overwrite_mode(self, monkeypatch):
        config = {"overwrite_sample_file": True}
        SAMPLE_FILENAME = "sample_%(datetime)s.tar"
        result = get_sample_filename(SAMPLE_FILENAME, config)
        assert result == "sample_.tar"

    def test_timestamp_mode(self, monkeypatch):
        config = {"overwrite_sample_file": False}
        SAMPLE_FILENAME = "sample_%(datetime)s.tar"
        result = get_sample_filename(SAMPLE_FILENAME, config)
        # Format is "sample__20260625-091046.tar" (datetime with leading _)
        assert result.startswith("sample_")
        assert result.endswith(".tar")
        # The part after "sample_" should include "_YYYYMMDD-HHMMSS"
        timestamp_part = result[len("sample_"):-len(".tar")]
        assert len(timestamp_part) > 0
        assert timestamp_part.startswith("_")


class TestExceptions:
    def test_FileNotFound_message(self):
        exc = FileNotFound("testfile.txt")
        assert "testfile.txt" in str(exc)

    def test_FileEmptyError_message(self):
        exc = FileEmptyError("emptyfile.txt")
        assert "emptyfile.txt" in str(exc)
