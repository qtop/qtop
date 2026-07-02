##
## qtop is a tool to monitor queuing systems - https://github.com/qtop/qtop
##
## Copyright (c) 2016 Fotis Georgatos
## Copyright (c) 2016 Sotiris Fragkiskos
## Copyright (c) 2026 Jacob Hatchett
##
## SPDX-License-Identifier: MIT
##

import os
import errno
import tempfile
import datetime
import tarfile
import pytest
from qtop_py import fileutils


class TestMkdirP:
    def test_creates_directory(self, tmp_path):
        newdir = os.path.join(tmp_path, "a", "b", "c")
        fileutils.mkdir_p(newdir)
        assert os.path.isdir(newdir)

    def test_no_error_on_existing_directory(self, tmp_path):
        newdir = os.path.join(tmp_path, "existing")
        os.makedirs(newdir)
        fileutils.mkdir_p(newdir)  # should not raise
        assert os.path.isdir(newdir)

    def test_raises_on_other_oserror(self, monkeypatch):
        original_makedirs = os.makedirs

        def mock_makedirs(path):
            exc = OSError()
            exc.errno = errno.EACCES  # permission denied, not EEXIST
            raise exc

        monkeypatch.setattr(os, "makedirs", mock_makedirs)
        with pytest.raises(OSError):
            fileutils.mkdir_p("/no/such/permission")


class TestCheckEmptyFile:
    def test_raises_on_empty_file(self, tmp_path):
        empty_file = tmp_path / "empty.txt"
        empty_file.write_text("")
        with pytest.raises(fileutils.FileEmptyError):
            fileutils.check_empty_file(str(empty_file))

    def test_passes_on_non_empty_file(self, tmp_path):
        nonempty_file = tmp_path / "data.txt"
        nonempty_file.write_text("content")
        fileutils.check_empty_file(str(nonempty_file))  # should not raise


class TestGetNewTempFile:
    def test_creates_temp_file(self, tmp_path):
        fd, temp_path = fileutils.get_new_temp_file(str(tmp_path), suffix=".txt", prefix="test_")
        assert os.path.exists(temp_path)
        assert temp_path.startswith(str(tmp_path))
        assert "test_" in os.path.basename(temp_path)
        assert temp_path.endswith(".txt")
        os.close(fd)

    def test_creates_multiple_unique_files(self, tmp_path):
        fd1, path1 = fileutils.get_new_temp_file(str(tmp_path), suffix=".log", prefix="a_")
        fd2, path2 = fileutils.get_new_temp_file(str(tmp_path), suffix=".log", prefix="b_")
        assert path1 != path2
        os.close(fd1)
        os.close(fd2)


class TestGetSampleFilename:
    def test_overwrite_mode_returns_fixed_name(self):
        config = {"overwrite_sample_file": True}
        result = fileutils.get_sample_filename("qtop_sample%(datetime)s.tar", config)
        assert result == "qtop_sample.tar"

    def test_non_overwrite_mode_adds_timestamp(self):
        config = {"overwrite_sample_file": False}
        result = fileutils.get_sample_filename("qtop_sample%(datetime)s.tar", config)
        # should contain underscore + datetime pattern
        assert result.startswith("qtop_sample_")
        assert result.endswith(".tar")
        # verify the middle part is a datetime-like string
        middle = result[len("qtop_sample_"):-len(".tar")]
        assert len(middle) == 15  # YYYYMMDD-HHMMSS


class TestParseTimeInput:
    def test_hours_suffix(self):
        result = fileutils.parse_time_input("5h")
        assert result == {"hours": 5}

    def test_minutes_suffix(self):
        result = fileutils.parse_time_input("10m")
        assert result == {"minutes": 10}

    def test_seconds_suffix(self):
        result = fileutils.parse_time_input("30s")
        assert result == {"seconds": 30}

    def test_raises_on_invalid_suffix(self):
        with pytest.raises(AssertionError):
            fileutils.parse_time_input("5x")

    def test_raises_on_non_numeric(self):
        with pytest.raises(Exception):  # ValueError in logging.critical path
            fileutils.parse_time_input("abch")


class TestGetTimedelta:
    def test_returns_timedelta(self):
        result = fileutils.get_timedelta({"hours": 2})
        assert result == datetime.timedelta(hours=2)

    def test_returns_timedelta_minutes(self):
        result = fileutils.get_timedelta({"minutes": 30})
        assert result == datetime.timedelta(minutes=30)


class TestFileNotFound:
    def test_message_format(self):
        exc = fileutils.FileNotFound("missing.txt")
        assert "missing.txt" in str(exc)
        assert exc.fn == "missing.txt"


class TestFileEmptyError:
    def test_message_format(self):
        exc = fileutils.FileEmptyError("blank.txt")
        assert "blank.txt" in str(exc)
        assert "empty" in str(exc).lower()


class TestAddToSample:
    def test_adds_files_to_tarfile(self, tmp_path):
        savepath = str(tmp_path)
        sample_file = os.path.join(savepath, "sample.tar")
        tar_out = tarfile.open(sample_file, mode="w")

        # create a test file
        test_file = tmp_path / "test_data.txt"
        test_file.write_text("hello world")

        fileutils.add_to_sample([str(test_file)], tar_out)
        tar_out.close()

        # verify the file is in the tar
        with tarfile.open(sample_file, "r") as tf:
            names = tf.getnames()
            assert "test_data.txt" in names

    def test_adds_files_with_subdir(self, tmp_path):
        savepath = str(tmp_path)
        sample_file = os.path.join(savepath, "sample.tar")
        tar_out = tarfile.open(sample_file, mode="w")

        test_file = tmp_path / "source.py"
        test_file.write_text("print('hello')")

        fileutils.add_to_sample([str(test_file)], tar_out, subdir="qtop_py")
        tar_out.close()

        with tarfile.open(sample_file, "r") as tf:
            names = tf.getnames()
            assert "qtop_py/source.py" in names

    def test_requires_list_input(self):
        with pytest.raises(AssertionError):
            fileutils.add_to_sample("not_a_list", None)


class TestDeprecateOldOutputFiles:
    def test_deletes_old_json_files(self, tmp_path):
        config = {
            "savepath": str(tmp_path),
            "auto_delete_old_output_files_after": "1s",
        }

        # create an old .json file
        old_json = tmp_path / "old_data.json"
        old_json.write_text("{}")
        # set mtime to 2 hours ago
        old_time = datetime.datetime.now() - datetime.timedelta(hours=2)
        os.utime(str(old_json), (old_time.timestamp(), old_time.timestamp()))

        fileutils.deprecate_old_output_files(config)
        assert not old_json.exists()

    def test_keeps_recent_files(self, tmp_path):
        config = {
            "savepath": str(tmp_path),
            "auto_delete_old_output_files_after": "24h",
        }

        # create a recent file
        recent_json = tmp_path / "recent.json"
        recent_json.write_text("{}")

        fileutils.deprecate_old_output_files(config)
        assert recent_json.exists()

    def test_keeps_rec_out_files(self, tmp_path):
        config = {
            "savepath": str(tmp_path),
            "auto_delete_old_output_files_after": "1s",
        }

        rec_out = tmp_path / "something.rec.out"
        rec_out.write_text("data")
        old_time = datetime.datetime.now() - datetime.timedelta(hours=2)
        os.utime(str(rec_out), (old_time.timestamp(), old_time.timestamp()))

        fileutils.deprecate_old_output_files(config)
        assert rec_out.exists()

    def test_keeps_non_json_non_out_files(self, tmp_path):
        config = {
            "savepath": str(tmp_path),
            "auto_delete_old_output_files_after": "1s",
        }

        txt_file = tmp_path / "notes.txt"
        txt_file.write_text("keep me")
        old_time = datetime.datetime.now() - datetime.timedelta(hours=2)
        os.utime(str(txt_file), (old_time.timestamp(), old_time.timestamp()))

        fileutils.deprecate_old_output_files(config)
        assert txt_file.exists()
