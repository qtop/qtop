import errno
import os

import qtop_py.fileutils as fileutils


def test_deprecate_old_output_files_handles_race_condition(tmp_path, monkeypatch):
    # Create dummy output files
    file1 = tmp_path / "test1.json"
    file1.touch()
    file2 = tmp_path / "test2.out"
    file2.touch()

    config = {
        "auto_delete_old_output_files_after": "1s",
        "savepath": str(tmp_path)
    }

    original_getmtime = os.path.getmtime

    def mock_getmtime(path):
        # Force a file not found error for one of the files
        if "test1.json" in path:
            raise OSError(errno.ENOENT, "No such file or directory")
        return original_getmtime(path)

    monkeypatch.setattr(os.path, "getmtime", mock_getmtime)

    # This should not raise OSError because we handle ENOENT gracefully!
    fileutils.deprecate_old_output_files(config)
