import datetime
import errno
import os
import tempfile
import unittest
from unittest import mock

from qtop_py import fileutils


class DeprecateOldOutputFilesTest(unittest.TestCase):
    def test_ignores_raced_deletion(self):
        with tempfile.TemporaryDirectory() as tmp_path:
            stale_output = os.path.join(tmp_path, "qtop_partview_20180311T140756.out")
            with open(stale_output, "w") as output_file:
                output_file.write("old output")

            real_getmtime = os.path.getmtime

            def remove_before_stat(path):
                if path == stale_output:
                    os.unlink(stale_output)
                    raise OSError(errno.ENOENT, "No such file or directory", path)
                return real_getmtime(path)

            with mock.patch.object(fileutils.os.path, "getmtime", remove_before_stat):
                fileutils.deprecate_old_output_files(
                    {
                        "auto_delete_old_output_files_after": "1s",
                        "savepath": tmp_path,
                    }
                )

    def test_removes_stale_outputs(self):
        with tempfile.TemporaryDirectory() as tmp_path:
            stale_output = os.path.join(tmp_path, "qtop_fullview_20180311T140756.out")
            with open(stale_output, "w") as output_file:
                output_file.write("old output")
            old_timestamp = (datetime.datetime.now() - datetime.timedelta(hours=1)).timestamp()
            os.utime(stale_output, (old_timestamp, old_timestamp))

            fileutils.deprecate_old_output_files(
                {
                    "auto_delete_old_output_files_after": "1s",
                    "savepath": tmp_path,
                }
            )

            self.assertFalse(os.path.exists(stale_output))


if __name__ == "__main__":
    unittest.main()
