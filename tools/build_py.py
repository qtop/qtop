# SPDX-License-Identifier: MIT

import os

from setuptools.command.build_py import build_py


class BuildPy(build_py):
    """Copy the source defaults into installed packages."""

    def run(self):
        super().run()
        package_dir = os.path.join(self.build_lib, "qtop_py")
        for filename in ("qtopconf.yaml", "helpfile.txt"):
            self.copy_file(filename, os.path.join(package_dir, filename))
