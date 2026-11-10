## Copyright (c) 2026 qtop contributors
## SPDX-License-Identifier: MIT
"""setuptools build step that ships qtopconf.yaml, palettes.yaml and helpfile.txt inside the package."""

import os

from setuptools.command.build_py import build_py


class BuildPy(build_py):
    """Copy the source defaults into installed packages."""

    def run(self):
        """Build the package, then copy the default config and help file into it."""
        super().run()
        package_dir = os.path.join(self.build_lib, "qtop_py")
        for filename in ("qtopconf.yaml", "palettes.yaml", "helpfile.txt"):
            self.copy_file(filename, os.path.join(package_dir, filename))
