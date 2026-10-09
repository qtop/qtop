import io
import os
import re

from setuptools import find_packages, setup
from setuptools.command.build_py import build_py as _build_py


class build_py(_build_py):
    """Install runtime data beside qtop_py without duplicating source files."""

    def run(self):
        super().run()
        package_dir = os.path.join(self.build_lib, "qtop_py")
        for filename in ("qtopconf.yaml", "helpfile.txt"):
            self.copy_file(filename, os.path.join(package_dir, filename))


def read_file(path):
    with io.open(path, encoding="utf-8") as handle:
        return handle.read()


def read_version():
    match = re.search(r'__version__ = "([^"]+)"', read_file("qtop_py/__init__.py"))
    if not match:
        raise RuntimeError("Unable to read qtop_py.__version__")
    return match.group(1)


setup(
    name="qtop",
    version=read_version(),
    description="qtop: the fast text mode way to monitor your cluster's utilization and status",
    long_description=read_file("README.md"),
    long_description_content_type="text/markdown",
    author="Sotiris Fragkiskos, Fotis Georgatos",
    author_email="sfranky@gmail.com, kefalonia@gmail.com",
    url="https://github.com/qtop/qtop",
    packages=find_packages(include=["qtop_py", "qtop_py.*"]),
    include_package_data=True,
    cmdclass={"build_py": build_py},
    python_requires=">=3.6",
    entry_points={"console_scripts": ["qtop=qtop_py.qtop:cli_main"]},
)
