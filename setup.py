import os
import re

import setuptools


def _setuptools_version_tuple():
    parts = re.split(r"[^\d]+", setuptools.__version__)
    numbers = [int(part) for part in parts if part.isdigit()]
    return tuple(numbers[:2])


def _read(path):
    with open(os.path.join(os.path.dirname(__file__), path), encoding="utf-8") as handle:
        return handle.read()


def _version():
    match = re.search(r'__version__ = "([^"]+)"', _read("qtop_py/__init__.py"))
    if not match:
        raise RuntimeError("Cannot find qtop version")
    return match.group(1)


legacy_kwargs = {}
if _setuptools_version_tuple() < (61, 0):
    legacy_kwargs = {
        "name": "qtop",
        "version": _version(),
        "description": "qtop: the fast text mode way to monitor your cluster's utilization and status",
        "long_description": _read("README.md"),
        "long_description_content_type": "text/markdown",
        "author": "Sotiris Fragkiskos, Fotis Georgatos",
        "author_email": "sfranky@gmail.com, kefalonia@gmail.com",
        "url": "https://github.com/qtop/qtop",
        "packages": ["qtop_py", "qtop_py.plugins", "qtop_py.ui"],
        "python_requires": ">=3",
        "entry_points": {"console_scripts": ["qtop=qtop_py.qtop:main"]},
    }


setuptools.setup(**legacy_kwargs)
