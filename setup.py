# SPDX-License-Identifier: MIT
"""Fallback metadata for Python 3.6-3.8 build backends without PEP 621."""

import os
import sys

from setuptools import find_packages, setup

if sys.version_info < (3, 9):
    # PEP 517 runners need not put the source directory on sys.path.
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    from qtop_py import __version__
    from tools.build_py import BuildPy

    with open("README.md", encoding="utf-8") as readme:
        long_description = readme.read()
    setup(
        name="qtop",
        version=__version__,
        description="qtop: the fast text mode way to monitor your cluster's utilization and status; the time has come to take back control of your cluster's scheduling business",
        long_description=long_description,
        long_description_content_type="text/markdown",
        license="MIT",
        author="Sotiris Fragkiskos, Fotis Georgatos, Evangelos Papadopoulos, Nicola Trozzi",
        keywords="HPC monitoring PBS scheduler SGE Slurm",
        python_requires=">=3.6",
        packages=find_packages(include=["qtop_py", "qtop_py.*"]),
        include_package_data=True,
        cmdclass={"build_py": BuildPy},
        entry_points={"console_scripts": ["qtop = qtop_py.qtop:cli_main"]},
        project_urls={
            "Homepage": "https://github.com/qtop/qtop",
            "Changelog": "https://github.com/qtop/qtop/blob/main/CHANGELOG.md",
            "Documentation": "https://github.com/qtop/qtop/blob/main/docs/documentation.md",
            "Issues": "https://github.com/qtop/qtop/issues",
            "Security": "https://github.com/qtop/qtop/security/policy",
        },
        classifiers=[
            "Development Status :: 4 - Beta",
            "Environment :: Console",
            "Operating System :: POSIX :: Linux",
            "Programming Language :: Python :: 3",
            "Programming Language :: Python :: 3 :: Only",
            "Intended Audience :: System Administrators",
            "Topic :: System :: Clustering",
            "Topic :: System :: Monitoring",
        ],
    )
else:
    # Modern backends continue to take their metadata from pyproject.toml.
    setup()
