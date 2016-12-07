from setuptools import setup
from qtop_py import __version__

import sys

if sys.version_info < (2, 5):
    sys.exit('Sorry, Python < 2.5 is not supported')

f = open("README.rst", 'r')
try:
    long_description = f.read()
finally:
    f.close()

setup(
    name='qtop',
    version=__version__,
    description="""qtop: the fast text mode way to monitor your cluster's utilization and status;
      the time has come to take back control of your cluster's scheduling business""",
    license="MIT",
    long_description=long_description,
    author='Sotiris Fragkiskos',
    author_email='sfranky@gmail.com',
    url="https://github.com/qtop/qtop",
    packages=['qtop_py',
              'qtop_py.legacy',
              'qtop_py.plugins',
              'qtop_py.ui'
              ],
    package_dir={'qtop_py': 'qtop_py'},
    package_data={'qtop_py': ['../qtopconf.yaml', '../qtop.py']},
    scripts=['qtop']
)
