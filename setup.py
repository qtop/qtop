from setuptools import setup

import sys

if sys.version_info < (2, 5):
    sys.exit('Sorry, Python < 2.5 is not supported')

with open("README.rst", 'r') as f:
    long_description = f.read()

setup(
    name='qtop',
    version='0.9.0rc1',
    description="""qtop: the fast text mode way to monitor your cluster's utilization and status;
      the time has come to take back control of your cluster's scheduling business""",
    license="MIT",
    long_description=long_description,
    author='Sotiris Fragkiskos',
    author_email='sfranky@gmail.com',
    url="https://github.com/qtop/qtop",
    packages=['qtop'],
    scripts=[
        'inputs/joboutputs.sh',
        'inputs/jobstatuses.sh',
        'inputs/jobsubmits.sh',
        'inputs/qtop-input.jdl',
        'inputs/qtop-input.sh',
        'contrib/func_tests.sh',
        'contrib/oar1_dvv_out.ref',
        'contrib/oarnodes_s_Y.txt',
        'contrib/oarnodes_Y.txt',
        'contrib/oarstat.txt',
        'contrib/pbs_dvv_out.ref',
        'contrib/pbsnodes_a.txt',
        'contrib/qstat.F.xml.stdout',
        'contrib/qstat.txt',
        'contrib/qstat_q.txt',
        'contrib/qtop_demo.gif',
        'contrib/sger_dvv_out.ref',
        'web/index.html',
        'qtop/qtopconf.yaml'
     ]
)
