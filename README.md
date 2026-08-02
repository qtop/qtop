# qtop [![build-qtop](https://github.com/qtop/qtop/actions/workflows/build.yml/badge.svg?branch=develop)](https://github.com/qtop/qtop/actions/workflows/build.yml) ![python versions](https://img.shields.io/badge/python-3.x-blue.svg) [![OpenSSF Scorecard](https://api.scorecard.dev/projects/github.com/qtop/qtop/badge)](https://scorecard.dev/viewer/?uri=github.com/qtop/qtop)

qtop: the fast text mode way to monitor your cluster's utilization and
status; the time has come to take back control of your cluster's
scheduling business

Python port by Sotiris Fragkiskos / Original bash version by Fotis
Georgatos

## Summary

![Example](https://raw.githubusercontent.com/qtop/qtop/master/qtop_py/contrib/qtop_demo.gif)

qtop is a Python tool for monitoring Torque, PBS, OAR, SGE, or Slurm
clusters. The *instant replay* feature is handy for debugging scheduling
mishaps as they occur.
qtop is and will remain a work-in-progress project; it is intended to be
built upon and extended - please come along ;)

Work continues to make the tool better. We hope to build an active open
source community that drives the future of qtop, both by providing
feedback and by actively contributing to the source code.

This program is currently in pre-release mode, with experimental
features. If it works, peace :)

qtop targets Python 3 and aims to remain runnable across several Linux
distributions and HPC environments. The CI matrix includes modern Python
lanes and a dependency-light AlmaLinux 8 / Python 3.6 compatibility lane
because HPC sites can lag behind general-purpose developer environments.

## Installation

To install qtop, you can either do

    git clone https://github.com/qtop/qtop.git
    cd qtop
    ./qtop --version

or

    pip install qtop --user ## run it without --user to install it as root
    $HOME/.local/bin/qtop --version

## Usage

To run a demo, just run

    ./qtop -b demo -FGTw  ## show demo, -F for full node names, -T to transpose the matrix, -G for full GECOS field, and -w for watch mode

Otherwise, for daily usage you can run

    ./qtop -b sge -FGw ## replace sge with pbs, oar or slurm, depending on your setup (this is often picked up automagically)

Try `--help` for all available options.

## Differential Debugging

To perform differential debugging across two HPC clusters:

1. Execute qtop on the first cluster and capture the output using `qstat -f -F json > cluster1_output.json`.
2. Execute qtop on the second cluster and capture the output using `qstat -f -F json > cluster2_output.json`.
3. Compare the outputs using a diff tool, e.g., `diff cluster1_output.json cluster2_output.json`.
4. Document the differences and provide screenshots of the qtop runs on both clusters.

**Note:** Ensure that qtop is pinned to the same version on both clusters for accurate comparison.

Documentation/tutorial [here](docs/documentation.rst).

## Profile

    Description: the fast text mode way to monitor your cluster's utilization and status; the time has come to take back control of your cluster's scheduling business
    License: MIT
    Version: 0.9.20260610 / Date: 2026-06-10
    Homepage: https://github.com/qtop/qtop
