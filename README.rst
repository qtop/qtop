qtop.py |Build Status| |python versions|
========================================

qtop: the fast text mode way to monitor your cluster’s utilization and
status; the time has come to take back control of your cluster’s
scheduling business

Python port by Sotiris Fragkiskos / Original bash version by Fotis
Georgatos

Summary
-------

.. figure:: qtop_py/contrib/qtop_demo.gif
   :alt: Demo run of qtop with artificial data

   Example

qtop.py is the python rewrite of qtop, a tool to monitor Torque, PBS,
OAR or SGE clusters, etc. This release provides for the *instant replay*
feature, which is handy for debugging scheduling mishaps as they occur. 
qtop is and will remain a work-in-progress project; it is intended to 
be built upon and extended - please come along ;)

This is an initial release of the source code, and work continues to
make it better. We hope to build an active open source community that
drives the future of this tool, both by providing feedback and by
actively contributing to the source code.

This program is currently in pre-release mode, with experimental features. If it works, peace :)

Installation
------------

To install qtop, you can either do

::

    git clone https://github.com/qtop/qtop.git
    cd qtop
    ./qtop --version

or

::

    pip install qtop --user ## run it without --user to install it as root
    $HOME/.local/bin/qtop --version

Usage
-----

To run a demo, just run

::

    ./qtop -b demo -FGTw  ## show demo, -F for full node names, -T to transpose the matrix, -G for full GECOS field, and -w for watch mode

Otherwise, for daily usage you can run

::

    ./qtop -b sge -FGw ## replace sge with pbs or oar, depending on your setup (this is often picked up automagically) 


Try ``--help`` for all available options.

Documentation
-------------

Documentation/tutorial `here`_.

Profile
-------

::

    Description: the fast text mode way to monitor your cluster’s utilization and status; the time has come to take back control of your cluster’s scheduling business
    License: MIT
    Version: 0.9.20161216 / Date: 2016-12-16
    Homepage: https://github.com/qtop/qtop

.. _here: docs/documentation.rst

.. |Build Status| image:: https://travis-ci.org/qtop/qtop.svg
   :target: https://travis-ci.org/qtop/qtop
.. |python versions| image:: https://img.shields.io/badge/python-2.5%2C%202.6%2C%202.7-blue.svg
