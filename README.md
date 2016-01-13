# qtop.py [![Build Status](https://travis-ci.org/qtop/qtop.svg)](https://travis-ci.org/qtop/qtop)

Ported in Python by Sotiris Fragkiskos / Original bash version by Fotis Georgatos

## Summary
qtop.py is the python rewrite of qtop, a tool to monitor Torque, PBS, OAR or SGE clusters, etc.
This release provides for the *anonymization* feature, which is handy for debugging it without data leaks.
qtop is and will remain a work-in-progress project; it is intended to be built upon and extended.

This is an initial release of the source code, and work continues to make it better. 
We hope to build an active open source community that drives the future of this tool, 
both by providing feedback and by actively contributing to the source code.

This program is currently in development mode, with experimental features. If it works, peace :)

## Getting started

We use vagrant and [a docker container with a Torque/PBS server](https://hub.docker.com/r/agaveapi/torque/) as a demo environment. Should run fine in any environment with native Docker or Docker + VirtualBox + AMD-V or VT-x hardware support enabled.

    git clone https://github.com/qtop/qtop.git
    cd qtop
    
    vagrant up           # This will take ~5 minutes and download half a GB
    
    vagrant ssh          # Give password: "testuser" when asked
    
    # Submit 20 jobs:
    for i in `seq 1 20`; do qsub ~/torque.submit; done
    
    # Run qtop
    cd qtop              # Your files are here.
    python qtop.py -w    # qtop auto-detects PBS mode in this case


## Profile

```
Description: The command-line tool to tame queueing or scheduling systems and some more
License: MIT/GPL
Version: 0.8.5 / Date: December 12, 2015
Homepage: https://github.com/qtop/qtop
```
