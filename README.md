# qtop.py [![Build Status](https://travis-ci.org/qtop/qtop.svg)](https://travis-ci.org/qtop/qtop)

Python port by Sotiris Fragkiskos / Original bash version by Fotis Georgatos

## Summary
qtop.py is the python rewrite of qtop, a tool to monitor Torque, PBS, OAR or SGE clusters, etc.
This release provides for the *anonymization* feature, which is handy for debugging it without data leaks.
qtop is and will remain a work-in-progress project; it is intended to be built upon and extended.

This is an initial release of the source code, and work continues to make it better. 
We hope to build an active open source community that drives the future of this tool, 
both by providing feedback and by actively contributing to the source code.

This program is currently in development mode, with experimental features. If it works, peace :)


## Getting started

To try qtop, you just have to do:

```
git clone https://github.com/qtop/qtop.git

cd qtop

./qtop.py 
```

Try ```./qtop.py --help``` for all available options.


### PBS
We use vagrant and [a docker container with a Torque/PBS server](https://hub.docker.com/r/agaveapi/torque/) as a demo environment (also for testing purposes). Should run fine in any environment with native Docker or Docker + VirtualBox + AMD-V or VT-x hardware support enabled.

    git clone https://github.com/qtop/qtop.git
    cd qtop
    
    vagrant up pbs       # This will take ~5 minutes and download half a GB
    
    vagrant ssh pbs          # Give password: "testuser" when asked
    
    # Submit 20 jobs:
    for i in `seq 1 20`; do qsub ~/torque.submit; done

### SGE
We use vagrant and [a docker container with a gridengine server](https://hub.docker.com/r/agaveapi/gridengine/) as a demo environment. 

    git clone https://github.com/qtop/qtop.git
    cd qtop
    
    vagrant up sge       # This will take ~5 minutes and download half a GB
    
    vagrant ssh sge      # Give password: "testuser" when asked
    
    # Submit 20 jobs:
    for i in `seq 1 20`; do qsub /home/testuser/gridengine.submit; done
    
    # Run qtop
    cd qtop              # Your files are here.
    python qtop.py -w    # qtop auto-detects SGE mode in this case

## Profile

```
Description: The command-line tool to tame queueing or scheduling systems and some more
License: MIT/GPL
Version: 0.8.6 / Date: January 21, 2016
Homepage: https://github.com/qtop/qtop
```
