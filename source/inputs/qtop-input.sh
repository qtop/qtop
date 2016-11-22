#!/bin/sh

pbsnodes -a \
	|/bin/sed 's/^/pbsnodes:/g'

qstat -q \
    |/bin/sed 's/^/qstat-q:/g' 

qstat \
    |/bin/sed 's/^/qstat:/g' 
