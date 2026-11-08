#!/bin/sh
## Copyright (c) 2012-2026 qtop contributors
## SPDX-License-Identifier: MIT

pbsnodes -a \
	|/bin/sed 's/^/pbsnodes:/g'

qstat -q \
    |/bin/sed 's/^/qstat-q:/g' 

qstat \
    |/bin/sed 's/^/qstat:/g' 
