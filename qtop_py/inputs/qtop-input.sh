#!/bin/sh
# SPDX-FileContributor: Sotiris Fragkiskos <sfranky@gmail.com>
# SPDX-FileCopyrightText: 2012 sfranky <sfranky@gmail.com>
# SPDX-License-Identifier: MIT


pbsnodes -a \
	|/bin/sed 's/^/pbsnodes:/g'

qstat -q \
    |/bin/sed 's/^/qstat-q:/g' 

qstat \
    |/bin/sed 's/^/qstat:/g' 
