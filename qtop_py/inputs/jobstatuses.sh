#!/bin/sh
## Copyright (c) 2012-2026 qtop contributors
## SPDX-License-Identifier: MIT
JOBNAME='qtop-input'
JOBIDDIR=$HOME/$JOBNAME/jobIDs

while read line; do
    fname3=$line
    fname2="${fname3//\//-}"
    fname="${fname2//:/-}"
glite-wms-job-status -i $JOBIDDIR/$fname-jobID
done < $HOME/$JOBNAME/CEs

