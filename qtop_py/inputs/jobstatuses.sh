#!/bin/sh
JOBNAME='qtop-input'
JOBIDDIR=$HOME/$JOBNAME/jobIDs

while read line; do
    fname3=$line
    fname2="${fname3//\//-}"
    fname="${fname2//:/-}"
glite-wms-job-status -i $JOBIDDIR/$fname-jobID
done < $HOME/$JOBNAME/CEs

