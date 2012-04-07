#!/bin/sh
JOBNAME='qstat'
JOBDIR=$HOME/$JOBNAME-job

j=1
while read line;
do
let j++
fname3=$line
fname2="${fname3//\//-}"
fname="${fname2//:/-}"
glite-wms-job-output --dir $JOBDIR/outputs/ `sed -n '0~2p' $JOBDIR/jobIDs/*`
done<$JOBDIR/CEs

