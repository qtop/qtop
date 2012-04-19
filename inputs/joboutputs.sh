#!/bin/sh
JOBNAME='qtop-input'
JOBIDDIR=$HOME/$JOBNAME/jobIDs #the jobIDs were saved here by jobsubmits.sh
JOBOUT=$HOME/$JOBNAME/outputs #the outputs are saved here
j=1
while read line;
do
let j++
fname3=$line
fname2="${fname3//\//-}"
fname="${fname2//:/-}"
glite-wms-job-output --dir $JOBOUT/ `sed -n '0~2p' $JOBIDDIR/*`
done< $HOME/$JOBNAME/CEs

