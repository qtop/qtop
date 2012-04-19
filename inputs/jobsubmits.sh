#!/bin/sh
#The file at the end lists all the CeIDs in separate lines.It is post-formatted in vim
#The CeIDs are thus stored in variable "line".
JOBNAME='qtop-input'
JOBIDDIR=$HOME/$JOBNAME/jobIDs
#JOBIDDIR is the folder where job IDs are saved
let i=0
while read line; do
let i=i+1
#replace slashes and colons in CEIds with dashes to use them l8r as filenames
fname3=$line
fname2="${fname3//\//-}"
fname="${fname2//:/-}"

glite-wms-job-submit -r $line -o $JOBIDDIR/$fname-jobID -a $JOBNAME.jdl
done < $HOME/$JOBNAME/CEs


