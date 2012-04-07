#!/bin/sh
#The file at the end lists all the CeIDs in separate lines.It is post-formatted in vim
#The CeIDs are thus stored in variable "line".
JOBNAME='qtop-input'
JOBDIR=$HOME/jobIDs/$JOBNAME-job
let i=0
while read line; do
let i=i+1
fname3=$line
#replace slashes and colons in CEIds with dashes to use them l8r as filenames
fname2="${fname3//\//-}"
fname="${fname2//:/-}"
#format:
#glite-wms-job-submit -r <CeId> -o <filepath to save the jobId to> -a jobfilename.jdl
#glite-wms-job-submit -r $line -o ~/sleep-oldVersion/jobIDs/$i -a sleep.jdl  #$fname-jobID -a sleep.jdl
glite-wms-job-submit -r $line -o $JOBDIR/$fname-jobID -a $JOBNAME.jdl
done <$JOBDIR/CEs


