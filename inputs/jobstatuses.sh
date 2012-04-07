#!/bin/sh

while read line; do
    fname3=$line
    fname2="${fname3//\//-}"
    fname="${fname2//:/-}"
glite-wms-job-status -i ~/qstat-job/jobIDs/$fname-jobID
done < ~/qstat-job/CEs

