# Synthetic LSF fixtures

These MIT-licensed traces are authored for qtop, not captured from a cluster.
They follow the column layouts IBM documents for LSF 10.1:

- `bhosts.txt`: `bhosts -w`
- `bqueues.txt`: `bqueues -w`
- `bjobs.txt`: `bjobs -u all -o "jobid jobindex stat user queue exec_host delimiter='|'"`

The scenario has five hosts (open with jobs, closed_Full, closed_Adm, unavail
and an idle open host), four queues (one Closed:Inact and empty) and nine jobs:
running, pending, system-suspended (still holding slots), pending-suspended,
a multi-host job and a two-element array (`1006[1]` running, `1006[2]`
pending). EXEC_HOST uses both the compact `N*host` form and repeated host
names, which the parser accepts interchangeably.

The bqueues NJOBS/PEND/RUN/SUSP columns count slots, not jobs, so qtop counts
running and pending jobs from bjobs and takes only status and MAX from bqueues;
the values here are deliberately not job counts. Real-cluster traces are still
needed to confirm site variations, such as wide host names, `-` job slots, the
"No unfinished job found" response and compute-unit statuses.
