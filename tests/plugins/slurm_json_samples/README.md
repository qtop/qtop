# Synthetic Slurm JSON fixtures

These MIT-licensed examples are authored for qtop, not captured from a cluster.
They model data_parser/v0.0.40 and v0.0.43 response shapes. The contract cases
also exercise the shared v0.0.41, v0.0.42, v0.0.44 and v0.0.45 allocation shape.
Core/socket layouts and CPU_IDs conversion were checked against SchedMD Slurm parser sources at tags
slurm-24-05-0-1, slurm-24-11-0-1, slurm-25-05-0-1, slurm-25-11-0-1 and
slurm-26-05-0-1 under src/plugins/data_parser/. The newer allocation field
names are stable across those supported versions.

Each family contains two running heterogeneous components, a pending array
task with multiple eligible partitions, uneven allocations, two-thread cores,
GPU/MIG index metadata, a drained node and an empty inactive partition.
Slurm JSON node CPU IDs are checked against corresponding legacy CPU_IDs in
the tests. The conversion follows src/scontrol/info_job.c: each allocated
core j expands to CPU IDs j * threads + k, using node thread counts.
Fields unrelated to this backend are deliberately omitted.
