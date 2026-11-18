# Synthetic Slurm JSON fixtures

These MIT-licensed examples are authored for qtop, not captured from a cluster.
They model data_parser/v0.0.40, v0.0.43, v0.0.44, v0.0.45 and v0.0.46
response shapes. Versions 41 and 42 exercise the shared v43 allocation shape.
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

## Version 44-46 source review

Reviewed against SchedMD tag `slurm-26-11-0-0rc1`, commit
`a202eca2bfaf2f17f945fa73b185a542bcc54d6a`:
https://github.com/SchedMD/slurm/tree/a202eca2bfaf2f17f945fa73b185a542bcc54d6a/src/plugins/data_parser

For each version, `parsers.c` defines `NODE`, `JOB_INFO`, `PARTITION_INFO`,
`JOB_RES`, `JOB_RES_NODE`, `JOB_RES_SOCKET`, `JOB_RES_CORE` and the custom
`_dump_node_res` / `JOB_INFO_GRES_DETAIL` dumpers. The consumed field paths
and types agree across these versions:

- Job/array/heterogeneous IDs retain their UINT32/UINT32_NO_VAL forms.
- Job CPU requests use UINT32_NO_VAL; allocation CPU counts remain integers.
- Allocation nodes remain under `job_resources.nodes.allocation`, with
  per-node `cpus.count`, socket/core indices and core-status flags.
- `UNALLOCATED` and `ALLOCATED, IN_USE` retain explicit core bitmap meaning.
- Node CPU/socket/core/thread counts, GRES strings, per-node job GRES details,
  partition memberships and `partition.state` retain their existing shapes.

The v44-46 fixtures include unset/set integer wrappers, all socket/core rows
(including unallocated cores), per-node memory, uneven CPU allocation and
GPU/MIG bindings. These independent on-disk responses use the same cluster
scenario so normalized output can be compared directly. They are hand-built
schema subsets, not full response-schema conformance or real RPC evidence.
Version 46 is validated against a prerelease; revisit on the final release.
The shared contract tests cover malformed/mixed versions, missing allocation
information, overlap, SMT estimates, arrays, anonymization and replay.
