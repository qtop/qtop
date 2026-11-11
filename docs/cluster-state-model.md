# Cluster state model

`ClusterState` is the plain-data boundary between scheduler backends and the
rest of qtop. Its contract:

> Plugins produce normalized observations; qtop derives views; FinOps adapters
> derive usage and cost records.

A `ClusterState` describes what one backend reported about one cluster at one
poll. It is not a history, a usage record or a bill. Anything derived from it,
such as the terminal views or future cost records, belongs outside it.

## Schema versions

| Version | Shape | Produced by |
|---|---|---|
| 1 | `{"jobs", "nodes", "queues"}` plus optional `*_diagnostics` | every current plugin, and existing snapshots and archives |
| 2 | version 1 plus `"schema_version": 2` and `"source": {"backend", "cluster_id"}` | backends that know their cluster's identity (opt-in) |

A state without `schema_version` is version 1, and `"schema_version": 1` means
the same. `validate_cluster_state()` in
[cluster_state.py](../qtop_py/cluster_state.py) rejects any other version, a
`source` without version 2, and version 2 fields (`qid`, `kind`) in a version 1
state, so a record's meaning never depends on guessing. Snapshot archives keep
`format_version: 1`, because the archive layout is unchanged. The state inside
an archive carries its own version, so old archives replay unchanged.

Version 2 adds only optional fields:

```python
ClusterState = {
    "schema_version": 2,
    "source": {"backend": "slurm", "cluster_id": "cluster-a"},
    "jobs": [{"id": "42", ..., "qid": "slurm:cluster-a:42", "kind": "job"}],
    "nodes": [{"domainname": "node001", ..., "qid": "slurm:cluster-a:node001"}],
    "queues": [...],
}
```

- `source.backend` names the plugin, and the operator chooses `source.cluster_id`.
  Both are nonempty text without `:`.
- `qid` on jobs and nodes must be `"<backend>:<cluster_id>:<id>"`. It is unique
  across all jobs and nodes of the observation, so qids from different backends
  or clusters cannot collide when observations are combined.
  `qid` stands for *qualified ID*: the backend's own job ID or node name,
  qualified by its source. It is deliberately not called `uid`, which in qtop
  means a Unix user ID, and it promises no more persistence than the backend
  identifier inside it (see [Identity lifetime](#identity-lifetime)).
- `kind` on jobs is one of `job`, `pod`, `vm` or `task`. Nodes have no kind.
- Existing fields are unchanged: `id`, `domainname`, `core_job_map`, the GPU and
  MIG descriptors, and the CPU allocation source. Views keep using them, and
  `qid` is never needed to render.

Anonymization ([sample.py](../qtop_py/sample.py)) keeps the backend, masks
`cluster_id`, rebuilds each `qid` from the masked cluster ID and a masked local
part, and keeps `kind`. Original identifiers never reach an archive.

## Logical relations

These are logical relations, not tables. Each row says where the relation lives
today.

| Relation | Purpose | Current representation |
|---|---|---|
| observation | schema, backend, cluster identity | the version 2 envelope (`schema_version`, `source`) |
| workload | identity, state, ownership | `jobs` (`id`, `qid`, `kind`, `user`, `state`) |
| resource | nodes, GPUs, MIG, capacity, hierarchy | `nodes` (`np`, `slurm_gpu_*` descriptors, `qid`) |
| resource_request | requested resources and constraints | future optional extension; today only backend text such as `slurm_tres_per_node` |
| allocation | actual or predicted assignments | `core_job_map`, `slurm_gpu_bindings`, `cpu_allocation_source`; future extensions |
| scheduling_domain | queues, partitions and analogous domains | `queues`, and `queue` on jobs, `qname` on nodes |

**Scheduling domains** are whatever a backend schedules into: Slurm partitions,
PBS, SGE or OAR queues, Kubernetes namespaces, or OpenStack projects. The
`queues` key stays for compatibility. The domains do not share semantics:
Slurm limits and Kubernetes quotas mean different things, so nothing in qtop
may compare domains across backends beyond their name and job counts.

## Identity lifetime

A `qid` is only as durable as the backend identifier it wraps. qtop adds a
namespace, never persistence:

- **Slurm and PBS job IDs** are unique within one cluster, until the counter
  wraps or the controller's state is reset. They are not stable across
  clusters, and a reused ID is a new job.
- **Slurm array and heterogeneous components** are identified by `id` (`101_3`,
  `101+1`), exactly as today.
- **Node names** stay stable while the node stays configured. A renamed or
  reinstalled host is, as far as qtop can tell, the same or a different node.
- **Kubernetes pod UIDs and OpenStack server UUIDs**, when such plugins exist,
  are unique by design, so their qids inherit that guarantee.

A backend that cannot name its cluster or its records stably omits `source`
and `qid` and stays at version 1. A consumer must not treat equal qids in two
observations as the same entity beyond what the backend guarantees.

## Requests, allocations and utilization

- **Request:** what a workload asked for (CPUs, GPUs, memory, constraints).
  It is not modelled yet; backend text such as `slurm_tres_alloc` is carried
  verbatim.
- **Allocation:** what the scheduler assigned or will assign. Today that is
  per-core (`core_job_map`, either exact from `scontrol` or `estimated`) and
  per-GPU (`slurm_gpu_bindings`). Allocation granularity is the finest unit the
  backend reports, and `cpu_allocation_source` says whether it is exact.
- **Utilization:** what was actually consumed over time. It is not part of an
  observation; it needs intervals and lives in FinOps adapters.

## Deferred on purpose

- Generic `resource_requests` and `allocations` collections. The future
  contract: each record references a workload `qid` and a resource `qid` from
  the same observation, with a quantity and the backend's unit, plus an exact or
  estimated flag. The existing per-node maps stay the fast path for views.
- Plugins emitting version 2. That needs a configured `cluster_id`, which is a
  new configuration key to agree first.
- Timestamps, usage intervals, FOCUS fields, billing, event history, databases
  and resource graphs or hierarchies beyond today's GPU and MIG descriptors.
