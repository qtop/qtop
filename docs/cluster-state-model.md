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

The version 2 envelope is opt-in. A version 2 state requires `source`; `qid`
and job `kind` remain optional:

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
- `qid` on jobs, nodes and queues must equal `"<backend>:<cluster_id>:<native-id>"`,
  where the native ID is the job's `id`, the node's `domainname` or the queue's
  `name`. It is unique within its relation, so qids from different backends or
  clusters cannot collide when observations are combined. A job, a node and a
  queue may share a native ID.
  `qid` stands for *qualified ID*: the backend's own job ID or node name,
  qualified by its source. It is deliberately not called `uid`, which in qtop
  means a Unix user ID, and it promises no more persistence than the backend
  identifier inside it (see [Identity lifetime](#identity-lifetime)).
- `kind` on jobs is one of `job`, `pod`, `vm` or `task`. Nodes have no kind.
- Existing fields are unchanged: `id`, `domainname`, `core_job_map`, the GPU and
  MIG descriptors, and the CPU allocation source. Views keep using them, and
  `qid` is never needed to render.

Anonymization ([sample.py](../qtop_py/sample.py)) keeps the backend, masks
`cluster_id`, and rebuilds each `qid` from the backend, the masked cluster ID
and the record's already-masked `id`, `domainname` or queue `name`. Queues,
like jobs and nodes, are rebuilt from known fields only, so an unknown field
that might hold a name is dropped. A masked qid therefore
still matches the masked job IDs in `core_job_map` and the other references.
`kind` is kept. Original identifiers never reach an archive.

## Logical relations

These are logical relations, not tables. Each row says where the relation lives
today.

| Relation | Purpose | Current representation |
|---|---|---|
| observation | schema, backend, cluster identity | the version 2 envelope (`schema_version`, `source`) |
| workload | identity, state, ownership | `jobs` (`id`, `qid`, `kind`, `user`, `state`) |
| resource | nodes, GPUs, MIG, capacity, hierarchy | `nodes` (`np`, `slurm_gpu_*` descriptors, `qid`) |
| resource_request | requested resources and constraints | future optional extension; today only backend text such as `slurm_tres_per_node` |
| allocation | actual assignments, exact or estimated | `core_job_map`, `slurm_gpu_bindings`, `cpu_allocation_source`; predictions are a separate future relation |
| scheduling_domain | queues, partitions and analogous domains | `queues`, and `queue` on jobs, `qname` on nodes |

**Scheduling domains** preserve backend-defined meanings: Slurm partitions,
PBS queues, SGE cluster queues and queue instances (`queue@host`), and OAR
queues. Future Flux scheduler scopes, Kubernetes namespaces and OpenStack
projects must retain their distinct semantics; a namespace or project is not
automatically a queue or an execution container. SGE queue instances must keep
their host-specific identities, and SGE slots are not necessarily CPU cores.
A Slurm partition's limits, a PBS queue's limits and a Kubernetes quota mean
different things, and a job may belong to several domains at once.
The `queues` key stays for compatibility. Each domain's meaning comes from its
backend, so nothing in qtop may compare domains across backends beyond their
name and job counts.

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
  It is not modelled yet; backend text such as `slurm_tres_per_node` is carried
  verbatim.
- **Allocation:** what the scheduler assigned. Today that is
  per-core (`core_job_map`, either exact from `scontrol` or `estimated`) and
  per-GPU (`slurm_gpu_bindings`). Allocation granularity is the finest unit the
  backend reports, and `cpu_allocation_source` says whether it is exact.
- **Utilization:** what was actually consumed over time. It is not part of an
  observation; it needs intervals and lives in FinOps adapters.

## Extending the schema

[Issue #593](https://github.com/qtop/qtop/issues/593) proposes typed sparse
relations (requests, allocations, topology, capacity pools, predictions). The
current schema is ready for them without carrying any of them:

- **The top level is closed.** `validate_cluster_state()` accepts only the
  relations, the known `*_diagnostics` and the envelope keys of the state's
  version (`SCHEMA_KEYS` in [cluster_state.py](../qtop_py/cluster_state.py)),
  and names any other key in its error. An older qtop therefore rejects a
  newer observation instead of silently showing part of it. A new top-level
  collection is one new `SCHEMA_KEYS` row, that is a new `schema_version`, plus
  its own validation; versions 1 and 2 keep their meaning.
- **Records are open but unvalidated.** Unknown record fields pass validation
  and are dropped by anonymization, which rebuilds jobs, nodes and queues from
  known fields. A new field that holds an identifier or a name must land with
  its validation and its masking in the same change (issue #593, invariant E12).
- **References use typed qids.** Every job, node and queue can already carry a
  validated qid. A future reference must also identify its entity relation,
  either through a type-specific field such as `workload_qid` or an explicit
  entity type. The same qid text may identify a job, a node and a queue in one
  observation; bare qid text is not a globally unique reference. References
  are scoped to an observation and inherit the backend's identity lifetime.
- **Absent is not empty.** A backend omits what it did not observe; it never
  emits empty collections or maps for completeness.
- **Predictions stay apart.** `core_job_map` holds actual or estimated
  assignments only (`cpu_allocation_source`), never predicted ones.

### Future extension contracts

The following contracts follow the
[compatibility guidance for issue #593](https://github.com/qtop/qtop/issues/593#issuecomment-6793829851).
They describe future capabilities without adding supported fields or validation
to the current schema. Implementing them requires a concrete backend observation
and a consumer, plus validation, anonymization and replay tests in the same change.
Existing `jobs`, `nodes`, `queues` and CPU/GPU maps remain authoritative for
current renderers. Plugin paths, discovery and entry points stay unchanged.

**Relationships remain distinct.** A future relation must declare its kind
and typed endpoints, preserving its backend meaning and provenance:

| Relationship | Meaning and safeguard |
|---|---|
| Workload containment | True execution nesting; a proposed `parent_qid` is only a convenience for this relation. Parent edges must be acyclic within the workload containment relation. |
| Controller ownership | For example, Kubernetes Deployment to ReplicaSet to Pod; ownership does not imply execution containment or resource allocation. |
| Resource containment | For example, GPU to node to tray to rack; validate cycles separately from workload containment. |
| Delegation | A parent reservation or nested Flux instance makes resources available to children; child consumption must not be added again to the parent's reserved capacity. |
| Domain or group membership | Administrative or scheduling scope, possibly many-to-many; membership does not imply physical containment. |
| Fabric membership and connectivity | Independent, potentially overlapping fabrics; membership alone promises no link, reachability, bandwidth or scheduling permission. |
| Workload dependency | A prerequisite and its backend condition; a predecessor absent from this observation is not necessarily invalid or completed. |
| Allocation | A workload's actual assignment to resources, with exact or estimated provenance; it is separate from requests, delegation and predicted placement. |

Each implemented relation must define how unresolved endpoints are handled.
Do not invent missing entities, infer connectivity from a host group, or use
controller ownership or project membership as a workload parent.

**Quantities carry units and semantics.** An identifiable GPU is a resource;
its HBM capacity is a quantity. A future quantity must identify its subject,
dimension, value, unit and source, and distinguish physical capacity,
allocatable capacity, request, limit, reservation, allocation, measured
utilization and administrative quota. Preserve source units or document an
explicit conversion before comparing or aggregating values. Missing,
unsupported or unknown information stays absent; a measured zero or observed
empty set is a distinct fact. Do not create objects per core, byte or other
quantity unit.

**Shared capacity is not additive by default.** Future capacity pools may
overlap or constrain several resources at once, such as fabric bandwidth,
rack power, storage throughput or licences. Record membership separately from
the pool's limit, unit and policy. Aggregation must account for overlapping
pools and nested reservations without double-counting. Pool membership alone
does not establish available capacity or a bandwidth guarantee; path,
direction and contention semantics need backend evidence before use.

**Attributes and policy stay sparse and namespaced.** Future metadata uses
collision-safe names such as `backend:key` or `site:key`; unknown attributes
must not become generic scheduling policy. Preserve native best-effort and
preemption semantics. Unsupported attributes are omitted, with no mandatory
empty dictionary per job or node. This documents a naming convention, not an
accepted `attributes` or `extensions` field or a policy language.

**Predictions require their own contract.** Before adding predicted starts or
placements, define scheduler-sourced versus estimated provenance and freshness.
Predictions never populate actual occupancy maps. Time-series accounting,
FinOps intervals, constraint evaluation and scheduling solvers stay outside
the display path.

**Implementation needs evidence.** Preserve schema-1 replay, schema-2 round
trips, Python 3.6 support and current CLI/watch rendering. New identifiers and
relations need anonymized referential-integrity tests; containment needs cycle
and missing-parent cases; dependencies need absent predecessors; overlapping
fabrics and pools need tests against false connectivity and double-counting.
Measure time and memory before and after on representative 100, 1,000 and
10,000-node fixtures, including observations without the extension. Do not
build graphs, indexes or empty extension objects unconditionally. A documented
contract alone establishes neither an adapter nor operational NVL72 support.

Open decisions, left to issue #593 on purpose:

- Extending version 2 in place or introducing version 3 for new collections,
  and whether an `extensions` namespace should open the top level to
  experimental relations.
- Whether `kind` stays a closed list (`JOB_KINDS`) or allows namespaced kinds.
- Serialized containment (`parent_qid`) and relation shapes, the extensible
  topology vocabulary, canonical units and conversion rules, and prediction
  provenance and freshness fields. The distinctions above constrain these
  decisions without prescribing runtime structures.

## Deferred on purpose

- Generic `resource_requests` and `allocations` collections. The future
  contract: each record references a workload `qid` and a resource `qid` from
  the same observation, with a quantity and the backend's unit, plus an exact or
  estimated flag. The existing per-node maps stay the fast path for views.
- Plugins emitting version 2. That needs a configured `cluster_id`, which is a
  new configuration key to agree first.
- Timestamps, usage intervals, FOCUS fields, billing, event history, databases
  and resource graphs or hierarchies beyond today's GPU and MIG descriptors.
