# Visualisation topics: ideas waiting for use cases

qtop's display grew from concrete questions people asked of their clusters. This page
collects the display ideas that do not have such a question yet. Each topic says what it
is about, what qtop already does, and what would help decide the design.

It replaces four `[VIS]` issues opened in 2015, which named a direction but no use case:
[#10](https://github.com/qtop/qtop/issues/10),
[#11](https://github.com/qtop/qtop/issues/11),
[#12](https://github.com/qtop/qtop/issues/12) and
[#34](https://github.com/qtop/qtop/issues/34). Their intent lives on here.

## How to take part

If one of these topics, or a related one, matters on your cluster, please
[open a feature request](https://github.com/qtop/qtop/issues/new?template=feature_request.yml)
that describes the use case. The most useful requests say:

- **The question you want answered at a glance**, e.g. "which nodes could take a
  4-GPU job right now?" or "whose best-effort jobs would a new job preempt?".
- **The scheduler and its version**, and the shape of the cluster: node count, CPUs
  and accelerators per node, partitions or queues.
- **What you do today** to answer it: commands, scripts, dashboards.
- **A sketch of the output you would like**, as a few lines of plain text.
- **Sample input**, if you can share it: an anonymized support archive
  (`qtop -L -A`, see [support archives](documentation.md#support-archives)) lets us
  reproduce your view without your user or node names.

One request per use case is best; mention the topic below it relates to.

## Topics from the original issues

### Several values per CPU, in one line ([#10](https://github.com/qtop/qtop/issues/10))

**Intent.** A node has many CPUs, each with a state of its own. Show the
"interesting" ones in the node's single row or column, for example the minimum and
maximum of a per-CPU value such as temperature, as two extra lines.

**Today.** Each CPU cell shows the user of the job on it, or `_` when it is free.
Each node has one state cell (and a Slurm state marker such as `*` or `~`), and
GPUs, TPUs and NPUs have their own cells with `?` when their use is unknown.
There is no per-CPU measurement: qtop reads scheduler output only, and live
telemetry (NVML, temperatures) is deferred on the [roadmap](../ROADMAP.md), with a
telemetry vocabulary tracked in [#553](https://github.com/qtop/qtop/issues/553).

**Open questions.** Which per-CPU or per-device values matter to you, and where do
they come from (scheduler, `sensors`, NVML/DCGM, a monitoring system)? Is a
min/max pair per node enough, or do you need to see which CPU is the outlier?

### Container jobs ([#11](https://github.com/qtop/qtop/issues/11))

**Intent.** Some schedulers let a job act as a container that other jobs run
inside: OAR's `container` job type (inner jobs, listed by `oarstat`), and in a
similar spirit Slurm allocations that run several job steps. Show which jobs run
inside which container, without counting the cores twice.

**Today.** qtop reads `oarstat` and shows each job on the cores it holds; it does
not read OAR job types, so a container and its inner jobs look like independent
jobs. Slurm array tasks and heterogeneous components are already counted
individually.

**Open questions.** On your cluster, who uses container jobs and why? Should the
matrix show the container's owner, the inner job's owner, or both? Should the
Summary count containers, inner jobs, or each separately?

### Best-effort jobs and queues ([#12](https://github.com/qtop/qtop/issues/12))

**Intent.** Best-effort (preemptible) work fills idle resources and gives way when
regular jobs arrive: OAR's `besteffort` jobs and queue, Slurm partitions or QOS
with preemption, scavenger queues elsewhere. Make it visible that those cores are
busy but reclaimable.

**Today.** Best-effort jobs show like any other job, and their queue appears in
the queue column and the Summary's Queueing line. Slurm's preempted state (`PR`)
is recognized and counted.

**Open questions.** How should a reclaimable cell look: a dimmed colour, a
different symbol, a separate count of "free if preempted" cores in the Summary?
Which scheduler fields mark a job or queue as best-effort on your system?

### Fat nodes ([#34](https://github.com/qtop/qtop/issues/34))

**Intent.** Nodes with many CPUs (and now many accelerators) make the matrix wide
or tall, and a few such nodes among thin ones are hard to read.

**Today.** Several things help already:

- the default transposed matrix gives each node one row, its CPUs along it;
- `-l | less -RS` scrolls a wide matrix sideways;
- `rem_empty_corelines` (`-r`, `-rr`, key `r`) drops CPU lines no node has or uses;
- `stack_jobs_per_node` (key `S`) packs each node's jobs from its first CPU;
- GPUs, TPUs and NPUs get their own cells before the CPUs, so a node's
  accelerators stay visible however wide its CPU block is
  ([accelerators by node](documentation.md#accelerators-by-node-slurm)).

`qtop -b demo72` shows 144-CPU trays with 4 GPUs each.

**Open questions.** Would you rather see one cell per group of CPUs (say per 8,
or per socket), a used/total count per node, or a separate compact view for fat
nodes? How mixed is your cluster: a few fat nodes among many thin ones, or all fat?

## Related topics

These have come up while working on the topics above. None is planned; each needs
a use case to move forward.

- **More accelerator kinds.** qtop reads Slurm `gpu`, `tpu` and `npu` resources.
  Sites name others differently (for example `hpu` for Gaudi or `neuron` for
  Trainium/Inferentia); tell us the names your cluster uses.
- **Shared and sliced GPUs.** MIG slices, MPS and Slurm shards are left out of the
  device cells today. What would you want to see of them?
- **NVLink domains and other topology.** The experimental
  [topology blocks](documentation.md#experimental-topology-blocks-nvlink-domains-slurm)
  notes summarize each NVL72-style block. Is a per-block summary the right shape,
  or should blocks show in the matrix?
- **Memory.** Jobs limited by memory rather than CPUs leave cores idle that look
  free. Should qtop show memory use per node or per job?
- **Grouping nodes.** By rack, partition, feature or hardware generation, instead
  of one long list.
- **Why jobs wait.** Pending reasons (resources, priority, dependencies, limits)
  per queue or per user.
- **Reservations and maintenance.** Nodes held for a reservation or draining for
  maintenance, and when they come back.
- **Accessibility.** Colour choices that work with colour-vision deficiencies, and
  views that read well without colour (`-c OFF`).
- **Other consumers.** A stable machine-readable output (on the
  [roadmap](../ROADMAP.md)) for dashboards built on qtop's view of the cluster.
