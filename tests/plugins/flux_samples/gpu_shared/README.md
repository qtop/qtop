# Flux test-instance traces: GPUs and shared nodes

Captured on 2026-11-28 from flux-core 0.91.0 and flux-sched (the `fluxrm/flux-sched`
container image), in a test instance started with `flux start --test-size=4
--test-hosts='node[1-4]'`. Each host has 8 cores; `node3` and `node4` also have four
GPUs. There are no named queues. The same five commands as `../basic` produced the
five files.

Jobs were placed with `--requires=host:...`:

| Node | Jobs | Each job's R (instance owner only) |
|---|---|---|
| node1 | pre1, pre2 (`-c2` each) | pre1 cores 6-7, pre2 cores 4-5 |
| node2 | cfd (`-n8`) | cores 0-7 |
| node3 | train (`-c4 -g2`), infer (`-c2 -g1`), gpuwait (`-c2 -g1`) | train cores 4-7 GPUs 2-3; infer cores 2-3 GPU 1; gpuwait cores 0-1 GPU 0 |
| node4 | llm (`-c8 -g4`) | cores 0-7, GPUs 0-3 |

Only the instance owner may read a job's R, so qtop does not use it: on node1 and node3
the cores in use are exact but their owners are estimated, and node3's GPUs are in use by
an unknown job (`?`). All jobs belong to the instance owner, `fluxuser`.
