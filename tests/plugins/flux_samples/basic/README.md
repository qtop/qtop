# Flux test-instance traces

These traces were captured on 2026-11-28 from flux-core 0.91.0 and flux-sched
(the `fluxrm/flux-sched` container image), in a test instance started with
`flux start --test-size=6 --test-hosts='node[01-06]'`. Its resource
configuration names eight hosts, `node[01-08]`, with 16 cores each and four GPUs
on `node[07-08]`. Hosts 07 and 08 have no broker, so they are offline.

- `flux_jobs.json`: `flux jobs -A --json`
- `flux_resource_R.json`: `flux resource R`
- `flux_resource_list.txt`: `flux resource list -no '{state} {rlist}'`
- `flux_resource_status.txt`: `flux resource status -no '{state} {nodelist}'`
- `flux_queue_list.txt`: `flux queue list -no '{queue} {enabled} {started}'`

Queues `batch` and `long` use `node[01-06]` and `gpu` uses `node[07-08]`; `long`
is stopped. `node05` was drained while running a job, so it is `draining`. The
eleven jobs cover running jobs on one and two nodes, a job that waits for four
whole nodes, a job held with urgency 0, a job waiting on a dependency, and jobs
queued behind them or in the stopped queue.

A test instance runs every job as the instance owner, so all jobs belong to one
user, `fluxuser`. Real-instance traces are still needed for several users, jobs
sharing nodes, GPUs in use and larger hostlists.
