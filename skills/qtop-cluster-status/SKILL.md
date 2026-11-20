---
name: qtop-cluster-status
description: Inspect the live, saved or historical state of an HPC batch cluster (Slurm, PBS/Torque, SGE, OAR) with qtop. Use when someone asks how busy the cluster is, why jobs are pending, which nodes are down or idle, who is using which queue, or what the cluster looked like earlier.
---

# qtop cluster status

qtop ("queue-top") turns scheduler output into one compact text view: a job
accounting summary, a worker-node by core occupancy matrix, and a table of
user accounts. It does not change the cluster, but it runs the scheduler's
query commands and writes its outputs, exports and logs to `savepath`
(`/tmp/qtop_results_$USER` by default) and `~/.local/qtop/logs/qtop.log`.

## Take one snapshot

1. Find the command: `qtop --version`, or `./qtop --version` in a checkout.
2. Run qtop once. Never use `-w` (watch mode): it is interactive and does not
   return. For repeated looks, run it again.

   ```sh
   qtop -c OFF -F                     # readable text, auto-detected scheduler
   qtop -b slurm -c OFF -F            # explicit backend: slurm, slurm_json, pbs, sge, oar
   qtop -b slurm -E -O -o savepath=DIR   # JSON export only, into DIR
   qtop -b demo -c OFF                # sanity check, no cluster needed
   ```

   - `-c OFF` keeps ANSI colour codes out of captured output.
   - `-F` shows full node names instead of numbers.
   - `-E` writes `qtop_json_<timestamp>_<random>.json` into `savepath`; `-O`
     suppresses the screen. Redirecting stdout does not capture the JSON, so
     read the file. It holds `jobs`, `nodes` and `queues` lists.
   - `-G` adds real names via `getent passwd` for every user; it can be slow on
     large LDAP sites, so add it only when names are wanted.
3. Scheduler choice: `-b` wins, then `QTOP_SCHEDULER`, then `scheduler:` in
   qtopconf.yaml. `auto` (the default) takes the first backend whose command
   exists on the host. If the output looks empty or wrong, pass `-b`.
4. A non-zero exit status with a message on stderr means qtop could not
   collect or parse the data. Report that message; do not guess the state.

## Read the output

Summarise in plain language, starting with section 1. Quote raw lines only
when asked.

- **1. Job accounting summary.** `Summary : Total:3 Up:3 Free:2 Nodes | 4/16
  cores | 3=2+1+0 jobs (Total=R+Q+Other)`: nodes in total, up and free; used
  over total cores; jobs as running + queued + other. The `Queueing` line
  gives running + queued per queue or partition; `*` marks a blocked one.
- **2. Worker nodes occupancy.** By default each node is a row (`-T` turns
  nodes into columns). The `Node state` line has one letter per node from the
  scheduler's state, e.g. `b` busy, `-` free, `d` down. Core lines hold one
  user ID character per core: `_` is a free core, `#` a core that node does
  not have.
- **3. User accounts and pool mappings.** Maps each one-character ID to its
  Unix account, with jobs as total >= R + Q + O and the number of nodes used.
- For "why is my job pending": report down or offline nodes, free cores and
  the queue's backlog. qtop shows state, not the scheduler's own reason;
  suggest the scheduler's command (e.g. `squeue -j ID -o %r` on Slurm) for that.

## Without cluster access

- `qtop -b slurm -s DIR` reads saved scheduler output from a directory or tar
  archive instead of running commands. Expected file names per scheduler are
  in docs/documentation.md.
- `qtop --load-sample ARCHIVE` reproduces an anonymized support snapshot made
  with `qtop -L -A`.
- Add `-E -O` to either for JSON instead of text.

## Earlier states (instant replay)

qtop keeps what it showed in `savepath` for 24 hours by default
(`auto_delete_old_output_files_after`). To see an earlier moment:

```sh
qtop -R 1823             # today (or yesterday) at 18:23, 2-minute window
qtop -R 1118T1823 1h     # 18 November, 18:23, 1-hour window
qtop -R 20261118T182300  # explicit date and time
```

Replay only exists for times when qtop ran in watch mode on this host with
this savepath, and prints exactly what was on screen then, filters and colour
codes included (`-c OFF` does not strip them). If qtop reports no recorded
instances, say it was not running then; do not reconstruct history.

## Configuration

- Personal settings go in `~/.local/qtop/qtopconf.yaml`, starting from the
  packaged `qtopconf.yaml`. `-o KEY=VALUE` overrides one setting for one run.
- The file uses a YAML subset read by qtop's own parser (docs/yaml-subset.md).
  Edit it as text; do not load and rewrite it with PyYAML.

## Pitfalls

- `-w` never returns in a non-interactive session.
- `--queue-nodes` is experimental: it needs `-e`, and on PBS and OAR also an
  `experimental_queue_nodes` mapping in qtopconf.yaml.
- Hosts may run Python 3.6 (e.g. AlmaLinux 8); do not assume a newer
  interpreter when scripting around qtop.
