---
name: Differential debugging report
about: Report a cluster-specific qtop issue with reproducible working/failing artifacts.
title: "[cluster debug] "
labels: bug
assignees: ""
---

### Environment

- Cluster scheduler(s):
- Clusters used (A/B, if different):
- Pinned qtop version / source (hash, branch, or install source):
- Exact pinned command used for working run:
- Exact pinned command used for failing run:

### Required artifacts (mandatory)

- [ ] One working screenshot (`working.png`) from a single frame of `./qtop -b <scheduler>`
- [ ] One failing screenshot (`failing.png`) from a single frame of `./qtop -b <scheduler>`
- [ ] `./qtop -b <scheduler> -L -L` run output tarball for both runs
- [ ] The first non-empty `qtop_fullview_*.out` diff hunk between the two runs

Use for example:

```bash
tar -xf <working_sample>.tar -C /tmp/qtop_cluster_a
tar -xf <failing_sample>.tar -C /tmp/qtop_cluster_b
A_VIEW=$(ls /tmp/qtop_cluster_a/qtop_fullview_*.out | sort | tail -n 1)
B_VIEW=$(ls /tmp/qtop_cluster_b/qtop_fullview_*.out | sort | tail -n 1)
diff -u "$A_VIEW" "$B_VIEW" | sed -n '/^@@/,+6p' | head
```

### Differential-debugging checklist

- [ ] Same pinned qtop version was used for both working and failing runs
- [ ] Explicitly record the exact pinned qtop command and `qtop --version` output for both runs
- [ ] Reproducible pair of runs was executed on the same or different clusters
- [ ] Initial scheduler/input diff was checked (for example, `qstat`, `oarstat`, `pbsnodes`, etc.)
- [ ] Verified whether the issue reproduces on the latest qtop version
- [ ] Explicitly state if bug reproduces on the latest version: `yes` / `no`

When reporting latest-version status, include:

- latest qtop version tested:
- command used for latest-version test:
