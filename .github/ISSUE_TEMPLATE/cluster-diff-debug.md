---
name: Differential debugging report
about: Report a cluster-specific qtop issue with reproducible working/failing artifacts.
title: "[cluster debug] "
labels: bug
assignees: ""
---

### Environment

- Cluster scheduler:
- qtop command used:
- Pinned qtop version / source (hash, branch, or install source):
- Exact pinned command for both runs (copy/paste):

### Required artifacts (mandatory)

- [ ] One working screenshot (`working.png`) from a single frame of `./qtop.py -b <scheduler>`
- [ ] One failing screenshot (`failing.png`) from a single frame of `./qtop.py -b <scheduler>`
- [ ] `./qtop.py -b <scheduler> -L -L` run output tarball for both runs
- [ ] The first non-empty `qtop_fullview_*.out` diff hunk between the two runs

### Differential-debugging checklist

- [ ] Same pinned qtop version was used for both working and failing runs
- [ ] Reproducible pair of runs was executed on the same or different clusters
- [ ] Initial scheduler/input diff was checked (for example, `qstat`, `oarstat`, `pbsnodes`, etc.)
- [ ] Verified and reported whether the issue reproduces on the latest qtop version

When reporting latest-version status, include:

- latest qtop version tested:
- command used for latest-version test:
